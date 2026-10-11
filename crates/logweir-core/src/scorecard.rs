use crate::outcome::{IntegrityLevel, IntegrityResult, LeverState, MatrixVerdict, Outcome};
use crate::spec::TargetMode;
use chrono::{DateTime, Utc};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct Scorecard {
    /// Semver of the document format, and the field every reader checks
    /// FIRST: Global Constraint 12 — readers ignore unknown fields and refuse
    /// a higher major.
    ///
    /// The schema PINS the major with a pattern rather than leaving the field
    /// an unconstrained string. Without it, a document reading `"9.9.9"`
    /// validated cleanly against the file named
    /// `logweir-drill-scorecard-1.0.0.json`, so a schema-only validator — the
    /// one route that does not go through
    /// `Scorecard::refuse_unreadable_major` — accepted exactly the document
    /// GC12 exists to refuse. Each published file pins its own major: the
    /// frozen 1.x files allow any `1.x.y` (a MINOR bump adds optional fields
    /// only and a 1.0.0 reader must still read it), and the current file,
    /// format 2.0.0 (PROD-11.1b, a partition-subset restore's), any `2.x.y`.
    #[schemars(regex(pattern = r"^2\.[0-9]+\.[0-9]+$"))]
    pub format_version: String,
    pub run_id: String,
    pub outcome: Outcome,
    /// Highest phase INDEX completed. The DOMAIN is eleven phase slots, -1
    /// through 9 (Global Constraint 18, panel decision D1, 2026-09-03); -1 is
    /// the source-side `--from-cluster` capture phase, whose code lands in a
    /// follow-up, so v0.1.0 never emits it.
    ///
    /// WHAT A v0.1.0 SIGNED DOCUMENT CAN ACTUALLY CARRY IS 5, 6 or 7, and the
    /// reason is structural rather than incidental: `phase8_score::run` signs
    /// a frozen clone, so phase 8's own record — and phase 9's, which happens
    /// after the put — are pushed onto the in-memory document AFTER the bytes
    /// were signed. A drill that completed teardown therefore reads 7 here and
    /// is NOT a drill whose teardown was skipped; the teardown is attested in
    /// its own separately signed document. `drill run`'s console line quotes
    /// this same value so the two cannot disagree.
    ///
    /// A previous version of this comment added "a value below -1 is
    /// impossible because the phase-0 admission guard refusing yields exit
    /// code 3 with last_phase_completed = -1", which implied such a document
    /// exists. It does not: exit 3 writes NO scorecard at all. The lower bound
    /// is a domain rule, enforced by `validate_invariants`, not a description
    /// of anything v0.1 emits.
    pub last_phase_completed: i8,
    pub requested_at: DateTime<Utc>,
    #[serde(default)]
    pub approval_validated_at: Option<DateTime<Utc>>,
    #[serde(default)]
    pub triggered_by: Option<String>,
    pub engine: EngineInfo,
    pub source: SourceInfo,
    pub target: TargetInfo,
    pub approval: ApprovalInfo,
    pub phases: Vec<PhaseRecord>,
    pub measured: Measured,
    pub objectives: Objectives,
    pub sample: SampleInfo,
    pub target_diff: TargetDiffSummary,
    pub integrity: Integrity,
    pub topic_parity: TopicParity,
    #[serde(default)]
    pub engine_subreport: Option<EngineSubreport>,
    pub evidence: EvidenceInfo,
    #[serde(default)]
    pub redactions: Vec<Redaction>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct EngineInfo {
    pub id: String,        // "oso-cli"
    pub version: String,   // "v0.21.0"
    pub digest: String,    // "sha256:…" — the image digest the binary came from
    pub execution: String, // "subprocess" in v0.1; "k8s-job" only under weirkeeper (SP5)
    pub levers: Levers,
    pub matrix_verdict: MatrixVerdict,
    /// REQUIRED when `matrix_verdict` is `fail`; null otherwise. Enforced by
    /// `validate_invariants`.
    #[serde(default)]
    pub matrix_verdict_reason: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct Levers {
    pub header_preflight: LeverState,
    pub dry_run_check_segments: LeverState,
    /// Every path the engine logged as `Ignoring unknown config key <path>`.
    /// A match on a Logweir-rendered key aborts the run (spec §7.2(a)).
    #[serde(default)]
    pub unknown_key_warnings: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct SourceInfo {
    pub backup_id: String,
    pub manifest_sha256: String,
    #[serde(default)]
    pub manifest_version_id: Option<String>,
    /// true ONLY under `--from-cluster`. `--from-cluster` is in v0.1 scope
    /// (Global Constraint 18 / docs/architecture.md#adr-0007-source-capture-scope); its
    /// execution path lands in a follow-up task, so nothing in the main task
    /// line yet sets this true. The invariant below is live regardless.
    pub captured_by_logweir: bool,
    /// **Format 1.3.0 (FX-8).** Which clock this restore's TIME SELECTION
    /// read, for each source topic where that is not the topic's own: see
    /// [`TimeBasisLabel`].
    ///
    /// ABSENT means NOT RECORDED — every document before 1.3.0 — and is never
    /// read as "every selection used the topic's own clock": before FX-8 a
    /// point-in-time restore over a `LogAppendTime` source was signed `pass`
    /// with no label at all (PROD-01.1, `lat`). Every run that reaches the
    /// time-basis decision writes `Some`, so a 1.3.0 document whose two lists
    /// are empty is the CLAIM that no topic was selected by producer time or
    /// with an unrecorded timestamp type AS FAR AS THE ARCHIVE MANIFEST'S
    /// SEGMENT BOUNDS SHOW (review L-1). A plan that states no point and whose
    /// `sample.window_end` is at or after every segment's first and last
    /// timestamp is not counted as a selection, yet with out-of-order
    /// timestamps inside a segment the engine's end filter can still drop a
    /// record later than both ends; that case is PROD-01.1b's.
    ///
    /// Global Constraint 12 as amended permits this as a NESTED optional
    /// field: `SourceInfo`'s properties are not the scorecard's 21.
    /// `skip_serializing_if`, so the signed 1.0.0 fixtures under
    /// `e2e/fixtures/signed/` round-trip byte for byte.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub time_basis: Option<TimeBasisLabel>,
    /// **Format 1.7.0 (PROD-11.1), and 2.0.0 (PROD-11.1b).** The plan's
    /// REPLAY SELECTION, when it states one: see [`SelectionLabel`].
    ///
    /// ABSENT means the restore selected every record of every partition of
    /// every restored topic from the archive's floor, which is what every
    /// restore before 1.7.0 did. A format-1 document's block states a window
    /// start only; a block naming partition subsets is format 2.0.0, and a
    /// 2.0.0 document always carries it (arm PS-1). Nested optional (Global
    /// Constraint 12 as amended); `skip_serializing_if`, so every document
    /// without a selection keeps its bytes.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub selection: Option<SelectionLabel>,
}

/// **PROD-11.1, scorecard format 1.7.0; PROD-11.1b, format 2.0.0.** What a
/// narrowed restore selected. The contract is
/// `docs/to-do/decisions/PROD-11.1-replay-selection.md`.
///
/// **Format 1.7.0: a START only.** `window_start_ms` and `window_end_ms`, and
/// nothing else: every partition of every restored topic is restored and
/// judged; only the window's start moved.
///
/// **Format 2.0.0: partition subsets** (the owner's decision OD-9 (a),
/// 2026-10-09). `partitions` names each narrowed topic's selected partitions
/// and `engine_runs` how many engine runs restored them (the engine's
/// partition filter applies to every topic of one run); `window_start_ms` is
/// ABSENT when the window started at the archive's floor. In a 2.0.0 document
/// the EXISTING fields name the selection, not the archive:
/// `integrity.verification.complete.partitions[]` lists every SELECTED
/// partition of every restored topic (and, with nothing expected, any other
/// partition the target holds a record in, which fails it), and the sampled
/// lane's fields (`sample.partitions`, the per-partition count bound, the
/// engine-report check) are the selected partitions'. A reader that predates
/// 2.0.0 refuses the document as an unsupported major, so none reads it as a
/// full restore.
///
/// Every verdict of such a document is judged over the selection only:
/// samples come only from selected partitions and from the stated start, the
/// count bound and the per-partition presence check are the selected
/// partitions' over `[start-or-floor, window_end_ms]`, a record in a partition
/// the plan did not select fails the run on both lanes, and a complete
/// verification expects records only from selected partitions.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct SelectionLabel {
    /// The plan's stated INCLUSIVE window start, epoch milliseconds. Present
    /// in every format-1 block (arm PS-2). In a 2.0.0 block ABSENT means the
    /// window started at the archive set's floor (guard G-WIN).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub window_start_ms: Option<i64>,
    /// The window's INCLUSIVE end, epoch milliseconds: the end of the plan's
    /// `restore.point_in_time` interval.
    pub window_end_ms: i64,
    /// **Format 2.0.0.** The per-topic partition subsets, one entry per
    /// narrowed SOURCE topic in ascending order, each list ascending and
    /// distinct (arm PS-3). A restored topic not listed was restored on every
    /// partition the archive lists for it. Required in a 2.0.0 document (arm
    /// PS-1) and never present in a format-1 one (arm PS-2).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub partitions: Option<Vec<TopicPartitions>>,
    /// **Format 2.0.0.** How many engine runs restored the selection: one per
    /// distinct subset, and one more when a restored topic has none (arm
    /// PS-4).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub engine_runs: Option<u32>,
}

/// One narrowed topic of a 2.0.0 [`SelectionLabel`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct TopicPartitions {
    /// The SOURCE topic.
    pub topic: String,
    /// Its selected partitions, ascending and distinct.
    pub partitions: Vec<i32>,
}

impl SelectionLabel {
    /// Whether `topic`'s `partition` is in this selection: a topic the block
    /// does not list restores every partition.
    #[must_use]
    pub fn selects_partition(&self, topic: &str, partition: i32) -> bool {
        self.partitions
            .iter()
            .flatten()
            .find(|tp| tp.topic == topic)
            .is_none_or(|tp| tp.partitions.contains(&partition))
    }

    /// Whether the block names a partition subset (format 2.0.0).
    #[must_use]
    pub fn narrows_partitions(&self) -> bool {
        self.partitions.as_ref().is_some_and(|p| !p.is_empty())
    }

    /// The `replay selection:` sentence, ending in what the document proves:
    /// about records of the OTHER partitions of a narrowed topic
    /// ([`OutsideTheSubset`], a 2.0.0 block only) and about records BEFORE a
    /// stated start (review N1, [`BeforeTheStart`]). The same words in the
    /// writer's `sample.coverage_note` ([`Self::coverage_note`]) and in the
    /// line both readers print (`logweir::verify::selection_lines`,
    /// `docs/verify_scorecard.py::_selection_lines`).
    #[must_use]
    pub fn sentence(&self, before: BeforeTheStart, outside: OutsideTheSubset) -> String {
        let end = self.window_end_ms;
        let Some(subsets) = self.partitions.as_ref().filter(|p| !p.is_empty()) else {
            // Format 1.7.0, a start only: the 1.23.0 sentence, byte for byte.
            return format!(
                "replay selection: every partition of every restored topic, from epoch-ms {} \
                 (the plan's stated window start, inclusive) to epoch-ms {end} (inclusive); {}",
                self.window_start_ms.unwrap_or_default(),
                before.words()
            );
        };
        let named: Vec<String> = subsets
            .iter()
            .map(|tp| {
                let list: Vec<String> = tp.partitions.iter().map(i32::to_string).collect();
                format!("{} partitions [{}]", tp.topic, list.join(", "))
            })
            .collect();
        let from = match self.window_start_ms {
            Some(ms) => format!("from epoch-ms {ms} (the plan's stated window start, inclusive)"),
            None => "from the archive's floor".to_string(),
        };
        let mut s = format!(
            "replay selection: ONLY {} (every partition of any other restored topic), {from} to \
             epoch-ms {end} (inclusive), in {} engine run(s); {}",
            named.join("; "),
            self.engine_runs.unwrap_or_default(),
            outside.words()
        );
        if self.window_start_ms.is_some() {
            s.push_str("; ");
            s.push_str(before.words());
        }
        s
    }

    /// The sentence that opens the writer's `sample.coverage_note`, naming
    /// the selection in an EXISTING field (PROD-11.1 §5.2). Written before
    /// phase 7 judges anything, so it claims only what the plan's lane can say
    /// then: a sampled lane says it cannot show that no record before the
    /// start was restored; a complete lane says only that none was expected
    /// (whether none was restored is the complete block's verdict, which a
    /// reader states — [`BeforeTheStart::of`]); and of the other partitions
    /// of a narrowed topic, only that no record was expected
    /// ([`OutsideTheSubset::of`] is the reader's).
    #[must_use]
    pub fn coverage_note(&self, coverage: crate::spec::Coverage) -> String {
        self.sentence(
            match coverage {
                crate::spec::Coverage::Sampled => BeforeTheStart::SampledUnproved,
                crate::spec::Coverage::Complete => BeforeTheStart::Expected,
            },
            OutsideTheSubset::Expected,
        )
    }
}

/// What a 2.0.0 document proves about the OTHER partitions of a topic its
/// selection narrowed (PROD-11.1b). A verification whose `integrity.result` is
/// `pass` shows that none of them holds a restored record, on either lane:
/// the sampled lane holds every target partition the plan did not select to
/// empty (a record there is a finding, `fail`), and the complete lane lists
/// such a partition with nothing expected, so a record there is `unexpected`
/// and IV-6 refuses the pass. Anything else (a verdict that did not pass, or
/// no verification) proves only that none was expected.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OutsideTheSubset {
    /// A verification that passed: none was restored or expected.
    ProvedNoneRestored,
    /// Anything else: none was expected, nothing more.
    Expected,
}

impl OutsideTheSubset {
    /// What THIS document proves, from its `integrity.result` and its
    /// `integrity.verification`. The one predicate both readers use.
    #[must_use]
    pub fn of(integrity: &IntegrityResult, verification: Option<&Verification>) -> Self {
        if verification.is_some() && *integrity == IntegrityResult::Pass {
            Self::ProvedNoneRestored
        } else {
            Self::Expected
        }
    }

    /// The clause the subset half of the `replay selection:` sentence ends
    /// in.
    #[must_use]
    pub fn words(self) -> &'static str {
        match self {
            Self::ProvedNoneRestored => {
                "no record of another partition of these topics was restored or expected"
            }
            Self::Expected => "no record of another partition of these topics was expected",
        }
    }
}

/// What a document from a stated start proves about the records BEFORE that
/// start (PROD-11.1 review N1). Only a COMPLETE verification whose
/// `integrity.result` is `pass` shows that none was restored: there a
/// restored record below the start is `unexpected`, and IV-6 holds a
/// complete pass to no unexpected record in any partition. A SAMPLED check
/// cannot: its sample is drawn from the window, and its per-partition count
/// bound counts every record of a segment that straddles the start, so an
/// engine that restored a straddling segment's earlier records stays inside
/// every bound.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BeforeTheStart {
    /// A complete verification that passed: none was restored or expected.
    ProvedNoneRestored,
    /// A sampled verification: none was expected; the check does not show
    /// that none was restored.
    SampledUnproved,
    /// Anything else — a complete verification that did not pass, or no
    /// verification (phase 7 never ran): none was expected, nothing more.
    Expected,
}

impl BeforeTheStart {
    /// What THIS document proves, from its `integrity.result` and its
    /// `integrity.verification`. The one predicate both readers use.
    #[must_use]
    pub fn of(integrity: &IntegrityResult, verification: Option<&Verification>) -> Self {
        match verification {
            Some(v) if v.coverage == COVERAGE_COMPLETE && *integrity == IntegrityResult::Pass => {
                Self::ProvedNoneRestored
            }
            Some(v) if v.coverage == COVERAGE_SAMPLED => Self::SampledUnproved,
            _ => Self::Expected,
        }
    }

    /// The clause that ends the `replay selection:` sentence.
    #[must_use]
    pub fn words(self) -> &'static str {
        match self {
            Self::ProvedNoneRestored => "no record before the start was restored or expected",
            Self::SampledUnproved => {
                "no record before the start was expected; a sampled check does not prove that \
                 none was restored"
            }
            Self::Expected => "no record before the start was expected",
        }
    }
}

/// **FX-8, scorecard format 1.3.0.** What the restore's time selection read,
/// per selected source topic.
///
/// # Why a restore needs this label at all
///
/// The pinned engine archives each record's PRODUCER timestamp (PROD-01.1
/// S3), so every selection by time — a stated `restore.point_in_time`, or a
/// `sample.window_end` earlier than what the archive holds — reads producer
/// time. For a `CreateTime` topic that is the topic's own clock. For a
/// `LogAppendTime` topic it is not, and phase 7 cannot notice: it compares the
/// target with the archive, and both carry producer time. So the runner
/// REFUSES such a selection (`PointInTimeByProducerTime`) unless the approved
/// plan states `restore.time_basis: producerTime`, and this block is what the
/// signed document says about the selections it did make.
///
/// # The three fields
///
/// | field | meaning |
/// |---|---|
/// | `plan` | the approved plan's `restore.time_basis`, copied: `producerTime`, or ABSENT when the plan stated none |
/// | `producer_time` | source topics whose recorded timestamp type is `LogAppendTime` and which this restore selected by time — by producer time, accepted by `plan` (arm TB-3) |
/// | `not_recorded` | source topics this restore selected by time while their timestamp type was NOT RECORDED: no `message.timestamp.type` override in the archive manifest, and no effective value in a verified backup receipt (FX-4) — so the selection may have read producer time |
///
/// A topic whose recorded type is `CreateTime`, and a topic the restore did
/// not select by time (no `restore.point_in_time`, and a `sample.window_end`
/// at or after every timestamp the manifest records for it), is in neither
/// list. Both lists are sorted topic names.
///
/// # Why the unknown case runs and is labelled rather than refused
///
/// Rule 3 of the expansion tracker: never read old evidence as a stronger
/// guarantee. A receipt from before FX-4, or a plan bound to no receipt,
/// carries no effective timestamp type, and most topics carry no manifest
/// override; reading that silence as `CreateTime` would be the stronger
/// reading, so it is never taken. Refusing would be the other mistake: it
/// would stop every point-in-time restore of every archive written before
/// FX-4, where a `CreateTime` topic's selection is right. So the restore runs
/// and this list says, in the signed document, that the clock it selected by
/// is unknown.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct TimeBasisLabel {
    /// `producerTime` ([`TIME_BASIS_PRODUCER_TIME`], arm TB-2) when the
    /// approved plan stated `restore.time_basis: producerTime`; absent when it
    /// stated none.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub plan: Option<String>,
    /// Source topics selected by PRODUCER time: recorded `LogAppendTime`,
    /// selected by time, accepted by `plan` (arm TB-3).
    pub producer_time: Vec<String>,
    /// Source topics selected by time whose timestamp type was not recorded.
    pub not_recorded: Vec<String>,
}

/// The first minor of scorecard format 1 that defines `source.time_basis`
/// (FX-8), which arm TB-1 enforces. **A renumber changes this and
/// [`crate::FORMAT_VERSION`] together**; TB-1's message is built from it,
/// `docs/verify_scorecard.py`'s `SCORECARD_TIME_BASIS_SINCE_MINOR` must equal
/// it (`docs/test_verify_scorecard.py::
/// test_the_time_basis_minor_is_the_rust_readers`), and
/// `the_written_version_defines_time_basis` keeps the pair coherent.
pub const TIME_BASIS_SINCE_MINOR: u64 = 3;

/// **PROD-01.3.** The first minor of scorecard format 1 whose
/// `target.auth.mode` may be one of `crate::connection::PROD_01_3_AUTH_MODES`
/// (`scramSha256`, `plain`, `mtls`). **A renumber changes this and
/// [`FORMAT_VERSION_WITH_AUTH_MODES`] together**, and
/// `docs/verify_scorecard.py`'s `SCORECARD_AUTH_MODES_SINCE_MINOR` follows it.
pub const AUTH_MODES_SINCE_MINOR: u64 = 5;

/// **PROD-01.3.** The `format_version` of a scorecard whose `target.auth.mode`
/// is one of the modes PROD-01.3 added — a MINOR bump over
/// [`crate::FORMAT_VERSION`] (1.4.0) for new content in an existing field that
/// an older reader can only refuse (OD-7, third case). Written only for those
/// modes ([`format_version_for_target`]), so every scorecard of a restore into
/// a `plaintext` or `scramSha512` target is the 1.4.0 document it was.
pub const FORMAT_VERSION_WITH_AUTH_MODES: &str = "1.5.0";

/// **FX-23.** The first minor of scorecard format 1 that defines
/// `sample.unsampled_topics`, which arm US-1 enforces. **A renumber changes
/// this and [`FORMAT_VERSION_WITH_UNSAMPLED_TOPICS`] together**, and
/// `docs/verify_scorecard.py`'s `SCORECARD_UNSAMPLED_TOPICS_SINCE_MINOR`
/// follows it.
pub const UNSAMPLED_TOPICS_SINCE_MINOR: u64 = 6;

/// **FX-23.** The `format_version` of every SAMPLED-lane scorecard a build
/// with FX-23's checks signs, and of every one that carries
/// `sample.unsampled_topics` — a MINOR bump for a new optional field, under
/// OD-7: arms US-1 to US-3 read only that field and can only refuse. Written
/// for every sampled document so the version marks the fixed build (the
/// orchestrator's decision on review M2, 2026-10-08): a 1.4.0 or 1.5.0
/// document is otherwise the same bytes whichever build signed it. A
/// complete verification's scorecard is the 1.4.0 or 1.5.0 document it was
/// ([`format_version_with_sample`]). The newest minor: the current schema
/// file is this version's.
pub const FORMAT_VERSION_WITH_UNSAMPLED_TOPICS: &str = "1.6.0";

/// **PROD-11.1.** The first minor of scorecard format 1 that defines
/// `source.selection`, which arm SEL-1 enforces. **A renumber changes this and
/// [`FORMAT_VERSION_WITH_SELECTION`] together**, and
/// `docs/verify_scorecard.py`'s `SCORECARD_SELECTION_SINCE_MINOR` follows it.
pub const SELECTION_SINCE_MINOR: u64 = 7;

/// **PROD-11.1.** The `format_version` of a scorecard that carries
/// `source.selection` (a stated window start) — a MINOR bump for a new
/// optional block, under OD-7: arms SEL-1 to SEL-3 read only that block, or
/// judge an existing field against it and can only refuse. Written only for a
/// restore that states a start ([`format_version_with_selection`]), so every
/// other document is the one it was. The newest minor: the current schema
/// file is this version's.
pub const FORMAT_VERSION_WITH_SELECTION: &str = "1.7.0";

/// **PROD-15.1.** The first minor of scorecard format 1 that defines
/// `target.original_name`, which arm ON-1 enforces. **A renumber changes this
/// and [`FORMAT_VERSION_WITH_ORIGINAL_NAME`] together**, and
/// `docs/verify_scorecard.py`'s `SCORECARD_ORIGINAL_NAME_SINCE_MINOR` follows
/// it.
pub const ORIGINAL_NAME_SINCE_MINOR: u64 = 8;

/// **PROD-15.1.** The `format_version` of a scorecard that carries
/// `target.original_name` — a restore under the source's ORIGINAL topic names
/// into absent topics (OD-2). A MINOR bump for a new optional block, under
/// OD-7 (a): arms ON-1 to ON-14 read only that block (ON-2, ON-3, ON-7,
/// ON-13 and ON-14 judge existing fields against it) and can only refuse.
/// Written only for an original-name restore
/// ([`format_version_with_original_name`]), so every other document is the
/// one it was. The newest minor of FORMAT 1:
/// `schemas/logweir-drill-scorecard-1.8.0.json` is this version's file
/// ([`crate::schema::scorecard_format_1_schema`]).
///
/// **The block is format 1's only.** An original-name restore restores whole
/// topics (`crate::original_name::refuse_shape`,
/// `OriginalNameNeedsWholeTopics`), and a 2.x document is a partition-subset
/// restore's, so no 2.x document carries the block: arm ON-14 refuses the
/// pair, ON-1 reads major 1 on purpose, and the 2.0.0 schema does not
/// describe it.
pub const FORMAT_VERSION_WITH_ORIGINAL_NAME: &str = "1.8.0";

/// The `format_version` a scorecard is written with once its target block is
/// known (PROD-15.1): at least [`FORMAT_VERSION_WITH_ORIGINAL_NAME`] when it
/// carries `target.original_name`, else `current` unchanged. 1.8.0 defines
/// everything 1.7.0 does. Monotonic: never lowers `current` — so a `current`
/// of 2.0.0 (a partition subset) stays 2.0.0, and such a document is then
/// refused at signing by arm ON-14. The runner never gets there: it refuses
/// the plan before anything is created.
#[must_use]
pub fn format_version_with_original_name<'a>(
    current: &'a str,
    block: Option<&OriginalNameInfo>,
) -> &'a str {
    if block.is_some() {
        newer_format_version(current, FORMAT_VERSION_WITH_ORIGINAL_NAME)
    } else {
        current
    }
}

// ---------------------------------------------------------------------------
// THE APPROVAL-MODE VOCABULARY — ONE DEFINITION (PROD-16.2)
//
// How a run was authorised, in the words a scorecard signs. A closed set
// inside signed evidence: every holder of it is listed here, and the two that
// cannot share this definition are held to it by a row.
//
// * the runner's writer takes its words from here
//   (`logweir::drill::phase1_approval::APPROVAL_MODE_*` are these constants);
// * `logweir_core::approval_policy::ApprovalRoute::approval_mode` maps a
//   policy's row to its word;
// * this file's arms ON-5, ON-11 and CA-2, CA-8 read the set;
// * `docs/verify_scorecard.py` holds its own copy (it shares no code with
//   this crate, on purpose), and
//   `crates/logweir/tests/two_reader_parity.rs` reads that copy out of the
//   script and compares it with this one, member for member;
// * the JSON schemas type both fields as strings and enumerate nothing;
// * nothing else holds it: the controller copies no approval mode into
//   `Restore.status`, the product API and the console show the POLICY's mode
//   (`confirm`, `two-person`, `strict`; `Governed`, `Ordinary` in the audit
//   record), which is `logweir_core::approval_policy`'s vocabulary, and no
//   metric is labelled with either.
// ---------------------------------------------------------------------------

/// A per-run approval document v1: an approver's personal key, through the
/// CLI or in a namespace on `legacy-governed-v1`.
pub const APPROVAL_MODE_V1: &str = "v1Approval";
/// An authorization document v2 under a `Governed` policy whose approval a
/// personal `GovernedApproval` key countersigns (`strict`).
pub const APPROVAL_MODE_GOVERNED: &str = "governed";
/// An authorization document v2 under an `Ordinary` policy: the requester's
/// own confirmation in the console (`confirm`; OD-10).
pub const APPROVAL_MODE_ORDINARY: &str = "ordinary";
/// **PROD-16.2.** An authorization document v2 under a `Governed` policy
/// whose `approverSignature` is `Console`: a second person signed in to the
/// console and approved, and the console signed who and when
/// (`two-person`). Defined from scorecard format 1.9.0 (and 2.1.0): never
/// [`APPROVAL_MODE_GOVERNED`], because no personal key countersigned and a
/// reader must not be told one did.
pub const APPROVAL_MODE_CONSOLE: &str = "consoleApproval";
/// A standing rehearsal authorization. Never signed into
/// `target.original_name.approval_mode`: it never authorises an original-name
/// restore.
pub const APPROVAL_MODE_STANDING: &str = "standing";

/// Every word phase 1 can report for the document that authorised a run.
pub const APPROVAL_MODES: [&str; 5] = [
    APPROVAL_MODE_V1,
    APPROVAL_MODE_GOVERNED,
    APPROVAL_MODE_ORDINARY,
    APPROVAL_MODE_CONSOLE,
    APPROVAL_MODE_STANDING,
];

/// `target.original_name.approval_mode`'s closed set (arm ON-5): the approval
/// document phase 1 verified — a per-run approval document v1 (the CLI, or a
/// namespace on `legacy-governed-v1`), or an authorization document v2 under
/// a `Governed` policy (a personal key, or — PROD-16.2, from 1.9.0 — a second
/// person in the console) or an `Ordinary` one. A standing rehearsal
/// authorization never authorises an original-name restore.
///
/// **The set is four from 1.9.0, and was three at 1.8.0**
/// ([`ORIGINAL_NAME_APPROVAL_MODES_AT_1_8_0`]): [`APPROVAL_MODE_CONSOLE`] is
/// the member PROD-16.2 added, a value of 1.9.0 and later. ON-5 is split by
/// version, as PROD-01.3 split `target.auth.mode`'s arm: under 1.8.0 the new
/// member is refused as a value that version does not define (the sentence
/// names the version), every 1.8.0 document is judged exactly as before, and
/// from 1.9.0 the closed set is these four. A reader built at 1.8.0 refuses a
/// 1.9.0 document naming the new member through its own closed-set sentence
/// (the safer verdict: OD-7's third case), and this build accepts the member
/// only beside the `approval.console` block that says who approved (arm
/// CA-8).
pub const ORIGINAL_NAME_APPROVAL_MODES: [&str; 4] = [
    APPROVAL_MODE_V1,
    APPROVAL_MODE_GOVERNED,
    APPROVAL_MODE_ORDINARY,
    APPROVAL_MODE_CONSOLE,
];

/// `target.original_name.approval_mode`'s closed set under scorecard 1.8.0,
/// PROD-15.1's: every member of [`ORIGINAL_NAME_APPROVAL_MODES`] but the one
/// 1.9.0 added.
pub const ORIGINAL_NAME_APPROVAL_MODES_AT_1_8_0: [&str; 3] = [
    APPROVAL_MODE_V1,
    APPROVAL_MODE_GOVERNED,
    APPROVAL_MODE_ORDINARY,
];

/// **PROD-16.2.** The first minor of scorecard format 1 that defines
/// `approval.console` (arm CA-1). A renumber changes this and
/// [`FORMAT_VERSION_WITH_CONSOLE_APPROVAL`] together, and
/// `docs/verify_scorecard.py`'s `SCORECARD_CONSOLE_APPROVAL_SINCE_MINOR`
/// follows it.
pub const CONSOLE_APPROVAL_SINCE_MINOR: u64 = 9;

/// **PROD-16.2.** The first minor of scorecard format 2 that defines
/// `approval.console`. Format 2 is a partition-subset restore's, and such a
/// restore may be approved in the console like any other, so the block is
/// defined in BOTH lines: this is the 2.x minor that "moves with" 1.9.0
/// (`docs/stability.md`).
pub const CONSOLE_APPROVAL_SINCE_MINOR_OF_MAJOR_2: u64 = 1;

/// **PROD-16.2.** The `format_version` of a scorecard that carries
/// `approval.console` — a restore a second person approved in the console —
/// when it is a format 1 document. A MINOR bump for a new optional block and
/// one new member of an existing closed set, under OD-7: arms CA-1 to CA-8
/// read only the block, or judge an existing field against it, and can only
/// refuse; a reader that knows `target.original_name` and not this refuses
/// the new member of `approval_mode` (the safer verdict). Written only for
/// such a restore ([`format_version_with_console_approval`]), so every other
/// document is the one it was. Format 1's newest minor:
/// `schemas/logweir-drill-scorecard-1.9.0.json`.
pub const FORMAT_VERSION_WITH_CONSOLE_APPROVAL: &str = "1.9.0";

/// **PROD-16.2.** The same, for a PARTITION-SUBSET restore approved in the
/// console: format 2's first minor. 2.1.0 is 2.0.0's fields plus the
/// optional block, and is written only for that pair.
/// `schemas/logweir-drill-scorecard-2.1.0.json`.
pub const FORMAT_VERSION_SUBSET_WITH_CONSOLE_APPROVAL: &str = "2.1.0";

/// Whether a document of `format_version` defines `approval.console` (arm
/// CA-1): a 1.x document from 1.9.0 on, or a 2.x document from 2.1.0 on.
#[must_use]
pub fn defines_console_approval(format_version: &str) -> bool {
    let Some(minor) = minor_version(format_version) else {
        return false;
    };
    match major_version(format_version) {
        Some(1) => minor >= CONSOLE_APPROVAL_SINCE_MINOR,
        Some(PARTITION_SUBSETS_MAJOR) => minor >= CONSOLE_APPROVAL_SINCE_MINOR_OF_MAJOR_2,
        _ => false,
    }
}

/// The `format_version` a scorecard is written with once its approval is
/// known (PROD-16.2): when it carries `approval.console`, at least
/// [`FORMAT_VERSION_SUBSET_WITH_CONSOLE_APPROVAL`] for a major-2 document
/// (a partition subset) and at least [`FORMAT_VERSION_WITH_CONSOLE_APPROVAL`]
/// for any other; else `current` unchanged. Monotonic within the major: it
/// never lowers `current` and never changes its major.
///
/// THE LAST VERSION STEP. It reads the major the earlier steps chose, so it
/// runs after [`format_version_with_selection`]; a document that carried the
/// block under a version that does not define it would be refused at signing
/// by arm CA-1, never signed.
#[must_use]
pub fn format_version_with_console_approval<'a>(
    current: &'a str,
    console: Option<&ConsoleApprovalInfo>,
) -> &'a str {
    if console.is_none() {
        return current;
    }
    if major_version(current) == Some(PARTITION_SUBSETS_MAJOR) {
        newer_format_version(current, FORMAT_VERSION_SUBSET_WITH_CONSOLE_APPROVAL)
    } else {
        newer_format_version(current, FORMAT_VERSION_WITH_CONSOLE_APPROVAL)
    }
}

/// One of the two people of a console approval, as the console attested
/// them: an identity provider's issuer and its subject.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct ConsolePrincipal {
    /// The identity issuer.
    pub issuer: String,
    /// The subject within that issuer.
    pub subject: String,
}

impl ConsolePrincipal {
    /// `<issuer>#<subject>`, the form `approval.approver` carries.
    #[must_use]
    pub fn principal_id(&self) -> String {
        format!("{}#{}", self.issuer, self.subject)
    }
}

/// **PROD-16.2, scorecard format 1.9.0 (and 2.1.0): the run was approved by a
/// SECOND PERSON IN THE CONSOLE.** Present exactly on such a run's documents.
///
/// It says WHO approved and HOW, so a reader does not have to infer it from a
/// key id: the mode, the requester, the approver, when the request was made,
/// when it was approved and when it would have expired, and the console key
/// that signed both the request and the approval.
///
/// **What it changes about the fields beside it**, which is why both readers
/// hold them to it (arms CA-5 to CA-7): under this mode `approval.approver`
/// is the approver's `<issuer>#<subject>`, `approval.approved_at` is the
/// instant they approved, and `approval.key_id` is the CONSOLE's key id — the
/// console signs the approval, so the approver's key is the console key, and
/// that is expected here and nowhere else.
///
/// **What it does not say.** That two different people exist: the console
/// attested two identities of one issuer, and whoever controls the console,
/// its key or the identity provider can produce both (`SECURITY.md`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct ConsoleApprovalInfo {
    /// [`APPROVAL_MODE_CONSOLE`] (arm CA-2).
    pub mode: String,
    /// Who asked, as the console attested them (arm CA-3).
    pub requester: ConsolePrincipal,
    /// Who approved, as the console attested them: a second person of the
    /// requester's own issuer (arm CA-3).
    pub approver: ConsolePrincipal,
    /// When the console signed the request.
    pub requested_at: DateTime<Utc>,
    /// When the approver approved: not before the request and before its
    /// expiry (arm CA-4).
    pub approved_at: DateTime<Utc>,
    /// When the request would have stopped being approvable.
    pub request_expires_at: DateTime<Utc>,
    /// The console key that signed the request and the approval.
    pub confirmation_key_id: String,
}

impl ConsoleApprovalInfo {
    /// The `console approval:` line both readers print for a console approval
    /// (`logweir::verify::console_approval_lines`,
    /// `docs/verify_scorecard.py::_console_approval_lines`;
    /// `scripts/check-verifier-parity.sh` compares every line starting
    /// `console approval:` between the two). It says WHO approved and HOW:
    /// the mode, both people, both instants, the request's expiry, and that
    /// the console key signed both documents, which is expected in this mode.
    /// Called only on a document whose invariants hold, so both principals
    /// are bounded, visible ASCII (arm CA-3). Instants are printed in UTC, to
    /// the precision the document carries.
    #[must_use]
    pub fn lines(&self) -> Vec<String> {
        vec![format!(
            "console approval: mode {}; requested by {} at {}; approved in the console by {} at \
             {} (the request expired at {}); the console key {} signed the request and the \
             approval, which is expected in this mode: no personal key is involved",
            self.mode,
            self.requester.principal_id(),
            self.requested_at
                .to_rfc3339_opts(chrono::SecondsFormat::AutoSi, true),
            self.approver.principal_id(),
            self.approved_at
                .to_rfc3339_opts(chrono::SecondsFormat::AutoSi, true),
            self.request_expires_at
                .to_rfc3339_opts(chrono::SecondsFormat::AutoSi, true),
            self.confirmation_key_id
        )]
    }
}

/// **PROD-15.1, scorecard format 1.8.0: the restore wrote under the source's
/// ORIGINAL topic names**, into topics phase 0 proved absent and the run
/// created itself, exclusively (`crate::original_name`). Present exactly on
/// such a restore's documents.
///
/// It records what the run PROVED before it wrote, so a reader can tell the
/// one original-name path from an ordinary restore and see which of OD-2's
/// conditions admitted it: the separate approval subject and the approval
/// mode it was verified under, which cluster condition held, where the run
/// looked for a declarative owner and what it found, and whether the plan
/// chose the owner path.
///
/// **The restored topic is a NEW GENERATION of its name, never the original
/// topic.** Kafka assigns topic ids at creation and none can be preserved; the
/// block claims the name, not the identity (PROD-01.4 §7).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct OriginalNameInfo {
    /// `originalName`, the approval subject phase 1 verified (arm ON-4).
    pub approval_subject: String,
    /// The approval document it was verified in
    /// ([`ORIGINAL_NAME_APPROVAL_MODES`], arm ON-5).
    pub approval_mode: String,
    /// Which of OD-2's two cluster conditions admitted the identity mapping
    /// (`crate::original_name::CLUSTER_CONDITIONS`, arm ON-6):
    /// `targetIsNotSource` (a known source cluster id differs from
    /// `target.cluster_id`) or `autoCreateDisabled` (the target is, or may be,
    /// the source cluster, and every broker reported
    /// `auto.create.topics.enable=false`).
    pub cluster_condition: String,
    /// The source cluster id the condition compared, when one was known: the
    /// bound point's verified receipt, measured at backup time (an unsigned
    /// runner input never counts). Required beside `targetIsNotSource` (arm
    /// ON-7).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_cluster_id: Option<String>,
    /// Where the run looked for declarative owners of the restored names
    /// (`crate::original_name::OWNER_DETECTION_PLACES`): never empty, each
    /// place once (arm ON-8).
    pub owner_detection: Vec<String>,
    /// Every owner found, sorted (arm ON-9).
    pub owners: Vec<OriginalNameOwner>,
    /// Whether the approved plan chose the owner path. Required for any owner
    /// found (arm ON-10).
    pub owner_path: bool,
    /// **OD-10.** How a one-person confirmation was made:
    /// [`crate::original_name::CONFIRMATION_TYPED_TOPIC_NAMES`] — the
    /// requester re-typed every original topic name, exactly, and the console
    /// signed what was typed. Present exactly when `approval_mode` is
    /// `ordinary` (arm ON-11): every other mode has a second person.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub confirmation: Option<String>,
    /// `sha256:<64 hex>` of the `KafkaTopic` resources file the runner was
    /// given (`--kafka-topic-resources`): an unsigned runner input, so the
    /// document names exactly which file it looked in. Present exactly when
    /// `owner_detection` lists `kafkaTopicResources` (arm ON-12).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub kafka_topic_resources_sha256: Option<String>,
}

/// One declarative owner an original-name restore found for a restored name.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct OriginalNameOwner {
    /// The restored (original) topic name.
    pub topic: String,
    /// `strimzi` or `external`.
    pub kind: String,
    /// Where the desired state lives (a `KafkaTopic` `namespace/name`, a
    /// repository path).
    pub reference: String,
    /// The place that named it: one of `owner_detection`.
    pub found_in: String,
}

impl OriginalNameInfo {
    /// The `original name:` line both readers print
    /// (`logweir::verify::original_name_lines`,
    /// `docs/verify_scorecard.py::_original_name_lines`).
    #[must_use]
    pub fn lines(&self) -> Vec<String> {
        let cluster = match self.cluster_condition.as_str() {
            "targetIsNotSource" => format!(
                "the target cluster is not the source cluster ({})",
                self.source_cluster_id.as_deref().unwrap_or("unknown")
            ),
            _ => match self.source_cluster_id.as_deref() {
                Some(source) => format!(
                    "the target may be the source cluster ({source}) and every broker reported \
                     auto.create.topics.enable=false"
                ),
                None => "no source cluster id was known and every broker reported \
                         auto.create.topics.enable=false"
                    .to_string(),
            },
        };
        let confirmed = match self.confirmation.as_deref() {
            Some(crate::original_name::CONFIRMATION_TYPED_TOPIC_NAMES) => {
                " (the requester re-typed every original topic name)"
            }
            _ => "",
        };
        let mut lines = vec![format!(
            "original name: restored under the source's own topic names, into topics this run \
             created (a new generation of each name, not the original topic); approval subject \
             {}, approved by {}{confirmed}; {cluster}",
            self.approval_subject, self.approval_mode
        )];
        let owners = if self.owners.is_empty() {
            "none found".to_string()
        } else {
            self.owners
                .iter()
                .map(|o| {
                    format!(
                        "{} ({} {}, from {})",
                        o.topic, o.kind, o.reference, o.found_in
                    )
                })
                .collect::<Vec<_>>()
                .join(", ")
        };
        lines.push(format!(
            "original name: declarative owners looked for in {}: {owners}{}{}",
            self.owner_detection.join(", "),
            if self.owner_path {
                "; the approved plan chose the owner path"
            } else {
                ""
            },
            match self.kafka_topic_resources_sha256.as_deref() {
                Some(digest) => format!("; KafkaTopic resources {digest}"),
                None => String::new(),
            }
        ));
        lines
    }
}

/// **PROD-11.1b (the owner's decision OD-9 (a), 2026-10-09).** The
/// `format_version` of a scorecard whose restore stated a PARTITION SUBSET —
/// the format's first MAJOR. It is 1.7.0's fields with the subset meaning:
/// `source.selection.partitions` is required (arm PS-1), and the existing
/// `integrity.verification.complete.partitions[]` and the sampled lane's
/// fields name the SELECTED partitions only, which a 1.x reader would read as
/// every partition. So every reader before it refuses it as an unsupported
/// major, and it is written ONLY for a subset restore
/// ([`format_version_with_selection`]): every other document stays 1.x, byte
/// for byte. The newest version: the current schema file is this version's.
pub const FORMAT_VERSION_WITH_PARTITION_SUBSETS: &str = "2.0.0";

/// The major of [`FORMAT_VERSION_WITH_PARTITION_SUBSETS`]: the newest major
/// this build reads, and only for that shape (arm PS-1).
pub const PARTITION_SUBSETS_MAJOR: u64 = 2;

/// Whether a document of `format_version` defines a field format 1 added at
/// minor `since_minor`: a 1.x document from that minor on, and every document
/// of major 2, which is 1.7.0's fields plus the subset meaning (PROD-11.1b).
/// `false` for a version that does not parse or names another major.
#[must_use]
pub fn defines_format_1_minor(format_version: &str, since_minor: u64) -> bool {
    match major_version(format_version) {
        Some(1) => minor_version(format_version).is_some_and(|minor| minor >= since_minor),
        Some(PARTITION_SUBSETS_MAJOR) => true,
        _ => false,
    }
}

/// Whether a document of `format_version` defines `target.original_name`
/// (arm ON-1, PROD-15.1): a 1.x document from 1.8.0 on, and NO document of
/// another major. Unlike [`defines_format_1_minor`], major 2 does not define
/// it: a 2.x document is a partition-subset restore's, and a restore under
/// the original topic names restores whole topics (arm ON-14).
#[must_use]
pub fn defines_original_name(format_version: &str) -> bool {
    major_version(format_version) == Some(1)
        && minor_version(format_version).is_some_and(|minor| minor >= ORIGINAL_NAME_SINCE_MINOR)
}

/// Whether this build reads `format_version`'s major at all: 1, and 2 (the
/// partition-subset shape only, arm PS-1). The scope of the arms that hold
/// for every document of a known major.
fn known_major(format_version: &str) -> bool {
    matches!(
        major_version(format_version),
        Some(1) | Some(PARTITION_SUBSETS_MAJOR)
    )
}

/// The NEWER of two scorecard versions, by major and then minor (PROD-11.1
/// review: every version step takes the max, so a later version is never
/// downgraded by an earlier step; PROD-11.1b: a 2.0.0 chosen for a subset is
/// never lowered to a 1.x minor). A version this build cannot parse is kept
/// as it is, so a malformed value is never silently replaced.
#[must_use]
pub fn newer_format_version<'a>(a: &'a str, b: &'a str) -> &'a str {
    match (
        major_version(a).zip(minor_version(a)),
        major_version(b).zip(minor_version(b)),
    ) {
        (Some(x), Some(y)) if y > x => b,
        _ => a,
    }
}

/// The `format_version` a scorecard is written with once its plan's replay
/// selection is known (PROD-11.1): [`FORMAT_VERSION_WITH_PARTITION_SUBSETS`]
/// (2.0.0) when its `source.selection` names a partition subset
/// (PROD-11.1b), at least [`FORMAT_VERSION_WITH_SELECTION`] when it carries a
/// start-only block, else `current` unchanged. 1.7.0 defines everything 1.6.0
/// does, and 2.0.0 everything 1.7.0 does. Monotonic: never lowers `current`.
#[must_use]
pub fn format_version_with_selection<'a>(
    current: &'a str,
    selection: Option<&SelectionLabel>,
) -> &'a str {
    match selection {
        Some(s) if s.narrows_partitions() => {
            newer_format_version(current, FORMAT_VERSION_WITH_PARTITION_SUBSETS)
        }
        Some(_) => newer_format_version(current, FORMAT_VERSION_WITH_SELECTION),
        None => current,
    }
}

/// **FX-23 review M2.** Whether a scorecard's `format_version` shows that a
/// build with FX-23's sampled-lane checks signed it: major 1, minor 1.6 or
/// later. Only such a build writes 1.6.0. A 1.4.0 or 1.5.0 document is
/// byte-for-byte the same whichever build signed it, so its sampled `pass`
/// proves only what such a pass always proved: the canary and one count
/// bound over every topic together. Both readers say which
/// (`logweir::verify::sampled_pass_lines`,
/// `docs/verify_scorecard.py::_sampled_pass_lines`).
#[must_use]
pub fn proves_fx23_sampled_checks(format_version: &str) -> bool {
    // PROD-11.1b: a 2.0.0 document is written only by a build with FX-23's
    // checks, which it holds over the selected partitions.
    defines_format_1_minor(format_version, UNSAMPLED_TOPICS_SINCE_MINOR)
}

/// The `format_version` a scorecard is written with once its plan's coverage
/// and its `sample` block are known (FX-23):
/// [`FORMAT_VERSION_WITH_UNSAMPLED_TOPICS`] for a SAMPLED verification — so
/// a sampled `pass` reads with the guarantees only an FX-23 build gives — and
/// for any block naming an unsampled topic; else `current`, the version
/// [`format_version_for_target`] chose, unchanged (a complete verification).
/// 1.6.0 defines everything 1.5.0 does, so a sampled document of a PROD-01.3
/// auth mode is 1.6.0 too. Monotonic: it never lowers `current`.
#[must_use]
pub fn format_version_with_sample<'a>(
    current: &'a str,
    sample: &SampleInfo,
    coverage: crate::spec::Coverage,
) -> &'a str {
    // MONOTONIC (the PROD-11.1 review): at least 1.6.0, never lower than the
    // version handed in, so a later minor a step before this one chose (1.7.0
    // for a stated window start) is never written over.
    if coverage == crate::spec::Coverage::Sampled || sample.unsampled_topics.is_some() {
        newer_format_version(current, FORMAT_VERSION_WITH_UNSAMPLED_TOPICS)
    } else {
        current
    }
}

/// The `format_version` a scorecard is written with, given its
/// `target.auth` block: [`FORMAT_VERSION_WITH_AUTH_MODES`] for a PROD-01.3
/// mode, else [`crate::FORMAT_VERSION`].
#[must_use]
pub fn format_version_for_target(auth: Option<&AuthSummary>) -> &'static str {
    match auth {
        Some(a) if crate::connection::is_prod_01_3_auth_mode(&a.mode) => {
            FORMAT_VERSION_WITH_AUTH_MODES
        }
        _ => crate::FORMAT_VERSION,
    }
}

/// `source.time_basis.plan`'s one value (arm TB-2):
/// `crate::spec::TimeBasis::ProducerTime`'s wire spelling.
pub const TIME_BASIS_PRODUCER_TIME: &str = "producerTime";

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct TargetInfo {
    pub cluster_id: String,
    /// **Which of the two target modes the run was in.** The signed document
    /// carried no discriminator at all until fix round 1 (review F1): the mode
    /// travelled only on `crate::teardown::TeardownAttestation.target_mode`, a
    /// different document under a different media type that a
    /// `Verdict::Block` run (exit 2) never writes, and
    /// `target.topic_mapping_prefix` is not a substitute because
    /// `topic_naming.prefix` is adopter-chosen (`examples/restore.yaml`
    /// suggests `incident-4471-`). Without it a reader cannot tell whether the
    /// three scratch segregation checks ran, which is the whole difference
    /// between the two modes.
    ///
    /// `#[serde(default)]` is `Scratch`, so every document written before this
    /// field existed means exactly what it meant, and
    /// `skip_serializing_if` keeps the scratch case ABSENT ON THE WIRE — the
    /// same rule and the same reason as `auth` below: the three checked-in
    /// SIGNED fixtures are byte-compared against the generator by
    /// `crates/logweir-core/tests/fixture_regen.rs`, they are never re-minted
    /// (ruling R-G), and a field that serialised as `"mode": "scratch"` would
    /// orphan three signatures this round may not replace.
    ///
    /// Global Constraint 12 as amended permits this as a NESTED optional
    /// field: `TargetInfo`'s own properties are not the scorecard's 21, so the
    /// top-level shape is unchanged (21 properties, 17 required).
    #[serde(default, skip_serializing_if = "TargetMode::is_scratch")]
    pub mode: TargetMode,
    /// The v0.1 segregation proof, verified over the logweir-kafka client:
    /// cluster_id ∈ allowedClusterIds AND this topic exists (spec §9.3 phase 0).
    ///
    /// **SCRATCH MODE ONLY, and therefore optional (review F1).** In
    /// `newTopic` mode those are skipped checks 1 and 3 of the mode branch —
    /// neither the allowlist membership nor the topic's existence is verified
    /// — so writing the spec's value here put a field with THAT documented
    /// meaning into a DSSE-signed document about a run that never checked it.
    /// A `newTopic` run now omits the field, and the invariant below refuses
    /// the converse: a `scratch` document that omits it claims a segregation
    /// proof nothing recorded. `None` is therefore not "unknown", it is
    /// "`mode` says this run had no marker topic to verify".
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub marker_topic: Option<String>,
    pub topic_mapping_prefix: String,
    /// sha256 over the rendered restore.yaml topic_mapping block, so an
    /// auditor can re-derive exactly what was written.
    pub topic_mapping_sha256: String,
    pub topic_mapping_entries: u32,
    /// How the TARGET client was told to authenticate. **Interface I1's
    /// scorecard end (Task 5b declares the shape; Task 6 fills it).**
    ///
    /// ABSENT IS LEGAL AND MEANS PLAINTEXT. Every scorecard this tree has ever
    /// written has no `auth` block, and a reader that demanded one would
    /// refuse all of them — which is why the field is `Option` with
    /// `#[serde(default)]` and why `two_reader_parity.rs`'s existing cases are
    /// the test that keeps it tolerant.
    ///
    /// `skip_serializing_if` is NOT decoration. `crates/logweir-core/tests/
    /// fixture_regen.rs::emit_fixture_reproduces_the_committed_scorecard_bytes`
    /// compares the generator's bytes against the checked-in SIGNED fixture
    /// byte for byte, and the signed fixtures are never re-minted here (ruling
    /// R-G). A field that serialised as `"auth": null` would change every
    /// document Logweir writes and orphan a signature this task may not
    /// replace, so the absent case stays absent on the wire.
    ///
    /// Global Constraint 12 as amended permits this as a NESTED optional
    /// field: `TargetInfo`'s own properties are not the scorecard's 21, so the
    /// top-level shape is unchanged (21 properties, 17 required) and
    /// `the_scorecard_top_level_shape_is_unchanged` still holds.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub auth: Option<AuthSummary>,
    /// **PROD-15.1 (format 1.8.0).** Present exactly when this restore wrote
    /// under the source's ORIGINAL topic names: see [`OriginalNameInfo`].
    /// ABSENT on every other document, which is every document before 1.8.0;
    /// nested optional (Global Constraint 12 as amended) and skipped when
    /// absent, so every other document keeps its bytes.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub original_name: Option<OriginalNameInfo>,
}

/// A nested optional block. GC12 as amended permits nested optional fields in
/// tag 1 and no new top-level property: TargetInfo's own properties are not
/// the scorecard's 21.
///
/// **Never a password, and no field that could hold one** — the same rule
/// `crate::backup_receipt::ReceiptAuth` and `crate::engine::AuthRender` state,
/// for the same reason: the secret reaches the engine through its own `${VAR}`
/// expansion and is never interpolated by us, so it can never be interpolated
/// into a document we then sign and publish.
///
/// `mode` is REQUIRED once the block is present, and blank is not a synonym
/// for `plaintext`: `validate_invariants`'s two `target.auth` arms refuse a
/// blank mode outright (ruling R-A's `trim().is_empty()`), because a SCRAM run
/// recorded as plaintext by omission is exactly the claim an auditor would
/// read the wrong way round. The way to say plaintext is to write no block.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct AuthSummary {
    /// **A CLOSED SET, VERSIONED: `"plaintext"` or `"scramSha512"` in every
    /// format, and from 1.5.0 also `"scramSha256"`, `"plain"` or `"mtls"`
    /// (PROD-01.3)** — the values `crate::backup_receipt::ReceiptAuth::mode`
    /// carries, and for the same reason: they are `crate::spec::AuthSpec`'s
    /// serde tag values, the `KafkaCluster` CRD's `auth.mode` enum byte for
    /// byte, and the only strings `AuthSpec::mode_str()` — which is what
    /// `logweir::drill::target_info` fills this field from — can return.
    ///
    /// Task 5b shipped this field with NO documented value set at all, and
    /// its review proved by execution that a scorecard carrying
    /// `target.auth = {"mode": "totally-made-up"}` verified 0/0 at both
    /// readers. `validate_invariants`'s THIRD `target.auth` arm closes it,
    /// mirrored word for word in `docs/verify_scorecard.py`.
    pub mode: String,
    #[serde(default)]
    pub username: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct ApprovalInfo {
    pub approver: String,
    pub ticket: String,
    pub plan_hash: String,
    /// The human's OUT-OF-BAND timestamp. Explicitly NOT an input to any
    /// measured field in v0.1 (spec §9.3 phase 8).
    pub approved_at: DateTime<Utc>,
    pub key_id: String,
    /// true when the approval key equals the signing key. Labelled, never
    /// refused (spec §10).
    pub self_attested: bool,
    /// **PROD-16.2 (format 1.9.0, and 2.1.0 for a partition subset).**
    /// Present exactly when a second person approved this run in the console:
    /// see [`ConsoleApprovalInfo`]. ABSENT on every other document, which is
    /// every document before 1.9.0; arms CA-1 to CA-8 judge it.
    ///
    /// **What the three fields above mean, by mode** (they are one field
    /// each, and what the writer puts there has always depended on the
    /// document that authorised the run):
    ///
    /// | mode | `approver` | `approved_at` | `key_id` |
    /// |---|---|---|---|
    /// | `v1Approval` | the approval document's own `approver` text | its `approved_at`, the human's out-of-band timestamp | the approver's personal key |
    /// | `governed` | `governed approver key <id>` | when the console signed the REQUEST (`issuedAt`): the document records no countersigning time | the approver's personal key |
    /// | `ordinary` | the requester's `<issuer>#<subject>` | when the console signed the confirmation (`issuedAt`) | the console key |
    /// | `consoleApproval` | the approver's `<issuer>#<subject>` | when the second person approved (`approvedAt`), never the request's time | the console key |
    /// | `standing` | the standing authorization's approver | its own | the approver's personal key |
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub console: Option<ConsoleApprovalInfo>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct PhaseRecord {
    pub phase: i8,
    pub name: String,
    pub at: DateTime<Utc>,
    pub outcome: String,
    pub duration_ms: u64,
    /// Free-form evidence attached to this phase record. Two producers today,
    /// and the list is corrected here rather than left stale (Task 20 fix
    /// round 2):
    /// - phase 5 (`crate::drill::phase5_preflight`) carries each `Finding`'s
    ///   tag/detail here so a `preflight-failed` scorecard states *why* the
    ///   drill was refused, not just that it was — without this field the
    ///   adjudication's reasoning had nowhere in the signed document to land.
    ///   Note that phase 5 defines WHAT it contributes; the `PhaseRecord`
    ///   itself is not constructed until the orchestrator lands (Task 21a).
    /// - phase 8 (`crate::drill::phase8_score`) writes onto the phase-8 record
    ///   when the engine-validation prefix is empty, and when it holds more
    ///   than one object (naming the key retained and that others were
    ///   dropped). Both are warnings that must not be silent: an absent or
    ///   ambiguous engine sub-report is a fact about the evidence, not a
    ///   failure.
    ///
    /// `skip_serializing_if` (matching `TargetDiffSummary::absent`, Task 16 fix
    /// round 1) so a document signed before this field existed keeps
    /// round-tripping byte-for-byte when it has nothing to report.
    ///
    /// This reverses `task-3-addendum.md` A2 ("do not add a `notes` field to
    /// `PhaseRecord`"), on controller authority, in Task 17 fix round 1: that
    /// ruling predates `task-21a-brief.md:259`, which writes `p.notes = …`
    /// against this exact struct, so leaving the field out only delayed the
    /// same `E0560` to Task 21a.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub notes: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct Measured {
    #[serde(default)]
    pub rto_seconds: Option<u64>,
    #[serde(default)]
    pub rto_requested_to_verified_seconds: Option<u64>,
    #[serde(default)]
    pub rto_restore_only_seconds: Option<u64>,
    /// THE value compared against objectives.rto_seconds (spec §9.3 phase 8).
    #[serde(default)]
    pub rto_excluding_preflight_seconds: Option<u64>,
    /// ARCHIVE COVERAGE GAP at the requested recovery point — NOT
    /// source-relative data loss.
    ///
    /// `requested_point_in_time - newest_restored_record`, in that order, and
    /// **never negative**: it is how far SHORT of the requested recovery point
    /// the newest restored record falls. A record at or beyond the requested
    /// point means no gap there, which is 0. A negative gap is not a smaller
    /// gap, it is a meaningless one, and a reader meeting `-90` here would
    /// most likely take it for "no data loss". Enforced by
    /// `validate_invariants`, so the rule holds for every writer, not only for
    /// today's single call site in `crate::drill::phase8_score`.
    #[serde(default)]
    pub rpo_seconds: Option<i64>,
    /// Source-relative data loss: how far behind the live source the restored
    /// data is. Null in v0.1 (the source is never contacted); becomes a number
    /// only under `--from-cluster`. **Never negative**, for the same reason as
    /// `rpo_seconds` and enforced by the same `validate_invariants` rule — a
    /// rule deliberately put in place BEFORE the first writer exists, so the
    /// `--from-cluster` implementation inherits it rather than has to invent
    /// it.
    #[serde(default)]
    pub rpo_source_relative_seconds: Option<i64>,
    #[serde(default)]
    pub rpo_source_relative_unmeasured_reason: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct Objectives {
    #[serde(default)]
    pub rto_seconds: Option<u64>,
    /// The REQUESTED maximum archive-coverage gap, copied verbatim from the
    /// adopter's spec. **Never negative** — a negative allowed gap is not a
    /// stricter objective, it is an unsatisfiable one (`measured.rpo_seconds`
    /// is itself non-negative, so `got <= want` could never hold), and a
    /// signed document asserting it would be incoherent. Enforced by
    /// `validate_invariants`; nothing else validates this value, since it
    /// travels straight from YAML into the signed document.
    #[serde(default)]
    pub rpo_seconds: Option<i64>,
    #[serde(default)]
    pub pass_rate: Option<f64>,
    /// The aggregate verdict over whichever objectives above are non-null.
    ///
    /// - `true`  — every non-null objective was met.
    /// - `false` — at least one non-null objective was missed.
    /// - `null`  — a `pass_rate` objective WAS requested but could not be
    ///   measured (`integrity.pass_rate_measured` is null), so the aggregate
    ///   is unmeasurable rather than satisfied.
    ///
    /// The null condition is about the MEASURED rate, not this block's
    /// `pass_rate`. The earlier wording ("null when pass_rate is null") read
    /// the other way round and was wrong in the direction that matters: when
    /// `objectives.pass_rate` is null, no rate was asked for, nothing is
    /// unmeasurable, and `met` is a plain true/false over the remaining
    /// objectives. Decided in exactly one place,
    /// `crate::drill::phase8_score::decide`.
    #[serde(default)]
    pub met: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct SampleInfo {
    pub window_start: DateTime<Utc>,
    pub window_end: DateTime<Utc>,
    pub topics: u32,
    pub partitions: u32,
    /// THE CANARY SIZE: how many records this drill set out to reconcile —
    /// the sum of `sample.records_per_partition` over the partitions actually
    /// selected. `integrity.records_sampled` is measured against THIS figure.
    ///
    /// It is NOT `crate::drill::phase4_sample::Selection::records_expected`
    /// (in `crates/logweir`), which shares the name and answers a different
    /// question: how many records the manifest says the whole sampled window
    /// holds. The two differ by orders of magnitude on any real archive — 25
    /// against 500 on a single partition of this repository's own fixtures —
    /// and substituting one for the other would make a signed document
    /// overstate or understate what was verified. Both definition sites carry
    /// this note deliberately (Task 16's parked item, discharged in Task
    /// 21a); the field is not renamed because a scorecard field name is a
    /// Global Constraint 12 question and the format is frozen at 1.0.0.
    pub records_expected: u64,
    pub records_restored: u64,
    pub anchor: String, // head | tail | random
    pub coverage_note: String,
    /// **FX-23, format 1.6.0.** The restored topics with records in the window
    /// that `sample.max_partitions` left WITHOUT a sampled partition: sorted,
    /// each once, never empty when present. Present exactly when the cap was
    /// below the number of such topics; the cap keeps one partition of every
    /// topic first (round-robin) before a second of any. Phase 7 still holds
    /// every partition of these topics to its count bound, so a topic named
    /// here was COUNTED, not reconciled record by record.
    ///
    /// ABSENT means no topic is recorded as unsampled. A document before
    /// 1.6.0 was written by a build whose cap kept the first partitions in
    /// manifest order and recorded nothing about the rest, so its absence
    /// says nothing. Arms US-1 to US-3 read it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub unsampled_topics: Option<Vec<String>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct Integrity {
    pub level: IntegrityLevel,
    pub result: IntegrityResult,
    #[serde(default)]
    pub partial_reason: Option<String>,
    pub records_sampled: u64,
    pub records_sampled_matching: u64,
    pub mismatches: u64,
    /// MEASURED `records_sampled_matching / records_sampled`. The REQUESTED
    /// rate stays in `objectives.pass_rate`, so an auditor can read both the
    /// ask and the result off one document.
    ///
    /// Null in THREE cases, not one — a `byte-fingerprint` document with a
    /// null rate here is well-formed, not malformed:
    /// 1. `level` is not `byte-fingerprint` (also enforced by
    ///    `validate_invariants`).
    /// 2. Not every selection's record lane reached a conclusion. A ratio over
    ///    part of the sample, published as if it were the whole, is misleading
    ///    even when every figure in it is true.
    /// 3. `records_sampled` is 0 — a zero denominator is withheld rather than
    ///    published as NaN.
    ///
    /// Decided in one place, `crate::drill::phase7_verify::roll_up`.
    #[serde(default)]
    pub pass_rate_measured: Option<f64>,
    /// SP3 only; null, never false, when not attempted. The wire name is
    /// camelCase because spec §6.1 and §14 SP3 both cite the path
    /// `integrity.restoredPrincipalCouldConsume`, and `format_version` freezes
    /// at 1.0.0 in this task — renaming later would be a MAJOR bump.
    #[serde(default, rename = "restoredPrincipalCouldConsume")]
    #[schemars(rename = "restoredPrincipalCouldConsume")]
    pub restored_principal_could_consume: Option<bool>,
    /// **Format 1.4.0 (PROD-08.1).** What this verdict covered, structured:
    /// see [`Verification`].
    ///
    /// ABSENT means NOT RECORDED — every document before 1.4.0, and one whose
    /// phase 7 never ran — and is read as SAMPLED, never as complete: before
    /// PROD-08.1 every drill sampled. Every 1.4.0 run that reaches phase 7
    /// writes it. A nested optional field, which Global Constraint 12 as
    /// amended permits; `skip_serializing_if`, so every earlier document
    /// round-trips byte for byte.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub verification: Option<Verification>,
}

/// **PROD-08.1, scorecard format 1.4.0.** What phase 7's verdict covered,
/// with the four kinds of evidence a restore can carry kept apart:
///
/// | evidence | where it is |
/// |---|---|
/// | the authenticated report | the DSSE envelope over these bytes (and, for a point-bound plan, the verified receipt) — not in this block |
/// | archive integrity | `complete.archive` (complete coverage); `evidence`/`integrity` counters for a sampled run |
/// | replay comparison | `complete.replay` and `complete.partitions` (complete coverage); `integrity.records_sampled*` for a sampled run |
/// | application validation | `application`: `notAttempted` in this build |
///
/// The full contract — the expected-output model, what each count means, and
/// what filters, partition subsets, compaction and transformations do to it —
/// is `docs/to-do/decisions/PROD-08.1-integrity-contract.md`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct Verification {
    /// `sampled` or `complete` ([`COVERAGE_SAMPLED`], [`COVERAGE_COMPLETE`];
    /// arm IV-2): the plan's `sample.coverage`, as run.
    pub coverage: String,
    /// What the restored records were compared WITH: `archive` in this build
    /// ([`COMPARISON_BASIS_ARCHIVE`]). Never the source: a loss that happened
    /// when the archive was written is in the archive and in the target, and
    /// this comparison cannot see it (PROD-01.1 V3).
    pub comparison_basis: String,
    /// Whether the comparison held each record's headers to their recorded
    /// ORDER: `verified` (complete coverage, which compares
    /// `logweir_kafka::fingerprint::record_digest_ordered`) or `notVerified`
    /// (sampled coverage, whose fingerprint sorts headers). Arm IV-3:
    /// `verified` only with `coverage: complete`.
    pub header_order: String,
    /// Application-level validation of the restored data:
    /// `notAttempted` in this build ([`APPLICATION_NOT_ATTEMPTED`]; PROD-06.2
    /// is where it is attempted).
    pub application: String,
    /// The capture gaps the manifest records for the partitions this run
    /// verified: source offset ranges the backup could NOT capture. Sorted by
    /// topic, partition and offset. Structured and signed; the free-text
    /// `sample.coverage_note` still carries the sampled-window subset.
    pub gaps: Vec<OffsetRange>,
    /// The ranges retention deliberately removed from the archive, for the
    /// same partitions, the same shape.
    pub pruned: Vec<OffsetRange>,
    /// Present exactly when `coverage` is `complete` (arm IV-4).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub complete: Option<CompleteVerification>,
}

/// One inclusive source offset range of one partition, as the manifest
/// records it.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, JsonSchema)]
pub struct OffsetRange {
    pub topic: String,
    pub partition: i32,
    pub from_offset: i64,
    pub to_offset: i64,
}

/// **PROD-08.1.** A complete verification's result: every archived segment
/// of every restored partition read, hashed and decoded; the expected output
/// computed from each record's own timestamp; every restored record compared
/// with it by `x-original-offset`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct CompleteVerification {
    /// `true` when every partition was compared; `false` when the bound
    /// stopped the verification or a partition could not be compared (an
    /// archive whose records carry no `x-original-offset`, a segment that
    /// could not be decoded). Arms IV-5 and IV-6: `false` names its reason
    /// and is never a pass.
    pub covered: bool,
    /// Why `covered` is `false`; absent when it is `true` (arm IV-5).
    #[serde(default)]
    pub incomplete_reason: Option<String>,
    /// The plan's `sample.complete_max_records`, the bound in force; absent
    /// when the plan set none.
    #[serde(default)]
    pub max_records: Option<u64>,
    /// The window the expected output was selected by, on each archived
    /// record's OWN timestamp: see [`CompleteWindow`].
    pub window: CompleteWindow,
    /// Archive integrity, over every segment of every restored partition.
    pub archive: ArchiveIntegrity,
    /// The replay comparison, summed over every compared partition (arm
    /// IV-7: the sums of `partitions[].replay`).
    pub replay: ReplayComparison,
    /// One entry per partition of every restored topic, sorted by topic and
    /// partition.
    pub partitions: Vec<PartitionVerification>,
}

/// The selection the expected output is computed with. A record is expected
/// when its own timestamp is at or before `end_ms` (inclusive, as the
/// engine's restore filter is) and, when `start_ms` is present, at or after
/// it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct CompleteWindow {
    /// ABSENT means NO LOWER BOUND: the plan's window starts at the archive
    /// (guard G-WIN, `ArchiveManifest`), so every archived record at or
    /// before `end_ms` is expected — including one older than every
    /// segment's FIRST record, which the engine's window floor drops
    /// (PROD-01.1 ts-floor, acceptance row 08-3).
    #[serde(default)]
    pub start_ms: Option<i64>,
    /// The restore window's end: the plan's point in time, or its sample
    /// window end when it states none. Inclusive.
    pub end_ms: i64,
}

/// Archive integrity over every segment of every restored partition.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct ArchiveIntegrity {
    /// Segments the manifest lists for the restored partitions.
    pub segments: u64,
    /// Segments read back whose sha256 matched the manifest, whose records
    /// decoded, and whose decoded count and offsets agreed with the manifest.
    pub segments_verified: u64,
    /// Segment keys examined and found wrong: a sha256 mismatch, an object
    /// the store does not hold, or a decoded count or offset range that
    /// disagrees with the manifest.
    pub segments_failed: Vec<String>,
    /// Segment keys that could not be examined: no sha256 (written before
    /// 0.21), a format the decoder does not read, or past the bound.
    pub segments_unverified: Vec<String>,
    /// Archived records decoded, inside the window or not.
    pub records_decoded: u64,
    /// Source offsets inside the decoded span that no archived record holds
    /// and no recorded gap or pruned range explains — a compacted source's
    /// holes. Disclosed, never a fault: the archive holds what the source
    /// held when it was read (PROD-01.1 C7).
    pub offset_holes: u64,
}

/// The replay comparison of one partition, or the sums over all of them.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct ReplayComparison {
    /// Archived records the window selects: the expected output.
    pub expected: u64,
    /// Records the target partition holds.
    pub restored: u64,
    /// Expected records whose restored copy (the first one, by
    /// `x-original-offset`) is byte-identical: key, value, timestamp and the
    /// headers in order.
    pub matching: u64,
    /// Expected records with no restored copy.
    pub missing: u64,
    /// Restored records that are no expected record: an `x-original-offset`
    /// the expected output does not hold, or none at all.
    pub unexpected: u64,
    /// Restored records that repeat an `x-original-offset` already seen.
    pub duplicates: u64,
    /// Restored records whose `x-original-offset` is below one seen before
    /// them (archive order is source-offset order).
    pub out_of_order: u64,
    /// Expected records whose first restored copy differs from the archive.
    pub mismatched: u64,
}

/// One partition of a complete verification.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct PartitionVerification {
    /// The archive-side (source) topic.
    pub topic: String,
    pub partition: i32,
    /// The restored topic it was compared with.
    pub target_topic: String,
    /// `false` when this partition was not compared — past the bound, or its
    /// expected output could not be established; `findings` says why.
    pub compared: bool,
    /// Segments the manifest lists for this partition, and how many verified.
    pub segments: u64,
    pub segments_verified: u64,
    pub records_decoded: u64,
    pub offset_holes: u64,
    pub replay: ReplayComparison,
    /// The first findings, in words: which offsets are missing, duplicated,
    /// out of order or different, and why a partition was not compared. At
    /// most [`PARTITION_FINDINGS_CAP`] entries plus one that says how many
    /// were left out; the counts above are complete.
    pub findings: Vec<String>,
}

/// The first minor of scorecard format 1 that defines
/// `integrity.verification` (PROD-08.1), which arm IV-1 enforces. **A
/// renumber changes this and [`crate::FORMAT_VERSION`] together**; IV-1's
/// message is built from it, `docs/verify_scorecard.py`'s
/// `SCORECARD_VERIFICATION_SINCE_MINOR` must equal it.
pub const VERIFICATION_SINCE_MINOR: u64 = 4;

/// `integrity.verification.coverage` for a sampled verification.
pub const COVERAGE_SAMPLED: &str = "sampled";
/// `integrity.verification.coverage` for a complete verification.
pub const COVERAGE_COMPLETE: &str = "complete";
/// `integrity.verification.comparison_basis`: the archive.
pub const COMPARISON_BASIS_ARCHIVE: &str = "archive";
/// `integrity.verification.header_order` when the comparison held headers to
/// their order.
pub const HEADER_ORDER_VERIFIED: &str = "verified";
/// `integrity.verification.header_order` when it did not.
pub const HEADER_ORDER_NOT_VERIFIED: &str = "notVerified";
/// `integrity.verification.application` in this build.
pub const APPLICATION_NOT_ATTEMPTED: &str = "notAttempted";
/// How many findings one partition carries in words before one more entry
/// says how many were left out.
pub const PARTITION_FINDINGS_CAP: usize = 20;

impl ReplayComparison {
    /// No fault of any kind: every expected record restored once, in order,
    /// unchanged, and nothing else restored.
    #[must_use]
    pub fn is_exact(&self) -> bool {
        self.missing == 0
            && self.unexpected == 0
            && self.duplicates == 0
            && self.out_of_order == 0
            && self.mismatched == 0
            && self.matching == self.expected
            && self.restored == self.expected
    }

    /// Adds `other` into `self`, saturating.
    pub fn add(&mut self, other: &ReplayComparison) {
        self.expected = self.expected.saturating_add(other.expected);
        self.restored = self.restored.saturating_add(other.restored);
        self.matching = self.matching.saturating_add(other.matching);
        self.missing = self.missing.saturating_add(other.missing);
        self.unexpected = self.unexpected.saturating_add(other.unexpected);
        self.duplicates = self.duplicates.saturating_add(other.duplicates);
        self.out_of_order = self.out_of_order.saturating_add(other.out_of_order);
        self.mismatched = self.mismatched.saturating_add(other.mismatched);
    }
}

/// The published sink for phase 3 — the phase no shipped artifact performs.
/// Without this block the diff would be computed and discarded, and the §4
/// positioning claim would rest on a value that reaches no reader.
#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
pub struct TargetDiffSummary {
    /// Mapped target topics that already EXIST on the target — regardless of
    /// whether they currently hold any records. Existence alone is what
    /// makes writing into them a collision worth flagging (Task 16 fix
    /// round 1, MINOR-2): an existing-but-empty topic can still carry
    /// configuration that differs from what the restore expects, and
    /// creating a topic fresh is a different operational fact from reusing
    /// one someone else already created — even an empty one. The prior
    /// wording here ("already existed AND already held records") described
    /// a narrower rule than the code implements; the code is right (refusing
    /// to write into an existing empty topic costs at most a false alarm,
    /// while the narrower rule would let a restore silently write into a
    /// topic someone else created), so this text was corrected to match it.
    #[serde(default)]
    pub collisions: Vec<String>,
    /// Mapped target topics that do NOT exist on the target — the normal
    /// case on a scratch cluster. Every entry here has a corresponding
    /// `would_create` entry at the partition count the restore will build.
    /// `skip_serializing_if` (unlike its siblings here) so a document
    /// signed before this field existed keeps round-tripping byte-for-byte
    /// when it has nothing to report — added in Task 16 fix round 1, after
    /// `TargetDiff.absent` was found computed and never read.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub absent: Vec<String>,
    /// (mapped target topic, partition count the restore created).
    #[serde(default)]
    pub would_create: Vec<(String, i32)>,
    /// "full" in v0.1. Becomes "shallow" only if spec §15 cut 0d is ever taken.
    pub level: String,
    /// **Format 1.1.0 (FX-4).** The `collisions` whose CONFIGURATION
    /// difference was not assessed, each as `"<target topic>:
    /// configuration (<why>)"`, the shape of `topic_parity.not_assessed`.
    /// `<why>` is the SOURCE topic's capture coverage from the verified
    /// backup receipt: `unknown` (the restore was bound to no receipt, or to
    /// one that predates 1.1.0), `notCaptured` or `captureDenied`.
    ///
    /// For a collision listed here, its `differing config: […]` names every
    /// difference the archive's OWN record shows, but an empty list proves
    /// nothing: a denied DescribeConfigs at capture leaves that record empty.
    /// The collision strings themselves stay byte for byte what a writer
    /// before FX-4 produced: a qualifier inside them would change an
    /// existing field's content, which `docs/stability.md` calls MAJOR and
    /// the owner's OD-7 rulings of 2026-10-05 did not make MINOR. This new
    /// optional field carries it instead.
    ///
    /// ABSENT means NOT RECORDED — every 1.0.0 document, and a document whose
    /// phase 3 never ran. `Some([])` is the claim that every collision's
    /// configuration difference was assessed (vacuously, when there is no
    /// collision); only phase 3 writes it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub not_assessed: Option<Vec<String>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct TopicParity {
    pub intentionally_deviated: Vec<String>,
    pub unexpected_divergence: Vec<String>,
    /// **Format 1.1.0 (FX-4).** What was not assessed per mapped target
    /// topic, each as `"<target topic>: <what> (<why>)"`.
    ///
    /// - `configuration` (FX-4): the topic's CONFIGURATION parity. `<why>` is
    ///   the SOURCE topic's capture coverage from the verified backup receipt
    ///   — `unknown` (the restore was bound to no receipt, or to one that
    ///   predates 1.1.0), `notCaptured` or `captureDenied` — or
    ///   `targetReadDenied` when the source was captured but the TARGET
    ///   topic's configuration could not be read (its keys are then not
    ///   compared at all). For such a topic, the two lists above still name
    ///   every configuration difference the archive's OWN record shows, but
    ///   their silence proves nothing: a denied DescribeConfigs at capture
    ///   leaves the manifest's configuration empty, which compares as "no
    ///   divergence".
    /// - `replication_factor` or `partition_count` (FX-21), `<why>` always
    ///   `notRecorded`: the source's value is recorded nowhere phase 7 reads
    ///   it (the manifest; for the factor, also the bound receipt's 1.3.0
    ///   `topic_configuration`), so it was not compared. Engine 0.23.3
    ///   records the factor for the first topic a backup saves only. Partition
    ///   count and replication factor do not depend on the configuration
    ///   capture and are compared wherever the source's value is recorded. A
    ///   writer before FX-21 compared the target's own value with itself
    ///   instead, so its silence about such a topic is not parity.
    ///
    /// Each entry has a fail-safe twin `"<target topic>: <what> not assessed
    /// (<why>)"` in `unexpected_divergence`.
    ///
    /// ABSENT means NOT RECORDED — every 1.0.0 document, and a document whose
    /// phase 7 never ran — and is never read as "every topic assessed";
    /// `Some([])` is that claim, and only phase 7 writes it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub not_assessed: Option<Vec<String>>,
    /// **Format 1.2.0 (FX-3).** The source settings a `newTopic` restore did
    /// NOT reconstruct on the target, each as `"<target topic>: <key>"`, the
    /// shape of the two lists above. `<key>` is one of the four the restore's
    /// own topic creation decides instead of copying from the source:
    /// `cleanup.policy` (left to the target broker's default), `retention.ms`
    /// (`-1`), `partition_count` and `replication_factor` (the plan's). A
    /// `scratch` drill reports the same four as `intentionally_deviated`,
    /// because a scratch cluster runs `cleanup.policy=delete`, infinite
    /// retention and one broker on purpose; a `newTopic` restore is the
    /// recovery itself, so before 1.2.0 that label signed lost compaction and
    /// replication factor 1 as "intended".
    ///
    /// **Every entry is ALSO in `unexpected_divergence`, and never in
    /// `intentionally_deviated`** (arms NR-2 and NR-3 of `validate_invariants`,
    /// in both readers): a reader that predates this field sees each one as an
    /// unexpected divergence — a weaker conclusion than the label it replaces,
    /// never a stronger one, and never silence that reads as parity. In a
    /// `newTopic` document carrying the field, `intentionally_deviated` is
    /// empty (NR-4) and every `unexpected_divergence` entry on one of the
    /// [`RESTORE_DECIDED_SETTINGS`] is also here (NR-5), so a writer that lost
    /// the mode cannot sign `[]` beside the scratch labels.
    ///
    /// ABSENT means NOT RECORDED: every document before 1.2.0, and one whose
    /// phase 7 never ran. In a `newTopic` document before 1.2.0 the writer
    /// applied the scratch rationale, so every `intentionally_deviated` entry
    /// there is a source setting that was NOT reconstructed, whatever its
    /// label; both readers print that. `Some([])` is the claim that no
    /// setting was left unreconstructed — every `scratch` run, and a
    /// `newTopic` run whose compared settings all matched — and only phase 7
    /// writes it, from 1.2.0 on (arm NR-1).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub not_reconstructed: Option<Vec<String>>,
}

/// The first minor of scorecard format 1 that defines
/// `topic_parity.not_reconstructed` (FX-3), which arm NR-1 of
/// `validate_invariants` enforces. **A renumber changes this and
/// [`crate::FORMAT_VERSION`] together**; NR-1's message is built from it,
/// `docs/verify_scorecard.py`'s `TOPIC_PARITY_NOT_RECONSTRUCTED_SINCE_MINOR`
/// must equal it (`docs/test_verify_scorecard.py::
/// test_the_not_reconstructed_minor_is_the_rust_readers`), and
/// `the_written_version_defines_not_reconstructed` keeps the pair coherent.
pub const NOT_RECONSTRUCTED_SINCE_MINOR: u64 = 2;

/// The four settings a restore's own topic creation DECIDES instead of copying
/// from the source (FX-3): `cleanup.policy` (left to the target broker),
/// `retention.ms` (`-1`), and the partition count and replication factor (the
/// manifest's and the plan's). They are the only `<key>`s phase 7 can write
/// into `intentionally_deviated` (a scratch drill) or `not_reconstructed` (a
/// `newTopic` restore). Arm NR-5 reads it; phase 7's
/// `classify_parity_decides_exactly_the_core_settings` test and
/// `docs/verify_scorecard.py`'s `RESTORE_DECIDED_SETTINGS` (pinned by
/// `test_the_restore_decided_settings_are_the_rust_readers`) must equal it.
pub const RESTORE_DECIDED_SETTINGS: [&str; 4] = [
    "cleanup.policy",
    "partition_count",
    "replication_factor",
    "retention.ms",
];

/// The `<key>` of a `topic_parity` entry `"<target topic>: <key>"`: the text
/// after the LAST `": "`, or the whole entry when it has none (a Kafka topic
/// name cannot contain `": "`). Read by arm NR-5 only;
/// `docs/verify_scorecard.py::_parity_key` is its mirror.
fn parity_key(entry: &str) -> &str {
    entry.rsplit_once(": ").map_or(entry, |(_, key)| key)
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct EngineSubreport {
    pub retained_verbatim: bool,
    pub retrieved_from: String,
    pub caveat: String,
    /// The engine's evidence report as the EXACT bytes read from the bucket,
    /// base64 (RFC 4648 standard alphabet, padded).
    ///
    /// NOT a parsed `serde_json::Value`. OSO's envelope "covers the exact,
    /// complete stored report bytes. No canonicalization is performed at
    /// verification time" and "no JSON parsing, re-serialization, or
    /// canonicalization influences the digest or signature check"
    /// [VERIFIED U/kafka-backup/crates/kafka-backup-core/src/evidence/envelope.rs:5,254].
    /// Re-emitting a parsed value through `to_deterministic_json`'s two-space
    /// `PrettyFormatter` would change whitespace, escaping and number
    /// formatting, so the digest OSO signed would no longer match and the SP1c
    /// exit criterion ("the embedded engine sub-report round-trips through
    /// OSO's own `validation evidence-verify`") could never be ticked.
    pub body_b64: String,
    /// `sha256:<hex>` of the DECODED bytes, so an auditor can re-check the
    /// binding without base64-decoding anything.
    pub body_sha256: String,
}

/// WHAT THIS BLOCK IS NOT: it is not evidence about the upload, and it is not
/// a statement about the storage backend's capabilities.
///
/// All four fields describe the upload of THIS scorecard — an event that has
/// not happened when the scorecard is signed, and cannot happen before it,
/// because a signature covers bytes and the bytes must exist first. The
/// scorecard is never re-serialised afterwards (that would invalidate the
/// signature). So `crate::drill::phase8_score::run` ZEROES all four
/// immediately before serialising, whatever the caller supplied, and every
/// scorecard Logweir emits in v0.1 carries
/// `{version_id: null, retain_until: null, immutable: false,
/// create_only_enforced: false}`.
///
/// Read that as **"no proof was obtainable at signing time"** — never as a
/// finding about the object or the backend. The block deliberately
/// UNDER-claims; what it buys is the guarantee that a valid Logweir signature
/// can never cover an unsubstantiated WORM or create-only assertion.
///
/// The real post-upload readback IS published — in a second, separately signed
/// document written after the put: `logweir/drills/<run_id>.receipt.json` plus
/// its `.receipt.sig` sidecar (`crate::drill::phase8_score::PutReceipt`, Task
/// 21a, discharging Task 20's carried obligation). It carries the observed
/// `create_only_enforced`, `version_id`, `immutable` and `retain_until`, bound
/// to the scorecard by the sha256 of the exact SIGNED BYTES. Read the two
/// documents together: the scorecard is the measurement and under-claims about
/// storage; the receipt is the storage evidence. `docs/stability.md` carries
/// the adopter-facing version of this note.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct EvidenceInfo {
    /// Always null in v0.1: the store's version id for this object is only
    /// known after the put, and the put follows the signature. NOT a claim
    /// that the bucket is unversioned.
    #[serde(default)]
    pub version_id: Option<String>,
    /// Always null in v0.1: a retention date can only come from a provider
    /// readback performed after the put. NOT a claim that no retention
    /// applies.
    #[serde(default)]
    pub retain_until: Option<DateTime<Utc>>,
    /// Spec §6 C3 permits `true` ONLY after a provider readback. Always false
    /// in v0.1 — the readback happens after signing, and `object_store` 0.14
    /// exposes no Object Lock / WORM API on any backend it can build, so there
    /// is nothing to read back. `false` means "not proven here"; it is NOT a
    /// claim that the object is mutable or unprotected.
    pub immutable: bool,
    /// Always false in v0.1, on EVERY backend, including those whose
    /// conditional put was in fact used: whether the put was conditional is
    /// only known after it happens, and this document is signed first. `false`
    /// is NOT evidence that the backend lacks conditional put (spec §11) and
    /// NOT evidence that the object could have been overwritten — the two
    /// cases are indistinguishable in the signed document by design.
    pub create_only_enforced: bool,
    /// The object key of the ENGINE's offset-mapping report for this run,
    /// `logweir/drills/<run_id>.offsets.json` (Task 9b, spec §6.1 N9).
    ///
    /// **A NESTED OPTIONAL FIELD, which is the only kind Global Constraint 12
    /// as amended permits.** The scorecard stays at its frozen 21 top-level
    /// properties and 17 required ones; this is the third such field in tag 1
    /// (after `target.auth`) and it pays GC12's full price in the same commit:
    /// both readers, a `SCRIPT_VERSION` bump, a corpus case with closed
    /// arithmetic, and the regenerated schema.
    ///
    /// `skip_serializing_if` is load-bearing, not tidiness: without it every
    /// document would gain a `"offset_report_key": null` line and the three
    /// checked-in signed fixtures under `e2e/fixtures/signed/` would stop
    /// verifying against their own sidecars. Absent means this run recorded no
    /// report — see `crate::drill::phase8_score::Signed::offset_report_key` in
    /// the `logweir` crate for the paths on which that is the truthful answer.
    ///
    /// # Why the report is uploaded at all
    ///
    /// The engine writes it to a POD-LOCAL path
    /// [U:crates/kafka-backup-core/src/config.rs:857-858] and the pod is then
    /// deleted, so without this upload the one artefact describing what the
    /// restore's offsets map to would exist only for the lifetime of a
    /// container. It is evidence, not an instruction: Global Constraint 20
    /// makes no offset commit anywhere in tag 1 and constraint 35 says tag 1
    /// renders the report and applies nothing.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub offset_report_key: Option<String>,
    /// `sha256:<hex>` of the EXACT offset-report bytes uploaded at
    /// `offset_report_key`, so a reader can bind the object to this signed
    /// document rather than trusting that the key still holds what the run
    /// wrote.
    ///
    /// **Present exactly when `offset_report_key` is.** The pair is checked by
    /// `validate_invariants` in both readers: a key with no digest names bytes
    /// nothing binds, and a digest with no key binds bytes nobody can fetch.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub offset_report_sha256: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct Redaction {
    /// A JSON pointer into THIS scorecard — never a missing field.
    pub path: String,
    pub reason: String,
    pub present: bool,
}

#[derive(Debug, thiserror::Error)]
#[error("scorecard invariant violated: {0}")]
pub struct InvariantError(pub String);

/// The leading dot-separated component of a semver string, parsed as an
/// integer. Returns `None` for anything that doesn't start with an integer
/// (so a malformed `format_version` is refused rather than silently treated
/// as major 0).
fn major_version(v: &str) -> Option<u64> {
    v.split('.').next()?.parse().ok()
}

/// The second dot-separated component of a semver string, parsed as an
/// integer; `None` when there is none or it does not parse. Read by the arms
/// that ask whether a document's version defines a field added in a minor
/// (FX-3's NR-1, FX-8's TB-1).
fn minor_version(v: &str) -> Option<u64> {
    v.split('.').nth(1)?.parse().ok()
}

/// What arm CA-3 says after its fixed opening, for one fault: the people it
/// is about and the fault's fixed clause
/// (`crate::approval_policy::SeparationFault::clause`). No part of it is the
/// document's. `docs/verify_scorecard.py::_console_separation_words` returns
/// the same words.
#[must_use]
pub fn console_separation_words(fault: crate::approval_policy::SeparationFault) -> String {
    use crate::approval_policy::SeparationFault;
    let who = match fault {
        SeparationFault::NotComparable(party)
        | SeparationFault::LocalAdmin(party)
        | SeparationFault::SystemIdentity(party) => format!("the {}", party.as_str()),
        SeparationFault::TwoIssuers => "the approver and the requester".to_string(),
        SeparationFault::SamePerson => "the approver".to_string(),
    };
    format!("{who} {}", fault.clause())
}

/// Arm CA-3's whole message for one fault, as `validate_invariants` returns
/// it (its opening is a literal in that function's body, where the corpus
/// accounting reads it).
#[must_use]
pub fn console_separation_message(fault: crate::approval_policy::SeparationFault) -> String {
    format!(
        "approval.console does not name two people: {}",
        console_separation_words(fault)
    )
}

impl Scorecard {
    /// GLOBAL CONSTRAINT 12, IN ONE PLACE: a reader must refuse a
    /// `format_version` whose major is newer than the one this binary
    /// understands, and must ignore unknown fields.
    ///
    /// Public, and separate from `validate_invariants`, because Logweir ships
    /// THREE readers and the rule belongs to all of them. It used to be an
    /// inlined first block of `validate_invariants`, which meant `drill
    /// verify` honoured it (it calls that function) and `drill show` did not
    /// (it never did) — a `format_version: "2.0.0"` document rendered its full
    /// table, header reading `v2.0.0`, `outcome Pass`, and exited 0. A rule the
    /// README states and the project keeps in one place out of three is not a
    /// rule. `docs/verify_scorecard.py` is the third reader and implements the
    /// same comparison against its own `FORMAT_VERSION` constant.
    ///
    /// The comparison is against [`FORMAT_VERSION_WITH_PARTITION_SUBSETS`]'s
    /// major ([`PARTITION_SUBSETS_MAJOR`]), never re-derived some other way.
    ///
    /// **Major 2 is read for ONE shape** (PROD-11.1b, the owner's OD-9 (a)):
    /// a partition-subset restore's document, which carries
    /// `source.selection.partitions`. A major-2 document without it is
    /// refused here (arm PS-1), before any other rule reads it, so a reader
    /// never applies 1.x meanings to a 2.x document whose shape it does not
    /// know. `docs/verify_scorecard.py` makes the same two refusals, in this
    /// order, with the same words.
    pub fn refuse_unreadable_major(&self) -> Result<(), InvariantError> {
        let doc_major = major_version(&self.format_version).ok_or_else(|| {
            InvariantError(format!(
                "format_version {:?} is not a parseable semver",
                self.format_version
            ))
        })?;
        if doc_major > PARTITION_SUBSETS_MAJOR {
            return Err(InvariantError(format!(
                "format_version {} has a major version newer than this reader understands \
                 (this build knows {})",
                self.format_version, FORMAT_VERSION_WITH_PARTITION_SUBSETS
            )));
        }
        // PS-1. Major 2 is the partition-subset format and nothing else.
        if doc_major == PARTITION_SUBSETS_MAJOR
            && !self
                .source
                .selection
                .as_ref()
                .is_some_and(SelectionLabel::narrows_partitions)
        {
            return Err(InvariantError(format!(
                "format_version {} is the format of a partition-subset restore, and this document \
                 carries no source.selection.partitions; a reader reads major \
                 {PARTITION_SUBSETS_MAJOR} only for that shape",
                self.format_version
            )));
        }
        Ok(())
    }

    /// The invariants spec §6.1 and §9.3 phase 8 state in prose. Called before
    /// signing (Task 20) and by `drill verify` (Task 6), so no signed document
    /// can carry a self-contradicting claim.
    pub fn validate_invariants(&self) -> Result<(), InvariantError> {
        // Checked FIRST, so a document from a future major bump is rejected
        // before any other invariant is evaluated against fields that build
        // may have changed the meaning of.
        self.refuse_unreadable_major()?;
        // T0-2: the four post-put fields are zeroed BEFORE signing
        // (`crate::drill::phase8_score` zeroes them unconditionally), because
        // they describe an upload that has not happened yet. This is a
        // RETROACTIVE TIGHTENING of the 1.0.0 reader, not a format change: no
        // byte of the format changes, the accepted set narrows, and because the
        // writer's zeroing is unconditional, no document Logweir has ever
        // written is refused. GC12 holds — format_version stays 1.0.0.
        // Scoped to major 1 so a future major may redefine the block.
        //
        // Field order is the struct's own declaration order, and
        // `docs/verify_scorecard.py::check_invariants` mirrors this arm in the
        // same position with the same order and the same words, so a document
        // violating two fields gets the SAME message from both readers.
        // PROD-11.1b: and major 2, which is 1.7.0's fields plus the subset
        // meaning, so every arm of major 1 holds for it.
        if known_major(&self.format_version) {
            if self.evidence.version_id.is_some() {
                return Err(InvariantError(
                    "evidence.version_id is set but the four post-put fields are zeroed before signing".into(),
                ));
            }
            if self.evidence.retain_until.is_some() {
                return Err(InvariantError(
                    "evidence.retain_until is set but the four post-put fields are zeroed before signing".into(),
                ));
            }
            if self.evidence.immutable {
                return Err(InvariantError(
                    "evidence.immutable is true but the four post-put fields are zeroed before signing".into(),
                ));
            }
            if self.evidence.create_only_enforced {
                return Err(InvariantError(
                    "evidence.create_only_enforced is true but the four post-put fields are zeroed before signing".into(),
                ));
            }
            // `evidence.offset_report_*` (Task 9b — Global Constraint 12's
            // price for two nested optional fields). ABSENT IS LEGAL and means
            // this run recorded no offset-mapping report; the arm fires only
            // on a document carrying ONE of the pair.
            //
            // BOTH DIRECTIONS, one statement, because the incoherence is
            // symmetric: a key with no digest names bytes nothing binds, and a
            // digest with no key binds bytes nobody can fetch. Neither is a
            // document Logweir can write — `phase8_score::run` sets both from
            // one `Option` — so this arm exists for the documents this tree
            // did not write, which is every document a reader is handed.
            //
            // BLANK COUNTS AS ABSENT (ruling R-A: `trim().is_empty()` here,
            // `.strip()` in `docs/verify_scorecard.py`). Without it the two
            // readers would disagree on `""`: Rust's `is_some()` is true for
            // `Some("")` while Python's truthiness test is false, which is
            // exactly the class of split the parity gate exists to catch.
            //
            // Scoped to major 1 like the four arms above, so a future major
            // may redefine the block. NOT INTERPOLATED: the message is joined
            // to `index.json`'s `arm` field by literal substring
            // (`crates/logweir/tests/two_reader_parity.rs::
            // every_invariant_arm_has_a_corpus_case`), and a key in a refusal
            // line would be an adopter-influenced string in a refusal line.
            let offset_key_named = self
                .evidence
                .offset_report_key
                .as_deref()
                .is_some_and(|k| !k.trim().is_empty());
            let offset_digest_named = self
                .evidence
                .offset_report_sha256
                .as_deref()
                .is_some_and(|d| !d.trim().is_empty());
            if offset_key_named != offset_digest_named {
                return Err(InvariantError(
                    "evidence.offset_report_key and evidence.offset_report_sha256 are present or absent together; a key with no digest names bytes nothing binds, and a digest with no key binds bytes nobody can fetch"
                        .into(),
                ));
            }
            // `target.marker_topic` is the SCRATCH SEGREGATION PROOF and
            // nothing else (review F1, fix round 1). Its own doc comment says
            // what carrying it means — `cluster_id ∈ allowedClusterIds` AND
            // this topic exists, both verified at phase 0 — and in `newTopic`
            // mode neither is verified, because they are skipped checks 1 and
            // 3 of the mode branch. So the field is optional and `mode` is
            // what says which run this was.
            //
            // ARMED IN ONE DIRECTION ONLY, and deliberately. An ABSENT marker
            // requires `mode: newTopic`: a scratch document with no marker
            // claims the proof phase 0 makes while omitting the thing the
            // proof was made about. The other direction is NOT an invariant —
            // a document may carry both `newTopic` and a marker topic — and
            // the reason is that a reader must not refuse a document a
            // FUTURE writer could legitimately produce (`TargetSpec::marker_topic`
            // is a required spec field that a `newTopic` spec still carries,
            // so recording it as an unverified echo is a coherent choice).
            // What THIS tree writes is narrower than what its readers accept,
            // which is the standing rule for every optional field in this
            // format, and the narrower claim is a WRITER-side test:
            // `crates/logweir/tests/restore_mode.rs::
            // a_new_topic_scorecard_carries_the_mode_and_no_marker_topic`.
            //
            // BLANK COUNTS AS ABSENT (ruling R-A): `trim().is_empty()` here,
            // `.strip()` in `docs/verify_scorecard.py`, so the two readers
            // cannot split on `""` the way T0-6 found them splitting.
            //
            // Scoped to major 1 like every arm above. NOT INTERPOLATED: the
            // message is joined to `index.json`'s `arm` field by literal
            // substring.
            let marker_named = self
                .target
                .marker_topic
                .as_deref()
                .is_some_and(|t| !t.trim().is_empty());
            if !marker_named && self.target.mode.is_scratch() {
                return Err(InvariantError(
                    "target.marker_topic is absent but target.mode is scratch; the marker topic is the segregation proof phase 0 verified, and a scratch document that omits it claims a check nothing recorded"
                        .into(),
                ));
            }
        }
        // Ruling R-A / T0-6: `.is_none()` accepted `""` and `"   "`, which
        // `docs/verify_scorecard.py`'s truthiness test refuses — the two
        // readers disagreed in the very file whose docstring claims (`ARM FOR
        // ARM, IN ORDER`) that they cannot. Trimmed-empty is the strict side:
        // it narrows the accepted set and changes no byte of the format, so it
        // is a RETROACTIVE TIGHTENING of the 1.0.0 reader, not a format change
        // (GC12). No document Logweir has written carries a blank reason —
        // `crate::drill::phase7_verify` builds it as `(!notes.is_empty())
        // .then(|| notes.join("; "))`, so it is either absent or says
        // something.
        //
        // The message is byte-identical to `docs/verify_scorecard.py`'s and is
        // not to be reworded: `crates/logweir/tests/two_reader_parity.rs`
        // compares the two readers' refusal text, not merely that both refused.
        if self.integrity.result == IntegrityResult::Partial
            && self
                .integrity
                .partial_reason
                .as_deref()
                .unwrap_or("")
                .trim()
                .is_empty()
        {
            return Err(InvariantError(
                "integrity.result is 'partial' but partial_reason is null".into(),
            ));
        }
        // The format's only two float fields. `serde_json::to_value` turns a
        // non-finite f64 into `Value::Null` before `det_json`'s own walk ever
        // sees it (see `det_json.rs`'s module doc comment), so finiteness has
        // to be enforced here, on the typed field, where the error can still
        // name which field was bad.
        if let Some(pass_rate) = self.objectives.pass_rate {
            if !pass_rate.is_finite() {
                return Err(InvariantError(
                    "objectives.pass_rate is not finite (NaN or +/-Inf)".into(),
                ));
            }
        }
        if let Some(pass_rate_measured) = self.integrity.pass_rate_measured {
            if !pass_rate_measured.is_finite() {
                return Err(InvariantError(
                    "integrity.pass_rate_measured is not finite (NaN or +/-Inf)".into(),
                ));
            }
        }
        // Global Constraint 18(a): captured_by_logweir is a biconditional.
        // true  => last_phase_completed >= -1, a measured source-relative RPO,
        //          and NO unmeasured reason.
        // false => no source-relative RPO, and a reason saying why not.
        if self.source.captured_by_logweir {
            if self.last_phase_completed < -1 {
                return Err(InvariantError(
                    "source.captured_by_logweir is true but last_phase_completed is below -1"
                        .into(),
                ));
            }
            if self.measured.rpo_source_relative_seconds.is_none() {
                return Err(InvariantError(
                    "source.captured_by_logweir is true but rpo_source_relative_seconds is null"
                        .into(),
                ));
            }
            if self
                .measured
                .rpo_source_relative_unmeasured_reason
                .is_some()
            {
                return Err(InvariantError(
                    "source.captured_by_logweir is true but an unmeasured reason is present".into(),
                ));
            }
        } else {
            if self.measured.rpo_source_relative_seconds.is_some() {
                return Err(InvariantError(
                    "rpo_source_relative_seconds is set but the source was never contacted".into(),
                ));
            }
            if self
                .measured
                .rpo_source_relative_unmeasured_reason
                .is_none()
            {
                return Err(InvariantError(
                    "source.captured_by_logweir is false but rpo_source_relative_unmeasured_reason is null".into(),
                ));
            }
        }
        if self.integrity.level != IntegrityLevel::ByteFingerprint
            && self.objectives.pass_rate.is_some()
            && self.objectives.met == Some(true)
        {
            return Err(InvariantError(
                "objectives.met must be null when pass_rate is not measurable".into(),
            ));
        }
        // Every seconds-valued gap in the document is non-negative. These are
        // the format's only signed integers, and each has a direction that,
        // until now, only one call site enforced: `measured.rpo_seconds` by
        // the clamp in `crate::drill::phase8_score::compute_measured`,
        // `objectives.rpo_seconds` by nothing at all (it travels straight from
        // adopter YAML into the signed document), and
        // `measured.rpo_source_relative_seconds` by a writer that does not
        // exist yet (`--from-cluster`). Stating the rule here, in the
        // scorecard's own validator, converts a property guaranteed by one
        // call site into one guaranteed by the type — including for phases not
        // yet written. Checked field by field so the message names the
        // offender. The four RTO figures need no equivalent: they are `u64`,
        // so negative is already unrepresentable.
        for (name, value) in [
            ("measured.rpo_seconds", self.measured.rpo_seconds),
            (
                "measured.rpo_source_relative_seconds",
                self.measured.rpo_source_relative_seconds,
            ),
            ("objectives.rpo_seconds", self.objectives.rpo_seconds),
        ] {
            if let Some(v) = value {
                if v < 0 {
                    return Err(InvariantError(format!(
                        "{name} is negative ({v}); a recovery-point gap of less than zero \
                         is not a smaller gap, it is a meaningless one"
                    )));
                }
            }
        }
        if self.integrity.records_sampled_matching > self.integrity.records_sampled {
            return Err(InvariantError(
                "records_sampled_matching exceeds records_sampled".into(),
            ));
        }
        // T0-4: `outcome` is not a label, it is a claim entailed by the rest of
        // the document. Until this block existed, `self.outcome` appeared in no
        // arm of either reader, so a document saying `pass` beside its own
        // contradicting evidence was accepted, signed and verified at exit 0 —
        // `outcome: "pass"` with `integrity.partial_reason` naming an
        // unreconciled topic, and `outcome: "pass"` with
        // `records_sampled_matching: 50` against `records_sampled: 100`, were
        // both live and falsifiable. A RETROACTIVE TIGHTENING of the 1.0.0
        // reader, not a format change (GC12): no field is added and the
        // accepted set only narrows.
        //
        // Mirrored arm for arm, in this order, with this wording, in
        // `docs/verify_scorecard.py::check_invariants`. The messages are not to
        // be reworded: `crates/logweir/tests/two_reader_parity.rs` compares the
        // two readers' refusal TEXT, not merely that both refused.
        if self.outcome == Outcome::Pass && self.integrity.result != IntegrityResult::Pass {
            return Err(InvariantError(
                "outcome is 'pass' but integrity.result is not 'pass'".into(),
            ));
        }
        // The converse of the `Partial => partial_reason` arm above, using Task
        // 4's trimmed-empty predicate (ruling R-A) so `""` and `"   "` count as
        // absent in both readers.
        if self.outcome == Outcome::Pass
            && !self
                .integrity
                .partial_reason
                .as_deref()
                .unwrap_or("")
                .trim()
                .is_empty()
        {
            return Err(InvariantError(
                "outcome is 'pass' but integrity.partial_reason is present".into(),
            ));
        }
        // `met: None` is legitimate on a pass (no objective requested, or an
        // unmeasurable pass rate — the arm above this block requires exactly
        // that); only an explicit `false` contradicts the outcome.
        if self.outcome == Outcome::Pass && self.objectives.met == Some(false) {
            return Err(InvariantError(
                "outcome is 'pass' but objectives.met is false".into(),
            ));
        }
        if self.outcome == Outcome::Pass
            && self.integrity.records_sampled_matching != self.integrity.records_sampled
        {
            return Err(InvariantError(format!(
                "outcome is 'pass' but only {} of {} sampled records matched",
                self.integrity.records_sampled_matching, self.integrity.records_sampled
            )));
        }
        // `sample.records_expected` is THE CANARY SIZE this drill set out to
        // reconcile (see its doc comment, which also warns it is not
        // `phase4_sample::Selection::records_expected`); reconciling more
        // records than were selected is not a stronger result, it is an
        // incoherent one.
        if self.integrity.records_sampled > self.sample.records_expected {
            return Err(InvariantError(format!(
                "records_sampled ({}) exceeds sample.records_expected ({})",
                self.integrity.records_sampled, self.sample.records_expected
            )));
        }
        // Ruling R-F, DISCHARGED against
        // `logweir::drill::phase8_score::matrix_verdict_for` (phase8_score.rs —
        // NOT phase7_verify.rs, which the backlog named and where the function
        // does not exist): that function returns `Pass` on exactly one path,
        // `(Outcome::Pass, IntegrityLevel::ByteFingerprint)`, reachable only
        // when the phase-5 readback was itself `Pass`. Every other path returns
        // `PassDegraded`, `Fail`, or the observed non-`Pass` value unchanged.
        // The predicate therefore holds for every value the producer can emit,
        // and this arm states it as a property of the DOCUMENT rather than of
        // one call site. `pass-degraded` is the correct value for a pass at a
        // reduced integrity level.
        if self.engine.matrix_verdict == MatrixVerdict::Pass
            && !(self.outcome == Outcome::Pass
                && self.integrity.level == IntegrityLevel::ByteFingerprint)
        {
            return Err(InvariantError(
                "engine.matrix_verdict is 'pass' but the drill did not pass at byte-fingerprint level"
                    .into(),
            ));
        }
        if self.engine.matrix_verdict == MatrixVerdict::Fail
            && self.engine.matrix_verdict_reason.is_none()
        {
            return Err(InvariantError(
                "engine.matrix_verdict is 'fail' but matrix_verdict_reason is null".into(),
            ));
        }
        if self.integrity.level != IntegrityLevel::ByteFingerprint
            && self.integrity.pass_rate_measured.is_some()
        {
            return Err(InvariantError(
                "integrity.pass_rate_measured is set but the level is not byte-fingerprint".into(),
            ));
        }
        if !(-1..=9).contains(&self.last_phase_completed) {
            return Err(InvariantError("last_phase_completed outside -1..=9".into()));
        }
        // `target.auth` (Task 5b — Global Constraint 12's price for one nested
        // optional block). ABSENT IS LEGAL and means plaintext; these two arms
        // fire only on a block that IS present.
        //
        // BLANK IS NOT PLAINTEXT (ruling R-A: `trim().is_empty()` here,
        // `.strip()` in `docs/verify_scorecard.py`). A blank mode read as
        // "plaintext" would let a SCRAM run be recorded as an unauthenticated
        // one by omission — the one direction an auditor must never have to
        // guess at — and a `username` beside a blank mode is a principal
        // recorded without the mechanism it authenticated with, which is the
        // same defect with more evidence that it was not an accident.
        //
        // NEITHER MESSAGE INTERPOLATES. `docs/verify_scorecard.py` mirrors
        // both word for word and `crates/logweir/tests/two_reader_parity.rs::
        // every_invariant_arm_has_a_corpus_case` joins `index.json`'s `arm`
        // field against this body by literal substring — a username in the
        // message would make that join impossible and would additionally put
        // an adopter-supplied string into a refusal line.
        //
        // THE VALUE SET IS CLOSED (third arm, Task 5b fix round 1 — the
        // controller's one-spelling ruling). `mode` is a `String` on the wire
        // so that a reader can REPORT a value it refuses, but the accepted
        // set is exactly `AuthSpec`'s two serde tags. Before the third arm a
        // scorecard carrying `{"mode": "totally-made-up"}` verified 0/0 at
        // both readers — proved by execution in Task 5b's review — which made
        // the agreement between this block and `AuthSpec` an assertion about
        // serde in one test file rather than a property of the document
        // (Task 6's review, finding F-2).
        //
        // Task 6 FILLS this field from `AuthSpec::mode_str()`, whose return
        // type is `&'static str` over exactly those two literals, so no
        // scorecard this tree writes can reach the third arm; it exists for
        // the documents this tree did not write, which is every document a
        // reader is handed.
        if let Some(auth) = &self.target.auth {
            let mode_blank = auth.mode.trim().is_empty();
            let username_named = auth
                .username
                .as_deref()
                .is_some_and(|u| !u.trim().is_empty());
            if mode_blank && username_named {
                return Err(InvariantError(
                    "target.auth names a username with no auth mode; a username without its mechanism is not a record of how the client authenticated"
                        .into(),
                ));
            }
            if mode_blank {
                return Err(InvariantError(
                    "target.auth.mode is blank; an absent auth block is how a scorecard says plaintext"
                        .into(),
                ));
            }
            // NOT INTERPOLATED, like the two arms above it and unlike the
            // receipt's arm 5: the value here is `AuthSummary.mode`, and
            // these three messages are joined to `index.json`'s `arm` field
            // by LITERAL substring against this body
            // (`crates/logweir/tests/two_reader_parity.rs::
            // every_invariant_arm_has_a_corpus_case`). A placeholder would
            // make that join a pattern match, and it would put an
            // adopter-supplied string into a scorecard refusal line, which is
            // the rule this block already states above.
            // The EXACT wire value, not a trimmed one: the accepted set is two
            // exact literals, the blank arm above has already refused a
            // whitespace-only mode, and `" plaintext "` is a value no writer
            // in this tree produces. Both documents use the same rule —
            // `ReceiptAuth::mode`'s arm 5 does not trim either — so one
            // spelling means one comparison as well as one string.
            //
            // PROD-01.3 (format 1.5.0) SPLITS THIS ARM BY VERSION and leaves
            // every document below 1.5.0 judged exactly as before: the three
            // modes PROD-01.3 adds are values of 1.5.0 and later, so under an
            // older minor they are refused as a value no writer of that
            // version produced (the second statement below, which names the
            // version and never the mode), and from 1.5.0 the closed set is
            // five (the third). An older reader refuses a 1.5.0 scorecard
            // naming a new mode through the first statement — the SAFER
            // verdict, OD-7's third case — so the change is MINOR.
            let five_defined = defines_format_1_minor(&self.format_version, AUTH_MODES_SINCE_MINOR);
            if crate::connection::is_prod_01_3_auth_mode(&auth.mode) {
                if !five_defined {
                    return Err(InvariantError(format!(
                        "target.auth.mode is a value defined from 1.{AUTH_MODES_SINCE_MINOR}.0 \
                         and format_version {:?} predates it",
                        self.format_version
                    )));
                }
            } else if !crate::connection::ORIGINAL_AUTH_MODES.contains(&auth.mode.as_str()) {
                if five_defined {
                    return Err(InvariantError(
                        "target.auth.mode is not one of the five values this format defines; it is \"plaintext\", \"scramSha512\", \"scramSha256\", \"plain\" or \"mtls\" and nothing else"
                            .into(),
                    ));
                }
                return Err(InvariantError(
                    "target.auth.mode is not one of the two values this format defines; it is \"plaintext\" or \"scramSha512\" and nothing else"
                        .into(),
                ));
            }
        }
        // `topic_parity.not_reconstructed` (format 1.2.0, FX-3): arms NR-1
        // to NR-5. They fire ONLY on a document that CARRIES the block,
        // so every document without it — every 1.0.0 and 1.1.0 scorecard, and
        // a 1.2.0 one whose phase 7 never ran — is decided exactly as before.
        // Each judges the new block, against `format_version` (NR-1) or the
        // two lists every reader already shows (NR-2 to NR-5; NR-4 and NR-5
        // only in a `newTopic` document), the way the receipt's FX-4 arms 6
        // and 7 judge `config_coverage` against `format_version` and
        // `source.topics`: MINOR under the owner's OD-7 (a)
        // (`docs/stability.md`, "The v0.1.0 tag is the compatibility
        // boundary"). Each can only refuse.
        //
        // NOT INTERPOLATED, except NR-1's version: an entry names a topic, an
        // adopter-influenced string, and the messages are joined to
        // `index.json`'s `arm` fields by literal substring.
        //
        // Mirrored arm for arm, in this order and this position (after
        // `target.auth`, before `redactions`, which stays last), in
        // `docs/verify_scorecard.py::check_invariants`.
        if let Some(not_reconstructed) = &self.topic_parity.not_reconstructed {
            // NR-1. A document declaring a version before 1.2.0 cannot carry
            // a 1.2.0 field: under it, `intentionally_deviated` used the
            // scratch rationale in every mode, so a reader would not know
            // which of the two meanings the lists carry.
            let defined =
                defines_format_1_minor(&self.format_version, NOT_RECONSTRUCTED_SINCE_MINOR);
            if !defined {
                return Err(InvariantError(format!(
                    "topic_parity.not_reconstructed is present but format_version {:?} predates \
                     it: the field is defined from 1.{NOT_RECONSTRUCTED_SINCE_MINOR}.0",
                    self.format_version
                )));
            }
            // NR-2. THE FAIL-SAFE TWIN: what makes the 1.2.0 move safe for an
            // older reader. It reads only the two lists it always had, so a
            // deviation named here and missing there would be SILENCE to it,
            // which reads as parity — the defect FX-4's review M5 named.
            if not_reconstructed
                .iter()
                .any(|e| !self.topic_parity.unexpected_divergence.contains(e))
            {
                return Err(InvariantError(
                    "topic_parity.not_reconstructed names a deviation that unexpected_divergence does not; a source setting the restore did not reconstruct is also an unexpected divergence, so a reader that predates not_reconstructed never reads it as parity"
                        .into(),
                ));
            }
            // NR-3. A setting the restore did not reconstruct is the opposite
            // of the label a scratch drill gives the same deviation.
            if not_reconstructed
                .iter()
                .any(|e| self.topic_parity.intentionally_deviated.contains(e))
            {
                return Err(InvariantError(
                    "topic_parity.not_reconstructed names a deviation that intentionally_deviated also names; a source setting the restore did not reconstruct is never an intended deviation"
                        .into(),
                ));
            }
            // NR-4 and NR-5 (FX-3 review F1). In a `newTopic` document the
            // block is the claim, so the two existing lists must agree with
            // it, or a writer that regressed to the scratch labels (the mode
            // lost on its way to phase 7) would sign `not_reconstructed: []`
            // beside intended deviations: "nothing was left unreconstructed",
            // a stronger claim than the pre-1.2.0 defect's, which both readers
            // at least re-read as not reconstructed. Each fires only on a
            // document that carries the block (so at 1.2.0 or later, NR-1) and
            // whose `target.mode` is `newTopic`, and each can only refuse.
            if self.target.mode == TargetMode::NewTopic {
                // NR-4. A `newTopic` restore labels nothing intended.
                if !self.topic_parity.intentionally_deviated.is_empty() {
                    return Err(InvariantError(
                        "topic_parity.intentionally_deviated is not empty in a newTopic document that carries not_reconstructed; a newTopic restore's deviations are source settings it did not reconstruct, never intended ones"
                            .into(),
                    ));
                }
                // NR-5. The converse of NR-2 over the settings the restore
                // decides: such a deviation in `unexpected_divergence` is one
                // the restore did not reconstruct, so the block names it.
                if self.topic_parity.unexpected_divergence.iter().any(|e| {
                    RESTORE_DECIDED_SETTINGS.contains(&parity_key(e))
                        && !not_reconstructed.contains(e)
                }) {
                    return Err(InvariantError(
                        "topic_parity.unexpected_divergence names a setting the restore decides (cleanup.policy, retention.ms, partition_count or replication_factor) that not_reconstructed does not, in a newTopic document; such a deviation is a source setting the restore did not reconstruct"
                            .into(),
                    ));
                }
            }
        }
        // `source.time_basis` (format 1.3.0, FX-8): arms TB-1 to TB-4. They
        // fire ONLY on a document that CARRIES the block and judge the block
        // alone (TB-1 against `format_version`, as the receipt's FX-4 arm 6
        // judges `config_coverage`), so every document without it — every
        // scorecard before 1.3.0 — is decided exactly as before: MINOR under
        // the owner's OD-7 (a) (`docs/stability.md`, "The v0.1.0 tag is the
        // compatibility boundary").
        //
        // NOT INTERPOLATED, except TB-1's version: the lists name topics, an
        // adopter-influenced string, and the messages are joined to
        // `index.json`'s `arm` fields by literal substring.
        //
        // Mirrored arm for arm, in this order and this position (after
        // `target.auth`, before `redactions`, which stays last), in
        // `docs/verify_scorecard.py::check_invariants`.
        if let Some(time_basis) = &self.source.time_basis {
            // TB-1. A document declaring a version before 1.3.0 cannot carry
            // a 1.3.0 field.
            let defined = defines_format_1_minor(&self.format_version, TIME_BASIS_SINCE_MINOR);
            if !defined {
                return Err(InvariantError(format!(
                    "source.time_basis is present but format_version {:?} predates it: the \
                     field is defined from 1.{TIME_BASIS_SINCE_MINOR}.0",
                    self.format_version
                )));
            }
            // TB-2. The plan's time basis has one value; any other spelling is
            // not one `restore.time_basis` can parse to.
            if time_basis
                .plan
                .as_deref()
                .is_some_and(|plan| plan != TIME_BASIS_PRODUCER_TIME)
            {
                return Err(InvariantError(
                    "source.time_basis.plan is not \"producerTime\", the one value restore.time_basis has"
                        .into(),
                ));
            }
            // TB-3. THE RULE ITSELF, in the signed document: a selection by
            // producer time over a `LogAppendTime` topic is one the approved
            // plan accepted, never a default.
            if !time_basis.producer_time.is_empty()
                && time_basis.plan.as_deref() != Some(TIME_BASIS_PRODUCER_TIME)
            {
                return Err(InvariantError(
                    "source.time_basis.producer_time names a topic but source.time_basis.plan is not \"producerTime\"; a selection by producer time is one the approved plan accepted, never a default"
                        .into(),
                ));
            }
            // TB-4. A topic's timestamp type was either recorded as
            // `LogAppendTime` or not recorded at all.
            if time_basis
                .producer_time
                .iter()
                .any(|t| time_basis.not_recorded.contains(t))
            {
                return Err(InvariantError(
                    "source.time_basis names a topic in both producer_time and not_recorded; a topic's timestamp type was either recorded as LogAppendTime or not recorded"
                        .into(),
                ));
            }
        }
        // `integrity.verification` (format 1.4.0, PROD-08.1): arms IV-1 to
        // IV-7. They fire ONLY on a document that CARRIES the block and judge
        // the block, or an existing field against it (IV-6, as NR-2 to NR-5
        // judge the existing parity lists against `not_reconstructed`), so
        // every document without it — every scorecard before 1.4.0 — is
        // decided exactly as before: MINOR under the owner's OD-7 (a)
        // (`docs/stability.md`, "The v0.1.0 tag is the compatibility
        // boundary"). Each can only refuse.
        //
        // NOT INTERPOLATED, except IV-1's version: the block names topics and
        // segment keys, adopter-influenced strings, and the messages are
        // joined to `index.json`'s `arm` fields by literal substring.
        //
        // Mirrored arm for arm, in this order and this position (after
        // `source.time_basis`, before `redactions`, which stays last), in
        // `docs/verify_scorecard.py::check_invariants`.
        if let Some(v) = &self.integrity.verification {
            // IV-1. A document declaring a version before 1.4.0 cannot carry
            // a 1.4.0 field.
            let defined = defines_format_1_minor(&self.format_version, VERIFICATION_SINCE_MINOR);
            if !defined {
                return Err(InvariantError(format!(
                    "integrity.verification is present but format_version {:?} predates it: the \
                     field is defined from 1.{VERIFICATION_SINCE_MINOR}.0",
                    self.format_version
                )));
            }
            // IV-2. A coverage this reader does not know is never read as
            // complete, and never as sampled either: it is refused.
            if v.coverage != COVERAGE_SAMPLED && v.coverage != COVERAGE_COMPLETE {
                return Err(InvariantError(
                    "integrity.verification.coverage is neither \"sampled\" nor \"complete\""
                        .into(),
                ));
            }
            // IV-3. Only a complete verification compares headers in order: a
            // sampled one reconciles a fingerprint that sorts them, so it can
            // never claim header order was verified.
            let complete = v.coverage == COVERAGE_COMPLETE;
            let order_known = v.header_order == HEADER_ORDER_VERIFIED
                || v.header_order == HEADER_ORDER_NOT_VERIFIED;
            if !order_known || (v.header_order == HEADER_ORDER_VERIFIED && !complete) {
                return Err(InvariantError(
                    "integrity.verification.header_order is not \"verified\" or \"notVerified\", or claims \"verified\" for a coverage that is not complete; a sampled verification compares a fingerprint that sorts headers"
                        .into(),
                ));
            }
            // IV-4. The complete block is the claim `coverage: complete` makes,
            // and nothing else carries it.
            if complete != v.complete.is_some() {
                return Err(InvariantError(
                    "integrity.verification.complete is present exactly when integrity.verification.coverage is \"complete\""
                        .into(),
                ));
            }
            if let Some(c) = &v.complete {
                // IV-5. An incomplete complete verification says why, and a
                // covered one has no reason to give.
                let reason_given = c
                    .incomplete_reason
                    .as_deref()
                    .is_some_and(|r| !r.trim().is_empty());
                if c.covered == reason_given {
                    return Err(InvariantError(
                        "integrity.verification.complete.incomplete_reason is required exactly when complete.covered is false"
                            .into(),
                    ));
                }
                // IV-6. THE RULE ITSELF, in the signed document: a pass over a
                // complete verification is a verification that covered every
                // partition, verified every segment and found the restored
                // output exactly the expected one — in total AND in every
                // partition, over at least one partition (review L-2: a pass
                // whose partitions are inexact but whose sums happen to be
                // exact, and a covered pass over no partition at all, were
                // accepted by both readers; the writer produces neither).
                if self.integrity.result == IntegrityResult::Pass {
                    let clean = c.covered
                        && !c.partitions.is_empty()
                        && c.archive.segments_failed.is_empty()
                        && c.archive.segments_unverified.is_empty()
                        && c.archive.segments_verified == c.archive.segments
                        && c.replay.is_exact()
                        && c.partitions
                            .iter()
                            .all(|p| p.compared && p.replay.is_exact());
                    if !clean {
                        return Err(InvariantError(
                            "integrity.result is pass but integrity.verification.complete is not covered, lists no partition, names a failed or unverified segment, or records a missing, unexpected, duplicate, out-of-order or mismatched record, in total or in a partition"
                                .into(),
                        ));
                    }
                }
                // IV-7. The totals are the partitions' sums, and every listed
                // segment is verified, failed or unverified.
                let mut replay = ReplayComparison::default();
                let (mut segments, mut verified, mut decoded, mut holes) = (0u64, 0u64, 0u64, 0u64);
                for p in &c.partitions {
                    replay.add(&p.replay);
                    segments = segments.saturating_add(p.segments);
                    verified = verified.saturating_add(p.segments_verified);
                    decoded = decoded.saturating_add(p.records_decoded);
                    holes = holes.saturating_add(p.offset_holes);
                }
                let accounted = c
                    .archive
                    .segments_verified
                    .saturating_add(c.archive.segments_failed.len() as u64)
                    .saturating_add(c.archive.segments_unverified.len() as u64);
                if replay != c.replay
                    || segments != c.archive.segments
                    || verified != c.archive.segments_verified
                    || decoded != c.archive.records_decoded
                    || holes != c.archive.offset_holes
                    || accounted != c.archive.segments
                {
                    return Err(InvariantError(
                        "integrity.verification.complete's totals are not the sums of its partitions, or its segments are not each verified, failed or unverified"
                            .into(),
                    ));
                }
            }
        }
        // `sample.unsampled_topics` (format 1.6.0, FX-23): arms US-1 to US-3.
        // They fire ONLY on a document that CARRIES the field, so every
        // document without it is decided exactly as before: MINOR under the
        // owner's OD-7 (a). US-3 judges an existing field against it (as IV-6
        // does) and can only refuse.
        //
        // NOT INTERPOLATED, except US-1's version: the list names topics, an
        // adopter-influenced string, and the messages are joined to
        // `index.json`'s `arm` fields by literal substring.
        //
        // Mirrored arm for arm, in this order and this position (after
        // `integrity.verification`, before `redactions`, which stays last), in
        // `docs/verify_scorecard.py::check_invariants`.
        if let Some(unsampled) = &self.sample.unsampled_topics {
            // US-1. A document declaring a version before 1.6.0 cannot carry
            // a 1.6.0 field.
            let defined =
                defines_format_1_minor(&self.format_version, UNSAMPLED_TOPICS_SINCE_MINOR);
            if !defined {
                return Err(InvariantError(format!(
                    "sample.unsampled_topics is present but format_version {:?} predates it: \
                     the field is defined from 1.{UNSAMPLED_TOPICS_SINCE_MINOR}.0",
                    self.format_version
                )));
            }
            // US-2. Each topic once, in order, and absent rather than empty:
            // one list has one spelling, so two readers compare it as one.
            if unsampled.is_empty()
                || unsampled.iter().any(|t| t.trim().is_empty())
                || !unsampled.windows(2).all(|w| w[0] < w[1])
            {
                return Err(InvariantError(
                    "sample.unsampled_topics is empty, names a blank topic, or is not sorted \
                     and free of repeats; it names each topic max_partitions left unsampled \
                     once, in order, and is absent when there is none"
                        .into(),
                ));
            }
            // US-3. A complete verification compares every restored partition
            // (phase 0 refuses it beside `max_partitions`), so it leaves no
            // topic unsampled.
            if self
                .integrity
                .verification
                .as_ref()
                .is_some_and(|v| v.coverage == COVERAGE_COMPLETE)
            {
                return Err(InvariantError(
                    "sample.unsampled_topics is present but integrity.verification.coverage is \
                     \"complete\"; a complete verification compares every restored partition \
                     and leaves no topic unsampled"
                        .into(),
                ));
            }
        }
        // `source.selection` (format 1.7.0, PROD-11.1): arms SEL-1 to SEL-3.
        // They fire ONLY on a document that CARRIES the block, so every
        // document without it is decided exactly as before: MINOR under the
        // owner's OD-7 (a). SEL-3 judges an existing field (the 1.4.0 complete
        // block's window) against it, as IV-6 does, and can only refuse.
        //
        // PROD-11.1b (format 2.0.0, the owner's OD-9 (a)): arms PS-2 to PS-5.
        // PS-1, that a major-2 document carries partition subsets, is
        // `refuse_unreadable_major`'s. PS-2 holds a format-1 block to the
        // 1.7.0 shape (a start and its end, nothing else), so a subset never
        // rides in a document an older reader would read as a full restore;
        // PS-3 to PS-5 read only a 2.0.0 block's own fields, or judge the
        // complete block against it, and can only refuse.
        //
        // NOT INTERPOLATED, except SEL-1's version, so the messages join
        // `index.json`'s `arm` fields by literal substring.
        //
        // Mirrored arm for arm, in this order and this position (after
        // `sample.unsampled_topics`, before `redactions`, which stays last), in
        // `docs/verify_scorecard.py::check_invariants`.
        if let Some(selection) = &self.source.selection {
            // SEL-1. A document declaring a version before 1.7.0 cannot carry
            // a 1.7.0 block.
            let defined = defines_format_1_minor(&self.format_version, SELECTION_SINCE_MINOR);
            if !defined {
                return Err(InvariantError(format!(
                    "source.selection is present but format_version {:?} predates it: the \
                     block is defined from 1.{SELECTION_SINCE_MINOR}.0",
                    self.format_version
                )));
            }
            // PS-2. A format-1 block is a window start and its end, nothing
            // else: a subset in a document an older reader accepts would be
            // read as every partition (OD-9).
            if major_version(&self.format_version) == Some(1)
                && (selection.window_start_ms.is_none()
                    || selection.partitions.is_some()
                    || selection.engine_runs.is_some())
            {
                return Err(InvariantError(
                    "source.selection under major 1 is a stated window start and its end, and \
                     nothing else: a block without window_start_ms, or with partitions or \
                     engine_runs, is a partition-subset selection, which is format 2.0.0"
                        .into(),
                ));
            }
            // SEL-2. A stated start is before the end (the plan refuses
            // anything else before it runs).
            if selection
                .window_start_ms
                .is_some_and(|start| start >= selection.window_end_ms)
            {
                return Err(InvariantError(
                    "source.selection.window_start_ms is not before window_end_ms; a selection's \
                     window holds at least one instant after its start"
                        .into(),
                ));
            }
            // SEL-3. A complete verification's expected output is selected by
            // the plan's own window: its start (absent for a 2.0.0 block from
            // the archive's floor, as the complete block's is) and its end.
            let complete = self
                .integrity
                .verification
                .as_ref()
                .and_then(|v| v.complete.as_ref());
            if let Some(c) = complete {
                if c.window.start_ms != selection.window_start_ms
                    || c.window.end_ms != selection.window_end_ms
                {
                    return Err(InvariantError(
                        "integrity.verification.complete.window is not source.selection's window; \
                         the expected output is selected by the plan's own start and end"
                            .into(),
                    ));
                }
            }
            if let Some(subsets) = &selection.partitions {
                // PS-3. One spelling per selection: each topic once, in order,
                // not blank, each list non-empty, ascending, distinct and not
                // negative.
                let well_formed = subsets.windows(2).all(|w| w[0].topic < w[1].topic)
                    && subsets.iter().all(|tp| {
                        !tp.topic.trim().is_empty()
                            && !tp.partitions.is_empty()
                            && tp.partitions.iter().all(|p| *p >= 0)
                            && tp.partitions.windows(2).all(|w| w[0] < w[1])
                    });
                if !well_formed {
                    return Err(InvariantError(
                        "source.selection.partitions does not name each topic once, in order, \
                         with a non-empty, sorted list of distinct partitions that are not \
                         negative"
                            .into(),
                    ));
                }
                // PS-4. The engine's partition filter applies to every topic
                // of one run, so each distinct subset is its own run, and at
                // most one more run restores the topics without one.
                let mut distinct: Vec<&Vec<i32>> =
                    subsets.iter().map(|tp| &tp.partitions).collect();
                distinct.sort();
                distinct.dedup();
                let need = distinct.len() as u64;
                if !selection
                    .engine_runs
                    .is_some_and(|runs| u64::from(runs) == need || u64::from(runs) == need + 1)
                {
                    return Err(InvariantError(
                        "source.selection.engine_runs is not one run per distinct partition \
                         subset, or one more for the topics without one"
                            .into(),
                    ));
                }
                // PS-5. Nothing is expected from a partition the plan did not
                // select: the complete block lists one only when the target
                // holds a record there, which is unexpected.
                if complete.is_some_and(|c| {
                    c.partitions.iter().any(|p| {
                        p.replay.expected > 0 && !selection.selects_partition(&p.topic, p.partition)
                    })
                }) {
                    return Err(InvariantError(
                        "integrity.verification.complete.partitions expects records from a \
                         partition source.selection does not select"
                            .into(),
                    ));
                }
            }
        }
        // `target.original_name` (format 1.8.0, PROD-15.1): arms ON-1 to
        // ON-14. They fire ONLY on a document that CARRIES the block, so every
        // document without it is decided exactly as before: MINOR under the
        // owner's OD-7 (a). ON-2, ON-3, ON-7, ON-13 and ON-14 judge existing
        // fields (`target`, `source.selection`, `integrity.verification`, the
        // outcome) against the block and can only refuse.
        //
        // NOT INTERPOLATED, except ON-1's version, so the messages join
        // `index.json`'s `arm` fields by literal substring.
        //
        // Mirrored arm for arm, in this order and this position (after
        // `source.selection`, before `redactions`, which stays last), in
        // `docs/verify_scorecard.py::check_invariants`. ON-14 is judged FIRST.
        if let Some(on) = &self.target.original_name {
            // ON-14, first. An original-name restore restores WHOLE topics:
            // the block never sits beside a partition subset. The runner
            // refuses such a plan before anything is created
            // (`OriginalNameNeedsWholeTopics`), so a document carrying both
            // was not written by one. Every 2.x document names a subset
            // (PS-1), so this is the arm a 2.x document carrying the block
            // meets; a subset under major 1 has already met PS-2.
            if self
                .source
                .selection
                .as_ref()
                .is_some_and(|selection| selection.partitions.is_some())
            {
                return Err(InvariantError(
                    "target.original_name is present beside source.selection.partitions; a \
                     restore under the original topic names restores whole topics, never a \
                     partition subset"
                        .into(),
                ));
            }
            // ON-1. The block is format 1's, from 1.8.0: a document declaring
            // an earlier minor cannot carry it. MAJOR 1 ON PURPOSE, where the
            // older optional blocks read `defines_format_1_minor`: a 2.x
            // document is a partition-subset restore's and never carries the
            // block (ON-14), so 2.0.0 does not define it.
            if !defines_original_name(&self.format_version) {
                return Err(InvariantError(format!(
                    "target.original_name is present but format_version {:?} does not define \
                     it: the block is format 1's, from 1.{ORIGINAL_NAME_SINCE_MINOR}.0, and no \
                     other major carries it",
                    self.format_version
                )));
            }
            // ON-2. The identity ban stays in scratch mode.
            if self.target.mode.is_scratch() {
                return Err(InvariantError(
                    "target.original_name is present but target.mode is scratch; a scratch drill \
                     never restores under the original topic names"
                        .into(),
                ));
            }
            // ON-3. The original names ARE the identity mapping.
            if !self.target.topic_mapping_prefix.is_empty() {
                return Err(InvariantError(
                    "target.original_name is present but target.topic_mapping_prefix is not \
                     empty; an original-name restore maps every topic onto its own name"
                        .into(),
                ));
            }
            // ON-4. Only its own approval subject authorises it.
            if on.approval_subject != crate::original_name::APPROVAL_SUBJECT_ORIGINAL_NAME {
                return Err(InvariantError(
                    "target.original_name.approval_subject is not \"originalName\"; an \
                     original-name restore is authorised only by its own approval subject"
                        .into(),
                ));
            }
            // ON-5. PROD-16.2 (format 1.9.0) SPLITS THIS ARM BY VERSION, as
            // PROD-01.3 split `target.auth.mode`'s, and leaves every document
            // below 1.9.0 judged exactly as before: `consoleApproval` is a
            // value of 1.9.0 and later, so under an older minor it is refused
            // as a value that version does not define (the first statement,
            // which names the version and never the document's word), and
            // from 1.9.0 the closed set is four (the second). A reader built
            // at 1.8.0 refuses a 1.9.0 document naming the new member through
            // the third statement, its own, unchanged — the SAFER verdict,
            // OD-7's third case — so the change is MINOR. This build accepts
            // the member only beside the block that says who approved (arm
            // CA-8, below). NOT INTERPOLATED but for the version.
            let four_defined = defines_console_approval(&self.format_version);
            if on.approval_mode == APPROVAL_MODE_CONSOLE {
                if !four_defined {
                    return Err(InvariantError(format!(
                        "target.original_name.approval_mode is a value defined from \
                         1.{CONSOLE_APPROVAL_SINCE_MINOR}.0 and format_version {:?} predates it",
                        self.format_version
                    )));
                }
            } else if !ORIGINAL_NAME_APPROVAL_MODES_AT_1_8_0.contains(&on.approval_mode.as_str()) {
                if four_defined {
                    return Err(InvariantError(
                        "target.original_name.approval_mode is not one of the four values this \
                         format defines; it is \"v1Approval\", \"governed\", \"ordinary\" or \
                         \"consoleApproval\" and nothing else"
                            .into(),
                    ));
                }
                return Err(InvariantError(
                    "target.original_name.approval_mode is not one of \"v1Approval\", \
                     \"governed\", \"ordinary\""
                        .into(),
                ));
            }
            // ON-6.
            if !crate::original_name::CLUSTER_CONDITIONS.contains(&on.cluster_condition.as_str()) {
                return Err(InvariantError(
                    "target.original_name.cluster_condition is not one of \"targetIsNotSource\", \
                     \"autoCreateDisabled\""
                        .into(),
                ));
            }
            // ON-7. "Not the source" is a comparison of two known ids.
            let source_named = on
                .source_cluster_id
                .as_deref()
                .filter(|s| !s.trim().is_empty());
            if on.cluster_condition == crate::original_name::CLUSTER_CONDITION_TARGET_IS_NOT_SOURCE
                && source_named.is_none_or(|s| s == self.target.cluster_id)
            {
                return Err(InvariantError(
                    "target.original_name.cluster_condition is targetIsNotSource but \
                     source_cluster_id is absent or equals target.cluster_id; the condition is a \
                     comparison of two known cluster ids"
                        .into(),
                ));
            }
            // ON-8. Somewhere was looked, each place once, from the closed set.
            let places = &crate::original_name::OWNER_DETECTION_PLACES;
            let mut seen = std::collections::BTreeSet::new();
            if on.owner_detection.is_empty()
                || on
                    .owner_detection
                    .iter()
                    .any(|p| !places.contains(&p.as_str()) || !seen.insert(p.as_str()))
            {
                return Err(InvariantError(
                    "target.original_name.owner_detection is empty, repeats a place, or names \
                     one outside \"plan\", \"kafkaTopicResources\", \"pointReceipt\"; an owner \
                     nobody looked for is never read as no owner"
                        .into(),
                ));
            }
            // ON-9. Every owner names a place that was looked in, a known
            // kind and a topic.
            if on.owners.iter().any(|o| {
                !on.owner_detection.contains(&o.found_in)
                    || !crate::topic_configuration::OWNER_KINDS.contains(&o.kind.as_str())
                    || o.topic.trim().is_empty()
            }) {
                return Err(InvariantError(
                    "target.original_name.owners names a place owner_detection does not list, a \
                     kind outside \"strimzi\" and \"external\", or a blank topic"
                        .into(),
                ));
            }
            // ON-10. An owned name is restored only on the owner path.
            if !on.owners.is_empty() && !on.owner_path {
                return Err(InvariantError(
                    "target.original_name.owners is not empty and owner_path is false; an owned \
                     name is restored only on the owner path"
                        .into(),
                ));
            }
            // ON-11 (OD-10). A one-person confirmation is signed only with the
            // topic names typed, and nothing else claims a typed confirmation.
            let typed = on.confirmation.as_deref()
                == Some(crate::original_name::CONFIRMATION_TYPED_TOPIC_NAMES);
            if (on.approval_mode == "ordinary") != typed || (on.confirmation.is_some() && !typed) {
                return Err(InvariantError(
                    "target.original_name.confirmation is not \"typedTopicNames\" exactly when \
                     approval_mode is \"ordinary\"; a one-person confirmation of an original-name \
                     restore is signed only with every original topic name re-typed"
                        .into(),
                ));
            }
            // ON-12. The resources file a runner looked in is named by digest,
            // exactly when it is a place that was looked in.
            let listed = on
                .owner_detection
                .iter()
                .any(|p| p == crate::original_name::OWNER_FOUND_IN_KAFKA_TOPIC_RESOURCES);
            let digest_ok = on
                .kafka_topic_resources_sha256
                .as_deref()
                .is_some_and(crate::check_contract::is_sha256_prefixed);
            if listed != digest_ok || (on.kafka_topic_resources_sha256.is_some() && !digest_ok) {
                return Err(InvariantError(
                    "target.original_name.kafka_topic_resources_sha256 is not a sha256 digest \
                     exactly when owner_detection lists \"kafkaTopicResources\"; the KafkaTopic \
                     resources a runner looked in are named by their digest"
                        .into(),
                ));
            }
            // ON-13. An original-name restore is verified COMPLETELY, never
            // by sample: the runner refuses a sampled original-name plan at
            // phase 0, so a document carrying the block beside a sampled
            // verification was not written by one. A run that stopped before
            // phase 7 records no verification, and is never a pass — so a
            // PASS that records none is refused too (the third case decided
            // to the safer side). IV-2 has already refused a coverage that is
            // neither value.
            let coverage = self
                .integrity
                .verification
                .as_ref()
                .map(|v| v.coverage.as_str());
            let passes =
                self.outcome == Outcome::Pass || self.integrity.result == IntegrityResult::Pass;
            if coverage.is_some_and(|c| c != COVERAGE_COMPLETE) || (coverage.is_none() && passes) {
                return Err(InvariantError(
                    "target.original_name is present but integrity.verification.coverage is not \"complete\", or a pass records no verification; a restore under the original topic names is verified completely, never by sample"
                        .into(),
                ));
            }
        }
        // `approval.console` (format 1.9.0, and 2.1.0; PROD-16.2): arms CA-1
        // to CA-8. CA-1 to CA-7 fire ONLY on a document that CARRIES the
        // block, so every document without it is decided exactly as before:
        // MINOR under the owner's OD-7 (a). CA-5, CA-6 and CA-7 judge existing
        // fields (`approval.approver`, `approval.key_id`,
        // `approval.approved_at`) against the block and can only refuse. CA-8
        // ties the block to `target.original_name.approval_mode`, whose new
        // member only this build writes.
        //
        // NOT INTERPOLATED, except CA-1's version: no message carries a
        // principal, an instant or a key id from the document, so the
        // messages join `index.json`'s `arm` fields by literal substring and
        // nothing a document's author chose reaches a reader's output through
        // a refusal.
        //
        // Mirrored arm for arm, in this order and this position (after the
        // ON arms, before `redactions`, which stays last), in
        // `docs/verify_scorecard.py::check_invariants`.
        if let Some(console) = &self.approval.console {
            // CA-1. The block is defined from 1.9.0 of format 1 and from
            // 2.1.0 of format 2.
            if !defines_console_approval(&self.format_version) {
                return Err(InvariantError(format!(
                    "approval.console is present but format_version {:?} does not define it: \
                     the block is defined from 1.{CONSOLE_APPROVAL_SINCE_MINOR}.0 of format 1 and \
                     from 2.{CONSOLE_APPROVAL_SINCE_MINOR_OF_MAJOR_2}.0 of format 2",
                    self.format_version
                )));
            }
            // CA-2. The block describes one mode.
            if console.mode != APPROVAL_MODE_CONSOLE {
                return Err(InvariantError(
                    "approval.console.mode is not \"consoleApproval\"".into(),
                ));
            }
            // CA-3. Two people: each in a form that can be compared, neither
            // the local administrator nor a system identity, of one issuer,
            // with different subjects — the rule the console, the controller
            // and the runner applied before this was signed
            // (`crate::approval_policy::separation_fault`).
            if let Some(fault) = crate::approval_policy::separation_fault(
                &console.requester.issuer,
                &console.requester.subject,
                &console.approver.issuer,
                &console.approver.subject,
            ) {
                return Err(InvariantError(format!(
                    "approval.console does not name two people: {}",
                    console_separation_words(fault)
                )));
            }
            // CA-4. No fabricated time: the approval lies inside the
            // request's own window.
            if console.approved_at < console.requested_at
                || console.approved_at >= console.request_expires_at
            {
                return Err(InvariantError(
                    "approval.console.approved_at is before requested_at or not before \
                     request_expires_at; an approval is given after the request was made and \
                     before it expires"
                        .into(),
                ));
            }
            // CA-5. The approver the scorecard names IS the second person.
            if self.approval.approver != console.approver.principal_id() {
                return Err(InvariantError(
                    "approval.approver is not approval.console.approver as \
                     \"<issuer>#<subject>\"; under a console approval the approver a scorecard \
                     names is the second person the console attested"
                        .into(),
                ));
            }
            // CA-6. The console's key signs the approval, so the approver's
            // key IS the console key in this mode — and a distinct personal
            // key is another mode.
            if console.confirmation_key_id.trim().is_empty()
                || self.approval.key_id != console.confirmation_key_id
            {
                return Err(InvariantError(
                    "approval.key_id is not approval.console.confirmation_key_id; under a \
                     console approval the console's key signs the approval, so the approver's \
                     key is the console key (expected in this mode), and a distinct personal key \
                     is not a console approval"
                        .into(),
                ));
            }
            // CA-7. The approval time is the instant the second person
            // approved, and no other.
            if self.approval.approved_at != console.approved_at {
                return Err(InvariantError(
                    "approval.approved_at is not approval.console.approved_at; under a console \
                     approval the approval time is the instant the second person approved, never \
                     the request's"
                        .into(),
                ));
            }
        }
        // CA-8. A restore under the original topic names that a second person
        // approved in the console says so in BOTH places, and no other
        // original-name restore says so in either.
        if let Some(on) = &self.target.original_name {
            if (on.approval_mode == APPROVAL_MODE_CONSOLE) != self.approval.console.is_some() {
                return Err(InvariantError(
                    "target.original_name.approval_mode is \"consoleApproval\" exactly when \
                     approval.console is present; a console approval names who approved, and a \
                     governed, ordinary or v1 approval carries no console approver"
                        .into(),
                ));
            }
        }
        // T0-3: `docs/formats/drill-scorecard.md`'s `## redactions` section
        // states "Always `[]` in v0.1" as a PROPERTY OF THE FORMAT, and until
        // now nothing enforced it and no surface displayed it — a third party
        // could hand an auditor a scorecard carrying
        // `redactions: [{"path": "/measured/rpo_seconds", …}]` and both readers
        // printed VALID while the document itself said a field the auditor
        // reads first had been removed.
        //
        // v0.1 has no writer that can produce a redaction (`redactions` is
        // only ever constructed as `vec![]`), so a non-empty one means the
        // document was edited after signing-time construction or came from a
        // reader-incompatible producer. Like the evidence arm above, this is a
        // RETROACTIVE TIGHTENING of the 1.0.0 reader rather than a format
        // change (GC12): no byte of the format changes, the accepted set
        // narrows, and no document Logweir has ever written is refused.
        //
        // LAST on purpose, and mutant-tested for it: a document violating this
        // and an earlier arm must report the earlier arm's message, from BOTH
        // readers. `docs/verify_scorecard.py::check_invariants` mirrors this
        // arm in the same position with the same words, and the message
        // interpolates the DOCUMENT's own `format_version` so a 1.0.1 document
        // reads correctly. A 2.x document is already refused above by
        // `refuse_unreadable_major`. When 0.1.1 adds `--redact`, this arm is
        // REPLACED by a path whitelist (`/target/cluster_id`,
        // `/approval/approver`) — it is not deleted.
        if !self.redactions.is_empty() {
            return Err(InvariantError(format!(
                "redactions is non-empty but format_version {} has no way to produce one; \
                 --redact is a v0.1.1 feature",
                self.format_version
            )));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    //! Direct coverage of `Scorecard::validate_invariants`'s per-arm behaviour.
    //! `crates/logweir-core/tests/scorecard_golden.rs` is frozen at exactly
    //! four tests (addendum ruling A1), so this coverage lives here instead.
    //! Every test asserts the SPECIFIC error message, not a bare `is_err()`,
    //! so deleting or merging an arm makes exactly one test fail.
    use super::*;

    /// A scorecard that satisfies every invariant `validate_invariants` checks.
    /// `captured_by_logweir` is false (the false-branch of Global Constraint
    /// 18(a)), matching what v0.1 ever actually produces. Each test below
    /// clones this and overrides only the field(s) needed to trip one arm.
    fn valid_scorecard() -> Scorecard {
        let t = |s: &str| DateTime::parse_from_rfc3339(s).unwrap().with_timezone(&Utc);
        Scorecard {
            format_version: crate::FORMAT_VERSION.to_string(),
            run_id: "01J9X2QK7C4V0R8YB3ZP6MTS5A".into(),
            outcome: Outcome::Pass,
            last_phase_completed: 9,
            requested_at: t("2026-09-03T09:00:00Z"),
            approval_validated_at: None,
            triggered_by: None,
            engine: EngineInfo {
                id: "oso-cli".into(),
                version: "v0.21.0".into(),
                digest: "sha256:0".into(),
                execution: "subprocess".into(),
                levers: Levers {
                    header_preflight: LeverState::Honoured,
                    dry_run_check_segments: LeverState::UnknownNotObservable,
                    unknown_key_warnings: vec![],
                },
                matrix_verdict: MatrixVerdict::Pass,
                matrix_verdict_reason: None,
            },
            source: SourceInfo {
                backup_id: "backup-1".into(),
                manifest_sha256: "sha256:0".into(),
                manifest_version_id: None,
                captured_by_logweir: false,
                // Absent, the shape of every document before 1.3.0. The four
                // `source.time_basis` arms have their own unit tests below,
                // over this same base document.
                time_basis: None,
                selection: None,
            },
            target: TargetInfo {
                cluster_id: "cluster-1".into(),
                // The DEFAULT, which is absent on the wire. Every test below
                // that needs the other mode says so by assignment.
                mode: TargetMode::Scratch,
                marker_topic: Some("logweir.scratch".into()),
                topic_mapping_prefix: "drill-".into(),
                topic_mapping_sha256: "sha256:0".into(),
                topic_mapping_entries: 1,
                // Absent, which is legal and means plaintext. The two
                // `target.auth` arms have their own unit tests below, over
                // this same base document.
                auth: None,
                original_name: None,
            },
            approval: ApprovalInfo {
                approver: "sre-oncall@example.com".into(),
                ticket: "CHG-1".into(),
                plan_hash: "sha256:0".into(),
                approved_at: t("2026-09-02T17:40:00Z"),
                key_id: "a".repeat(64),
                self_attested: false,
                console: None,
            },
            phases: vec![],
            measured: Measured {
                rto_seconds: None,
                rto_requested_to_verified_seconds: None,
                rto_restore_only_seconds: None,
                rto_excluding_preflight_seconds: None,
                rpo_seconds: None,
                rpo_source_relative_seconds: None,
                rpo_source_relative_unmeasured_reason: Some(
                    "source cluster never contacted".into(),
                ),
            },
            objectives: Objectives {
                rto_seconds: None,
                rpo_seconds: None,
                pass_rate: None,
                met: None,
            },
            sample: SampleInfo {
                window_start: t("2026-08-29T00:00:00Z"),
                window_end: t("2026-08-30T02:00:00Z"),
                topics: 1,
                partitions: 1,
                records_expected: 0,
                records_restored: 0,
                anchor: "head".into(),
                coverage_note: "no capture gap overlaps the sampled window".into(),
                unsampled_topics: None,
            },
            target_diff: TargetDiffSummary {
                collisions: vec![],
                absent: vec![],
                would_create: vec![],
                level: "full".into(),
                not_assessed: None,
            },
            integrity: Integrity {
                level: IntegrityLevel::ByteFingerprint,
                result: IntegrityResult::Pass,
                partial_reason: None,
                records_sampled: 0,
                records_sampled_matching: 0,
                mismatches: 0,
                pass_rate_measured: None,
                restored_principal_could_consume: None,
                verification: None,
            },
            topic_parity: TopicParity {
                intentionally_deviated: vec![],
                unexpected_divergence: vec![],
                not_assessed: None,
                not_reconstructed: None,
            },
            engine_subreport: None,
            evidence: EvidenceInfo {
                version_id: None,
                retain_until: None,
                immutable: false,
                create_only_enforced: false,
                offset_report_key: None,
                offset_report_sha256: None,
            },
            redactions: vec![],
        }
    }

    #[test]
    fn baseline_is_valid() {
        valid_scorecard()
            .validate_invariants()
            .expect("the test baseline itself must satisfy every invariant");
    }

    // --- the evidence block is zeroed before signing (T0-2) ---------------
    //
    // One test per field, each asserting the SPECIFIC message, so deleting a
    // single clause makes exactly that field's test fail. The field order
    // matches the struct's declaration order and the Python mirror's, so a
    // document violating two fields gets the same message from both readers.

    #[test]
    fn invariants_refuse_non_zeroed_evidence() {
        let mut sc = valid_scorecard();
        sc.evidence.create_only_enforced = true;
        let err = sc
            .validate_invariants()
            .expect_err("a non-zeroed evidence block must be refused");
        assert_eq!(
            err.0,
            "evidence.create_only_enforced is true but the four post-put fields are zeroed before signing"
        );
    }

    #[test]
    fn invariants_refuse_a_set_version_id() {
        let mut sc = valid_scorecard();
        sc.evidence.version_id = Some("3HL4kqtJlcpXroDTDmJ".into());
        let err = sc
            .validate_invariants()
            .expect_err("a set evidence.version_id must be refused");
        assert_eq!(
            err.0,
            "evidence.version_id is set but the four post-put fields are zeroed before signing"
        );
    }

    #[test]
    fn invariants_refuse_a_set_retain_until() {
        let mut sc = valid_scorecard();
        sc.evidence.retain_until = Some(
            DateTime::parse_from_rfc3339("2027-01-01T00:00:00Z")
                .unwrap()
                .with_timezone(&Utc),
        );
        let err = sc
            .validate_invariants()
            .expect_err("a set evidence.retain_until must be refused");
        assert_eq!(
            err.0,
            "evidence.retain_until is set but the four post-put fields are zeroed before signing"
        );
    }

    #[test]
    fn invariants_refuse_a_true_immutable() {
        let mut sc = valid_scorecard();
        sc.evidence.immutable = true;
        let err = sc
            .validate_invariants()
            .expect_err("a true evidence.immutable must be refused");
        assert_eq!(
            err.0,
            "evidence.immutable is true but the four post-put fields are zeroed before signing"
        );
    }

    /// The arm is SCOPED to major 1, and the scope is observable only on a
    /// LOWER major: on a higher one `refuse_unreadable_major` has already
    /// returned, so dropping the guard changes nothing there (brief §7 M8/M9
    /// are equivalent mutants for a 9.9.9 document — this is the test that is
    /// not). A future major may redefine the `evidence` block, and
    /// `docs/verify_scorecard.py` must agree: its mirrored arm carries the same
    /// `doc_major == 1` guard, and
    /// `test_a_lower_major_with_a_non_zeroed_evidence_block_is_accepted` is
    /// this test's other half.
    #[test]
    fn the_evidence_arm_is_scoped_to_major_1_and_does_not_touch_major_0() {
        let mut sc = valid_scorecard();
        sc.format_version = "0.9.9".into();
        sc.evidence.version_id = Some("v-from-another-major".into());
        sc.evidence.immutable = true;
        sc.evidence.create_only_enforced = true;
        sc.validate_invariants()
            .expect("the evidence arm is scoped to major 1 and must not fire on a 0.x document");
    }

    // --- the offset report's key and digest travel together (Task 9b) ----
    //
    // GC12's price for two nested optional fields, this reader's per-arm
    // coverage. The corpus's own README names these unit tests as the only
    // thing that survives a fully coordinated deletion across
    // `docs/verify_scorecard.py`, `e2e/fixtures/invariants/` and the pytest
    // suite — so BOTH directions get a row and each asserts the SPECIFIC
    // message, byte-identically with the Python mirror's.

    #[test]
    fn invariants_refuse_an_offset_report_key_without_its_sha256() {
        let mut sc = valid_scorecard();
        sc.evidence.offset_report_key = Some("logweir/drills/RUN.offsets.json".into());
        let err = sc
            .validate_invariants()
            .expect_err("a key with no digest names bytes nothing binds");
        assert_eq!(
            err.0,
            "evidence.offset_report_key and evidence.offset_report_sha256 are present or absent together; a key with no digest names bytes nothing binds, and a digest with no key binds bytes nobody can fetch"
        );
    }

    #[test]
    fn invariants_refuse_an_offset_report_sha256_without_its_key() {
        let mut sc = valid_scorecard();
        sc.evidence.offset_report_sha256 = Some(format!("sha256:{}", "0".repeat(64)));
        let err = sc
            .validate_invariants()
            .expect_err("a digest with no key binds bytes nobody can fetch");
        assert_eq!(
            err.0,
            "evidence.offset_report_key and evidence.offset_report_sha256 are present or absent together; a key with no digest names bytes nothing binds, and a digest with no key binds bytes nobody can fetch"
        );
    }

    /// Ruling R-A: BLANK counts as ABSENT, on both sides, so the two readers
    /// cannot disagree on `""` — Rust's `is_some()` is true for `Some("")`
    /// while Python's truthiness test is false, and that split is exactly what
    /// T0-6 found in the `partial_reason` arm.
    #[test]
    fn a_blank_offset_report_pair_counts_as_absent() {
        let mut sc = valid_scorecard();
        sc.evidence.offset_report_key = Some("   ".into());
        sc.evidence.offset_report_sha256 = Some(String::new());
        sc.validate_invariants()
            .expect("two blank fields are two ABSENT fields, not two present ones");

        // …and a blank key beside a REAL digest is still the refusal.
        let mut sc = valid_scorecard();
        sc.evidence.offset_report_key = Some("  ".into());
        sc.evidence.offset_report_sha256 = Some(format!("sha256:{}", "a".repeat(64)));
        sc.validate_invariants()
            .expect_err("a blank key beside a real digest is a digest with no key");
    }

    #[test]
    fn invariants_accept_both_offset_report_fields_or_neither() {
        // Neither — every scorecard this tree wrote before Task 9b.
        valid_scorecard()
            .validate_invariants()
            .expect("an absent pair is the state of every document written before it existed");
        // Both — what phase 8 writes when the engine produced a report.
        let mut sc = valid_scorecard();
        sc.evidence.offset_report_key = Some("logweir/drills/RUN.offsets.json".into());
        sc.evidence.offset_report_sha256 = Some(format!("sha256:{}", "b".repeat(64)));
        sc.validate_invariants()
            .expect("both present is what a completed restore records");
    }

    /// The two fields serialise to NOTHING when absent, which is what keeps
    /// the three checked-in signed fixtures under `e2e/fixtures/signed/`
    /// byte-identical — GC12's price, paid without re-minting them.
    #[test]
    fn an_absent_offset_report_adds_no_json_key_at_all() {
        let sc = valid_scorecard();
        let v = serde_json::to_value(&sc).expect("a scorecard serialises");
        let ev = &v["evidence"];
        assert!(
            ev.get("offset_report_key").is_none() && ev.get("offset_report_sha256").is_none(),
            "an absent pair must add no key, not a null one: {ev}"
        );
        // And the four fields that WERE there are still there, so this is a
        // statement about the two new ones and not about the block.
        for name in [
            "version_id",
            "retain_until",
            "immutable",
            "create_only_enforced",
        ] {
            assert!(ev.get(name).is_some(), "{name} must still be present: {ev}");
        }
    }

    // -----------------------------------------------------------------
    // `target.mode` / `target.marker_topic` (fix round 1, review F1)
    // -----------------------------------------------------------------

    /// The arm, in the direction that matters to an auditor: a `scratch`
    /// document with no marker topic claims the phase-0 segregation proof
    /// while omitting the thing the proof was about.
    #[test]
    fn invariants_refuse_a_scratch_document_with_no_marker_topic() {
        let mut sc = valid_scorecard();
        sc.target.marker_topic = None;
        let e = sc
            .validate_invariants()
            .expect_err("scratch with no marker topic is a claim about an unrecorded check");
        assert_eq!(
            e.0,
            "target.marker_topic is absent but target.mode is scratch; the marker topic is the segregation proof phase 0 verified, and a scratch document that omits it claims a check nothing recorded"
        );
        // BLANK COUNTS AS ABSENT — ruling R-A, the same predicate
        // `docs/verify_scorecard.py` applies with `.strip()`.
        for blank in ["", "   "] {
            let mut sc = valid_scorecard();
            sc.target.marker_topic = Some(blank.into());
            sc.validate_invariants()
                .expect_err("a blank marker topic is an absent one, in both readers");
        }
    }

    /// The mode branch's own document: `newTopic` with no marker topic is what
    /// `drill::target_info` writes, and it must verify.
    #[test]
    fn invariants_accept_a_new_topic_document_with_no_marker_topic() {
        let mut sc = valid_scorecard();
        sc.target.mode = TargetMode::NewTopic;
        sc.target.marker_topic = None;
        sc.validate_invariants()
            .expect("a newTopic run has no marker topic to verify, and says so by omission");
        // And a `newTopic` document that DOES name one is still accepted: what
        // this tree writes is narrower than what its readers accept.
        let mut sc = valid_scorecard();
        sc.target.mode = TargetMode::NewTopic;
        sc.validate_invariants()
            .expect("naming an unverified marker topic is coherent, if not what we write");
    }

    /// `mode` serialises to NOTHING for `Scratch`, which is what keeps the
    /// three checked-in signed fixtures under `e2e/fixtures/signed/`
    /// byte-identical — GC12's price, paid without re-minting them.
    #[test]
    fn a_scratch_scorecard_adds_no_mode_key_at_all() {
        let v = serde_json::to_value(valid_scorecard()).expect("a scorecard serialises");
        let t = &v["target"];
        assert!(
            t.get("mode").is_none(),
            "the default mode must add no key, not a `\"scratch\"` one: {t}"
        );
        assert_eq!(
            t.get("marker_topic").and_then(serde_json::Value::as_str),
            Some("logweir.scratch"),
            "and the scratch document still names its marker topic: {t}"
        );
    }

    /// The converse, on the wire: a `newTopic` document CARRIES the mode and
    /// carries no `marker_topic` key at all — not a null one.
    #[test]
    fn a_new_topic_scorecard_writes_its_mode_and_omits_the_marker_topic() {
        let mut sc = valid_scorecard();
        sc.target.mode = TargetMode::NewTopic;
        sc.target.marker_topic = None;
        let v = serde_json::to_value(&sc).expect("a scorecard serialises");
        let t = &v["target"];
        assert_eq!(
            t.get("mode").and_then(serde_json::Value::as_str),
            Some("newTopic"),
            "the wire spelling is the CRD's own enum value: {t}"
        );
        assert!(
            t.get("marker_topic").is_none(),
            "an absent marker topic must add no key, not a null one: {t}"
        );
        // …and it round-trips, so a reader gets back what was signed.
        let back: Scorecard = serde_json::from_value(v).expect("a scorecard deserialises");
        assert_eq!(back.target.mode, TargetMode::NewTopic);
        assert_eq!(back.target.marker_topic, None);
    }

    /// A document written before either field existed — no `mode`, a
    /// `marker_topic` — still means exactly what it meant.
    #[test]
    fn a_document_with_no_mode_key_reads_as_scratch() {
        let mut v = serde_json::to_value(valid_scorecard()).expect("serialises");
        v["target"]
            .as_object_mut()
            .expect("target is an object")
            .remove("mode");
        let back: Scorecard = serde_json::from_value(v).expect("a scorecard deserialises");
        assert!(back.target.mode.is_scratch());
        back.validate_invariants()
            .expect("every document v0.1 wrote is a scratch document and still verifies");
    }

    #[test]
    fn invariants_accept_zeroed_evidence() {
        let mut sc = valid_scorecard();
        sc.evidence.version_id = None;
        sc.evidence.retain_until = None;
        sc.evidence.immutable = false;
        sc.evidence.create_only_enforced = false;
        sc.validate_invariants()
            .expect("a fully zeroed evidence block is what every Logweir scorecard carries");
    }

    // --- T0-6: `partial_reason` must be non-BLANK, not merely non-null ----
    //
    // The message text below is byte-identical to
    // `docs/verify_scorecard.py::check_invariants`'s. That is not a
    // coincidence and must not be "improved": the two readers are documented
    // as reaching the same verdict with the same words, and
    // `crates/logweir/tests/two_reader_parity.rs` compares the two strings.

    #[test]
    fn invariants_refuse_null_partial_reason() {
        // The case the arm ALWAYS refused. Kept alongside the two new ones so
        // a predicate that stopped handling `None` — say
        // `as_deref().unwrap_or("x")` — cannot slip through while the blank
        // cases still pass.
        let mut sc = valid_scorecard();
        sc.integrity.result = IntegrityResult::Partial;
        sc.integrity.partial_reason = None;
        let err = sc
            .validate_invariants()
            .expect_err("a partial result with no reason must be refused");
        assert_eq!(
            err.0,
            "integrity.result is 'partial' but partial_reason is null"
        );
    }

    #[test]
    fn invariants_refuse_whitespace_partial_reason() {
        let mut sc = valid_scorecard();
        sc.integrity.result = IntegrityResult::Partial;
        sc.integrity.partial_reason = Some("   ".into());
        let err = sc
            .validate_invariants()
            .expect_err("a whitespace-only partial_reason says nothing and must be refused");
        assert_eq!(
            err.0,
            "integrity.result is 'partial' but partial_reason is null"
        );
    }

    #[test]
    fn invariants_refuse_empty_string_partial_reason() {
        let mut sc = valid_scorecard();
        sc.integrity.result = IntegrityResult::Partial;
        sc.integrity.partial_reason = Some(String::new());
        let err = sc
            .validate_invariants()
            .expect_err("an empty partial_reason says nothing and must be refused");
        assert_eq!(
            err.0,
            "integrity.result is 'partial' but partial_reason is null"
        );
    }

    #[test]
    fn invariants_accept_a_real_partial_reason() {
        // The control for the three above: the arm narrows the accepted set,
        // it does not close it. A document that says WHY it is partial is
        // exactly what the format asks for.
        //
        // T0-4: the other three fields are what a `partial` integrity result
        // ACTUALLY travels with in a document Logweir can produce.
        // `crate::drill::phase8_score::decide` maps a non-`pass` integrity
        // result to `fail-integrity`, and `matrix_verdict_for` then returns
        // `fail` carrying that sentence as its reason. Until the outcome arms
        // existed this control left `outcome: Pass` standing beside
        // `result: Partial` — a document no writer emits — and both readers
        // accepted it.
        let mut sc = valid_scorecard();
        sc.outcome = Outcome::FailIntegrity;
        sc.integrity.result = IntegrityResult::Partial;
        sc.integrity.partial_reason = Some("only 2 of 3 partitions reached a conclusion".into());
        sc.engine.matrix_verdict = MatrixVerdict::Fail;
        sc.engine.matrix_verdict_reason =
            Some("the drill ran and did not pass: outcome fail-integrity".into());
        sc.validate_invariants()
            .expect("a partial result that names its reason is a valid document");
    }

    // --- Global Constraint 18(a), true branch -----------------------------

    #[test]
    fn captured_by_logweir_true_rejects_last_phase_completed_below_neg1() {
        let mut sc = valid_scorecard();
        sc.source.captured_by_logweir = true;
        sc.last_phase_completed = -2;
        // Needed so the true-branch's OTHER checks would pass if reached —
        // isolates the failure to the last_phase_completed arm specifically.
        sc.measured.rpo_source_relative_seconds = Some(0);
        sc.measured.rpo_source_relative_unmeasured_reason = None;
        let err = sc
            .validate_invariants()
            .expect_err("last_phase_completed below -1 must be rejected");
        assert_eq!(
            err.0,
            "source.captured_by_logweir is true but last_phase_completed is below -1"
        );
    }

    #[test]
    fn captured_by_logweir_true_requires_measured_source_relative_rpo() {
        let mut sc = valid_scorecard();
        sc.source.captured_by_logweir = true;
        sc.last_phase_completed = 9;
        sc.measured.rpo_source_relative_seconds = None;
        sc.measured.rpo_source_relative_unmeasured_reason = None;
        let err = sc.validate_invariants().expect_err(
            "a null rpo_source_relative_seconds must be rejected when captured_by_logweir is true",
        );
        assert_eq!(
            err.0,
            "source.captured_by_logweir is true but rpo_source_relative_seconds is null"
        );
    }

    #[test]
    fn captured_by_logweir_true_rejects_a_lingering_unmeasured_reason() {
        let mut sc = valid_scorecard();
        sc.source.captured_by_logweir = true;
        sc.last_phase_completed = 9;
        sc.measured.rpo_source_relative_seconds = Some(0);
        sc.measured.rpo_source_relative_unmeasured_reason = Some("stale reason".into());
        let err = sc.validate_invariants().expect_err(
            "a non-null unmeasured reason must be rejected when captured_by_logweir is true",
        );
        assert_eq!(
            err.0,
            "source.captured_by_logweir is true but an unmeasured reason is present"
        );
    }

    // --- Global Constraint 18(a), false branch ------------------------------

    #[test]
    fn captured_by_logweir_false_rejects_a_source_relative_rpo() {
        let mut sc = valid_scorecard();
        sc.source.captured_by_logweir = false;
        sc.measured.rpo_source_relative_seconds = Some(0);
        // Left as Some(...) so, if the seconds arm were deleted, the reason
        // arm below would not incidentally catch this case too.
        sc.measured.rpo_source_relative_unmeasured_reason =
            Some("source cluster never contacted".into());
        let err = sc
            .validate_invariants()
            .expect_err("a source-relative RPO with the source never contacted must be rejected");
        assert_eq!(
            err.0,
            "rpo_source_relative_seconds is set but the source was never contacted"
        );
    }

    #[test]
    fn captured_by_logweir_false_requires_an_unmeasured_reason() {
        let mut sc = valid_scorecard();
        sc.source.captured_by_logweir = false;
        sc.measured.rpo_source_relative_seconds = None;
        sc.measured.rpo_source_relative_unmeasured_reason = None;
        let err = sc.validate_invariants().expect_err(
            "a null unmeasured reason must be rejected when captured_by_logweir is false",
        );
        assert_eq!(
            err.0,
            "source.captured_by_logweir is false but rpo_source_relative_unmeasured_reason is null"
        );
    }

    // --- Non-negativity of the format's three signed integers ---------------
    //
    // One test per field, each asserting the SPECIFIC message, so collapsing
    // the loop to cover fewer fields makes exactly the dropped field's test
    // fail.

    #[test]
    fn measured_rpo_seconds_may_not_be_negative() {
        let mut sc = valid_scorecard();
        sc.measured.rpo_seconds = Some(-90);
        let err = sc
            .validate_invariants()
            .expect_err("a negative archive-coverage gap must be refused");
        assert_eq!(
            err.0,
            "measured.rpo_seconds is negative (-90); a recovery-point gap of less than zero \
             is not a smaller gap, it is a meaningless one"
        );
    }

    /// Null in v0.1 and written by no code yet — which is exactly why the rule
    /// is here rather than at a call site: `--from-cluster` inherits it.
    #[test]
    fn measured_rpo_source_relative_seconds_may_not_be_negative() {
        let mut sc = valid_scorecard();
        // The Global Constraint 18(a) true-branch, so this test trips the
        // non-negativity arm and not the captured_by_logweir arms.
        sc.source.captured_by_logweir = true;
        sc.measured.rpo_source_relative_seconds = Some(-1);
        sc.measured.rpo_source_relative_unmeasured_reason = None;
        let err = sc
            .validate_invariants()
            .expect_err("a negative source-relative RPO must be refused");
        assert_eq!(
            err.0,
            "measured.rpo_source_relative_seconds is negative (-1); a recovery-point gap of \
             less than zero is not a smaller gap, it is a meaningless one"
        );
    }

    /// This one is adopter input travelling straight from YAML into a signed
    /// document, validated by nothing else anywhere.
    #[test]
    fn objectives_rpo_seconds_may_not_be_negative() {
        let mut sc = valid_scorecard();
        sc.objectives.rpo_seconds = Some(-300);
        let err = sc
            .validate_invariants()
            .expect_err("a negative REQUESTED rpo objective must be refused");
        assert_eq!(
            err.0,
            "objectives.rpo_seconds is negative (-300); a recovery-point gap of less than zero \
             is not a smaller gap, it is a meaningless one"
        );
    }

    #[test]
    fn a_zero_gap_is_valid_the_rule_refuses_only_negatives() {
        let mut sc = valid_scorecard();
        sc.measured.rpo_seconds = Some(0);
        sc.objectives.rpo_seconds = Some(0);
        sc.validate_invariants()
            .expect("zero is a legitimate gap — the archive covers the requested point");
    }

    // --- Finiteness of the format's two float fields ------------------------

    #[test]
    fn objectives_pass_rate_must_be_finite() {
        let mut sc = valid_scorecard();
        sc.objectives.pass_rate = Some(f64::NAN);
        let err = sc.validate_invariants().expect_err(
            "a NaN objectives.pass_rate must be rejected before it can be signed as a bare null",
        );
        assert_eq!(err.0, "objectives.pass_rate is not finite (NaN or +/-Inf)");
    }

    #[test]
    fn integrity_pass_rate_measured_must_be_finite() {
        let mut sc = valid_scorecard();
        // records_sampled_matching / records_sampled with records_sampled == 0
        // is exactly the reachable trigger: a drill that samples zero records.
        sc.integrity.pass_rate_measured = Some(f64::NAN);
        let err = sc.validate_invariants().expect_err(
            "a NaN integrity.pass_rate_measured must be rejected before it can be signed as a bare null",
        );
        assert_eq!(
            err.0,
            "integrity.pass_rate_measured is not finite (NaN or +/-Inf)"
        );
    }

    // --- Global Constraint 12: refuse a higher-major format_version --------

    #[test]
    fn format_version_with_a_higher_major_is_refused() {
        let mut sc = valid_scorecard();
        sc.format_version = "9.9.9".into();
        // Deliberately ALSO violating the T0-2 evidence arm. The evidence arm
        // is scoped to the known majors (1, and 2 since PROD-11.1b), so on this
        // document it is skipped whichever
        // side of `refuse_unreadable_major` it sits on — the two orders are
        // behaviourally identical and the reorder alone is an equivalent
        // mutant (brief §7 M8). What this line does kill is the COMBINED
        // mutant "drop the `Some(1)` scope guard AND move the arm first",
        // which would return the evidence message here instead of the
        // major-version one.
        sc.evidence.create_only_enforced = true;
        // Task 4 addendum A4: a non-empty `redactions` makes the POSITION of
        // the redactions arm load-bearing here. Unlike the evidence arm it
        // carries no major scoping, so moving it above
        // `refuse_unreadable_major()?;` makes this document return the
        // redactions message and the `assert_eq!` below fails at assertion
        // time (brief §7 M7). Without this line that mutant survives.
        sc.redactions = vec![Redaction {
            path: "/target/cluster_id".into(),
            reason: "addendum A4 mutant surface".into(),
            present: false,
        }];
        let err = sc
            .validate_invariants()
            .expect_err("a format_version from a future major must be refused");
        assert_eq!(
            err.0,
            format!(
                "format_version 9.9.9 has a major version newer than this reader understands \
                 (this build knows {})",
                FORMAT_VERSION_WITH_PARTITION_SUBSETS
            )
        );
    }

    #[test]
    fn format_version_with_a_lower_or_equal_major_is_accepted() {
        let mut sc = valid_scorecard();
        sc.format_version = "1.9.9".into();
        sc.validate_invariants()
            .expect("a same-major minor/patch bump must not be refused");
        sc.format_version = "0.9.9".into();
        sc.validate_invariants()
            .expect("an older major must not be refused by this check");
    }

    #[test]
    fn format_version_that_does_not_parse_is_refused() {
        let mut sc = valid_scorecard();
        sc.format_version = "not-a-semver".into();
        let err = sc
            .validate_invariants()
            .expect_err("an unparseable format_version must be refused, not treated as major 0");
        assert_eq!(
            err.0,
            "format_version \"not-a-semver\" is not a parseable semver"
        );
    }

    // --- T0-3: `redactions` is documented as always `[]` in v0.1 ----------

    #[test]
    fn invariants_refuse_non_empty_redactions() {
        let mut sc = valid_scorecard();
        sc.redactions = vec![Redaction {
            path: "/measured/rpo_seconds".into(),
            reason: "customer policy".into(),
            present: false,
        }];
        let err = sc
            .validate_invariants()
            .expect_err("v0.1 has no writer that can produce a redaction");
        assert_eq!(
            err.0,
            format!(
                "redactions is non-empty but format_version {} has no way to produce one; \
                 --redact is a v0.1.1 feature",
                crate::FORMAT_VERSION
            )
        );
    }

    #[test]
    fn the_redactions_message_names_the_documents_own_format_version() {
        // The message interpolates the DOCUMENT's version, so a 1.0.1 document
        // reads correctly rather than being told about a version it does not
        // claim. `1.0.1` is a same-major minor, so `refuse_unreadable_major`
        // lets it through to this arm.
        let mut sc = valid_scorecard();
        sc.format_version = "1.0.1".into();
        sc.redactions = vec![Redaction {
            path: "/target/cluster_id".into(),
            reason: "customer policy".into(),
            present: true,
        }];
        let err = sc
            .validate_invariants()
            .expect_err("a same-major minor reaches the redactions arm");
        assert_eq!(
            err.0,
            "redactions is non-empty but format_version 1.0.1 has no way to produce one; \
             --redact is a v0.1.1 feature"
        );
    }

    #[test]
    fn invariants_accept_empty_redactions() {
        // The control: without it, an arm that refused every document would
        // make the test above pass for the wrong reason.
        assert!(valid_scorecard().validate_invariants().is_ok());
    }

    // --- Two pre-existing arms that `uncovered-arms.json` records as carried
    // by unit tests in BOTH readers. Fix round 1, F1: each was carried by a
    // pytest case only — neither had a Rust unit test, so that file's own
    // coverage claim was a written guarantee no code delivered, in the one
    // file whose job is honest coverage accounting.
    //
    // The corpus walker cannot stand in for these.
    // `every_invariant_arm_has_a_corpus_case` counts `return Err(...)`
    // STATEMENTS and asserts `covered + uncovered == n`, so deleting an arm
    // together with its entry drops both sides by one and the arithmetic
    // re-balances silently. The per-arm unit test is the only thing that kills
    // that mutant.

    #[test]
    fn invariants_refuse_a_met_objective_at_a_reduced_integrity_level() {
        // `met: true` is a claim about a pass rate, and below byte-fingerprint
        // level there is no measured rate to support it, so `null` is the
        // honest value. Python sibling:
        // `docs/test_verify_scorecard.py::test_met_true_is_refused_when_the_pass_rate_was_not_measurable`.
        let mut sc = valid_scorecard();
        sc.integrity.level = IntegrityLevel::ConsumeOnly;
        sc.objectives.pass_rate = Some(1.0);
        sc.objectives.met = Some(true);
        let err = sc
            .validate_invariants()
            .expect_err("a met objective needs a measurable pass rate");
        assert_eq!(
            err.0,
            "objectives.met must be null when pass_rate is not measurable"
        );
    }

    #[test]
    fn invariants_refuse_more_matching_records_than_were_sampled() {
        // The base direction of the sampling pair: Task 5 added the CONVERSE
        // (`matching != sampled` under a `pass`) and gave it a corpus case,
        // but this older arm had no Rust test at all. `records_expected` moves
        // with `records_sampled` so the coverage arm cannot be the reason the
        // document is refused. Python sibling:
        // `docs/test_verify_scorecard.py::test_more_matching_records_than_sampled_is_refused`.
        let mut sc = valid_scorecard();
        sc.integrity.records_sampled = 50;
        sc.integrity.records_sampled_matching = 100;
        sc.sample.records_expected = 50;
        let err = sc
            .validate_invariants()
            .expect_err("more records matched than were ever sampled");
        assert_eq!(err.0, "records_sampled_matching exceeds records_sampled");
    }

    // --- Task 5c: the three arms that were RUST-BARE ------------------------
    //
    // Task 5's re-review measured each of the three messages below occurring
    // exactly ONCE in this crate — the arm itself — with only a pytest case
    // behind it. That is the same defect the two tests above closed, and it is
    // not one the corpus walker can close for us:
    // `every_invariant_arm_has_a_corpus_case` counts `return Err(...)`
    // STATEMENTS and asserts `covered + uncovered == n`, so deleting an arm
    // together with its `uncovered-arms.json` entry (or flipping that entry to
    // `occurrences: 0`) drops both sides by one and the arithmetic re-balances
    // in silence. Measured under mutation, the walker, the corpus shell gate
    // and pytest all stayed green on such a deletion; the per-arm assertion on
    // the exact refusal text is the only thing that kills it. Each of the
    // three now also has a corpus case, which is what turns a SILENT deletion
    // into a walker-visible two-reader disagreement — the two protections
    // answer different mutants and neither replaces the other.

    #[test]
    fn invariants_refuse_a_matrix_fail_without_a_reason() {
        // A `fail` matrix verdict that does not say why is the one shape of
        // matrix verdict an auditor cannot act on. One override: the baseline
        // already carries `matrix_verdict_reason: None`, which is correct
        // beside a `pass` and incoherent beside a `fail`. Python sibling:
        // `docs/test_verify_scorecard.py::test_a_matrix_fail_without_a_reason_is_refused`.
        let mut sc = valid_scorecard();
        sc.engine.matrix_verdict = MatrixVerdict::Fail;
        let err = sc
            .validate_invariants()
            .expect_err("a `fail` matrix verdict must name its reason");
        assert_eq!(
            err.0,
            "engine.matrix_verdict is 'fail' but matrix_verdict_reason is null"
        );
    }

    #[test]
    fn invariants_refuse_a_pass_rate_measured_without_byte_fingerprint() {
        // A measured pass rate is a byte-fingerprint result; below that level
        // there is nothing to measure it from, so a number there is a claim
        // the drill was not in a position to make.
        //
        // `matrix_verdict` moves to `pass-degraded` because the matrix arm
        // sits EARLIER in `validate_invariants` and would otherwise fire
        // first: a `pass` matrix verdict at a reduced integrity level is
        // exactly what that arm refuses. `pass-degraded` is the honest value
        // for this document, not a weakening — the same override the corpus
        // case `pass_rate_measured_without_byte_fingerprint` carries, and the
        // same one Task 5 had to make to keep the pytest case isolated.
        // Python sibling:
        // `docs/test_verify_scorecard.py::test_a_pass_rate_measured_without_byte_fingerprint_is_refused`.
        let mut sc = valid_scorecard();
        sc.integrity.level = IntegrityLevel::ConsumeOnly;
        sc.integrity.pass_rate_measured = Some(1.0);
        sc.engine.matrix_verdict = MatrixVerdict::PassDegraded;
        let err = sc
            .validate_invariants()
            .expect_err("a measured pass rate below byte-fingerprint level has no source");
        assert_eq!(
            err.0,
            "integrity.pass_rate_measured is set but the level is not byte-fingerprint"
        );
    }

    #[test]
    fn invariants_refuse_a_last_phase_completed_outside_the_domain() {
        // Global Constraint 18: ELEVEN phase slots, -1 through 9. Both ends
        // are exercised, so an arm narrowed to one side of the range — say
        // `last_phase_completed > 9` alone — fails here rather than passing on
        // the half it kept. Python sibling:
        // `docs/test_verify_scorecard.py::test_a_last_phase_completed_outside_the_domain_is_refused`.
        //
        // `captured_by_logweir` stays FALSE, so the true-branch arm
        // `last_phase_completed is below -1` is never reached and cannot steal
        // the `-2` case's failure.
        for outside in [-2, 10, 42] {
            let mut sc = valid_scorecard();
            sc.last_phase_completed = outside;
            let err = sc.validate_invariants().expect_err(
                "a last_phase_completed outside the eleven phase slots must be refused",
            );
            assert_eq!(err.0, "last_phase_completed outside -1..=9", "at {outside}");
        }
    }

    #[test]
    fn invariants_accept_every_phase_slot_including_both_ends() {
        // The control for the test above: the domain is CLOSED at both ends,
        // so an arm written `-1..9` (exclusive) or `0..=9` refuses a real
        // document and this test says so.
        for inside in [-1, 0, 5, 9] {
            let mut sc = valid_scorecard();
            sc.last_phase_completed = inside;
            assert!(
                sc.validate_invariants().is_ok(),
                "last_phase_completed {inside} is one of the eleven slots"
            );
        }
    }

    // --- T0-4: `outcome` is a claim entailed by the rest of the document ---
    //
    // One test per arm, each asserting the SPECIFIC message, so deleting a
    // single arm makes exactly one test fail. Every case moves ONLY the fields
    // that trip its own arm off `valid_scorecard()`, and each is mirrored by a
    // document in `e2e/fixtures/invariants/` so
    // `crates/logweir/tests/two_reader_parity.rs` decides the same case with
    // BOTH readers.

    #[test]
    fn invariants_refuse_pass_with_partial_integrity() {
        let mut sc = valid_scorecard();
        // Both fields move: with `partial_reason` left null the pre-existing
        // `Partial => partial_reason` arm above would fire first and steal the
        // failure, and this test would pass while testing that arm instead.
        sc.integrity.result = IntegrityResult::Partial;
        sc.integrity.partial_reason = Some("orders/7 never reconciled".into());
        let err = sc
            .validate_invariants()
            .expect_err("a `pass` cannot sit beside a non-`pass` integrity result");
        assert_eq!(
            err.0,
            "outcome is 'pass' but integrity.result is not 'pass'"
        );
    }

    #[test]
    fn invariants_refuse_pass_with_partial_reason() {
        // T0-4's first documented "before" case, verbatim: this document made
        // `drill verify` exit 0 under both readers.
        let mut sc = valid_scorecard();
        sc.integrity.partial_reason = Some("orders/7 never reconciled".into());
        let err = sc
            .validate_invariants()
            .expect_err("a named unreconciled topic contradicts a `pass`");
        assert_eq!(
            err.0,
            "outcome is 'pass' but integrity.partial_reason is present"
        );
    }

    #[test]
    fn a_pass_with_a_blank_partial_reason_is_still_accepted() {
        // Ruling R-A's trimmed-empty predicate, reused verbatim by the arm
        // above: `""` and `"   "` count as ABSENT in both readers, so a blank
        // reason is not the contradiction a named topic is. Dropping `.trim()`
        // from the new arm makes this test fail.
        for blank in ["", "   ", "\t\n "] {
            let mut sc = valid_scorecard();
            sc.integrity.partial_reason = Some(blank.into());
            assert!(
                sc.validate_invariants().is_ok(),
                "a blank partial_reason ({blank:?}) is absent, not a contradiction"
            );
        }
    }

    #[test]
    fn invariants_refuse_pass_with_unmet_objective() {
        let mut sc = valid_scorecard();
        sc.objectives.met = Some(false);
        let err = sc
            .validate_invariants()
            .expect_err("a missed objective contradicts a `pass`");
        assert_eq!(err.0, "outcome is 'pass' but objectives.met is false");
    }

    #[test]
    fn a_pass_may_carry_a_null_or_true_objectives_met() {
        // Only an explicit `false` contradicts the outcome. `null` is the
        // legitimate value when no objective was requested or the pass rate was
        // not measurable, and `true` is the ordinary case — an arm written as
        // `met != Some(true)` refuses the first of those and this test says so.
        let mut sc = valid_scorecard();
        sc.objectives.met = None;
        assert!(sc.validate_invariants().is_ok(), "met: null is legal");
        let mut sc = valid_scorecard();
        sc.objectives.met = Some(true);
        assert!(sc.validate_invariants().is_ok(), "met: true is legal");
    }

    #[test]
    fn invariants_refuse_pass_with_incomplete_sample() {
        // T0-4's second documented "before" case. `records_expected` is raised
        // to 100 so the coverage arm below cannot fire first.
        let mut sc = valid_scorecard();
        sc.integrity.records_sampled = 100;
        sc.integrity.records_sampled_matching = 50;
        sc.sample.records_expected = 100;
        let err = sc
            .validate_invariants()
            .expect_err("half the sample not matching contradicts a `pass`");
        assert_eq!(
            err.0,
            "outcome is 'pass' but only 50 of 100 sampled records matched"
        );
    }

    #[test]
    fn invariants_refuse_sample_exceeding_expected() {
        let mut sc = valid_scorecard();
        sc.integrity.records_sampled = 100;
        sc.integrity.records_sampled_matching = 100;
        sc.sample.records_expected = 75;
        let err = sc
            .validate_invariants()
            .expect_err("reconciling more records than were selected is incoherent");
        assert_eq!(
            err.0,
            "records_sampled (100) exceeds sample.records_expected (75)"
        );
    }

    #[test]
    fn invariants_refuse_pass_matrix_without_byte_fingerprint() {
        // `outcome` is moved off `Pass` and `result` off `Pass` so the two
        // arms above cannot fire first; `matrix_verdict` is left `Pass`.
        let mut sc = valid_scorecard();
        sc.integrity.level = IntegrityLevel::ConsumeOnly;
        sc.outcome = Outcome::FailIntegrity;
        sc.integrity.result = IntegrityResult::Fail;
        let err = sc
            .validate_invariants()
            .expect_err("a matrix `pass` claims a drill that passed at byte-fingerprint level");
        assert_eq!(
            err.0,
            "engine.matrix_verdict is 'pass' but the drill did not pass at byte-fingerprint level"
        );
    }

    #[test]
    fn invariants_refuse_a_matrix_pass_on_a_degraded_pass() {
        // The `level == ByteFingerprint` conjunct, where it is the ONLY thing
        // that fires: a real pass at a reduced integrity level, whose correct
        // matrix value is `pass-degraded`. Dropping that conjunct from the arm
        // leaves the test above green and this one failing.
        let mut sc = valid_scorecard();
        sc.integrity.level = IntegrityLevel::ConsumeOnly;
        let err = sc
            .validate_invariants()
            .expect_err("a pass at consume-only level is `pass-degraded`, not `pass`");
        assert_eq!(
            err.0,
            "engine.matrix_verdict is 'pass' but the drill did not pass at byte-fingerprint level"
        );
    }

    #[test]
    fn invariants_refuse_a_matrix_pass_on_a_non_pass_at_byte_fingerprint() {
        // The `outcome == Pass` conjunct, where it is the ONLY thing that
        // fires: byte-fingerprint level, but the drill did not pass. Dropping
        // that conjunct leaves the two tests above green and this one failing.
        let mut sc = valid_scorecard();
        sc.outcome = Outcome::FailIntegrity;
        sc.integrity.result = IntegrityResult::Fail;
        let err = sc
            .validate_invariants()
            .expect_err("a matrix `pass` cannot stand beside a drill that did not pass");
        assert_eq!(
            err.0,
            "engine.matrix_verdict is 'pass' but the drill did not pass at byte-fingerprint level"
        );
    }

    #[test]
    fn the_integrity_result_arm_reports_before_the_matrix_arm() {
        // ORDER is part of the contract: `docs/verify_scorecard.py`'s docstring
        // claims the two readers mirror each other "ARM FOR ARM, IN ORDER", and
        // `crates/logweir/tests/two_reader_parity.rs` compares refusal TEXT, so
        // a reordering in either reader is a parity failure rather than a
        // cosmetic one. This document violates the first new arm and the last
        // one at once and must report the first.
        let mut sc = valid_scorecard();
        sc.integrity.result = IntegrityResult::Fail;
        sc.integrity.level = IntegrityLevel::ConsumeOnly;
        let err = sc
            .validate_invariants()
            .expect_err("a `pass` beside a failed integrity result is refused");
        assert_eq!(
            err.0,
            "outcome is 'pass' but integrity.result is not 'pass'"
        );
    }

    #[test]
    fn invariants_accept_a_coherent_pass() {
        // The control for the six arms above: without it, an arm written
        // backwards would refuse every document and every test above would
        // pass for the wrong reason.
        let sc = valid_scorecard();
        assert_eq!(sc.outcome, Outcome::Pass, "the control really is a `pass`");
        assert!(sc.validate_invariants().is_ok());
    }

    // --- FX-3: `topic_parity.not_reconstructed` (format 1.2.0) -------------
    //
    // One test per arm, each asserting the EXACT message, plus the accept
    // controls and the order row. `docs/verify_scorecard.py` mirrors the three
    // arms in the same position and words; the corpus cases under
    // `e2e/fixtures/invariants/` hold both readers to that.

    /// A `newTopic` document as phase 7 writes it from 1.2.0: the four kinds
    /// leave `intentionally_deviated`, and each is in `unexpected_divergence`
    /// AND in `not_reconstructed`.
    fn new_topic_not_reconstructed() -> Scorecard {
        let mut sc = valid_scorecard();
        sc.format_version = crate::FORMAT_VERSION.to_string();
        sc.target.mode = TargetMode::NewTopic;
        sc.target.marker_topic = None;
        let moved = [
            "restore-x-orders: cleanup.policy",
            "restore-x-orders: replication_factor",
            "restore-x-orders: retention.ms",
        ];
        sc.topic_parity.unexpected_divergence = moved.iter().map(|e| e.to_string()).collect();
        sc.topic_parity.not_reconstructed = Some(moved.iter().map(|e| e.to_string()).collect());
        sc.topic_parity.not_assessed = Some(vec![]);
        sc
    }

    #[test]
    fn invariants_accept_a_new_topic_document_whose_not_reconstructed_entries_are_unexpected() {
        new_topic_not_reconstructed()
            .validate_invariants()
            .expect("phase 7's 1.2.0 newTopic shape is coherent");
        // And a scratch document with the claim `[]` beside its intended
        // deviations: what phase 7 writes for every scratch drill.
        let mut sc = valid_scorecard();
        sc.format_version = crate::FORMAT_VERSION.to_string();
        sc.topic_parity.intentionally_deviated = vec!["drill-orders: cleanup.policy".into()];
        sc.topic_parity.not_reconstructed = Some(vec![]);
        sc.validate_invariants()
            .expect("a scratch drill's deviations are intended and nothing is not reconstructed");
    }

    /// ABSENT is legal at every version, and a document without the block is
    /// decided exactly as before — including a pre-1.2.0 `newTopic` document
    /// whose `intentionally_deviated` carries the scratch rationale's labels.
    #[test]
    fn invariants_accept_an_absent_not_reconstructed_at_every_version() {
        for version in ["1.0.0", "1.1.0", crate::FORMAT_VERSION] {
            let mut sc = valid_scorecard();
            sc.format_version = version.to_string();
            sc.target.mode = TargetMode::NewTopic;
            sc.target.marker_topic = None;
            sc.topic_parity.intentionally_deviated = vec![
                "restore-x-orders: cleanup.policy".into(),
                "restore-x-orders: replication_factor".into(),
            ];
            assert!(sc.topic_parity.not_reconstructed.is_none());
            sc.validate_invariants()
                .unwrap_or_else(|e| panic!("{version}: an absent block is NOT RECORDED: {e}"));
        }
    }

    #[test]
    fn invariants_refuse_not_reconstructed_under_a_version_that_predates_it() {
        for version in ["1.1.0", "1.0.0", "0.9.9", "1", "1.x.0"] {
            let mut sc = new_topic_not_reconstructed();
            sc.format_version = version.to_string();
            let err = sc
                .validate_invariants()
                .expect_err("a version before 1.2.0 cannot carry the 1.2.0 field");
            assert_eq!(
                err.0,
                format!(
                    "topic_parity.not_reconstructed is present but format_version {version:?} \
                     predates it: the field is defined from 1.{NOT_RECONSTRUCTED_SINCE_MINOR}.0"
                ),
                "{version}"
            );
        }
        // Even the claim `[]` is a 1.2.0 claim.
        let mut sc = valid_scorecard();
        sc.format_version = "1.1.0".into();
        sc.topic_parity.not_reconstructed = Some(vec![]);
        assert!(sc.validate_invariants().is_err());
        // The boundary itself, and a later minor, are accepted.
        for version in ["1.2.0", "1.2", "1.3.0", "1.12.7"] {
            let mut sc = new_topic_not_reconstructed();
            sc.format_version = version.to_string();
            sc.validate_invariants()
                .unwrap_or_else(|e| panic!("{version} defines the field: {e}"));
        }
    }

    #[test]
    fn invariants_refuse_a_not_reconstructed_entry_missing_from_unexpected_divergence() {
        // THE "dropped instead of moved" document: the deviation left
        // `intentionally_deviated` and reached only the new field, so a reader
        // older than 1.2.0 would see nothing at all for it.
        let mut sc = new_topic_not_reconstructed();
        sc.topic_parity
            .unexpected_divergence
            .retain(|e| e != "restore-x-orders: replication_factor");
        let err = sc
            .validate_invariants()
            .expect_err("a not-reconstructed setting must stay visible to an older reader");
        assert_eq!(
            err.0,
            "topic_parity.not_reconstructed names a deviation that unexpected_divergence does \
             not; a source setting the restore did not reconstruct is also an unexpected \
             divergence, so a reader that predates not_reconstructed never reads it as parity"
        );
    }

    #[test]
    fn invariants_refuse_a_not_reconstructed_entry_that_is_also_intended() {
        let mut sc = new_topic_not_reconstructed();
        sc.topic_parity.intentionally_deviated = vec!["restore-x-orders: cleanup.policy".into()];
        let err = sc
            .validate_invariants()
            .expect_err("intended and not reconstructed contradict each other");
        assert_eq!(
            err.0,
            "topic_parity.not_reconstructed names a deviation that intentionally_deviated also \
             names; a source setting the restore did not reconstruct is never an intended \
             deviation"
        );
    }

    /// NR-4 (FX-3 review F1): the document a writer signs when the mode is
    /// lost on its way to phase 7: the scratch labels beside the claim `[]`.
    /// Without NR-4 it was VALID under both readers and printed no
    /// reconstruction line, a stronger claim than the pre-1.2.0 defect.
    #[test]
    fn invariants_refuse_a_new_topic_document_that_labels_its_deviations_intended() {
        let mut sc = new_topic_not_reconstructed();
        sc.topic_parity.intentionally_deviated = vec![
            "restore-x-orders: cleanup.policy".into(),
            "restore-x-orders: replication_factor".into(),
        ];
        sc.topic_parity.unexpected_divergence.clear();
        sc.topic_parity.not_reconstructed = Some(vec![]);
        let err = sc
            .validate_invariants()
            .expect_err("a newTopic restore labels nothing intended");
        assert_eq!(
            err.0,
            "topic_parity.intentionally_deviated is not empty in a newTopic document that carries \
             not_reconstructed; a newTopic restore's deviations are source settings it did not \
             reconstruct, never intended ones"
        );
        // Scoped: the same lists in a SCRATCH drill are what phase 7 writes,
        // and a newTopic document WITHOUT the block predates the claim.
        let mut scratch = sc.clone();
        scratch.target.mode = TargetMode::Scratch;
        scratch.target.marker_topic = Some("logweir.scratch".into());
        scratch
            .validate_invariants()
            .expect("a scratch drill's deviations are intended");
        let mut absent = sc.clone();
        absent.topic_parity.not_reconstructed = None;
        absent
            .validate_invariants()
            .expect("absent is not recorded: decided as before");
    }

    /// NR-5 (FX-3 review F1, the converse of NR-2): a `newTopic` document's
    /// deviation on a setting the restore decides is one it did not
    /// reconstruct, so `not_reconstructed: []`, or a list missing it, beside
    /// it is refused. Any other key, and FX-4's fail-safe marker, stays a
    /// plain unexpected divergence.
    #[test]
    fn invariants_refuse_a_decided_setting_in_unexpected_divergence_that_not_reconstructed_omits() {
        let message = "topic_parity.unexpected_divergence names a setting the restore decides \
                       (cleanup.policy, retention.ms, partition_count or replication_factor) \
                       that not_reconstructed does not, in a newTopic document; such a \
                       deviation is a source setting the restore did not reconstruct";
        let mut empty = new_topic_not_reconstructed();
        empty.topic_parity.not_reconstructed = Some(vec![]);
        assert_eq!(
            empty.validate_invariants().expect_err("[] beside it").0,
            message
        );
        for omitted in RESTORE_DECIDED_SETTINGS {
            let mut sc = new_topic_not_reconstructed();
            let entry = format!("restore-x-orders: {omitted}");
            if !sc.topic_parity.unexpected_divergence.contains(&entry) {
                sc.topic_parity.unexpected_divergence.push(entry.clone());
            }
            sc.topic_parity
                .not_reconstructed
                .as_mut()
                .expect("the block")
                .retain(|e| *e != entry);
            assert_eq!(
                sc.validate_invariants().expect_err(omitted).0,
                message,
                "{omitted}"
            );
        }
        let mut other = new_topic_not_reconstructed();
        other.topic_parity.unexpected_divergence = vec![
            "restore-x-orders: min.insync.replicas".into(),
            "restore-x-orders: configuration not assessed (unknown)".into(),
        ];
        other.topic_parity.not_reconstructed = Some(vec![]);
        other
            .validate_invariants()
            .expect("a key the restore does not decide is not 'not reconstructed'");
        // FX-21: the fail-safe twin of a partition count or replication factor
        // the source's record lacks names neither setting as its `<key>`, so a
        // newTopic document carrying it is accepted without the twin in
        // `not_reconstructed` (the restore's deviation on it is unknown).
        let mut unrecorded = new_topic_not_reconstructed();
        unrecorded.topic_parity.unexpected_divergence = vec![
            "restore-x-orders: partition_count not assessed (notRecorded)".into(),
            "restore-x-orders: replication_factor not assessed (notRecorded)".into(),
        ];
        unrecorded.topic_parity.not_reconstructed = Some(vec![]);
        unrecorded
            .validate_invariants()
            .expect("a twin of an unrecorded source value is not 'not reconstructed'");
        let mut scratch = empty.clone();
        scratch.target.mode = TargetMode::Scratch;
        scratch.target.marker_topic = Some("logweir.scratch".into());
        scratch
            .validate_invariants()
            .expect("NR-5 judges newTopic documents only");
    }

    /// ORDER: NR-4 reports before NR-5, and both after NR-3.
    #[test]
    fn the_new_topic_label_arms_report_in_order() {
        let mut both = new_topic_not_reconstructed();
        both.topic_parity.intentionally_deviated = vec!["restore-x-orders: retention.ms".into()];
        both.topic_parity.not_reconstructed = Some(vec![]);
        assert!(both
            .validate_invariants()
            .expect_err("NR-4 and NR-5 fire")
            .0
            .starts_with("topic_parity.intentionally_deviated is not empty"));
    }

    #[test]
    fn parity_key_is_the_text_after_the_last_separator() {
        assert_eq!(
            parity_key("restore-x-orders: cleanup.policy"),
            "cleanup.policy"
        );
        assert_eq!(parity_key("cleanup.policy"), "cleanup.policy");
        assert_eq!(
            parity_key("t: configuration not assessed (unknown)"),
            "configuration not assessed (unknown)"
        );
        assert_eq!(parity_key("a: b: retention.ms"), "retention.ms");
        assert_eq!(
            parity_key("t: replication_factor not assessed (notRecorded)"),
            "replication_factor not assessed (notRecorded)"
        );
    }

    /// ORDER: a deviation COPIED into the new field and left intended violates
    /// NR-2 and NR-3 at once, and both readers report NR-2.
    #[test]
    fn the_unexpected_twin_arm_reports_before_the_intended_arm() {
        let mut sc = new_topic_not_reconstructed();
        sc.topic_parity.intentionally_deviated = sc.topic_parity.unexpected_divergence.clone();
        sc.topic_parity.unexpected_divergence.clear();
        let err = sc.validate_invariants().expect_err("both arms fire");
        assert!(
            err.0.starts_with(
                "topic_parity.not_reconstructed names a deviation that unexpected_divergence"
            ),
            "{}",
            err.0
        );
    }

    /// The arms sit before `redactions`, which stays LAST: a document violating
    /// both reports the not-reconstructed arm.
    #[test]
    fn the_not_reconstructed_arms_report_before_the_redactions_arm() {
        let mut sc = new_topic_not_reconstructed();
        sc.topic_parity.unexpected_divergence.clear();
        sc.redactions = vec![Redaction {
            path: "/topic_parity".into(),
            reason: "order".into(),
            present: false,
        }];
        let err = sc.validate_invariants().expect_err("both arms fire");
        assert!(
            err.0.starts_with("topic_parity.not_reconstructed"),
            "{}",
            err.0
        );
    }

    /// **The version pair a renumber must move together** (FX-4's
    /// `the_written_version_defines_config_coverage`, for the scorecard). The
    /// scorecard this build WRITES must be one that may carry
    /// `not_reconstructed`, or phase 8 would refuse to sign every document
    /// phase 7 produces (its own NR-1). Should another scorecard field take
    /// 1.2.0 first, FX-3 becomes 1.3.0: both constants move.
    #[test]
    fn the_written_version_defines_not_reconstructed() {
        assert_eq!(major_version(crate::FORMAT_VERSION), Some(1));
        let minor = minor_version(crate::FORMAT_VERSION).expect("a numeric minor");
        assert!(
            minor >= NOT_RECONSTRUCTED_SINCE_MINOR,
            "FORMAT_VERSION {} predates NOT_RECONSTRUCTED_SINCE_MINOR {}",
            crate::FORMAT_VERSION,
            NOT_RECONSTRUCTED_SINCE_MINOR
        );
    }

    // --- FX-8: `source.time_basis`, arms TB-1 to TB-4 ----------------------

    fn time_basis(plan: Option<&str>, producer: &[&str], unknown: &[&str]) -> TimeBasisLabel {
        TimeBasisLabel {
            plan: plan.map(str::to_string),
            producer_time: producer.iter().map(|t| t.to_string()).collect(),
            not_recorded: unknown.iter().map(|t| t.to_string()).collect(),
        }
    }

    /// The writer's block at the writer's version is coherent in each of its
    /// shapes: the control for TB-1..TB-4, so an arm written backwards cannot
    /// pass the refusing tests below for the wrong reason.
    #[test]
    fn the_time_basis_block_is_accepted_in_every_shape_the_writer_produces() {
        for label in [
            time_basis(None, &[], &[]),
            time_basis(None, &[], &["old"]),
            time_basis(Some("producerTime"), &["lat"], &[]),
            time_basis(Some("producerTime"), &["bd", "lat"], &["old"]),
            time_basis(Some("producerTime"), &[], &[]),
        ] {
            let mut sc = valid_scorecard();
            sc.source.time_basis = Some(label.clone());
            assert!(sc.validate_invariants().is_ok(), "{label:?}");
        }
    }

    /// TB-1. KILLS: deleting the arm (a 1.1.0 document carrying the block is
    /// accepted); comparing against the wrong minor (1.2.0 accepted).
    #[test]
    fn tb1_refuses_the_block_under_a_version_that_predates_it() {
        for version in ["1.0.0", "1.1.0", "1.2.0", "1.x.0"] {
            let mut sc = valid_scorecard();
            sc.format_version = version.into();
            sc.source.time_basis = Some(time_basis(None, &[], &[]));
            let err = sc
                .validate_invariants()
                .expect_err("the block predates its version");
            assert_eq!(
                err.0,
                format!(
                    "source.time_basis is present but format_version {version:?} predates it: \
                     the field is defined from 1.{TIME_BASIS_SINCE_MINOR}.0"
                )
            );
        }
        // Absent, every earlier version is decided exactly as before.
        let mut sc = valid_scorecard();
        sc.format_version = "1.1.0".into();
        assert!(sc.validate_invariants().is_ok());
    }

    /// TB-2. KILLS: deleting the arm; accepting a second spelling.
    #[test]
    fn tb2_refuses_a_plan_value_restore_time_basis_cannot_have() {
        for plan in ["appendTime", "ProducerTime", ""] {
            let mut sc = valid_scorecard();
            sc.source.time_basis = Some(time_basis(Some(plan), &[], &[]));
            let err = sc.validate_invariants().expect_err("one value only");
            assert_eq!(
                err.0,
                "source.time_basis.plan is not \"producerTime\", the one value restore.time_basis has"
            );
        }
    }

    /// TB-3, the rule in the signed document. KILLS: deleting the arm (a
    /// producer-time selection with no opt-in is accepted).
    #[test]
    fn tb3_refuses_a_producer_time_selection_the_plan_did_not_accept() {
        let mut sc = valid_scorecard();
        sc.source.time_basis = Some(time_basis(None, &["lat"], &[]));
        let err = sc
            .validate_invariants()
            .expect_err("producer time without the plan's opt-in");
        assert_eq!(
            err.0,
            "source.time_basis.producer_time names a topic but source.time_basis.plan is not \"producerTime\"; a selection by producer time is one the approved plan accepted, never a default"
        );
    }

    /// TB-4. KILLS: deleting the arm.
    #[test]
    fn tb4_refuses_a_topic_in_both_lists() {
        let mut sc = valid_scorecard();
        sc.source.time_basis = Some(time_basis(Some("producerTime"), &["lat"], &["lat"]));
        let err = sc.validate_invariants().expect_err("both lists");
        assert_eq!(
            err.0,
            "source.time_basis names a topic in both producer_time and not_recorded; a topic's timestamp type was either recorded as LogAppendTime or not recorded"
        );
    }

    /// The TB arms sit before `redactions`, which stays LAST: a document
    /// violating both reports the TB arm, from both readers.
    #[test]
    fn the_time_basis_arms_report_before_the_redactions_arm() {
        let mut sc = valid_scorecard();
        sc.source.time_basis = Some(time_basis(None, &["lat"], &[]));
        sc.redactions = vec![Redaction {
            path: "/target/cluster_id".into(),
            reason: "ordering".into(),
            present: false,
        }];
        let err = sc.validate_invariants().expect_err("two arms");
        assert!(
            err.0.starts_with("source.time_basis.producer_time"),
            "{}",
            err.0
        );
    }

    /// The writer's version defines the field it writes: a renumber that moved
    /// [`crate::FORMAT_VERSION`] without [`TIME_BASIS_SINCE_MINOR`] (or back)
    /// would sign documents its own TB-1 refuses.
    #[test]
    fn the_written_version_defines_time_basis() {
        assert_eq!(major_version(crate::FORMAT_VERSION), Some(1));
        // `>=`, not `==` (review L-6): a later MINOR still defines the
        // field; the literal pin of the writer's version is `lib.rs`'s.
        assert!(
            minor_version(crate::FORMAT_VERSION)
                .is_some_and(|minor| minor >= TIME_BASIS_SINCE_MINOR),
            "the writer's version must define source.time_basis, which TB-1 admits from 1.{}.0",
            TIME_BASIS_SINCE_MINOR
        );
        assert_eq!(
            crate::spec::TimeBasis::ProducerTime.as_str(),
            TIME_BASIS_PRODUCER_TIME
        );
    }

    // --- PROD-08.1: `integrity.verification`, arms IV-1 to IV-7 -----------

    fn sampled_verification() -> Verification {
        Verification {
            coverage: COVERAGE_SAMPLED.into(),
            comparison_basis: COMPARISON_BASIS_ARCHIVE.into(),
            header_order: HEADER_ORDER_NOT_VERIFIED.into(),
            application: APPLICATION_NOT_ATTEMPTED.into(),
            gaps: vec![OffsetRange {
                topic: "orders".into(),
                partition: 0,
                from_offset: 10,
                to_offset: 19,
            }],
            pruned: Vec::new(),
            complete: None,
        }
    }

    fn exact(n: u64) -> ReplayComparison {
        ReplayComparison {
            expected: n,
            restored: n,
            matching: n,
            ..ReplayComparison::default()
        }
    }

    fn partition(p: i32, replay: ReplayComparison) -> PartitionVerification {
        PartitionVerification {
            topic: "orders".into(),
            partition: p,
            target_topic: "drill-orders".into(),
            compared: true,
            segments: 2,
            segments_verified: 2,
            records_decoded: replay.expected + 1,
            offset_holes: 0,
            replay,
            findings: Vec::new(),
        }
    }

    /// A covered, exact complete verification over two partitions — the shape
    /// a passing complete run signs.
    fn complete_verification() -> Verification {
        let partitions = vec![partition(0, exact(5)), partition(1, exact(7))];
        Verification {
            coverage: COVERAGE_COMPLETE.into(),
            header_order: HEADER_ORDER_VERIFIED.into(),
            complete: Some(CompleteVerification {
                covered: true,
                incomplete_reason: None,
                max_records: None,
                window: CompleteWindow {
                    start_ms: None,
                    end_ms: 1_760_000_005_000,
                },
                archive: ArchiveIntegrity {
                    segments: 4,
                    segments_verified: 4,
                    segments_failed: Vec::new(),
                    segments_unverified: Vec::new(),
                    records_decoded: 14,
                    offset_holes: 0,
                },
                replay: exact(12),
                partitions,
            }),
            ..sampled_verification()
        }
    }

    fn with_verification(v: Verification) -> Scorecard {
        let mut sc = valid_scorecard();
        sc.integrity.verification = Some(v);
        sc
    }

    /// `sc` as a drill that did not pass: `result` with a reason, and an
    /// engine matrix verdict that does not claim a byte-level pass.
    fn not_a_pass(mut sc: Scorecard, result: IntegrityResult) -> Scorecard {
        sc.outcome = Outcome::FailIntegrity;
        sc.integrity.result = result;
        sc.integrity.partial_reason = Some("not a pass".into());
        sc.engine.matrix_verdict = MatrixVerdict::PassDegraded;
        sc
    }

    /// The writer's two shapes are accepted at the writer's version: the
    /// control for IV-1..IV-7, so an arm written backwards cannot pass the
    /// refusing tests below for the wrong reason. Also: an incomplete or
    /// failing complete block beside a non-pass result is a coherent
    /// document.
    #[test]
    fn the_verification_block_is_accepted_in_every_shape_the_writer_produces() {
        for v in [sampled_verification(), complete_verification()] {
            let sc = with_verification(v.clone());
            assert!(sc.validate_invariants().is_ok(), "{v:?}");
        }
        let mut v = complete_verification();
        let c = v.complete.as_mut().unwrap();
        c.covered = false;
        c.incomplete_reason = Some("stopped at sample.complete_max_records".into());
        c.partitions[1].compared = false;
        let sc = not_a_pass(with_verification(v), IntegrityResult::Partial);
        assert_eq!(sc.validate_invariants().map_err(|e| e.0), Ok(()));
    }

    /// IV-1. KILLS: deleting the arm; comparing against the wrong minor.
    #[test]
    fn iv1_refuses_the_block_under_a_version_that_predates_it() {
        for version in ["1.0.0", "1.1.0", "1.2.0", "1.3.0", "1.x.0"] {
            let mut sc = with_verification(sampled_verification());
            sc.format_version = version.into();
            let err = sc
                .validate_invariants()
                .expect_err("the block predates its version");
            assert_eq!(
                err.0,
                format!(
                    "integrity.verification is present but format_version {version:?} predates \
                     it: the field is defined from 1.{VERIFICATION_SINCE_MINOR}.0"
                )
            );
        }
        // Absent, every earlier version is decided exactly as before.
        let mut sc = valid_scorecard();
        sc.format_version = "1.3.0".into();
        assert!(sc.validate_invariants().is_ok());
    }

    /// IV-2. KILLS: deleting the arm; accepting a third spelling.
    #[test]
    fn iv2_refuses_a_coverage_outside_its_set() {
        for coverage in ["Complete", "full", ""] {
            let mut v = sampled_verification();
            v.coverage = coverage.into();
            let err = with_verification(v)
                .validate_invariants()
                .expect_err("closed set");
            assert_eq!(
                err.0,
                "integrity.verification.coverage is neither \"sampled\" nor \"complete\""
            );
        }
    }

    /// IV-3. KILLS: deleting either half (an unknown value; `verified` beside
    /// sampled coverage).
    #[test]
    fn iv3_refuses_header_order_a_sampled_run_cannot_claim() {
        let msg = "integrity.verification.header_order is not \"verified\" or \"notVerified\", or claims \"verified\" for a coverage that is not complete; a sampled verification compares a fingerprint that sorts headers";
        let mut v = sampled_verification();
        v.header_order = HEADER_ORDER_VERIFIED.into();
        assert_eq!(
            with_verification(v).validate_invariants().unwrap_err().0,
            msg
        );
        let mut v = complete_verification();
        v.header_order = "ordered".into();
        assert_eq!(
            with_verification(v).validate_invariants().unwrap_err().0,
            msg
        );
        // A complete run MAY say header order was not verified (weaker).
        let mut v = complete_verification();
        v.header_order = HEADER_ORDER_NOT_VERIFIED.into();
        assert!(with_verification(v).validate_invariants().is_ok());
    }

    /// IV-4. KILLS: deleting the arm, in either direction.
    #[test]
    fn iv4_ties_the_complete_block_to_complete_coverage() {
        let msg = "integrity.verification.complete is present exactly when integrity.verification.coverage is \"complete\"";
        let mut v = complete_verification();
        v.complete = None;
        assert_eq!(
            with_verification(v).validate_invariants().unwrap_err().0,
            msg
        );
        let mut v = complete_verification();
        v.coverage = COVERAGE_SAMPLED.into();
        v.header_order = HEADER_ORDER_NOT_VERIFIED.into();
        assert_eq!(
            with_verification(v).validate_invariants().unwrap_err().0,
            msg
        );
    }

    /// IV-5. KILLS: deleting the arm; a blank reason accepted.
    #[test]
    fn iv5_requires_a_reason_exactly_when_not_covered() {
        let msg = "integrity.verification.complete.incomplete_reason is required exactly when complete.covered is false";
        for reason in [None, Some("  ".to_string())] {
            let mut v = complete_verification();
            let c = v.complete.as_mut().unwrap();
            c.covered = false;
            c.incomplete_reason = reason;
            let sc = not_a_pass(with_verification(v), IntegrityResult::Partial);
            assert_eq!(sc.validate_invariants().unwrap_err().0, msg);
        }
        let mut v = complete_verification();
        v.complete.as_mut().unwrap().incomplete_reason = Some("why".into());
        assert_eq!(
            with_verification(v).validate_invariants().unwrap_err().0,
            msg
        );
    }

    /// IV-6, the rule in the signed document. KILLS: deleting the arm, or any
    /// one of its conjuncts (each mutation below is one conjunct's case).
    #[test]
    fn iv6_refuses_a_pass_over_a_complete_block_that_is_not_clean() {
        let msg = "integrity.result is pass but integrity.verification.complete is not covered, lists no partition, names a failed or unverified segment, or records a missing, unexpected, duplicate, out-of-order or mismatched record, in total or in a partition";
        type Mutation = fn(&mut CompleteVerification);
        let cases: [(&str, Mutation); 11] = [
            ("not covered", |c| {
                c.covered = false;
                c.incomplete_reason = Some("bound".into());
            }),
            ("failed segment", |c| {
                c.archive.segments_verified -= 1;
                c.archive.segments_failed.push("k".into());
                c.partitions[0].segments_verified -= 1;
            }),
            ("unverified segment", |c| {
                c.archive.segments_verified -= 1;
                c.archive.segments_unverified.push("k".into());
                c.partitions[0].segments_verified -= 1;
            }),
            ("missing", |c| {
                c.replay.missing += 1;
                c.replay.matching -= 1;
                c.replay.restored -= 1;
                c.partitions[0].replay.missing += 1;
                c.partitions[0].replay.matching -= 1;
                c.partitions[0].replay.restored -= 1;
            }),
            ("unexpected", |c| {
                c.replay.unexpected += 1;
                c.replay.restored += 1;
                c.partitions[0].replay.unexpected += 1;
                c.partitions[0].replay.restored += 1;
            }),
            ("duplicate", |c| {
                c.replay.duplicates += 1;
                c.replay.restored += 1;
                c.partitions[0].replay.duplicates += 1;
                c.partitions[0].replay.restored += 1;
            }),
            ("out of order", |c| {
                c.replay.out_of_order += 1;
                c.partitions[0].replay.out_of_order += 1;
            }),
            ("mismatched", |c| {
                c.replay.mismatched += 1;
                c.replay.matching -= 1;
                c.partitions[0].replay.mismatched += 1;
                c.partitions[0].replay.matching -= 1;
            }),
            ("a partition not compared", |c| {
                c.partitions[1].compared = false
            }),
            // Review L-2 (a): every partition inexact, the sums exact.
            ("partitions inexact, totals exact", |c| {
                c.partitions[0].replay.restored += 1;
                c.partitions[0].replay.matching += 1;
                c.partitions[1].replay.restored -= 1;
                c.partitions[1].replay.matching -= 1;
            }),
            // Review L-2 (b): a covered pass over no partition at all.
            ("no partition", |c| {
                c.partitions.clear();
                c.archive.segments = 0;
                c.archive.segments_verified = 0;
                c.archive.records_decoded = 0;
                c.replay = ReplayComparison::default();
            }),
        ];
        for (what, mutate) in cases {
            let mut v = complete_verification();
            mutate(v.complete.as_mut().unwrap());
            let sc = with_verification(v.clone());
            assert_eq!(
                sc.validate_invariants().map_err(|e| e.0),
                Err(msg.to_string()),
                "{what}"
            );
            // The same block beside a verdict that is not a pass is coherent:
            // IV-6 judges a PASS, nothing else.
            let sc = not_a_pass(with_verification(v), IntegrityResult::Fail);
            assert_eq!(sc.validate_invariants().map_err(|e| e.0), Ok(()), "{what}");
        }
    }

    /// IV-7. KILLS: deleting the arm, or any one sum.
    #[test]
    fn iv7_refuses_totals_that_are_not_the_partitions_sums() {
        let msg = "integrity.verification.complete's totals are not the sums of its partitions, or its segments are not each verified, failed or unverified";
        type Mutation = fn(&mut CompleteVerification);
        let cases: [(&str, Mutation); 7] = [
            ("replay", |c| c.replay.expected += 1),
            ("segments", |c| c.archive.segments += 1),
            ("segments verified", |c| {
                c.partitions[0].segments_verified -= 1
            }),
            ("records decoded", |c| c.archive.records_decoded += 1),
            ("offset holes", |c| c.partitions[1].offset_holes += 3),
            ("unaccounted segment", |c| {
                c.archive.segments_failed.push("k".into());
            }),
            ("partition replay", |c| c.partitions[1].replay.restored += 1),
        ];
        for (what, mutate) in cases {
            let mut v = complete_verification();
            mutate(v.complete.as_mut().unwrap());
            let sc = not_a_pass(with_verification(v), IntegrityResult::Fail);
            assert_eq!(
                sc.validate_invariants().map_err(|e| e.0),
                Err(msg.to_string()),
                "{what}"
            );
        }
    }

    /// The IV arms sit before `redactions`, which stays LAST.
    #[test]
    fn the_verification_arms_report_before_the_redactions_arm() {
        let mut v = sampled_verification();
        v.coverage = "full".into();
        let mut sc = with_verification(v);
        sc.redactions = vec![Redaction {
            path: "/target/cluster_id".into(),
            reason: "ordering".into(),
            present: false,
        }];
        let err = sc.validate_invariants().expect_err("two arms");
        assert!(
            err.0.starts_with("integrity.verification.coverage"),
            "{}",
            err.0
        );
    }

    /// The writer's version defines the field it writes, and the spec's
    /// spelling is the scorecard's.
    #[test]
    fn the_written_version_defines_verification() {
        assert_eq!(major_version(crate::FORMAT_VERSION), Some(1));
        assert!(
            minor_version(crate::FORMAT_VERSION)
                .is_some_and(|minor| minor >= VERIFICATION_SINCE_MINOR),
            "the writer's version must define integrity.verification, which IV-1 admits from 1.{}.0",
            VERIFICATION_SINCE_MINOR
        );
        assert_eq!(crate::spec::Coverage::Sampled.as_str(), COVERAGE_SAMPLED);
        assert_eq!(crate::spec::Coverage::Complete.as_str(), COVERAGE_COMPLETE);
    }

    /// Absent is the spelling of "not recorded": a scorecard without the block
    /// serialises without the key, so every earlier document round-trips.
    #[test]
    fn an_absent_verification_block_is_not_serialised() {
        let sc = valid_scorecard();
        let json = serde_json::to_value(&sc).unwrap();
        assert!(json["integrity"].get("verification").is_none(), "{json}");
        let sc = with_verification(complete_verification());
        let json = serde_json::to_value(&sc).unwrap();
        assert_eq!(json["integrity"]["verification"]["coverage"], "complete");
        let back: Scorecard = serde_json::from_value(json).unwrap();
        assert_eq!(back.integrity.verification, Some(complete_verification()));
    }

    /// Review L-1: ruling R-A's "blank" is `str::trim().is_empty()`, and
    /// `trim` strips `char::is_whitespace`. `docs/verify_scorecard.py`'s
    /// `RUST_WHITESPACE` is that set, code point for code point (pinned there
    /// by `test_the_blank_set_is_the_rust_readers`); this pins it here, so a
    /// toolchain whose Unicode data moves the set fails beside its twin.
    #[test]
    fn the_blank_set_is_the_unicode_white_space_property_the_script_strips() {
        let set: Vec<u32> = (0u32..=0x10FFFF)
            .filter_map(char::from_u32)
            .filter(|c| c.is_whitespace())
            .map(u32::from)
            .collect();
        let mut want: Vec<u32> = (0x9..=0xd).collect();
        want.extend([0x20, 0x85, 0xa0, 0x1680]);
        want.extend(0x2000..=0x200a);
        want.extend([0x2028, 0x2029, 0x202f, 0x205f, 0x3000]);
        assert_eq!(set, want);
        assert!(
            !'\u{1f}'.is_whitespace(),
            "U+001F is not blank to this reader"
        );
    }

    // ---- FX-23: `sample.unsampled_topics`, arms US-1 to US-3 ----

    /// A sampled scorecard at 1.6.0 that names `topics` as unsampled.
    fn with_unsampled(topics: &[&str]) -> Scorecard {
        let mut sc = with_verification(sampled_verification());
        sc.format_version = FORMAT_VERSION_WITH_UNSAMPLED_TOPICS.into();
        sc.sample.unsampled_topics = Some(topics.iter().map(|t| (*t).to_string()).collect());
        sc
    }

    /// The writer's version rule: 1.6.0 for every SAMPLED verification and
    /// for a block naming an unsampled topic; the version it was handed for a
    /// complete one. KILLS: the old version for a sampled document (a new
    /// sampled pass would read without its guarantee line); 1.6.0 for a
    /// complete one; never 1.6.0 beside the field (US-1 refuses it).
    #[test]
    fn the_version_with_sample_is_1_6_0_for_every_sampled_document() {
        use crate::spec::Coverage::{Complete, Sampled};
        let sc = with_unsampled(&["orders"]);
        assert_eq!(
            format_version_with_sample("1.4.0", &sc.sample, Sampled),
            "1.6.0"
        );
        assert_eq!(
            format_version_with_sample("1.5.0", &sc.sample, Sampled),
            "1.6.0"
        );
        assert_eq!(
            format_version_with_sample("1.4.0", &sc.sample, Complete),
            "1.6.0"
        );
        let sc = valid_scorecard();
        assert_eq!(
            format_version_with_sample("1.4.0", &sc.sample, Sampled),
            "1.6.0"
        );
        assert_eq!(
            format_version_with_sample("1.5.0", &sc.sample, Sampled),
            "1.6.0"
        );
        assert_eq!(
            format_version_with_sample("1.4.0", &sc.sample, Complete),
            "1.4.0"
        );
        assert_eq!(
            format_version_with_sample("1.5.0", &sc.sample, Complete),
            "1.5.0"
        );
        assert!(with_unsampled(&["audit", "orders"])
            .validate_invariants()
            .is_ok());
    }

    /// FX-23 review M2: only 1.6.0 and later prove an FX-23 build signed the
    /// document — and every 2.x document (PROD-11.1b), which only a build
    /// with FX-23's checks writes. KILLS: comparing against the wrong minor;
    /// accepting a major this build does not read.
    #[test]
    fn only_1_6_0_and_later_prove_the_fx23_sampled_checks() {
        for (v, want) in [
            ("1.0.0", false),
            ("1.4.0", false),
            ("1.5.0", false),
            ("1.6.0", true),
            ("1.7.0", true),
            ("2.0.0", true),
            ("3.6.0", false),
            ("0.9.0", false),
            ("1.x.0", false),
        ] {
            assert_eq!(proves_fx23_sampled_checks(v), want, "{v}");
        }
    }

    /// US-1. KILLS: deleting the arm; comparing against the wrong minor.
    #[test]
    fn us1_refuses_unsampled_topics_under_a_version_that_predates_them() {
        for version in ["1.0.0", "1.3.0", "1.4.0", "1.5.0", "1.x.0"] {
            let mut sc = with_unsampled(&["orders"]);
            // No 1.4.0 block, so IV-1 does not answer first.
            sc.integrity.verification = None;
            sc.format_version = version.into();
            assert_eq!(
                sc.validate_invariants().unwrap_err().0,
                format!(
                    "sample.unsampled_topics is present but format_version {version:?} predates \
                     it: the field is defined from 1.{UNSAMPLED_TOPICS_SINCE_MINOR}.0"
                )
            );
        }
        // Absent, every version is decided exactly as before.
        let mut sc = with_verification(sampled_verification());
        sc.format_version = "1.6.0".into();
        assert!(sc.validate_invariants().is_ok());
    }

    /// US-2. KILLS: deleting the arm, or any one of its three conditions
    /// (an empty list, a blank name, a repeat or a wrong order).
    #[test]
    fn us2_refuses_an_empty_blank_repeated_or_unordered_list() {
        let msg = "sample.unsampled_topics is empty, names a blank topic, or is not sorted and free of repeats; it names each topic max_partitions left unsampled once, in order, and is absent when there is none";
        for bad in [
            &[][..],
            &["  "][..],
            &["audit", "\u{2003}"][..],
            &["orders", "audit"][..],
            &["orders", "orders"][..],
        ] {
            assert_eq!(
                with_unsampled(bad).validate_invariants().unwrap_err().0,
                msg,
                "{bad:?}"
            );
        }
    }

    /// US-3. KILLS: deleting the arm. A complete verification leaves no topic
    /// unsampled; a sampled one may, and a document with no block (before
    /// 1.4.0's writer) is not judged by it.
    #[test]
    fn us3_refuses_unsampled_topics_beside_a_complete_verification() {
        let mut sc = with_unsampled(&["orders"]);
        sc.integrity.verification = Some(complete_verification());
        assert_eq!(
            sc.validate_invariants().unwrap_err().0,
            "sample.unsampled_topics is present but integrity.verification.coverage is \"complete\"; a complete verification compares every restored partition and leaves no topic unsampled"
        );
        let mut sc = with_unsampled(&["orders"]);
        sc.integrity.verification = None;
        assert!(sc.validate_invariants().is_ok());
    }

    /// US-1 to US-3 sit after IV-1 to IV-7 and before `redactions`: a document
    /// that breaks IV-1 and US-1 reports IV-1, and one that breaks US-1 and
    /// the redactions arm reports US-1. KILLS: moving the block.
    #[test]
    fn the_unsampled_arms_sit_between_the_verification_arms_and_redactions() {
        let mut sc = with_unsampled(&["orders"]);
        sc.format_version = "1.3.0".into();
        assert!(sc
            .validate_invariants()
            .unwrap_err()
            .0
            .starts_with("integrity.verification is present"));
        let mut sc = with_unsampled(&["orders"]);
        sc.integrity.verification = None;
        sc.format_version = "1.5.0".into();
        sc.redactions = vec![Redaction {
            path: "/x".into(),
            reason: "y".into(),
            present: true,
        }];
        assert!(sc
            .validate_invariants()
            .unwrap_err()
            .0
            .starts_with("sample.unsampled_topics is present"));
    }

    // ---- PROD-11.1: `source.selection`, arms SEL-1 to SEL-3 ----

    /// A sampled 1.7.0 scorecard narrowed to a window starting at `start`.
    fn with_selection(start: i64) -> Scorecard {
        let mut sc = with_verification(sampled_verification());
        sc.format_version = FORMAT_VERSION_WITH_SELECTION.into();
        sc.source.selection = Some(SelectionLabel {
            window_start_ms: Some(start),
            window_end_ms: 1_760_000_005_000,
            partitions: None,
            engine_runs: None,
        });
        sc
    }

    fn sel_err(sc: &Scorecard) -> String {
        sc.validate_invariants().unwrap_err().0
    }

    /// The writer's shapes are accepted, and the version rule: at least 1.7.0
    /// exactly when the block is present. KILLS: writing 1.7.0 for every
    /// document, or the old version beside the block (SEL-1 would refuse it).
    #[test]
    fn a_selection_is_written_as_1_7_0_and_accepted() {
        let sc = with_selection(1_760_000_001_000);
        assert!(sc.validate_invariants().is_ok());
        assert_eq!(
            format_version_with_selection("1.6.0", sc.source.selection.as_ref()),
            "1.7.0"
        );
        assert_eq!(format_version_with_selection("1.6.0", None), "1.6.0");
        assert_eq!(format_version_with_selection("1.4.0", None), "1.4.0");
        assert!(proves_fx23_sampled_checks(FORMAT_VERSION_WITH_SELECTION));
    }

    /// **The version steps are MONOTONIC** (the PROD-11.1 review): each takes
    /// the newer of what it was handed and its own minor, so FX-23's sampled
    /// step can never write 1.6.0 over a 1.7.0 a step before it chose, and the
    /// selection step never lowers a later minor. KILLS: either step returning
    /// its own constant unconditionally.
    #[test]
    fn every_version_step_takes_the_newer_minor() {
        use crate::spec::Coverage::{Complete, Sampled};
        let sc = with_selection(1_760_000_001_000);
        assert_eq!(
            format_version_with_sample("1.7.0", &sc.sample, Sampled),
            "1.7.0"
        );
        assert_eq!(
            format_version_with_sample("1.4.0", &sc.sample, Sampled),
            "1.6.0"
        );
        assert_eq!(
            format_version_with_sample("1.7.0", &sc.sample, Complete),
            "1.7.0"
        );
        assert_eq!(
            format_version_with_selection("1.8.0", sc.source.selection.as_ref()),
            "1.8.0"
        );
        assert_eq!(newer_format_version("1.6.0", "1.7.0"), "1.7.0");
        assert_eq!(newer_format_version("1.7.0", "1.6.0"), "1.7.0");
        assert_eq!(newer_format_version("1.x.0", "1.6.0"), "1.x.0");
        // PROD-11.1b: a major outranks every minor, in both directions.
        assert_eq!(newer_format_version("1.7.0", "2.0.0"), "2.0.0");
        assert_eq!(newer_format_version("2.0.0", "1.9.0"), "2.0.0");
    }

    /// SEL-1. KILLS: deleting the arm; comparing against the wrong minor.
    #[test]
    fn sel1_refuses_a_selection_under_a_version_that_predates_it() {
        for version in ["1.4.0", "1.5.0", "1.6.0", "1.x.0"] {
            let mut sc = with_selection(1_760_000_001_000);
            // No 1.4.0 block, so IV-1 does not answer first.
            sc.integrity.verification = None;
            sc.format_version = version.into();
            assert_eq!(
                sel_err(&sc),
                format!(
                    "source.selection is present but format_version {version:?} predates it: \
                     the block is defined from 1.{SELECTION_SINCE_MINOR}.0"
                )
            );
        }
    }

    /// SEL-2. KILLS: deleting the arm; `>` for `>=`.
    #[test]
    fn sel2_refuses_a_start_at_or_after_the_end() {
        for start in [1_760_000_005_000, 1_760_000_006_000] {
            assert_eq!(
                sel_err(&with_selection(start)),
                "source.selection.window_start_ms is not before window_end_ms; a selection's window holds at least one instant after its start"
            );
        }
        assert!(with_selection(1_760_000_004_999)
            .validate_invariants()
            .is_ok());
    }

    /// SEL-3, over the 1.4.0 complete block. KILLS: deleting it; comparing
    /// only the start, or only the end.
    #[test]
    fn sel3_holds_the_complete_block_to_the_selections_window() {
        let complete = |start: Option<i64>, end: i64| {
            let mut v = complete_verification();
            v.complete.as_mut().unwrap().window = CompleteWindow {
                start_ms: start,
                end_ms: end,
            };
            v
        };
        let mut ok = with_selection(1_760_000_001_000);
        ok.integrity.verification = Some(complete(Some(1_760_000_001_000), 1_760_000_005_000));
        assert!(
            ok.validate_invariants().is_ok(),
            "{:?}",
            ok.validate_invariants()
        );
        for (start, end) in [
            (None, 1_760_000_005_000),
            (Some(1_760_000_000_000), 1_760_000_005_000),
            (Some(1_760_000_001_000), 1_760_000_004_000),
        ] {
            let mut sc = ok.clone();
            sc.integrity.verification = Some(complete(start, end));
            assert_eq!(
                sel_err(&sc),
                "integrity.verification.complete.window is not source.selection's window; the expected output is selected by the plan's own start and end",
                "{start:?} {end}"
            );
        }
    }

    /// SEL-1 to SEL-3 sit after US-1 to US-3 and before `redactions`. KILLS:
    /// moving the block.
    #[test]
    fn the_selection_arms_sit_between_the_unsampled_arms_and_redactions() {
        let mut sc = with_selection(1_760_000_001_000);
        sc.sample.unsampled_topics = Some(vec![]);
        assert!(sel_err(&sc).starts_with("sample.unsampled_topics is empty"));
        let mut sc = with_selection(1_760_000_001_000);
        sc.format_version = "1.6.0".into();
        sc.redactions = vec![Redaction {
            path: "/x".into(),
            reason: "y".into(),
            present: true,
        }];
        assert!(sel_err(&sc).starts_with("source.selection is present"));
    }

    /// The `sample.coverage_note` sentence names the window, and claims about
    /// the records before the start only what the lane can show (review N1):
    /// the writer's note never says none was restored — a sampled lane says it
    /// cannot show it, a complete lane says only that none was expected, its
    /// verdict being the complete block's. KILLS: the sampled note claiming
    /// "restored"; the complete note claiming it before phase 7 has judged.
    #[test]
    fn the_coverage_note_names_the_window_and_claims_per_lane() {
        use crate::spec::Coverage;
        let label = with_selection(1_760_000_001_000).source.selection.unwrap();
        let head = "replay selection: every partition of every restored topic, from epoch-ms 1760000001000 (the plan's stated window start, inclusive) to epoch-ms 1760000005000 (inclusive); ";
        assert_eq!(
            label.coverage_note(Coverage::Sampled),
            format!("{head}no record before the start was expected; a sampled check does not prove that none was restored")
        );
        assert_eq!(
            label.coverage_note(Coverage::Complete),
            format!("{head}no record before the start was expected")
        );
        for lane in [Coverage::Sampled, Coverage::Complete] {
            assert!(!label
                .coverage_note(lane)
                .contains("was restored or expected"));
        }
    }

    // ---- PROD-15.1: `target.original_name` (format 1.8.0), ON-1 to ON-14 ----

    fn original_name_block() -> OriginalNameInfo {
        OriginalNameInfo {
            approval_subject: "originalName".into(),
            approval_mode: "governed".into(),
            cluster_condition: "targetIsNotSource".into(),
            source_cluster_id: Some("SOURCE-CLUSTER".into()),
            owner_detection: vec!["plan".into()],
            owners: Vec::new(),
            owner_path: false,
            confirmation: None,
            kafka_topic_resources_sha256: None,
        }
    }

    const RESOURCES_DIGEST: &str =
        "sha256:0000000000000000000000000000000000000000000000000000000000000000";

    /// A valid original-name document: newTopic, the empty prefix, 1.8.0, and
    /// the COMPLETE verification such a restore requires (ON-13).
    fn with_original_name() -> Scorecard {
        let mut sc = with_verification(complete_verification());
        sc.format_version = FORMAT_VERSION_WITH_ORIGINAL_NAME.into();
        sc.target.mode = TargetMode::NewTopic;
        sc.target.marker_topic = None;
        sc.target.topic_mapping_prefix = String::new();
        sc.target.original_name = Some(original_name_block());
        sc
    }

    fn on_err(mutate: impl FnOnce(&mut Scorecard)) -> String {
        let mut sc = with_original_name();
        mutate(&mut sc);
        sc.validate_invariants().unwrap_err().0
    }

    /// The writer's shape is accepted, with and without an owner on the owner
    /// path, under both cluster conditions; and the version rule: at least
    /// 1.8.0 exactly when the block is present. KILLS: writing 1.8.0 for every
    /// document, or the old version beside the block.
    #[test]
    fn an_original_name_block_is_written_as_1_8_0_and_accepted() {
        let sc = with_original_name();
        assert!(
            sc.validate_invariants().is_ok(),
            "{:?}",
            sc.validate_invariants()
        );
        let mut owned = with_original_name();
        let block = owned.target.original_name.as_mut().unwrap();
        block.owner_detection = vec!["plan".into(), "kafkaTopicResources".into()];
        block.owners = vec![OriginalNameOwner {
            topic: "orders".into(),
            kind: "strimzi".into(),
            reference: "kafka/orders".into(),
            found_in: "kafkaTopicResources".into(),
        }];
        block.owner_path = true;
        block.cluster_condition = "autoCreateDisabled".into();
        block.source_cluster_id = None;
        block.kafka_topic_resources_sha256 = Some(RESOURCES_DIGEST.into());
        assert!(owned.validate_invariants().is_ok());
        // OD-10: a one-person confirmation, with the names typed.
        let mut typed = with_original_name();
        let block = typed.target.original_name.as_mut().unwrap();
        block.approval_mode = "ordinary".into();
        block.confirmation = Some(crate::original_name::CONFIRMATION_TYPED_TOPIC_NAMES.into());
        assert!(
            typed.validate_invariants().is_ok(),
            "{:?}",
            typed.validate_invariants()
        );
        assert_eq!(
            format_version_with_original_name("1.6.0", sc.target.original_name.as_ref()),
            "1.8.0"
        );
        assert_eq!(
            format_version_with_original_name("1.7.0", sc.target.original_name.as_ref()),
            "1.8.0"
        );
        assert_eq!(format_version_with_original_name("1.6.0", None), "1.6.0");
        assert_eq!(
            format_version_with_original_name("1.9.0", sc.target.original_name.as_ref()),
            "1.9.0",
            "monotonic: never lowers a later minor"
        );
        // Absent on the wire for every other document.
        let plain = serde_json::to_value(valid_scorecard()).unwrap();
        assert!(plain["target"].get("original_name").is_none());
    }

    /// ON-1. KILLS: deleting the arm; comparing against the wrong minor.
    #[test]
    fn on1_refuses_the_block_under_a_version_that_does_not_define_it() {
        for version in ["1.4.0", "1.6.0", "1.7.0", "1.x.0"] {
            assert_eq!(
                on_err(|sc| {
                    sc.format_version = version.into();
                    // An unreadable minor also predates the verification
                    // block (IV-1, an earlier arm): without that block the
                    // refusal read here is ON-1's own.
                    if version == "1.x.0" {
                        sc.integrity.verification = None;
                    }
                }),
                format!(
                    "target.original_name is present but format_version {version:?} does not \
                     define it: the block is format 1's, from 1.{ORIGINAL_NAME_SINCE_MINOR}.0, \
                     and no other major carries it"
                )
            );
        }
    }

    /// A 2.0.0 document that is otherwise BOTH a valid partition-subset
    /// restore's and a valid original-name restore's: `orders` [0, 1] under
    /// a complete verification of exactly those partitions, newTopic, the
    /// empty prefix, the block.
    fn subset_with_original_name() -> Scorecard {
        let mut sc = with_original_name();
        sc.format_version = FORMAT_VERSION_WITH_PARTITION_SUBSETS.into();
        let end_ms = sc
            .integrity
            .verification
            .as_ref()
            .and_then(|v| v.complete.as_ref())
            .expect("a complete block")
            .window
            .end_ms;
        sc.source.selection = Some(SelectionLabel {
            window_start_ms: None,
            window_end_ms: end_ms,
            partitions: Some(vec![tp("orders", &[0, 1])]),
            engine_runs: Some(1),
        });
        sc
    }

    /// **ON-14: the block never sits beside a partition subset** (an
    /// original-name restore restores whole topics). The pair is refused
    /// under 2.0.0, where it is the first ON arm a document meets, and the
    /// version step never turns such a document into a 1.x one.
    ///
    /// The fixture is otherwise valid twice over, and the row shows it: the
    /// same document without the block is an accepted subset document, and
    /// without the subset (as 1.8.0) an accepted original-name one. KILLS:
    /// deleting ON-14 (the refusal would become ON-1's, another sentence);
    /// deleting ON-14 and reading major 2 in ON-1 (the document would be
    /// accepted: a production-named topic with unselected partitions empty,
    /// signed as covered).
    #[test]
    fn on14_refuses_the_block_beside_a_partition_subset() {
        const ON14: &str = "target.original_name is present beside source.selection.partitions; a restore under the original topic names restores whole topics, never a partition subset";
        let sc = subset_with_original_name();
        assert_eq!(sc.validate_invariants().map_err(|e| e.0), Err(ON14.into()));

        // Without the block: an accepted 2.0.0 subset document.
        let mut subset_only = subset_with_original_name();
        subset_only.target.original_name = None;
        assert_eq!(subset_only.validate_invariants().map_err(|e| e.0), Ok(()));
        // Without the subset, as 1.8.0: an accepted original-name document.
        let mut whole = subset_with_original_name();
        whole.source.selection = None;
        whole.format_version = FORMAT_VERSION_WITH_ORIGINAL_NAME.into();
        assert_eq!(whole.validate_invariants().map_err(|e| e.0), Ok(()));
        // A stated window START beside the block is whole partitions, bounded
        // in time: accepted, as 1.8.0.
        let mut windowed = whole.clone();
        let window = windowed
            .integrity
            .verification
            .as_mut()
            .and_then(|v| v.complete.as_mut())
            .map(|c| &mut c.window)
            .expect("a complete block");
        window.start_ms = Some(window.end_ms - 1_000);
        windowed.source.selection = Some(SelectionLabel {
            window_start_ms: Some(window.end_ms - 1_000),
            window_end_ms: window.end_ms,
            partitions: None,
            engine_runs: None,
        });
        assert_eq!(windowed.validate_invariants().map_err(|e| e.0), Ok(()));

        // The writer's version steps never make the pair a 1.x document: a
        // subset is 2.0.0 and the original-name step keeps it.
        let block = sc.target.original_name.as_ref();
        assert_eq!(
            format_version_with_original_name(
                format_version_with_selection("1.4.0", sc.source.selection.as_ref()),
                block
            ),
            "2.0.0"
        );
        assert_eq!(
            format_version_with_selection(
                format_version_with_original_name("1.4.0", block),
                sc.source.selection.as_ref()
            ),
            "2.0.0"
        );
        // And 2.0.0 does not DEFINE the block the way it defines format 1's
        // older optional blocks: ON-1 reads major 1 on purpose.
        assert!(defines_format_1_minor("2.0.0", SELECTION_SINCE_MINOR));
        assert!(!defines_original_name("2.0.0"));
        assert!(!defines_original_name("2.8.0"));
        assert!(defines_original_name("1.8.0") && defines_original_name("1.9.0"));
        assert!(!defines_original_name("1.7.0") && !defines_original_name("1.x.0"));
    }

    /// ON-2 and ON-3. KILLS: deleting either; reading a scratch document or
    /// a prefixed one as an original-name restore.
    #[test]
    fn on2_and_on3_hold_the_block_to_new_topic_and_the_empty_prefix() {
        assert_eq!(
            on_err(|sc| {
                sc.target.mode = TargetMode::Scratch;
                sc.target.marker_topic = Some("logweir.scratch".into());
            }),
            "target.original_name is present but target.mode is scratch; a scratch drill never restores under the original topic names"
        );
        assert_eq!(
            on_err(|sc| sc.target.topic_mapping_prefix = "restore-".into()),
            "target.original_name is present but target.topic_mapping_prefix is not empty; an original-name restore maps every topic onto its own name"
        );
    }

    /// ON-4 to ON-6, the closed sets. KILLS: deleting any; accepting an
    /// ordinary subject, a standing authorization, an unknown condition.
    #[test]
    fn on4_to_on6_hold_the_subject_mode_and_condition_to_their_closed_sets() {
        for subject in ["ordinary", "", "OriginalName"] {
            assert_eq!(
                on_err(|sc| sc.target.original_name.as_mut().unwrap().approval_subject = subject.into()),
                "target.original_name.approval_subject is not \"originalName\"; an original-name restore is authorised only by its own approval subject"
            );
        }
        // PROD-16.2: ON-5 is split by version. A 1.8.0 document (what
        // `on_err` builds) is judged exactly as before, in the same words...
        for mode in ["standing", "Governed", "", "consoleapproval", "two-person"] {
            assert_eq!(
                on_err(|sc| sc.target.original_name.as_mut().unwrap().approval_mode = mode.into()),
                "target.original_name.approval_mode is not one of \"v1Approval\", \"governed\", \"ordinary\""
            );
        }
        // ... the member 1.9.0 added is refused under 1.8.0 BY THE VERSION,
        // in a sentence that names it and never the document's word...
        assert_eq!(
            on_err(|sc| {
                sc.target.original_name.as_mut().unwrap().approval_mode =
                    APPROVAL_MODE_CONSOLE.into();
            }),
            "target.original_name.approval_mode is a value defined from 1.9.0 and format_version \"1.8.0\" predates it"
        );
        // ... and from 1.9.0 the closed set is four.
        for mode in ["standing", "Governed", "", "consoleapproval", "two-person"] {
            assert_eq!(
                on_err(|sc| {
                    sc.format_version = "1.9.0".into();
                    sc.target.original_name.as_mut().unwrap().approval_mode = mode.into();
                }),
                "target.original_name.approval_mode is not one of the four values this format defines; it is \"v1Approval\", \"governed\", \"ordinary\" or \"consoleApproval\" and nothing else"
            );
        }
        // NEGATIVE CONTROL: the three members of 1.8.0 are still accepted
        // under 1.9.0 (a newer minor defines everything an older one did).
        for mode in ["governed", "v1Approval"] {
            let mut sc = with_original_name();
            sc.format_version = "1.9.0".into();
            sc.target.original_name.as_mut().unwrap().approval_mode = mode.into();
            assert!(sc.validate_invariants().is_ok(), "{mode}");
        }
        assert_eq!(
            on_err(|sc| sc.target.original_name.as_mut().unwrap().cluster_condition = "sameCluster".into()),
            "target.original_name.cluster_condition is not one of \"targetIsNotSource\", \"autoCreateDisabled\""
        );
    }

    /// ON-7. KILLS: deleting it; checking only presence, or only inequality.
    #[test]
    fn on7_makes_target_is_not_source_a_comparison_of_two_known_ids() {
        let message = "target.original_name.cluster_condition is targetIsNotSource but source_cluster_id is absent or equals target.cluster_id; the condition is a comparison of two known cluster ids";
        assert_eq!(
            on_err(|sc| sc.target.original_name.as_mut().unwrap().source_cluster_id = None),
            message
        );
        assert_eq!(
            on_err(
                |sc| sc.target.original_name.as_mut().unwrap().source_cluster_id =
                    Some("  ".into())
            ),
            message
        );
        assert_eq!(
            on_err(|sc| {
                let target = sc.target.cluster_id.clone();
                sc.target.original_name.as_mut().unwrap().source_cluster_id = Some(target);
            }),
            message
        );
    }

    /// ON-8 to ON-10. KILLS: deleting any; reading "looked nowhere" as "no
    /// owner"; an owner from a place not looked in; an owned name off the
    /// owner path.
    #[test]
    fn on8_to_on10_hold_the_owner_facts_together() {
        let detection = "target.original_name.owner_detection is empty, repeats a place, or names one outside \"plan\", \"kafkaTopicResources\", \"pointReceipt\"; an owner nobody looked for is never read as no owner";
        for places in [vec![], vec!["plan", "plan"], vec!["kubernetes"]] {
            assert_eq!(
                on_err(|sc| {
                    sc.target.original_name.as_mut().unwrap().owner_detection =
                        places.iter().map(|p| (*p).to_string()).collect();
                }),
                detection
            );
        }
        let owner = |found_in: &str, kind: &str, topic: &str| OriginalNameOwner {
            topic: topic.into(),
            kind: kind.into(),
            reference: "kafka/orders".into(),
            found_in: found_in.into(),
        };
        let owners = "target.original_name.owners names a place owner_detection does not list, a kind outside \"strimzi\" and \"external\", or a blank topic";
        for bad in [
            owner("pointReceipt", "strimzi", "orders"),
            owner("plan", "terraform", "orders"),
            owner("plan", "strimzi", " "),
        ] {
            assert_eq!(
                on_err(|sc| {
                    let block = sc.target.original_name.as_mut().unwrap();
                    block.owners = vec![bad.clone()];
                    block.owner_path = true;
                }),
                owners
            );
        }
        assert_eq!(
            on_err(|sc| {
                sc.target.original_name.as_mut().unwrap().owners =
                    vec![owner("plan", "strimzi", "orders")];
            }),
            "target.original_name.owners is not empty and owner_path is false; an owned name is restored only on the owner path"
        );
    }

    /// ON-11 (OD-10) and ON-12. KILLS: a one-person confirmation signed
    /// without the typed names; a typed confirmation claimed for a mode with a
    /// second person; an unknown confirmation; a resources file looked in
    /// without its digest, or a digest for a file nobody looked in.
    #[test]
    fn on11_and_on12_tie_the_confirmation_and_the_resources_digest_to_their_facts() {
        let confirmation = "target.original_name.confirmation is not \"typedTopicNames\" exactly when approval_mode is \"ordinary\"; a one-person confirmation of an original-name restore is signed only with every original topic name re-typed";
        assert_eq!(
            on_err(
                |sc| sc.target.original_name.as_mut().unwrap().approval_mode = "ordinary".into()
            ),
            confirmation
        );
        assert_eq!(
            on_err(|sc| {
                sc.target.original_name.as_mut().unwrap().confirmation =
                    Some(crate::original_name::CONFIRMATION_TYPED_TOPIC_NAMES.into());
            }),
            confirmation
        );
        assert_eq!(
            on_err(|sc| {
                let block = sc.target.original_name.as_mut().unwrap();
                block.approval_mode = "ordinary".into();
                block.confirmation = Some("clicked".into());
            }),
            confirmation
        );
        let digest = "target.original_name.kafka_topic_resources_sha256 is not a sha256 digest exactly when owner_detection lists \"kafkaTopicResources\"; the KafkaTopic resources a runner looked in are named by their digest";
        assert_eq!(
            on_err(|sc| {
                sc.target.original_name.as_mut().unwrap().owner_detection =
                    vec!["kafkaTopicResources".into()];
            }),
            digest
        );
        assert_eq!(
            on_err(|sc| {
                sc.target
                    .original_name
                    .as_mut()
                    .unwrap()
                    .kafka_topic_resources_sha256 = Some(RESOURCES_DIGEST.into());
            }),
            digest
        );
        assert_eq!(
            on_err(|sc| {
                let block = sc.target.original_name.as_mut().unwrap();
                block.owner_detection = vec!["kafkaTopicResources".into()];
                block.kafka_topic_resources_sha256 = Some("sha256:XYZ".into());
            }),
            digest
        );
    }

    /// ON-13. KILLS: an original-name document signed over a SAMPLED
    /// verification; a pass that records no verification at all; refusing
    /// the honest shapes — a complete verification (passing, failing, or not
    /// covered and not a pass) and a run that stopped before phase 7.
    #[test]
    fn on13_an_original_name_restore_is_verified_completely_or_is_not_a_pass() {
        let msg = "target.original_name is present but integrity.verification.coverage is not \"complete\", or a pass records no verification; a restore under the original topic names is verified completely, never by sample";
        // A sampled verification beside the block.
        assert_eq!(
            on_err(|sc| sc.integrity.verification = Some(sampled_verification())),
            msg
        );
        // ... even when the run did not pass.
        let mut sc = not_a_pass(with_original_name(), IntegrityResult::Fail);
        sc.integrity.verification = Some(sampled_verification());
        assert_eq!(sc.validate_invariants().unwrap_err().0, msg);
        // A pass that records no verification.
        assert_eq!(on_err(|sc| sc.integrity.verification = None), msg);
        // An integrity pass beside a failed objective, with none recorded.
        let mut sc = with_original_name();
        sc.integrity.verification = None;
        sc.outcome = Outcome::FailObjective;
        sc.engine.matrix_verdict = MatrixVerdict::PassDegraded;
        assert_eq!(sc.validate_invariants().unwrap_err().0, msg);

        // CONTROLS. The writer's passing shape:
        assert_eq!(
            with_original_name().validate_invariants().map_err(|e| e.0),
            Ok(())
        );
        // a complete verification that found something (fail-integrity):
        let sc = not_a_pass(with_original_name(), IntegrityResult::Fail);
        assert_eq!(sc.validate_invariants().map_err(|e| e.0), Ok(()));
        // a run that stopped before phase 7 records none, and is not a pass:
        let mut sc = not_a_pass(with_original_name(), IntegrityResult::Fail);
        sc.integrity.verification = None;
        assert_eq!(sc.validate_invariants().map_err(|e| e.0), Ok(()));
        // and a document WITHOUT the block is decided as before, sampled.
        let sc = with_verification(sampled_verification());
        assert_eq!(sc.validate_invariants().map_err(|e| e.0), Ok(()));
    }

    /// ON-1 to ON-14 sit after SEL-1 to SEL-3 and before `redactions`.
    /// KILLS: moving the block.
    #[test]
    fn the_original_name_arms_sit_between_the_selection_arms_and_redactions() {
        let mut sc = with_original_name();
        sc.source.selection = Some(SelectionLabel {
            window_start_ms: Some(5),
            window_end_ms: 5,
            partitions: None,
            engine_runs: None,
        });
        assert!(sc
            .validate_invariants()
            .unwrap_err()
            .0
            .starts_with("source.selection.window_start_ms is not before"));
        let mut sc = with_original_name();
        sc.format_version = "1.7.0".into();
        sc.redactions = vec![Redaction {
            path: "/x".into(),
            reason: "y".into(),
            present: true,
        }];
        assert!(sc
            .validate_invariants()
            .unwrap_err()
            .0
            .starts_with("target.original_name is present"));
    }

    /// The two lines both readers print. KILLS: a line claiming the original
    /// topic rather than a new generation of its name; dropping the subject,
    /// the approval mode, the condition or the owner path.
    #[test]
    fn the_original_name_lines_say_what_was_proved() {
        let lines = original_name_block().lines();
        assert_eq!(
            lines,
            vec![
                "original name: restored under the source's own topic names, into topics this run created (a new generation of each name, not the original topic); approval subject originalName, approved by governed; the target cluster is not the source cluster (SOURCE-CLUSTER)".to_string(),
                "original name: declarative owners looked for in plan: none found".to_string(),
            ]
        );
        let mut owned = original_name_block();
        owned.cluster_condition = "autoCreateDisabled".into();
        owned.owners = vec![OriginalNameOwner {
            topic: "orders".into(),
            kind: "strimzi".into(),
            reference: "kafka/orders".into(),
            found_in: "plan".into(),
        }];
        owned.owner_path = true;
        let lines = owned.lines();
        assert!(lines[0].ends_with("the target may be the source cluster (SOURCE-CLUSTER) and every broker reported auto.create.topics.enable=false"), "{lines:?}");
        assert_eq!(
            lines[1],
            "original name: declarative owners looked for in plan: orders (strimzi kafka/orders, from plan); the approved plan chose the owner path"
        );
        // OD-10 and the resources digest, each in its line.
        let mut typed = original_name_block();
        typed.approval_mode = "ordinary".into();
        typed.confirmation = Some(crate::original_name::CONFIRMATION_TYPED_TOPIC_NAMES.into());
        typed.owner_detection = vec!["kafkaTopicResources".into()];
        typed.kafka_topic_resources_sha256 = Some(RESOURCES_DIGEST.into());
        let lines = typed.lines();
        assert!(
            lines[0].contains(
                "approved by ordinary (the requester re-typed every original topic name); "
            ),
            "{lines:?}"
        );
        assert_eq!(
            lines[1],
            format!("original name: declarative owners looked for in kafkaTopicResources: none found; KafkaTopic resources {RESOURCES_DIGEST}")
        );
    }

    /// **Review N1, the reader's predicate, a row per lane.** Only a complete
    /// verification whose integrity passed proves no record before the start
    /// was restored (IV-6: no unexpected record anywhere); a sampled one
    /// never does, pass or not; a complete one that did not pass, and a
    /// document with no verification (phase 7 never ran), say only that none
    /// was expected. KILLS: a sampled pass printing "restored"; a failed
    /// complete verification printing it; a missing verification printing it.
    #[test]
    fn what_a_document_proves_before_the_start_follows_its_lane_and_verdict() {
        use crate::outcome::IntegrityResult::{Fail, Pass};
        let (sampled, complete) = (sampled_verification(), complete_verification());
        assert_eq!(
            BeforeTheStart::of(&Pass, Some(&complete)),
            BeforeTheStart::ProvedNoneRestored
        );
        assert_eq!(
            BeforeTheStart::of(&Fail, Some(&complete)),
            BeforeTheStart::Expected
        );
        assert_eq!(
            BeforeTheStart::of(&Pass, Some(&sampled)),
            BeforeTheStart::SampledUnproved
        );
        assert_eq!(
            BeforeTheStart::of(&Fail, Some(&sampled)),
            BeforeTheStart::SampledUnproved
        );
        assert_eq!(BeforeTheStart::of(&Pass, None), BeforeTheStart::Expected);
        assert_eq!(
            BeforeTheStart::ProvedNoneRestored.words(),
            "no record before the start was restored or expected"
        );
        for unproved in [BeforeTheStart::SampledUnproved, BeforeTheStart::Expected] {
            assert!(!unproved.words().contains("restored or"), "{unproved:?}");
        }
    }

    // ---- PROD-11.1b: partition subsets, format 2.0.0 (OD-9 (a)), PS-1 to PS-5 ----

    /// A 2.0.0 scorecard narrowed to `orders` [0, 2] (from the floor when
    /// `start` is `None`), restored by one run, over a sampled verification.
    fn with_subset(start: Option<i64>) -> Scorecard {
        let mut sc = with_verification(sampled_verification());
        sc.format_version = FORMAT_VERSION_WITH_PARTITION_SUBSETS.into();
        sc.source.selection = Some(SelectionLabel {
            window_start_ms: start,
            window_end_ms: 1_760_000_005_000,
            partitions: Some(vec![TopicPartitions {
                topic: "orders".into(),
                partitions: vec![0, 2],
            }]),
            engine_runs: Some(1),
        });
        sc
    }

    fn tp(topic: &str, partitions: &[i32]) -> TopicPartitions {
        TopicPartitions {
            topic: topic.into(),
            partitions: partitions.to_vec(),
        }
    }

    const PS1: &str = "is the format of a partition-subset restore, and this document carries no source.selection.partitions; a reader reads major 2 only for that shape";

    /// **The version choice** (OD-9 (a)): 2.0.0 EXACTLY when the block names
    /// a partition subset — with or without a start, whatever the version
    /// before — and never for a start-only block or no block, which stay the
    /// 1.x they were. The steps after it keep it (monotonic). KILLS: 2.0.0
    /// written for a non-subset run; 1.x written for a subset run; a later
    /// step lowering 2.0.0 to a minor.
    #[test]
    fn a_subset_is_written_as_2_0_0_and_only_a_subset() {
        use crate::spec::Coverage::{Complete, Sampled};
        for start in [None, Some(1_760_000_001_000)] {
            let sc = with_subset(start);
            assert_eq!(
                sc.validate_invariants().map_err(|e| e.0),
                Ok(()),
                "{start:?}"
            );
            for current in ["1.4.0", "1.5.0", "1.6.0", "1.7.0"] {
                assert_eq!(
                    format_version_with_selection(current, sc.source.selection.as_ref()),
                    "2.0.0",
                    "{current} {start:?}"
                );
            }
            assert_eq!(
                format_version_with_sample("2.0.0", &sc.sample, Sampled),
                "2.0.0"
            );
            assert_eq!(
                format_version_with_sample("2.0.0", &sc.sample, Complete),
                "2.0.0"
            );
        }
        let start_only = with_selection(1_760_000_001_000);
        assert_eq!(
            format_version_with_selection("1.6.0", start_only.source.selection.as_ref()),
            "1.7.0"
        );
        assert_eq!(format_version_with_selection("1.4.0", None), "1.4.0");
        assert_eq!(format_version_with_selection("1.6.0", None), "1.6.0");
        // An EMPTY subset list narrows nothing and is not a 2.0.0 block.
        let mut empty = start_only.source.selection.clone().unwrap();
        empty.partitions = Some(Vec::new());
        assert!(!empty.narrows_partitions());
        assert_eq!(
            format_version_with_selection("1.6.0", Some(&empty)),
            "1.7.0"
        );
        assert!(proves_fx23_sampled_checks(
            FORMAT_VERSION_WITH_PARTITION_SUBSETS
        ));
        assert!(defines_format_1_minor("2.0.0", SELECTION_SINCE_MINOR));
        assert!(defines_format_1_minor("2.3.1", VERIFICATION_SINCE_MINOR));
        assert!(!defines_format_1_minor("3.0.0", TIME_BASIS_SINCE_MINOR));
        assert!(!defines_format_1_minor("1.6.0", SELECTION_SINCE_MINOR));
    }

    /// **PS-1, in `refuse_unreadable_major`**: major 2 is read for the
    /// partition-subset shape and nothing else — no block, a start-only block
    /// and an empty list are refused before any other arm; a major above 2 is
    /// refused as newer than this reader. KILLS: reading every 2.x document
    /// (deleting PS-1); refusing every 2.x document (the old known major).
    #[test]
    fn ps1_reads_major_2_only_for_a_partition_subset() {
        assert!(with_subset(None).refuse_unreadable_major().is_ok());
        let mut none = with_subset(None);
        none.source.selection = None;
        let mut start_only = with_subset(Some(1_760_000_001_000));
        start_only.source.selection.as_mut().unwrap().partitions = None;
        let mut empty = with_subset(None);
        empty.source.selection.as_mut().unwrap().partitions = Some(Vec::new());
        for (label, mut sc) in [("none", none), ("start-only", start_only), ("empty", empty)] {
            // A redaction too: PS-1 answers before every arm, `redactions`
            // included.
            sc.redactions = vec![Redaction {
                path: "/x".into(),
                reason: "y".into(),
                present: true,
            }];
            assert_eq!(
                sel_err(&sc),
                format!("format_version 2.0.0 {PS1}"),
                "{label}"
            );
        }
        let mut newer = with_subset(None);
        newer.format_version = "3.0.0".into();
        assert_eq!(
            sel_err(&newer),
            "format_version 3.0.0 has a major version newer than this reader understands (this build knows 2.0.0)"
        );
    }

    /// **2.0.0 is 1.7.0's fields**: every arm of major 1 holds for it — the
    /// evidence arm (once scoped to major 1), and the arms that ask whether a
    /// version defines a field (IV-1, US-1 and SEL-1 accept a 2.0.0 block).
    /// KILLS: an arm left scoped to `Some(1)`, which a 2.0.0 document would
    /// pass silently.
    #[test]
    fn every_format_1_arm_holds_for_a_2_0_0_document() {
        let mut sc = with_subset(None);
        sc.evidence.create_only_enforced = true;
        assert_eq!(
            sel_err(&sc),
            "evidence.create_only_enforced is true but the four post-put fields are zeroed before signing"
        );
        let mut sc = with_subset(None);
        sc.target.marker_topic = None;
        sc.target.mode = TargetMode::Scratch;
        assert!(sel_err(&sc).starts_with("target.marker_topic is absent"));
        let mut sc = with_subset(None);
        sc.sample.unsampled_topics = Some(vec!["a".into()]);
        sc.source.time_basis = Some(TimeBasisLabel::default());
        assert_eq!(sc.validate_invariants().map_err(|e| e.0), Ok(()));
    }

    /// **PS-2**: a format-1 block is a start and its end, nothing else — a
    /// subset (or an engine-run count) under major 1, or a block with no
    /// start, is refused, so no subset ever rides in a document an older
    /// reader would accept. KILLS: deleting the arm; any one of its three
    /// conditions.
    #[test]
    fn ps2_holds_a_format_1_block_to_a_start_only() {
        let want = "source.selection under major 1 is a stated window start and its end, and nothing else: a block without window_start_ms, or with partitions or engine_runs, is a partition-subset selection, which is format 2.0.0";
        let mut with_partitions = with_selection(1_760_000_001_000);
        with_partitions
            .source
            .selection
            .as_mut()
            .unwrap()
            .partitions = Some(vec![tp("orders", &[0])]);
        let mut with_runs = with_selection(1_760_000_001_000);
        with_runs.source.selection.as_mut().unwrap().engine_runs = Some(1);
        let mut no_start = with_selection(1_760_000_001_000);
        no_start.source.selection.as_mut().unwrap().window_start_ms = None;
        let mut subset_as_1_9 = with_subset(None);
        subset_as_1_9.format_version = "1.9.0".into();
        for (label, sc) in [
            ("partitions", with_partitions),
            ("engine_runs", with_runs),
            ("no start", no_start),
            ("a subset document as 1.9.0", subset_as_1_9),
        ] {
            assert_eq!(sel_err(&sc), want, "{label}");
        }
        assert!(with_selection(1_760_000_001_000)
            .validate_invariants()
            .is_ok());
    }

    /// **PS-3**: one spelling per subset list. KILLS: deleting the arm or any
    /// of its conditions.
    #[test]
    fn ps3_refuses_a_subset_list_with_two_spellings() {
        let want = "source.selection.partitions does not name each topic once, in order, with a non-empty, sorted list of distinct partitions that are not negative";
        for bad in [
            vec![tp("orders", &[])],
            vec![tp("orders", &[2, 0])],
            vec![tp("orders", &[1, 1])],
            vec![tp("orders", &[-1])],
            vec![tp("\u{2003}", &[0])],
            vec![tp("payments", &[0]), tp("orders", &[0])],
            vec![tp("orders", &[0]), tp("orders", &[1])],
        ] {
            let mut sc = with_subset(None);
            sc.source.selection.as_mut().unwrap().partitions = Some(bad.clone());
            assert_eq!(sel_err(&sc), want, "{bad:?}");
        }
    }

    /// **PS-4**: one run per distinct subset, and at most one more. KILLS:
    /// deleting the arm; accepting an absent count; either bound.
    #[test]
    fn ps4_holds_the_engine_runs_to_the_distinct_subsets() {
        let want = "source.selection.engine_runs is not one run per distinct partition subset, or one more for the topics without one";
        let two_distinct = Some(vec![tp("a", &[0]), tp("b", &[1, 2]), tp("c", &[0])]);
        for (runs, ok) in [
            (None, false),
            (Some(0), false),
            (Some(1), false),
            (Some(2), true),
            (Some(3), true),
            (Some(4), false),
        ] {
            let mut sc = with_subset(None);
            let sel = sc.source.selection.as_mut().unwrap();
            sel.partitions = two_distinct.clone();
            sel.engine_runs = runs;
            let r = sc.validate_invariants().map_err(|e| e.0);
            if ok {
                assert_eq!(r, Ok(()), "{runs:?}");
            } else {
                assert_eq!(r, Err(want.to_string()), "{runs:?}");
            }
        }
    }

    /// **SEL-3 and PS-5 over a 2.0.0 complete block**: its window is the
    /// selection's (no start for a subset from the floor), and nothing is
    /// expected from a partition the plan did not select — a listed
    /// unselected partition expecting nothing (a stray record's) is the
    /// selection's own finding, refused by IV-6 if the document passes.
    /// KILLS: deleting PS-5; judging a partition that expects nothing;
    /// comparing a floor subset's window against a start.
    #[test]
    fn ps5_and_sel3_hold_the_complete_block_to_the_subset() {
        let complete = |start: Option<i64>| {
            let mut v = complete_verification();
            v.complete.as_mut().unwrap().window = CompleteWindow {
                start_ms: start,
                end_ms: 1_760_000_005_000,
            };
            v
        };
        // The fixture block lists orders/0 and orders/1, both with records
        // expected; selecting [0, 1] holds.
        let mut ok = with_subset(None);
        ok.source.selection.as_mut().unwrap().partitions = Some(vec![tp("orders", &[0, 1])]);
        ok.integrity.verification = Some(complete(None));
        assert_eq!(ok.validate_invariants().map_err(|e| e.0), Ok(()));
        let mut started = ok.clone();
        started.source.selection.as_mut().unwrap().window_start_ms = Some(1_760_000_001_000);
        assert_eq!(
            sel_err(&started),
            "integrity.verification.complete.window is not source.selection's window; the expected output is selected by the plan's own start and end"
        );
        let mut narrower = ok.clone();
        narrower.source.selection.as_mut().unwrap().partitions = Some(vec![tp("orders", &[0])]);
        assert_eq!(
            sel_err(&narrower),
            "integrity.verification.complete.partitions expects records from a partition source.selection does not select"
        );
        // orders/1 listed but expecting nothing: not PS-5's.
        let mut stray = not_a_pass(narrower, IntegrityResult::Fail);
        let c = stray
            .integrity
            .verification
            .as_mut()
            .unwrap()
            .complete
            .as_mut()
            .unwrap();
        c.partitions[1].replay.expected = 0;
        c.partitions[1].replay.matching = 0;
        c.partitions[1].replay.unexpected = 7;
        c.replay.expected = 5;
        c.replay.matching = 5;
        c.replay.unexpected = 7;
        let r = stray.validate_invariants();
        assert!(
            !matches!(&r, Err(e) if e.0.starts_with("integrity.verification.complete.partitions expects")),
            "PS-5 does not judge a partition that expects nothing: {r:?}"
        );
    }

    /// The subset sentence, in the writer's note and both readers' line: the
    /// partitions named, the start (or the floor), the runs, what the
    /// document proves of the other partitions and, with a start, of the
    /// records before it. KILLS: a non-pass claiming "restored"; a floor
    /// subset naming a start; a subset sentence dropping the start clause.
    #[test]
    fn the_subset_sentence_names_the_partitions_and_claims_per_verdict() {
        use crate::outcome::IntegrityResult::{Fail, Pass};
        let floor = with_subset(None).source.selection.unwrap();
        assert_eq!(
            floor.sentence(BeforeTheStart::Expected, OutsideTheSubset::ProvedNoneRestored),
            "replay selection: ONLY orders partitions [0, 2] (every partition of any other restored topic), from the archive's floor to epoch-ms 1760000005000 (inclusive), in 1 engine run(s); no record of another partition of these topics was restored or expected"
        );
        let started = with_subset(Some(1_760_000_001_000))
            .source
            .selection
            .unwrap();
        assert_eq!(
            started.coverage_note(crate::spec::Coverage::Sampled),
            "replay selection: ONLY orders partitions [0, 2] (every partition of any other restored topic), from epoch-ms 1760000001000 (the plan's stated window start, inclusive) to epoch-ms 1760000005000 (inclusive), in 1 engine run(s); no record of another partition of these topics was expected; no record before the start was expected; a sampled check does not prove that none was restored"
        );
        let (sampled, complete) = (sampled_verification(), complete_verification());
        for v in [&sampled, &complete] {
            assert_eq!(
                OutsideTheSubset::of(&Pass, Some(v)),
                OutsideTheSubset::ProvedNoneRestored
            );
            assert_eq!(
                OutsideTheSubset::of(&Fail, Some(v)),
                OutsideTheSubset::Expected
            );
        }
        assert_eq!(
            OutsideTheSubset::of(&Pass, None),
            OutsideTheSubset::Expected
        );
        assert!(!OutsideTheSubset::Expected.words().contains("restored"));
        // A start-only block keeps the 1.23.0 sentence byte for byte.
        assert_eq!(
            with_selection(1_760_000_001_000)
                .source
                .selection
                .unwrap()
                .sentence(BeforeTheStart::Expected, OutsideTheSubset::ProvedNoneRestored),
            "replay selection: every partition of every restored topic, from epoch-ms 1760000001000 (the plan's stated window start, inclusive) to epoch-ms 1760000005000 (inclusive); no record before the start was expected"
        );
    }

    // ---- PROD-16.2: `approval.console` (format 1.9.0 and 2.1.0), CA-1 to CA-8 ----

    fn instant(text: &str) -> DateTime<Utc> {
        DateTime::parse_from_rfc3339(text)
            .expect("an instant")
            .with_timezone(&Utc)
    }

    const CONSOLE_KEY_ID: &str = "c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0";

    fn console_block() -> ConsoleApprovalInfo {
        ConsoleApprovalInfo {
            mode: APPROVAL_MODE_CONSOLE.into(),
            requester: ConsolePrincipal {
                issuer: "https://idp.example".into(),
                subject: "alice".into(),
            },
            approver: ConsolePrincipal {
                issuer: "https://idp.example".into(),
                subject: "bob".into(),
            },
            requested_at: instant("2026-10-10T12:00:00Z"),
            approved_at: instant("2026-10-10T12:04:00Z"),
            request_expires_at: instant("2026-10-10T13:00:00Z"),
            confirmation_key_id: CONSOLE_KEY_ID.into(),
        }
    }

    /// Give `sc` the approval a console-approved run signs: the block, and
    /// the three fields beside it that the block changes the meaning of.
    fn approve_in_console(sc: &mut Scorecard) {
        let block = console_block();
        sc.approval.approver = block.approver.principal_id();
        sc.approval.approved_at = block.approved_at;
        sc.approval.key_id = block.confirmation_key_id.clone();
        sc.approval.self_attested = false;
        sc.approval.console = Some(block);
    }

    /// A valid console-approved document of format 1: an ordinary restore.
    fn with_console_approval() -> Scorecard {
        let mut sc = with_verification(sampled_verification());
        approve_in_console(&mut sc);
        sc.format_version = format_version_with_console_approval(
            &sc.format_version.clone(),
            sc.approval.console.as_ref(),
        )
        .to_string();
        sc
    }

    fn ca_err(mutate: impl FnOnce(&mut Scorecard)) -> String {
        let mut sc = with_console_approval();
        mutate(&mut sc);
        sc.validate_invariants().unwrap_err().0
    }

    /// **THE VOCABULARY IS ONE SET.** Every word phase 1 can report is here
    /// once; the original-name subset is the set minus the one word that
    /// never authorises such a restore; and each row of the approval table
    /// maps to a member. KILLS: a member added to one list and not the other;
    /// a route that signs a word the readers do not hold.
    #[test]
    fn the_approval_mode_vocabulary_is_one_set() {
        use crate::approval_policy::ApprovalRoute;
        let all: std::collections::BTreeSet<&str> = APPROVAL_MODES.into_iter().collect();
        assert_eq!(all.len(), APPROVAL_MODES.len(), "no word twice");
        assert_eq!(
            all,
            [
                "v1Approval",
                "governed",
                "ordinary",
                "consoleApproval",
                "standing"
            ]
            .into_iter()
            .collect()
        );
        let original: std::collections::BTreeSet<&str> =
            ORIGINAL_NAME_APPROVAL_MODES.into_iter().collect();
        let mut expected = all.clone();
        expected.remove(APPROVAL_MODE_STANDING);
        assert_eq!(original, expected, "every word but `standing`");
        // The set at 1.8.0 is the set from 1.9.0 without the one new member,
        // in the same order.
        assert_eq!(
            ORIGINAL_NAME_APPROVAL_MODES_AT_1_8_0.to_vec(),
            ORIGINAL_NAME_APPROVAL_MODES
                .into_iter()
                .filter(|m| *m != APPROVAL_MODE_CONSOLE)
                .collect::<Vec<_>>()
        );
        for route in [
            ApprovalRoute::RequesterConfirms,
            ApprovalRoute::SecondPersonInConsole,
            ApprovalRoute::PersonalKey,
        ] {
            assert!(
                original.contains(route.approval_mode()),
                "{route:?} signs a word the readers hold"
            );
        }
        assert_eq!(
            ApprovalRoute::SecondPersonInConsole.approval_mode(),
            APPROVAL_MODE_CONSOLE
        );
        assert_eq!(
            ApprovalRoute::PersonalKey.approval_mode(),
            APPROVAL_MODE_GOVERNED
        );
        assert_eq!(
            ApprovalRoute::RequesterConfirms.approval_mode(),
            APPROVAL_MODE_ORDINARY
        );
    }

    /// **The version step.** 1.9.0 exactly when the block is present on a
    /// format-1 document; 2.1.0 exactly when it is present on a
    /// partition-subset one; `current` otherwise, whatever it is. Monotonic,
    /// and the major is never changed. KILLS: 1.9.0 for every document; a
    /// subset document lowered to 1.9.0 or left at 2.0.0; a later minor
    /// lowered.
    #[test]
    fn a_console_approval_is_written_as_1_9_0_or_as_2_1_0_for_a_partition_subset() {
        let block = console_block();
        let with = Some(&block);
        for (current, expected) in [
            ("1.4.0", "1.9.0"),
            ("1.6.0", "1.9.0"),
            ("1.8.0", "1.9.0"),
            ("1.9.0", "1.9.0"),
            ("1.12.0", "1.12.0"),
            ("2.0.0", "2.1.0"),
            ("2.1.0", "2.1.0"),
            ("2.3.0", "2.3.0"),
        ] {
            assert_eq!(
                format_version_with_console_approval(current, with),
                expected,
                "{current}"
            );
            assert_eq!(
                format_version_with_console_approval(current, None),
                current,
                "no block, no step: {current}"
            );
        }
        assert_eq!(FORMAT_VERSION_WITH_CONSOLE_APPROVAL, "1.9.0");
        assert_eq!(FORMAT_VERSION_SUBSET_WITH_CONSOLE_APPROVAL, "2.1.0");
        for (version, defines) in [
            ("1.8.0", false),
            ("1.9.0", true),
            ("1.10.0", true),
            ("2.0.0", false),
            ("2.1.0", true),
            ("2.2.0", true),
            ("3.1.0", false),
            ("0.9.0", false),
            ("1", false),
            ("x.9.0", false),
        ] {
            assert_eq!(defines_console_approval(version), defines, "{version}");
        }
        // The writer's documents are accepted: an ordinary restore, a
        // partition subset, and an original-name restore.
        let ordinary = with_console_approval();
        assert_eq!(ordinary.format_version, "1.9.0");
        assert!(
            ordinary.validate_invariants().is_ok(),
            "{:?}",
            ordinary.validate_invariants()
        );
        let mut subset = with_subset(None);
        approve_in_console(&mut subset);
        subset.format_version =
            format_version_with_console_approval("2.0.0", subset.approval.console.as_ref()).into();
        assert_eq!(subset.format_version, "2.1.0");
        assert!(
            subset.validate_invariants().is_ok(),
            "{:?}",
            subset.validate_invariants()
        );
        let mut original = with_original_name();
        approve_in_console(&mut original);
        original
            .target
            .original_name
            .as_mut()
            .unwrap()
            .approval_mode = APPROVAL_MODE_CONSOLE.into();
        original.format_version = format_version_with_console_approval(
            FORMAT_VERSION_WITH_ORIGINAL_NAME,
            original.approval.console.as_ref(),
        )
        .into();
        assert_eq!(original.format_version, "1.9.0");
        assert!(
            original.validate_invariants().is_ok(),
            "{:?}",
            original.validate_invariants()
        );
    }

    /// **A document without the block is the document it was**: the field is
    /// not serialised when absent, so no existing scorecard's bytes move.
    #[test]
    fn a_scorecard_without_a_console_approval_serialises_no_console_key() {
        let plain = serde_json::to_value(valid_scorecard()).unwrap();
        assert!(
            plain["approval"].get("console").is_none(),
            "{}",
            plain["approval"]
        );
        let approved = serde_json::to_value(with_console_approval()).unwrap();
        assert_eq!(approved["approval"]["console"]["mode"], "consoleApproval");
        assert_eq!(
            approved["approval"]["console"]["approver"],
            serde_json::json!({"issuer": "https://idp.example", "subject": "bob"})
        );
    }

    /// CA-1. KILLS: deleting it; a reader that accepts the block under 1.8.0
    /// or 2.0.0; one that refuses 1.9.0 or 2.1.0.
    #[test]
    fn ca1_refuses_the_block_under_a_version_that_predates_it() {
        // 1.8.0, and the version this document had before the console step
        // (older versions meet the arms of the blocks they predate first).
        let before = with_verification(sampled_verification()).format_version;
        assert!(!defines_console_approval(&before), "{before}");
        for version in ["1.8.0", before.as_str()] {
            assert_eq!(
                ca_err(|sc| sc.format_version = version.into()),
                format!(
                    "approval.console is present but format_version {version:?} does not define \
                     it: the block is defined from 1.9.0 of format 1 and from 2.1.0 of format 2"
                )
            );
        }
        let mut subset = with_subset(None);
        approve_in_console(&mut subset);
        assert_eq!(
            subset.validate_invariants().unwrap_err().0,
            "approval.console is present but format_version \"2.0.0\" does not define it: the \
             block is defined from 1.9.0 of format 1 and from 2.1.0 of format 2"
        );
        subset.format_version = "2.1.0".into();
        assert!(subset.validate_invariants().is_ok());
        let mut later = with_console_approval();
        later.format_version = "1.10.0".into();
        assert!(later.validate_invariants().is_ok());
    }

    /// CA-2. The block describes one mode.
    #[test]
    fn ca2_holds_the_blocks_mode_to_console_approval() {
        for mode in ["governed", "ordinary", "", "ConsoleApproval", "two-person"] {
            assert_eq!(
                ca_err(|sc| sc.approval.console.as_mut().unwrap().mode = mode.into()),
                "approval.console.mode is not \"consoleApproval\""
            );
        }
    }

    /// CA-3: THE IDENTITY RULE in the readers, in the fixed words of
    /// `approval_policy::SeparationFault`. KILLS: deleting it; a reader that
    /// compares exact strings (case, a trailing slash), or subjects alone
    /// (two issuers), or accepts what it cannot compare.
    #[test]
    fn ca3_needs_two_people_the_same_issuer_vouches_for() {
        use crate::approval_policy::{Party, SeparationFault};
        let set = |sc: &mut Scorecard, who: Party, issuer: &str, subject: &str| {
            let block = sc.approval.console.as_mut().unwrap();
            let principal = ConsolePrincipal {
                issuer: issuer.into(),
                subject: subject.into(),
            };
            match who {
                Party::Requester => block.requester = principal,
                Party::Approver => {
                    block.approver = principal;
                    // Keep CA-5 satisfied, so only CA-3 can refuse.
                    sc.approval.approver = sc
                        .approval
                        .console
                        .as_ref()
                        .unwrap()
                        .approver
                        .principal_id();
                }
            }
        };
        let idp = "https://idp.example";
        type Case = (
            &'static str,
            Party,
            &'static str,
            &'static str,
            SeparationFault,
        );
        let cases: Vec<Case> = vec![
            (
                "the requester",
                Party::Approver,
                idp,
                "alice",
                SeparationFault::SamePerson,
            ),
            (
                "another case",
                Party::Approver,
                idp,
                "ALICE",
                SeparationFault::SamePerson,
            ),
            (
                "a trailing slash",
                Party::Approver,
                "https://idp.example/",
                "alice",
                SeparationFault::SamePerson,
            ),
            (
                "another issuer",
                Party::Approver,
                "https://other.example",
                "alice",
                SeparationFault::TwoIssuers,
            ),
            (
                "whitespace",
                Party::Approver,
                idp,
                "alice ",
                SeparationFault::NotComparable(Party::Approver),
            ),
            (
                "a decomposed letter",
                Party::Approver,
                idp,
                "jose\u{301}",
                SeparationFault::NotComparable(Party::Approver),
            ),
            (
                "a blank requester",
                Party::Requester,
                idp,
                "",
                SeparationFault::NotComparable(Party::Requester),
            ),
            (
                "the local admin approving",
                Party::Approver,
                "urn:logweir:local-admin",
                "admin",
                SeparationFault::LocalAdmin(Party::Approver),
            ),
            (
                "the local admin requesting",
                Party::Requester,
                "urn:logweir:local-admin",
                "admin",
                SeparationFault::LocalAdmin(Party::Requester),
            ),
            (
                "a service account requesting",
                Party::Requester,
                idp,
                "system:serviceaccount:team-a:deployer",
                SeparationFault::SystemIdentity(Party::Requester),
            ),
        ];
        for (label, who, issuer, subject, fault) in cases {
            let message = ca_err(|sc| set(sc, who, issuer, subject));
            assert_eq!(message, console_separation_message(fault), "{label}");
            assert!(
                message.starts_with("approval.console does not name two people: the "),
                "{label}: {message}"
            );
            // No part of the message is the document's.
            if !subject.is_empty() {
                assert!(
                    !message.contains(subject) || subject == "admin",
                    "{label}: {message}"
                );
            }
        }
        assert_eq!(
            console_separation_message(SeparationFault::SamePerson),
            "approval.console does not name two people: the approver is the requester (the \
             issuer and the subject are compared without case); a two-person approval needs a \
             second person, and no role changes that"
        );
        assert_eq!(
            console_separation_message(SeparationFault::TwoIssuers),
            "approval.console does not name two people: the approver and the requester come \
             from two issuers; whether a subject of one is a subject of the other cannot be \
             known, so a principal of another issuer is never a second person"
        );
        // THE CONTROL: a second subject behind a trailing slash is accepted.
        let mut ok = with_console_approval();
        set(&mut ok, Party::Approver, "https://idp.example/", "bob");
        assert!(
            ok.validate_invariants().is_ok(),
            "{:?}",
            ok.validate_invariants()
        );
    }

    /// CA-4: no fabricated time. The approval lies inside the request's own
    /// window: not before it, and before its expiry.
    #[test]
    fn ca4_holds_the_approval_inside_the_requests_window() {
        let message = "approval.console.approved_at is before requested_at or not before request_expires_at; an approval is given after the request was made and before it expires";
        for at in [
            "2026-10-10T11:59:59Z",
            "2026-10-10T13:00:00Z",
            "2026-10-10T13:00:01Z",
        ] {
            assert_eq!(
                ca_err(|sc| {
                    sc.approval.console.as_mut().unwrap().approved_at = instant(at);
                    sc.approval.approved_at = instant(at);
                }),
                message,
                "{at}"
            );
        }
        let mut at_request = with_console_approval();
        at_request.approval.console.as_mut().unwrap().approved_at = instant("2026-10-10T12:00:00Z");
        at_request.approval.approved_at = instant("2026-10-10T12:00:00Z");
        assert!(at_request.validate_invariants().is_ok());
    }

    /// CA-5, CA-6, CA-7: the three fields beside the block are what the block
    /// says. KILLS: an approver label that is not the second person (the
    /// requester, a key label); a distinct personal key under a console
    /// approval; the request's time written as the approval's.
    #[test]
    fn ca5_to_ca7_hold_the_approver_the_key_and_the_time_to_the_block() {
        for label in [
            "https://idp.example#alice",
            "governed approver key abc",
            "bob",
            "https://idp.example#bob ",
        ] {
            assert_eq!(
                ca_err(|sc| sc.approval.approver = label.into()),
                "approval.approver is not approval.console.approver as \"<issuer>#<subject>\"; under a console approval the approver a scorecard names is the second person the console attested",
                "{label}"
            );
        }
        let key = "approval.key_id is not approval.console.confirmation_key_id; under a console approval the console's key signs the approval, so the approver's key is the console key (expected in this mode), and a distinct personal key is not a console approval";
        assert_eq!(
            ca_err(|sc| sc.approval.key_id = "a".repeat(64)),
            key,
            "a distinct personal key"
        );
        assert_eq!(
            ca_err(|sc| {
                sc.approval.key_id = String::new();
                sc.approval.console.as_mut().unwrap().confirmation_key_id = String::new();
            }),
            key,
            "no key at all"
        );
        assert_eq!(
            ca_err(|sc| sc.approval.approved_at = instant("2026-10-10T12:00:00Z")),
            "approval.approved_at is not approval.console.approved_at; under a console approval the approval time is the instant the second person approved, never the request's",
            "the request's time written as the approval's"
        );
    }

    /// CA-8: an original-name restore says `consoleApproval` exactly when it
    /// carries the block. KILLS: `governed` beside a console approver (a
    /// reader would be told a personal key countersigned); `consoleApproval`
    /// with nobody named.
    #[test]
    fn ca8_ties_the_original_name_mode_to_the_block() {
        let message = "target.original_name.approval_mode is \"consoleApproval\" exactly when approval.console is present; a console approval names who approved, and a governed, ordinary or v1 approval carries no console approver";
        // `consoleApproval` and no block, under 1.9.0 (under 1.8.0 the word
        // itself is refused first, by ON-5's version rule).
        assert_eq!(
            on_err(|sc| {
                sc.format_version = "1.9.0".into();
                sc.target.original_name.as_mut().unwrap().approval_mode =
                    APPROVAL_MODE_CONSOLE.into();
            }),
            message
        );
        // The block beside each other mode.
        for mode in ["governed", "v1Approval"] {
            let mut sc = with_original_name();
            approve_in_console(&mut sc);
            sc.format_version = "1.9.0".into();
            sc.target.original_name.as_mut().unwrap().approval_mode = mode.into();
            assert_eq!(sc.validate_invariants().unwrap_err().0, message, "{mode}");
        }
        // NEGATIVE CONTROL: a governed original-name document without the
        // block is the 1.8.0 document it was.
        assert!(with_original_name().validate_invariants().is_ok());
    }

    /// **What a reader prints.** One `console approval:` line that says who
    /// approved and how: the mode, both people, both instants, and that the
    /// console key signed both documents, which is expected in this mode. An
    /// instant is printed in UTC to the precision the document carries (the
    /// Python twin normalises the same way).
    #[test]
    fn a_console_approval_prints_who_approved_and_how() {
        assert_eq!(
            console_block().lines(),
            vec![format!(
                "console approval: mode consoleApproval; requested by https://idp.example#alice \
                 at 2026-10-10T12:00:00Z; approved in the console by https://idp.example#bob at \
                 2026-10-10T12:04:00Z (the request expired at 2026-10-10T13:00:00Z); the console \
                 key {CONSOLE_KEY_ID} signed the request and the approval, which is expected in \
                 this mode: no personal key is involved"
            )]
        );
        let mut precise = console_block();
        precise.approved_at = "2026-10-10T14:04:00.250+02:00".parse().unwrap();
        assert!(
            precise.lines()[0].contains("#bob at 2026-10-10T12:04:00.250Z (the request"),
            "{:?}",
            precise.lines()
        );
    }
}
