//! Phase 7: verify. Three checks, none of which may read "matched" over
//! nothing: (a) segment sha256 over the sampled archive segments, (c) canary
//! consume-and-reconcile by per-record fingerprints (not watermark counts),
//! and (d) topic-config parity against what phase 6 deliberately altered:
//! intended in a scratch drill, not reconstructed in a `newTopic` restore
//! (`classify_parity`). There is no (b): the engine never implemented its
//! `validation run`.
//!
//! ## One chokepoint, every lane
//!
//! "A `Pass` means every selection was actually checked" is stated once:
//!
//! - **`Evidence`** has three variants and no default, so a check that
//!   concluded nothing cannot be constructed; silence is not expressible.
//! - **`SelectionVerdict`** exists exactly once per `SampleSelection`: `run`
//!   iterates `sel` itself and `verdict_for_selection` cannot return "no
//!   opinion".
//! - **`roll_up`** is the only place `IntegrityResult` is decided. `Pass`
//!   requires `all(fully_verified)`, positive evidence from every selection
//!   on both lanes, and the empty ledger is answered explicitly (`all()` over
//!   an empty slice is vacuously true).
//! - **`ConsumeOnly` is a weaker claim, not a weaker check:** it must still
//!   show its partition gave back at least the records the manifest claims;
//!   nothing in `roll_up` branches on `level`.
//! - **Short counts as unverified, against what the manifest can support:**
//!   `claimed` = `min(sel.count, Σ record_count over the matched segments
//!   wholly inside the sampled window)`, both ends inclusive (see
//!   `supportable_claim`). It is not `sample.records_expected`, whose manifest
//!   total is uncapped by `records_per_partition`. Pinned by
//!   `claimed_is_the_manifests_wholly_inside_sum_capped_by_the_selections_own_count`.
//! - **The mode is per selection:** a legacy segment in one partition
//!   downgrades that partition only, and `roll_up` names in `partial_reason`
//!   every selection whose byte-level claim was established.
//!
//! ## Reconciling per selection, never pooled
//!
//! `compare()` keys on the bare record offset (every partition starts at 0,
//! and `ConsumedRecord` carries no topic), so it is safe only scoped to one
//! partition. It is called from `verdict_for_selection`, which is handed one
//! selection. Pinned by `verify_phase.rs`'s
//! `a_topic_restored_to_zero_records_must_fail_not_pass_even_when_pooled_with_a_healthy_topic`
//! and `run_reconciles_two_partitions_of_one_topic_independently_not_pooled`.
//!
//! ## Exit-code routing
//!
//! Like `phase5_preflight`, `run` returns `Ok(VerifyOutcome)` even when the
//! drill did not pass: a failed reconciliation is a drill result carried in
//! `integrity.{result,partial_reason}` (`Pass` becomes `Outcome::Pass`;
//! `Partial` and `Fail` reach exit 2 with a signed scorecard). It returns
//! `Err(DrillError::Operational(..))` only when the check could not be
//! performed at all (zero sample selections, a store I/O failure, an
//! unreachable broker), never for a negative comparison or an archive that
//! under-delivered: those are facts about the archive or the restore, and a
//! signed `partial` naming the partition tells an auditor more than exit 1
//! and no document.
//!
//! ## What the signed sample counters aggregate
//!
//! `Integrity`'s three counters aggregate over exactly the selections whose
//! records were reconciled against archive fingerprints; every other
//! selection is named in `partial_reason` and forbids a `Pass`.
//! `pass_rate_measured` is a ratio over the whole sample or null, so
//! `Some(1.0)` never sits beside a partial verdict.
//!
//! ## `IntegrityResult::Partial`, and one declared gap
//!
//! Any selection whose segment or record lane is `Evidence::Unverified`, with
//! no selection failing outright: zero or short fingerprints, a window no
//! segment matches, a pre-0.21 segment with no sha256, a consume-only
//! partition that gave back too little. Each names itself in
//! `partial_reason`.
//!
//! A compacted topic that legitimately holds fewer records (spec §9.3) has no
//! distinct path: telling compaction from loss would need cross-referencing
//! `topic_parity`'s `cleanup.policy=compact`, which this phase does not do,
//! so it is reported as `Fail` with the offsets logged, a known limitation.
//! `fixtures::verify_outcome_for_compacted_topic` is a shape test only.
//!
//! ## The topic-rename mapping
//!
//! `SampleSelection.topic` and `RecordFingerprint.topic` name the source
//! topic; phase 6 may have restored under another name (`mapping`). Both
//! target reads translate first: `verdict_for_selection` (via
//! `mapped_topic`) and `classify_parity_all`, pinned by
//! `verdict_for_selection_reads_the_mapped_target_topic_never_the_archive_name`
//! and `classify_parity_all_reads_the_mapped_target_topic_never_the_archive_name`.
//! A topic in `sel` with no `mapping` entry is refused
//! (`DrillError::Operational`), never skipped.
use crate::drill::DrillError;
use logweir_core::backup_receipt::{ConfigCoverage, SourceConfigCoverage};
use logweir_core::engine::{
    expected_restored_count, partition_count_bound, BackupSetFacts, DataEngine, EngineError,
    EngineReport, RecordFingerprint, RestorePlan, SampleSelection, SegmentFacts,
};
use logweir_core::outcome::{IntegrityLevel, IntegrityResult};
use logweir_core::replay_selection::ReplaySelection;
use logweir_core::scorecard::{Integrity, TopicParity};
use logweir_core::scorecard::{
    Verification, APPLICATION_NOT_ATTEMPTED, COMPARISON_BASIS_ARCHIVE, HEADER_ORDER_NOT_VERIFIED,
    HEADER_ORDER_VERIFIED,
};
use logweir_core::spec::Coverage;
use logweir_core::spec::TargetMode;
use logweir_engine_oso::storage::Store;
use logweir_kafka::reader::{ClusterReader, ConsumedRecord};
use std::collections::BTreeMap;

/// The complete lane (`sample.coverage: complete`).
pub mod complete;

#[derive(Debug)]
pub struct VerifyOutcome {
    pub integrity: Integrity,
    pub topic_parity: TopicParity,
    pub records_restored: u64,
    pub newest_restored_ts_ms: i64,
    pub verified_at: chrono::DateTime<chrono::Utc>,
}

impl VerifyOutcome {
    /// MEASURED `records_sampled_matching / records_sampled`. `None` when
    /// `integrity.level` is not `byte-fingerprint` (spec's null-not-NaN rule;
    /// see `run`'s own computation of this field for why).
    pub fn pass_rate(&self) -> Option<f64> {
        self.integrity.pass_rate_measured
    }
}

/// The engine writes x-original-offset as `record.offset.to_le_bytes()` — an
/// 8-byte little-endian i64 (U:crates/kafka-backup-core/src/backup/engine.rs:1846);
/// the restore-side injection matches (U:…/restore/helpers.rs:130). Anything
/// that is not exactly 8 bytes is NOT an original offset and yields None.
///
/// # No ASCII fallback
///
/// A decimal-text arm for the "not 8 bytes" case would bring back the silent
/// fallback to the target's own offsets in `compare`, and `b"12345678"` is a
/// valid little-endian i64 and decodes as one. Pinned by
/// `decode_original_offset_accepts_only_eight_bytes`
/// (`crates/logweir/tests/windowed_reconciliation.rs`).
pub fn decode_original_offset(raw: &[u8]) -> Option<i64> {
    Some(i64::from_le_bytes(raw.try_into().ok()?))
}

/// Reconciles by (partition, offset) — never by position, because a restored
/// topic's offsets need not start at the source's. Returns
/// (sampled, matching, mismatch descriptions).
pub fn compare(
    archive: &[RecordFingerprint],
    consumed: &[ConsumedRecord],
) -> (u64, u64, Vec<String>) {
    // The restored record carries x-original-offset when the backup wrote it;
    // otherwise we fall back to the target offset, which is correct on a fresh
    // scratch topic restored from offset 0 — and ONLY then, which is why
    // `decode_original_offset` must not be allowed to answer `None` for a
    // header the engine actually wrote (see its doc comment).
    //
    // The last `x-original-offset`, as the complete lane reads it
    // (`complete::lineage_offset`): the backup appends its own after the
    // record's headers, so the last is this archive's lineage. A restored
    // record from the pinned engine carries exactly one, so first and last
    // agree on every target it writes.
    let by_offset: BTreeMap<i64, &ConsumedRecord> = consumed
        .iter()
        .map(|c| {
            let orig = complete::lineage_offset(&c.headers);
            (orig.unwrap_or(c.offset), c)
        })
        .collect();

    let mut matching = 0u64;
    let mut mismatches = Vec::new();
    for a in archive {
        match by_offset.get(&a.offset) {
            None => mismatches.push(format!(
                "{}/{} offset {}: present in the archive, absent from the target",
                a.topic, a.partition, a.offset
            )),
            Some(c) if c.fingerprint() == a.sha256 => matching += 1,
            Some(_) => mismatches.push(format!(
                "{}/{} offset {}: fingerprint mismatch",
                a.topic, a.partition, a.offset
            )),
        }
    }
    (archive.len() as u64, matching, mismatches)
}

/// The two topic-CONFIG keys whose target value the restore's own topic
/// creation DECIDES instead of copying from the source: `cleanup.policy` is
/// left to the target broker's default (normally `delete`) and `retention.ms`
/// is `-1` (`logweir_kafka::reader::TARGET_TOPIC_CONFIGS`), in BOTH modes.
///
/// Whether a deviation on one of them is intended depends on the mode: a
/// scratch cluster runs `cleanup.policy=delete` with infinite retention on
/// purpose, but a `newTopic` restore is the recovery itself, so there the
/// same deviation is a source setting it did not reconstruct.
const DECIDED_BY_THE_RESTORE: [&str; 2] = ["cleanup.policy", "retention.ms"];

/// The `<key>` of a partition-count entry in `topic_parity`.
const PARTITION_COUNT: &str = "partition_count";
/// The `<key>` of a replication-factor entry in `topic_parity`.
const REPLICATION_FACTOR: &str = "replication_factor";

/// One topic's deviations, by class. Each entry is a bare key here;
/// `classify_parity_all` prefixes it with the target topic.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ParityClasses {
    /// `topic_parity.intentionally_deviated`: a scratch drill's deviations on
    /// the four kinds the restore decides. Always empty in `newTopic` mode.
    pub intended: Vec<String>,
    /// `topic_parity.unexpected_divergence`: every other differing key, plus,
    /// in `newTopic` mode, every entry of `not_reconstructed` (the fail-safe
    /// twin a reader older than format 1.2.0 sees).
    pub unexpected: Vec<String>,
    /// `topic_parity.not_reconstructed` (format 1.2.0): a `newTopic`
    /// restore's deviations on the four kinds. Always empty in `scratch` mode.
    pub not_reconstructed: Vec<String>,
    /// `partition_count` and/or `replication_factor` when the
    /// SOURCE's value is not recorded, so the two were NOT compared: never a
    /// match, never a deviation. `classify_parity_all` names each in
    /// `topic_parity.not_assessed` as `"<target topic>: <key> (notRecorded)"`
    /// and writes its fail-safe twin into `unexpected_divergence`.
    pub not_assessed: Vec<String>,
}

/// Spec §9.3 phase 7(d) requires the replication-factor and partition-count
/// divergence recorded in phase 3 to be reported. Neither is a topic-config
/// key, so they are passed as integers and compared separately.
///
/// # The mode decides the label
///
/// The four kinds (the two config keys of [`DECIDED_BY_THE_RESTORE`],
/// `partition_count` and `replication_factor`) are deviations the restore's
/// topic creation makes by construction: it renders the plan's
/// `default_replication_factor` and the manifest's partition count, and
/// leaves `cleanup.policy` and `retention.ms` to its own values. That is a
/// scratch rationale:
///
/// - **`scratch`**: the four are `intended`.
/// - **`newTopic`**: the four are `not_reconstructed` AND `unexpected`, never
///   `intended`. A reader of format 1.2.0 reads `not_reconstructed`; an older
///   reader sees the same deviation in `unexpected_divergence`, a weaker
///   conclusion and never silence.
///
/// Every other differing key is `unexpected` in both modes.
///
/// # A source value that is not recorded is not a match
///
/// `src_partitions` and `src_rf` are what the source's record says, `None`
/// when it says nothing (engine 0.23.3 records the replication factor for the
/// first topic it saves only). A missing value is neither compared nor
/// matched: its key goes into [`ParityClasses::not_assessed`], whatever the
/// mode.
pub fn classify_parity(
    mode: TargetMode,
    source_cfg: &BTreeMap<String, String>,
    target_cfg: &BTreeMap<String, String>,
    src_partitions: Option<i32>,
    tgt_partitions: i32,
    src_rf: Option<i16>,
    tgt_rf: i16,
) -> ParityClasses {
    // What the restore's creation decided, whatever the mode.
    let mut by_construction = Vec::new();
    let mut unexpected = Vec::new();
    for (k, v) in source_cfg {
        let differs = target_cfg.get(k).map(|t| t != v).unwrap_or(true);
        if !differs {
            continue;
        }
        if DECIDED_BY_THE_RESTORE.contains(&k.as_str()) {
            by_construction.push(k.clone())
        } else {
            unexpected.push(k.clone())
        }
    }
    // Compared only where the source's value is recorded. A recorded count of
    // 0 or less is no count (the receipt's arm 19 refuses one), so it is not
    // recorded either.
    let mut not_assessed = Vec::new();
    match src_partitions.filter(|n| *n >= 1) {
        Some(src) if src != tgt_partitions => by_construction.push(PARTITION_COUNT.into()),
        Some(_) => {}
        None => not_assessed.push(PARTITION_COUNT.to_string()),
    }
    match src_rf.filter(|n| *n >= 1) {
        Some(src) if src != tgt_rf => by_construction.push(REPLICATION_FACTOR.into()),
        Some(_) => {}
        None => not_assessed.push(REPLICATION_FACTOR.to_string()),
    }
    by_construction.sort();
    let mut classes = match mode {
        TargetMode::Scratch => ParityClasses {
            intended: by_construction,
            unexpected,
            not_reconstructed: Vec::new(),
            not_assessed,
        },
        TargetMode::NewTopic => {
            // MOVED, never dropped: the twin in the list every reader shows.
            unexpected.extend(by_construction.iter().cloned());
            ParityClasses {
                intended: Vec::new(),
                unexpected,
                not_reconstructed: by_construction,
                not_assessed,
            }
        }
    };
    classes.unexpected.sort();
    classes
}

// The chokepoint. "A Pass means every selection was actually checked" is
// spelled once, in `roll_up`, over a ledger every lane must fill:
// `SelectionVerdict`, one per `SampleSelection`, carrying `Evidence` that is
// a value. Do not add a lane-specific guard below; add the lane's conclusion
// to its `Evidence`.

/// What one check concluded about one selection. Three variants and no
/// default: a check that concluded nothing cannot be constructed, so "I
/// checked nothing" is a value someone downstream must handle, never
/// silence.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Evidence {
    /// POSITIVE evidence: `checked` units of this selection's own claim were
    /// examined and held. The ONLY variant that can contribute to a `Pass`.
    Verified { checked: u64 },
    /// Examined, and found wrong. Contributes a `Fail`.
    Failed { why: String },
    /// Not examined at all, or examined over LESS than the claim. Never a
    /// `Pass` — and never a `Fail` either: "I did not check this" is a
    /// different statement from "I checked this and it was corrupt", and
    /// collapsing the two would either fabricate corruption or hide silence.
    Unverified { why: String },
}

impl Evidence {
    fn is_verified(&self) -> bool {
        matches!(self, Evidence::Verified { .. })
    }
    fn is_failed(&self) -> bool {
        matches!(self, Evidence::Failed { .. })
    }
    fn is_unverified(&self) -> bool {
        matches!(self, Evidence::Unverified { .. })
    }
    fn why(&self) -> Option<&str> {
        match self {
            Evidence::Verified { .. } => None,
            Evidence::Failed { why } | Evidence::Unverified { why } => Some(why.as_str()),
        }
    }
}

/// Exactly one of these per `SampleSelection`, always. There is no path
/// through phase 7 on which a selection contributes nothing: `run` builds the
/// vector by iterating `sel` itself, and `verdict_for_selection` returns
/// either a verdict or an `Err` — it cannot return "nothing to say".
#[derive(Debug)]
struct SelectionVerdict {
    /// `"topic/partition"` — the selection this verdict is about, and the
    /// prefix every reason string carries into the signed `partial_reason`.
    id: String,
    /// What the manifest can support for this selection:
    /// `min(sel.count, Σ record_count)` over the matched segments wholly
    /// inside the sampled window (see `supportable_claim`). Not the
    /// scorecard's `sample.records_expected`, which is Σ `count`.
    claimed: u64,
    /// (a) archive-segment sha256 evidence for THIS selection's segments.
    segments: Evidence,
    /// (c) record-level evidence for THIS selection's records — byte
    /// reconciliation, or (on the consume-only lane) read-back.
    records: Evidence,
    /// Records actually consumed from this selection's own target partition.
    records_restored: u64,
    /// `(compared, matching)` when `compare()` actually ran for this
    /// selection; `None` on the consume-only lane and whenever the archive
    /// offered zero fingerprints to compare.
    reconciled: Option<(u64, u64)>,
    /// Why THIS selection could not be fingerprinted, when it could not.
    /// Per selection — never for the whole backup set (see
    /// `probe_archive_modes`).
    downgrade: Option<String>,
}

impl SelectionVerdict {
    fn failed(&self) -> bool {
        self.segments.is_failed() || self.records.is_failed()
    }
    /// The ONLY predicate `roll_up` accepts as grounds for a `Pass`: BOTH
    /// lanes concluded positively for this selection.
    fn fully_verified(&self) -> bool {
        self.segments.is_verified() && self.records.is_verified()
    }
    /// True when the record lane reached a conclusion either way — the
    /// precondition for this selection's numbers being safe to fold into a
    /// whole-drill ratio.
    fn records_conclusive(&self) -> bool {
        !self.records.is_unverified()
    }
    fn notes(&self) -> Vec<String> {
        let mut out = Vec::new();
        if let Some(reason) = &self.downgrade {
            out.push(format!(
                "{}: archive fingerprints unavailable ({reason}); this selection makes no \
                 per-record integrity claim",
                self.id
            ));
        }
        for why in [self.segments.why(), self.records.why()]
            .into_iter()
            .flatten()
        {
            out.push(format!("{}: {why}", self.id));
        }
        out
    }
}

/// THE roll-up, and the only place in this phase where `IntegrityResult` is
/// decided. Three properties, each load-bearing:
///
/// 1. **`result` is never initialised to `Pass`.** It is COMPUTED from the
///    ledger. A `Pass` requires `all(fully_verified)` — positive evidence
///    from every selection on both lanes — not the absence of a recorded
///    failure. Deleting any lane's ability to record a failure therefore
///    produces `Partial`, never a pass, because every lane constructs
///    `Evidence::Verified` from a positive, non-vacuous predicate, never as
///    the fall-through of negative tests (`segment_evidence`,
///    `verdict_for_selection`).
/// 2. **The empty ledger is answered first and explicitly.** `all()` over an
///    empty slice is vacuously TRUE — precisely the false pass this function
///    exists to prevent — so zero verdicts returns `Partial`, never `Pass`,
///    without relying on any caller's own guard.
/// 3. **`ConsumeOnly` is a weaker CLAIM, not a weaker CHECK.** Nothing here
///    branches on `level`: a consume-only selection reaches `Pass` through
///    the same `Evidence::Verified` every other lane must produce, which for
///    that lane means "this partition actually gave back at least the
///    records the manifest claims". The lane cannot opt out of the
///    obligation, because the obligation is not written in the lane.
///
/// # `count_bound`: the whole-drill check that is not a selection
///
/// Guard G-WIN's second half compares the count the target holds against the
/// bound the manifest puts on the restore window, over every mapped topic at
/// once, so it cannot be a `SelectionVerdict`, and applying it after this
/// function would make a second decider. It arrives as an argument:
/// `Some(text)` when a whole-drill finding was made (a `Fail`, whose text
/// `partial_reason` carries), `None` otherwise. It can only make the result
/// worse. It carries every whole-drill finding, joined: the count bound (the
/// sum, then each mapped partition outside its own bound, decided by
/// `check_restored_count`) and the engine report's missing partitions
/// (`check_engine_report`), each a measured contradiction, never missing
/// coverage.
fn roll_up(verdicts: &[SelectionVerdict], count_bound: Option<&str>) -> Integrity {
    if verdicts.is_empty() {
        // Still folds the bound in: an empty ledger is already not a pass, but
        // an auditor reading `partial_reason` must find every finding the run
        // actually made, and a count outside the bound is a finding.
        let mut why = vec![
            "no selection produced a verdict; a verification that examined \
                            nothing can never report a pass"
                .to_string(),
        ];
        if let Some(bound) = count_bound {
            why.push(bound.to_string());
        }
        return Integrity {
            level: IntegrityLevel::NotAttempted,
            result: if count_bound.is_some() {
                IntegrityResult::Fail
            } else {
                IntegrityResult::Partial
            },
            partial_reason: Some(why.join("; ")),
            records_sampled: 0,
            records_sampled_matching: 0,
            mismatches: 0,
            pass_rate_measured: None,
            restored_principal_could_consume: None,
            verification: None,
        };
    }

    // The reported LEVEL is the weakest claim any selection could support:
    // a drill containing one legacy segment cannot honestly tell an auditor
    // "byte-fingerprint" for the restore as a whole. What it must not do is
    // let that downgrade weaken the CHECK applied to the other selections
    // (they are still reconciled record-for-record, and their mismatches
    // still fail the drill) or erase the fact that a byte-level claim was
    // genuinely established for them — `roll_up` says so explicitly below,
    // naming those selections in `partial_reason`.
    let level = if verdicts.iter().any(|v| v.downgrade.is_some()) {
        IntegrityLevel::ConsumeOnly
    } else {
        IntegrityLevel::ByteFingerprint
    };

    let result = if count_bound.is_some() || verdicts.iter().any(SelectionVerdict::failed) {
        // The count bound is FIRST and is a `Fail`, not a `Partial`: the
        // manifest proves the window holds a number of records the target does
        // not hold, which is a measured contradiction and not missing
        // coverage. Exit 2 through `Outcome::FailIntegrity`, with the
        // scorecard written and signed (Global Constraint 11).
        IntegrityResult::Fail
    } else if !verdicts.is_empty() && verdicts.iter().all(SelectionVerdict::fully_verified) {
        // The `!verdicts.is_empty()` conjunct is redundant with the early
        // return above and is kept deliberately: `all()` over an empty slice
        // is vacuously TRUE, so the ONE predicate that can produce a `Pass`
        // must not be able to fire over nothing even if some future edit
        // removes, reorders or short-circuits that early return. Pinned by
        // `roll_up_on_an_empty_ledger_is_partial_never_pass`.
        IntegrityResult::Pass
    } else {
        IntegrityResult::Partial
    };

    // The scorecard's three sample counters aggregate over exactly the
    // selections whose records were reconciled against archive fingerprints
    // (`reconciled.is_some()`); every other selection appears by name in
    // `partial_reason` and forbids a `Pass`.
    let sampled: u64 = verdicts
        .iter()
        .filter_map(|v| v.reconciled)
        .map(|(compared, _)| compared)
        .sum();
    let matching: u64 = verdicts
        .iter()
        .filter_map(|v| v.reconciled)
        .map(|(_, m)| m)
        .sum();

    // `pass_rate_measured` is a ratio over the whole sample or null:
    // `Some(1.0)` beside a partial verdict misleads even when every figure in
    // it is true. It also keeps "set implies level == byte-fingerprint", and a
    // zero denominator null rather than NaN (`to_deterministic_json` refuses
    // non-finite floats). This predicate is consulted only when no selection
    // was downgraded (the `level == ByteFingerprint` conjunct short-circuits
    // first), so a mutant that skips downgraded selections here is
    // equivalent; reorder that conjunction and it needs its own test.
    let whole_sample_reconciled = verdicts.iter().all(SelectionVerdict::records_conclusive);
    let pass_rate_measured =
        if level == IntegrityLevel::ByteFingerprint && whole_sample_reconciled && sampled > 0 {
            Some(matching as f64 / sampled as f64)
        } else {
            None
        };

    let mut notes: Vec<String> = verdicts.iter().flat_map(SelectionVerdict::notes).collect();
    // FIRST in the joined string, ahead of every per-selection note: it is the
    // only finding here that is about the restore as a whole.
    if let Some(bound) = count_bound {
        notes.insert(0, bound.to_string());
    }
    if level == IntegrityLevel::ConsumeOnly {
        let byte_level: Vec<&str> = verdicts
            .iter()
            .filter(|v| v.downgrade.is_none() && v.reconciled.is_some())
            .map(|v| v.id.as_str())
            .collect();
        if !byte_level.is_empty() {
            notes.push(format!(
                "byte-fingerprint reconciliation WAS established for {} — one selection's \
                 unsupported archive does not erase that; `level` reports the weakest claim \
                 any selection could support, never the strongest",
                byte_level.join(", ")
            ));
        }
    }
    // `Scorecard::validate_invariants` refuses a `partial` with a null
    // reason. `Partial` here always comes from an `Evidence::Unverified`,
    // which always carries a `why`, so this is belt-and-braces rather than a
    // reachable path — and it is the cheap kind: a generic sentence beats an
    // invariant violation at signing time.
    if result == IntegrityResult::Partial && notes.is_empty() {
        notes.push("at least one selection produced no positive evidence".into());
    }

    Integrity {
        level,
        result,
        partial_reason: (!notes.is_empty()).then(|| notes.join("; ")),
        records_sampled: sampled,
        records_sampled_matching: matching,
        mismatches: sampled.saturating_sub(matching),
        pass_rate_measured,
        restored_principal_could_consume: None,
        verification: None,
    }
}

/// Every archive segment one selection samples, matched by (topic,
/// partition) and window overlap, the window filter `phase4_sample` uses.
/// Zero matched segments is not refused here: `segment_evidence` reports it
/// as `Evidence::Unverified`.
fn sampled_segments_for<'a>(
    facts: &'a BackupSetFacts,
    s: &SampleSelection,
) -> Vec<&'a SegmentFacts> {
    let (w0, w1) = s.window;
    facts
        .topics
        .iter()
        .filter(|t| t.name == s.topic)
        .flat_map(|t| t.partitions.iter())
        .filter(|p| p.partition_id == s.partition)
        .flat_map(|p| p.segments.iter())
        .filter(|seg| seg.start_timestamp <= w1 && seg.end_timestamp >= w0)
        .collect()
}

/// Check (a) for one selection: sha256 over that selection's own sampled
/// archive segments, read back from the store.
///
/// Segments written before 0.21 carry an empty sha256 (manifest.rs:376-380);
/// a skipped segment is `Unverified`, coverage the drill did not obtain.
/// `Evidence::Verified` comes only from the positive, non-vacuous predicate
/// `!segs.is_empty() && checked == segs.len()`, so deleting the `failed`
/// branch lands a mismatching segment on `Unverified`, never `Verified`, and
/// no segments is never vacuously verified.
fn segment_evidence(store: &Store, segs: &[&SegmentFacts]) -> Result<Evidence, DrillError> {
    if segs.is_empty() {
        return Ok(Evidence::Unverified {
            why: "no archive segment matches this selection in the sampled window, so the \
                  segment sha256 check examined nothing for it"
                .into(),
        });
    }
    let mut checked = 0u64;
    let mut skipped = Vec::new();
    let mut failed = Vec::new();
    for seg in segs {
        if seg.sha256.is_empty() {
            tracing::warn!(target: "logweir::verify", key = %seg.key,
                           "no sha256 (segment written before 0.21); cannot be verified");
            skipped.push(seg.key.clone());
            continue;
        }
        // `StoreError` reaches `DrillError` through `EngineError`'s existing
        // conversion. Under the segment ceiling: an object over it is an
        // operational failure naming the cap, never a pass.
        let (bytes, _vid) = store
            .get_capped(&seg.key, logweir_engine_oso::storage::caps::SEGMENT)
            .map_err(EngineError::from)?;
        // Compared as bare hex: the engine's manifest writes bare hex, and
        // Logweir's own form is `sha256:<hex>`. `strip_prefix` accepts either,
        // so a manifest that adopts the prefixed spelling still compares.
        let want = seg
            .sha256
            .strip_prefix("sha256:")
            .unwrap_or(seg.sha256.as_str());
        if logweir_core::ids::sha256_hex(&bytes) != want {
            tracing::error!(target: "logweir::verify", key = %seg.key,
                            "sha256 mismatch against the manifest");
            failed.push(seg.key.clone());
        } else {
            checked += 1;
        }
    }
    // POSITIVE first, and non-vacuous: every one of this selection's own
    // sampled segments was read back from the store and matched the manifest.
    if !segs.is_empty() && checked == segs.len() as u64 {
        return Ok(Evidence::Verified { checked });
    }
    // "Examined and found wrong" outranks "not examined": a run holding both
    // a mismatching segment and a skipped one is a `Fail`, not a `Partial`.
    if !failed.is_empty() {
        return Ok(Evidence::Failed {
            why: format!(
                "segment sha256 mismatch against the manifest for {}",
                failed.join(", ")
            ),
        });
    }
    if !skipped.is_empty() {
        return Ok(Evidence::Unverified {
            why: format!(
                "{} of {} sampled segments carry no sha256 (written before 0.21) and could not \
                 be verified: {}",
                skipped.len(),
                segs.len(),
                skipped.join(", ")
            ),
        });
    }
    // Unreachable while the loop's only three outcomes are checked/skipped/
    // failed, and deliberately NOT `unreachable!()`: the honest answer to "I
    // cannot account for every sampled segment" is that this selection was
    // not fully examined, which is exactly `Unverified`.
    Ok(Evidence::Unverified {
        why: format!(
            "only {checked} of {} sampled segments could be accounted for",
            segs.len()
        ),
    })
}

/// What one selection's archive side offered, per selection: one legacy
/// partition must not erase the byte-level claim of every other.
#[derive(Debug)]
enum SelectionArchive {
    Fingerprints(Vec<RecordFingerprint>),
    /// THIS selection's segments cannot be fingerprinted (KBAK level below
    /// the gate). Carries the engine's own reason.
    Unsupported(String),
}

/// Probes each selection's archive support independently, in `sel`'s own
/// order (`out.len() == sel.len()`).
///
/// An `Unsupported` for one selection downgrades ONLY that selection.
/// `EngineError::Unsupported`'s doc comment describes a fact about a
/// segment's KBAK level, and a backup set can mix levels (a set written
/// across an engine upgrade, a re-uploaded partition); even where it cannot,
/// probing per selection costs one extra call on a set that is uniformly
/// unsupported and removes an entire class of cross-selection contamination.
///
/// Refuses an empty `sel` outright — a fingerprint comparison over an empty
/// set is the same false-pass shape this whole file exists to prevent.
fn probe_archive_modes(
    engine: &dyn DataEngine,
    sel: &[SampleSelection],
) -> Result<Vec<SelectionArchive>, EngineError> {
    if sel.is_empty() {
        return Err(EngineError::Operational(
            "probe_archive_modes called with zero sample selections; refusing to compare \
             fingerprints over an empty set"
                .into(),
        ));
    }
    let mut out = Vec::with_capacity(sel.len());
    for s in sel {
        match engine.fingerprints(s) {
            Ok(fp) => out.push(SelectionArchive::Fingerprints(fp)),
            // Only THIS selection is downgraded; the loop continues, so a
            // sibling selection's genuinely-established byte-level claim
            // survives.
            Err(EngineError::Unsupported(reason)) => {
                out.push(SelectionArchive::Unsupported(reason))
            }
            Err(e) => return Err(e),
        }
    }
    Ok(out)
}

/// Resolves the target-side name for one archive-side (source) topic,
/// applying the topic-rename mapping (module doc, point 1). Refuses
/// (`DrillError::Operational`) rather than silently reading the archive-side
/// name when `topic` has no entry in `mapping` — see the module doc comment.
fn mapped_topic<'a>(
    mapping: &'a BTreeMap<String, String>,
    topic: &str,
) -> Result<&'a str, DrillError> {
    mapping.get(topic).map(|s| s.as_str()).ok_or_else(|| {
        DrillError::Operational(format!(
            "no target-side mapping for archive topic `{topic}`; refusing to read the wrong topic"
        ))
    })
}

/// What the manifest can support for one selection, and the straddling
/// segments it did not count: `(claimed, straddlers)` where `claimed` is
/// `min(sel.count, Σ record_count over the matched segments wholly inside the
/// sample window)` and `straddlers` is `(how many, how many records they
/// hold)`.
///
/// # Why wholly inside, not overlapping
///
/// The archive side (`OsoCliEngine::fingerprints`) returns exactly the records
/// with `s <= timestamp <= e`, while `record_count` is a property of a whole
/// segment. A segment a window edge falls inside (the normal point-in-time
/// case) holds records the window excludes, so counting it demands
/// fingerprints that cannot exist and fails a correct restore. A straddler's
/// `record_count` is an upper bound, never a claim: the same rule
/// `logweir_core::engine::expected_restored_count` applies to the restored
/// count. Straddlers are returned so the `Unverified` message can disclose
/// them.
///
/// The bound is a lower bound, so it can under-claim inside a straddler: the
/// manifest carries no per-record timestamp, and a bound that over-claims
/// fails correct implementations. A selection the archive answered with
/// nothing is still `Unverified` (`compared > 0` at the call site). A negative
/// `record_count` contributes 0 rather than wrapping a `u64` (interface I5).
fn supportable_claim(s: &SampleSelection, segs: &[&SegmentFacts]) -> (u64, (usize, u64)) {
    let (w0, w1) = s.window;
    let wholly_inside = |g: &SegmentFacts| g.start_timestamp >= w0 && g.end_timestamp <= w1;
    let supported: u64 = segs
        .iter()
        .filter(|g| wholly_inside(g))
        .map(|g| g.record_count.max(0) as u64)
        .sum();
    let straddling: Vec<_> = segs.iter().filter(|g| !wholly_inside(g)).collect();
    let straddling_records: u64 = straddling
        .iter()
        .map(|g| g.record_count.max(0) as u64)
        .sum();
    let cap = s.count as u64;
    let claimed = if segs.is_empty() {
        cap
    } else {
        cap.min(supported)
    };
    (claimed, (straddling.len(), straddling_records))
}

/// The parenthetical the short-sample messages carry when the claim was held
/// down by segments the window cuts across — empty when none were matched, so
/// a message never invents a straddler that is not there.
fn straddler_note((count, records): (usize, u64)) -> String {
    if count == 0 {
        String::new()
    } else {
        format!(
            " ({count} straddling segment{} hold{} {records} records the window may exclude)",
            if count == 1 { "" } else { "s" },
            if count == 1 { "s" } else { "" },
        )
    }
}

/// The sampled lane's lineage check. The restored records `compare`
/// reconciles are keyed by `x-original-offset` in a map, where a repeated
/// source offset collapses and order is invisible. This reads the same
/// consumed head in target order and names every record whose lineage offset
/// repeats one already read (`duplicates`) or is below one (`out of order`;
/// archive order is source-offset order). Records without a lineage header
/// are `compare`'s fallback case and are skipped.
///
/// A new cause for the sampled lane's existing `Failed`, which can only move
/// a verdict to the safer side, over the head the sample read; the whole
/// output is the complete lane's.
#[must_use]
pub fn lineage_faults(consumed: &[ConsumedRecord]) -> Vec<String> {
    let mut seen = std::collections::HashSet::new();
    let mut max_seen: Option<i64> = None;
    let mut out = Vec::new();
    for c in consumed {
        let Some(o) = complete::lineage_offset(&c.headers) else {
            continue;
        };
        if !seen.insert(o) {
            out.push(format!(
                "source offset {o} is restored again at target offset {}",
                c.offset
            ));
            continue;
        }
        if let Some(m) = max_seen.filter(|m| o < *m) {
            out.push(format!(
                "source offset {o} is restored at target offset {} after source offset {m}",
                c.offset
            ));
        }
        max_seen = Some(max_seen.map_or(o, |m| m.max(o)));
    }
    out
}

/// Builds the one verdict for one selection. Every lane's conclusion is a
/// value in the returned `SelectionVerdict`; nothing here decides pass or
/// fail, and nothing here may return "no opinion".
///
/// Consumes from the target, applying the topic-rename mapping before calling
/// `reader`, and calls `compare()` with only this selection's fingerprints and
/// consumed records, so offsets cannot collide across selections (module
/// doc). `count` is `phase4_sample`'s per-partition cap; `from` is 0 because
/// the destination topic is freshly created.
///
/// Both lanes carry the same obligation, positive evidence for at least
/// `claimed`: byte-fingerprint, that many fingerprints, all reconciling;
/// consume-only, that many records read back. Short is unverified, not a
/// smaller successful sample, and short against what the manifest supports
/// (`supportable_claim`), never what it overlaps.
///
/// These findings are drill results (exit 2, signed scorecard): facts about
/// the archive or the restore, like phase 6's `RestoreNoOp`. I/O against the
/// store or the broker stays `Operational`: that is Logweir failing to
/// perform the check.
fn verdict_for_selection(
    reader: &dyn ClusterReader,
    store: &Store,
    facts: &BackupSetFacts,
    s: &SampleSelection,
    mapping: &BTreeMap<String, String>,
    archive: &SelectionArchive,
) -> Result<SelectionVerdict, DrillError> {
    let id = format!("{}/{}", s.topic, s.partition);
    let mapped = mapped_topic(mapping, &s.topic)?;

    let segs = sampled_segments_for(facts, s);
    // `min(sel.count, Σ record_count over the segments wholly inside the
    // window)`: without the cap a 25-record sample of a large partition always
    // reads short; without the sum a window holding fewer records than
    // `records_per_partition` would. With no matched segment the plan's ask
    // stands (and `segments` is already `Unverified`). Every overlapping
    // segment's sha256 is still checked by `segment_evidence`.
    let (claimed, straddlers) = supportable_claim(s, &segs);

    let segments = segment_evidence(store, &segs)?;
    let consumed = reader.consume_range(mapped, s.partition, 0, s.count)?;
    let records_restored = consumed.len() as u64;

    let (records, reconciled, downgrade) = match archive {
        SelectionArchive::Fingerprints(fp) => {
            let (compared, matching, why) = compare(fp, &consumed);
            for w in why {
                tracing::error!(target: "logweir::verify", detail = %w, "reconciliation mismatch");
            }
            // Positive first and non-vacuous (`compared > 0`): at least
            // `claimed` fingerprints came back and all reconciled, so deleting
            // the `matching < compared` arm lands a mismatching sample on
            // `Unverified`. A repeated or backwards lineage offset in the head
            // is examined-and-wrong, whatever `compare` saw through its
            // offset-keyed map.
            let order = lineage_faults(&consumed);
            for w in &order {
                tracing::error!(target: "logweir::verify", detail = %w, "lineage order fault");
            }
            let evidence = if compared > 0
                && compared >= claimed
                && matching == compared
                && order.is_empty()
            {
                Evidence::Verified { checked: matching }
            } else if !order.is_empty() {
                Evidence::Failed {
                    why: format!(
                        "{} restored records repeat or reorder the archive's source offsets: {}",
                        order.len(),
                        order.iter().take(5).cloned().collect::<Vec<_>>().join("; ")
                    ),
                }
            } else if matching < compared {
                Evidence::Failed {
                    why: format!(
                        "{} of {compared} sampled records did not reconcile against the archive",
                        compared - matching
                    ),
                }
            } else if compared == 0 {
                Evidence::Unverified {
                    why: "zero archive fingerprints were available to reconcile against; a \
                          byte-fingerprint comparison cannot verify anything it never sampled, \
                          even when other selections in the same drill produced real matches"
                        .into(),
                }
            } else {
                // "SUPPORTS AT LEAST", not "claims": `claimed` is a LOWER
                // bound over the wholly-inside segments, and the straddlers
                // it did not count are disclosed so an auditor can see the
                // difference between this figure and the manifest's own total
                // (see `supportable_claim`).
                Evidence::Unverified {
                    why: format!(
                        "the archive returned {compared} fingerprints where the manifest \
                         supports at least {claimed} for this selection{}; a short sample is \
                         coverage the drill did not obtain, not a smaller successful sample",
                        straddler_note(straddlers)
                    ),
                }
            };
            // `Some` whenever `compare()` actually ran with something to
            // compare — including the failing and short cases, whose real
            // numbers belong in the signed counters.
            let counts = (compared > 0).then_some((compared, matching));
            (evidence, counts, None)
        }
        SelectionArchive::Unsupported(reason) => {
            // The same obligation at the level this lane can claim: the
            // partition gave back the records the manifest says it holds.
            // Positive first and non-vacuous, the same shape as the
            // byte-fingerprint arm.
            let evidence = if records_restored > 0 && records_restored >= claimed {
                Evidence::Verified {
                    checked: records_restored,
                }
            } else if records_restored == 0 {
                Evidence::Unverified {
                    why: "the target partition gave back zero records, so consume-only \
                          verification proved nothing about this selection"
                        .into(),
                }
            } else {
                // Mirrors the byte-fingerprint lane's wording over the same
                // `claimed` and disclosure.
                Evidence::Unverified {
                    why: format!(
                        "the target partition gave back {records_restored} records where the \
                         manifest supports at least {claimed} for this selection{}; a short \
                         read-back is coverage the drill did not obtain",
                        straddler_note(straddlers)
                    ),
                }
            };
            (evidence, None, Some(reason.clone()))
        }
    };

    Ok(SelectionVerdict {
        id,
        claimed,
        segments,
        records,
        records_restored,
        reconciled,
        downgrade,
    })
}

/// Folds `classify_parity` over every entry in `mapping`, applying the
/// topic-rename mapping (module doc) before calling `reader`. The target
/// partition count is read (`reader.end_offsets`), and so is the target
/// replication factor (`reader.replication_factors`): a plan carrying
/// `target.topic_configuration` creates each topic at its own bounded factor,
/// not at `plan.default_replication_factor`. Only a reader that cannot answer
/// (a test double) falls back to the plan's factor.
///
/// # A source value that is not recorded is not assessed
///
/// The source's partition count is the manifest's `original_partition_count`
/// and its replication factor the manifest's `source_replication_factor`,
/// else the verified receipt's ([`SourceConfigCoverage::replication_factor`],
/// format 1.3.0). Where neither records one (engine 0.23.3 records the factor
/// for the first topic it saves only; manifests before engine 0.17 record
/// neither) the key is named in `not_assessed` as `"<target>: <key>
/// (notRecorded)"`, with its fail-safe twin in `unexpected_divergence`, and
/// is never compared.
///
/// Refuses an empty `mapping`: a parity check over zero topics reads exactly
/// like "checked every topic and found no divergence".
///
/// # Parity is assessed only where the capture was
///
/// A source whose DescribeConfigs was denied at capture has
/// `configurations: {}`, and comparing against that empty record reads as no
/// divergence. So every mapped topic whose source coverage (from the verified
/// receipt, `coverage`) is not `captured` is named in `not_assessed` as
/// `"<target>: configuration (<coverage>)"`, `unknown` when the plan binds no
/// recovery point or the receipt predates 1.1.0. A refused read of the
/// target's configuration is named `"<target>: configuration
/// (targetReadDenied)"` and its keys are not compared.
///
/// The archive's own record is still compared wherever the target was read:
/// a difference it shows is a fact whatever the coverage; a `not_assessed`
/// entry withdraws only the silence. Partition count and replication factor
/// come from metadata and are classified whatever the coverage, wherever the
/// source's value is recorded.
///
/// # The mode decides `intended` versus `not_reconstructed`
///
/// `mode` is the run's `target.mode`, and [`classify_parity`] is where it
/// decides the label. `not_reconstructed` is `Some` always from here, like
/// `not_assessed`: `Some([])` for every scratch drill (its deviations are
/// intended) and for a `newTopic` restore whose compared settings matched;
/// absent stays the spelling of "not recorded".
fn classify_parity_all(
    facts: &BackupSetFacts,
    reader: &dyn ClusterReader,
    mapping: &BTreeMap<String, String>,
    plan: &RestorePlan,
    coverage: &SourceConfigCoverage,
    mode: TargetMode,
) -> Result<TopicParity, DrillError> {
    if mapping.is_empty() {
        return Err(DrillError::Operational(
            "topic parity check ran with zero mapped topics; refusing to report agreement over \
             an empty set"
                .into(),
        ));
    }
    let mut intended_all = Vec::new();
    let mut unexpected_all = Vec::new();
    let mut not_assessed = Vec::new();
    let mut not_reconstructed_all = Vec::new();
    for (src, tgt) in mapping {
        let Some(t) = facts.topics.iter().find(|t| &t.name == src) else {
            return Err(DrillError::Operational(format!(
                "topic parity check: no archive facts for mapped source topic `{src}`"
            )));
        };
        // A refused target read is a recorded fact, not an empty configuration
        // and not an aborted drill; any other failure is operational.
        let target_cfg = match reader.topic_configs(tgt) {
            Ok(cfg) => Some(cfg),
            Err(logweir_kafka::reader::KafkaError::NotAuthorized(_)) => None,
            Err(other) => return Err(other.into()),
        };
        let tgt_partitions = reader.end_offsets(tgt)?.len() as i32;
        // The factor the topic HAS, measured: with `target.topic_configuration`
        // it is the bounded source factor, never the plan's default.
        let tgt_rf = reader
            .replication_factors(std::slice::from_ref(tgt))?
            .get(tgt)
            .and_then(|f| i16::try_from(*f).ok())
            .unwrap_or(plan.default_replication_factor);
        // What the source's record says, and nothing when it says nothing,
        // never the target's own value: the manifest's factor first, then the
        // verified receipt's.
        let src_partitions = t.original_partition_count;
        let src_rf = t.source_replication_factor.or_else(|| {
            coverage
                .replication_factor(src)
                .and_then(|f| i16::try_from(f).ok())
        });
        let why = match (coverage.of(src), &target_cfg) {
            (ConfigCoverage::Captured, Some(_)) => None,
            (ConfigCoverage::Captured, None) => Some(NOT_ASSESSED_TARGET_READ_DENIED),
            (source, _) => Some(source.wire_name()),
        };
        if let Some(why) = why {
            not_assessed.push(not_assessed_entry(tgt, CONFIGURATION, why));
            // The fail-safe twin in the array every reader already shows: a
            // reader that predates `not_assessed` must never see this topic's
            // silence as parity.
            unexpected_all.push(not_assessed_marker(tgt, why));
        }
        let unread = BTreeMap::new();
        let (source_cfg, target_cfg) = match &target_cfg {
            Some(cfg) => (&t.configurations, cfg),
            // Nothing to compare the archive's record against.
            None => (&unread, &unread),
        };
        let classes = classify_parity(
            mode,
            source_cfg,
            target_cfg,
            src_partitions,
            tgt_partitions,
            src_rf,
            tgt_rf,
        );
        intended_all.extend(classes.intended.into_iter().map(|k| format!("{tgt}: {k}")));
        unexpected_all.extend(
            classes
                .unexpected
                .into_iter()
                .map(|k| format!("{tgt}: {k}")),
        );
        not_reconstructed_all.extend(
            classes
                .not_reconstructed
                .into_iter()
                .map(|k| format!("{tgt}: {k}")),
        );
        // A partition count or replication factor the source's record lacks,
        // named with its fail-safe twin, as an unassessed configuration is.
        for key in classes.not_assessed {
            not_assessed.push(not_assessed_entry(tgt, &key, NOT_ASSESSED_NOT_RECORDED));
            unexpected_all.push(not_assessed_twin(tgt, &key, NOT_ASSESSED_NOT_RECORDED));
        }
    }
    intended_all.sort();
    unexpected_all.sort();
    not_assessed.sort();
    not_reconstructed_all.sort();
    Ok(TopicParity {
        intentionally_deviated: intended_all,
        unexpected_divergence: unexpected_all,
        // `Some`, always, from here: phase 7 RAN, so "every topic assessed"
        // (`Some([])`) is a claim this function can make, and absent stays the
        // spelling of "not recorded".
        not_assessed: Some(not_assessed),
        // `Some`, always, for the same reason (format 1.2.0). Every
        // entry is also in `unexpected_divergence` (`classify_parity` moved
        // it there), which `validate_invariants`' NR-2 holds phase 8 to
        // before it signs.
        not_reconstructed: Some(not_reconstructed_all),
    })
}

/// The `not_assessed` reason for a topic whose SOURCE configuration was
/// captured but whose TARGET configuration read was refused.
pub const NOT_ASSESSED_TARGET_READ_DENIED: &str = "targetReadDenied";

/// The `not_assessed` reason for a `partition_count` or
/// `replication_factor` the SOURCE's record does not carry: neither the
/// archive's manifest nor, for the factor, the verified receipt's
/// `topic_configuration` names one.
pub const NOT_ASSESSED_NOT_RECORDED: &str = "notRecorded";

/// What a configuration `not_assessed` entry names: the topic's overrides.
const CONFIGURATION: &str = "configuration";

/// One `topic_parity.not_assessed` entry: `"<target topic>: <what> (<why>)"`,
/// where `<what>` is `configuration`, `partition_count` or
/// `replication_factor`.
fn not_assessed_entry(target_topic: &str, what: &str, why: &str) -> String {
    format!("{target_topic}: {what} ({why})")
}

/// The fail-safe twin of a `not_assessed` entry in `unexpected_divergence`:
/// `"<target topic>: <what> not assessed (<why>)"` (see
/// [`not_assessed_marker`] for why it exists). Its `<key>` — the
/// text after the last `": "` — is `<what> not assessed (<why>)`, never one of
/// the four settings the restore decides, so the scorecard's NR-5 arm does not
/// read a `partition_count` or `replication_factor` twin as a deviation
/// `not_reconstructed` must name.
fn not_assessed_twin(target_topic: &str, what: &str, why: &str) -> String {
    format!("{target_topic}: {what} not assessed ({why})")
}

/// The entry phase 7 also writes into `unexpected_divergence` for every topic
/// it names in `not_assessed`.
///
/// **Why.** `not_assessed` is new in format 1.1.0, so a reader that predates
/// it sees only the two older arrays and reads an empty
/// `unexpected_divergence` as configuration parity. This twin keeps that
/// reader's conclusion fail-safe.
///
/// **Shape.** `"<target topic>: configuration not assessed (<why>)"`, where
/// `<why>` is the `not_assessed` reason. A configuration key never contains a
/// space, so a reader that parses `"<topic>: <key>"` entries can tell this
/// one apart; the authoritative list stays `not_assessed`.
/// `not_assessed_twin` writes the same for `partition_count` and
/// `replication_factor`.
#[must_use]
pub fn not_assessed_marker(target_topic: &str, why: &str) -> String {
    not_assessed_twin(target_topic, CONFIGURATION, why)
}

/// The maximum `ConsumedRecord.timestamp_ms` across every mapped target
/// topic/partition, which phase 8's RPO reads. It reads the last record of
/// each partition (`end_offsets`, then `consume_range` at `hi - 1`, count 1),
/// not the sample: a `head` anchor's sample holds the oldest records, so its
/// maximum would overstate the signed RPO. One extra round trip per
/// partition, and it assumes offset order approximates timestamp order.
///
/// Refuses an empty `mapping` and zero records across every mapped partition
/// (phase 6's post-condition makes the latter unreachable from `run`), so no
/// caller gets a fabricated `0` (1970-01-01) as a real timestamp.
fn newest_ts(
    reader: &dyn ClusterReader,
    mapping: &BTreeMap<String, String>,
) -> Result<i64, DrillError> {
    if mapping.is_empty() {
        return Err(DrillError::Operational(
            "newest_ts called with zero mapped topics; there is nothing to measure".into(),
        ));
    }
    let mut newest: Option<i64> = None;
    for target in mapping.values() {
        for (partition, hi) in reader.end_offsets(target)? {
            if hi <= 0 {
                continue;
            }
            let recs = reader.consume_range(target, partition, hi - 1, 1)?;
            if let Some(r) = recs.first() {
                newest = Some(newest.map_or(r.timestamp_ms, |n| n.max(r.timestamp_ms)));
            }
        }
    }
    newest.ok_or_else(|| {
        DrillError::Operational(
            "no restored records found on any mapped target topic; cannot measure the newest \
             restored timestamp"
                .into(),
        )
    })
}

/// Guard G-WIN, second half: the count the target holds against the bound
/// the manifest puts on `[floor_ms, pit_ms]`. `None` inside the bound;
/// `Some(text)`, which `roll_up` turns into `IntegrityResult::Fail` (exit 2),
/// outside it.
///
/// # Both numbers are measured
///
/// The expectation is `logweir_core::engine::expected_restored_count` over
/// the manifest facts, the actual count `ClusterReader::end_offsets`. An
/// expectation parsed back out of the document Logweir emitted would be a
/// tautology (the engine filters `s <= timestamp <= e`, so both sides shrink
/// together), so the two instants arrive as plain `i64` parameters, and
/// `the_count_expectation_never_reads_the_rendered_document`
/// (`crates/logweir/tests/windowed_reconciliation.rs`) reads this function's
/// source to keep it that way.
///
/// # Scope and counting
///
/// The facts are restricted to the source topics `mapping` names and the
/// count summed over their targets: an archive set may hold topics this
/// restore does not name. Every target topic was created in this run, so its
/// high watermark is its record count; a negative watermark contributes 0.
///
/// # And per mapped partition
///
/// The sum is reported first when outside its bound; then each mapped
/// partition is held to its own bound
/// (`logweir_core::engine::partition_count_bound`) and every one outside it
/// is named: an empty partition the manifest proves holds records, a count
/// outside the bound, and records in a partition the manifest lists no
/// source partition for. The engine does not repartition, so target
/// partition `p` holds source partition `p`'s window. This catches a restore
/// an engine stopped early whichever partitions the sample selected.
fn check_restored_count(
    reader: &dyn ClusterReader,
    facts: &BackupSetFacts,
    mapping: &BTreeMap<String, String>,
    floor_ms: i64,
    pit_ms: i64,
    selection: &ReplaySelection,
) -> Result<Option<String>, DrillError> {
    // The bound is the SELECTION's. A partition the plan did not
    // select is no partition of this restore: it proves nothing and must be
    // empty on the target (below). A plan with no subset restricts nothing.
    let facts = &selection.restrict(facts);
    // Unreachable from `run` (phase 4 and `run`'s own `sel.is_empty()` refuse
    // an empty mapping first), and kept: over zero topics the bound is
    // `(0, 0)` and the sum is 0, so this would answer `Ok(None)`, an agreement
    // over nothing, to any future caller. `classify_parity_all` refuses an
    // empty mapping for the same reason
    // (`classify_parity_all_refuses_an_empty_mapping`).
    if mapping.is_empty() {
        return Err(DrillError::Operational(
            "restored-count bound ran with zero mapped topics; refusing to report a count \
             inside a bound computed over nothing"
                .into(),
        ));
    }
    let named = BackupSetFacts {
        topics: facts
            .topics
            .iter()
            .filter(|t| mapping.contains_key(&t.name))
            .cloned()
            .collect(),
        ..facts.clone()
    };
    let (lower, upper) = expected_restored_count(&named, floor_ms, pit_ms);

    // The same bound per mapped partition, off the same end-offset reads. The
    // sum alone has slack: a topic an engine never restored (a SIGTERM stops
    // it between topics, exit 0) can hide inside it. A straddler whose first
    // or last record is inside the window proves at least one record, so an
    // empty partition the manifest proves non-empty is outside its own bound.
    let mut restored = 0u64;
    let mut per_partition = Vec::new();
    for (src, target) in mapping {
        let ends: BTreeMap<i32, u64> = reader
            .end_offsets(target)?
            .into_iter()
            .map(|(p, hi)| (p, hi.max(0) as u64))
            .collect();
        restored += ends.values().sum::<u64>();
        let mut listed = std::collections::BTreeSet::new();
        for p in named
            .topics
            .iter()
            .filter(|t| &t.name == src)
            .flat_map(|t| t.partitions.iter())
        {
            listed.insert(p.partition_id);
            let (lo, up) = partition_count_bound(p, floor_ms, pit_ms);
            let hi = ends.get(&p.partition_id).copied().unwrap_or(0);
            let id = p.partition_id;
            if hi == 0 && lo > 0 {
                per_partition.push(format!(
                    "{src}/{id}: {target}/{id} holds no record but the manifest proves at least \
                     {lo} in the window [{floor_ms}, {pit_ms}]"
                ));
            } else if hi < lo || hi > up {
                per_partition.push(format!(
                    "{src}/{id}: {target}/{id} holds {hi} records but the manifest bounds this \
                     partition's window [{floor_ms}, {pit_ms}] at [{lo}, {up}]"
                ));
            }
        }
        for (id, hi) in &ends {
            if *hi > 0 && !selection.selects_partition(src, *id) {
                per_partition.push(format!(
                    "{target}/{id} holds {hi} records but the plan's restore.partitions does not \
                     select partition {id} of {src}"
                ));
            } else if *hi > 0 && !listed.contains(id) {
                per_partition.push(format!(
                    "{target}/{id} holds {hi} records but the manifest lists no partition {id} \
                     of {src}"
                ));
            }
        }
    }

    let mut findings = Vec::new();
    if restored < lower || restored > upper {
        findings.push(format!(
            "restored {restored} records but the manifest bounds the window \
             [{floor_ms}, {pit_ms}] at [{lower}, {upper}]"
        ));
    }
    findings.extend(per_partition);
    Ok((!findings.is_empty()).then(|| findings.join("; ")))
}

/// The engine's offset report against
/// the partitions the manifest PROVES hold records in the restore window
/// (`partition_count_bound`'s `lower` above 0): `Some(text)` naming every
/// such mapped partition the report has no entry for.
///
/// The pinned engine writes its report only from a restore that returned
/// `Ok`, and a SIGTERM makes it return `Ok` after the topic it is on, so the
/// report of an early-stopped restore exists and lacks every topic it never
/// started (`logweir_core::engine::EngineReport`). The report keys its
/// entries by the TARGET topic, so the lookup is `(mapping[src], partition)`.
///
/// Only a negative signal: an entry vouches for nothing, and a report this
/// build could not read, or none at all, checks nothing and is logged — the
/// per-partition bound and the sample are still the checks that bind.
fn check_engine_report(
    facts: &BackupSetFacts,
    mapping: &BTreeMap<String, String>,
    floor_ms: i64,
    pit_ms: i64,
    report: &EngineReport,
) -> Option<String> {
    let entries = match report {
        EngineReport::Read(entries) => entries,
        EngineReport::Absent => {
            tracing::warn!(target: "logweir::verify",
                           "no engine offset report to check; whether the engine stopped \
                            before a mapped topic rests on the count bound and the sample");
            return None;
        }
        EngineReport::Unreadable(why) => {
            tracing::warn!(target: "logweir::verify", reason = %why,
                           "the engine offset report could not be read and checks nothing; \
                            whether the engine stopped before a mapped topic rests on the \
                            count bound and the sample");
            return None;
        }
    };
    let mut missing = Vec::new();
    for (src, target) in mapping {
        for p in facts
            .topics
            .iter()
            .filter(|t| &t.name == src)
            .flat_map(|t| t.partitions.iter())
        {
            let (lo, _) = partition_count_bound(p, floor_ms, pit_ms);
            if lo > 0 && !entries.contains(&(target.clone(), p.partition_id)) {
                missing.push(format!("{src}/{id} -> {target}/{id}", id = p.partition_id));
            }
        }
    }
    (!missing.is_empty()).then(|| {
        format!(
            "the engine's offset report has no entry for {} mapped partition(s) the manifest \
             proves hold records in the window [{floor_ms}, {pit_ms}]: {}; an engine that \
             stopped early (it honours a SIGTERM between topics and exits 0) reports only the \
             topics it finished",
            missing.len(),
            missing.join(", ")
        )
    })
}

/// Phase 7's four checks, routed through one ledger and one roll-up: `run`
/// builds exactly one `SelectionVerdict` per `SampleSelection` and hands the
/// ledger to `roll_up`, the single place `IntegrityResult` is decided.
///
/// `mode` is the run's `target.mode`: it decides whether the deviations the
/// restore's topic creation makes are `intended` (scratch) or
/// `not_reconstructed` (`newTopic`), see [`classify_parity`]. No default on
/// purpose: a default is how the scratch rationale reached every mode.
#[allow(clippy::too_many_arguments)]
pub fn run(
    engine: &dyn DataEngine,
    reader: &dyn ClusterReader,
    store: &Store,
    facts: &BackupSetFacts,
    sel: &[SampleSelection],
    mapping: &BTreeMap<String, String>,
    plan: &RestorePlan,
    coverage: &SourceConfigCoverage,
    mode: TargetMode,
) -> Result<VerifyOutcome, DrillError> {
    run_with_coverage(
        engine,
        reader,
        store,
        facts,
        sel,
        mapping,
        plan,
        coverage,
        mode,
        Coverage::Sampled,
        None,
    )
}

/// [`run`], with the plan's `sample.coverage` and
/// `sample.complete_max_records`. `run` is this with `Coverage::Sampled`.
///
/// - **`Sampled`**: the sampled lane below; a restored head whose
///   `x-original-offset` repeats or goes backwards fails its selection
///   (`lineage_faults`), and the signed block says `coverage: sampled`,
///   `header_order: notVerified`, with the sampled partitions' structured gap
///   and pruned ranges.
/// - **`Complete`**: [`complete::run`]: every segment of every restored
///   partition, the expected output from each record's own timestamp, every
///   restored record compared, exact counts. The manifest's first/last count
///   bound is not consulted (it fails a correct point-in-time restore and
///   passes a skipped segment); the same `roll_up` decides from one verdict
///   per partition.
#[allow(clippy::too_many_arguments)]
pub fn run_with_coverage(
    engine: &dyn DataEngine,
    reader: &dyn ClusterReader,
    store: &Store,
    facts: &BackupSetFacts,
    sel: &[SampleSelection],
    mapping: &BTreeMap<String, String>,
    plan: &RestorePlan,
    coverage: &SourceConfigCoverage,
    mode: TargetMode,
    verify: Coverage,
    complete_max_records: Option<u64>,
) -> Result<VerifyOutcome, DrillError> {
    run_after_restore(
        engine,
        reader,
        store,
        facts,
        sel,
        mapping,
        plan,
        coverage,
        mode,
        verify,
        complete_max_records,
        &EngineReport::Absent,
    )
}

/// [`run_with_coverage`], with the engine's offset report as phase 6 read it,
/// what the drill orchestrator calls. A report that lacks a mapped
/// partition the manifest proves holds records in the restore window
/// (`check_engine_report`) is a whole-drill finding beside the count bound, on
/// both lanes: `Fail`, first in `partial_reason`. `run_with_coverage` is this
/// with no report (`EngineReport::Absent`), for callers that verify a target
/// no engine of theirs wrote.
#[allow(clippy::too_many_arguments)]
pub fn run_after_restore(
    engine: &dyn DataEngine,
    reader: &dyn ClusterReader,
    store: &Store,
    facts: &BackupSetFacts,
    sel: &[SampleSelection],
    mapping: &BTreeMap<String, String>,
    plan: &RestorePlan,
    coverage: &SourceConfigCoverage,
    mode: TargetMode,
    verify: Coverage,
    complete_max_records: Option<u64>,
    engine_report: &EngineReport,
) -> Result<VerifyOutcome, DrillError> {
    // Zero selections scanned is never a positive result. This one is
    // operational, a plan that named no partition to sample; `roll_up`
    // refuses an empty ledger independently anyway.
    if sel.is_empty() {
        return Err(DrillError::Operational(
            "phase7_verify::run called with zero sample selections; a fingerprint comparison \
             over an empty set would report a pass that means nothing"
                .into(),
        ));
    }

    // (a) + (c). One verdict per selection, built by indexing `sel` itself and
    // refusing a missing archive answer, never by zipping with what
    // `probe_archive_modes` returned: `zip` stops at the shorter side, and a
    // ledger missing a selection is `all(fully_verified)` over the survivors.
    // `roll_up` is sound only if the ledger covers every selection.
    let selected: Vec<(String, i32)> = sel.iter().map(|s| (s.topic.clone(), s.partition)).collect();
    // The plan's replay selection (its own start, its subsets), which every
    // whole-drill check below is judged over. A plan that states none selects
    // every partition from the archive's floor.
    let selection = ReplaySelection::from_plan(plan);
    let (verdicts, count_bound, block) = match verify {
        Coverage::Sampled => {
            let archives = probe_archive_modes(engine, sel)?;
            let mut verdicts = Vec::with_capacity(sel.len());
            for (i, s) in sel.iter().enumerate() {
                let archive = archives.get(i).ok_or_else(|| {
                    DrillError::Operational(format!(
                        "probe_archive_modes answered for {} of {} selections; refusing to \
                         verify a ledger that cannot cover {}/{}",
                        archives.len(),
                        sel.len(),
                        s.topic,
                        s.partition
                    ))
                })?;
                verdicts.push(verdict_for_selection(
                    reader, store, facts, s, mapping, archive,
                )?);
            }

            // Guard **G-WIN**, second half. The two instants are the PLAN's
            // window — `(floor, point_in_time-or-window_end)`, whose floor
            // plan construction has already checked against the manifest
            // (`build_plan_with_floor`, exit 3) and whose rendered spelling
            // phase 5 has already re-derived from the manifest
            // (`phase5_preflight::check_rendered_window_floor`).
            // `check_restored_count` takes them as integers so it can reach no
            // document of its own.
            let count_bound = check_restored_count(
                reader,
                facts,
                mapping,
                plan.time_window.0.timestamp_millis(),
                plan.time_window.1.timestamp_millis(),
                &selection,
            )?;
            if let Some(why) = &count_bound {
                tracing::error!(target: "logweir::verify", detail = %why,
                                "restored count is outside the manifest's bound for the window");
            }
            let (gaps, pruned) = complete::ranges_for(facts, &selected);
            let block = Verification {
                coverage: Coverage::Sampled.as_str().to_string(),
                comparison_basis: COMPARISON_BASIS_ARCHIVE.to_string(),
                // The sampled lane reconciles `record_fingerprint`, which
                // sorts headers: it never verified their order.
                header_order: HEADER_ORDER_NOT_VERIFIED.to_string(),
                application: APPLICATION_NOT_ATTEMPTED.to_string(),
                gaps,
                pruned,
                complete: None,
            };
            (verdicts, count_bound, block)
        }
        Coverage::Complete => {
            let out = complete::run(
                reader,
                store,
                facts,
                &selected,
                mapping,
                plan,
                complete_max_records,
            )?;
            let block = Verification {
                coverage: Coverage::Complete.as_str().to_string(),
                comparison_basis: COMPARISON_BASIS_ARCHIVE.to_string(),
                header_order: HEADER_ORDER_VERIFIED.to_string(),
                application: APPLICATION_NOT_ATTEMPTED.to_string(),
                gaps: out.gaps,
                pruned: out.pruned,
                complete: Some(out.block),
            };
            // No count BOUND here: each partition's verdict carries its exact
            // count, and the bound's first/last reading is the defect this
            // lane exists to replace.
            (out.verdicts, None, block)
        }
    };

    // The engine's own report, on both lanes, after the count bound in the
    // joined text: both are whole-drill findings `roll_up` takes as one
    // argument and can only turn into a `Fail`.
    let report_finding = check_engine_report(
        // Only the selected partitions must be in the report.
        &selection.restrict(facts),
        mapping,
        plan.time_window.0.timestamp_millis(),
        plan.time_window.1.timestamp_millis(),
        engine_report,
    );
    if let Some(why) = &report_finding {
        tracing::error!(target: "logweir::verify", detail = %why,
                        "the engine's offset report lacks a mapped partition with records in \
                         the window");
    }
    let whole_drill: Vec<String> = count_bound.into_iter().chain(report_finding).collect();
    let whole_drill = (!whole_drill.is_empty()).then(|| whole_drill.join("; "));
    let mut integrity = roll_up(&verdicts, whole_drill.as_deref());
    integrity.verification = Some(block);
    for v in &verdicts {
        tracing::info!(target: "logweir::verify", selection = %v.id, claimed = v.claimed,
                       segments = ?v.segments, records = ?v.records,
                       records_restored = v.records_restored, "selection verdict");
    }
    // The sum over every selection's own consumed count — see
    // `records_restored_is_the_consumed_count_not_matched_plus_mismatched`.
    let records_restored: u64 = verdicts.iter().map(|v| v.records_restored).sum();

    // (d) Topic-config parity, assessed only where the verified receipt says
    // the source configuration was captured, and labelled by the run's mode.
    let topic_parity = classify_parity_all(facts, reader, mapping, plan, coverage, mode)?;

    Ok(VerifyOutcome {
        integrity,
        topic_parity,
        records_restored,
        newest_restored_ts_ms: newest_ts(reader, mapping)?,
        verified_at: chrono::Utc::now(),
    })
}

#[cfg(test)]
mod tests {
    //! Direct, fine-grained coverage of the private helpers' own empty-set
    //! guards — narrower and faster than driving them through `run`.
    //! `crates/logweir/tests/verify_phase.rs` additionally pins each of these
    //! at its actual call site inside `run`.
    use super::*;
    use logweir_core::engine::{BackupSetRef, PartitionFacts, TopicFacts};

    /// Arm NR-5 reads `logweir_core::scorecard::
    /// RESTORE_DECIDED_SETTINGS` as "the keys phase 7 can label intended or
    /// not reconstructed". Driven with EVERY key different, a `newTopic`
    /// classification names exactly that set, and a scratch one labels
    /// exactly that set intended, so the core list and this function cannot
    /// drift apart without one of these failing.
    #[test]
    fn classify_parity_decides_exactly_the_core_settings() {
        let source: BTreeMap<String, String> = [
            ("cleanup.policy", "compact"),
            ("retention.ms", "604800000"),
            ("min.insync.replicas", "2"),
            ("max.message.bytes", "2097152"),
        ]
        .into_iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect();
        let core: Vec<String> = logweir_core::scorecard::RESTORE_DECIDED_SETTINGS
            .iter()
            .map(|k| k.to_string())
            .collect();
        let nt = classify_parity(
            TargetMode::NewTopic,
            &source,
            &BTreeMap::new(),
            Some(6),
            3,
            Some(3),
            1,
        );
        assert_eq!(nt.not_reconstructed, core);
        assert!(nt.intended.is_empty());
        assert!(nt.not_assessed.is_empty());
        let sc = classify_parity(
            TargetMode::Scratch,
            &source,
            &BTreeMap::new(),
            Some(6),
            3,
            Some(3),
            1,
        );
        assert_eq!(sc.intended, core);
        assert!(sc.not_reconstructed.is_empty());
        assert!(sc.not_assessed.is_empty());
        // The other keys stay unexpected in both modes, never decided.
        for classes in [&nt, &sc] {
            assert!(classes
                .unexpected
                .contains(&"min.insync.replicas".to_string()));
            assert!(classes
                .unexpected
                .contains(&"max.message.bytes".to_string()));
        }
    }

    /// A source partition count or replication factor that is not recorded is
    /// neither a match nor a deviation, in both modes, whatever the target's
    /// value (including the target's own). A recorded `0` or less is no count
    /// either. The control: the same values recorded are compared.
    #[test]
    fn a_source_layout_that_is_not_recorded_is_not_assessed_never_matched() {
        let none = BTreeMap::new();
        for mode in [TargetMode::Scratch, TargetMode::NewTopic] {
            for (sp, sr) in [(None, None), (Some(0), Some(0)), (Some(-1), Some(-3))] {
                let c = classify_parity(mode, &none, &none, sp, 3, sr, 1);
                assert_eq!(
                    c.not_assessed,
                    vec![
                        "partition_count".to_string(),
                        "replication_factor".to_string()
                    ],
                    "{mode:?} {sp:?} {sr:?}"
                );
                assert!(c.intended.is_empty(), "{mode:?}: {c:?}");
                assert!(c.unexpected.is_empty(), "{mode:?}: {c:?}");
                assert!(c.not_reconstructed.is_empty(), "{mode:?}: {c:?}");
            }
            // Only the factor missing: the count is still compared.
            let c = classify_parity(mode, &none, &none, Some(6), 3, None, 1);
            assert_eq!(c.not_assessed, vec!["replication_factor".to_string()]);
            let decided = if mode == TargetMode::Scratch {
                &c.intended
            } else {
                &c.not_reconstructed
            };
            assert_eq!(decided, &vec!["partition_count".to_string()]);
            // The control: recorded and equal is a match, recorded and
            // different a deviation, and nothing is not assessed.
            let same = classify_parity(mode, &none, &none, Some(3), 3, Some(1), 1);
            assert_eq!(same, ParityClasses::default(), "{mode:?}");
            let differ = classify_parity(mode, &none, &none, Some(3), 3, Some(3), 1);
            assert!(differ.not_assessed.is_empty());
            assert!(
                differ.intended.contains(&"replication_factor".to_string())
                    || differ
                        .not_reconstructed
                        .contains(&"replication_factor".to_string()),
                "{mode:?}: {differ:?}"
            );
        }
    }

    /// The twin's key, as arm NR-5 reads it (`parity_key`: the text
    /// after the LAST `": "`), is never one of the settings the restore
    /// decides, so a `newTopic` document carrying a `replication_factor`
    /// twin is not refused for omitting it from `not_reconstructed`.
    #[test]
    fn a_not_assessed_twin_is_never_a_restore_decided_key() {
        for what in [CONFIGURATION, PARTITION_COUNT, REPLICATION_FACTOR] {
            let twin = not_assessed_twin("restore-x-orders", what, NOT_ASSESSED_NOT_RECORDED);
            let key = twin.rsplit_once(": ").map_or(twin.as_str(), |(_, k)| k);
            assert!(
                !logweir_core::scorecard::RESTORE_DECIDED_SETTINGS.contains(&key),
                "{twin}"
            );
            assert_eq!(
                not_assessed_entry("restore-x-orders", what, NOT_ASSESSED_NOT_RECORDED),
                format!("restore-x-orders: {what} (notRecorded)")
            );
        }
        assert_eq!(
            not_assessed_marker("t", "unknown"),
            "t: configuration not assessed (unknown)",
            "FX-4's marker is byte for byte what it was"
        );
    }

    fn facts_one_segment() -> BackupSetFacts {
        BackupSetFacts {
            backup_id: "b".into(),
            created_at: chrono::Utc::now(),
            source_cluster_id: None,
            manifest_sha256: "sha256:0".into(),
            manifest_version_id: None,
            consumer_group_snapshot_sha256: None,
            topics: vec![TopicFacts {
                name: "orders".into(),
                original_partition_count: Some(1),
                source_replication_factor: Some(3),
                configurations: BTreeMap::new(),
                partitions: vec![PartitionFacts {
                    partition_id: 0,
                    segments: vec![SegmentFacts {
                        key: "logweir/k".into(),
                        start_offset: 0,
                        end_offset: 9,
                        start_timestamp: 0,
                        end_timestamp: 100,
                        record_count: 10,
                        sha256: "sha256:whatever".into(),
                        uploaded_at: 0,
                    }],
                    gaps: vec![],
                    pruned: vec![],
                }],
            }],
        }
    }

    fn sel_for(topic: &str, partition: i32, window: (i64, i64)) -> SampleSelection {
        SampleSelection {
            set: BackupSetRef {
                backup_id: "b".into(),
                manifest_key: "b/manifest.json".into(),
            },
            topic: topic.into(),
            partition,
            anchor: logweir_core::spec::Anchor::Head,
            count: 10,
            window,
        }
    }

    /// A `Store` holding `key` with contents whose sha256 is returned, so a
    /// selection's segment lane can be driven to `Verified` on purpose.
    fn store_with(key: &str, bytes: &[u8]) -> (Store, String) {
        let store = Store::in_memory("logweir");
        store.put_create_only(key, bytes).unwrap();
        (store, logweir_core::ids::sha256_prefixed(bytes))
    }

    /// A `SelectionVerdict` with both lanes forced to the given `Evidence` —
    /// the direct way to drive `roll_up` without standing up a whole `run`.
    fn verdict(id: &str, segments: Evidence, records: Evidence) -> SelectionVerdict {
        SelectionVerdict {
            id: id.into(),
            claimed: 10,
            segments,
            records,
            records_restored: 10,
            reconciled: Some((10, 10)),
            downgrade: None,
        }
    }

    // `sampled_segments_for`: a selection matching nothing can never be
    // counted as checked; the empty match produces `Unverified`.

    #[test]
    fn sampled_segments_for_a_partition_the_facts_lack_matches_nothing_and_is_unverified() {
        let facts = facts_one_segment();
        // A selection naming a partition the facts do not have: zero segments
        // can possibly match. The empty Vec must not read as "checked and
        // fine" — `segment_evidence` over it is `Unverified`, never
        // `Verified`, so the selection can never reach a `Pass`.
        let segs = sampled_segments_for(&facts, &sel_for("orders", 7, (0, 100)));
        assert!(segs.is_empty());
        let (store, _) = store_with("logweir/k", b"anything");
        let ev = segment_evidence(&store, &segs).unwrap();
        assert!(
            ev.is_unverified(),
            "zero matched segments must be Unverified, never Verified: {ev:?}"
        );
        assert!(ev.why().unwrap().contains("no archive segment matches"));
    }

    #[test]
    fn sampled_segments_for_finds_the_real_match() {
        let facts = facts_one_segment();
        let segs = sampled_segments_for(&facts, &sel_for("orders", 0, (0, 100)));
        assert_eq!(segs.len(), 1);
        assert_eq!(segs[0].key, "logweir/k");
    }

    /// The right topic and partition, but a window that does not overlap the
    /// segment's `start_timestamp..end_timestamp` (0..100): excluded. Pins the
    /// window filter itself. KILLS: the window filter deleted.
    #[test]
    fn sampled_segments_for_excludes_a_segment_outside_the_requested_window() {
        let facts = facts_one_segment();
        let segs = sampled_segments_for(&facts, &sel_for("orders", 0, (200, 300)));
        assert!(segs.is_empty());
        let (store, _) = store_with("logweir/k", b"anything");
        assert!(segment_evidence(&store, &segs).unwrap().is_unverified());
    }

    /// A ledger holding one fully verified selection and one whose segments
    /// matched nothing is `Partial`, and names the empty one: an empty
    /// selection cannot hide behind a healthy sibling.
    #[test]
    fn one_selection_matching_no_segment_cannot_hide_behind_a_healthy_sibling() {
        let facts = facts_one_segment();
        let healthy = sampled_segments_for(&facts, &sel_for("orders", 0, (0, 100)));
        let empty = sampled_segments_for(&facts, &sel_for("orders", 7, (0, 100)));
        assert_eq!(healthy.len(), 1);
        assert!(empty.is_empty());

        let (store, _) = store_with("logweir/k", b"anything");
        let integrity = roll_up(
            &[
                verdict(
                    "orders/0",
                    Evidence::Verified { checked: 1 },
                    Evidence::Verified { checked: 10 },
                ),
                verdict(
                    "orders/7",
                    segment_evidence(&store, &empty).unwrap(),
                    Evidence::Verified { checked: 10 },
                ),
            ],
            None,
        );
        assert_eq!(
            integrity.result,
            IntegrityResult::Partial,
            "a selection whose segments matched nothing must forbid a Pass no matter what a \
             sibling selection contributed"
        );
        assert!(integrity.partial_reason.unwrap().contains("orders/7"));
    }

    // `roll_up`, driven directly: these are properties of the roll-up itself.

    /// `all()` over an empty slice is vacuously true, so an empty ledger must
    /// be `Partial`, never `Pass`.
    #[test]
    fn roll_up_on_an_empty_ledger_is_partial_never_pass() {
        let integrity = roll_up(&[], None);
        assert_ne!(
            integrity.result,
            IntegrityResult::Pass,
            "all() over an empty slice is vacuously true; a verification that examined \
             nothing must never report a pass"
        );
        assert_eq!(integrity.result, IntegrityResult::Partial);
        assert_eq!(integrity.level, IntegrityLevel::NotAttempted);
        assert!(integrity.partial_reason.is_some());
        assert_eq!(integrity.records_sampled, 0);
        assert_eq!(integrity.pass_rate_measured, None);
    }

    /// The positive control for the test above: the SAME roll-up does reach
    /// `Pass` when every selection produced positive evidence on both lanes.
    /// Without this, `roll_up_on_an_empty_ledger_is_partial_never_pass` would
    /// still pass against a `roll_up` that could never return `Pass` at all.
    #[test]
    fn roll_up_reaches_pass_only_with_positive_evidence_on_both_lanes() {
        let both = roll_up(
            &[verdict(
                "orders/0",
                Evidence::Verified { checked: 1 },
                Evidence::Verified { checked: 10 },
            )],
            None,
        );
        assert_eq!(both.result, IntegrityResult::Pass);

        // Segment lane silent -> not a pass.
        let seg_silent = roll_up(
            &[verdict(
                "orders/0",
                Evidence::Unverified {
                    why: "no sha256".into(),
                },
                Evidence::Verified { checked: 10 },
            )],
            None,
        );
        assert_eq!(seg_silent.result, IntegrityResult::Partial);

        // Record lane silent -> not a pass.
        let rec_silent = roll_up(
            &[verdict(
                "orders/0",
                Evidence::Verified { checked: 1 },
                Evidence::Unverified {
                    why: "nothing came back".into(),
                },
            )],
            None,
        );
        assert_eq!(rec_silent.result, IntegrityResult::Partial);

        // Either lane failing -> Fail, which outranks both of the above.
        let failed = roll_up(
            &[verdict(
                "orders/0",
                Evidence::Verified { checked: 1 },
                Evidence::Failed {
                    why: "mismatch".into(),
                },
            )],
            None,
        );
        assert_eq!(failed.result, IntegrityResult::Fail);
    }

    /// A failed selection produces `Fail` on a downgraded selection too, not
    /// `Partial`: "we checked, and it is wrong" is not "we could not fully
    /// check this". KILLS: `any(failed)` filtered to non-downgraded
    /// selections. The segment lane is reachable (`segment_evidence` runs on
    /// every lane; end to end in `verify_phase.rs`'s
    /// `a_consume_only_selection_with_a_corrupt_segment_fails_the_drill_never_merely_partial`);
    /// the record lane is not today, and is asserted as a property of
    /// `roll_up`.
    #[test]
    fn a_failed_selection_reports_fail_on_every_lane_including_a_downgraded_one() {
        // Reachable: the segment lane failed on a downgraded selection.
        let mut segment_failed = verdict(
            "orders/0",
            Evidence::Failed {
                why: "segment sha256 mismatch against the manifest".into(),
            },
            Evidence::Verified { checked: 10 },
        );
        segment_failed.downgrade = Some("kbak level below the gate".into());
        segment_failed.reconciled = None;
        let integrity = roll_up(&[segment_failed], None);
        assert_eq!(
            integrity.result,
            IntegrityResult::Fail,
            "a downgraded selection that was EXAMINED AND FOUND WRONG is a Fail, not a \
             Partial; `Partial` would tell an auditor the drill could not check this, which \
             is the opposite of what happened — got {integrity:?}"
        );
        assert_eq!(integrity.level, IntegrityLevel::ConsumeOnly);

        // Defensive: the record lane failed on a downgraded selection.
        let mut record_failed = verdict(
            "orders/0",
            Evidence::Verified { checked: 1 },
            Evidence::Failed {
                why: "records did not reconcile".into(),
            },
        );
        record_failed.downgrade = Some("kbak level below the gate".into());
        record_failed.reconciled = None;
        assert_eq!(
            roll_up(&[record_failed], None).result,
            IntegrityResult::Fail
        );

        // And a failed DOWNGRADED selection still outranks a healthy sibling,
        // so the whole-drill verdict cannot be rescued by averaging.
        let mut mixed = verdict(
            "orders/0",
            Evidence::Failed {
                why: "segment sha256 mismatch against the manifest".into(),
            },
            Evidence::Verified { checked: 10 },
        );
        mixed.downgrade = Some("kbak level below the gate".into());
        mixed.reconciled = None;
        let integrity = roll_up(
            &[
                mixed,
                verdict(
                    "payments/0",
                    Evidence::Verified { checked: 1 },
                    Evidence::Verified { checked: 10 },
                ),
            ],
            None,
        );
        assert_eq!(integrity.result, IntegrityResult::Fail);
    }

    /// The signed sample counters key on `reconciled.is_some()` and nothing
    /// else. A downgraded selection that still carries reconciliation counts
    /// keeps them: filtering on `downgrade` would under-report
    /// `records_sampled` while `partial_reason` named the selection.
    #[test]
    fn the_signed_counters_key_on_reconciled_alone_never_on_the_downgrade_flag() {
        // Not a shape `verdict_for_selection` builds today: `roll_up`'s
        // contract holds for the inputs it declares.
        let mut downgraded_but_reconciled = verdict(
            "orders/0",
            Evidence::Verified { checked: 1 },
            Evidence::Verified { checked: 10 },
        );
        downgraded_but_reconciled.downgrade = Some("kbak level below the gate".into());
        downgraded_but_reconciled.reconciled = Some((7, 5));

        let integrity = roll_up(&[downgraded_but_reconciled], None);
        assert_eq!(
            integrity.records_sampled, 7,
            "a selection carrying reconciliation counts contributes them to the signed \
             counters; the downgrade flag governs the LEVEL, never what was counted"
        );
        assert_eq!(integrity.records_sampled_matching, 5);
        assert_eq!(integrity.mismatches, 2);
    }

    /// `roll_up` must not branch on `level`: a consume-only selection reaches
    /// `Pass` through the same `Evidence::Verified` every other lane must
    /// produce, and an UNVERIFIED consume-only selection is refused just as
    /// hard as an unverified byte-fingerprint one.
    #[test]
    fn roll_up_applies_the_same_obligation_to_the_consume_only_lane() {
        let mut downgraded = verdict(
            "orders/0",
            Evidence::Verified { checked: 1 },
            Evidence::Unverified {
                why: "the target partition gave back zero records".into(),
            },
        );
        downgraded.downgrade = Some("kbak level below the gate".into());
        downgraded.reconciled = None;
        downgraded.records_restored = 0;

        let integrity = roll_up(&[downgraded], None);
        assert_eq!(integrity.level, IntegrityLevel::ConsumeOnly);
        assert_ne!(
            integrity.result,
            IntegrityResult::Pass,
            "consume-only is a weaker CLAIM, never a weaker CHECK"
        );
        assert_eq!(integrity.result, IntegrityResult::Partial);
        assert_eq!(integrity.pass_rate_measured, None);
    }

    /// A downgraded selection reports the weakest LEVEL for the drill, but
    /// must not erase the byte-level claim another selection genuinely
    /// established. `roll_up` names those selections
    /// in `partial_reason` and keeps their real counters.
    #[test]
    fn roll_up_names_the_selections_whose_byte_level_claim_survives_a_downgrade() {
        let mut downgraded = verdict(
            "orders/0",
            Evidence::Verified { checked: 1 },
            Evidence::Verified { checked: 10 },
        );
        downgraded.downgrade = Some("kbak level below the gate".into());
        downgraded.reconciled = None;

        let integrity = roll_up(
            &[
                downgraded,
                verdict(
                    "payments/0",
                    Evidence::Verified { checked: 1 },
                    Evidence::Verified { checked: 10 },
                ),
            ],
            None,
        );
        assert_eq!(integrity.level, IntegrityLevel::ConsumeOnly);
        assert_eq!(integrity.result, IntegrityResult::Pass);
        assert_eq!(
            integrity.records_sampled, 10,
            "payments' genuine reconciliation must survive orders' downgrade"
        );
        assert!(integrity
            .partial_reason
            .as_deref()
            .unwrap()
            .contains("payments/0"));
        assert_eq!(
            integrity.pass_rate_measured, None,
            "the level is not byte-fingerprint, so a measured rate would violate the \
             scorecard's own invariant"
        );
    }

    /// `pass_rate_measured = Some(1.0)` beside a verdict that is not a pass,
    /// a ratio over only the selections that reconciled, is refused: a rate
    /// is a ratio over the whole sample or null.
    #[test]
    fn roll_up_withholds_a_pass_rate_when_any_selection_never_reconciled() {
        let mut short = verdict(
            "orders/0",
            Evidence::Verified { checked: 1 },
            Evidence::Unverified {
                why: "1 fingerprint where the manifest claims 25".into(),
            },
        );
        short.reconciled = Some((1, 1));

        let integrity = roll_up(
            &[
                short,
                verdict(
                    "payments/0",
                    Evidence::Verified { checked: 1 },
                    Evidence::Verified { checked: 10 },
                ),
            ],
            None,
        );
        assert_eq!(integrity.result, IntegrityResult::Partial);
        assert_eq!(
            integrity.pass_rate_measured, None,
            "Some(1.0) beside a partial verdict is the fourth door's exact false assurance"
        );
    }

    /// A pre-0.21 segment (empty sha256) is skipped as coverage the drill did
    /// not obtain: `Unverified`, never `Pass`.
    #[test]
    fn a_segment_with_no_sha256_is_unverified_never_verified() {
        let (store, _) = store_with("logweir/k", b"anything");
        let mut facts = facts_one_segment();
        facts.topics[0].partitions[0].segments[0].sha256 = String::new();
        let segs = sampled_segments_for(&facts, &sel_for("orders", 0, (0, 100)));
        assert_eq!(segs.len(), 1);
        let ev = segment_evidence(&store, &segs).unwrap();
        assert!(ev.is_unverified(), "{ev:?}");
        assert!(ev.why().unwrap().contains("before 0.21"));
    }

    /// The positive and the negative in one place, so neither arm can be
    /// deleted unnoticed: a segment whose bytes hash to the manifest's own
    /// sha256 is `Verified`; the same segment against a manifest claiming a
    /// different sha256 is `Failed` — never `Verified`, and never merely
    /// `Unverified` (it was examined, and it was wrong).
    #[test]
    fn segment_evidence_verifies_a_matching_segment_and_fails_a_mismatching_one() {
        let (store, sha) = store_with("logweir/k", b"real segment bytes");

        let mut good = facts_one_segment();
        good.topics[0].partitions[0].segments[0].sha256 = sha;
        let ev = segment_evidence(
            &store,
            &sampled_segments_for(&good, &sel_for("orders", 0, (0, 100))),
        )
        .unwrap();
        assert_eq!(ev, Evidence::Verified { checked: 1 });

        let bad = facts_one_segment(); // sha256 is "sha256:whatever"
        let ev = segment_evidence(
            &store,
            &sampled_segments_for(&bad, &sel_for("orders", 0, (0, 100))),
        )
        .unwrap();
        assert!(ev.is_failed(), "{ev:?}");
        assert!(ev.why().unwrap().contains("mismatch"));
    }

    #[test]
    fn probe_archive_modes_refuses_an_empty_selection_list() {
        struct NeverCalled;
        impl DataEngine for NeverCalled {
            fn id(&self) -> logweir_core::engine::EngineId {
                unimplemented!()
            }
            fn list_backup_sets(
                &self,
                _: &logweir_core::engine::StorageUrl,
            ) -> Result<Vec<BackupSetRef>, EngineError> {
                unimplemented!()
            }
            fn describe(&self, _: &BackupSetRef) -> Result<BackupSetFacts, EngineError> {
                unimplemented!()
            }
            fn preflight(
                &self,
                _: &RestorePlan,
            ) -> Result<logweir_core::engine::PreflightReport, EngineError> {
                unimplemented!()
            }
            fn restore(
                &self,
                _: &RestorePlan,
                _: &mut dyn logweir_core::engine::PhaseObserver,
            ) -> Result<logweir_core::engine::RestoreFacts, EngineError> {
                unimplemented!()
            }
            fn fingerprints(
                &self,
                _: &SampleSelection,
            ) -> Result<Vec<RecordFingerprint>, EngineError> {
                panic!(
                    "probe_archive_modes must refuse an empty selection before calling the engine"
                )
            }
        }
        let err = probe_archive_modes(&NeverCalled, &[]).unwrap_err();
        assert!(matches!(err, EngineError::Operational(_)));
    }

    /// The mode is per selection: one `Unsupported` selection downgrades only
    /// itself, the loop continues, and the answer count equals the selection
    /// count.
    #[test]
    fn probe_archive_modes_downgrades_only_the_unsupported_selection() {
        struct MixedLevels;
        impl DataEngine for MixedLevels {
            fn id(&self) -> logweir_core::engine::EngineId {
                unimplemented!()
            }
            fn list_backup_sets(
                &self,
                _: &logweir_core::engine::StorageUrl,
            ) -> Result<Vec<BackupSetRef>, EngineError> {
                unimplemented!()
            }
            fn describe(&self, _: &BackupSetRef) -> Result<BackupSetFacts, EngineError> {
                unimplemented!()
            }
            fn preflight(
                &self,
                _: &RestorePlan,
            ) -> Result<logweir_core::engine::PreflightReport, EngineError> {
                unimplemented!()
            }
            fn restore(
                &self,
                _: &RestorePlan,
                _: &mut dyn logweir_core::engine::PhaseObserver,
            ) -> Result<logweir_core::engine::RestoreFacts, EngineError> {
                unimplemented!()
            }
            /// Partition 0 is a legacy (level-1) segment; partition 1 is
            /// fine. A backup set can mix levels — a set written across an
            /// engine upgrade, or a re-uploaded partition.
            fn fingerprints(
                &self,
                s: &SampleSelection,
            ) -> Result<Vec<RecordFingerprint>, EngineError> {
                if s.partition == 0 {
                    Err(EngineError::Unsupported("kbak level 1".into()))
                } else {
                    Ok(vec![RecordFingerprint {
                        topic: s.topic.clone(),
                        partition: s.partition,
                        offset: 0,
                        sha256: "sha256:x".into(),
                    }])
                }
            }
        }
        let sel = vec![
            sel_for("orders", 0, (0, 100)),
            sel_for("orders", 1, (0, 100)),
        ];
        let modes = probe_archive_modes(&MixedLevels, &sel).unwrap();
        assert_eq!(
            modes.len(),
            sel.len(),
            "exactly one answer per selection, in sel's own order"
        );
        assert!(matches!(modes[0], SelectionArchive::Unsupported(_)));
        match &modes[1] {
            SelectionArchive::Fingerprints(fp) => assert_eq!(fp.len(), 1),
            other => panic!("partition 1 is supported and must keep its fingerprints: {other:?}"),
        }
    }

    #[test]
    fn verdict_for_selection_refuses_a_topic_with_no_mapping_entry() {
        struct Unreachable;
        impl ClusterReader for Unreachable {
            fn cluster_id(&self) -> Result<String, logweir_kafka::reader::KafkaError> {
                unimplemented!()
            }
            fn list_topics(
                &self,
            ) -> Result<Vec<logweir_kafka::reader::TopicMeta>, logweir_kafka::reader::KafkaError>
            {
                unimplemented!()
            }
            fn end_offsets(
                &self,
                _: &str,
            ) -> Result<Vec<(i32, i64)>, logweir_kafka::reader::KafkaError> {
                unimplemented!()
            }
            fn broker_configs(
                &self,
            ) -> Result<BTreeMap<String, String>, logweir_kafka::reader::KafkaError> {
                // Phase 7 never reads it: the target-topic preflight is phase 0's.
                unimplemented!()
            }
            fn topic_configs(
                &self,
                _: &str,
            ) -> Result<BTreeMap<String, String>, logweir_kafka::reader::KafkaError> {
                unimplemented!()
            }
            fn consume_range(
                &self,
                _t: &str,
                _p: i32,
                _from: i64,
                _max: usize,
            ) -> Result<Vec<ConsumedRecord>, logweir_kafka::reader::KafkaError> {
                panic!(
                    "verdict_for_selection must refuse an unmapped topic before calling the reader"
                )
            }
        }
        let facts = facts_one_segment();
        let (store, _) = store_with("logweir/k", b"anything");
        // The archive mode is never consulted: `mapped_topic` must refuse
        // BEFORE `verdict_for_selection` reads the store or the broker.
        let archive = SelectionArchive::Unsupported("n/a".into());
        let err = verdict_for_selection(
            &Unreachable,
            &store,
            &facts,
            &sel_for("orders", 0, (0, 100)),
            &BTreeMap::new(),
            &archive,
        )
        .unwrap_err();
        assert!(matches!(err, DrillError::Operational(_)));
        assert!(err.to_string().contains("no target-side mapping"));
    }

    /// `claimed` is `min(sel.count, Σ record_count)` over the matched segments
    /// **wholly inside** the sampled window. All three halves are exercised:
    /// the selection's own `count` binding when smaller, the wholly-inside
    /// manifest sum binding when smaller, and a straddler contributing
    /// nothing.
    #[test]
    fn claimed_is_the_manifests_wholly_inside_sum_capped_by_the_selections_own_count() {
        struct Empty;
        impl ClusterReader for Empty {
            fn cluster_id(&self) -> Result<String, logweir_kafka::reader::KafkaError> {
                unimplemented!()
            }
            fn list_topics(
                &self,
            ) -> Result<Vec<logweir_kafka::reader::TopicMeta>, logweir_kafka::reader::KafkaError>
            {
                unimplemented!()
            }
            fn end_offsets(
                &self,
                _: &str,
            ) -> Result<Vec<(i32, i64)>, logweir_kafka::reader::KafkaError> {
                unimplemented!()
            }
            fn broker_configs(
                &self,
            ) -> Result<BTreeMap<String, String>, logweir_kafka::reader::KafkaError> {
                // Phase 7 never reads it: the target-topic preflight is phase 0's.
                unimplemented!()
            }
            fn topic_configs(
                &self,
                _: &str,
            ) -> Result<BTreeMap<String, String>, logweir_kafka::reader::KafkaError> {
                unimplemented!()
            }
            fn consume_range(
                &self,
                _t: &str,
                _p: i32,
                _from: i64,
                _max: usize,
            ) -> Result<Vec<ConsumedRecord>, logweir_kafka::reader::KafkaError> {
                Ok(vec![])
            }
        }
        let (store, sha) = store_with("logweir/k", b"real segment bytes");
        let mut facts = facts_one_segment();
        facts.topics[0].partitions[0].segments[0].sha256 = sha;
        let mapping: BTreeMap<String, String> =
            [("orders".to_string(), "drill-orders".to_string())]
                .into_iter()
                .collect();
        let archive = SelectionArchive::Unsupported("n/a".into());

        // `sel_for` builds `count: 10`; the one segment spans [0, 100] and
        // holds 10 records. Over the window (0, 100) it is WHOLLY inside.
        // Cap binds: count 4 < manifest 10.
        let mut small = sel_for("orders", 0, (0, 100));
        small.count = 4;
        let v = verdict_for_selection(&Empty, &store, &facts, &small, &mapping, &archive).unwrap();
        assert_eq!(v.claimed, 4, "the selection's own count must cap the claim");

        // Manifest sum binds: count 40 > manifest 10.
        let mut large = sel_for("orders", 0, (0, 100));
        large.count = 40;
        let v = verdict_for_selection(&Empty, &store, &facts, &large, &mapping, &archive).unwrap();
        assert_eq!(
            v.claimed, 10,
            "the manifest's own wholly-inside record_count must cap the claim when it is smaller"
        );

        // The same segment against a window ending one millisecond before it
        // does: it now straddles, supports nothing, and the claim falls to 0.
        let mut straddled = sel_for("orders", 0, (0, 99));
        straddled.count = 40;
        let v =
            verdict_for_selection(&Empty, &store, &facts, &straddled, &mapping, &archive).unwrap();
        assert_eq!(
            v.claimed, 0,
            "a segment the window cuts across holds records the window excludes; its \
             record_count is an upper bound, never a claim"
        );

        // No segment matches the window at all: there is no manifest figure
        // to use, so the plan's own ask stands — and the segment lane is
        // already `Unverified`, so the selection cannot pass regardless.
        let outside = sel_for("orders", 0, (200, 300));
        let v =
            verdict_for_selection(&Empty, &store, &facts, &outside, &mapping, &archive).unwrap();
        assert_eq!(v.claimed, 10);
        assert!(v.segments.is_unverified());
    }

    // `claimed` is what the manifest can support, not what it overlaps: only
    // segments wholly inside the sample window, both ends inclusive, support
    // a claim (the rule `expected_restored_count` writes down).

    /// `2025-10-09T08:53:20Z`, the same instant `e2e/fixtures/drill-pitr.yaml`
    /// uses as its `point_in_time`.
    const T_MS: i64 = 1_760_000_000_000;

    /// A `ClusterReader` that answers `consume_range` from a fixed vector,
    /// honouring `max` exactly as a broker read does, and refuses everything
    /// `verdict_for_selection` does not call.
    struct Restored(Vec<ConsumedRecord>);
    impl ClusterReader for Restored {
        fn cluster_id(&self) -> Result<String, logweir_kafka::reader::KafkaError> {
            unimplemented!("verdict_for_selection never reads the cluster id")
        }
        fn list_topics(
            &self,
        ) -> Result<Vec<logweir_kafka::reader::TopicMeta>, logweir_kafka::reader::KafkaError>
        {
            unimplemented!("verdict_for_selection never lists topics")
        }
        fn end_offsets(
            &self,
            _: &str,
        ) -> Result<Vec<(i32, i64)>, logweir_kafka::reader::KafkaError> {
            unimplemented!("the count bound is `check_restored_count`'s, not this function's")
        }
        fn broker_configs(
            &self,
        ) -> Result<BTreeMap<String, String>, logweir_kafka::reader::KafkaError> {
            unimplemented!()
        }
        fn topic_configs(
            &self,
            _: &str,
        ) -> Result<BTreeMap<String, String>, logweir_kafka::reader::KafkaError> {
            unimplemented!()
        }
        fn consume_range(
            &self,
            _t: &str,
            _p: i32,
            _from: i64,
            max: usize,
        ) -> Result<Vec<ConsumedRecord>, logweir_kafka::reader::KafkaError> {
            Ok(self.0.iter().take(max).cloned().collect())
        }
    }

    /// One archive fingerprint and the consumed record a correct restore
    /// produces from it: the same key, value, timestamp and 8-byte
    /// little-endian `x-original-offset` header, at a DIFFERENT target offset,
    /// so `compare` has to read the header to match them (guard G-HDR).
    fn pair(offset: i64, ts_ms: i64) -> (RecordFingerprint, ConsumedRecord) {
        let key = format!("k{offset}").into_bytes();
        let value = format!("v{offset}").into_bytes();
        let headers = vec![(
            "x-original-offset".to_string(),
            Some(offset.to_le_bytes().to_vec()),
        )];
        (
            RecordFingerprint {
                topic: "orders".into(),
                partition: 0,
                offset,
                sha256: logweir_kafka::fingerprint::record_fingerprint(
                    Some(&key),
                    Some(&value),
                    &headers,
                    ts_ms,
                ),
            },
            ConsumedRecord {
                partition: 0,
                offset: offset - 1_000,
                timestamp_ms: ts_ms,
                key: Some(key),
                value: Some(value),
                headers,
            },
        )
    }

    /// `n` matching pairs at original offsets `1000..1000+n`, one millisecond
    /// apart ending on `newest_ms`.
    fn pairs(n: usize, newest_ms: i64) -> (Vec<RecordFingerprint>, Vec<ConsumedRecord>) {
        (0..n)
            .map(|i| pair(1_000 + i as i64, newest_ms - (n - 1 - i) as i64))
            .unzip()
    }

    /// `BackupSetFacts` for `orders/0` over `segments`, seeding each segment's
    /// bytes into `store` so `segment_evidence` performs a real sha256
    /// comparison rather than one the fixture satisfies by construction.
    ///
    /// `(key, start_timestamp, end_timestamp, record_count)` per segment.
    fn facts_and_store(segments: &[(&str, i64, i64, i64)]) -> (BackupSetFacts, Store) {
        let store = Store::in_memory("logweir");
        let mut out = Vec::new();
        for (i, (key, t0, t1, n)) in segments.iter().enumerate() {
            let bytes = format!("segment bytes {key}").into_bytes();
            store.put_create_only(key, &bytes).unwrap();
            out.push(SegmentFacts {
                key: (*key).into(),
                start_offset: i as i64 * 1_000,
                end_offset: i as i64 * 1_000 + n.max(&0) - 1,
                start_timestamp: *t0,
                end_timestamp: *t1,
                record_count: *n,
                sha256: logweir_core::ids::sha256_prefixed(&bytes),
                uploaded_at: *t1,
            });
        }
        let mut facts = facts_one_segment();
        facts.topics[0].partitions[0].segments = out;
        (facts, store)
    }

    fn orders_mapping() -> BTreeMap<String, String> {
        [("orders".to_string(), "drill-orders".to_string())]
            .into_iter()
            .collect()
    }

    /// A selection over `window` asking for `count` records per partition.
    fn sel_window(count: usize, window: (i64, i64)) -> SampleSelection {
        let mut s = sel_for("orders", 0, window);
        s.count = count;
        s
    }

    /// **The straddler.** One segment holding 3 records across `[T−1, T+1]`;
    /// the sample window is `[T−1, T]`, so it straddles the end and the
    /// archive can only return the 2 records inside. Both reconcile, and the
    /// selection is `Verified` (the `compared > 0` conjunct keeps it an
    /// assertion). KILLS: the overlap sum restored (`claimed` 3, a correct
    /// restore `Unverified`).
    #[test]
    fn a_straddling_segment_supports_no_claim_and_its_in_window_records_verify() {
        let (facts, store) = facts_and_store(&[("logweir/straddler.kbak", T_MS - 1, T_MS + 1, 3)]);
        let (fps, consumed) = pairs(2, T_MS);
        let s = sel_window(25, (T_MS - 1, T_MS));

        let v = verdict_for_selection(
            &Restored(consumed),
            &store,
            &facts,
            &s,
            &orders_mapping(),
            &SelectionArchive::Fingerprints(fps),
        )
        .unwrap();

        assert_eq!(
            v.claimed, 0,
            "a segment straddling the window edge holds records the window EXCLUDES; its \
             record_count is an upper bound, never a claim the archive must meet"
        );
        assert_eq!(
            v.records,
            Evidence::Verified { checked: 2 },
            "the archive returned every record the window contains and all of them \
             reconciled; got {:?}",
            v.records
        );
        assert_eq!(v.reconciled, Some((2, 2)));
        assert!(
            v.segments.is_verified(),
            "the straddling segment's own sha256 is still checked — the overlap predicate \
             stays exactly where it was for `segment_evidence`: {:?}",
            v.segments
        );
        assert!(v.fully_verified());
    }

    /// **The kept property.** One segment WHOLLY inside
    /// the window holding 5 records; the archive returns 3, all matching.
    /// That is a genuinely truncated read of a segment the window contains,
    /// which is the case "short counts as unverified" was built for, and it
    /// must stay `Unverified` — with the text naming 5.
    #[test]
    fn a_wholly_inside_segment_still_binds_the_claim_and_a_short_read_is_unverified() {
        let (facts, store) = facts_and_store(&[("logweir/inside.kbak", T_MS - 10, T_MS - 1, 5)]);
        let (fps, consumed) = pairs(3, T_MS - 2);
        let s = sel_window(25, (T_MS - 20, T_MS));

        let v = verdict_for_selection(
            &Restored(consumed),
            &store,
            &facts,
            &s,
            &orders_mapping(),
            &SelectionArchive::Fingerprints(fps),
        )
        .unwrap();

        assert_eq!(
            v.claimed, 5,
            "the manifest PROVES 5 records are in the window"
        );
        let why = v
            .records
            .why()
            .expect("a short sample must say so")
            .to_string();
        assert!(
            v.records.is_unverified(),
            "3 of a proven 5 is coverage the drill did not obtain: {:?}",
            v.records
        );
        assert!(
            why.contains(
                "the archive returned 3 fingerprints where the manifest supports at \
                          least 5 for this selection"
            ),
            "the message must name the manifest's supportable claim: {why}"
        );
        assert!(
            !why.contains("straddling"),
            "no segment straddles this window, so the message must not invent one: {why}"
        );
    }

    /// **The mix.** One wholly-inside segment holding 5 and one straddler
    /// holding 3: the claim is 5, not 8. `compared == 5` verifies (KILLS: the
    /// overlap sum); `compared == 4` does not, and the message discloses the
    /// straddler.
    #[test]
    fn a_mixed_window_claims_the_wholly_inside_segment_and_discloses_the_straddler() {
        let segments = [
            ("logweir/inside.kbak", T_MS - 10, T_MS - 1, 5),
            ("logweir/straddler.kbak", T_MS - 1, T_MS + 1, 3),
        ];
        let s = sel_window(25, (T_MS - 20, T_MS));

        let (facts, store) = facts_and_store(&segments);
        let (fps, consumed) = pairs(5, T_MS - 2);
        let v = verdict_for_selection(
            &Restored(consumed),
            &store,
            &facts,
            &s,
            &orders_mapping(),
            &SelectionArchive::Fingerprints(fps),
        )
        .unwrap();
        assert_eq!(
            v.claimed, 5,
            "5 wholly inside + 3 straddling supports 5, never 8"
        );
        assert_eq!(v.records, Evidence::Verified { checked: 5 });

        let (facts, store) = facts_and_store(&segments);
        let (fps, consumed) = pairs(4, T_MS - 2);
        let v = verdict_for_selection(
            &Restored(consumed),
            &store,
            &facts,
            &s,
            &orders_mapping(),
            &SelectionArchive::Fingerprints(fps),
        )
        .unwrap();
        assert_eq!(v.claimed, 5);
        let why = v.records.why().expect("a short sample must say so");
        assert!(v.records.is_unverified(), "4 of a proven 5 is short");
        assert!(
            why.contains(
                "the archive returned 4 fingerprints where the manifest supports at \
                          least 5 for this selection (1 straddling segment holds 3 records the \
                          window may exclude)"
            ),
            "the message must disclose the straddler it deliberately did not count: {why}"
        );
    }

    /// **The consume-only lane carries the same rule.** The mirrors of the
    /// straddler and the kept property: a straddler supports nothing and the
    /// read-back verifies; a wholly-inside segment still binds, and a short
    /// read-back is `Unverified` with the claim named. KILLS: the rule on one
    /// lane only.
    #[test]
    fn the_consume_only_lane_claims_the_same_wholly_inside_sum() {
        // The straddler's mirror.
        let (facts, store) = facts_and_store(&[("logweir/straddler.kbak", T_MS - 1, T_MS + 1, 3)]);
        let (_, consumed) = pairs(2, T_MS);
        let v = verdict_for_selection(
            &Restored(consumed),
            &store,
            &facts,
            &sel_window(25, (T_MS - 1, T_MS)),
            &orders_mapping(),
            &SelectionArchive::Unsupported("segment written before 0.21".into()),
        )
        .unwrap();
        assert_eq!(v.claimed, 0);
        assert_eq!(
            v.records,
            Evidence::Verified { checked: 2 },
            "the target gave back every record the window holds: {:?}",
            v.records
        );

        // The kept property's mirror: the wholly-inside segment.
        let (facts, store) = facts_and_store(&[("logweir/inside.kbak", T_MS - 10, T_MS - 1, 5)]);
        let (_, consumed) = pairs(3, T_MS - 2);
        let v = verdict_for_selection(
            &Restored(consumed),
            &store,
            &facts,
            &sel_window(25, (T_MS - 20, T_MS)),
            &orders_mapping(),
            &SelectionArchive::Unsupported("segment written before 0.21".into()),
        )
        .unwrap();
        assert_eq!(v.claimed, 5);
        let why = v.records.why().expect("a short read-back must say so");
        assert!(v.records.is_unverified(), "{:?}", v.records);
        assert!(
            why.contains(
                "the target partition gave back 3 records where the manifest supports \
                          at least 5 for this selection"
            ),
            "the consume-only message must name the same claim: {why}"
        );
    }

    /// **Both ends are inclusive.** A segment flush with both window edges is
    /// wholly inside, as the engine's own filter `s <= timestamp <= e` reads
    /// it. KILLS: `>=` → `>` on either end (the segment stops counting,
    /// `claimed` falls to 0, and a 3-fingerprint read of a proven 5 reports
    /// `Verified`).
    #[test]
    fn a_segment_flush_with_both_window_edges_is_wholly_inside() {
        let (facts, store) = facts_and_store(&[("logweir/flush.kbak", T_MS - 10, T_MS, 5)]);
        let (fps, consumed) = pairs(3, T_MS - 2);

        let v = verdict_for_selection(
            &Restored(consumed),
            &store,
            &facts,
            &sel_window(25, (T_MS - 10, T_MS)),
            &orders_mapping(),
            &SelectionArchive::Fingerprints(fps),
        )
        .unwrap();

        assert_eq!(
            v.claimed, 5,
            "start_timestamp == w0 and end_timestamp == w1 is INSIDE [w0, w1], both ends \
             inclusive — the same reading `expected_restored_count` gives the restore window"
        );
        assert!(
            v.records.is_unverified(),
            "3 of a proven 5 is short; if the flush segment stopped counting this would be a \
             vacuous pass: {:?}",
            v.records
        );
    }

    /// `supportable_claim` clamps a negative `record_count` to 0 (interface
    /// I5) on both sums, the wholly-inside claim and the straddler disclosure.
    /// A wrapped `-1` is `u64::MAX`, which would make `compared >= claimed`
    /// unreachable and fail a correct restore. KILLS: either `.max(0)`
    /// deleted.
    #[test]
    fn a_negative_or_zero_record_count_contributes_nothing_and_never_wraps() {
        // (a) WHOLLY INSIDE, record_count -1 → supports 0, not 2^64-1.
        let (facts, store) = facts_and_store(&[("logweir/neg.kbak", T_MS - 10, T_MS - 1, -1)]);
        let (fps, consumed) = pairs(2, T_MS - 2);
        let v = verdict_for_selection(
            &Restored(consumed),
            &store,
            &facts,
            &sel_window(25, (T_MS - 20, T_MS)),
            &orders_mapping(),
            &SelectionArchive::Fingerprints(fps),
        )
        .unwrap();
        assert_eq!(
            v.claimed, 0,
            "a negative record_count clamps to 0 and never wraps a u64 (I5); a wrapped \
             u64::MAX would make `compared >= claimed` unreachable for every archive"
        );
        assert_eq!(v.records, Evidence::Verified { checked: 2 });

        // (b) a STRADDLER with a negative count discloses 0 records, not 2^64-1.
        let straddler_rows: [(&str, i64, i64, i64); 2] = [
            ("logweir/inside.kbak", T_MS - 10, T_MS - 1, 5),
            ("logweir/neg-straddler.kbak", T_MS - 1, T_MS + 1, -1),
        ];
        let (facts, store) = facts_and_store(&straddler_rows);
        let (fps, consumed) = pairs(3, T_MS - 2);
        let v = verdict_for_selection(
            &Restored(consumed),
            &store,
            &facts,
            &sel_window(25, (T_MS - 20, T_MS)),
            &orders_mapping(),
            &SelectionArchive::Fingerprints(fps),
        )
        .unwrap();
        assert_eq!(
            v.claimed, 5,
            "the negative straddler adds nothing to the claim"
        );
        let why = v.records.why().expect("a short sample must say so");
        assert!(
            why.contains("(1 straddling segment holds 0 records the window may exclude)"),
            "a negative straddler is disclosed as 0 records, never as a wrapped u64: {why}"
        );

        // (c) a straddler whose count is exactly 0 is still DISCLOSED.
        let zero_rows: [(&str, i64, i64, i64); 2] = [
            ("logweir/inside.kbak", T_MS - 10, T_MS - 1, 5),
            ("logweir/zero-straddler.kbak", T_MS - 1, T_MS + 1, 0),
        ];
        let (facts, store) = facts_and_store(&zero_rows);
        let (fps, consumed) = pairs(3, T_MS - 2);
        let v = verdict_for_selection(
            &Restored(consumed),
            &store,
            &facts,
            &sel_window(25, (T_MS - 20, T_MS)),
            &orders_mapping(),
            &SelectionArchive::Fingerprints(fps),
        )
        .unwrap();
        assert_eq!(
            v.claimed, 5,
            "a zero-count straddler adds nothing to the claim"
        );
        assert!(v
            .records
            .why()
            .expect("short")
            .contains("1 straddling segment holds 0 records"));
    }

    #[test]
    fn classify_parity_all_refuses_an_empty_mapping() {
        struct Unreachable;
        impl ClusterReader for Unreachable {
            fn cluster_id(&self) -> Result<String, logweir_kafka::reader::KafkaError> {
                unimplemented!()
            }
            fn list_topics(
                &self,
            ) -> Result<Vec<logweir_kafka::reader::TopicMeta>, logweir_kafka::reader::KafkaError>
            {
                unimplemented!()
            }
            fn end_offsets(
                &self,
                _: &str,
            ) -> Result<Vec<(i32, i64)>, logweir_kafka::reader::KafkaError> {
                panic!("classify_parity_all must refuse an empty mapping before calling the reader")
            }
            fn broker_configs(
                &self,
            ) -> Result<BTreeMap<String, String>, logweir_kafka::reader::KafkaError> {
                // Phase 7 never reads it: the target-topic preflight is phase 0's.
                unimplemented!()
            }
            fn topic_configs(
                &self,
                _: &str,
            ) -> Result<BTreeMap<String, String>, logweir_kafka::reader::KafkaError> {
                panic!("classify_parity_all must refuse an empty mapping before calling the reader")
            }
            fn consume_range(
                &self,
                _t: &str,
                _p: i32,
                _from: i64,
                _max: usize,
            ) -> Result<Vec<ConsumedRecord>, logweir_kafka::reader::KafkaError> {
                unimplemented!()
            }
        }
        let facts = facts_one_segment();
        let plan = test_plan();
        let err = classify_parity_all(
            &facts,
            &Unreachable,
            &BTreeMap::new(),
            &plan,
            &SourceConfigCoverage::unknown(),
            TargetMode::Scratch,
        )
        .unwrap_err();
        assert!(matches!(err, DrillError::Operational(_)));
        assert!(err.to_string().contains("zero mapped topics"));
    }

    /// A mapped topic with no matching entry in `facts.topics` is refused,
    /// never skipped: skipping would let `TopicParity` report agreement over
    /// fewer topics than asked. The `Unreachable` reader panics if reached:
    /// the refusal fires on the facts lookup alone.
    #[test]
    fn classify_parity_all_refuses_a_mapped_topic_with_no_archive_facts() {
        struct Unreachable;
        impl ClusterReader for Unreachable {
            fn cluster_id(&self) -> Result<String, logweir_kafka::reader::KafkaError> {
                unimplemented!()
            }
            fn list_topics(
                &self,
            ) -> Result<Vec<logweir_kafka::reader::TopicMeta>, logweir_kafka::reader::KafkaError>
            {
                unimplemented!()
            }
            fn end_offsets(
                &self,
                _: &str,
            ) -> Result<Vec<(i32, i64)>, logweir_kafka::reader::KafkaError> {
                panic!(
                    "classify_parity_all must refuse a mapped topic with no archive facts \
                     before calling the reader"
                )
            }
            fn broker_configs(
                &self,
            ) -> Result<BTreeMap<String, String>, logweir_kafka::reader::KafkaError> {
                // Phase 7 never reads it: the target-topic preflight is phase 0's.
                unimplemented!()
            }
            fn topic_configs(
                &self,
                _: &str,
            ) -> Result<BTreeMap<String, String>, logweir_kafka::reader::KafkaError> {
                panic!(
                    "classify_parity_all must refuse a mapped topic with no archive facts \
                     before calling the reader"
                )
            }
            fn consume_range(
                &self,
                _t: &str,
                _p: i32,
                _from: i64,
                _max: usize,
            ) -> Result<Vec<ConsumedRecord>, logweir_kafka::reader::KafkaError> {
                unimplemented!()
            }
        }
        // `facts_one_segment()` only ever describes "orders" — "payments" has
        // no corresponding `TopicFacts` entry at all.
        let facts = facts_one_segment();
        let plan = test_plan();
        let mut mapping = BTreeMap::new();
        mapping.insert("payments".to_string(), "drill-payments".to_string());
        let err = classify_parity_all(
            &facts,
            &Unreachable,
            &mapping,
            &plan,
            &SourceConfigCoverage::unknown(),
            TargetMode::Scratch,
        )
        .unwrap_err();
        assert!(matches!(err, DrillError::Operational(_)));
        assert!(err.to_string().contains("no archive facts"));
    }

    /// **The signed parity compares the factor the topic was created with.**
    /// A plan with `target.topic_configuration` creates a topic at its own
    /// bounded factor (here 3, the source's), not at the plan's default (1):
    /// no `replication_factor` divergence. CONTROL: a topic the target reports
    /// at 1 is named `not_reconstructed`. KILLS: the plan's default read as
    /// the target's factor.
    #[test]
    fn the_parity_reads_the_created_replication_factor() {
        struct Target(u32);
        impl ClusterReader for Target {
            fn cluster_id(&self) -> Result<String, logweir_kafka::reader::KafkaError> {
                unimplemented!()
            }
            fn list_topics(
                &self,
            ) -> Result<Vec<logweir_kafka::reader::TopicMeta>, logweir_kafka::reader::KafkaError>
            {
                unimplemented!()
            }
            fn end_offsets(
                &self,
                _: &str,
            ) -> Result<Vec<(i32, i64)>, logweir_kafka::reader::KafkaError> {
                Ok(vec![(0, 10)])
            }
            fn broker_configs(
                &self,
            ) -> Result<BTreeMap<String, String>, logweir_kafka::reader::KafkaError> {
                unimplemented!()
            }
            fn topic_configs(
                &self,
                _: &str,
            ) -> Result<BTreeMap<String, String>, logweir_kafka::reader::KafkaError> {
                Ok(BTreeMap::new())
            }
            fn replication_factors(
                &self,
                topics: &[String],
            ) -> Result<BTreeMap<String, u32>, logweir_kafka::reader::KafkaError> {
                Ok(topics.iter().map(|t| (t.clone(), self.0)).collect())
            }
            fn consume_range(
                &self,
                _t: &str,
                _p: i32,
                _from: i64,
                _max: usize,
            ) -> Result<Vec<ConsumedRecord>, logweir_kafka::reader::KafkaError> {
                unimplemented!()
            }
        }
        let facts = facts_one_segment();
        let plan = test_plan();
        let mapping = BTreeMap::from([("orders".to_string(), "drill-orders".to_string())]);
        let parity = |rf: u32| {
            classify_parity_all(
                &facts,
                &Target(rf),
                &mapping,
                &plan,
                &SourceConfigCoverage::unknown(),
                TargetMode::NewTopic,
            )
            .expect("parity")
        };
        let rf_entry = "drill-orders: replication_factor".to_string();
        let created_at_3 = parity(3);
        assert!(
            !created_at_3
                .not_reconstructed
                .clone()
                .unwrap_or_default()
                .contains(&rf_entry),
            "{created_at_3:?}"
        );
        assert!(!created_at_3.unexpected_divergence.contains(&rf_entry));
        let created_at_1 = parity(1);
        assert!(
            created_at_1
                .not_reconstructed
                .clone()
                .unwrap_or_default()
                .contains(&rf_entry),
            "{created_at_1:?}"
        );
    }

    #[test]
    fn newest_ts_refuses_an_empty_mapping() {
        struct Unreachable;
        impl ClusterReader for Unreachable {
            fn cluster_id(&self) -> Result<String, logweir_kafka::reader::KafkaError> {
                unimplemented!()
            }
            fn list_topics(
                &self,
            ) -> Result<Vec<logweir_kafka::reader::TopicMeta>, logweir_kafka::reader::KafkaError>
            {
                unimplemented!()
            }
            fn end_offsets(
                &self,
                _: &str,
            ) -> Result<Vec<(i32, i64)>, logweir_kafka::reader::KafkaError> {
                panic!("newest_ts must refuse an empty mapping before calling the reader")
            }
            fn broker_configs(
                &self,
            ) -> Result<BTreeMap<String, String>, logweir_kafka::reader::KafkaError> {
                // Phase 7 never reads it: the target-topic preflight is phase 0's.
                unimplemented!()
            }
            fn topic_configs(
                &self,
                _: &str,
            ) -> Result<BTreeMap<String, String>, logweir_kafka::reader::KafkaError> {
                unimplemented!()
            }
            fn consume_range(
                &self,
                _t: &str,
                _p: i32,
                _from: i64,
                _max: usize,
            ) -> Result<Vec<ConsumedRecord>, logweir_kafka::reader::KafkaError> {
                unimplemented!()
            }
        }
        let err = newest_ts(&Unreachable, &BTreeMap::new()).unwrap_err();
        assert!(matches!(err, DrillError::Operational(_)));
        assert!(err.to_string().contains("nothing to measure"));
    }

    #[test]
    fn newest_ts_refuses_when_every_mapped_partition_is_empty() {
        struct AllEmpty;
        impl ClusterReader for AllEmpty {
            fn cluster_id(&self) -> Result<String, logweir_kafka::reader::KafkaError> {
                unimplemented!()
            }
            fn list_topics(
                &self,
            ) -> Result<Vec<logweir_kafka::reader::TopicMeta>, logweir_kafka::reader::KafkaError>
            {
                unimplemented!()
            }
            fn end_offsets(
                &self,
                _: &str,
            ) -> Result<Vec<(i32, i64)>, logweir_kafka::reader::KafkaError> {
                Ok(vec![(0, 0)])
            }
            fn broker_configs(
                &self,
            ) -> Result<BTreeMap<String, String>, logweir_kafka::reader::KafkaError> {
                // Phase 7 never reads it: the target-topic preflight is phase 0's.
                unimplemented!()
            }
            fn topic_configs(
                &self,
                _: &str,
            ) -> Result<BTreeMap<String, String>, logweir_kafka::reader::KafkaError> {
                unimplemented!()
            }
            fn consume_range(
                &self,
                _t: &str,
                _p: i32,
                _from: i64,
                _max: usize,
            ) -> Result<Vec<ConsumedRecord>, logweir_kafka::reader::KafkaError> {
                panic!("no partition has a positive end offset; consume_range must not be called")
            }
        }
        let mut m = BTreeMap::new();
        m.insert("orders".to_string(), "drill-orders".to_string());
        let err = newest_ts(&AllEmpty, &m).unwrap_err();
        assert!(matches!(err, DrillError::Operational(_)));
        assert!(err.to_string().contains("no restored records"));
    }

    /// `newest_ts` takes the maximum across partitions with different
    /// last-record timestamps: it feeds `measured.rpo_seconds`, so the oldest
    /// would corrupt a signed RPO. KILLS: `max` swapped for `min`.
    #[test]
    fn newest_ts_returns_the_maximum_not_the_minimum_across_partitions() {
        struct TwoPartitionsDistinctTimestamps;
        impl ClusterReader for TwoPartitionsDistinctTimestamps {
            fn cluster_id(&self) -> Result<String, logweir_kafka::reader::KafkaError> {
                unimplemented!()
            }
            fn list_topics(
                &self,
            ) -> Result<Vec<logweir_kafka::reader::TopicMeta>, logweir_kafka::reader::KafkaError>
            {
                unimplemented!()
            }
            fn end_offsets(
                &self,
                _: &str,
            ) -> Result<Vec<(i32, i64)>, logweir_kafka::reader::KafkaError> {
                Ok(vec![(0, 1), (1, 1)])
            }
            fn broker_configs(
                &self,
            ) -> Result<BTreeMap<String, String>, logweir_kafka::reader::KafkaError> {
                // Phase 7 never reads it: the target-topic preflight is phase 0's.
                unimplemented!()
            }
            fn topic_configs(
                &self,
                _: &str,
            ) -> Result<BTreeMap<String, String>, logweir_kafka::reader::KafkaError> {
                unimplemented!()
            }
            fn consume_range(
                &self,
                _t: &str,
                p: i32,
                _from: i64,
                _max: usize,
            ) -> Result<Vec<ConsumedRecord>, logweir_kafka::reader::KafkaError> {
                // Partition 0's last record is OLDER; partition 1's is NEWER.
                let ts = if p == 0 { 100 } else { 900 };
                Ok(vec![ConsumedRecord {
                    partition: p,
                    offset: 0,
                    timestamp_ms: ts,
                    key: None,
                    value: None,
                    headers: vec![],
                }])
            }
        }
        let mut m = BTreeMap::new();
        m.insert("orders".to_string(), "drill-orders".to_string());
        let newest = newest_ts(&TwoPartitionsDistinctTimestamps, &m).unwrap();
        assert_eq!(
            newest, 900,
            "must be the MAXIMUM restored timestamp across every mapped partition, \
             never the minimum or merely the first one seen"
        );
    }

    fn test_plan() -> RestorePlan {
        RestorePlan {
            set: BackupSetRef {
                backup_id: "b".into(),
                manifest_key: "b/manifest.json".into(),
            },
            storage: logweir_core::engine::StorageUrl::Filesystem {
                path: "/tmp".into(),
            },
            target_bootstrap: vec!["broker:9092".into()],
            target_auth: logweir_core::engine::AuthRender::Plaintext,
            topic_mapping: BTreeMap::new(),
            time_window: (chrono::Utc::now(), chrono::Utc::now()),
            window_floor_source: logweir_core::engine::WindowFloorSource::ArchiveManifest,
            source_partitions: Default::default(),
            default_replication_factor: 1,
            checkpoint_state: "/tmp/checkpoint.json".into(),
            checkpoint_interval_secs: 30,
            offset_report: "/var/lib/logweir/01J9X/offsets.json".into(),
        }
    }
}
