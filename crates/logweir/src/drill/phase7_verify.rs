//! Phase 7 — verify. Four checks, none of which may read "matched" over
//! nothing: (a) segment sha256 over the sampled archive segments, (b) the
//! engine's own `validation run` (evidence only — see below), (c) canary
//! consume-and-reconcile, per-record fingerprints (not watermark counts),
//! and (d) topic-config parity against what phase 6 deliberately altered.
//!
//! ## ONE chokepoint, every lane (Task 19 fix round 3)
//!
//! This phase has now shipped the same critical false pass — a signed `Pass`
//! over a restore that was never checked — through FOUR different doors:
//!
//! 1. `compare()` pooled every selection's records into one offset-keyed map,
//!    so an un-restored topic reconciled against a DIFFERENT topic's records
//!    (`Pass, 50/50, pass_rate 1.0`). Round 1 made `compare()` per-selection.
//! 2. The zero-sample guard fired only on the AGGREGATE count, so one empty
//!    selection hid inside a healthy sibling's total. Round 2 made it
//!    per-selection.
//! 3. `sampled_segments`' empty guard had the same aggregate shape. Closed in
//!    round 2 alongside (2).
//! 4. Both of those guards lived INSIDE the `ByteFingerprint` arm, so the
//!    `ConsumeOnly` lane had no zero-record guard of any kind: one
//!    `Unsupported` fingerprint moved the whole run onto a lane where
//!    `result` was initialised to `Pass` and nothing could move it. A drill
//!    against a legacy archive reported `Pass` over a restore that did
//!    nothing — and the trigger (a pre-0.21 archive, a KBAK level-1 segment)
//!    is a NORMAL production condition this code treats as a routine
//!    downgrade.
//!
//! Each round closed the door the previous round's defect was found in. A
//! fourth lane-specific guard predicts a fifth door, so round 3 does not add
//! one. The invariant is now stated ONCE, structurally:
//!
//! - **`Evidence`** is a value with three variants and no default. A check
//!   that concluded nothing cannot be constructed. Silence — an untouched
//!   `result`, an unsummed zero, an early `return` — is no longer expressible.
//! - **`SelectionVerdict`** exists exactly once per `SampleSelection`. `run`
//!   builds the vector by iterating `sel` itself and `verdict_for_selection`
//!   cannot return "no opinion", so no selection can silently contribute
//!   nothing.
//! - **`roll_up`** is the ONLY place `IntegrityResult` is decided. `Pass`
//!   requires `all(fully_verified)` — POSITIVE evidence from every selection
//!   on both lanes — never the absence of a recorded failure. It answers the
//!   empty ledger explicitly, because `all()` over an empty slice is
//!   vacuously true and that is precisely the bug.
//! - **`ConsumeOnly` carries the same obligation as `ByteFingerprint`.** It
//!   is a weaker CLAIM, not a weaker CHECK: it asserts nothing about record
//!   contents, and it must still show that its partition gave back at least
//!   the records the manifest claims. Nothing in `roll_up` branches on
//!   `level`, so the lane cannot opt out of an obligation that is not
//!   written in the lane.
//! - **Short counts as unverified.** What the archive (or the target)
//!   actually returned is compared against what the manifest claims —
//!   `min(sel.count, Σ SegmentFacts.record_count)` over the segments the
//!   sampled window matches. Both halves come from figures the drill already
//!   signs: `Σ record_count` over the in-window segments is precisely the
//!   per-partition term `phase4_sample` sums into `sample.records_expected`,
//!   and `sel.count` is `sample.records_per_partition`. It is the MINIMUM of
//!   the two, not `records_expected` itself — `phase4_sample`'s per-partition
//!   term is deliberately UNCAPPED by `records_per_partition`, so holding a
//!   25-record sample of a 250-record window to 250 would fail every healthy
//!   drill. Holding the archive to the min therefore adds no new claim; it
//!   holds the drill to the smaller of two numbers it already publishes.
//!   Pinned by `claimed_is_the_manifest_window_sum_capped_by_the_selections
//!   _own_count`. One fingerprint where the manifest claims 25 is 24 records
//!   unexamined, not a smaller successful sample.
//! - **The mode is PER SELECTION.** `probe_archive_modes` no longer returns
//!   one `ConsumeOnly` for the whole backup set on the first `Unsupported`
//!   (which also discarded the fingerprints it had already collected): a
//!   legacy segment in one partition downgrades that partition only, and
//!   `roll_up` names in `partial_reason` every selection whose byte-level
//!   claim was genuinely established, so the downgrade cannot erase it.
//!
//! ## Reconciling PER SELECTION, never pooled (round 1, review finding F1)
//!
//! `compare()` keys its lookup on the bare record offset alone — deliberately:
//! every Kafka partition starts at offset 0, and a restored topic's offsets
//! need not start where the source's did, so matching by (topic, partition,
//! offset) all at once is not simpler, it is just as offset-collision-prone
//! UNLESS every call to `compare()` is scoped to exactly one partition. The
//! first version of this file called `compare()` ONCE over every selection's
//! archive fingerprints and every selection's consumed records, flattened
//! into two pooled lists. Because `ConsumedRecord` carries no topic field
//! and every partition starts at offset 0, records from DIFFERENT topics or
//! DIFFERENT partitions collided in that one shared lookup table — an
//! entirely un-restored topic could reconcile against a DIFFERENT topic's
//! records and sign a byte-fingerprint `Pass` over data that was never
//! restored (demonstrated against the shipped code in the review; see
//! `verify_phase.rs`'s
//! `a_topic_restored_to_zero_records_must_fail_not_pass_even_when_pooled_with_a_healthy_topic`).
//!
//! The fix is now structural rather than disciplinary: `compare()` is called
//! from `verdict_for_selection`, which is handed ONE selection, so it cannot
//! see two selections' data at once even by accident. The cross-PARTITION
//! half of this (two partitions of the SAME topic, which pool just as readily
//! as two different topics since both start at offset 0) is separately pinned
//! by `run_reconciles_two_partitions_of_one_topic_independently_not_pooled`.
//!
//! ## Exit-code routing (contract for the future orchestrator)
//!
//! Mirrors `phase5_preflight`'s documented contract exactly: `run` below
//! returns `Ok(VerifyOutcome)` even when the drill did NOT pass — a failed
//! reconciliation is a DRILL RESULT, not an operational failure, and is
//! carried home inside `VerifyOutcome.integrity.{result,partial_reason}`,
//! never as an `Err`. The orchestrator (Task 21a, not implemented here) reads
//! `integrity.result`. `Pass` becomes `Outcome::Pass` (subject to the other
//! phases' verdicts). `Partial` is NOT a pass, but still lands at exit 2 with
//! a signed scorecard. `Fail` becomes `Outcome::FailIntegrity`, exit 2,
//! signed scorecard.
//!
//! `run` returns `Err(DrillError::Operational(..))` ONLY when the check
//! genuinely could not be performed at all (zero sample selections, a store
//! I/O failure, a broker unreachable) — never because a comparison came back
//! negative, and (round 3) never because the ARCHIVE under-delivered. Round 2
//! treated this one class of finding two ways: an empty archive sample
//! degraded to `Partial` and continued, while a selection matching zero
//! archive segments was a hard `Operational` (exit 1, no artifact at all).
//! Same class, same treatment now: both are drill results. "The archive holds
//! no segment in this window", "it returned fewer records than its own
//! manifest claims" and "the target gave back nothing" are all positively
//! established facts ABOUT THE ARCHIVE OR THE RESTORE, which is exactly what
//! phase 6's `DrillError::RestoreNoOp` precedent settled the same way; exit 1
//! is reserved for Logweir failing to run. An auditor learns strictly more
//! from a signed `partial` naming the exact partition than from exit 1 and no
//! document. If this file is ever found routing a negative comparison result
//! through `Err`, or an `Err` here through anything but exit 1, that is the
//! exact defect this comment exists to prevent.
//!
//! ## What the signed sample counters aggregate
//!
//! The verdict is per selection; `Integrity`'s three counters are one number
//! each, and Task 20 signs them. They aggregate over EXACTLY the selections
//! whose records were reconciled against archive fingerprints — no others
//! contribute, and no non-contributing selection is silent, because every one
//! of them is named in `partial_reason` and forbids a `Pass`.
//! `pass_rate_measured` is stricter still: it is a ratio over the WHOLE
//! sample or it is null, so a `Some(1.0)` can never sit beside a partial
//! verdict the way the fourth door signed it over a 96%-lossy partition.
//!
//! ## `IntegrityResult::Partial` — what reaches it, and one declared gap
//!
//! Any selection whose segment lane or record lane is `Evidence::Unverified`,
//! with no selection failing outright. Reachable in production through every
//! lane: an archive returning zero (or short) fingerprints for a selection;
//! a `(topic, partition, window)` the manifest facts claim exists but no
//! segment matches; a pre-0.21 segment carrying no sha256 to check; a
//! consume-only selection whose target partition gave back nothing or less
//! than the manifest claims. Each names itself in `partial_reason`.
//!
//! The brief's OTHER named `Partial` scenario — a compacted topic that
//! legitimately holds fewer records than the archive (spec §9.3) — is
//! DELIBERATELY still not given a distinct reachable path. Distinguishing
//! "compaction removed this record on purpose" from "the restore or the
//! archive lost it" would require cross-referencing a specific mismatch
//! against `topic_parity`'s `cleanup.policy=compact` flag, which this phase
//! does not attempt. A compacted topic today is honestly reported via the
//! SAME path a real mismatch takes (`Fail`, with the specific offsets
//! logged) — a known limitation, not a defect, and out of scope to resolve
//! algorithmically. `fixtures::verify_outcome_for_compacted_topic` (used by
//! `a_compacted_topic_is_partial_with_a_reason`) remains a SHAPE test only,
//! pinning the struct literal Task 20/21 consume — it does not, and cannot
//! yet, describe a path `run` itself takes.
//!
//! ## The topic-rename mapping ("the known issue")
//!
//! Every `SampleSelection.topic` and `RecordFingerprint.topic` names the
//! ARCHIVE-side (source) topic — e.g. "orders". Phase 6 may have written the
//! restored data under a DIFFERENT target-side name (`mapping`, e.g.
//! "drill-orders"). Two call sites in this file translate source -> target
//! via `mapping` BEFORE talking to the target cluster, and each is pinned by
//! a named test.
//!
//! First, `verdict_for_selection` (via `mapped_topic`) calls
//! `reader.consume_range(mapping[&sel.topic], ...)`, never
//! `reader.consume_range(&sel.topic, ...)` — proven by `verify_phase.rs`'s
//! `verdict_for_selection_reads_the_mapped_target_topic_never_the_archive_name`.
//! Second, `classify_parity_all` calls `reader.topic_configs(mapping[&src])`
//! and `reader.end_offsets(mapping[&src])`, never the source name — proven
//! by
//! `classify_parity_all_reads_the_mapped_target_topic_never_the_archive_name`.
//! A topic named in `sel` with no entry in `mapping` is refused loudly
//! (`DrillError::Operational`), never silently skipped — silently skipping it
//! would under-sample without saying so, exactly the failure mode this phase
//! exists to prevent.
//!
//! A THIRD, separate topic-rename bug lives one layer further out and is NOT
//! fixable from this file: `kafka-backup`'s own `validation run` resolves its
//! `message_count`/`offset_range` checks against the manifest's SOURCE topic
//! names — `ValidationConfig` has no topic-mapping field at all [Task 11's
//! finding, carried forward by `progress.md`'s ruling: "Task 19's dispatch
//! must carry this, and its review must confirm the resolution"]. Invoked
//! against a renamed target, both engine-side checks report a discrepancy
//! for EVERY topic, EVERY time, even on a perfectly healthy drill. That
//! report is real, signed engine evidence, and is USELESS as a pass/fail
//! signal for this drill — worse, it is EXPECTED to look like a failure on
//! every renamed drill, healthy or not.
//!
//! Resolution, in two parts. First, `engine_validation_run`'s `Result` is
//! never propagated with `?` into `run`'s own error type — `run` matches on
//! it directly and logs either arm, so an engine-side failure (an `Err`, or
//! an `Ok` with a non-zero exit code) can neither abort this phase nor be
//! mistaken for an operational failure. Second, only the exit code is ever
//! read even on the `Ok` arm, and it never sets `integrity.result` or gates
//! anything. Logweir's own `compare()` over correctly-mapped consumed
//! records (mapping point 1 above) is the ONLY input to `integrity.result`.
//! Nothing under this file's control can teach the upstream engine about the
//! mapping — that would require a `ValidationConfig` schema change upstream
//! — so the honest fix available here is to keep inviting the engine's own
//! evidence while never trusting, or even depending on, its verdict. Pinned
//! by `a_failing_engine_validation_run_never_fails_or_aborts_the_drill`
//! (`verify_phase.rs`).
use crate::drill::DrillError;
use crate::exit::ExitCode;
use logweir_core::engine::{
    BackupSetFacts, DataEngine, EngineError, EngineRun, RecordFingerprint, RestorePlan,
    SampleSelection, SegmentFacts,
};
use logweir_core::outcome::{IntegrityLevel, IntegrityResult};
use logweir_core::scorecard::{Integrity, Scorecard, TopicParity};
use logweir_core::spec::Notifications;
use logweir_engine_oso::storage::Store;
use logweir_kafka::reader::{ClusterReader, ConsumedRecord};
use std::collections::BTreeMap;
use std::time::Duration;

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

/// Reconciles by (partition, offset) — never by position, because a restored
/// topic's offsets need not start at the source's. Returns
/// (sampled, matching, mismatch descriptions).
pub fn compare(
    archive: &[RecordFingerprint],
    consumed: &[ConsumedRecord],
) -> (u64, u64, Vec<String>) {
    // The restored record carries x-original-offset when the backup wrote it;
    // otherwise we fall back to the target offset, which is correct on a fresh
    // scratch topic restored from offset 0.
    let by_offset: BTreeMap<i64, &ConsumedRecord> = consumed
        .iter()
        .map(|c| {
            let orig = c
                .headers
                .iter()
                .find(|(k, _)| k == "x-original-offset")
                .and_then(|(_, v)| v.as_ref())
                .and_then(|v| std::str::from_utf8(v).ok())
                .and_then(|s| s.parse::<i64>().ok());
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

/// A scratch cluster runs `cleanup.policy=delete` with infinite retention, so
/// those two topic-CONFIG keys are intended deviations.
const INTENDED: [&str; 2] = ["cleanup.policy", "retention.ms"];

/// Spec §9.3 phase 7(d) requires the replication-factor and partition-count
/// divergence RECORDED IN PHASE 3 to appear in `intentionally_deviated`. Neither
/// is a Kafka topic-config key, so neither can ever appear in either config map
/// — putting them in `INTENDED` (as an earlier draft did) meant the two values
/// phase 6 deliberately sets could never be reported at all. They are therefore
/// passed as explicit integers and compared separately.
pub fn classify_parity(
    source_cfg: &BTreeMap<String, String>,
    target_cfg: &BTreeMap<String, String>,
    src_partitions: i32,
    tgt_partitions: i32,
    src_rf: i16,
    tgt_rf: i16,
) -> (Vec<String>, Vec<String>) {
    let mut intended = Vec::new();
    let mut unexpected = Vec::new();
    for (k, v) in source_cfg {
        let differs = target_cfg.get(k).map(|t| t != v).unwrap_or(true);
        if !differs {
            continue;
        }
        if INTENDED.contains(&k.as_str()) {
            intended.push(k.clone())
        } else {
            unexpected.push(k.clone())
        }
    }
    // Phase 6 renders `default_replication_factor` (a scratch cluster has one
    // broker) and creates topics at the manifest's partition count, so both
    // divergences are intentional by construction.
    if src_partitions != tgt_partitions {
        intended.push("partition_count".into());
    }
    if src_rf != tgt_rf {
        intended.push("replication_factor".into());
    }
    intended.sort();
    unexpected.sort();
    (intended, unexpected)
}

// ---------------------------------------------------------------------------
// THE CHOKEPOINT (Task 19 fix round 3). Read this before adding any check.
//
// Rounds 1-3 each closed the lane the previous round's false pass was found
// in, and each left another lane open, because the invariant ("a Pass means
// every selection was actually checked") was spelled once per lane instead of
// once for the phase. It is now spelled exactly once, in `roll_up`, over a
// ledger every lane is obliged to fill: `SelectionVerdict`, one per
// `SampleSelection`, carrying `Evidence` that is a VALUE — never the absence
// of a recorded failure.
//
// If you find yourself adding a lane-specific guard below, that is the signal
// the mandate names: you have not found the chokepoint, you are patching a
// door. Add the lane's conclusion to its `Evidence` instead.

/// What one check concluded about one selection. Three variants, no default,
/// no `Ok`-shaped fourth state: a check that concluded nothing CANNOT be
/// constructed, which is the whole structural point. Every false pass this
/// phase has shipped came from a lane whose "I checked nothing" state was
/// spelled as *silence* — an untouched `result: Pass`, an unsummed zero, an
/// early `return` — rather than as a value someone downstream had to handle.
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
    /// What the MANIFEST claims this selection covers, capped by the
    /// selection's own `count`: `min(sel.count, Σ record_count)` over the
    /// segments the sampled window actually matches. `Σ record_count` over
    /// the in-window segments is exactly the per-partition term
    /// `phase4_sample` sums into the scorecard's `sample.records_expected`,
    /// and `sel.count` is `sample.records_per_partition` — but this is the
    /// MIN of the two, NOT `records_expected` itself, which `phase4_sample`
    /// deliberately leaves uncapped by `records_per_partition`. Both inputs
    /// are already-signed figures, so holding the archive to their minimum
    /// adds no new claim.
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
///    cannot produce a pass; it produces `Partial`, because the lane's
///    `Evidence` still is not `Verified`. That last sentence is only true
///    because every lane constructs `Evidence::Verified` from a POSITIVE,
///    non-vacuous predicate rather than as the fall-through of a chain of
///    negative tests (Task 19 fix round 3, second pass — see
///    `segment_evidence` and `verdict_for_selection`, where it was NOT true
///    when this comment was first written: deleting either lane's `Failed`
///    arm made a wholly corrupt selection fall through to `Verified`).
///    Pinned by mutants m5/m8 in the task-19 report's mutant table.
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
fn roll_up(verdicts: &[SelectionVerdict]) -> Integrity {
    if verdicts.is_empty() {
        return Integrity {
            level: IntegrityLevel::NotAttempted,
            result: IntegrityResult::Partial,
            partial_reason: Some(
                "no selection produced a verdict; a verification that examined nothing can \
                 never report a pass"
                    .into(),
            ),
            records_sampled: 0,
            records_sampled_matching: 0,
            mismatches: 0,
            pass_rate_measured: None,
            restored_principal_could_consume: None,
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

    let result = if verdicts.iter().any(SelectionVerdict::failed) {
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

    // The scorecard's three sample counters aggregate over EXACTLY the
    // selections whose records were reconciled against archive fingerprints
    // (`reconciled.is_some()`) — no others contribute, and none is silently
    // omitted, because every selection that does not contribute appears by
    // name in `partial_reason` and forbids a `Pass`. Task 20 signs these,
    // so what they aggregate is stated here rather than inferred.
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

    // `pass_rate_measured` is a RATIO OVER THE WHOLE SAMPLE or it is null.
    // Publishing `Some(1.0)` beside a partial verdict — the shape the
    // fourth-door review found signed onto a 96%-lossy partition — is a
    // misleading number even when every individual figure in it is true, so
    // the rate is withheld unless every selection's record lane reached a
    // conclusion (verified or failed). Also keeps Task 3's invariant
    // "pass_rate_measured set implies level == byte-fingerprint", and keeps
    // a zero denominator null rather than NaN (`to_deterministic_json`
    // refuses non-finite floats).
    // NOTE for anyone mutating this line (Task 19 round 3, third pass): its
    // value is only ever CONSULTED when no selection was downgraded, because
    // the `level == ByteFingerprint` conjunct below short-circuits first and
    // `level` is `ConsumeOnly` whenever any selection carries a `downgrade`.
    // A mutant that makes this predicate skip downgraded selections — or that
    // makes `records_conclusive` auto-true for them — is therefore
    // PROVABLY EQUIVALENT, not an untested gap: on the only inputs that can
    // reach the `&&`'s right-hand side, no selection is downgraded and the
    // two forms coincide. That equivalence rests on the ordering of this
    // conjunction, so if the `level` guard is ever moved, weakened or
    // reordered, this predicate becomes observable and needs its own test.
    let whole_sample_reconciled = verdicts.iter().all(SelectionVerdict::records_conclusive);
    let pass_rate_measured =
        if level == IntegrityLevel::ByteFingerprint && whole_sample_reconciled && sampled > 0 {
            Some(matching as f64 / sampled as f64)
        } else {
            None
        };

    let mut notes: Vec<String> = verdicts.iter().flat_map(SelectionVerdict::notes).collect();
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
    }
}

/// Every archive segment ONE selection actually samples, matched by (topic,
/// partition) and window overlap — the same window filter `phase4_sample`
/// itself uses. Free function, not `BackupSetFacts::sampled_segments`:
/// `BackupSetFacts` is defined in `logweir-core` (out of this task's declared
/// file scope — see `task-19-addendum.md` A2's corrected Files block), so an
/// inherent method cannot be added to it from here. The same substitution
/// applies to `probe_archive_modes` below.
///
/// Task 19 fix round 3: this no longer returns `Result`, and no longer
/// refuses. "Zero segments matched this selection" is now reported as
/// `Evidence::Unverified` by `segment_evidence` and routed through `roll_up`
/// like every other "compared nothing" case in the phase — see
/// `verdict_for_selection`'s doc comment for why that severity change was
/// deliberate.
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

/// Check (a) for ONE selection: sha256 over that selection's own sampled
/// archive segments, read back from the store.
///
/// Segments written before 0.21 carry an empty sha256 [VERIFIED
/// manifest.rs:376-380]. Round 2's code skipped them with a logged note and
/// the module doc claimed they were "never counted as a pass" — but nothing
/// in the code made that true: a pre-0.21 archive skipped every segment and
/// the run still reported `Pass`, the same silence-reads-as-success shape as
/// the fourth door, in a different lane. A skip is now `Unverified`: it is
/// coverage the drill did not obtain, and it is reported as such.
///
/// Task 19 fix round 3, second pass: `Evidence::Verified` is returned from a
/// POSITIVE, non-vacuous predicate (`!segs.is_empty() && checked ==
/// segs.len()`) rather than as the fall-through of a chain of negative
/// branches. That is what makes `roll_up`'s stated property 1 — "deleting a
/// lane's ability to record a failure produces `Partial`, never a pass" —
/// actually true here: with the `failed` branch deleted, a mismatching
/// segment simply never increments `checked`, so the positive predicate fails
/// and the selection lands on `Unverified`. Under the previous
/// negative-branch-chain shape it fell through to `Verified` and signed a
/// pass over segments whose sha256 did not match. The `!segs.is_empty()`
/// conjunct is load-bearing for the same reason `roll_up` answers the empty
/// ledger explicitly: `checked == segs.len()` is `0 == 0` over no segments,
/// vacuously true, which is this file's recurring bug in miniature.
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
        // `Store::get` returns `StoreError`, not directly convertible to
        // `DrillError` — routed through the existing `StoreError -> EngineError`
        // conversion (`logweir-engine-oso/src/storage.rs`) so `?` can then use
        // `DrillError`'s existing `#[from] EngineError`, rather than adding a
        // second `From` impl for a type this crate does not own.
        let (bytes, _vid) = store.get(&seg.key).map_err(EngineError::from)?;
        // Compared as BARE HEX on both sides. `SegmentFacts.sha256` is copied
        // verbatim out of the engine's manifest, where the field is bare hex
        // (`"sha256": "27f6c448…"` — measured against a real 0.21.0 manifest);
        // `sha256_prefixed` produces Logweir's own `sha256:<hex>` form, which
        // is what `engine.digest`, `approval.plan_hash` and
        // `target.topic_mapping_sha256` use. Comparing the two forms directly
        // made EVERY segment of a real archive report
        // "sha256 mismatch against the manifest", so the segment lane could
        // never return `Verified` outside the unit tests — which construct
        // `SegmentFacts` by hand and happen to write the prefixed form. Found
        // by Task 21c against MinIO. The `strip_prefix` accepts either form, so
        // a manifest that ever adopts the prefixed spelling still compares
        // correctly.
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

/// What one selection's ARCHIVE side offered. Task 19 fix round 3: PER
/// SELECTION. Round 2's `ArchiveMode` was one value for the whole backup set,
/// and `probe_archive_mode` returned `ConsumeOnly` on the FIRST `Unsupported`
/// — discarding every fingerprint set already collected, so one legacy
/// partition silently erased the byte-level claim for every other selection
/// AND moved the whole run onto a lane that had no zero-record guard at all.
#[derive(Debug)]
enum SelectionArchive {
    Fingerprints(Vec<RecordFingerprint>),
    /// THIS selection's segments cannot be fingerprinted (KBAK level below
    /// the gate). Carries the engine's own reason.
    Unsupported(String),
}

/// Probes each selection's archive support independently, in `sel`'s own
/// order (`out.len() == sel.len()`). A free function, not a method on `dyn
/// DataEngine` — see `sampled_segments_for`'s doc comment for why.
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

/// Builds the ONE verdict for ONE selection. Every lane's conclusion is a
/// value in the returned `SelectionVerdict`; nothing here decides pass or
/// fail, and nothing here may return "no opinion".
///
/// Consumes from the TARGET cluster — applying the topic-rename mapping
/// before ever calling `reader` — and calls `compare()` with ONLY this
/// selection's own archive fingerprints and ONLY this selection's own
/// consumed records. That is what makes the round-1 cross-partition offset
/// collision structurally impossible rather than merely untested (module doc,
/// "Reconciling PER SELECTION"): `compare()` never sees two selections at
/// once because it is handed one selection at a time.
///
/// `count` is the same per-partition cap `phase4_sample` applied to the
/// archive side; `from` is 0 because a drill's destination topic is always a
/// freshly created scratch topic (see `compare`'s own doc comment).
///
/// **The two lanes carry the same obligation.** Whether the archive could be
/// fingerprinted or not, this selection must produce positive evidence:
/// - byte-fingerprint: at least `claimed` fingerprints returned, and every
///   one of them reconciling;
/// - consume-only: at least `claimed` records actually read back from the
///   target.
///
/// Consume-only is a weaker CLAIM (no per-record integrity assertion) and not
/// a weaker CHECK. The fourth door existed because the obligation lived
/// inside the `ByteFingerprint` arm; it now lives in `Evidence`, which both
/// arms must construct.
///
/// **Short is unverified, not a smaller successful sample.** An archive that
/// returns 1 fingerprint where the manifest claims 25 has not sampled less
/// successfully — it has left 24 records unexamined, and reporting a
/// `pass_rate` of 1.0 over the 1 is exactly the false assurance the fourth
/// door signed over a 96%-lossy partition.
///
/// **Severity: a drill result, not an operational failure.** Round 2 split
/// this class of finding two ways — the zero-sample case degraded to
/// `Partial` and continued, the zero-segment case was a hard
/// `DrillError::Operational` (exit 1, no artifact). Both are now drill
/// results (exit 2, signed scorecard). Justification: every one of these
/// findings is a positively established fact ABOUT THE ARCHIVE OR THE RESTORE
/// (the archive holds no segment in the window; it returned fewer records
/// than its own manifest claims; the target gave back nothing) — not a
/// statement about Logweir's ability to run, which is what `Operational` and
/// exit 1 mean everywhere else in this codebase (see `DrillError`'s own
/// variant docs and phase 6's `RestoreNoOp` precedent, decided the same way
/// for the same reason). An auditor learns strictly more from a signed
/// `partial` naming the exact partition than from exit 1 with no document.
/// I/O against the store or the broker stays `Operational`: that genuinely
/// is Logweir failing to perform the check.
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
    // `min(sel.count, Σ record_count)`: the cap the sample asked for, or all
    // the manifest says exists in the window, whichever is smaller. Both
    // halves matter — without the cap a 25-record sample of a million-record
    // partition would always read short; without the manifest sum a window
    // holding fewer records than `records_per_partition` would too. When the
    // window matches no segment at all there is no manifest figure to use,
    // so the plan's own ask stands as the claim (and `segments` is already
    // `Unverified`, so the selection cannot pass regardless).
    let manifest_records: u64 = segs.iter().map(|g| g.record_count.max(0) as u64).sum();
    let cap = s.count as u64;
    let claimed = if segs.is_empty() {
        cap
    } else {
        cap.min(manifest_records)
    };

    let segments = segment_evidence(store, &segs)?;
    let consumed = reader.consume_range(mapped, s.partition, 0, s.count)?;
    let records_restored = consumed.len() as u64;

    let (records, reconciled, downgrade) = match archive {
        SelectionArchive::Fingerprints(fp) => {
            let (compared, matching, why) = compare(fp, &consumed);
            for w in why {
                tracing::error!(target: "logweir::verify", detail = %w, "reconciliation mismatch");
            }
            // POSITIVE first, and non-vacuous (`compared > 0`): at least
            // `claimed` fingerprints came back and every one of them
            // reconciled. Ordering the arms this way — rather than letting
            // `Verified` be the fall-through of a chain of negative tests —
            // is what makes `roll_up`'s stated property 1 true on this lane:
            // delete the `matching < compared` arm and a 100%-mismatching
            // sample lands on `Unverified` (Partial), never on `Verified`.
            let evidence = if compared > 0 && compared >= claimed && matching == compared {
                Evidence::Verified { checked: matching }
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
                Evidence::Unverified {
                    why: format!(
                        "the archive returned {compared} fingerprints where the manifest claims \
                         {claimed} for this selection; a short sample is coverage the drill did \
                         not obtain, not a smaller successful sample"
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
            // The SAME obligation, expressed at the only level this lane can
            // honestly claim: this partition must be shown to have given
            // back the records the manifest says it holds. It asserts
            // nothing about their contents — that is what makes the claim
            // weaker — and it asserts something, which is what the fourth
            // door did not.
            // POSITIVE first, and non-vacuous, exactly as on the
            // byte-fingerprint lane above — the two arms of this `match` are
            // deliberately the same shape, because they carry the same
            // obligation and differ only in the strength of the CLAIM.
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
                Evidence::Unverified {
                    why: format!(
                        "the target partition gave back {records_restored} records where the \
                         manifest claims {claimed} for this selection; a short read-back is \
                         coverage the drill did not obtain"
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
/// topic-rename mapping (module doc, point 2) before ever calling `reader`.
/// Target partition count comes from `reader.end_offsets` (one entry per
/// partition — a real, per-run READ of the cluster). Target replication
/// factor does NOT have an equivalent read: `ClusterReader` exposes no RF
/// accessor at all, so `plan.default_replication_factor` is used instead —
/// this is Task 19 fix round 2's correction (review's FIX 7 remainder) of a
/// wording bug in an earlier draft of this comment, which claimed "the exact
/// value phase 6 rendered when it created the topic" as if it were a
/// measurement. It is not: `plan.default_replication_factor` is what phase 6
/// ASKED the engine to create the topic with, never read back from the
/// broker afterward, so a genuine mismatch between the plan and what the
/// engine actually created (a bug, a broker-side override, anything) would
/// not be detected here. Read this field as an assertion about the PLAN, not
/// a measurement of the cluster (see this file's `classify_parity` test
/// `scratch_deviations_are_intentional_and_anything_else_is_not`, which
/// exercises the same assumption). When a backup predates
/// original-partition-count/replication-factor capture
/// (`TopicFacts.original_partition_count`/`source_replication_factor` are
/// `None`), the target's own value is used as the source value too, so an
/// unknown quantity is reported as "not different" rather than fabricating a
/// divergence claim this phase never actually measured.
///
/// Refuses `mapping.is_empty()` outright: a parity check folded over zero
/// topics returns `(vec![], vec![])`, which reads exactly like "checked every
/// topic and found no divergence" — this build has already shipped that
/// exact bug once (a drift checker that reported agreement having compared
/// nothing) and must not ship it again here.
fn classify_parity_all(
    facts: &BackupSetFacts,
    reader: &dyn ClusterReader,
    mapping: &BTreeMap<String, String>,
    plan: &RestorePlan,
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
    for (src, tgt) in mapping {
        let Some(t) = facts.topics.iter().find(|t| &t.name == src) else {
            return Err(DrillError::Operational(format!(
                "topic parity check: no archive facts for mapped source topic `{src}`"
            )));
        };
        let target_cfg = reader.topic_configs(tgt)?;
        let tgt_partitions = reader.end_offsets(tgt)?.len() as i32;
        let tgt_rf = plan.default_replication_factor;
        let src_partitions = t.original_partition_count.unwrap_or(tgt_partitions);
        let src_rf = t.source_replication_factor.unwrap_or(tgt_rf);
        let (intended, unexpected) = classify_parity(
            &t.configurations,
            &target_cfg,
            src_partitions,
            tgt_partitions,
            src_rf,
            tgt_rf,
        );
        intended_all.extend(intended.into_iter().map(|k| format!("{tgt}: {k}")));
        unexpected_all.extend(unexpected.into_iter().map(|k| format!("{tgt}: {k}")));
    }
    intended_all.sort();
    unexpected_all.sort();
    Ok(TopicParity {
        intentionally_deviated: intended_all,
        unexpected_divergence: unexpected_all,
    })
}

/// Wraps `DataEngine::validation_run` — see that method's doc comment
/// (`logweir-core/src/engine.rs`) for why the trait needed a new default
/// method to make this call reachable through `&dyn DataEngine` at all.
///
/// Returns `Result<EngineRun, EngineError>` — the engine's OWN error type,
/// deliberately NOT converted (and NOT propagated with `?`) into
/// `DrillError` at this call site. This is the load-bearing half of "the
/// known issue" (this file's module doc comment): because the engine's own
/// `message_count`/`offset_range` checks compare against the manifest's
/// SOURCE topic names, they are EXPECTED to report a discrepancy — quite
/// possibly a non-zero exit, quite possibly an `Err` from a real
/// implementation that maps a failing check to one — on every renamed drill,
/// including a perfectly healthy one. If this function's result were
/// propagated with `?` into `run`'s own `Result<_, DrillError>`, that
/// EXPECTED engine-side failure would abort phase 7 entirely: exactly the
/// "fails healthy drills" failure mode the module doc comment warns against.
/// The caller (`run`, below) therefore matches on this `Result` itself and
/// logs either arm — never a `?`. Pinned by
/// `verify_phase.rs`'s `a_failing_engine_validation_run_never_fails_or_aborts_the_drill`.
fn engine_validation_run(
    engine: &dyn DataEngine,
    plan: &RestorePlan,
) -> Result<EngineRun, EngineError> {
    engine.validation_run(plan)
}

/// The maximum `ConsumedRecord.timestamp_ms` across every mapped TARGET
/// topic/partition — what phase 8's RPO reads. Reads only the last record of
/// each partition (`end_offsets` then `consume_range` at `hi - 1`, count 1),
/// a DECLARED DEVIATION (Task 19 fix round 2, review finding F11) from the
/// brief's literal prose ("`newest_ts` takes the maximum
/// `ConsumedRecord.timestamp_ms` seen"), which would mean reusing whatever
/// `verdict_for_selection` already consumed. The real reason for the
/// deviation, not "a local didn't survive a match arm" (an implementation
/// detail of a since-removed code shape, not a justification): a `head`
/// anchor's sample holds the OLDEST records in the window (`SampleSelection`'s
/// own doc comment), so taking the max over ONLY what was sampled/consumed
/// for the canary would badly UNDERSTATE the true newest restored timestamp
/// and badly OVERSTATE the signed RPO. Reading the actual last record of
/// each partition — independent of the sample — is what makes the RPO
/// figure honest regardless of which anchor the drill used. This costs one
/// extra broker round trip per mapped partition and assumes offset order
/// approximates timestamp order (true for any topic that is not manually
/// reordered), both accepted trade-offs for that correctness.
///
/// Refuses `mapping.is_empty()` (nothing to measure) and refuses finding zero
/// records across every mapped partition (there is no honest timestamp to
/// report) — the latter should be unreachable in practice, because phase 6's
/// own post-condition (`phase6_restore::assert_post_condition`) already
/// requires at least one non-empty partition among these same mapped
/// destinations before phase 7 ever runs; the guard stays here anyway so a
/// caller that reaches this function some other way cannot get a fabricated
/// `0` (1970-01-01) read as a real timestamp.
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

/// Phase 7's four checks, routed through ONE ledger and ONE roll-up.
///
/// The shape is deliberate and is the fix for the fourth false pass: `run`
/// does not compute a verdict. It builds exactly one `SelectionVerdict` per
/// `SampleSelection` — every lane, every selection, no exceptions — and hands
/// the whole ledger to `roll_up`, which is the single place `IntegrityResult`
/// is decided. There is no `result` variable here to initialise to `Pass` and
/// forget to move.
pub fn run(
    engine: &dyn DataEngine,
    reader: &dyn ClusterReader,
    store: &Store,
    facts: &BackupSetFacts,
    sel: &[SampleSelection],
    mapping: &BTreeMap<String, String>,
    plan: &RestorePlan,
) -> Result<VerifyOutcome, DrillError> {
    // OSO's own rule, adopted throughout this codebase (phase4_sample's own
    // empty-candidates guard is the precedent): zero selections scanned is
    // never a positive result. This one IS operational — a plan that named
    // no partition to sample is a broken plan, not a fact about the archive
    // — and `roll_up` refuses an empty ledger independently anyway, so
    // deleting this guard cannot produce a pass either.
    if sel.is_empty() {
        return Err(DrillError::Operational(
            "phase7_verify::run called with zero sample selections; a fingerprint comparison \
             over an empty set would report a pass that means nothing"
                .into(),
        ));
    }

    // (b) `validation run --config validation.yaml --triggered-by <s>`
    // [VERIFIED U/kafka-backup/crates/kafka-backup-cli/src/main.rs:475-489]. Only
    // the exit code is recorded; the content is never read as corroboration of
    // Logweir's own measurement. Deliberately NOT `?` — see
    // `engine_validation_run`'s doc comment ("the known issue"): an engine-side
    // failure here is EXPECTED on a renamed drill and must never abort this
    // phase or influence `integrity.result`.
    match engine_validation_run(engine, plan) {
        Ok(vr) => tracing::info!(target: "logweir::verify", exit_code = vr.exit_code,
                                  "engine `validation run` finished"),
        Err(e) => tracing::warn!(target: "logweir::verify", error = %e,
                                  "engine `validation run` failed or could not be run; \
                                   continuing — its verdict is never corroboration for \
                                   Logweir's own measurement"),
    }

    // (a) + (c). One verdict per selection, built by iterating `sel` ITSELF —
    // never by iterating whatever `probe_archive_modes` happened to return —
    // so `verdicts.len() == sel.len()` holds by construction of this loop and
    // a selection cannot go unjudged without being deleted from the plan.
    //
    // Task 19 fix round 3, second pass: this loop was `sel.iter().zip(
    // archives.iter())` guarded by a `debug_assert_eq!` on the two lengths.
    // `zip` stops at the SHORTER side and `debug_assert!` is compiled out of
    // every release build (this workspace sets no `[profile.release]
    // debug-assertions`), so in the SHIPPED binary a short `archives` would
    // have silently dropped the trailing selections from the ledger — and a
    // ledger that is missing a selection entirely is `all(fully_verified)`
    // over the survivors, i.e. the same "compared nothing, said nothing"
    // false pass in a fifth door, one the ledger cannot see because the
    // missing selection leaves no `Evidence::Unverified` behind. Indexing
    // `sel` and REFUSING a missing answer (rather than truncating to it)
    // makes the ledger total in every build. This is not a lane-specific
    // guard: it is the chokepoint's own precondition — `roll_up` can only be
    // sound if the ledger it reads covers every selection.
    let archives = probe_archive_modes(engine, sel)?;
    let mut verdicts = Vec::with_capacity(sel.len());
    for (i, s) in sel.iter().enumerate() {
        let archive = archives.get(i).ok_or_else(|| {
            DrillError::Operational(format!(
                "probe_archive_modes answered for {} of {} selections; refusing to verify a \
                 ledger that cannot cover {}/{}",
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

    let integrity = roll_up(&verdicts);
    for v in &verdicts {
        tracing::info!(target: "logweir::verify", selection = %v.id, claimed = v.claimed,
                       segments = ?v.segments, records = ?v.records,
                       records_restored = v.records_restored, "selection verdict");
    }
    // The sum over every selection's own consumed count — see
    // `records_restored_is_the_consumed_count_not_matched_plus_mismatched`.
    let records_restored: u64 = verdicts.iter().map(|v| v.records_restored).sum();

    // (d) Topic-config parity.
    let topic_parity = classify_parity_all(facts, reader, mapping, plan)?;

    Ok(VerifyOutcome {
        integrity,
        topic_parity,
        records_restored,
        newest_restored_ts_ms: newest_ts(reader, mapping)?,
        verified_at: chrono::Utc::now(),
    })
}

/// A webhook URL reduced to the part that identifies the SINK, never the part
/// that authorises posting to it.
///
/// The implementation moved to `logweir_core::spec::redact_url` in fix round 1
/// (finding F1) and is re-exported here so that every existing caller and
/// every test keeps the path it already uses. It had to move because the
/// second site that has to redact — `Notifications`' hand-written `Debug` —
/// lives in `logweir-core`, which cannot depend on this crate; a second copy
/// of a redactor is how the two copies come to disagree. The function is pure
/// string arithmetic and reads no clock, no file and no socket, so it costs
/// the pure layer nothing (GC1; `scripts/check-pure-core.sh` passes).
pub use logweir_core::spec::redact_url;

/// What a failed notification may say about itself, with the URL taken out.
///
/// `ureq::Error::Status(code, response)` displays as `<url>: status code <n>`
/// and `Transport` embeds the URL too, so neither may be printed. The status
/// code is the whole diagnostic payload and carries no secret; a transport
/// failure is reported by kind only.
fn redact_ureq_error(e: &ureq::Error) -> String {
    match e {
        ureq::Error::Status(code, _) => format!("status code {code}"),
        ureq::Error::Transport(_) => "transport error".into(),
    }
}

/// The JSON summary posted to every sink and embedded in PagerDuty's
/// `custom_details`.
///
/// Public, and separate from `notify`, so its SHAPE can be asserted without a
/// network — `redact_url` next door is public for exactly the same reason, and
/// `crates/logweir/tests/notify.rs` drives both directly.
///
/// This is **the only surface that reaches a human away from a terminal**, so
/// what it omits is what an on-call reader never learns. T0-3: it carried no
/// `redactions`, so a redacted scorecard notified as if whole.
///
/// `redactions` is always present — an empty array for the whole document
/// every v0.1 run writes — so a sink can branch on it without having to tell
/// "absent" from "none". The PATHS travel and the `reason` strings do NOT: a
/// `reason` is free text arriving with a document that, by construction, no
/// Logweir writer produced, and this body is pasted verbatim into Slack and
/// PagerDuty. The path is the whole actionable signal.
pub fn notify_body(sc: &Scorecard) -> serde_json::Value {
    serde_json::json!({
        "run_id": sc.run_id,
        "outcome": sc.outcome,
        "rto_excluding_preflight_seconds": sc.measured.rto_excluding_preflight_seconds,
        "rpo_seconds": sc.measured.rpo_seconds,
        "integrity": { "level": sc.integrity.level, "result": sc.integrity.result },
        "self_attested": sc.approval.self_attested,
        "redactions": sc.redactions.iter().map(|r| r.path.as_str()).collect::<Vec<_>>(),
    })
}

/// How long one notification POST may take before it is written off.
///
/// Task 5b. There was NO timeout here at all: every sink was posted with
/// ureq 2.12.1's free-function request builder, which constructs a throwaway
/// agent carrying that crate's defaults — no connect, read or overall timeout,
/// in its own words *"requests may block forever on reads by default"*. (The
/// spelling is not repeated here: `crates/logweir/tests/notify.rs` fails if
/// those builders appear anywhere under `crates/logweir/src/`.) A webhook
/// endpoint that ACCEPTS the
/// connection and then never replies hung this function, and therefore the
/// drill, forever — after the scorecard was signed and uploaded. The drill had
/// already succeeded and produced its evidence; the process just never
/// returned to say so, and the operator watching it had no way to tell a
/// hung notification from a hung restore.
///
/// A refused connection was never the risk: the kernel answers a closed port
/// with an RST in microseconds, which is why the existing tests were fast and
/// why this went unnoticed. A firewall that DROPs instead of REJECTing, or a
/// sink that is merely wedged, is the case with no bound.
///
/// The numbers. `timeout_connect` bounds the TCP handshake; `timeout` bounds
/// the whole call, handshake included, and takes precedence over the
/// per-socket read and write timeouts (ureq's own contract), so the two
/// together are the complete bound: **no sink can cost more than
/// `NOTIFY_TIMEOUT`, and the worst case for a spec with `w` webhooks, a Slack
/// sink and a PagerDuty key is `(w + 2) * NOTIFY_TIMEOUT`.** Ten seconds is
/// long enough that a merely slow sink is still notified — losing a page
/// because PagerDuty took four seconds would be a worse failure than the one
/// being fixed — and short enough that a wedged one is written off promptly.
pub const NOTIFY_CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
/// The overall bound on one notification POST. See `NOTIFY_CONNECT_TIMEOUT`.
pub const NOTIFY_TIMEOUT: Duration = Duration::from_secs(10);

/// The agent every notification POST goes through. Public so a test can build
/// one with a short bound and drive `notify_with` against a listener that
/// accepts and never replies — the failure mode with no bound — without
/// spending the production bound to do it.
pub fn notify_agent_with(connect: Duration, overall: Duration) -> ureq::Agent {
    ureq::AgentBuilder::new()
        .timeout_connect(connect)
        .timeout(overall)
        .build()
}

/// The production agent: `NOTIFY_CONNECT_TIMEOUT` and `NOTIFY_TIMEOUT`.
pub fn notify_agent() -> ureq::Agent {
    notify_agent_with(NOTIFY_CONNECT_TIMEOUT, NOTIFY_TIMEOUT)
}

/// PagerDuty's Events v2 enqueue endpoint for the **US** service region — the
/// default when `notifications.pagerduty_endpoint` is absent.
///
/// This was a bare string literal inside the POST. An adopter whose PagerDuty
/// account lives on the EU service region therefore enqueued into a region
/// that does not hold their account and never saw a page; the non-2xx was
/// swallowed into a warning. The literal now exists exactly once, here, and
/// `pagerduty_endpoint` below is the only thing that decides what is posted to.
pub const PAGERDUTY_US_ENDPOINT: &str = "https://events.pagerduty.com/v2/enqueue";

/// The one line an operator greps for when a page did not arrive.
///
/// Emitted whenever a configured PagerDuty route produced NO incident — the
/// endpoint was refused before a request, or the request itself failed. A
/// silenced alert that logs nothing is indistinguishable from a drill nobody
/// configured an alert for, which is the whole defect this task closes: the
/// route must either deliver an event that describes *this* drill or say, by
/// name, that it did not.
pub const PAGERDUTY_SILENCED: &str = "pagerduty alert NOT delivered";

/// The seam every outbound notification POST goes through.
///
/// It exists so a test can observe the exact payload — `event_action`,
/// `dedup_key`, the endpoint URL — without opening a socket, which GC17
/// requires: no test in this repository may reach `events.pagerduty.com` or
/// any other public host. The production implementation is `UreqSink`, which
/// posts through `notify_agent()` and therefore keeps Task 5b's bound; the
/// structural test `every_notification_post_goes_through_the_bounded_agent`
/// still forbids a bare `ureq::post` anywhere under `crates/logweir/src/`.
pub trait EventSink {
    /// `Err` carries an **already-redacted** description: no URL, no
    /// credential, no `ureq::Error` `Display` (which embeds the request URL).
    fn post(&self, url: &str, body: &serde_json::Value) -> Result<(), String>;
}

/// The production `EventSink`: one bounded `ureq::Agent`, reused for every
/// sink in a run so the timeout policy is decided in exactly one place.
pub struct UreqSink {
    agent: ureq::Agent,
}

impl UreqSink {
    /// The production sink, carrying `notify_agent()`'s bounds.
    pub fn new() -> Self {
        Self {
            agent: notify_agent(),
        }
    }

    /// A sink over a caller-supplied agent, so a test can inject a short bound
    /// instead of waiting out the production one.
    pub fn with_agent(agent: ureq::Agent) -> Self {
        Self { agent }
    }
}

impl Default for UreqSink {
    fn default() -> Self {
        Self::new()
    }
}

impl EventSink for UreqSink {
    fn post(&self, url: &str, body: &serde_json::Value) -> Result<(), String> {
        self.agent
            .post(url)
            .send_json(body)
            .map(|_| ())
            // Redacted HERE, at the boundary, so no caller can reach a
            // `ureq::Error` whose `Display` embeds the request URL.
            .map_err(|e| redact_ureq_error(&e))
    }
}

/// Which endpoint this spec's PagerDuty events go to, or why none will be sent.
///
/// `Ok(url)` is the endpoint to POST to; `Err(reason)` means the route is
/// REFUSED before any request is made and the caller must log
/// `PAGERDUTY_SILENCED`.
///
/// Only `https://` is accepted. The routing key travels in the request body,
/// so a plaintext endpoint would put a bearer credential on the wire, and a
/// misconfigured scheme is a configuration error a human must fix rather than
/// something to attempt and let fail. The refusal names the SCHEME and nothing
/// else: the rest of the URL may carry whatever the adopter typed, and this
/// string reaches a log.
pub fn pagerduty_endpoint(n: &Notifications) -> Result<String, String> {
    match &n.pagerduty_endpoint {
        None => Ok(PAGERDUTY_US_ENDPOINT.to_string()),
        Some(u) if u.starts_with("https://") => Ok(u.clone()),
        Some(u) => {
            let scheme = match u.split_once("://") {
                Some((s, _)) if !s.is_empty() => s,
                _ => "<no scheme>",
            };
            Err(format!(
                "notifications.pagerduty_endpoint must be an https:// URL; \
                 refusing scheme `{scheme}`"
            ))
        }
    }
}

/// R-D's interim dedup key for a drill that produced a scorecard.
///
/// PagerDuty's Events v2 API keys an incident by `dedup_key`. This was
/// `format!("logweir-drill-{}", sc.target.cluster_id)`, so every drill spec
/// pointed at one cluster shared ONE incident — and because a passing drill
/// sends `event_action: resolve`, a nightly smoke drill passing at 03:00
/// silently closed the weekly full drill's open page. Two different outcomes
/// must produce two different incidents.
///
/// The identity is the spec's own `name` when it has one. When it does not,
/// the fallback is the first 12 hex characters of the approval's `plan_hash`
/// — a sha256 over the approved plan bytes, so it is distinct per spec and
/// stable across re-runs of the same spec, which is exactly what a dedup key
/// has to be. The run id would be distinct but NOT stable, and a key
/// containing it never dedups at all.
///
/// **`PLAN_HASH_IDENT_LEN` is a guard, not a length calculation** (fix round 1,
/// finding F3). A `plan_hash` too short to supply 12 characters — an empty one
/// above all — collapsed this to `logweir-drill--{cluster_id}`, which is the
/// PRE-FIX SHAPE with a stray hyphen: one incident per cluster, every drill
/// resolving every other drill's page, the exact defect R-D closes. It is not
/// reachable today, because `phase1_approval::verify` recomputes
/// `sha256(spec_text)` and refuses before any scorecard exists, so the field is
/// always a real digest by the time this runs. But it was SILENT, and a
/// degenerate dedup key does not announce itself: the page simply stops
/// arriving. So a hash that cannot identify anything falls back to `"unnamed"`,
/// the same sentinel `failure_dedup_key` uses for a spec with no name — a key
/// that is honest about having no identity, and structurally incapable of being
/// the old one.
///
/// Ruling **R-D**. The durable fix is an artifact-side `drill_id` (backlog
/// T1-8), which §12 assigns to decision **O16** with default *not funded*; the
/// residual is recorded in `docs/stability.md`.
pub fn dedup_key(spec_name: Option<&str>, sc: &Scorecard) -> String {
    /// How much of the `plan_hash` identifies the spec. 12 hex characters is
    /// 48 bits — collision-free across any plausible number of drill specs.
    const PLAN_HASH_IDENT_LEN: usize = 12;

    let ident = match spec_name {
        Some(n) => n.to_string(),
        None => {
            let h: String = sc
                .approval
                .plan_hash
                .trim_start_matches("sha256:")
                .chars()
                .take(PLAN_HASH_IDENT_LEN)
                .collect();
            if h.len() < PLAN_HASH_IDENT_LEN {
                "unnamed".to_string()
            } else {
                h
            }
        }
    };
    format!("logweir-drill-{ident}-{}", sc.target.cluster_id)
}

/// The dedup key for a drill that produced NO scorecard — exits 1, 3 and 4.
///
/// There is no cluster id to key on: `TargetSpec` does not carry one, and
/// `Scorecard::target.cluster_id` is discovered at runtime in phase 0, which
/// on these paths may never have run. So the key is the spec name alone.
///
/// It is DELIBERATELY distinct from `dedup_key`'s, and that is a property, not
/// an accident: "logweir could not run this drill" and "this drill ran and did
/// not pass" are different facts about different things, and a later passing
/// run's `resolve` must not automatically close an operational-failure
/// incident that nobody has looked at. An unnamed spec collapses to one
/// incident per Logweir install — the second half of the residual recorded in
/// `docs/stability.md`, and the reason to give a spec a `name`.
pub fn failure_dedup_key(spec_name: Option<&str>) -> String {
    format!("logweir-drill-{}-preflight", spec_name.unwrap_or("unnamed"))
}

/// Resolve the endpoint, POST, and make a silenced alert visible either way.
///
/// GC11: every outcome here is logged and swallowed. A refused endpoint, a
/// down PagerDuty and a timeout all leave the exit code exactly as the drill
/// decided it.
fn enqueue_pagerduty(
    n: &Notifications,
    ev: &serde_json::Value,
    dedup_key: &str,
    run_id: &str,
    sink: &dyn EventSink,
) {
    let url = match pagerduty_endpoint(n) {
        Ok(u) => u,
        Err(reason) => {
            tracing::warn!(target: "logweir::notify", run_id = %run_id,
                           dedup_key = %dedup_key, reason = %reason,
                           silenced = true, "{PAGERDUTY_SILENCED}");
            return;
        }
    };
    // THE POST GOES TO `url`; ONLY THE LOG SEES `shown`. Fix round 1, F1: this
    // logged the endpoint verbatim, on the argument that it is not a secret.
    // It is not — but it is free-form adopter input, and `redact_url` keeps
    // `scheme://host/…`, which is 100 % of the stated diagnostic (WHICH region
    // the events went to) while dropping the userinfo, path and query, which
    // is where a token in a pasted URL lives. The failure arm and the success
    // arm are redacted identically: a credential that is safe on one and
    // logged on the other is still logged. The routing key travels in the BODY
    // and is never logged at all.
    let shown = redact_url(&url);
    if let Err(e) = sink.post(&url, ev) {
        tracing::warn!(target: "logweir::notify", run_id = %run_id,
                       dedup_key = %dedup_key, endpoint = %shown, error = %e,
                       silenced = true, "{PAGERDUTY_SILENCED}");
    } else {
        tracing::info!(target: "logweir::notify", run_id = %run_id,
                       dedup_key = %dedup_key, endpoint = %shown,
                       "pagerduty event enqueued");
    }
}

/// The exit-1 / exit-3 / exit-4 page: a drill that produced no scorecard.
///
/// The PagerDuty route used to fire from exactly one place — phase 8, AFTER
/// the scorecard was signed and uploaded — so it was loud when the drill had
/// already succeeded at producing evidence and SILENT in the three situations
/// that need a human: an operational failure with no artifact (1), a plan a
/// guard refused (3), and a result nobody could attest (4).
///
/// It sends nothing to `webhooks` / `slack_webhook` on purpose. Those are the
/// scorecard-summary route: their body is `notify_body(sc)` and there is no
/// scorecard here. Inventing a scorecard-shaped body without a scorecard is
/// how a dashboard starts reporting drills that never ran.
pub fn notify_failure(
    n: &Notifications,
    spec_name: Option<&str>,
    run_id: &str,
    code: ExitCode,
    message: &str,
) {
    notify_failure_with(n, spec_name, run_id, code, message, &UreqSink::new())
}

/// `notify_failure` with the sink injected — see `EventSink`.
pub fn notify_failure_with(
    n: &Notifications,
    spec_name: Option<&str>,
    run_id: &str,
    code: ExitCode,
    message: &str,
    sink: &dyn EventSink,
) {
    // Opt-in, exactly as `notify_with_sink` is: no routing key, no route.
    let Some(key) = &n.pagerduty_routing_key else {
        return;
    };
    let dedup = failure_dedup_key(spec_name);
    let severity = match code {
        // A guard refusing a plan BEFORE anything ran is a configuration
        // finding, not an outage: nothing was touched and nothing is at risk.
        ExitCode::GuardRefused => "warning",
        // 1 and 4 both mean the drill's evidence does not exist. Anything else
        // reaching here would be a bug, and "critical" is the safe default for
        // a case nobody anticipated.
        _ => "critical",
    };
    let ev = serde_json::json!({
        "routing_key": key,
        // NEVER `resolve`. This path has no result to clear.
        "event_action": "trigger",
        "dedup_key": dedup,
        "payload": {
            "summary": format!(
                "logweir drill {run_id}: no scorecard (exit {}) — {message}",
                code as u8
            ),
            "source": spec_name.unwrap_or("unnamed"),
            "severity": severity,
            "custom_details": {
                "run_id": run_id,
                "exit_code": code as u8 as i64,
                "error": message,
            },
        },
    });
    enqueue_pagerduty(n, &ev, &dedup, run_id, sink);
}

/// POSTs one JSON summary per configured sink. EVERY transport failure is
/// logged and swallowed: the drill result is a measurement, and a webhook being
/// down must never change it. A TIMEOUT is one of those transport failures and
/// is swallowed identically — it arrives as `ureq::Error::Transport`, is
/// logged as `transport error`, and changes neither the scorecard nor the exit
/// code. The exit contract (GC11) is untouched: by the time this runs the
/// scorecard is signed and uploaded, and a sink that would not answer is not
/// a drill result. `ureq` is blocking on purpose — it adds no async runtime to
/// `crates/logweir`.
///
/// NO SINK URL REACHES A LOG LINE INTACT — see `redact_url`. That includes the
/// error arm: `ureq::Error`'s own `Display` embeds the request URL, so
/// `error = %e` leaked the same credential a second time, by a route a reader
/// of the `url = %url` field alone would not have noticed.
///
/// The body it posts is `notify_body` next door, which is where the shape —
/// and what it deliberately omits — is documented and tested.
pub fn notify(n: &Notifications, spec_name: Option<&str>, sc: &Scorecard) {
    notify_with(&notify_agent(), n, spec_name, sc)
}

/// `notify` with the agent injected, so the timeout bound is a parameter of
/// the test rather than a property the test has to wait out. Every POST in
/// this module goes through the passed agent — `crates/logweir/tests/notify.rs`
/// asserts structurally that no bare `ureq::post`/`ureq::get` survives
/// anywhere in `crates/logweir/src/`, because a revert to one would restore
/// the unbounded wait and pass every behavioural test that supplies its own
/// agent.
pub fn notify_with(
    agent: &ureq::Agent,
    n: &Notifications,
    spec_name: Option<&str>,
    sc: &Scorecard,
) {
    notify_with_sink(n, spec_name, sc, &UreqSink::with_agent(agent.clone()))
}

/// `notify` with the SINK injected rather than the agent, so a test can read
/// the exact `event_action`, `dedup_key` and endpoint URL off the recorded
/// payload without opening a socket (GC17).
///
/// This is the brief's `notify_with(n, spec_name, sc, sink)` under a different
/// name: `notify_with` was already taken by Task 5b's agent-injected variant
/// above, whose behavioural tests must keep working unchanged.
pub fn notify_with_sink(
    n: &Notifications,
    spec_name: Option<&str>,
    sc: &Scorecard,
    sink: &dyn EventSink,
) {
    let body = notify_body(sc);
    let mut sinks: Vec<String> = n.webhooks.clone();
    if let Some(u) = &n.slack_webhook {
        sinks.push(u.clone());
    }
    for url in sinks {
        let shown = redact_url(&url);
        match sink.post(&url, &body) {
            Ok(()) => tracing::info!(target: "logweir::notify", sink = %shown, "notified"),
            // `error = %e` is NOT logged: `ureq::Error`'s `Display` embeds the
            // request URL, credential and all. What an operator needs from a
            // failed notification is which sink and what kind of failure, and
            // both survive `redact_url` + the variant name — `EventSink::post`
            // hands back an already-redacted description for that reason.
            Err(e) => tracing::warn!(target: "logweir::notify", sink = %shown,
                                     error = %e,
                                     "notification failed; continuing"),
        }
    }
    if let Some(key) = &n.pagerduty_routing_key {
        let dedup = dedup_key(spec_name, sc);
        let ev = serde_json::json!({
            "routing_key": key,
            "event_action": if sc.outcome == logweir_core::outcome::Outcome::Pass
                            { "resolve" } else { "trigger" },
            "dedup_key": dedup,
            "payload": { "summary": format!("logweir drill {}: {:?}", sc.run_id, sc.outcome),
                         "source": sc.target.cluster_id, "severity": "warning",
                         "custom_details": body },
        });
        enqueue_pagerduty(n, &ev, &dedup, &sc.run_id, sink);
    }
}

#[cfg(test)]
mod tests {
    //! Direct, fine-grained coverage of the private helpers' own empty-set
    //! guards — narrower and faster than driving them through `run`.
    //! `crates/logweir/tests/verify_phase.rs` additionally pins each of these
    //! at its actual call site inside `run`.
    use super::*;
    use logweir_core::engine::{BackupSetRef, PartitionFacts, TopicFacts};

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

    // -----------------------------------------------------------------
    // `sampled_segments_for` (was `sampled_segments`, which took the whole
    // `sel` slice and returned `Result`).
    //
    // PREMISE CHANGE, deliberate and documented in `sampled_segments_for`'s
    // own doc comment: round 2's function REFUSED (`DrillError::Operational`,
    // exit 1, no artifact) when a selection matched zero segments. Round 3
    // routes that finding through `Evidence::Unverified` and `roll_up` like
    // every other "compared nothing" case, because "the archive holds no
    // segment in this window" is a fact ABOUT THE ARCHIVE — a drill result,
    // exit 2 with a signed scorecard — not Logweir failing to run. The three
    // tests below therefore assert the SAME safety property the originals
    // did (a selection matching nothing can never be counted as checked),
    // restated at its new home: an empty match plus the `Unverified` the
    // empty match produces.

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
    /// segment's own `start_timestamp..end_timestamp` (0..100): must be
    /// excluded, not matched regardless of window. Pins the window filter
    /// itself, distinct from
    /// `..._a_partition_the_facts_lack_matches_nothing_and_is_unverified`
    /// (which pins the empty-match consequence via a partition mismatch, not
    /// a window mismatch) and from `..._finds_the_real_match` (which never
    /// exercises a non-overlapping window, so a mutant deleting the window
    /// filter entirely would still pass it).
    #[test]
    fn sampled_segments_for_excludes_a_segment_outside_the_requested_window() {
        let facts = facts_one_segment();
        let segs = sampled_segments_for(&facts, &sel_for("orders", 0, (200, 300)));
        assert!(segs.is_empty());
        let (store, _) = store_with("logweir/k", b"anything");
        assert!(segment_evidence(&store, &segs).unwrap().is_unverified());
    }

    /// Task 19 fix round 2 ("check for the third door"), restated for round
    /// 3's structure. The original asserted that `sampled_segments`' AGGREGATE
    /// `out.is_empty()` check could not hide an empty selection behind a
    /// healthy sibling. That aggregate no longer exists to be wrong:
    /// `sampled_segments_for` is handed ONE selection and cannot see a
    /// sibling. What must still be proven is the consequence at the
    /// chokepoint — a ledger holding one fully-verified selection and one
    /// whose segments matched nothing is `Partial`, and names the empty one.
    #[test]
    fn one_selection_matching_no_segment_cannot_hide_behind_a_healthy_sibling() {
        let facts = facts_one_segment();
        let healthy = sampled_segments_for(&facts, &sel_for("orders", 0, (0, 100)));
        let empty = sampled_segments_for(&facts, &sel_for("orders", 7, (0, 100)));
        assert_eq!(healthy.len(), 1);
        assert!(empty.is_empty());

        let (store, _) = store_with("logweir/k", b"anything");
        let integrity = roll_up(&[
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
        ]);
        assert_eq!(
            integrity.result,
            IntegrityResult::Partial,
            "a selection whose segments matched nothing must forbid a Pass no matter what a \
             sibling selection contributed"
        );
        assert!(integrity.partial_reason.unwrap().contains("orders/7"));
    }

    // -----------------------------------------------------------------
    // `roll_up` — THE chokepoint. Driven directly, because these are
    // properties of the roll-up itself rather than of any lane.

    /// The trap the module doc names explicitly: `all()` over an EMPTY slice
    /// is vacuously TRUE, so a `roll_up` that reached its `all(fully_verified)`
    /// arm with no verdicts at all would report the strongest possible claim
    /// over the weakest possible evidence. This is the fifth door in embryo
    /// and it is answered first and explicitly.
    #[test]
    fn roll_up_on_an_empty_ledger_is_partial_never_pass() {
        let integrity = roll_up(&[]);
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
        let both = roll_up(&[verdict(
            "orders/0",
            Evidence::Verified { checked: 1 },
            Evidence::Verified { checked: 10 },
        )]);
        assert_eq!(both.result, IntegrityResult::Pass);

        // Segment lane silent -> not a pass.
        let seg_silent = roll_up(&[verdict(
            "orders/0",
            Evidence::Unverified {
                why: "no sha256".into(),
            },
            Evidence::Verified { checked: 10 },
        )]);
        assert_eq!(seg_silent.result, IntegrityResult::Partial);

        // Record lane silent -> not a pass.
        let rec_silent = roll_up(&[verdict(
            "orders/0",
            Evidence::Verified { checked: 1 },
            Evidence::Unverified {
                why: "nothing came back".into(),
            },
        )]);
        assert_eq!(rec_silent.result, IntegrityResult::Partial);

        // Either lane failing -> Fail, which outranks both of the above.
        let failed = roll_up(&[verdict(
            "orders/0",
            Evidence::Verified { checked: 1 },
            Evidence::Failed {
                why: "mismatch".into(),
            },
        )]);
        assert_eq!(failed.result, IntegrityResult::Fail);
    }

    /// Task 19 fix round 3, third pass (coordinator's surviving mutant M3).
    /// The mutant was
    ///
    /// ```text
    /// - verdicts.iter().any(SelectionVerdict::failed)
    /// + verdicts.iter().filter(|v| v.downgrade.is_none()).any(SelectionVerdict::failed)
    /// ```
    ///
    /// and it changed nothing any test observed. Sized honestly, it is NOT a
    /// fifth false-pass door: `Evidence`'s three variants are mutually
    /// exclusive, so a `Failed` selection is never `Verified`,
    /// `all(fully_verified)` still fails, and the result degrades to
    /// `Partial`. `Pass` stays unreachable and the chokepoint holds.
    ///
    /// What it exposed is the LANE ASYMMETRY round 3 exists to close,
    /// surviving in the one direction nobody probed: "a `Failed` selection
    /// produces `Fail` no matter which lane it is on" was never asserted for
    /// a DOWNGRADED selection. Under the mutant, a consume-only selection
    /// examined and found WRONG reports `Partial` — "we could not fully check
    /// this" — instead of `Fail` — "we checked, and it is wrong". Those say
    /// materially different things to an auditor, and reporting the second as
    /// the first understates a real, demonstrated failure.
    ///
    /// Both lanes of a downgraded selection are covered here, and they are
    /// NOT equally reachable — worth stating, because the difference is the
    /// reason this gap matters in production rather than only in theory:
    ///
    /// - The SEGMENT lane is genuinely reachable. `verdict_for_selection`
    ///   calls `segment_evidence` BEFORE it matches on the archive mode, so
    ///   it runs on every lane, and it can return `Failed` (a sha256 mismatch
    ///   against the manifest). A pre-0.21 / KBAK-level-1 archive whose
    ///   segment bytes do not match its own manifest is exactly that case,
    ///   and it is pinned end to end by `verify_phase.rs`'s
    ///   `a_consume_only_selection_with_a_corrupt_segment_fails_the_drill_never_merely_partial`.
    /// - The RECORD lane is currently unreachable: the `Unsupported` arm of
    ///   `verdict_for_selection` constructs only `Verified` and `Unverified`,
    ///   never `Failed`, because consume-only asserts nothing about record
    ///   contents and so cannot find them wrong. It is asserted anyway, as a
    ///   property of `roll_up` rather than of today's callers — `roll_up` is
    ///   the chokepoint, and a chokepoint that is only correct for the inputs
    ///   its present callers happen to produce is not a chokepoint.
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
        let integrity = roll_up(&[segment_failed]);
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
        assert_eq!(roll_up(&[record_failed]).result, IntegrityResult::Fail);

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
        let integrity = roll_up(&[
            mixed,
            verdict(
                "payments/0",
                Evidence::Verified { checked: 1 },
                Evidence::Verified { checked: 10 },
            ),
        ]);
        assert_eq!(integrity.result, IntegrityResult::Fail);
    }

    /// The signed sample counters key on `reconciled.is_some()` and on
    /// NOTHING else — the rule `roll_up`'s own comment and the module doc both
    /// state ("they aggregate over EXACTLY the selections whose records were
    /// reconciled against archive fingerprints").
    ///
    /// Found while auditing siblings of the coordinator's M3: slipping a
    /// `.filter(|v| v.downgrade.is_none())` in front of the counters' own
    /// `filter_map(|v| v.reconciled)` changed nothing any test observed. That
    /// mutant is equivalent TODAY, but only by a coupling to a different
    /// function: `verdict_for_selection`'s `Unsupported` arm happens to pass
    /// `reconciled = None`, so `downgrade.is_some()` currently implies
    /// `reconciled.is_none()`. It is not equivalent by any logic inside
    /// `roll_up`. Should a future lane ever downgrade a selection while still
    /// reconciling part of it, the mutated form would silently drop real
    /// reconciliations out of counters Task 20 SIGNS, under-reporting
    /// `records_sampled` while `partial_reason` still named the selection —
    /// a signed document disagreeing with itself.
    ///
    /// So the documented rule is pinned as stated, on the axis it is stated
    /// on, rather than left resting on a neighbour's current behaviour.
    #[test]
    fn the_signed_counters_key_on_reconciled_alone_never_on_the_downgrade_flag() {
        // Deliberately NOT a shape `verdict_for_selection` builds today: a
        // downgraded selection that nonetheless carries reconciliation
        // counts. `roll_up` is the chokepoint, and its contract must hold for
        // the inputs it DECLARES, not merely for the ones today's single
        // caller happens to produce.
        let mut downgraded_but_reconciled = verdict(
            "orders/0",
            Evidence::Verified { checked: 1 },
            Evidence::Verified { checked: 10 },
        );
        downgraded_but_reconciled.downgrade = Some("kbak level below the gate".into());
        downgraded_but_reconciled.reconciled = Some((7, 5));

        let integrity = roll_up(&[downgraded_but_reconciled]);
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
    /// hard as an unverified byte-fingerprint one. This is the fourth door
    /// stated as a property of the roll-up rather than of a lane.
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

        let integrity = roll_up(&[downgraded]);
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
    /// established (the fifth reproduction). `roll_up` names those selections
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

        let integrity = roll_up(&[
            downgraded,
            verdict(
                "payments/0",
                Evidence::Verified { checked: 1 },
                Evidence::Verified { checked: 10 },
            ),
        ]);
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

    /// The fourth door's exact signature: `pass_rate_measured = Some(1.0)`
    /// published beside a verdict that is NOT a pass, because the ratio was
    /// taken over only the selections that happened to reconcile. A rate is a
    /// ratio over the WHOLE sample or it is null.
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

        let integrity = roll_up(&[
            short,
            verdict(
                "payments/0",
                Evidence::Verified { checked: 1 },
                Evidence::Verified { checked: 10 },
            ),
        ]);
        assert_eq!(integrity.result, IntegrityResult::Partial);
        assert_eq!(
            integrity.pass_rate_measured, None,
            "Some(1.0) beside a partial verdict is the fourth door's exact false assurance"
        );
    }

    // -----------------------------------------------------------------
    // `segment_evidence`

    /// Round 2 skipped a pre-0.21 segment (empty sha256) with a logged note
    /// and let the run still report `Pass`. A skip is coverage the drill did
    /// not obtain and must be `Unverified`.
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

    // -----------------------------------------------------------------
    // `probe_archive_modes`

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

    /// Task 19 fix round 3: the mode is PER SELECTION. Round 2's
    /// `probe_archive_mode` returned one `ConsumeOnly` for the WHOLE backup
    /// set on the first `Unsupported`, discarding every fingerprint set it
    /// had already collected — the fifth reproduction, at its source. One
    /// `Unsupported` selection must downgrade only itself, the loop must
    /// continue, and the answer count must equal the selection count.
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

    // -----------------------------------------------------------------
    // `verdict_for_selection` (was `consume_and_reconcile`, which took the
    // whole `sel` slice and one whole-backup-set `ArchiveMode`).

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
                // Task 8 (guard G-TS) added this to `ClusterReader`. Phase 7
                // never reads it: the target-topic preflight is phase 0's.
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

    /// `claimed` is `min(sel.count, Σ record_count)` over the matched
    /// segments — the module doc's "short counts as unverified" rule rests
    /// entirely on this figure, and the doc's claim about WHICH figure it is
    /// was wrong once already (it said "byte-for-byte the same" as
    /// `phase4_sample`'s uncapped `sample.records_expected`). Both halves are
    /// exercised: the selection's own `count` binding when it is smaller, and
    /// the manifest sum binding when IT is smaller. A mutant dropping either
    /// half changes an observed number here.
    #[test]
    fn claimed_is_the_manifest_window_sum_capped_by_the_selections_own_count() {
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
                // Task 8 (guard G-TS) added this to `ClusterReader`. Phase 7
                // never reads it: the target-topic preflight is phase 0's.
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

        // `sel_for` builds `count: 10`; the one segment claims 10 records.
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
            "the manifest's own in-window record_count must cap the claim when it is smaller"
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
                // Task 8 (guard G-TS) added this to `ClusterReader`. Phase 7
                // never reads it: the target-topic preflight is phase 0's.
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
        let err = classify_parity_all(&facts, &Unreachable, &BTreeMap::new(), &plan).unwrap_err();
        assert!(matches!(err, DrillError::Operational(_)));
        assert!(err.to_string().contains("zero mapped topics"));
    }

    /// Task 19 fix round 1 (review finding F4, surviving mutant r16): a
    /// mapped topic with no matching entry in `facts.topics` must be
    /// REFUSED, never silently `continue`d past. Silently skipping it would
    /// let `TopicParity` report agreement over fewer topics than the
    /// operator actually asked about, with no signal that anything was
    /// dropped. The `Unreachable` reader panics if `classify_parity_all`
    /// ever reaches it — the refusal must fire on the facts lookup alone,
    /// before any target read.
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
                // Task 8 (guard G-TS) added this to `ClusterReader`. Phase 7
                // never reads it: the target-topic preflight is phase 0's.
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
        let err = classify_parity_all(&facts, &Unreachable, &mapping, &plan).unwrap_err();
        assert!(matches!(err, DrillError::Operational(_)));
        assert!(err.to_string().contains("no archive facts"));
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
                // Task 8 (guard G-TS) added this to `ClusterReader`. Phase 7
                // never reads it: the target-topic preflight is phase 0's.
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
                // Task 8 (guard G-TS) added this to `ClusterReader`. Phase 7
                // never reads it: the target-topic preflight is phase 0's.
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

    /// Task 19 fix round 1 (review finding F5, surviving mutant r14): the
    /// cross-partition aggregation itself — `n.max(r.timestamp_ms)` — was
    /// unpinned; every prior fixture gave each mapped topic a single
    /// partition, or gave every partition the SAME timestamp, so a mutant
    /// swapping `max` for `min` survived undetected. Two partitions with
    /// DELIBERATELY DIFFERENT last-record timestamps close that gap:
    /// `newest_ts` is the direct input to `measured.rpo_seconds` (module doc
    /// on `Restored`/phase 8), so reporting the OLDEST instead of the NEWEST
    /// restored record would corrupt a signed RPO with nothing red.
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
                // Task 8 (guard G-TS) added this to `ClusterReader`. Phase 7
                // never reads it: the target-topic preflight is phase 0's.
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
            default_replication_factor: 1,
            checkpoint_state: "/tmp/checkpoint.json".into(),
            checkpoint_interval_secs: 30,
        }
    }
}
