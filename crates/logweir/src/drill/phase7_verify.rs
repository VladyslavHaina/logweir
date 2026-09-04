//! Phase 7 — verify. Four checks, none of which may read "matched" over
//! nothing: (a) segment sha256 over the sampled archive segments, (b) the
//! engine's own `validation run` (evidence only — see below), (c) canary
//! consume-and-reconcile, per-record fingerprints (not watermark counts),
//! and (d) topic-config parity against what phase 6 deliberately altered.
//!
//! ## Reconciling PER SELECTION, never pooled (Task 19 fix round 1, review
//! finding F1 — CRITICAL)
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
//! The fix: `consume_and_reconcile` (below) calls `compare()` ONCE PER
//! SELECTION — never once over everything — and sums the per-selection
//! results. Each selection names exactly one (topic, partition), so its own
//! `compare()` call can never see another selection's records, making a
//! cross-partition or cross-topic collision structurally impossible rather
//! than merely untested. The cross-PARTITION half of this (two partitions
//! of the SAME topic, which pool just as readily as two different topics
//! since both start at offset 0) is separately pinned by
//! `verify_phase.rs`'s `run_reconciles_two_partitions_of_one_topic_independently_not_pooled`.
//!
//! ## Exit-code routing (contract for the future orchestrator)
//!
//! Mirrors `phase5_preflight`'s documented contract exactly: `run` below
//! returns `Ok(VerifyOutcome)` even when the drill did NOT pass — a failed
//! reconciliation is a DRILL RESULT, not an operational failure, and is
//! carried home inside `VerifyOutcome.integrity.{result,partial_reason}`,
//! never as an `Err`. The orchestrator (Task 21a, not implemented here) reads
//! `integrity.result`. `Pass` becomes `Outcome::Pass` (subject to the other
//! phases' verdicts). `Partial` is NOT a pass — either a byte-fingerprint
//! comparison that sampled zero records (this file's own reachable path, see
//! "Partial" below) or, in principle, a compacted topic that legitimately
//! holds fewer records than the archive (see the caveat below: v0.1 cannot
//! yet distinguish that case from real corruption, so it is NOT a separate
//! reachable path here) — but still lands at exit 2 with a signed scorecard.
//! `Fail` becomes `Outcome::FailIntegrity`, exit 2, signed scorecard.
//!
//! `run` returns `Err(DrillError::Operational(..))` ONLY when the check
//! genuinely could not be performed at all (an empty sample selection, a
//! store I/O failure, a broker unreachable) — never because a comparison
//! came back negative. If this file is ever found routing a negative
//! comparison result through `Err`, or an `Err` here through anything but
//! exit 1, that is the exact defect this comment exists to prevent.
//!
//! ## `IntegrityResult::Partial` — one reachable path, one declared gap
//! (Task 19 fix rounds 1-2, review findings F2/F3)
//!
//! A byte-fingerprint comparison in which ANY selection's own archive
//! fingerprint set is empty is reported as `Partial`, never `Pass` —
//! "compared nothing for this partition" must not read as success, even
//! when a SIBLING selection in the same drill produced real matches. Fix
//! round 1 checked only the AGGREGATE `sampled == 0` across every selection
//! summed together — which a single under-sampled selection could hide
//! inside a passing aggregate the moment any other selection contributed
//! real matches, the SAME critical false pass surviving through a narrower
//! door (fix round 2 re-demonstrated it: pooled with a healthy selection,
//! the round-1 code reported `Pass, byte-fingerprint, 25/25, pass_rate 1.0`
//! for a topic that sampled nothing). `consume_and_reconcile`'s `unverified`
//! output now names every such selection individually, and `run` treats a
//! non-empty `unverified` as disqualifying from `Pass` regardless of the
//! aggregate total (see `consume_and_reconcile`'s own doc comment). This is
//! strictly broader than the aggregate check, not a replacement running
//! alongside it: every selection that would have produced the aggregate
//! `sampled == 0` also appears in `unverified`.
//!
//! Reachable in production: `sample.records_per_partition: 0` used to reach
//! `SampleSelection.count` with nothing rejecting it (closed at the source
//! too — see `phase4_sample::run`'s own guard — but this file no longer
//! trusts that as the only line of defence); `segment_keys_for_set` finding
//! no key for a `(topic, partition, window)` the manifest facts claim
//! exists; or no decoded record's timestamp landing in the sampled window.
//! Pinned by `verify_phase.rs`'s
//! `a_byte_fingerprint_comparison_that_samples_zero_records_is_partial_never_pass`
//! (the aggregate case) and
//! `a_selection_that_sampled_zero_archive_fingerprints_cannot_hide_inside_a_passing_aggregate`
//! (the narrow case fix round 2 closed, with a named healthy sibling
//! selection pooled alongside it).
//!
//! The brief's OTHER named `Partial` scenario — a compacted topic that
//! legitimately holds fewer records than the archive (spec §9.3) — is
//! DELIBERATELY NOT given a reachable path in `run` by this fix round.
//! Distinguishing "compaction removed this record on purpose" from "the
//! restore or the archive lost it" would require cross-referencing a
//! specific mismatch against `topic_parity`'s `cleanup.policy=compact` flag,
//! which this phase does not attempt. A compacted topic today is honestly
//! reported via the SAME path a real mismatch takes (`Fail`, with the
//! specific offsets logged, `partial_reason: None`) — a known limitation,
//! not a defect, and out of this fix round's scope to resolve algorithmically.
//! `fixtures::verify_outcome_for_compacted_topic` (used by
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
//! First, `consume_and_reconcile` (via `mapped_topic`) calls
//! `reader.consume_range(mapping[&sel.topic], ...)`, never
//! `reader.consume_range(&sel.topic, ...)` — proven by `verify_phase.rs`'s
//! `consume_all_reads_the_mapped_target_topic_never_the_archive_name`.
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

/// Every archive segment `sel` actually samples, matched by (topic,
/// partition) and window overlap — the same window filter `phase4_sample`
/// itself uses. Free function, not `BackupSetFacts::sampled_segments` /
/// `facts.sampled_segments(sel)` as the brief's Step 4 literally writes it:
/// `BackupSetFacts` is defined in `logweir-core` (out of this task's declared
/// file scope — see `task-19-addendum.md` A2's corrected Files block), so an
/// inherent method cannot be added to it from here. A free function reading
/// `sampled_segments(facts, sel)` is functionally identical and changes
/// nothing this task is not already scoped to change; the same substitution
/// applies to `probe_archive_mode` below.
///
/// Errors — rather than silently returning an empty `Vec` — when `sel` is
/// non-empty but matches ZERO segments: an empty result here would make the
/// sha256 loop in `run` iterate nothing and report nothing, which reads
/// exactly like "every sampled segment verified", the false-pass shape this
/// whole phase exists to prevent. Pinned by
/// `sampled_segments_refuses_to_silently_check_nothing` (unit test, below).
///
/// Task 19 fix round 2 ("check for the third door"): the guard is PER
/// SELECTION, not only on the aggregate `out` — checked, and refused,
/// immediately after each selection's own contribution is gathered, before
/// moving to the next. An aggregate-only check (`out.is_empty()` after the
/// whole loop) would still pass when ONE selection among several matches
/// zero segments but another selection's segments keep `out` non-empty —
/// that selection's sha256 check would silently never run while a sibling
/// selection's did, with nothing in the result to say so. Exactly the
/// narrow-door shape review finding "FIX 2" found in the canary
/// reconciliation; closed here before it could be found the same way.
/// Pinned by `sampled_segments_refuses_when_one_of_several_selections_matches_nothing`.
fn sampled_segments<'a>(
    facts: &'a BackupSetFacts,
    sel: &[SampleSelection],
) -> Result<Vec<&'a SegmentFacts>, DrillError> {
    let mut out = Vec::new();
    for s in sel {
        let (w0, w1) = s.window;
        let mut found_for_this_selection = 0usize;
        for t in facts.topics.iter().filter(|t| t.name == s.topic) {
            for p in t
                .partitions
                .iter()
                .filter(|p| p.partition_id == s.partition)
            {
                let matches: Vec<&SegmentFacts> = p
                    .segments
                    .iter()
                    .filter(|seg| seg.start_timestamp <= w1 && seg.end_timestamp >= w0)
                    .collect();
                found_for_this_selection += matches.len();
                out.extend(matches);
            }
        }
        if found_for_this_selection == 0 {
            return Err(DrillError::Operational(format!(
                "sampled_segments matched zero archive segments for {}/{} in the sampled \
                 window; the segment sha256 check would silently skip this partition while \
                 checking others",
                s.topic, s.partition
            )));
        }
    }
    Ok(out)
}

/// The archive side's support level, determined ONCE across every selection
/// — never re-probed per partition — because an unsupported KBAK level is a
/// fact about the whole backup set's FORMAT (`EngineError::Unsupported`'s own
/// doc comment), not about one partition. The FIRST `Unsupported` found wins
/// for every selection; there is no value in checking the rest once the
/// first says so.
///
/// Carries one archive fingerprint `Vec` PER SELECTION, in lock-step with
/// `sel`'s own order (`ByteFingerprint(v)` has `v.len() == sel.len()`) — this
/// is what lets `consume_and_reconcile` reconcile selection `i`'s archive
/// side against selection `i`'s own consumed records only, never pooling two
/// selections' data into one lookup (module doc, "Reconciling PER SELECTION").
#[derive(Debug)]
enum ArchiveMode {
    ByteFingerprint(Vec<Vec<RecordFingerprint>>),
    ConsumeOnly(String),
}

/// Determines `ArchiveMode` for the whole selection list. A free function,
/// not a method on `dyn DataEngine` — see `sampled_segments`'s doc comment
/// for why a method call became a function call: `DataEngine` is defined in
/// `logweir-core`, out of this task's file scope.
///
/// Refuses an empty `sel` outright — the same false-pass shape
/// `sampled_segments` guards against.
fn probe_archive_mode(
    engine: &dyn DataEngine,
    sel: &[SampleSelection],
) -> Result<ArchiveMode, EngineError> {
    if sel.is_empty() {
        return Err(EngineError::Operational(
            "probe_archive_mode called with zero sample selections; refusing to compare \
             fingerprints over an empty set"
                .into(),
        ));
    }
    let mut per_selection = Vec::with_capacity(sel.len());
    for s in sel {
        match engine.fingerprints(s) {
            Ok(fp) => per_selection.push(fp),
            Err(EngineError::Unsupported(reason)) => return Ok(ArchiveMode::ConsumeOnly(reason)),
            Err(e) => return Err(e),
        }
    }
    Ok(ArchiveMode::ByteFingerprint(per_selection))
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

/// Consumes every selection's records from the TARGET cluster — applying the
/// topic-rename mapping before ever calling `reader` — and, when `mode` is
/// `ByteFingerprint`, reconciles that SAME selection's own archive
/// fingerprints against ONLY that selection's own consumed records, summing
/// the per-selection results. This is the Task 19 fix round 1 fix for review
/// finding F1: see the module doc comment's "Reconciling PER SELECTION"
/// section for why calling `compare()` once per selection — never once over
/// everything pooled — is what makes a cross-partition/cross-topic offset
/// collision structurally impossible rather than merely untested.
///
/// `count` is the same per-partition cap `phase4_sample` applied to the
/// archive side, so a healthy restore's target read is bounded the same way
/// the archive sample was; `from` is 0 because a drill's destination topic is
/// always a freshly created scratch topic (see `compare`'s own doc comment).
///
/// A selection whose topic has NO entry in `mapping` is refused
/// (`DrillError::Operational`), never silently skipped — see `mapped_topic`.
///
/// Returns `(records_restored, sampled, matching, mismatched, unverified)`.
/// `unverified` names every selection (as `"topic/partition"`) whose OWN
/// archive fingerprint set was empty while attempting `ByteFingerprint`
/// reconciliation — Task 19 fix round 2 (review's FIX 2, the same critical
/// finding surviving through a narrower door): round 1 only refused an
/// AGGREGATE `sampled == 0` after summing every selection, which a single
/// under-sampled selection can hide inside a passing aggregate the moment
/// ANY other selection in the same `sel` contributes real matches — exactly
/// the false pass the review re-demonstrated. `run` treats a non-empty
/// `unverified` as disqualifying a `Pass` regardless of how healthy the
/// other selections were, because "compared nothing for this partition" can
/// never be outweighed by a sibling partition's real data.
fn consume_and_reconcile(
    reader: &dyn ClusterReader,
    sel: &[SampleSelection],
    mapping: &BTreeMap<String, String>,
    mode: &ArchiveMode,
) -> Result<(u64, u64, u64, u64, Vec<String>), DrillError> {
    // Cheaper than the panic `archives[i]` would otherwise produce if this
    // ever desynced from `sel`: today that is structurally impossible (a
    // private function, one call site in `run`, and `ArchiveMode::ByteFingerprint`
    // is only ever built by `probe_archive_mode` pushing exactly one entry
    // per selection in `sel`'s own order) — so this is a documented
    // invariant check, not a defense against a reachable bug, and costs
    // nothing in a release build.
    if let ArchiveMode::ByteFingerprint(archives) = mode {
        debug_assert_eq!(
            archives.len(),
            sel.len(),
            "ArchiveMode::ByteFingerprint must carry exactly one archive fingerprint set per \
             selection, in sel's own order"
        );
    }
    let mut records_restored = 0u64;
    let mut sampled = 0u64;
    let mut matching = 0u64;
    let mut unverified = Vec::new();
    for (i, s) in sel.iter().enumerate() {
        let mapped = mapped_topic(mapping, &s.topic)?;
        let consumed = reader.consume_range(mapped, s.partition, 0, s.count)?;
        records_restored += consumed.len() as u64;
        if let ArchiveMode::ByteFingerprint(archives) = mode {
            let (this_sampled, this_matching, why) = compare(&archives[i], &consumed);
            if this_sampled == 0 {
                unverified.push(format!("{}/{}", s.topic, s.partition));
            }
            sampled += this_sampled;
            matching += this_matching;
            for w in why {
                tracing::error!(target: "logweir::verify", detail = %w, "reconciliation mismatch");
            }
        }
    }
    Ok((
        records_restored,
        sampled,
        matching,
        sampled.saturating_sub(matching),
        unverified,
    ))
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
/// `consume_and_reconcile` already consumed. The real reason for the
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
    // never a positive result. Checked FIRST, before any comparison below
    // runs over what would otherwise be an empty set.
    if sel.is_empty() {
        return Err(DrillError::Operational(
            "phase7_verify::run called with zero sample selections; a fingerprint comparison \
             over an empty set would report a pass that means nothing"
                .into(),
        ));
    }

    // No `Integrity::default()`: Task 3 derives only Debug/Clone/Serialize/Deserialize.
    let mut integrity = Integrity {
        level: IntegrityLevel::ByteFingerprint,
        result: IntegrityResult::Pass,
        partial_reason: None,
        records_sampled: 0,
        records_sampled_matching: 0,
        mismatches: 0,
        pass_rate_measured: None,
        restored_principal_could_consume: None,
    };

    // (a) Segment sha256 over the SAMPLED segments only. Segments written before
    // 0.21 carry an empty sha256 [VERIFIED manifest.rs:376-380]; they are
    // SKIPPED with a logged note, never counted as a pass. `Integrity` has no
    // `notes` field and format_version is frozen at 1.0.0 (Global Constraint 12),
    // so these diagnostics go to the log, not to the scorecard.
    for seg in sampled_segments(facts, sel)? {
        if seg.sha256.is_empty() {
            tracing::warn!(target: "logweir::verify", key = %seg.key,
                           "no sha256 (segment written before 0.21); skipped");
            continue;
        }
        // `Store::get` returns `StoreError`, not directly convertible to
        // `DrillError` — routed through the existing `StoreError -> EngineError`
        // conversion (`logweir-engine-oso/src/storage.rs`) so `?` can then use
        // `DrillError`'s existing `#[from] EngineError`, rather than adding a
        // second `From` impl for a type this crate does not own.
        let (bytes, _vid) = store.get(&seg.key).map_err(EngineError::from)?;
        if logweir_core::ids::sha256_prefixed(&bytes) != seg.sha256 {
            integrity.result = IntegrityResult::Fail;
            tracing::error!(target: "logweir::verify", key = %seg.key,
                            "sha256 mismatch against the manifest");
        }
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

    // (c) Canary consume and reconcile — per selection, never pooled. See
    // the module doc comment's "Reconciling PER SELECTION" section (Task 19
    // fix round 1, review finding F1, CRITICAL): `probe_archive_mode`
    // determines support ONCE across every selection; `consume_and_reconcile`
    // then reconciles EACH selection's archive fingerprints against ONLY
    // that same selection's own consumed records, summing across selections,
    // so a collision between two selections' offsets is structurally
    // impossible rather than merely untested.
    let mode = probe_archive_mode(engine, sel)?;
    match &mode {
        ArchiveMode::ByteFingerprint(_) => integrity.level = IntegrityLevel::ByteFingerprint,
        // The archive side cannot be fingerprinted (KBAK level below the
        // gate). The drill STILL consumes (inside `consume_and_reconcile`
        // below), so the restore is proved to have produced readable
        // records — but NOTHING was sampled, so all three sample counters
        // stay 0 and pass_rate_measured stays null.
        ArchiveMode::ConsumeOnly(reason) => {
            integrity.level = IntegrityLevel::ConsumeOnly;
            integrity.partial_reason = Some(reason.clone());
        }
    }
    let (records_restored, sampled, matching, mismatched, unverified) =
        consume_and_reconcile(reader, sel, mapping, &mode)?;
    integrity.records_sampled = sampled;
    integrity.records_sampled_matching = matching;
    integrity.mismatches = mismatched;
    // Consume-only makes NO byte claim, and a zero denominator is `None` rather
    // than a NaN — `to_deterministic_json` refuses non-finite floats (Task 3
    // step 5) precisely so a NaN can never be signed as `null`. This also keeps
    // Task 3's invariant "pass_rate_measured set implies level == byte-fingerprint".
    integrity.pass_rate_measured = match integrity.level {
        IntegrityLevel::ByteFingerprint if sampled > 0 => Some(matching as f64 / sampled as f64),
        _ => None,
    };
    if integrity.result != IntegrityResult::Fail && mismatched > 0 {
        integrity.result = IntegrityResult::Fail;
    }
    // Task 19 fix round 2 (review findings F2/F3, and the SAME critical
    // finding surviving fix round 1 through a narrower door): a
    // byte-fingerprint comparison in which ANY selection's own archive
    // fingerprint set was empty must never read as `Pass` — "compared
    // nothing for this partition" is not success, no matter how healthy a
    // SIBLING selection in the same drill was. Round 1's guard checked only
    // the AGGREGATE `sampled == 0`, which a single under-sampled selection
    // could hide inside: pooled with a healthy selection, the aggregate
    // total stayed positive and the whole drill still reported `Pass`. This
    // check is strictly broader — every selection that produced the
    // aggregate `sampled == 0` also appears in `unverified`, so it subsumes
    // round 1's guard rather than sitting alongside it. See the module doc
    // comment's "`IntegrityResult::Partial`" section for why `Partial`, not
    // `Fail`, is the right bucket: this means "could not reconcile
    // record-for-record", not "reconciled and found corruption". Never
    // downgrades an already-established `Fail` from the sha256 check above.
    if integrity.level == IntegrityLevel::ByteFingerprint
        && !unverified.is_empty()
        && integrity.result != IntegrityResult::Fail
    {
        integrity.result = IntegrityResult::Partial;
        integrity.partial_reason = Some(format!(
            "zero archive fingerprints were available to reconcile against for: {} — a \
             byte-fingerprint comparison cannot verify anything it never sampled, even when \
             other selections in the same drill produced real matches",
            unverified.join(", ")
        ));
    }

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

/// POSTs one JSON summary per configured sink. EVERY transport failure is
/// logged and swallowed: the drill result is a measurement, and a webhook being
/// down must never change it. `ureq` is blocking on purpose — it adds no async
/// runtime to `crates/logweir`.
pub fn notify(n: &Notifications, sc: &Scorecard) {
    let body = serde_json::json!({
        "run_id": sc.run_id,
        "outcome": sc.outcome,
        "rto_excluding_preflight_seconds": sc.measured.rto_excluding_preflight_seconds,
        "rpo_seconds": sc.measured.rpo_seconds,
        "integrity": { "level": sc.integrity.level, "result": sc.integrity.result },
        "self_attested": sc.approval.self_attested,
    });
    let mut sinks: Vec<String> = n.webhooks.clone();
    if let Some(u) = &n.slack_webhook {
        sinks.push(u.clone());
    }
    for url in sinks {
        match ureq::post(&url).send_json(&body) {
            Ok(_) => tracing::info!(target: "logweir::notify", url = %url, "notified"),
            Err(e) => tracing::warn!(target: "logweir::notify", url = %url,
                                     error = %e, "notification failed; continuing"),
        }
    }
    if let Some(key) = &n.pagerduty_routing_key {
        let ev = serde_json::json!({
            "routing_key": key,
            "event_action": if sc.outcome == logweir_core::outcome::Outcome::Pass
                            { "resolve" } else { "trigger" },
            "dedup_key": format!("logweir-drill-{}", sc.target.cluster_id),
            "payload": { "summary": format!("logweir drill {}: {:?}", sc.run_id, sc.outcome),
                         "source": sc.target.cluster_id, "severity": "warning",
                         "custom_details": body },
        });
        if let Err(e) = ureq::post("https://events.pagerduty.com/v2/enqueue").send_json(&ev) {
            tracing::warn!(target: "logweir::notify", error = %e, "pagerduty enqueue failed");
        }
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
                        key: "k".into(),
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
            anchor: "head".into(),
            count: 10,
            window,
        }
    }

    #[test]
    fn sampled_segments_refuses_to_silently_check_nothing() {
        let facts = facts_one_segment();
        // A selection naming a partition the facts do not have: zero segments
        // can possibly match. Must error, not return an empty Vec that would
        // make the caller's sha256 loop iterate nothing and report nothing.
        let sel = vec![sel_for("orders", 7, (0, 100))];
        let err = sampled_segments(&facts, &sel).unwrap_err();
        assert!(matches!(err, DrillError::Operational(_)));
        assert!(err.to_string().contains("zero archive segments"));
    }

    #[test]
    fn sampled_segments_finds_the_real_match() {
        let facts = facts_one_segment();
        let sel = vec![sel_for("orders", 0, (0, 100))];
        let segs = sampled_segments(&facts, &sel).unwrap();
        assert_eq!(segs.len(), 1);
        assert_eq!(segs[0].key, "k");
    }

    /// The right topic and partition, but a window that does not overlap the
    /// segment's own `start_timestamp..end_timestamp` (0..100): must be
    /// excluded, not matched regardless of window. Pins the window filter
    /// itself, distinct from `..._refuses_to_silently_check_nothing` (which
    /// pins the empty-result guard via a partition mismatch, not a window
    /// mismatch) and from `..._finds_the_real_match` (which never exercises a
    /// non-overlapping window, so a mutant deleting the window filter
    /// entirely would still pass it).
    #[test]
    fn sampled_segments_excludes_a_segment_outside_the_requested_window() {
        let facts = facts_one_segment();
        let sel = vec![sel_for("orders", 0, (200, 300))];
        let err = sampled_segments(&facts, &sel).unwrap_err();
        assert!(matches!(err, DrillError::Operational(_)));
        assert!(err.to_string().contains("zero archive segments"));
    }

    /// Task 19 fix round 2 ("check for the third door"): TWO selections, one
    /// that matches a real segment and one that matches nothing. The
    /// aggregate `out.is_empty()` check this function used to have would NOT
    /// fire here — `out` ends up non-empty because of the healthy selection
    /// — silently leaving the second selection's segment sha256 unchecked.
    /// The per-selection guard must refuse regardless of what any other
    /// selection contributed.
    #[test]
    fn sampled_segments_refuses_when_one_of_several_selections_matches_nothing() {
        let facts = facts_one_segment();
        let sel = vec![
            sel_for("orders", 0, (0, 100)), // matches the one real segment
            sel_for("orders", 7, (0, 100)), // partition 7 does not exist
        ];
        let err = sampled_segments(&facts, &sel).unwrap_err();
        assert!(matches!(err, DrillError::Operational(_)));
        assert!(err.to_string().contains("zero archive segments"));
        assert!(err.to_string().contains("orders/7"));
    }

    #[test]
    fn probe_archive_mode_refuses_an_empty_selection_list() {
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
                    "probe_archive_mode must refuse an empty selection before calling the engine"
                )
            }
        }
        let err = probe_archive_mode(&NeverCalled, &[]).unwrap_err();
        assert!(matches!(err, EngineError::Operational(_)));
    }

    #[test]
    fn consume_and_reconcile_refuses_a_topic_with_no_mapping_entry() {
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
                    "consume_and_reconcile must refuse an unmapped topic before calling the reader"
                )
            }
        }
        let sel = vec![sel_for("orders", 0, (0, 100))];
        // The mode is never consulted: `mapped_topic` must refuse BEFORE
        // `consume_and_reconcile` ever looks at `mode`.
        let mode = ArchiveMode::ConsumeOnly("n/a".into());
        let err = consume_and_reconcile(&Unreachable, &sel, &BTreeMap::new(), &mode).unwrap_err();
        assert!(matches!(err, DrillError::Operational(_)));
        assert!(err.to_string().contains("no target-side mapping"));
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
            topic_mapping: BTreeMap::new(),
            time_window: (chrono::Utc::now(), chrono::Utc::now()),
            default_replication_factor: 1,
            checkpoint_state: "/tmp/checkpoint.json".into(),
            checkpoint_interval_secs: 30,
        }
    }
}
