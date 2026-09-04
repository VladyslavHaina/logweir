//! Phase 7 — verify. Four checks, none of which may read "matched" over
//! nothing: (a) segment sha256 over the sampled archive segments, (b) the
//! engine's own `validation run` (evidence only — see below), (c) canary
//! consume-and-reconcile, per-record fingerprints (not watermark counts),
//! and (d) topic-config parity against what phase 6 deliberately altered.
//!
//! ## Exit-code routing (contract for the future orchestrator)
//!
//! Mirrors `phase5_preflight`'s documented contract exactly: `run` below
//! returns `Ok(VerifyOutcome)` even when the drill did NOT pass — a failed
//! reconciliation is a DRILL RESULT, not an operational failure, and is
//! carried home inside `VerifyOutcome.integrity.{result,partial_reason}`,
//! never as an `Err`. The orchestrator (Task 21a, not implemented here) reads
//! `integrity.result`. `Pass` becomes `Outcome::Pass` (subject to the other
//! phases' verdicts). `Partial` is NOT a pass — a compacted topic
//! legitimately holds fewer records than the archive — but still lands at
//! exit 2 with a signed scorecard: the reconciliation ran and reported
//! honestly. `Fail` becomes `Outcome::FailIntegrity`, exit 2, signed
//! scorecard.
//!
//! `run` returns `Err(DrillError::Operational(..))` ONLY when the check
//! genuinely could not be performed at all (an empty sample selection, a
//! store I/O failure, a broker unreachable) — never because a comparison
//! came back negative. If this file is ever found routing a negative
//! comparison result through `Err`, or an `Err` here through anything but
//! exit 1, that is the exact defect this comment exists to prevent.
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
//! First, `consume_all` calls `reader.consume_range(mapping[&sel.topic],
//! ...)`, never `reader.consume_range(&sel.topic, ...)` — proven by
//! `verify_phase.rs`'s
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
/// applies to `fingerprints_for` below.
///
/// Errors — rather than silently returning an empty `Vec` — when `sel` is
/// non-empty but matches ZERO segments: an empty result here would make the
/// sha256 loop in `run` iterate nothing and report nothing, which reads
/// exactly like "every sampled segment verified", the false-pass shape this
/// whole phase exists to prevent. Pinned by
/// `sampled_segments_refuses_to_silently_check_nothing` (unit test, below).
fn sampled_segments<'a>(
    facts: &'a BackupSetFacts,
    sel: &[SampleSelection],
) -> Result<Vec<&'a SegmentFacts>, DrillError> {
    let mut out = Vec::new();
    for s in sel {
        let (w0, w1) = s.window;
        for t in facts.topics.iter().filter(|t| t.name == s.topic) {
            for p in t
                .partitions
                .iter()
                .filter(|p| p.partition_id == s.partition)
            {
                out.extend(
                    p.segments
                        .iter()
                        .filter(|seg| seg.start_timestamp <= w1 && seg.end_timestamp >= w0),
                );
            }
        }
    }
    if out.is_empty() {
        return Err(DrillError::Operational(format!(
            "sampled_segments matched zero archive segments for {} sample selection(s); the \
             segment sha256 check would silently verify nothing",
            sel.len()
        )));
    }
    Ok(out)
}

/// Aggregates `DataEngine::fingerprints` across every selection into one flat
/// archive-side set. A free function, not `engine.fingerprints_for(sel)` — see
/// `sampled_segments`'s doc comment for why a method call became a function
/// call: `DataEngine` is defined in `logweir-core`, out of this task's file
/// scope, and `fingerprints_for` (plural selections) is not itself in that
/// trait (only the existing per-selection `fingerprints` is).
///
/// Refuses an empty `sel` outright — the same false-pass shape
/// `sampled_segments` guards against — and short-circuits on the FIRST
/// `EngineError::Unsupported`: an unsupported KBAK level is a fact about the
/// whole backup set's format, not about one partition, so there is no value
/// in reading every remaining selection once the first says so.
fn fingerprints_for(
    engine: &dyn DataEngine,
    sel: &[SampleSelection],
) -> Result<Vec<RecordFingerprint>, EngineError> {
    if sel.is_empty() {
        return Err(EngineError::Operational(
            "fingerprints_for called with zero sample selections; refusing to compare \
             fingerprints over an empty set"
                .into(),
        ));
    }
    let mut out = Vec::new();
    for s in sel {
        out.extend(engine.fingerprints(s)?);
    }
    Ok(out)
}

/// Consumes every selection's records from the TARGET cluster — applying the
/// topic-rename mapping (module doc, point 1) before ever calling `reader`.
/// `count` is the same per-partition cap `phase4_sample` applied to the
/// archive side, so a healthy restore's target read is bounded the same way
/// the archive sample was; `from` is 0 because a drill's destination topic is
/// always a freshly created scratch topic (see `compare`'s own doc comment).
///
/// A selection whose topic has NO entry in `mapping` is refused
/// (`DrillError::Operational`), never silently skipped — see the module doc
/// comment.
fn consume_all(
    reader: &dyn ClusterReader,
    sel: &[SampleSelection],
    mapping: &BTreeMap<String, String>,
) -> Result<Vec<ConsumedRecord>, DrillError> {
    let mut out = Vec::new();
    for s in sel {
        let Some(mapped) = mapping.get(&s.topic) else {
            return Err(DrillError::Operational(format!(
                "no target-side mapping for archive topic `{}`; refusing to read the wrong topic",
                s.topic
            )));
        };
        out.extend(reader.consume_range(mapped, s.partition, 0, s.count)?);
    }
    Ok(out)
}

/// Folds `classify_parity` over every entry in `mapping`, applying the
/// topic-rename mapping (module doc, point 2) before ever calling `reader`.
/// Target partition count comes from `reader.end_offsets` (one entry per
/// partition); target replication factor comes from `plan.default_replication_factor`
/// — the exact value phase 6 rendered when it created the topic (see phase
/// 6's own `Restored` doc comment and this file's `classify_parity` test
/// `scratch_deviations_are_intentional_and_anything_else_is_not`). When a
/// backup predates original-partition-count/replication-factor capture
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
/// each partition (`end_offsets` then `consume_range` at `hi - 1`, count 1)
/// rather than re-consuming everything `consume_all` already read: this is a
/// separate, cheap pass because `run`'s two canary-branch match arms bind
/// `consumed` to a local that does not survive the match.
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

    // (c) Canary consume and reconcile.
    let (records_restored, sampled, matching, mismatched) = match fingerprints_for(engine, sel) {
        Ok(archive) => {
            integrity.level = IntegrityLevel::ByteFingerprint;
            let consumed = consume_all(reader, sel, mapping)?;
            let restored = consumed.len() as u64;
            let (sampled, matching, why) = compare(&archive, &consumed);
            for w in why {
                tracing::error!(target: "logweir::verify", detail = %w, "reconciliation mismatch");
            }
            (
                restored,
                sampled,
                matching,
                sampled.saturating_sub(matching),
            )
        }
        // The archive side cannot be fingerprinted (KBAK level below the gate).
        // The drill STILL consumes, so the restore is proved to have produced
        // readable records — but NOTHING was sampled, so all three sample
        // counters stay 0 and pass_rate_measured stays null.
        Err(EngineError::Unsupported(reason)) => {
            integrity.level = IntegrityLevel::ConsumeOnly;
            integrity.partial_reason = Some(reason);
            let consumed = consume_all(reader, sel, mapping)?;
            (consumed.len() as u64, 0, 0, 0)
        }
        Err(e) => return Err(e.into()),
    };
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

    #[test]
    fn fingerprints_for_refuses_an_empty_selection_list() {
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
                panic!("fingerprints_for must refuse an empty selection before calling the engine")
            }
        }
        let err = fingerprints_for(&NeverCalled, &[]).unwrap_err();
        assert!(matches!(err, EngineError::Operational(_)));
    }

    #[test]
    fn consume_all_refuses_a_topic_with_no_mapping_entry() {
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
                panic!("consume_all must refuse an unmapped topic before calling the reader")
            }
        }
        let sel = vec![sel_for("orders", 0, (0, 100))];
        let err = consume_all(&Unreachable, &sel, &BTreeMap::new()).unwrap_err();
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
