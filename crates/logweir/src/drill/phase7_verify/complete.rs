//! **PROD-08.1 — complete verification.** Phase 7's second lane, chosen by
//! the plan's `sample.coverage: complete`. The contract it implements is
//! `docs/to-do/decisions/PROD-08.1-integrity-contract.md`; this comment says
//! how.
//!
//! # What it checks, per partition of every restored topic
//!
//! 1. **Archive integrity, every segment.** Every segment the manifest lists
//!    for the partition — not the segments a sample window overlaps, and not
//!    the first `max_partitions` partitions — is read back from the store,
//!    its sha256 compared with the manifest, its records decoded with
//!    Logweir's own `kbak` decoder, and its decoded count and offsets
//!    compared with the manifest's.
//! 2. **The expected output, from each record's OWN timestamp.** An archived
//!    record is expected when its timestamp is at or before the window's end
//!    (inclusive, as the engine's restore filter is) and, when the plan's
//!    window has a start of its own, at or after it. NOT from the segments'
//!    first and last timestamps, which the engine selects by (PROD-01.1 S6,
//!    S7): with out-of-order timestamps those misstate what a segment holds,
//!    so a selection that reads them is not independent of the engine.
//! 3. **The replay comparison, every restored record.** The target partition
//!    is read from offset 0 to its high watermark, in chunks, and each record
//!    is mapped back to its source offset by its LAST `x-original-offset`
//!    header (the one the backup appends after the record's own headers,
//!    PROD-01.1 S5). A repeated source offset is a duplicate; a source offset
//!    below one already seen is out of order (archive order is source-offset
//!    order, and the engine produces each partition in archive order); a
//!    source offset the expected output does not hold — or no lineage header
//!    at all — is unexpected; an expected record never seen is missing. The
//!    first copy of each expected record is compared byte for byte — key,
//!    value, timestamp, and headers IN ORDER
//!    (`logweir_kafka::fingerprint::record_digest_ordered`).
//!
//! The count check is EXACT: a partition passes only when the restored count
//! equals the expected count and every record is accounted for. The
//! manifest's first/last-timestamp count BOUND (`check_restored_count`) is
//! not consulted in this lane; it is what makes a correct point-in-time
//! restore fail (PROD-01.1 ts-bound) and a skipped segment pass (ts-pit).
//!
//! # One ledger, still
//!
//! Each partition becomes exactly one `SelectionVerdict`, and `roll_up` — the
//! one place `IntegrityResult` is decided — reads them as it reads the sampled
//! lane's. Nothing here decides pass or fail. What this lane adds is the
//! structured block (`Verification::complete`) the verdict is signed beside.
//!
//! # What makes a partition NOT compared (never a pass, never silent)
//!
//! - **The bound** (`sample.complete_max_records`): the partition's segments
//!   would take the decoded total past it. Its segments are `unverified`, its
//!   records lane `Unverified`, `complete.covered` is false and its
//!   `incomplete_reason` names the bound.
//! - **An expected output that cannot be established**: a segment with no
//!   sha256 (written before 0.21), one the decoder does not read, one that
//!   failed its check, a source offset archived twice, or archived records
//!   that carry no `x-original-offset` (an archive written with
//!   `include_offset_headers: false`), which no restored record could be
//!   mapped back from.
//!
//! # Memory
//!
//! One partition at a time: a 32-byte digest per expected record and an
//! 8-byte offset per restored record, never the records themselves. Bounded
//! memory regardless of partition size is PROD-08.3's.
use super::{Evidence, SelectionVerdict};
use crate::drill::DrillError;
use logweir_core::engine::{BackupSetFacts, PartitionFacts, RestorePlan, WindowFloorSource};
use logweir_core::scorecard::{
    ArchiveIntegrity, CompleteVerification, CompleteWindow, OffsetRange, PartitionVerification,
    ReplayComparison, PARTITION_FINDINGS_CAP,
};
use logweir_engine_oso::storage::{Store, StoreError};
use logweir_kafka::fingerprint::record_digest_ordered;
use logweir_kafka::reader::ClusterReader;
use std::collections::{BTreeMap, BTreeSet, HashSet};

/// How many restored records one `consume_range` call reads. The reader's
/// own deadline is per call, so a whole large partition is never one read.
pub const TARGET_READ_CHUNK: usize = 5_000;

/// The header the backup appends to every archived record: the record's
/// source offset, 8 bytes little-endian (`phase7_verify::decode_original_offset`).
pub const LINEAGE_HEADER: &str = "x-original-offset";

/// The source offset a record's LAST `x-original-offset` header names, or
/// `None` when it carries none (or the last one is not 8 bytes).
///
/// The LAST, not the first: the backup appends its own header AFTER the
/// record's own headers (PROD-01.1 S5), so a record that already carried
/// `x-original-offset` — a topic that was itself restored — is archived with
/// two, and only the last is this archive's lineage. With the pinned engine a
/// restored record carries one (its produce side collapses repeated keys,
/// PROD-01.1 R4), so first and last agree today; PROD-00.3e keeps both.
#[must_use]
pub fn lineage_offset(headers: &[(String, Option<Vec<u8>>)]) -> Option<i64> {
    headers
        .iter()
        .rev()
        .find(|(k, _)| k == LINEAGE_HEADER)
        .and_then(|(_, v)| v.as_deref())
        .and_then(super::decode_original_offset)
}

/// The window the expected output is selected by, from the PLAN: no lower
/// bound when the plan's floor is the archive's (guard G-WIN,
/// `ArchiveManifest`); the plan's own start when it inherited one
/// (`InheritedFromSpec`, PROD-11.1's sub-window).
#[must_use]
pub fn window_of(plan: &RestorePlan) -> CompleteWindow {
    CompleteWindow {
        start_ms: match plan.window_floor_source {
            WindowFloorSource::ArchiveManifest => None,
            WindowFloorSource::InheritedFromSpec => Some(plan.time_window.0.timestamp_millis()),
        },
        end_ms: plan.time_window.1.timestamp_millis(),
    }
}

fn selected(window: &CompleteWindow, ts: i64) -> bool {
    ts <= window.end_ms && window.start_ms.is_none_or(|s| ts >= s)
}

/// A partition's findings in words, capped: the counts are complete, the
/// words illustrate.
#[derive(Default)]
struct Findings {
    lines: Vec<String>,
    left_out: u64,
}

impl Findings {
    fn push(&mut self, line: String) {
        if self.lines.len() < PARTITION_FINDINGS_CAP {
            self.lines.push(line);
        } else {
            self.left_out += 1;
        }
    }
    fn into_vec(mut self) -> Vec<String> {
        if self.left_out > 0 {
            self.lines
                .push(format!("… and {} more findings not listed", self.left_out));
        }
        self.lines
    }
}

/// What the archive side of one partition established.
struct ArchiveSide {
    segments: u64,
    verified: u64,
    failed: Vec<String>,
    unverified: Vec<String>,
    decoded: u64,
    offset_holes: u64,
    /// source offset -> ordered digest, for every SELECTED record.
    expected: BTreeMap<i64, [u8; 32]>,
    /// Why the expected output is not known, when it is not.
    unknown: Option<String>,
    /// The decoder's own reason, when a segment's format is one it does not
    /// read (a legacy segment): the selection's downgrade.
    downgrade: Option<String>,
}

/// How many source offsets in `[lo, hi]` the ranges cover.
fn covered_by(ranges: &[(i64, i64)], lo: i64, hi: i64) -> u64 {
    // Merge first, so overlapping gap and pruned ranges count once.
    let mut clipped: Vec<(i64, i64)> = ranges
        .iter()
        .map(|&(a, b)| (a.max(lo), b.min(hi)))
        .filter(|(a, b)| a <= b)
        .collect();
    clipped.sort_unstable();
    let mut total = 0u64;
    let mut cur: Option<(i64, i64)> = None;
    for (a, b) in clipped {
        cur = match cur {
            Some((ca, cb)) if a <= cb.saturating_add(1) => Some((ca, cb.max(b))),
            Some((ca, cb)) => {
                total += (cb - ca + 1) as u64;
                Some((a, b))
            }
            None => Some((a, b)),
        };
    }
    if let Some((ca, cb)) = cur {
        total += (cb - ca + 1) as u64;
    }
    total
}

/// Reads, hashes and decodes every segment of one partition, and builds the
/// expected output. Store I/O other than "not found" is `Operational`: that
/// is Logweir failing to perform the check, not a fact about the archive.
fn archive_side(
    store: &Store,
    p: Option<&PartitionFacts>,
    window: &CompleteWindow,
    findings: &mut Findings,
) -> Result<ArchiveSide, DrillError> {
    let mut side = ArchiveSide {
        segments: 0,
        verified: 0,
        failed: Vec::new(),
        unverified: Vec::new(),
        decoded: 0,
        offset_holes: 0,
        expected: BTreeMap::new(),
        unknown: None,
        downgrade: None,
    };
    let Some(p) = p else {
        return Ok(side);
    };
    let explained: Vec<(i64, i64)> = p.gaps.iter().chain(p.pruned.iter()).copied().collect();
    let mut segs: Vec<_> = p.segments.iter().collect();
    // Archive order is source-offset order; nothing orders a real manifest.
    segs.sort_by_key(|s| (s.start_offset, s.end_offset));
    side.segments = segs.len() as u64;
    let mut prev: Option<i64> = None;
    let mut lineage_missing = 0u64;
    for seg in segs {
        let unknown = |side: &mut ArchiveSide, why: String| {
            side.unknown.get_or_insert(why);
        };
        if seg.sha256.is_empty() {
            side.unverified.push(seg.key.clone());
            findings.push(format!(
                "segment {} carries no sha256 (written before 0.21) and cannot be verified",
                seg.key
            ));
            unknown(
                &mut side,
                format!("segment {} cannot be verified (no sha256)", seg.key),
            );
            continue;
        }
        let bytes = match store.get(&seg.key) {
            Ok((bytes, _)) => bytes,
            Err(StoreError::NotFound(_)) => {
                side.failed.push(seg.key.clone());
                findings.push(format!(
                    "segment {} is listed in the manifest and the store does not hold it",
                    seg.key
                ));
                unknown(
                    &mut side,
                    format!("segment {} is missing from the store", seg.key),
                );
                continue;
            }
            Err(e) => {
                return Err(DrillError::Engine(logweir_core::engine::EngineError::from(
                    e,
                )))
            }
        };
        let want = seg
            .sha256
            .strip_prefix("sha256:")
            .unwrap_or(seg.sha256.as_str());
        if logweir_core::ids::sha256_hex(&bytes) != want {
            side.failed.push(seg.key.clone());
            findings.push(format!(
                "segment {} does not match the manifest's sha256",
                seg.key
            ));
            unknown(
                &mut side,
                format!("segment {} failed its sha256 check", seg.key),
            );
            continue;
        }
        let records = match logweir_engine_oso::kbak::decode_segment(&bytes) {
            Ok(r) => r,
            Err(e) => {
                side.unverified.push(seg.key.clone());
                findings.push(format!("segment {} could not be decoded: {e}", seg.key));
                side.downgrade.get_or_insert_with(|| e.to_string());
                unknown(
                    &mut side,
                    format!("segment {} could not be decoded", seg.key),
                );
                continue;
            }
        };
        let in_range = records
            .iter()
            .all(|r| r.offset >= seg.start_offset && r.offset <= seg.end_offset);
        if records.len() as i64 != seg.record_count || !in_range {
            side.failed.push(seg.key.clone());
            findings.push(format!(
                "segment {} decodes to {} records, the manifest says {} in offsets {}..={}{}",
                seg.key,
                records.len(),
                seg.record_count,
                seg.start_offset,
                seg.end_offset,
                if in_range {
                    ""
                } else {
                    ", and a decoded offset lies outside that range"
                }
            ));
            unknown(
                &mut side,
                format!("segment {} disagrees with the manifest", seg.key),
            );
            continue;
        }
        side.verified += 1;
        for r in records {
            side.decoded += 1;
            if let Some(pv) = prev {
                if r.offset > pv.saturating_add(1) {
                    let span = (r.offset - pv - 1) as u64;
                    side.offset_holes +=
                        span.saturating_sub(covered_by(&explained, pv + 1, r.offset - 1));
                }
            }
            prev = Some(prev.map_or(r.offset, |pv| pv.max(r.offset)));
            if !selected(window, r.timestamp) {
                continue;
            }
            if lineage_offset(&r.headers) != Some(r.offset) {
                lineage_missing += 1;
            }
            let digest = record_digest_ordered(
                r.key.as_deref(),
                r.value.as_deref(),
                &r.headers,
                r.timestamp,
            );
            if side.expected.insert(r.offset, digest).is_some() {
                findings.push(format!(
                    "source offset {} is archived more than once",
                    r.offset
                ));
                unknown(
                    &mut side,
                    format!("source offset {} is archived more than once", r.offset),
                );
            }
        }
    }
    if lineage_missing > 0 && side.unknown.is_none() {
        findings.push(format!(
            "{lineage_missing} selected archived records carry no x-original-offset naming their \
             own offset (an archive written with include_offset_headers: false), so no restored \
             record can be mapped back to them"
        ));
        side.unknown = Some(format!(
            "{lineage_missing} selected archived records carry no x-original-offset header"
        ));
    }
    Ok(side)
}

/// The replay comparison of one target partition against `expected`.
fn replay_side(
    reader: &dyn ClusterReader,
    target: &str,
    partition: i32,
    hi: i64,
    expected: &BTreeMap<i64, [u8; 32]>,
    findings: &mut Findings,
) -> Result<ReplayComparison, DrillError> {
    let mut r = ReplayComparison {
        expected: expected.len() as u64,
        ..ReplayComparison::default()
    };
    let mut seen: HashSet<i64> = HashSet::with_capacity(expected.len());
    let mut max_seen: Option<i64> = None;
    let mut from = 0i64;
    while from < hi {
        let chunk = reader.consume_range(target, partition, from, TARGET_READ_CHUNK)?;
        if chunk.is_empty() {
            return Err(DrillError::Operational(format!(
                "{target}/{partition}: reading the restored records stopped at offset {from} \
                 below the high watermark {hi}; a complete verification that read less than the \
                 whole partition cannot report on it"
            )));
        }
        for c in &chunk {
            r.restored += 1;
            let Some(o) = lineage_offset(&c.headers) else {
                r.unexpected += 1;
                findings.push(format!(
                    "target offset {} carries no x-original-offset",
                    c.offset
                ));
                continue;
            };
            if !seen.insert(o) {
                r.duplicates += 1;
                findings.push(format!(
                    "source offset {o} is restored again at target offset {}",
                    c.offset
                ));
                continue;
            }
            if max_seen.is_some_and(|m| o < m) {
                r.out_of_order += 1;
                findings.push(format!(
                    "source offset {o} is restored at target offset {} after source offset {}",
                    c.offset,
                    max_seen.unwrap_or_default()
                ));
            }
            max_seen = Some(max_seen.map_or(o, |m| m.max(o)));
            match expected.get(&o) {
                None => {
                    r.unexpected += 1;
                    findings.push(format!(
                        "source offset {o} at target offset {} is not in the expected output",
                        c.offset
                    ));
                }
                Some(want) => {
                    let got = record_digest_ordered(
                        c.key.as_deref(),
                        c.value.as_deref(),
                        &c.headers,
                        c.timestamp_ms,
                    );
                    if &got == want {
                        r.matching += 1;
                    } else {
                        r.mismatched += 1;
                        findings.push(format!(
                            "source offset {o} at target offset {} differs from the archive",
                            c.offset
                        ));
                    }
                }
            }
        }
        from = chunk.last().map_or(hi, |c| c.offset + 1);
    }
    for o in expected.keys().filter(|o| !seen.contains(o)) {
        r.missing += 1;
        findings.push(format!("source offset {o} is missing from the target"));
    }
    Ok(r)
}

/// What [`run`] hands back: the ledger entries and the block they are signed
/// beside.
pub(super) struct CompleteOutcome {
    pub(super) verdicts: Vec<SelectionVerdict>,
    pub(super) block: CompleteVerification,
    /// The manifest's capture gaps and pruned ranges for every verified
    /// partition, structured.
    pub(super) gaps: Vec<OffsetRange>,
    pub(super) pruned: Vec<OffsetRange>,
}

/// The complete lane: one verdict per partition of every restored topic —
/// every partition the manifest lists for a mapped topic, and every partition
/// of its target — in topic and partition order.
///
/// `listed` is phase 4's selection; every entry must be one of the
/// partitions this lane verifies, or the ledger could not cover it, which is
/// refused (`Operational`) rather than truncated.
pub(super) fn run(
    reader: &dyn ClusterReader,
    store: &Store,
    facts: &BackupSetFacts,
    listed: &[(String, i32)],
    mapping: &BTreeMap<String, String>,
    plan: &RestorePlan,
    max_records: Option<u64>,
) -> Result<CompleteOutcome, DrillError> {
    let window = window_of(plan);
    // Every (source topic, partition) to verify: the manifest's and the
    // target's, unioned.
    let mut parts: BTreeMap<(String, i32), (String, i64)> = BTreeMap::new();
    for (src, tgt) in mapping {
        let ends: BTreeMap<i32, i64> = reader.end_offsets(tgt)?.into_iter().collect();
        if let Some(t) = facts.topics.iter().find(|t| &t.name == src) {
            for p in &t.partitions {
                parts.insert(
                    (src.clone(), p.partition_id),
                    (tgt.clone(), ends.get(&p.partition_id).copied().unwrap_or(0)),
                );
            }
        }
        for (pid, hi) in ends {
            parts.insert((src.clone(), pid), (tgt.clone(), hi));
        }
    }
    for (t, p) in listed {
        if !parts.contains_key(&(t.clone(), *p)) {
            return Err(DrillError::Operational(format!(
                "phase 4 selected {t}/{p}, which is neither in the manifest nor on the target of \
                 a mapped topic; refusing a complete verification whose ledger cannot cover it"
            )));
        }
    }

    let mut verdicts = Vec::with_capacity(parts.len());
    let mut partitions = Vec::with_capacity(parts.len());
    let mut archive = ArchiveIntegrity {
        segments: 0,
        segments_verified: 0,
        segments_failed: Vec::new(),
        segments_unverified: Vec::new(),
        records_decoded: 0,
        offset_holes: 0,
    };
    let mut replay_total = ReplayComparison::default();
    let (mut gaps, mut pruned) = (Vec::new(), Vec::new());
    let mut stopped_at: Option<String> = None;
    let mut not_compared = 0u64;
    let mut budget_used = 0u64;

    for ((src, pid), (tgt, hi)) in &parts {
        let id = format!("{src}/{pid}");
        let pf = facts
            .topics
            .iter()
            .find(|t| &t.name == src)
            .and_then(|t| t.partitions.iter().find(|p| p.partition_id == *pid));
        if let Some(pf) = pf {
            for &(a, b) in &pf.gaps {
                gaps.push(range(src, *pid, a, b));
            }
            for &(a, b) in &pf.pruned {
                pruned.push(range(src, *pid, a, b));
            }
        }
        let mut findings = Findings::default();
        // THE BOUND, decided before any byte of the partition is read, from
        // the manifest's own counts: a cost bound, never a verification claim.
        let estimate: u64 = pf.map_or(0, |p| {
            p.segments
                .iter()
                .map(|s| s.record_count.max(0) as u64)
                .sum()
        });
        let over = stopped_at.is_some()
            || max_records.is_some_and(|m| budget_used.saturating_add(estimate) > m);
        if over {
            if stopped_at.is_none() {
                stopped_at = Some(id.clone());
            }
            not_compared += 1;
            let keys: Vec<String> = pf.map_or_else(Vec::new, |p| {
                p.segments.iter().map(|s| s.key.clone()).collect()
            });
            let why = format!(
                "not compared: the complete verification stopped at its bound of {} decoded \
                 records (sample.complete_max_records) before this partition",
                max_records.unwrap_or_default()
            );
            findings.push(why.clone());
            archive.segments += keys.len() as u64;
            archive.segments_unverified.extend(keys.iter().cloned());
            let replay = ReplayComparison {
                restored: (*hi).max(0) as u64,
                ..ReplayComparison::default()
            };
            replay_total.add(&replay);
            partitions.push(PartitionVerification {
                topic: src.clone(),
                partition: *pid,
                target_topic: tgt.clone(),
                compared: false,
                segments: keys.len() as u64,
                segments_verified: 0,
                records_decoded: 0,
                offset_holes: 0,
                replay: replay.clone(),
                findings: findings.into_vec(),
            });
            verdicts.push(SelectionVerdict {
                id,
                claimed: 0,
                segments: Evidence::Unverified { why: why.clone() },
                records: Evidence::Unverified { why },
                records_restored: replay.restored,
                reconciled: None,
                downgrade: None,
            });
            continue;
        }
        budget_used = budget_used.saturating_add(estimate);

        let side = archive_side(store, pf, &window, &mut findings)?;
        archive.segments += side.segments;
        archive.segments_verified += side.verified;
        archive.segments_failed.extend(side.failed.iter().cloned());
        archive
            .segments_unverified
            .extend(side.unverified.iter().cloned());
        archive.records_decoded += side.decoded;
        archive.offset_holes += side.offset_holes;

        // Segments lane. POSITIVE first, and non-vacuous over a listed
        // partition: every one of its segments verified. A partition the
        // manifest does not list (only the target has it) has no segment to
        // examine; its replay lane carries the whole check (nothing expected,
        // so anything restored there is unexpected).
        let segments = if side.failed.is_empty()
            && side.unverified.is_empty()
            && side.verified == side.segments
        {
            Evidence::Verified {
                checked: side.verified,
            }
        } else if !side.failed.is_empty() {
            Evidence::Failed {
                why: format!(
                    "{} of {} archived segments failed their check: {}",
                    side.failed.len(),
                    side.segments,
                    side.failed.join(", ")
                ),
            }
        } else {
            Evidence::Unverified {
                why: format!(
                    "{} of {} archived segments could not be verified: {}",
                    side.unverified.len(),
                    side.segments,
                    side.unverified.join(", ")
                ),
            }
        };

        let (replay, records, compared) = match &side.unknown {
            Some(why) => {
                not_compared += 1;
                let replay = ReplayComparison {
                    restored: (*hi).max(0) as u64,
                    ..ReplayComparison::default()
                };
                let why = format!("not compared: the expected output is not known ({why})");
                findings.push(why.clone());
                (replay, Evidence::Unverified { why }, false)
            }
            None => {
                let replay = replay_side(reader, tgt, *pid, *hi, &side.expected, &mut findings)?;
                let records = if replay.is_exact() {
                    Evidence::Verified {
                        checked: replay.expected,
                    }
                } else {
                    Evidence::Failed {
                        why: format!(
                            "the restored partition is not the expected output: {} expected, {} \
                             restored, {} missing, {} unexpected, {} duplicates, {} out of \
                             order, {} different from the archive",
                            replay.expected,
                            replay.restored,
                            replay.missing,
                            replay.unexpected,
                            replay.duplicates,
                            replay.out_of_order,
                            replay.mismatched
                        ),
                    }
                };
                (replay, records, true)
            }
        };
        replay_total.add(&replay);
        verdicts.push(SelectionVerdict {
            id,
            claimed: replay.expected,
            segments,
            records,
            records_restored: replay.restored,
            reconciled: compared.then_some((replay.expected, replay.matching)),
            downgrade: side.downgrade.clone(),
        });
        partitions.push(PartitionVerification {
            topic: src.clone(),
            partition: *pid,
            target_topic: tgt.clone(),
            compared,
            segments: side.segments,
            segments_verified: side.verified,
            records_decoded: side.decoded,
            offset_holes: side.offset_holes,
            replay,
            findings: findings.into_vec(),
        });
    }

    let covered = not_compared == 0;
    let incomplete_reason = (!covered).then(|| match &stopped_at {
        Some(first) => format!(
            "{not_compared} of {} partitions were not compared: the verification stopped at \
             sample.complete_max_records = {} decoded records, first at {first}",
            partitions.len(),
            max_records.unwrap_or_default()
        ),
        None => format!(
            "{not_compared} of {} partitions were not compared because their expected output \
             could not be established (their findings say why)",
            partitions.len()
        ),
    });
    gaps.sort();
    pruned.sort();
    // A segment key is unique per partition; the sorted lists read the same
    // whatever order the manifest listed them in.
    archive.segments_failed = sorted_unique(archive.segments_failed);
    archive.segments_unverified = sorted_unique(archive.segments_unverified);
    Ok(CompleteOutcome {
        verdicts,
        block: CompleteVerification {
            covered,
            incomplete_reason,
            max_records,
            window,
            archive,
            replay: replay_total,
            partitions,
        },
        gaps,
        pruned,
    })
}

fn range(topic: &str, partition: i32, from_offset: i64, to_offset: i64) -> OffsetRange {
    OffsetRange {
        topic: topic.to_string(),
        partition,
        from_offset,
        to_offset,
    }
}

fn sorted_unique(v: Vec<String>) -> Vec<String> {
    v.into_iter().collect::<BTreeSet<_>>().into_iter().collect()
}

/// The manifest's capture gaps and pruned ranges for the partitions a
/// SAMPLED verification selected, structured — the sampled lane's half of
/// `integrity.verification.gaps`/`pruned`.
#[must_use]
pub fn ranges_for(
    facts: &BackupSetFacts,
    selected: &[(String, i32)],
) -> (Vec<OffsetRange>, Vec<OffsetRange>) {
    let (mut gaps, mut pruned) = (Vec::new(), Vec::new());
    let wanted: BTreeSet<&(String, i32)> = selected.iter().collect();
    for t in &facts.topics {
        for p in &t.partitions {
            if !wanted.contains(&(t.name.clone(), p.partition_id)) {
                continue;
            }
            for &(a, b) in &p.gaps {
                gaps.push(range(&t.name, p.partition_id, a, b));
            }
            for &(a, b) in &p.pruned {
                pruned.push(range(&t.name, p.partition_id, a, b));
            }
        }
    }
    gaps.sort();
    gaps.dedup();
    pruned.sort();
    pruned.dedup();
    (gaps, pruned)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn covered_by_merges_overlaps_and_clips_to_the_hole() {
        assert_eq!(covered_by(&[], 5, 9), 0);
        assert_eq!(covered_by(&[(0, 100)], 5, 9), 5);
        assert_eq!(covered_by(&[(6, 7), (7, 8)], 5, 9), 3);
        assert_eq!(covered_by(&[(6, 6), (8, 8)], 5, 9), 2);
        assert_eq!(covered_by(&[(20, 30)], 5, 9), 0);
    }

    #[test]
    fn lineage_is_the_last_eight_byte_header() {
        let h = |v: i64| (LINEAGE_HEADER.to_string(), Some(v.to_le_bytes().to_vec()));
        assert_eq!(lineage_offset(&[]), None);
        assert_eq!(lineage_offset(&[h(777), h(6)]), Some(6));
        assert_eq!(
            lineage_offset(&[h(6), (LINEAGE_HEADER.into(), Some(b"6".to_vec()))]),
            None,
            "the LAST header decides; a malformed last one is no lineage, never a fallback"
        );
        assert_eq!(lineage_offset(&[("other".into(), Some(vec![0; 8]))]), None);
    }

    #[test]
    fn the_window_is_inclusive_at_its_end_and_open_below_an_archive_floor() {
        let w = CompleteWindow {
            start_ms: None,
            end_ms: 10,
        };
        assert!(selected(&w, i64::MIN));
        assert!(selected(&w, 10));
        assert!(!selected(&w, 11));
        let w = CompleteWindow {
            start_ms: Some(5),
            end_ms: 10,
        };
        assert!(!selected(&w, 4));
        assert!(selected(&w, 5));
    }
}
