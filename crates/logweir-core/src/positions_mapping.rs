//! **Consumer position translation** (PROD-04.2; the design is
//! `docs/to-do/decisions/PROD-04.2-cutover.md` §2): the positions-mapping
//! document a passing complete restore writes, the scorecard block that binds
//! it, and the one rule that maps a captured position onto the target.
//!
//! # What is mapped, and when
//!
//! A restore bound to a recovery point whose receipt captured consumer
//! groups (PROD-04.1) maps every captured position of that point onto the
//! restored topics, in the same in-order scan of the target that phase 7's
//! complete verification already makes. The mapping is WRITTEN only over a
//! passing complete verification (`outcome: pass`, `coverage: complete`,
//! `complete.covered: true`; arm PM-2): otherwise there is no document and a
//! `Switchover` is refused `VerificationNotComplete`. Nothing here commits a
//! position; the `Switchover` kind applies a reviewed subset of the document.
//!
//! # The rule ([`classify_entry`])
//!
//! For a captured position `P` of source partition `(t, p)`, restored as
//! `(M(t), p)`:
//!
//! 1. `found` := the first target record whose LAST `x-original-offset` is
//!    `≥ P`, in target order ([`Prober`]); else the target's high watermark.
//! 2. `skipped` := archived records with a source offset in `[P, lineage(found))`
//!    (`[P, ∞)` at the end) that the restore's own selection excluded. An offset
//!    hole is not a record and does not count.
//! 3. The class:
//!
//! | coverage of `P` | condition | class | target offset |
//! |---|---|---|---|
//! | `withinArchive`, `atArchiveEnd` | `[P, found)` overlaps no gap or pruned range, `skipped = 0` | `exact` | `offset(found)` or the end |
//! | same | no gap or pruned range, `skipped > 0` | `approximate` (`selectionExcluded`) | same |
//! | same | `[P, found)` overlaps a gap or pruned range | `unavailable: ArchiveGap` | — |
//! | `noArchivedData` | `log_start = high_watermark = P` at capture and the target partition is empty | `exact` | 0 |
//! | any other coverage, `PositionBeyondEnd`, a position or group not captured | — | `unavailable` with its name | — |
//! | any | the partition was not restored | `unavailable: PartitionNotRestored` | — |
//! | any | a transactional archive | `unavailable: TransactionalArchive` | — |
//!
//! A captured position is read BEFORE the engine reads records, so it is at or
//! behind what the group had read: the error is re-delivery, never loss.

use crate::consumer_positions::{
    ConsumerPositions, DocumentRef, PartitionFacts, PositionEntry, PositionsDocument,
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

/// The document's own format. Major 1; arm PMD-3 refuses any other.
pub const DOCUMENT_FORMAT_VERSION: &str = "1.0.0";

/// The one rule the document's offsets were derived by (arm PMD-3).
pub const RULE: &str = "firstTargetRecordAtOrAboveLineage";

/// The largest document a run writes. Over it the block says
/// `omitted: tooLarge` with the counts, and a `Switchover` is refused
/// `NoPositionsMapping`.
pub const MAX_DOCUMENT_BYTES: u64 = 4 * 1024 * 1024;

pub const EXACT: &str = "exact";
pub const APPROXIMATE: &str = "approximate";
pub const UNAVAILABLE: &str = "unavailable";
/// `MappingEntry::class`'s closed set (arm PMD-5).
pub const CLASSES: [&str; 3] = [EXACT, APPROXIMATE, UNAVAILABLE];

/// The reason an `approximate` entry carries.
pub const SELECTION_EXCLUDED: &str = "selectionExcluded";

pub const TRANSACTIONAL_ARCHIVE: &str = "TransactionalArchive";
pub const ARCHIVE_GAP: &str = "ArchiveGap";
pub const PARTITION_NOT_RESTORED: &str = "PartitionNotRestored";
pub const NO_ARCHIVED_DATA: &str = "NoArchivedData";
pub const BEFORE_LOG_START: &str = "BeforeLogStart";
pub const BEFORE_ARCHIVE: &str = "BeforeArchive";
pub const BEYOND_ARCHIVE: &str = "BeyondArchive";
pub const POSITION_BEYOND_END: &str = "PositionBeyondEnd";
/// The prefix of the reason a position or group the backup did not capture
/// carries: `NotCaptured: <the capture's reason>`.
pub const NOT_CAPTURED: &str = "NotCaptured";

/// An `unavailable` entry's reason: one of these, or `NotCaptured: <reason>`
/// (arm PMD-5).
pub const UNAVAILABLE_REASONS: [&str; 8] = [
    TRANSACTIONAL_ARCHIVE,
    ARCHIVE_GAP,
    PARTITION_NOT_RESTORED,
    NO_ARCHIVED_DATA,
    BEFORE_LOG_START,
    BEFORE_ARCHIVE,
    BEYOND_ARCHIVE,
    POSITION_BEYOND_END,
];

pub const OMITTED_TOO_LARGE: &str = "tooLarge";
pub const OMITTED_WRITE_FAILED: &str = "writeFailed";
/// `PositionsMapping::omitted`'s closed set (arm PM-3).
pub const OMITTED_REASONS: [&str; 2] = [OMITTED_TOO_LARGE, OMITTED_WRITE_FAILED];

/// Where a run's mapping lives: beside its scorecard
/// (`logweir/drills/<run_id>.json`). Arm PM-4 holds the block's key to it.
#[must_use]
pub fn document_key(run_id: &str) -> String {
    format!("logweir/drills/{run_id}.positions-mapping.json")
}

/// Whether `reason` is a legal `unavailable` reason.
#[must_use]
pub fn is_unavailable_reason(reason: &str) -> bool {
    UNAVAILABLE_REASONS.contains(&reason)
        || reason
            .strip_prefix(NOT_CAPTURED)
            .and_then(|r| r.strip_prefix(": "))
            .is_some_and(|r| !r.is_empty())
}

/// **The positions-mapping document** (`<run_id>.positions-mapping.json`,
/// format 1.0.0). Unsigned: the scorecard's signed block binds its digest and
/// length. Field order is byte order (`det_json`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct MappingDocument {
    /// [`DOCUMENT_FORMAT_VERSION`].
    pub format_version: String,
    /// The restore run this mapping belongs to (the scorecard's `run_id`).
    pub run_id: String,
    /// The recovery point the positions came from (the plan's `source.point`).
    pub point_id: String,
    /// The positions document's digest, as the point's signed receipt binds it.
    pub positions_sha256: String,
    /// [`RULE`].
    pub rule: String,
    /// One entry per group the point's receipt selected, keyed by its id.
    pub groups: BTreeMap<String, GroupMapping>,
}

/// One selected group's mapping.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct GroupMapping {
    /// The receipt's group type (`classic`, `consumer`, or `other`); absent
    /// when the receipt records none.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub group_type: Option<String>,
    /// Whether the group had members when its positions were read: they may
    /// have moved after, so the review says so.
    pub active_at_capture: bool,
    /// `NotCaptured: <reason>` for a group the backup did not capture; it then
    /// has no entry (arm PMD-7).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    /// One entry per position entry of the positions document, in its order.
    pub entries: Vec<MappingEntry>,
    /// Partitions the group had never committed on: they stay without a
    /// position on the target, so the consumer's `auto.offset.reset` behaves
    /// as it did on the source.
    pub no_committed_position: u32,
}

/// One captured position, mapped.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct MappingEntry {
    /// The SOURCE topic.
    pub topic: String,
    pub partition: u32,
    /// The captured next-to-consume offset, when the backup read one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub position: Option<i64>,
    /// The restored topic, when the restore restored this topic.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target_topic: Option<String>,
    /// [`CLASSES`].
    pub class: String,
    /// The target offset to commit: present exactly when the class is not
    /// `unavailable`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target_offset: Option<i64>,
    /// Archived records the restore's selection excluded between the position
    /// and the record it maps to: present exactly on an `approximate` entry.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub skipped: Option<u64>,
    /// [`SELECTION_EXCLUDED`] on an `approximate` entry; an unavailable
    /// reason on an `unavailable` one; absent on an `exact` one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
}

impl MappingEntry {
    /// Whether the entry can be applied (it names a target offset).
    #[must_use]
    pub fn mapped(&self) -> bool {
        self.class != UNAVAILABLE && self.target_offset.is_some()
    }
}

/// **Scorecard block `integrity.verification.positions_mapping`** (format
/// 1.12.0, and 2.4.0): the mapping a passing complete restore wrote, bound by
/// digest. Arms PM-1 to PM-5 ([`check_block`]).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct PositionsMapping {
    /// Where the document is, and the digest and length of its bytes. Absent
    /// exactly when `omitted` is present (arm PM-3).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub document: Option<DocumentRef>,
    /// Why no document was written: [`OMITTED_REASONS`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub omitted: Option<String>,
    /// The entries by class, over every group.
    pub counts: MappingCounts,
    /// The groups the document maps (every group the point selected).
    pub groups: u32,
}

/// Entries counted by class.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct MappingCounts {
    pub exact: u64,
    pub approximate: u64,
    pub unavailable: u64,
}

impl MappingDocument {
    /// The entries counted by class.
    #[must_use]
    pub fn counts(&self) -> MappingCounts {
        let mut c = MappingCounts::default();
        for e in self.groups.values().flat_map(|g| g.entries.iter()) {
            match e.class.as_str() {
                EXACT => c.exact += 1,
                APPROXIMATE => c.approximate += 1,
                _ => c.unavailable += 1,
            }
        }
        c
    }

    /// The document's exact bytes: deterministic JSON.
    ///
    /// # Errors
    ///
    /// The document does not serialise (it always does).
    pub fn to_bytes(&self) -> Result<Vec<u8>, String> {
        crate::det_json::to_deterministic_json(self)
            .map_err(|e| format!("the positions-mapping document does not serialise: {e}"))
    }

    /// The scorecard block for this document and the bytes to write: no bytes
    /// and `omitted: tooLarge` over [`MAX_DOCUMENT_BYTES`].
    ///
    /// # Errors
    ///
    /// The document does not serialise.
    pub fn block(&self) -> Result<(PositionsMapping, Option<Vec<u8>>), String> {
        let bytes = self.to_bytes()?;
        let counts = self.counts();
        let groups = u32::try_from(self.groups.len()).unwrap_or(u32::MAX);
        if bytes.len() as u64 > MAX_DOCUMENT_BYTES {
            return Ok((
                PositionsMapping {
                    document: None,
                    omitted: Some(OMITTED_TOO_LARGE.to_string()),
                    counts,
                    groups,
                },
                None,
            ));
        }
        Ok((
            PositionsMapping {
                document: Some(DocumentRef {
                    key: document_key(&self.run_id),
                    sha256: crate::ids::sha256_prefixed(&bytes),
                    bytes: bytes.len() as u64,
                }),
                omitted: None,
                counts,
                groups,
            },
            Some(bytes),
        ))
    }
}

/// Where the first target record at or above a position's lineage was found.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Found {
    /// The record at `target_offset`, whose last `x-original-offset` is
    /// `lineage`.
    Record { target_offset: i64, lineage: i64 },
    /// No restored record carries a lineage at or above the position: the
    /// target partition's end.
    End,
}

/// **Step 1 of the rule, streamed**: fed the target partition's records in
/// order, it finds, for each probed position, the FIRST record whose lineage
/// is at or above it. One pass and memory for the positions only. In a
/// passing restore lineages ascend and no source offset repeats; with a
/// duplicate the first copy is the one found (PROD-01.1 04-4).
#[derive(Debug, Clone, Default)]
pub struct Prober {
    pending: Vec<i64>,
    next: usize,
    found: BTreeMap<i64, Found>,
}

impl Prober {
    /// A prober for these positions (any order, repeats folded).
    #[must_use]
    pub fn new(positions: impl IntoIterator<Item = i64>) -> Self {
        let pending: Vec<i64> = positions
            .into_iter()
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect();
        Self {
            pending,
            next: 0,
            found: BTreeMap::new(),
        }
    }

    /// Whether it probes nothing.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.pending.is_empty()
    }

    /// One target record, in target order.
    pub fn observe(&mut self, target_offset: i64, lineage: i64) {
        // Every pending position at or below this lineage is resolved here: an
        // earlier record at or above a larger position would also have been at
        // or above the smaller ones before it, so this is the first for each.
        while let Some(&p) = self.pending.get(self.next) {
            if p > lineage {
                break;
            }
            self.found.insert(
                p,
                Found::Record {
                    target_offset,
                    lineage,
                },
            );
            self.next += 1;
        }
    }

    /// The answers, every unresolved position at the end.
    #[must_use]
    pub fn finish(mut self) -> BTreeMap<i64, Found> {
        for &p in &self.pending[self.next..] {
            self.found.insert(p, Found::End);
        }
        self.found
    }
}

/// What phase 7's scan established for one restored SOURCE partition.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionScan {
    /// The restored topic.
    pub target_topic: String,
    /// The target partition's high watermark when it was read.
    pub high_watermark: i64,
    /// Step 1's answer for every probed position.
    pub found: BTreeMap<i64, Found>,
    /// The archived source offsets the restore's selection excluded, sorted.
    pub excluded: Vec<i64>,
    /// The manifest's capture gaps and pruned ranges, inclusive.
    pub gaps: Vec<(i64, i64)>,
}

/// How many of the sorted `offsets` lie in `[lo, hi)`.
#[must_use]
pub fn count_in(offsets: &[i64], lo: i64, hi: i64) -> u64 {
    if hi <= lo {
        return 0;
    }
    let a = offsets.partition_point(|&o| o < lo);
    let b = offsets.partition_point(|&o| o < hi);
    b.saturating_sub(a) as u64
}

/// Whether an inclusive range of `ranges` overlaps `[lo, hi)`.
#[must_use]
pub fn overlaps(ranges: &[(i64, i64)], lo: i64, hi: i64) -> bool {
    ranges.iter().any(|&(a, b)| a < hi && b >= lo)
}

fn unavailable(mut e: MappingEntry, reason: impl Into<String>) -> MappingEntry {
    e.class = UNAVAILABLE.to_string();
    e.target_offset = None;
    e.skipped = None;
    e.reason = Some(reason.into());
    e
}

/// The reason an unrelated coverage word gives.
fn coverage_reason(word: &str) -> String {
    match word {
        "noArchivedData" => NO_ARCHIVED_DATA.to_string(),
        "beforeLogStart" => BEFORE_LOG_START.to_string(),
        "beforeArchive" => BEFORE_ARCHIVE.to_string(),
        "beyondArchive" => BEYOND_ARCHIVE.to_string(),
        other => format!("{NOT_CAPTURED}: {other}"),
    }
}

/// **The rule** for one position entry of a captured group (module doc).
///
/// `facts` are the partition's capture facts, `target_topic` the restored
/// topic of the entry's source topic (absent when the restore did not restore
/// it), `scan` phase 7's scan of the partition (absent when it was not
/// restored) and `transactional` the plan's `allow_transaction_markers`.
#[must_use]
pub fn classify_entry(
    entry: &PositionEntry,
    facts: Option<&PartitionFacts>,
    target_topic: Option<&str>,
    scan: Option<&PartitionScan>,
    transactional: bool,
) -> MappingEntry {
    let base = MappingEntry {
        topic: entry.topic.clone(),
        partition: entry.partition,
        position: entry.position,
        target_topic: target_topic.map(str::to_string),
        class: UNAVAILABLE.to_string(),
        target_offset: None,
        skipped: None,
        reason: None,
    };
    // A restored marker or aborted record could sit under any position.
    if transactional {
        return unavailable(base, TRANSACTIONAL_ARCHIVE);
    }
    match entry.status.as_str() {
        "captured" => {}
        "excluded" => {
            let reason = entry.reason.as_deref().unwrap_or(POSITION_BEYOND_END);
            return unavailable(base, reason);
        }
        other => {
            let why = entry.reason.as_deref().unwrap_or(other);
            return unavailable(base, format!("{NOT_CAPTURED}: {why}"));
        }
    }
    let Some(p) = entry.position else {
        return unavailable(base, format!("{NOT_CAPTURED}: NotAPosition"));
    };
    let (Some(_), Some(scan)) = (target_topic, scan) else {
        return unavailable(base, PARTITION_NOT_RESTORED);
    };
    let coverage = entry.coverage.as_deref().unwrap_or("");
    match coverage {
        "withinArchive" | "atArchiveEnd" => {
            // Every captured position of a restored partition is probed, so a
            // miss cannot happen; it is never read as the end.
            let Some(found) = scan.found.get(&p) else {
                return unavailable(base, PARTITION_NOT_RESTORED);
            };
            let (upper, target_offset) = match *found {
                Found::Record {
                    target_offset,
                    lineage,
                } => (lineage, target_offset),
                Found::End => (i64::MAX, scan.high_watermark),
            };
            if overlaps(&scan.gaps, p, upper) {
                return unavailable(base, ARCHIVE_GAP);
            }
            let skipped = count_in(&scan.excluded, p, upper);
            let mut e = base;
            e.target_offset = Some(target_offset);
            if skipped == 0 {
                e.class = EXACT.to_string();
            } else {
                e.class = APPROXIMATE.to_string();
                e.skipped = Some(skipped);
                e.reason = Some(SELECTION_EXCLUDED.to_string());
            }
            e
        }
        "noArchivedData"
            if facts.is_some_and(|f| f.log_start == Some(p) && f.high_watermark == Some(p))
                && scan.high_watermark == 0 =>
        {
            let mut e = base;
            e.class = EXACT.to_string();
            e.target_offset = Some(0);
            e
        }
        other => unavailable(base, coverage_reason(other)),
    }
}

/// Every captured position of every captured group, per source partition:
/// what phase 7's scan must probe.
#[must_use]
pub fn probes(document: &PositionsDocument) -> BTreeMap<(String, u32), BTreeSet<i64>> {
    let mut out: BTreeMap<(String, u32), BTreeSet<i64>> = BTreeMap::new();
    for g in document.groups.values() {
        for e in &g.positions {
            if e.status == "captured" {
                if let Some(p) = e.position {
                    out.entry((e.topic.clone(), e.partition))
                        .or_default()
                        .insert(p);
                }
            }
        }
    }
    out
}

/// What [`build`] maps from.
pub struct MapInputs<'a> {
    pub run_id: &'a str,
    pub point_id: &'a str,
    /// The positions document's digest (`sha256:<hex>`).
    pub positions_sha256: &'a str,
    /// The receipt's verified `consumer_positions` block.
    pub receipt: &'a ConsumerPositions,
    /// The positions document, verified against `receipt`.
    pub document: &'a PositionsDocument,
    /// The restore's source → target topic mapping.
    pub topic_mapping: &'a BTreeMap<String, String>,
    /// Phase 7's scans, per RESTORED source partition.
    pub scans: &'a BTreeMap<(String, u32), PartitionScan>,
    /// The plan's `restore.allow_transaction_markers`.
    pub transactional: bool,
}

/// The positions-mapping document of one restore.
#[must_use]
pub fn build(i: &MapInputs<'_>) -> MappingDocument {
    let facts_of = |topic: &str, partition: u32| {
        i.document
            .topics
            .get(topic)
            .and_then(|t| t.partitions.iter().find(|f| f.partition == partition))
    };
    let mut groups = BTreeMap::new();
    for (id, snap) in &i.receipt.groups {
        let doc = i.document.groups.get(id);
        let (true, Some(doc)) = (snap.outcome == "captured", doc) else {
            let why = snap.reason.as_deref().unwrap_or(snap.outcome.as_str());
            groups.insert(
                id.clone(),
                GroupMapping {
                    group_type: snap.group_type.clone(),
                    active_at_capture: snap.active.unwrap_or(false),
                    reason: Some(format!("{NOT_CAPTURED}: {why}")),
                    entries: Vec::new(),
                    no_committed_position: 0,
                },
            );
            continue;
        };
        let entries = doc
            .positions
            .iter()
            .map(|e| {
                classify_entry(
                    e,
                    facts_of(&e.topic, e.partition),
                    i.topic_mapping.get(&e.topic).map(String::as_str),
                    i.scans.get(&(e.topic.clone(), e.partition)),
                    i.transactional,
                )
            })
            .collect();
        groups.insert(
            id.clone(),
            GroupMapping {
                group_type: snap.group_type.clone(),
                // A captured group always records it; absent reads as active.
                active_at_capture: snap.active.unwrap_or(true),
                reason: None,
                entries,
                no_committed_position: doc.no_committed_position,
            },
        );
    }
    MappingDocument {
        format_version: DOCUMENT_FORMAT_VERSION.to_string(),
        run_id: i.run_id.to_string(),
        point_id: i.point_id.to_string(),
        positions_sha256: i.positions_sha256.to_string(),
        rule: RULE.to_string(),
        groups,
    }
}

/// What arms PM-1 to PM-5 read off the scorecard around the block.
pub struct BlockContext<'a> {
    pub run_id: &'a str,
    /// Whether the document's `format_version` defines the block.
    pub defined: bool,
    /// `outcome == pass`.
    pub passed: bool,
    /// `integrity.verification.coverage == complete`.
    pub complete: bool,
    /// `integrity.verification.complete.covered`.
    pub covered: bool,
}

/// **Arms PM-1 to PM-5**, the block's invariants, called once from
/// `Scorecard::validate_invariants` and mirrored in
/// `docs/verify_scorecard.py::_positions_mapping_fault`. Not interpolated but
/// for the versions and the closed words.
///
/// # Errors
///
/// The first arm the block breaks, as its message.
pub fn check_block(b: &PositionsMapping, cx: &BlockContext<'_>) -> Result<(), String> {
    // PM-1. Defined from 1.12.0 of format 1 and 2.4.0 of format 2.
    if !cx.defined {
        return Err(format!(
            "integrity.verification.positions_mapping is present but the format_version does \
             not define it: the block is defined from 1.{}.0 of format 1 and from 2.{}.0 of \
             format 2",
            crate::scorecard::POSITIONS_MAPPING_SINCE_MINOR,
            crate::scorecard::POSITIONS_MAPPING_SINCE_MINOR_OF_MAJOR_2
        ));
    }
    // PM-2. Only over a passing complete verification (08.1-A4).
    if !(cx.passed && cx.complete && cx.covered) {
        return Err(
            "integrity.verification.positions_mapping is present but the run is not a \
                    passing complete verification; positions are mapped only when outcome is \
                    pass, coverage is complete and complete.covered is true"
                .to_string(),
        );
    }
    // PM-3. A document, or the reason there is none: exactly one.
    match (&b.document, &b.omitted) {
        (Some(_), None) => {}
        (None, Some(o)) if OMITTED_REASONS.contains(&o.as_str()) => {}
        _ => {
            return Err(
                "integrity.verification.positions_mapping names exactly one of document \
                        and omitted, and omitted is tooLarge or writeFailed"
                    .to_string(),
            )
        }
    }
    // PM-4. The document is this run's, and its digest and length are sound.
    if let Some(d) = &b.document {
        if d.key != document_key(cx.run_id)
            || !crate::check_contract::is_sha256_prefixed(&d.sha256)
            || d.bytes == 0
            || d.bytes > MAX_DOCUMENT_BYTES
        {
            return Err(format!(
                "integrity.verification.positions_mapping.document is not this run's mapping: \
                 its key is logweir/drills/<run_id>.positions-mapping.json, its sha256 is \
                 sha256:<64 lowercase hex> and its length is 1 to {MAX_DOCUMENT_BYTES} bytes"
            ));
        }
    }
    // PM-5. It maps at least one group (a receipt never selects none).
    if b.groups == 0 {
        return Err(
            "integrity.verification.positions_mapping.groups is 0; a mapping is written only \
             for a point that selected consumer groups"
                .to_string(),
        );
    }
    Ok(())
}

/// One verified partition of the scorecard: `(source topic, partition)` →
/// its target topic (`integrity.verification.complete.partitions[]`).
pub type VerifiedPartitions = BTreeMap<(String, i64), String>;

/// **Arms PMD-1 to PMD-7**: the document `--positions-mapping` names, checked
/// against the signed block and the scorecard's verified partitions. Mirrored
/// in `docs/verify_scorecard.py::check_positions_mapping_document`.
///
/// # Errors
///
/// The first arm the document breaks, as its message.
pub fn validate_document(
    block: &PositionsMapping,
    run_id: &str,
    verified: &VerifiedPartitions,
    bytes: &[u8],
    doc: &MappingDocument,
) -> Result<(), String> {
    // PMD-1. The block names a document.
    let Some(d) = &block.document else {
        return Err(
            "the scorecard's positions_mapping block names no document (omitted), so there is \
             no mapping to verify"
                .to_string(),
        );
    };
    // PMD-2. The exact bytes the scorecard's signature covers.
    let digest = crate::ids::sha256_prefixed(bytes);
    if digest != d.sha256 || bytes.len() as u64 != d.bytes {
        return Err(format!(
            "the positions-mapping document is {digest} over {} bytes but the scorecard binds \
             {} over {} bytes: it is not the mapping this scorecard signed",
            bytes.len(),
            d.sha256,
            d.bytes
        ));
    }
    // PMD-3. Format 1, this run's, by the one rule.
    if doc.format_version.split('.').next() != Some("1") || doc.run_id != run_id || doc.rule != RULE
    {
        return Err(format!(
            "the positions-mapping document is not a format 1 mapping of run {run_id:?} by rule \
             {RULE}"
        ));
    }
    // PMD-4. The block's counts are what the document says.
    if doc.counts() != block.counts || doc.groups.len() as u64 != u64::from(block.groups) {
        return Err(
            "the positions-mapping document's groups and class counts are not the ones the \
             scorecard's block records"
                .to_string(),
        );
    }
    for (id, g) in &doc.groups {
        // PMD-7. A group the backup did not capture maps nothing.
        if let Some(r) = &g.reason {
            if !is_unavailable_reason(r) || !r.starts_with(NOT_CAPTURED) || !g.entries.is_empty() {
                return Err(format!(
                    "group {id:?} carries a reason, so it was not captured: its reason is \
                     \"NotCaptured: <why>\" and it has no entry"
                ));
            }
        }
        for e in &g.entries {
            // PMD-5. The class, the offset, the count and the reason fit.
            let fits = match e.class.as_str() {
                EXACT => {
                    e.target_offset.is_some_and(|o| o >= 0)
                        && e.skipped.is_none()
                        && e.reason.is_none()
                }
                APPROXIMATE => {
                    e.target_offset.is_some_and(|o| o >= 0)
                        && e.skipped.is_some_and(|s| s >= 1)
                        && e.reason.as_deref() == Some(SELECTION_EXCLUDED)
                }
                UNAVAILABLE => {
                    e.target_offset.is_none()
                        && e.skipped.is_none()
                        && e.reason.as_deref().is_some_and(is_unavailable_reason)
                }
                _ => false,
            };
            if !fits {
                return Err(format!(
                    "group {id:?} entry {:?}/{}: an exact entry carries a target offset and \
                     nothing else, an approximate one a target offset, skipped of at least 1 and \
                     reason selectionExcluded, and an unavailable one only a reason from the \
                     closed set",
                    e.topic, e.partition
                ));
            }
            // PMD-6. A mapped entry lands on a partition this scorecard verified,
            // in its restored topic; a partition said not restored was not.
            let at = verified.get(&(e.topic.clone(), i64::from(e.partition)));
            let landed = match (e.class.as_str(), at) {
                (UNAVAILABLE, at) => {
                    e.reason.as_deref() != Some(PARTITION_NOT_RESTORED) || at.is_none()
                }
                (_, Some(t)) => e.target_topic.as_deref() == Some(t.as_str()),
                (_, None) => false,
            };
            if !landed {
                return Err(format!(
                    "group {id:?} entry {:?}/{}: a mapped entry names a partition the scorecard \
                     verified and its restored topic, and one marked PartitionNotRestored names \
                     none",
                    e.topic, e.partition
                ));
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::consumer_positions::{GroupPositions, GroupSnapshot, TopicPartitions};

    fn entry(status: &str, position: Option<i64>, coverage: Option<&str>) -> PositionEntry {
        PositionEntry {
            topic: "orders".into(),
            partition: 0,
            status: status.into(),
            position,
            reason: None,
            coverage: coverage.map(str::to_string),
        }
    }

    fn facts(log_start: i64, hw: i64, archived: Option<(i64, i64)>) -> PartitionFacts {
        PartitionFacts {
            partition: 0,
            observed: true,
            log_start: Some(log_start),
            high_watermark: Some(hw),
            log_start_after: Some(log_start),
            high_watermark_after: Some(hw),
            archived_first: archived.map(|a| a.0),
            archived_last: archived.map(|a| a.1),
        }
    }

    /// A scan of a target holding `lineages` at offsets 0.., probed at
    /// `positions`.
    fn scan(
        lineages: &[i64],
        positions: &[i64],
        excluded: &[i64],
        gaps: &[(i64, i64)],
    ) -> PartitionScan {
        let mut prober = Prober::new(positions.iter().copied());
        for (o, l) in lineages.iter().enumerate() {
            prober.observe(o as i64, *l);
        }
        PartitionScan {
            target_topic: "restored-orders".into(),
            high_watermark: lineages.len() as i64,
            found: prober.finish(),
            excluded: excluded.to_vec(),
            gaps: gaps.to_vec(),
        }
    }

    fn classify(p: i64, coverage: &str, s: &PartitionScan, f: &PartitionFacts) -> MappingEntry {
        classify_entry(
            &entry("captured", Some(p), Some(coverage)),
            Some(f),
            Some("restored-orders"),
            Some(s),
            false,
        )
    }

    /// §2.2 row 1: within the archive, no gap, nothing excluded: exact at the
    /// first record whose lineage is at or above P.
    #[test]
    fn a_position_within_the_archive_maps_exact_to_its_record() {
        // Source offsets 10..20 restored at target 0..10; the log start was 10.
        let lineages: Vec<i64> = (10..20).collect();
        let s = scan(&lineages, &[14], &[], &[]);
        let e = classify(14, "withinArchive", &s, &facts(10, 20, Some((10, 19))));
        assert_eq!((e.class.as_str(), e.target_offset), (EXACT, Some(4)));
        assert_eq!((e.skipped, e.reason), (None, None));
    }

    /// The headline's negative control: committing P unmapped on a point whose
    /// log start is 5 lands 5 records off.
    #[test]
    fn the_mapped_offset_is_not_the_source_offset() {
        let lineages: Vec<i64> = (5..15).collect();
        let s = scan(&lineages, &[8], &[], &[]);
        let e = classify(8, "withinArchive", &s, &facts(5, 15, Some((5, 14))));
        assert_eq!(e.target_offset, Some(3));
        assert_ne!(e.target_offset, Some(8));
    }

    /// §2.3: a compaction hole at P is exact at the next surviving record;
    /// holes are not records and a lookup for lineage = P finds nothing.
    #[test]
    fn a_compaction_hole_at_the_position_maps_exact_to_the_next_record() {
        let lineages = [0, 1, 2, 7, 8];
        let s = scan(&lineages, &[3], &[], &[]);
        let e = classify(3, "withinArchive", &s, &facts(0, 9, Some((0, 8))));
        assert_eq!((e.class.as_str(), e.target_offset), (EXACT, Some(3)));
        assert!(!lineages.contains(&3), "no record carries lineage = P");
    }

    /// §2.3: a caught-up group maps to the target's end (not end − 1).
    #[test]
    fn a_caught_up_position_maps_exact_to_the_end() {
        let lineages: Vec<i64> = (0..40).collect();
        let s = scan(&lineages, &[40], &[], &[]);
        let e = classify(40, "atArchiveEnd", &s, &facts(0, 40, Some((0, 39))));
        assert_eq!((e.class.as_str(), e.target_offset), (EXACT, Some(40)));
    }

    /// §2.2 row 2: records the selection excluded between P and the record it
    /// maps to make it approximate, with the exact count; holes do not count.
    #[test]
    fn excluded_records_before_the_found_record_make_it_approximate() {
        // Source 0..10 archived; the window start excluded 0..6; restored 6..10.
        let lineages = [6, 7, 8, 9];
        let s = scan(&lineages, &[2], &[0, 1, 2, 3, 4, 5], &[]);
        let e = classify(2, "withinArchive", &s, &facts(0, 10, Some((0, 9))));
        assert_eq!(e.class, APPROXIMATE);
        assert_eq!(e.target_offset, Some(0));
        assert_eq!(e.skipped, Some(4), "offsets 2, 3, 4 and 5");
        assert_eq!(e.reason.as_deref(), Some(SELECTION_EXCLUDED));
        // Excluded records AFTER the found one are not counted.
        let s = scan(&[0, 1, 2], &[1], &[3, 4], &[]);
        let e = classify(1, "withinArchive", &s, &facts(0, 5, Some((0, 4))));
        assert_eq!((e.class.as_str(), e.target_offset), (EXACT, Some(1)));
    }

    /// One excluded record is already approximate (C6's live fixture):
    /// `skipped` is a count, never a threshold.
    #[test]
    fn a_single_excluded_record_is_approximate() {
        let s = scan(&[3, 4], &[2], &[2], &[]);
        let e = classify(2, "withinArchive", &s, &facts(0, 5, Some((0, 4))));
        assert_eq!((e.class.as_str(), e.skipped), (APPROXIMATE, Some(1)));
    }

    /// A window END that excluded everything from P on maps to the end,
    /// approximate, counting every excluded record from P.
    #[test]
    fn excluded_records_to_the_end_count_from_the_position() {
        let s = scan(&[0, 1, 2], &[2], &[3, 4, 5], &[]);
        let e = classify(2, "withinArchive", &s, &facts(0, 6, Some((0, 5))));
        assert_eq!((e.class.as_str(), e.target_offset), (EXACT, Some(2)));
        let s = scan(&[0, 1, 2], &[3], &[3, 4, 5], &[]);
        let e = classify(3, "withinArchive", &s, &facts(0, 6, Some((0, 5))));
        assert_eq!(e.class, APPROXIMATE);
        assert_eq!((e.target_offset, e.skipped), (Some(3), Some(3)));
    }

    /// §2.2 row 3: a recorded gap or pruned range between P and its record.
    #[test]
    fn a_gap_between_the_position_and_its_record_is_unavailable() {
        let s = scan(&[0, 1, 6, 7], &[2], &[], &[(2, 5)]);
        let e = classify(2, "withinArchive", &s, &facts(0, 8, Some((0, 7))));
        assert_eq!(
            (e.class.as_str(), e.reason.as_deref()),
            (UNAVAILABLE, Some(ARCHIVE_GAP))
        );
        assert_eq!(e.target_offset, None);
        // A gap entirely before P, or at or after the found record, does not.
        let s = scan(&[0, 1, 6, 7], &[6], &[], &[(2, 5), (7, 7)]);
        let e = classify(6, "withinArchive", &s, &facts(0, 8, Some((0, 7))));
        assert_eq!((e.class.as_str(), e.target_offset), (EXACT, Some(2)));
    }

    /// §2.2 row 4 and C9: an emptied source partition maps exact to 0; copying
    /// P would be above the target's end.
    #[test]
    fn an_emptied_partition_maps_exact_to_zero() {
        let s = scan(&[], &[5], &[], &[]);
        let e = classify(5, "noArchivedData", &s, &facts(5, 5, None));
        assert_eq!((e.class.as_str(), e.target_offset), (EXACT, Some(0)));
        // Not when the source still held records, or the target is not empty.
        let e = classify(5, "noArchivedData", &s, &facts(3, 5, None));
        assert_eq!(e.reason.as_deref(), Some(NO_ARCHIVED_DATA));
        let s = scan(&[9], &[5], &[], &[]);
        let e = classify(5, "noArchivedData", &s, &facts(5, 5, None));
        assert_eq!(e.reason.as_deref(), Some(NO_ARCHIVED_DATA));
        // An empty source and empty target at 0 is exact at 0 too.
        let s = scan(&[], &[0], &[], &[]);
        let e = classify(0, "noArchivedData", &s, &facts(0, 0, None));
        assert_eq!((e.class.as_str(), e.target_offset), (EXACT, Some(0)));
    }

    /// §2.2 row 5: every other relation is unavailable by its name.
    #[test]
    fn unrelated_coverage_words_are_unavailable_by_name() {
        let s = scan(&[0, 1], &[], &[], &[]);
        let f = facts(0, 2, Some((0, 1)));
        for (word, reason) in [
            ("beforeLogStart", BEFORE_LOG_START),
            ("beforeArchive", BEFORE_ARCHIVE),
            ("beyondArchive", BEYOND_ARCHIVE),
        ] {
            let e = classify(1, word, &s, &f);
            assert_eq!(
                (e.class.as_str(), e.reason.as_deref()),
                (UNAVAILABLE, Some(reason))
            );
        }
        let mut beyond = entry("excluded", Some(99), None);
        beyond.reason = Some(POSITION_BEYOND_END.into());
        let e = classify_entry(&beyond, Some(&f), Some("restored-orders"), Some(&s), false);
        assert_eq!(e.reason.as_deref(), Some(POSITION_BEYOND_END));
        assert_eq!(e.position, Some(99), "the position is kept");
        let mut failed = entry("failed", None, None);
        failed.reason = Some("MarksNotRead".into());
        let e = classify_entry(&failed, Some(&f), Some("restored-orders"), Some(&s), false);
        assert_eq!(e.reason.as_deref(), Some("NotCaptured: MarksNotRead"));
        assert!(is_unavailable_reason(e.reason.as_deref().unwrap_or("")));
    }

    /// §2.2 last row and C6: a partition (or topic) the restore did not restore.
    #[test]
    fn a_partition_not_restored_is_unavailable() {
        let f = facts(0, 2, Some((0, 1)));
        let e = classify_entry(
            &entry("captured", Some(1), Some("withinArchive")),
            Some(&f),
            Some("restored-orders"),
            None,
            false,
        );
        assert_eq!(e.reason.as_deref(), Some(PARTITION_NOT_RESTORED));
        let s = scan(&[0, 1], &[1], &[], &[]);
        let e = classify_entry(
            &entry("captured", Some(1), Some("withinArchive")),
            Some(&f),
            None,
            Some(&s),
            false,
        );
        assert_eq!(e.reason.as_deref(), Some(PARTITION_NOT_RESTORED));
    }

    /// C16 / 04-1: a transactional archive maps every position unavailable.
    #[test]
    fn a_transactional_archive_maps_nothing() {
        let s = scan(&[0, 1, 2], &[1], &[], &[]);
        let f = facts(0, 3, Some((0, 2)));
        let e = classify_entry(
            &entry("captured", Some(1), Some("withinArchive")),
            Some(&f),
            Some("restored-orders"),
            Some(&s),
            true,
        );
        assert_eq!(e.reason.as_deref(), Some(TRANSACTIONAL_ARCHIVE));
        assert!(!e.mapped());
    }

    /// 04-4: with a duplicate the FIRST copy is found; with lineages out of
    /// order the first record at or above P in target order.
    #[test]
    fn the_prober_finds_the_first_copy() {
        let mut p = Prober::new([3, 1, 3]);
        for (o, l) in [(0, 0), (1, 2), (2, 2), (3, 5)] {
            p.observe(o, l);
        }
        let found = p.finish();
        assert_eq!(
            found.get(&1),
            Some(&Found::Record {
                target_offset: 1,
                lineage: 2
            })
        );
        assert_eq!(
            found.get(&3),
            Some(&Found::Record {
                target_offset: 3,
                lineage: 5
            })
        );
        let mut p = Prober::new([4]);
        p.observe(0, 1);
        assert_eq!(p.finish().get(&4), Some(&Found::End));
    }

    #[test]
    fn count_and_overlap_are_half_open() {
        assert_eq!(count_in(&[1, 2, 3, 4], 2, 4), 2);
        assert_eq!(count_in(&[1, 2, 3, 4], 4, 4), 0);
        assert_eq!(count_in(&[1, 2, 3, 4], 0, i64::MAX), 4);
        assert!(overlaps(&[(3, 5)], 5, 6));
        assert!(!overlaps(&[(3, 5)], 6, 9));
        assert!(!overlaps(&[(3, 5)], 0, 3));
        assert!(overlaps(&[(3, 5)], 0, 4));
    }

    fn receipt_and_document() -> (ConsumerPositions, PositionsDocument) {
        let at = chrono::DateTime::<chrono::Utc>::from_timestamp(1_700_000_000, 0).unwrap();
        let mut groups = BTreeMap::new();
        groups.insert(
            "app".to_string(),
            GroupSnapshot {
                outcome: "captured".into(),
                reason: None,
                group_type: Some("classic".into()),
                state: Some("Empty".into()),
                listed_state: Some("Empty".into()),
                members: Some(0),
                active: Some(false),
                counts: None,
            },
        );
        groups.insert(
            "gone".to_string(),
            GroupSnapshot {
                outcome: "excluded".into(),
                reason: Some("GroupNotFound".into()),
                group_type: None,
                state: None,
                listed_state: None,
                members: None,
                active: None,
                counts: None,
            },
        );
        let receipt = ConsumerPositions {
            observed_from: at,
            observed_to: at,
            listing: "complete".into(),
            document: DocumentRef {
                key: "k".into(),
                sha256: "sha256:x".into(),
                bytes: 1,
            },
            groups,
        };
        let mut topics = BTreeMap::new();
        topics.insert(
            "orders".to_string(),
            TopicPartitions {
                partitions: vec![facts(0, 100, Some((0, 99)))],
                changed_during_capture: false,
            },
        );
        let mut dgroups = BTreeMap::new();
        dgroups.insert(
            "app".to_string(),
            GroupPositions {
                positions: vec![entry("captured", Some(40), Some("withinArchive"))],
                no_committed_position: 0,
            },
        );
        let doc = PositionsDocument {
            format_version: "1.0.0".into(),
            backup_id: "b".into(),
            run_id: "r".into(),
            topics,
            groups: dgroups,
        };
        (receipt, doc)
    }

    #[test]
    fn build_maps_captured_groups_and_names_the_rest() {
        let (receipt, doc) = receipt_and_document();
        let probes = probes(&doc);
        assert_eq!(
            probes.get(&("orders".to_string(), 0)).map(|s| s.len()),
            Some(1)
        );
        let lineages: Vec<i64> = (0..100).collect();
        let mut scans = BTreeMap::new();
        scans.insert(
            ("orders".to_string(), 0u32),
            scan(&lineages, &[40], &[], &[]),
        );
        let mapping: BTreeMap<String, String> =
            [("orders".to_string(), "restored-orders".to_string())].into();
        let m = build(&MapInputs {
            run_id: "R1",
            point_id: "P1",
            positions_sha256: "sha256:p",
            receipt: &receipt,
            document: &doc,
            topic_mapping: &mapping,
            scans: &scans,
            transactional: false,
        });
        assert_eq!(m.groups["app"].entries[0].target_offset, Some(40));
        assert_eq!(m.groups["app"].reason, None);
        assert_eq!(
            m.groups["gone"].reason.as_deref(),
            Some("NotCaptured: GroupNotFound")
        );
        assert!(m.groups["gone"].entries.is_empty());
        assert_eq!(
            m.counts(),
            MappingCounts {
                exact: 1,
                approximate: 0,
                unavailable: 0
            }
        );
        let (block, bytes) = m.block().unwrap();
        let bytes = bytes.expect("small enough to write");
        assert_eq!(block.groups, 2);
        let d = block.document.as_ref().unwrap();
        assert_eq!(d.key, "logweir/drills/R1.positions-mapping.json");
        assert_eq!(d.bytes, bytes.len() as u64);
        // Both readers' document arms accept it against the verified partition.
        let verified: VerifiedPartitions =
            [(("orders".to_string(), 0), "restored-orders".to_string())].into();
        validate_document(&block, "R1", &verified, &bytes, &m).expect("accepted");
        // ...and refuse a changed byte, another run, and a mapped entry on a
        // partition the scorecard did not verify.
        let mut other = bytes.clone();
        other[0] ^= 1;
        assert!(validate_document(&block, "R1", &verified, &other, &m)
            .unwrap_err()
            .contains("not the mapping this scorecard signed"));
        assert!(validate_document(&block, "R2", &verified, &bytes, &m)
            .unwrap_err()
            .contains("not a format 1 mapping"));
        assert!(
            validate_document(&block, "R1", &BTreeMap::new(), &bytes, &m)
                .unwrap_err()
                .contains("names a partition the scorecard verified")
        );
    }

    #[test]
    fn the_block_arms_refuse_what_they_name() {
        let ok = PositionsMapping {
            document: Some(DocumentRef {
                key: document_key("R1"),
                sha256: format!("sha256:{}", "a".repeat(64)),
                bytes: 10,
            }),
            omitted: None,
            counts: MappingCounts::default(),
            groups: 1,
        };
        let cx = |defined, passed| BlockContext {
            run_id: "R1",
            defined,
            passed,
            complete: true,
            covered: true,
        };
        check_block(&ok, &cx(true, true)).expect("accepted");
        assert!(check_block(&ok, &cx(false, true))
            .unwrap_err()
            .contains("does not define it"));
        assert!(check_block(&ok, &cx(true, false))
            .unwrap_err()
            .contains("not a passing complete verification"));
        let mut both = ok.clone();
        both.omitted = Some(OMITTED_TOO_LARGE.into());
        assert!(check_block(&both, &cx(true, true))
            .unwrap_err()
            .contains("exactly one of document"));
        let mut other_key = ok.clone();
        if let Some(d) = other_key.document.as_mut() {
            d.key = document_key("R2");
        }
        assert!(check_block(&other_key, &cx(true, true))
            .unwrap_err()
            .contains("not this run's mapping"));
        let mut none = ok.clone();
        none.groups = 0;
        assert!(check_block(&none, &cx(true, true))
            .unwrap_err()
            .contains("groups is 0"));
        let omitted = PositionsMapping {
            document: None,
            omitted: Some(OMITTED_TOO_LARGE.into()),
            ..ok
        };
        check_block(&omitted, &cx(true, true)).expect("an omitted document is legal");
    }
}
