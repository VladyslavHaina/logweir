//! **PROD-03.0 — schema dependency, judged at backup from the segments the run
//! just wrote.**
//!
//! After the engine exits and the manifest is read back, this reads a BOUNDED
//! SAMPLE of each named topic's archived segments through the read-only
//! archive handle, decodes them with Logweir's own `.kbak` decoder
//! (`logweir_engine_oso::kbak`, the one the complete verification lane uses),
//! and judges each record's key and value bytes against the Confluent
//! wire-format framing — `logweir_core::schema_dependency` is the contract.
//! The verdict lands in the receipt's 1.5.0 `schema_dependency` block, and
//! from there in the catalog point, the API and the console.
//!
//! **No schema registry is contacted.** The judgement reads archived bytes and
//! nothing else; there is no registry client anywhere in this crate.
//!
//! # The sample
//!
//! Per named topic: its partitions that hold records, lowest id first, at most
//! `SAMPLE_PARTITIONS`; per sampled partition, the first
//! `SAMPLE_RECORDS_PER_END` records of its first segment and the last
//! `SAMPLE_RECORDS_PER_END` of its last segment (by start offset; one segment
//! is read once, and every record of it is judged when it holds no more than
//! twice that many). The entry says `complete` when the judged records are
//! every record the manifest counts for the topic, `sampled` otherwise.
//!
//! # Never fatal
//!
//! The archive and its manifest exist by now; a judgement that cannot be made
//! is a value the receipt records, never a failed backup. A segment that cannot
//! be read or decoded, or decodes to a record count its manifest entry does
//! not state, makes the topic `notAssessed` with `segmentUnreadable` — never
//! `notDetected` — and is logged at `warn`. A topic the archive holds no record
//! of is `notAssessed` with `noRecords`.

use logweir_core::backup_receipt::TopicSchemaDependency;
use logweir_core::engine::{BackupSetFacts, PartitionFacts, SegmentFacts, TopicFacts};
use logweir_core::schema_dependency::{
    self as sd, TopicTally, REASON_NO_RECORDS, REASON_SEGMENT_UNREADABLE, SAMPLE_PARTITIONS,
    SAMPLE_RECORDS_PER_END,
};
use logweir_engine_oso::kbak::ArchivedRecord;
use std::collections::BTreeMap;

/// Where segment bytes come from: the archive [`logweir_engine_oso::storage::Store`]
/// in production, a double in a row that needs a read to fail.
pub trait SegmentSource {
    /// The exact bytes of the object at `key`.
    fn segment(&self, key: &str) -> Result<Vec<u8>, String>;
}

impl SegmentSource for logweir_engine_oso::storage::Store {
    fn segment(&self, key: &str) -> Result<Vec<u8>, String> {
        self.get(key)
            .map(|(bytes, _)| bytes)
            .map_err(|e| e.to_string())
    }
}

/// One entry per named topic, in name order: the judgement of what `archive`
/// holds for it. A named topic `archive` does not mention holds no record.
#[must_use]
pub fn detect(
    archive: &BackupSetFacts,
    topics: &[String],
    source: &dyn SegmentSource,
) -> BTreeMap<String, TopicSchemaDependency> {
    topics
        .iter()
        .map(|name| {
            let facts = archive.topics.iter().find(|t| &t.name == name);
            let entry = judge_topic(facts, source).unwrap_or_else(|why| {
                tracing::warn!(
                    topic = %name,
                    error = %why,
                    "a segment of this topic could not be judged for Confluent wire-format \
                     framing; the receipt records its schema dependency as notAssessed \
                     (segmentUnreadable), never as not schema-dependent"
                );
                sd::not_assessed(REASON_SEGMENT_UNREADABLE)
            });
            tracing::info!(
                topic = %name,
                verdict = %entry.verdict,
                basis = entry.basis.as_deref().unwrap_or("-"),
                reason = entry.reason.as_deref().unwrap_or("-"),
                schema_ids = ?sd::dependent_ids(&entry).0,
                "schema dependency judged from the archived bytes (no registry contacted)"
            );
            (name.clone(), entry)
        })
        .collect()
}

/// A segment's manifest count, `0` for a negative one (the manifest's `i64`
/// read as a count, the way `phase_run::run` reads it).
fn count_of(seg: &SegmentFacts) -> u64 {
    seg.record_count.max(0) as u64
}

fn holds_records(p: &PartitionFacts) -> bool {
    p.segments.iter().any(|s| count_of(s) > 0)
}

/// One topic's judgement, or why a segment it needed could not be judged.
fn judge_topic(
    facts: Option<&TopicFacts>,
    source: &dyn SegmentSource,
) -> Result<TopicSchemaDependency, String> {
    let Some(facts) = facts else {
        return Ok(sd::not_assessed(REASON_NO_RECORDS));
    };
    let counted: u64 = facts
        .partitions
        .iter()
        .flat_map(|p| p.segments.iter())
        .map(count_of)
        .fold(0u64, u64::saturating_add);
    if counted == 0 {
        return Ok(sd::not_assessed(REASON_NO_RECORDS));
    }
    let mut partitions: Vec<&PartitionFacts> = facts
        .partitions
        .iter()
        .filter(|p| holds_records(p))
        .collect();
    partitions.sort_by_key(|p| p.partition_id);
    let mut tally = TopicTally::default();
    for p in partitions.iter().take(SAMPLE_PARTITIONS) {
        let mut segs: Vec<&SegmentFacts> = p.segments.iter().filter(|s| count_of(s) > 0).collect();
        // Archive order is source-offset order; nothing orders a manifest.
        segs.sort_by_key(|s| (s.start_offset, s.end_offset));
        let (Some(first), Some(last)) = (segs.first(), segs.last()) else {
            continue;
        };
        if std::ptr::eq(*first, *last) {
            let records = decoded(first, source)?;
            let n = records.len();
            if n <= 2 * SAMPLE_RECORDS_PER_END {
                observe(&mut tally, &records);
            } else {
                observe(&mut tally, &records[..SAMPLE_RECORDS_PER_END]);
                observe(&mut tally, &records[n - SAMPLE_RECORDS_PER_END..]);
            }
        } else {
            let head = decoded(first, source)?;
            observe(&mut tally, &head[..head.len().min(SAMPLE_RECORDS_PER_END)]);
            let tail = decoded(last, source)?;
            observe(
                &mut tally,
                &tail[tail.len().saturating_sub(SAMPLE_RECORDS_PER_END)..],
            );
        }
    }
    // `decoded` holds every segment to its manifest count, so the judged
    // records are never more than the topic's count, and equal it exactly when
    // every record was read.
    Ok(tally.finish(tally.records() == counted))
}

fn observe(tally: &mut TopicTally, records: &[ArchivedRecord]) {
    for r in records {
        tally.observe(r.key.as_deref(), r.value.as_deref());
    }
}

/// The segment's records, read and decoded — and refused when they are not
/// the records its manifest entry counts.
fn decoded(seg: &SegmentFacts, source: &dyn SegmentSource) -> Result<Vec<ArchivedRecord>, String> {
    let bytes = source
        .segment(&seg.key)
        .map_err(|e| format!("segment {} could not be read: {e}", seg.key))?;
    let records = logweir_engine_oso::kbak::decode_segment(&bytes)
        .map_err(|e| format!("segment {} could not be decoded: {e}", seg.key))?;
    if records.len() as u64 != count_of(seg) {
        return Err(format!(
            "segment {} decodes to {} records and its manifest entry counts {}",
            seg.key,
            records.len(),
            count_of(seg)
        ));
    }
    Ok(records)
}
