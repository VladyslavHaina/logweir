//! **PROD-03.0 — schema dependency, judged at backup from the segments the run
//! just wrote.**
//!
//! After the engine exits and the manifest is read back, this reads a BOUNDED
//! SAMPLE of each named topic's archived segments through the read-only
//! archive handle, STREAMS them through Logweir's own `.kbak` decoder
//! (`logweir_engine_oso::kbak::scan_segment`), and judges each sampled
//! record's key and value against the Confluent wire-format framing —
//! `logweir_core::schema_dependency` is the contract. The verdict lands in the
//! receipt's 1.5.0 `schema_dependency` block, and from there in the catalog
//! point, the API and the console.
//!
//! **No schema registry is contacted.** The judgement reads archived bytes and
//! nothing else; there is no registry client anywhere in this crate.
//!
//! # The sample
//!
//! Per named topic: its partitions that hold records, lowest id first, at most
//! `SAMPLE_PARTITIONS`; per sampled partition, the first
//! `SAMPLE_RECORDS_PER_END` records of its first segment and the last
//! `SAMPLE_RECORDS_PER_END` of its last segment (by start offset; a partition
//! with one segment is scanned once, and every record of it is judged when it
//! holds no more than twice that many). The entry says `complete` when the
//! judged records are every record the manifest counts for the topic,
//! `sampled` otherwise.
//!
//! # Bounded — the security review of the first version (orchestrator note)
//!
//! The first version fetched each sampled segment whole and decoded EVERY
//! record into owned keys and values, with no bound on decompression — one
//! large segment or a decompression bomb could OOM-kill a runner whose backup
//! had already succeeded (the class of FX-23's M1). Now:
//!
//! - **no record is materialised**: the scan keeps each key's and value's
//!   first `MIN_FRAMED_LEN` bytes — all the framing test reads — and skips the
//!   rest; the head scan STOPS after its records; the tail is a ring of
//!   `SAMPLE_RECORDS_PER_END` prefixes;
//! - **the bytes are capped**: a segment the store reports larger than
//!   [`DetectionLimits::max_segment_bytes`] (16 MiB) is never fetched
//!   (`Store::get_bounded`: a size check, then a ranged read to that size);
//!   a body that decompresses past [`DetectionLimits::max_decompressed_bytes`]
//!   stops the scan; a zstd frame declaring a window above 8 MiB is refused
//!   before it is decoded; lz4 is decoded as a stream — all
//!   `segmentTooLargeForDetection`. What one segment's judgement holds is the
//!   stored bytes plus at most the zstd window and a few hundred KiB (the
//!   review's fix round, M1; measured in `tests/schema_dependency_memory.rs`);
//! - **the time is a hard stop**: [`DetectionLimits::time_budget`] runs from
//!   the start of detection; every store read gets only what is left of it
//!   (`Store::get_bounded`'s `within`), every scan stops reading when it runs
//!   out (`kbak::ScanLimits::deadline`), and every topic not yet judged is
//!   `detectionTimeBudgetExceeded`.
//!
//! # A topic the manifest lists twice
//!
//! Every manifest entry with the topic's name is judged together — their
//! partitions merged — exactly as `phase_run::run` counts its records, so the
//! judgement and the receipt's `records` describe the same records (the
//! review's L1).
//!
//! # Never fatal
//!
//! The archive and its manifest exist by now; a judgement that cannot be made
//! is a value the receipt records, never a failed backup. A segment that
//! cannot be read or decoded, decodes to a record count its manifest entry
//! does not state, or makes the detector panic, leaves the topic
//! `notAssessed` with `segmentUnreadable` — never `notDetected` — logged at
//! `warn`; the caps leave it `notAssessed` with their own reasons. A topic the
//! archive holds no record of is `notAssessed` with `noRecords`.

use logweir_core::backup_receipt::TopicSchemaDependency;
use logweir_core::engine::{BackupSetFacts, PartitionFacts, SegmentFacts};
use logweir_core::schema_dependency::{
    self as sd, TopicTally, MIN_FRAMED_LEN, REASON_NO_RECORDS, REASON_SEGMENT_TOO_LARGE,
    REASON_SEGMENT_UNREADABLE, REASON_TIME_BUDGET, SAMPLE_PARTITIONS, SAMPLE_RECORDS_PER_END,
};
use logweir_engine_oso::kbak::{scan_segment, RecordPrefix, ScanError, ScanLimits};
use std::collections::{BTreeMap, VecDeque};
use std::time::{Duration, Instant};

/// The largest STORED segment detection fetches: 16 MiB. The engine's default
/// segment is 10 MiB before compression (`backup.segment_max_bytes`), so a
/// default backup is judged whole; a set written with larger segments is
/// judged where they fit and `segmentTooLargeForDetection` where they do not.
/// The fetched bytes are held while one segment is scanned, so this is most of
/// what detection can hold at once.
pub const MAX_SEGMENT_BYTES: u64 = 16 << 20;

/// The most bytes one segment's body may decompress to before its scan stops:
/// 256 MiB. A bound on the work, not on memory: the scan streams the body and
/// holds none of it past its buffers.
pub const MAX_DECOMPRESSED_BYTES: u64 = 256 << 20;

/// The time one backup spends on detection: 120 s, from its start, with no
/// read or scan continuing past it. Every topic still to judge is then
/// `detectionTimeBudgetExceeded`. A `Backup`'s deadline should leave this much
/// after the engine.
pub const TIME_BUDGET: Duration = Duration::from_secs(120);

/// The bounds detection runs within. [`DetectionLimits::default`] is the
/// production one; a row narrows it to make a cap fire.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DetectionLimits {
    /// The largest stored segment fetched.
    pub max_segment_bytes: u64,
    /// The most bytes one segment's body may decompress to.
    pub max_decompressed_bytes: u64,
    /// The time detection may take, from its start.
    pub time_budget: Duration,
}

impl Default for DetectionLimits {
    fn default() -> Self {
        Self {
            max_segment_bytes: MAX_SEGMENT_BYTES,
            max_decompressed_bytes: MAX_DECOMPRESSED_BYTES,
            time_budget: TIME_BUDGET,
        }
    }
}

/// Where segment bytes come from: the archive
/// [`logweir_engine_oso::storage::Store`] in production, a double in a row that
/// needs a read to fail, to be slow, or an object to be large.
pub trait SegmentSource {
    /// The exact bytes of the object at `key` when it is at most `max_bytes`
    /// long; `Ok(None)`, with nothing fetched, when it is larger. The read
    /// must end within `within` (what is left of the budget), or fail.
    fn segment_bounded(
        &self,
        key: &str,
        max_bytes: u64,
        within: Duration,
    ) -> Result<Option<Vec<u8>>, String>;
}

impl SegmentSource for logweir_engine_oso::storage::Store {
    fn segment_bounded(
        &self,
        key: &str,
        max_bytes: u64,
        within: Duration,
    ) -> Result<Option<Vec<u8>>, String> {
        self.get_bounded(key, max_bytes, Some(within))
            .map_err(|e| e.to_string())
    }
}

/// Why a topic was not judged: the receipt's reason, and the detail logged.
struct NotJudged {
    reason: &'static str,
    why: String,
}

impl NotJudged {
    fn unreadable(why: String) -> Self {
        Self {
            reason: REASON_SEGMENT_UNREADABLE,
            why,
        }
    }
}

/// The budget as one detection sees it: how much of it is spent, read from a
/// clock that starts at zero.
struct Budget<'a> {
    elapsed: &'a dyn Fn() -> Duration,
    limit: Duration,
}

impl Budget<'_> {
    /// What is left, `None` once nothing is.
    fn left(&self) -> Option<Duration> {
        let left = self.limit.saturating_sub((self.elapsed)());
        (!left.is_zero()).then_some(left)
    }

    fn spent(&self, what: &str) -> NotJudged {
        NotJudged {
            reason: REASON_TIME_BUDGET,
            why: format!(
                "detection has used {:?} of its {:?} budget; {what}",
                (self.elapsed)(),
                self.limit
            ),
        }
    }
}

/// One entry per named topic, in name order: the judgement of what `archive`
/// holds for it, within [`DetectionLimits::default`]. A named topic `archive`
/// does not mention holds no record.
#[must_use]
pub fn detect(
    archive: &BackupSetFacts,
    topics: &[String],
    source: &dyn SegmentSource,
) -> BTreeMap<String, TopicSchemaDependency> {
    detect_within(archive, topics, source, &DetectionLimits::default())
}

/// [`detect`], within `limits`, on the wall clock.
#[must_use]
pub fn detect_within(
    archive: &BackupSetFacts,
    topics: &[String],
    source: &dyn SegmentSource,
    limits: &DetectionLimits,
) -> BTreeMap<String, TopicSchemaDependency> {
    let started = Instant::now();
    detect_on_clock(archive, topics, source, limits, &|| started.elapsed())
}

/// [`detect_within`], on a clock the caller supplies — `elapsed` answers how
/// much time this detection has used so far. A row uses it to make the budget
/// run out between two reads deterministically.
#[must_use]
pub fn detect_on_clock(
    archive: &BackupSetFacts,
    topics: &[String],
    source: &dyn SegmentSource,
    limits: &DetectionLimits,
    elapsed: &dyn Fn() -> Duration,
) -> BTreeMap<String, TopicSchemaDependency> {
    // ONE budget for the whole backup, measured from here: never restarted
    // per topic (the review's mutant RR1).
    let budget = Budget {
        elapsed,
        limit: limits.time_budget,
    };
    topics
        .iter()
        .map(|name| {
            // Every manifest entry with the name, merged (L1).
            let partitions: Vec<&PartitionFacts> = archive
                .topics
                .iter()
                .filter(|t| &t.name == name)
                .flat_map(|t| t.partitions.iter())
                .collect();
            // ANY failure is a value: a panic inside the decoder or the
            // detector is caught here and recorded as `segmentUnreadable`,
            // never let through to fail a backup whose archive exists.
            let judged = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                judge_topic(&partitions, source, limits, &budget)
            }))
            .unwrap_or_else(|_| {
                Err(NotJudged::unreadable(
                    "the detector panicked; nothing was judged".to_string(),
                ))
            });
            let entry = judged.unwrap_or_else(|not| {
                tracing::warn!(
                    topic = %name,
                    reason = not.reason,
                    error = %not.why,
                    "this topic's archived records were not judged for Confluent wire-format \
                     framing; the receipt records its schema dependency as notAssessed, never as \
                     not schema-dependent, and the backup is unaffected"
                );
                sd::not_assessed(not.reason)
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

/// One record's prefixes, as the tally takes them.
type Prefixes = (Option<Vec<u8>>, Option<Vec<u8>>);

/// One topic's judgement, over every partition the manifest lists for it, or
/// why it was not judged.
fn judge_topic(
    partitions: &[&PartitionFacts],
    source: &dyn SegmentSource,
    limits: &DetectionLimits,
    budget: &Budget<'_>,
) -> Result<TopicSchemaDependency, NotJudged> {
    let counted: u64 = partitions
        .iter()
        .flat_map(|p| p.segments.iter())
        .map(count_of)
        .fold(0u64, u64::saturating_add);
    if counted == 0 {
        return Ok(sd::not_assessed(REASON_NO_RECORDS));
    }
    let mut holding: Vec<&PartitionFacts> = partitions
        .iter()
        .copied()
        .filter(|p| holds_records(p))
        .collect();
    // Partition id first; a partition listed twice keeps both entries, side by
    // side, and each is sampled as listed.
    holding.sort_by_key(|p| p.partition_id);
    let mut tally = TopicTally::default();
    for p in holding.iter().take(SAMPLE_PARTITIONS) {
        let mut segs: Vec<&SegmentFacts> = p.segments.iter().filter(|s| count_of(s) > 0).collect();
        // Archive order is source-offset order; nothing orders a manifest.
        segs.sort_by_key(|s| (s.start_offset, s.end_offset));
        let (Some(first), Some(last)) = (segs.first(), segs.last()) else {
            continue;
        };
        let (head, tail) = if std::ptr::eq(*first, *last) {
            // One segment, scanned once: its head, and a ring of the records
            // after the head. A segment of up to twice the sample is judged
            // whole.
            sample(first, source, limits, budget, true)?
        } else {
            let (head, _) = sample(first, source, limits, budget, false)?;
            // The LAST segment's last records: its head and its ring together
            // hold at most twice the sample, and the last of them are kept.
            let (last_head, last_ring) = sample(last, source, limits, budget, true)?;
            let mut tail: VecDeque<Prefixes> = last_head.into_iter().chain(last_ring).collect();
            while tail.len() > SAMPLE_RECORDS_PER_END {
                tail.pop_front();
            }
            (head, tail)
        };
        for (key, value) in head.iter().chain(tail.iter()) {
            tally.observe(key.as_deref(), value.as_deref());
        }
    }
    // A scan read whole is held to its manifest count (`sample`), so the
    // judged records are never more than the topic's count, and equal it
    // exactly when every record was read.
    Ok(tally.finish(tally.records() == counted))
}

/// The head of `seg` (its first `SAMPLE_RECORDS_PER_END` records' prefixes)
/// and, when `whole`, a ring of the last `SAMPLE_RECORDS_PER_END` records
/// AFTER the head. Without `whole` the scan stops once the head is full.
fn sample(
    seg: &SegmentFacts,
    source: &dyn SegmentSource,
    limits: &DetectionLimits,
    budget: &Budget<'_>,
    whole: bool,
) -> Result<(Vec<Prefixes>, VecDeque<Prefixes>), NotJudged> {
    let Some(left) = budget.left() else {
        return Err(budget.spent(&format!("segment {} was not read", seg.key)));
    };
    let too_large = |why: String| NotJudged {
        reason: REASON_SEGMENT_TOO_LARGE,
        why,
    };
    let bytes = match source.segment_bounded(&seg.key, limits.max_segment_bytes, left) {
        Ok(Some(bytes)) => bytes,
        Ok(None) => {
            return Err(too_large(format!(
                "segment {} is stored larger than the {} bytes detection reads",
                seg.key, limits.max_segment_bytes
            )))
        }
        // A read the budget ended is the budget's, whatever the store said.
        Err(e) if budget.left().is_none() => {
            return Err(budget.spent(&format!("segment {}: {e}", seg.key)))
        }
        Err(e) => {
            return Err(NotJudged::unreadable(format!(
                "segment {} could not be read: {e}",
                seg.key
            )))
        }
    };
    // The scan stops reading when what was left of the budget at its start
    // has passed.
    let scan_limits = ScanLimits {
        max_decompressed: limits.max_decompressed_bytes,
        deadline: budget.left().map(|left| Instant::now() + left),
    };
    if scan_limits.deadline.is_none() {
        return Err(budget.spent(&format!("segment {} was read but not scanned", seg.key)));
    }
    let mut head: Vec<Prefixes> = Vec::with_capacity(SAMPLE_RECORDS_PER_END);
    let mut tail: VecDeque<Prefixes> = VecDeque::with_capacity(SAMPLE_RECORDS_PER_END + 1);
    let mut visit = |r: RecordPrefix| {
        if head.len() < SAMPLE_RECORDS_PER_END {
            head.push((r.key, r.value));
            return whole || head.len() < SAMPLE_RECORDS_PER_END;
        }
        tail.push_back((r.key, r.value));
        if tail.len() > SAMPLE_RECORDS_PER_END {
            tail.pop_front();
        }
        true
    };
    let read =
        scan_segment(&bytes, &scan_limits, MIN_FRAMED_LEN, &mut visit).map_err(|e| match e {
            ScanError::TooLarge(m) => too_large(format!("segment {}: {m}", seg.key)),
            ScanError::TimedOut(m) => budget.spent(&format!("segment {}: {m}", seg.key)),
            ScanError::Unreadable(m) => {
                NotJudged::unreadable(format!("segment {} could not be decoded: {m}", seg.key))
            }
        })?;
    // A scan that read the whole segment is held to its manifest count. A
    // head scan that stopped early cannot be, and its records are still
    // records the manifest counts.
    let stopped_early = !whole && read == SAMPLE_RECORDS_PER_END as u64;
    if !stopped_early && read != count_of(seg) {
        return Err(NotJudged::unreadable(format!(
            "segment {} decodes to {read} records and its manifest entry counts {}",
            seg.key,
            count_of(seg)
        )));
    }
    if stopped_early && count_of(seg) < read {
        return Err(NotJudged::unreadable(format!(
            "segment {} holds at least {read} records and its manifest entry counts {}",
            seg.key,
            count_of(seg)
        )));
    }
    Ok((head, tail))
}
