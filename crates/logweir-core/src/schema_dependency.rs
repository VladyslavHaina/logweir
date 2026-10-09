//! **PROD-03.0 — the detection contract for schema-dependent topics.**
//!
//! A restore can succeed while no application can read what it restored: a
//! record a Confluent serializer wrote is `0x00`, a 4-byte schema id and a
//! payload that only that schema decodes, and the schema lives in a registry
//! Logweir never captures (`docs/stability.md` Never #2). This module decides,
//! from archived record BYTES alone, whether a topic's keys or values carry
//! that framing — so the receipt, the catalog and the console can say
//! "schema-dependent, registry not captured" before anyone restores it.
//!
//! **No registry is contacted, by this module or by anything that calls it.**
//! It is pure (Global Constraint 1): no I/O, no clock, no network.
//!
//! # What counts as framed ([`framed_schema_id`])
//!
//! A payload is FRAMED when all three hold:
//!
//! 1. its first byte is the magic byte `0x00` ([`MAGIC_BYTE`]);
//! 2. bytes 1..5, read as a big-endian unsigned integer, are a PLAUSIBLE
//!    schema id: `1..=`[`MAX_SCHEMA_ID`] (16 777 215, i.e. the id's first
//!    byte is zero). Registries issue ids sequentially from 1 (Confluent
//!    Cloud from 100 001), so an id at or above 2^24 is not one a registry
//!    plausibly issued, and id 0 is never issued;
//! 3. at least one byte follows the id ([`MIN_FRAMED_LEN`] = 6): the Avro or
//!    JSON payload, or a Protobuf message's index bytes (a single `0x00` for
//!    the first message type) and its payload.
//!
//! Avro, JSON Schema and Protobuf share that prefix, so one test detects all
//! three; the payload after the id is not interpreted (no schema is at hand to
//! interpret it with).
//!
//! # Keys and values, nulls and tombstones
//!
//! Every judged record counts once on EACH side ([`SideTally`]): its key as
//! framed, unframed or null, and its value the same way. A null key and a
//! null value (a tombstone) count as `nulls` and never toward the share: a
//! compacted topic of framed values with tombstones is still dependent. An
//! empty, non-null payload is unframed.
//!
//! # The threshold, and how false positives are bounded
//!
//! A side is DEPENDENT ([`dependent_by_share`]) when at least one record is
//! framed AND at least one in [`DEPENDENT_SHARE_DENOMINATOR`] (10 %) of its
//! non-null judged records are. A topic is `schemaDependent` when its key side
//! or its value side is.
//!
//! - For bytes unrelated to the framing (uniform random), one payload passes
//!   the per-record test with probability 2^-16 x (1 - 2^-24), about
//!   1.5 x 10^-5 (a zero first byte, a zero id byte, a non-zero id). With
//!   `n` non-null records the side is flagged only if at least
//!   `max(1, ceil(n/10))` of them pass: about 1.5 x 10^-5 for `n` = 1, and
//!   below 10^-30 for `n` = 100. Random 4-byte ids after a zero byte are
//!   refused per record by rule 2 (255 in 256 of them are at or above 2^24).
//! - Short binary payloads that start with a zero byte — a big-endian 32-bit
//!   integer key, a 5-byte value — are refused by rule 3.
//! - A side where fewer than one in ten non-null records pass is not
//!   dependent, so occasional framing-shaped records in an unframed topic do
//!   not flag it; a migration topic where at least a tenth of the records are
//!   framed IS flagged, because those records need the registry.
//! - **Residual, stated:** structured binary payloads whose first two bytes
//!   are zero and that are six bytes or longer pass rule by rule — most
//!   notably big-endian 64-bit integers from 2^24 to 2^56 (a `LongSerializer`
//!   key holding a database id or an epoch-millisecond timestamp). Such a
//!   side reads as dependent, with the "ids" its bytes happen to hold. The
//!   flag only ever adds a warning; it never blocks a restore or changes what
//!   is restored. The "ids" listed for such a side are four bytes of the key
//!   itself.
//!
//! # What it does NOT detect (false negatives, stated)
//!
//! Only Confluent's payload prefix is read, so these read `notDetected`, and a
//! reader must not take `notDetected` for "no registry needed" without them in
//! mind:
//! - schema ids carried in record HEADERS (Confluent's header-based schema-id
//!   serializers, Apicurio's header mode);
//! - Apicurio's default 8-byte global id after the magic byte (its high bytes
//!   are zero, so the 4-byte "id" is 0, which is refused);
//! - other registries' framing: AWS Glue (magic byte 3), and anything else not
//!   starting with Confluent's magic byte 0;
//! - Confluent ids of 2^24 and above;
//! - a 5-byte framed value with an empty body (an Avro record with no fields);
//! - framing under one in ten of a side's sampled records, or only in
//!   segments or partitions outside the sample (`basis: sampled` says so).
//!
//! # Which records are read
//!
//! The caller decides (the backup runner reads the segments it just wrote:
//! `logweir`'s `backup::schema_dependency`), and says how much it read in the
//! entry's `basis`: `complete` when every record the receipt counts for the
//! topic was judged, `sampled` otherwise. The runner's bounded sample is, per
//! topic, the first [`SAMPLE_PARTITIONS`] partitions holding records (lowest
//! id first) and, per sampled partition, the first [`SAMPLE_RECORDS_PER_END`]
//! records of its first segment and the last [`SAMPLE_RECORDS_PER_END`] of its
//! last segment (every record when the partition holds no more than twice
//! that in at most two segments).
//!
//! # Bounded, and never a failed backup
//!
//! The runner reads only what the sample needs and holds only what the
//! detector looks at: segments are streamed (`logweir_engine_oso::kbak::
//! scan_segment`), each key and value kept to its first [`MIN_FRAMED_LEN`]
//! bytes, the head scan stops after its records and the tail is a ring of
//! [`SAMPLE_RECORDS_PER_END`]. A segment stored larger than the runner's
//! fetch cap, or one that decompresses past its decompression cap, is not
//! judged ([`REASON_SEGMENT_TOO_LARGE`]); a backup whose detection outlives
//! its time budget leaves the rest [`REASON_TIME_BUDGET`]; any other failure,
//! a panic included, is [`REASON_SEGMENT_UNREADABLE`]. All three are
//! `notAssessed`, never "not schema-dependent", and none fails the backup.

use crate::backup_receipt::{SideFraming, TopicSchemaDependency};
use std::collections::BTreeSet;

/// The Confluent wire format's magic byte.
pub const MAGIC_BYTE: u8 = 0x00;

/// The largest schema id the detector accepts as plausible: 2^24 - 1. The
/// smallest is 1.
pub const MAX_SCHEMA_ID: u32 = 0x00FF_FFFF;

/// The magic byte, four id bytes, and at least one byte after them.
pub const MIN_FRAMED_LEN: usize = 6;

/// A side is dependent when at least one in this many of its non-null records
/// is framed (and at least one is).
pub const DEPENDENT_SHARE_DENOMINATOR: u64 = 10;

/// The most schema ids a side LISTS (the smallest ones); `schema_id_count`
/// counts them all.
pub const SCHEMA_IDS_LISTED: usize = 16;

/// The runner's sample: at most this many partitions per topic, those holding
/// records, lowest id first.
pub const SAMPLE_PARTITIONS: usize = 8;

/// The runner's sample: this many records from the head of a sampled
/// partition's first segment, and as many from the tail of its last.
pub const SAMPLE_RECORDS_PER_END: usize = 500;

/// `verdict`: at least one side is dependent.
pub const SCHEMA_DEPENDENT: &str = "schemaDependent";
/// `verdict`: records were judged and neither side is dependent.
pub const NOT_DETECTED: &str = "notDetected";
/// `verdict`: nothing could be judged; `reason` says why.
pub const NOT_ASSESSED: &str = "notAssessed";
/// `TopicSchemaDependency::verdict`'s closed set (arm 24).
pub const VERDICTS: [&str; 3] = [SCHEMA_DEPENDENT, NOT_DETECTED, NOT_ASSESSED];

/// `reason`: the archive holds no record of the topic.
pub const REASON_NO_RECORDS: &str = "noRecords";
/// `reason`: a segment the sample needed could not be read or decoded, or
/// decoded to a record count its manifest does not record — or the detector
/// failed in any other way. Never a failed backup.
pub const REASON_SEGMENT_UNREADABLE: &str = "segmentUnreadable";
/// `reason`: a segment the sample needed is larger than the detector reads
/// at backup time — stored, or decompressed — so it was not judged (a large
/// segment, or a decompression bomb). Never "not schema-dependent".
pub const REASON_SEGMENT_TOO_LARGE: &str = "segmentTooLargeForDetection";
/// `reason`: the backup's time budget for detection ran out before this
/// topic was judged.
pub const REASON_TIME_BUDGET: &str = "detectionTimeBudgetExceeded";
/// `TopicSchemaDependency::reason`'s closed set (arm 24).
pub const NOT_ASSESSED_REASONS: [&str; 4] = [
    REASON_NO_RECORDS,
    REASON_SEGMENT_UNREADABLE,
    REASON_SEGMENT_TOO_LARGE,
    REASON_TIME_BUDGET,
];

/// `basis`: some of the topic's records were judged.
pub const BASIS_SAMPLED: &str = "sampled";
/// `basis`: every record the receipt counts for the topic was judged.
pub const BASIS_COMPLETE: &str = "complete";
/// `TopicSchemaDependency::basis`'s closed set (arm 24).
pub const BASES: [&str; 2] = [BASIS_SAMPLED, BASIS_COMPLETE];

/// The schema id `payload` names, when it is FRAMED (the module doc's three
/// rules); `None` otherwise.
#[must_use]
pub fn framed_schema_id(payload: &[u8]) -> Option<u32> {
    if payload.len() < MIN_FRAMED_LEN || payload[0] != MAGIC_BYTE {
        return None;
    }
    let id = u32::from_be_bytes([payload[1], payload[2], payload[3], payload[4]]);
    (1..=MAX_SCHEMA_ID).contains(&id).then_some(id)
}

/// THE THRESHOLD: at least one framed record, and at least one in
/// [`DEPENDENT_SHARE_DENOMINATOR`] of the side's non-null records framed.
/// Exact in `u128`, so no count a document can carry overflows it.
#[must_use]
pub fn dependent_by_share(framed: u64, unframed: u64) -> bool {
    framed >= 1
        && u128::from(framed) * u128::from(DEPENDENT_SHARE_DENOMINATOR)
            >= u128::from(framed) + u128::from(unframed)
}

/// How many records a side judged: framed, unframed and nulls together.
/// Exact in `u128`.
#[must_use]
pub fn judged_records(side: &SideFraming) -> u128 {
    u128::from(side.framed) + u128::from(side.unframed) + u128::from(side.nulls)
}

/// One side's running count.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SideTally {
    framed: u64,
    unframed: u64,
    nulls: u64,
    ids: BTreeSet<u32>,
}

impl SideTally {
    /// Count one record's bytes on this side; `None` is a null key or a null
    /// value.
    pub fn observe(&mut self, payload: Option<&[u8]>) {
        match payload {
            None => self.nulls = self.nulls.saturating_add(1),
            Some(bytes) => match framed_schema_id(bytes) {
                Some(id) => {
                    self.framed = self.framed.saturating_add(1);
                    self.ids.insert(id);
                }
                None => self.unframed = self.unframed.saturating_add(1),
            },
        }
    }

    /// The side as the receipt records it.
    #[must_use]
    pub fn finish(&self) -> SideFraming {
        SideFraming {
            dependent: dependent_by_share(self.framed, self.unframed),
            framed: self.framed,
            unframed: self.unframed,
            nulls: self.nulls,
            schema_ids: self.ids.iter().take(SCHEMA_IDS_LISTED).copied().collect(),
            schema_id_count: self.ids.len() as u64,
        }
    }
}

/// One topic's running count: its keys and its values, over the same
/// records.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TopicTally {
    key: SideTally,
    value: SideTally,
    records: u64,
}

impl TopicTally {
    /// Count one record.
    pub fn observe(&mut self, key: Option<&[u8]>, value: Option<&[u8]>) {
        self.key.observe(key);
        self.value.observe(value);
        self.records = self.records.saturating_add(1);
    }

    /// How many records were judged.
    #[must_use]
    pub fn records(&self) -> u64 {
        self.records
    }

    /// The topic as the receipt records it. `complete` says every record the
    /// receipt counts for the topic was judged. A tally of no record is
    /// `notAssessed` with `noRecords`: nothing was judged, so nothing is
    /// claimed.
    #[must_use]
    pub fn finish(&self, complete: bool) -> TopicSchemaDependency {
        if self.records == 0 {
            return not_assessed(REASON_NO_RECORDS);
        }
        let key = self.key.finish();
        let value = self.value.finish();
        let verdict = if key.dependent || value.dependent {
            SCHEMA_DEPENDENT
        } else {
            NOT_DETECTED
        };
        TopicSchemaDependency {
            verdict: verdict.to_string(),
            reason: None,
            basis: Some(
                if complete {
                    BASIS_COMPLETE
                } else {
                    BASIS_SAMPLED
                }
                .to_string(),
            ),
            key: Some(key),
            value: Some(value),
        }
    }
}

/// A topic nothing could be judged for, and why: one of
/// [`NOT_ASSESSED_REASONS`] — [`REASON_NO_RECORDS`],
/// [`REASON_SEGMENT_UNREADABLE`], [`REASON_SEGMENT_TOO_LARGE`] or
/// [`REASON_TIME_BUDGET`].
#[must_use]
pub fn not_assessed(reason: &str) -> TopicSchemaDependency {
    TopicSchemaDependency {
        verdict: NOT_ASSESSED.to_string(),
        reason: Some(reason.to_string()),
        basis: None,
        key: None,
        value: None,
    }
}

/// The ids a DEPENDENT side names, for a surface that lists what a restore
/// needs from a registry: the dependent sides' listed ids, merged, ascending,
/// distinct, at most [`SCHEMA_IDS_LISTED`]; and whether any id was left out
/// (a side listing fewer than it counts, or a merge above the cap).
#[must_use]
pub fn dependent_ids(entry: &TopicSchemaDependency) -> (Vec<u32>, bool) {
    let mut ids = BTreeSet::new();
    let mut omitted = false;
    for side in [&entry.key, &entry.value].into_iter().flatten() {
        if side.dependent {
            ids.extend(side.schema_ids.iter().copied());
            omitted |= (side.schema_ids.len() as u64) < side.schema_id_count;
        }
    }
    omitted |= ids.len() > SCHEMA_IDS_LISTED;
    (ids.into_iter().take(SCHEMA_IDS_LISTED).collect(), omitted)
}

/// The sides that are dependent, by name (`key`, `value`), in that order.
#[must_use]
pub fn dependent_sides(entry: &TopicSchemaDependency) -> Vec<&'static str> {
    let mut out = Vec::new();
    if entry.key.as_ref().is_some_and(|s| s.dependent) {
        out.push("key");
    }
    if entry.value.as_ref().is_some_and(|s| s.dependent) {
        out.push("value");
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn framing_needs_the_magic_byte_a_plausible_id_and_a_payload() {
        assert_eq!(framed_schema_id(&[0, 0, 0, 0, 7, 2]), Some(7));
        assert_eq!(
            framed_schema_id(&[0, 0, 0xFF, 0xFF, 0xFF, 0]),
            Some(MAX_SCHEMA_ID)
        );
        // Five bytes: an id and no payload.
        assert_eq!(framed_schema_id(&[0, 0, 0, 0, 7]), None);
        // Id 0 is never issued.
        assert_eq!(framed_schema_id(&[0, 0, 0, 0, 0, 2]), None);
        // An id at 2^24 is not plausible.
        assert_eq!(framed_schema_id(&[0, 1, 0, 0, 0, 2]), None);
        // Not the magic byte.
        assert_eq!(framed_schema_id(&[1, 0, 0, 0, 7, 2]), None);
        assert_eq!(framed_schema_id(&[]), None);
    }

    #[test]
    fn the_share_is_one_in_ten_of_the_non_null_records() {
        assert!(!dependent_by_share(0, 0));
        assert!(!dependent_by_share(0, 5));
        assert!(dependent_by_share(1, 0));
        assert!(dependent_by_share(1, 9));
        assert!(!dependent_by_share(1, 10));
        assert!(dependent_by_share(10, 90));
        assert!(!dependent_by_share(9, 82));
        assert!(dependent_by_share(u64::MAX, u64::MAX));
    }
}
