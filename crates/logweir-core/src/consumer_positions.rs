//! **PROD-04.1: consumer position evidence** — the backup receipt's
//! `consumer_positions` block (format 1.5.0), the positions DOCUMENT it binds,
//! their closed vocabularies and the pure rules both the writer and the
//! readers decide with.
//!
//! # Two documents, and why
//!
//! The receipt is one signed document that the catalog reads whole (at most
//! 256 KiB) and the evidence fetch relays (at most 1 MiB). Positions grow with
//! groups × partitions, so they are NOT in it (PROD-04.1 review H1):
//!
//! - **The receipt's block** ([`ConsumerPositions`]) carries, per selected
//!   group, its outcome, type, states, members, whether it was active and its
//!   position COUNTS, the observation window, and the positions document's
//!   key, digest and length. Its size depends on the number of groups only
//!   (at most [`MAX_SELECTED_GROUPS`]), never on partitions, and
//!   [`MAX_BLOCK_BYTES`] is ENFORCED on its encoded bytes:
//!   [`refuse_selection`] refuses, by name, a selection whose block could be
//!   larger as JSON writes it ([`worst_case_block_bytes`]), escapes counted.
//! - **The positions document** ([`PositionsDocument`],
//!   `logweir/backups/<backup_id>/<run_id>.consumer-positions.json`, beside
//!   the receipt) carries every named partition's facts and every captured
//!   group's positions. It is not signed itself: the receipt's signature
//!   covers its SHA-256 and length (`document`), so a reader that fetches it
//!   verifies it against the signed receipt (arms CP-1 to CP-14,
//!   `BackupReceipt::validate_consumer_positions_document`).
//!
//! # What the receipt says per group
//!
//! A backup that names consumer groups (`source.consumer_groups`, the
//! `Backup`/`BackupSchedule` `spec.consumerGroups`, or `logweir backup run
//! --consumer-group`) records, for EVERY selected group, exactly one outcome:
//!
//! | `outcome` | `reason` | carries |
//! |---|---|---|
//! | `captured` | — | the group's type (`classic` or `consumer`), the state its listing and its description gave, its member count, whether it was `active`, and its position `counts`; its positions are in the document |
//! | `excluded` | `GroupTypeNotCaptured` (a share or streams group, a non-consumer protocol, or a type the client cannot name: every group on a broker below ListGroups v5) | `group_type: other` |
//! | `excluded` | `GroupNotFound` | nothing: the listings were complete, or a targeted describe answered the id as absent |
//! | `failed` | one of [`FAILED_REASONS`] | nothing: the group may exist and hold positions, and none is claimed |
//!
//! **Absence is never offset 0.** A group that is not `captured` carries no
//! position at all. A captured group's document entry lists every partition
//! with something to say — a committed position, a failed read, a partition
//! the capture did not observe — and COUNTS every other partition of every
//! named topic as having no committed position (`no_committed_position`), so
//! a partition is never silently missing and never read as 0 (arm CP-11).
//!
//! # Which positions relate to archived data
//!
//! Each committed position is judged against the partition's facts, which the
//! document records ONCE per partition ([`PartitionFacts`]): the log start and
//! high watermark read right after the positions (the group-capture marks,
//! READ_UNCOMMITTED), the same marks read again after the engine, and the
//! offsets the archive's manifest records for the partition. [`relation`] is
//! the one rule:
//!
//! | condition, first that holds | status / coverage |
//! |---|---|
//! | the marks were not read | `failed: MarksNotRead` (no position) |
//! | position > high watermark | `excluded: PositionBeyondEnd` (the position is kept, so a reader sees why) |
//! | position < log start | `beforeLogStart`: the records the group would read next had expired from the source |
//! | the archive holds nothing for the partition | `noArchivedData` |
//! | position < first archived offset | `beforeArchive` |
//! | position ≤ last archived offset | `withinArchive` |
//! | position = last archived offset + 1 | `atArchiveEnd`: the group had read everything archived |
//! | otherwise | `beyondArchive` |
//!
//! Only `withinArchive` and `atArchiveEnd` RELATE to archived data
//! ([`RELATED`]). Both readers re-derive every coverage word from the recorded
//! facts (arm CP-13), so a document cannot claim a relation its own numbers do
//! not support, and the receipt's counts are what the document's positions
//! say (arm CP-14). A `read_committed` group fully caught up past a trailing
//! transaction marker sits at the last archived offset + 2 and reads
//! `beyondArchive`: an under-claim, on the safe side.
//!
//! # Positions are not atomic with the records
//!
//! The groups are read BEFORE the engine starts (`observed_from` ..
//! `observed_to`), while applications may be running: an active group's
//! positions can move after they were read, and the engine reads records
//! after that. The block says which groups were active, and when the positions
//! were observed; it never claims a consistent cut across groups and records.
//!
//! # A topic that changed during the capture
//!
//! A topic whose marks after the engine REGRESS against the group-capture
//! marks (a log start or a high watermark moved backwards) is
//! `changed_during_capture`, and no group may claim a position on it: a group
//! holding one is `failed: GenerationChangedDuringCapture` (PROD-01.4
//! TI-04.1-3, arm CP-9), never captured. Detection is a REGRESSION of marks
//! only: a topic recreated and refilled past its old marks before the read
//! after the engine is not seen, and a read after the engine that failed
//! decides nothing. Topic identity (PROD-01.4a) is what closes that gap.
//!
//! # A topic whose partitions were never read
//!
//! A named topic none of the three reads (the capture, the read after the
//! engine, the archive) gave a partition for records `partitions: []`. A group
//! cannot be captured over it — its positions there are unknown, and absence
//! would read as "no partition" — so every group that would have been is
//! `failed: PartitionsNotRead` (arm CP-10, PROD-04.1 review L4).
//!
//! # Generation
//!
//! The receipt records no topic generation token and no topic ID yet (PROD-02.1
//! and PROD-01.4a). Until it does, every snapshot relates to "generation
//! unknown" (PROD-01.4 TI-04.1-4): its positions are about THIS point's data
//! only, through the marks above, and never about another point's.

use chrono::{DateTime, Utc};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// `GroupSnapshot::outcome`'s closed set (arm 33).
pub const GROUP_OUTCOMES: [&str; 3] = ["captured", "excluded", "failed"];

/// `GroupSnapshot::reason`'s closed set for an `excluded` group (arm 33).
pub const EXCLUDED_REASONS: [&str; 2] = ["GroupTypeNotCaptured", "GroupNotFound"];

/// `GroupSnapshot::reason`'s closed set for a `failed` group (arm 33).
///
/// - `NotVisibleToPrincipal`: no listing shows it and a targeted call was
///   refused GROUP_AUTHORIZATION_FAILED; it may exist (PROD-04.0 §5, T14).
/// - `NotVisibleOrUnreachable`: a bounded call got no answer for a group no
///   listing shows.
/// - `NotAuthorized`: a listed group whose description or positions this
///   principal may not read.
/// - `PositionsUnstable`: the bounded RequireStable fetch got no answer (a
///   pending transactional offset commit, or the coordinator).
/// - `ListingInconsistent`, `AbsenceUnproven`, `TypeUnproven`, `Unreachable`:
///   PROD-04.0b's classification failures.
/// - `DescribeFailed`, `PositionsFailed`: the description or the fetch failed
///   for another reason.
/// - `GenerationChangedDuringCapture`: the group held a position on a topic
///   that changed during the capture (arm CP-9).
/// - `CaptureUnavailable`: the run's reader could not read consumer groups.
/// - `GroupVanishedDuringCapture`: its description answered `Dead` with no
///   member — the stand-in for "no such group" (PROD-04.0 T2) — so it was
///   deleted, or its offsets expired, while it was being read (arm 34).
/// - `PartitionsNotRead`: a named topic's partitions were never read, so its
///   positions there are unknown (arm CP-10).
pub const FAILED_REASONS: [&str; 14] = [
    "NotVisibleToPrincipal",
    "NotVisibleOrUnreachable",
    "NotAuthorized",
    "PositionsUnstable",
    "ListingInconsistent",
    "AbsenceUnproven",
    "TypeUnproven",
    "Unreachable",
    "DescribeFailed",
    "PositionsFailed",
    "GenerationChangedDuringCapture",
    "CaptureUnavailable",
    "GroupVanishedDuringCapture",
    "PartitionsNotRead",
];

/// The reason a group holding a position on a changed topic fails with.
pub const GENERATION_CHANGED: &str = "GenerationChangedDuringCapture";

/// The reason a group described `Dead` with no member fails with.
pub const GROUP_VANISHED: &str = "GroupVanishedDuringCapture";

/// The reason a group fails with when a named topic's partitions were never
/// read.
pub const PARTITIONS_NOT_READ: &str = "PartitionsNotRead";

/// The reason an excluded group of a type Logweir does not capture carries.
pub const GROUP_TYPE_NOT_CAPTURED: &str = "GroupTypeNotCaptured";

/// The group types a `captured` group carries (arm 34).
pub const CAPTURED_TYPES: [&str; 2] = ["classic", "consumer"];

/// The type an `excluded: GroupTypeNotCaptured` group carries (arm 34).
pub const OTHER_TYPE: &str = "other";

/// `GroupSnapshot::state` and `listed_state`'s closed set (arm 34): the states
/// librdkafka names, and `stateUnknownToClient` for any other (KIP-848's
/// `Assigning` and `Reconciling` among them), which counts as active.
pub const GROUP_STATES: [&str; 6] = [
    "PreparingRebalance",
    "CompletingRebalance",
    "Stable",
    "Dead",
    "Empty",
    "stateUnknownToClient",
];

/// The states that say a group has no member. Every other state, the unknown
/// one included, is active (PROD-04.0 T4).
pub const INACTIVE_STATES: [&str; 2] = ["Empty", "Dead"];

/// The state a group being removed is described in. A captured group is never
/// `Dead` with no member (arm 34).
pub const DEAD_STATE: &str = "Dead";

/// `PositionEntry::status`'s closed set (arm CP-12). A partition with no
/// committed offset has NO entry: the group's `no_committed_position` counts
/// it.
pub const POSITION_STATUSES: [&str; 4] = ["captured", "excluded", "failed", "notObserved"];

/// The reason an `excluded` position carries: its offset is above the
/// partition's high watermark at capture (PROD-01.4 TI-04.1-2).
pub const POSITION_BEYOND_END: &str = "PositionBeyondEnd";

/// `PositionEntry::reason`'s closed set for a `failed` position (arm CP-12).
pub const POSITION_FAILED_REASONS: [&str; 5] = [
    "TopicNotAuthorized",
    "Unstable",
    "NotAPosition",
    "PartitionFailed",
    "MarksNotRead",
];

/// The reason a committed position whose partition marks were not read fails
/// with: without them it cannot be judged against the partition's end.
pub const MARKS_NOT_READ: &str = "MarksNotRead";

/// `PositionEntry::reason`'s closed set for a `notObserved` position (arm
/// CP-12): the partition was added after the group capture read the topic's
/// partitions, or that read failed for the topic.
pub const NOT_OBSERVED_REASONS: [&str; 2] = ["PartitionAddedDuringCapture", "TopicNotObserved"];

/// `PositionEntry::coverage`'s closed set (arm CP-13), in [`relation`]'s order.
pub const COVERAGE_RELATIONS: [&str; 6] = [
    "beforeLogStart",
    "noArchivedData",
    "beforeArchive",
    "withinArchive",
    "atArchiveEnd",
    "beyondArchive",
];

/// The coverage words that RELATE a position to archived data.
pub const RELATED: [&str; 2] = ["withinArchive", "atArchiveEnd"];

/// `ConsumerPositions::listing`'s closed set (arm 31).
pub const LISTING_VALUES: [&str; 2] = ["complete", "notComplete"];

/// **The most groups one backup may select.** A larger selection is refused
/// before anything runs ([`SELECTION_TOO_LARGE`]: phase −1 exits 3, the
/// controller refuses the spec, and the CRDs' `maxItems` is the same number):
/// the receipt records one summary per group, and [`MAX_BLOCK_BYTES`] is the
/// bound that number buys.
pub const MAX_SELECTED_GROUPS: usize = 100;

/// The longest group id a selection may name, in BYTES of UTF-8 (an ASCII id
/// of 255 characters; fewer characters outside ASCII). Bytes, not
/// characters, because the bound below is over the receipt's bytes.
pub const MAX_GROUP_ID_BYTES: usize = 255;

/// **The receipt block's size cap, ENFORCED on its encoded bytes** (PROD-04.1
/// review N1). [`refuse_selection`] measures the block a selection could
/// produce AFTER JSON encoding ([`worst_case_block_bytes`]: every group
/// captured, every field at its longest) and refuses one over this, by name
/// ([`SELECTION_TOO_LARGE`]) and before anything runs — never a truncated
/// block. Measured, not assumed: an id's characters count as JSON writes them,
/// so `"` and `\` count two bytes each. Under a third of the catalog's 256 KiB
/// read cap, whatever the number of partitions. 100 groups of 255-byte ids
/// that need no escape fit (71,236 bytes at worst, four-byte characters
/// included); ids of 255 `"` or `\` fit 84 to a selection (81,220 bytes at
/// worst), and the 85th is refused (82,171).
pub const MAX_BLOCK_BYTES: usize = 80 * 1024;

/// The longest document key [`worst_case_block_bytes`] assumes: an object
/// key's limit in S3, so no real `<backup_id>/<run_id>` key is longer.
const WORST_CASE_DOCUMENT_KEY_BYTES: usize = 1024;

/// **The encoded size of the largest block `selected` can produce**: the
/// deterministic JSON (the receipt's own encoding) of a block in which every
/// selected group is captured with its longest type and states, `u32::MAX`
/// members and counts, beside the longest capture window and document
/// reference. A real block of the same selection is never larger: a group
/// that is not captured carries fewer fields, and every other value is a
/// word from a closed set or a bounded number. Measured AFTER encoding, so an
/// id's escapes count as written — the review's N1, where `"` and `\` made
/// the block larger than the bound it claimed.
#[must_use]
pub fn worst_case_block_bytes(selected: &[String]) -> usize {
    // 2038-01-19T03:14:07.999999999Z: a four-digit year with nine fraction
    // digits, as long as any instant a capture records. In range, so the
    // default (never taken) is only there to keep the pure layer clock-free.
    let at = DateTime::<Utc>::from_timestamp(i64::from(i32::MAX), 999_999_999).unwrap_or_default();
    let counts = PositionCounts {
        related: u32::MAX,
        not_related: u32::MAX,
        never_committed: u32::MAX,
        beyond_end: u32::MAX,
        failed: u32::MAX,
        not_observed: u32::MAX,
    };
    let block = ConsumerPositions {
        observed_from: at,
        observed_to: at,
        listing: "notComplete".to_string(),
        document: DocumentRef {
            key: "x".repeat(WORST_CASE_DOCUMENT_KEY_BYTES),
            sha256: format!("sha256:{}", "f".repeat(64)),
            bytes: u64::MAX,
        },
        groups: selected
            .iter()
            .map(|id| {
                (
                    id.clone(),
                    GroupSnapshot {
                        outcome: "captured".to_string(),
                        reason: None,
                        group_type: Some("consumer".to_string()),
                        state: Some("stateUnknownToClient".to_string()),
                        listed_state: Some("stateUnknownToClient".to_string()),
                        members: Some(u32::MAX),
                        active: Some(true),
                        counts: Some(counts),
                    },
                )
            })
            .collect(),
    };
    crate::det_json::to_deterministic_json(&block).map_or(usize::MAX, |bytes| bytes.len())
}

/// The named refusal of a selection over [`MAX_SELECTED_GROUPS`] groups.
pub const SELECTION_TOO_LARGE: &str = "ConsumerGroupSelectionTooLarge";

/// The named refusal of a blank id, one with a control character, or one
/// longer than [`MAX_GROUP_ID_BYTES`].
pub const SELECTION_ID_INVALID: &str = "ConsumerGroupIdInvalid";

/// The named refusal of an id selected twice.
pub const SELECTION_REPEATED: &str = "ConsumerGroupSelectedTwice";

/// The positions document's own format. Major 1; arm CP-3 refuses any other.
pub const DOCUMENT_FORMAT_VERSION: &str = "1.0.0";

/// Where a run's positions document lives: beside its receipt
/// (`logweir/backups/<backup_id>/<run_id>.receipt.json`). Arm 24 holds the
/// receipt's `document.key` to exactly this.
#[must_use]
pub fn document_key(backup_id: &str, run_id: &str) -> String {
    format!("logweir/backups/{backup_id}/{run_id}.consumer-positions.json")
}

/// **Receipt format 1.5.0.** The consumer position evidence of one backup:
/// per selected group its outcome and counts, and the positions document
/// that carries the positions themselves.
///
/// Field order is byte order (`det_json`); appended fields only.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct ConsumerPositions {
    /// When the group capture started: before the listings, the
    /// descriptions and the positions.
    pub observed_from: DateTime<Utc>,
    /// When it ended, after the group-capture marks and BEFORE the engine
    /// started. At or after `observed_from` (arm 31).
    pub observed_to: DateTime<Utc>,
    /// `complete` when the group listings were complete (Describe on the
    /// cluster, no listing error), else `notComplete`: an unlisted id was then
    /// classified by a targeted describe (PROD-04.0 §5).
    pub listing: String,
    /// The positions document this receipt binds (arm 32).
    pub document: DocumentRef,
    /// One entry per selected group id, keyed by it. Never empty (arm 31): a
    /// backup that selects no group carries no block.
    pub groups: BTreeMap<String, GroupSnapshot>,
}

/// The positions document a receipt binds: where it is, and the digest and
/// length of its exact bytes. The receipt's signature covers these, so the
/// document needs none of its own.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct DocumentRef {
    /// [`document_key`] of the receipt's own backup and run.
    pub key: String,
    /// `sha256:<64 lowercase hex>` over the document's bytes.
    pub sha256: String,
    /// The document's length in bytes, at least 1.
    pub bytes: u64,
}

/// One selected group's outcome, as the receipt records it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct GroupSnapshot {
    /// [`GROUP_OUTCOMES`].
    pub outcome: String,
    /// Present exactly when `outcome` is not `captured`: [`EXCLUDED_REASONS`]
    /// or [`FAILED_REASONS`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    /// `classic` or `consumer` for a captured group; `other` for one excluded
    /// `GroupTypeNotCaptured`; absent otherwise.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub group_type: Option<String>,
    /// The state the group's DESCRIPTION gave (captured only).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub state: Option<String>,
    /// The state the group's LISTING gave, read before the description
    /// (captured only). The two differ when the group changed between them.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub listed_state: Option<String>,
    /// The members the description listed (captured only).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub members: Option<u32>,
    /// Whether either state says the group had members, so its positions may
    /// have moved after they were read ([`active`], arm 35; captured only).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub active: Option<bool>,
    /// Its positions, counted by what they say about archived data, over
    /// every partition of every named topic (captured only; arm 34). The
    /// positions themselves are the document's, and arm CP-14 holds these
    /// counts to them.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub counts: Option<PositionCounts>,
}

/// **The positions document** (`<run_id>.consumer-positions.json`, format
/// 1.0.0): every named partition's facts and every captured group's
/// positions. Bound to its receipt by the receipt's `document` (arm CP-2).
///
/// Field order is byte order (`det_json`); appended fields only.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct PositionsDocument {
    /// [`DOCUMENT_FORMAT_VERSION`].
    pub format_version: String,
    /// The receipt's `backup_id` (arm CP-3).
    pub backup_id: String,
    /// The receipt's `run_id` (arm CP-3).
    pub run_id: String,
    /// Per named topic, the facts of each partition. Exactly `source.topics`
    /// (arm CP-4).
    pub topics: BTreeMap<String, TopicPartitions>,
    /// One entry per CAPTURED group, keyed by its id (arm CP-8).
    pub groups: BTreeMap<String, GroupPositions>,
}

/// One named topic's partitions, as the capture saw them.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct TopicPartitions {
    /// One entry per partition, `partition` equal to its index (arm CP-5): the
    /// partitions the group capture read, then any the read after the engine
    /// or the archive shows beyond them. EMPTY only when no read gave one,
    /// and then no group is captured (arm CP-10).
    pub partitions: Vec<PartitionFacts>,
    /// Whether any partition's marks after the engine regress against its
    /// group-capture marks ([`changed_during_capture`], arm CP-7).
    pub changed_during_capture: bool,
}

/// One partition's facts. Every mark is ABSENT when it was not read, never 0.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct PartitionFacts {
    pub partition: u32,
    /// Whether the group capture read this partition: `false` for one added
    /// after it read the topic's partitions (or when that read failed), whose
    /// positions were therefore never asked for.
    pub observed: bool,
    /// The log start offset read right after the positions (READ_UNCOMMITTED).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub log_start: Option<i64>,
    /// The high watermark read with `log_start`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub high_watermark: Option<i64>,
    /// The log start offset read again after the engine.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub log_start_after: Option<i64>,
    /// The high watermark read again after the engine.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub high_watermark_after: Option<i64>,
    /// The lowest offset the archive's manifest records for the partition.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub archived_first: Option<i64>,
    /// The highest offset the archive's manifest records for the partition
    /// (inclusive).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub archived_last: Option<i64>,
}

/// One captured group's positions, SPARSE: an entry for each partition with
/// something to say, and a count of the rest.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct GroupPositions {
    /// Topics in name order, partitions in order (arm CP-11): every committed
    /// position, every failed read, and every partition the capture did not
    /// observe.
    pub positions: Vec<PositionEntry>,
    /// How many partitions of the named topics have NO committed offset:
    /// exactly every partition `positions` does not list (arm CP-11). Never
    /// read as offset 0.
    pub no_committed_position: u32,
}

/// One partition's position for one captured group.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct PositionEntry {
    pub topic: String,
    pub partition: u32,
    /// [`POSITION_STATUSES`].
    pub status: String,
    /// The committed next-to-consume offset: present exactly when `status` is
    /// `captured` or `excluded`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub position: Option<i64>,
    /// Present exactly when `status` is `excluded`, `failed` or `notObserved`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    /// [`COVERAGE_RELATIONS`], present exactly when `status` is `captured`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub coverage: Option<String>,
}

/// What [`relation`] decides for one committed position.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Relation {
    /// The group-capture marks were not read: `failed: MarksNotRead`.
    MarksNotRead,
    /// Above the high watermark: `excluded: PositionBeyondEnd`.
    BeyondEnd,
    /// `captured`, with this coverage word.
    Coverage(&'static str),
}

/// **The one rule** for a committed position against its partition's facts
/// (module doc's table). Pure; the writer and arm CP-13 both decide with it,
/// and `docs/verify_scorecard.py` mirrors it.
#[must_use]
pub fn relation(position: i64, facts: &PartitionFacts) -> Relation {
    let (Some(log_start), Some(high)) = (facts.log_start, facts.high_watermark) else {
        return Relation::MarksNotRead;
    };
    if position > high {
        return Relation::BeyondEnd;
    }
    if position < log_start {
        return Relation::Coverage("beforeLogStart");
    }
    let (Some(first), Some(last)) = (facts.archived_first, facts.archived_last) else {
        return Relation::Coverage("noArchivedData");
    };
    if position < first {
        Relation::Coverage("beforeArchive")
    } else if position <= last {
        Relation::Coverage("withinArchive")
    } else if position == last.saturating_add(1) {
        Relation::Coverage("atArchiveEnd")
    } else {
        Relation::Coverage("beyondArchive")
    }
}

/// Whether a topic changed during the capture: some partition with both pairs
/// of marks has a log start or a high watermark after the engine BELOW the one
/// read at group capture. Kafka never moves either backwards on one topic
/// generation, so a regression is a recreation or a truncation (PROD-01.4
/// §4.4). A recreation refilled past the old marks is not a regression, and
/// an unread pair decides nothing (the module doc's limit).
#[must_use]
pub fn changed_during_capture(partitions: &[PartitionFacts]) -> bool {
    partitions.iter().any(|p| {
        let start = matches!((p.log_start, p.log_start_after), (Some(b), Some(a)) if a < b);
        let end = matches!((p.high_watermark, p.high_watermark_after), (Some(b), Some(a)) if a < b);
        start || end
    })
}

/// Whether a captured group was active: either of its two states is not one
/// of [`INACTIVE_STATES`]. A state the client could not name is active.
#[must_use]
pub fn active(state: &str, listed_state: &str) -> bool {
    !INACTIVE_STATES.contains(&state) || !INACTIVE_STATES.contains(&listed_state)
}

/// Whether a description answers "no such group": `Dead` with no member
/// (PROD-04.0 T2). A group described so is never captured (arm 34).
#[must_use]
pub fn vanished(state: &str, members: u32) -> bool {
    state == DEAD_STATE && members == 0
}

impl ConsumerPositions {
    /// `sha256:<hex>` over the block's deterministic JSON: the digest the
    /// catalog point record binds (`consumer_positions.sha256`), recomputed
    /// by a reader from the verified receipt. The block carries the positions
    /// document's own digest, so this binds the positions too.
    ///
    /// # Errors
    ///
    /// The block does not serialise (it always does: strings, integers and
    /// booleans).
    pub fn digest(&self) -> Result<String, String> {
        crate::det_json::to_deterministic_json(self)
            .map(|bytes| crate::ids::sha256_prefixed(&bytes))
            .map_err(|e| format!("the consumer_positions block does not serialise: {e}"))
    }

    /// The groups whose outcome is `captured`.
    pub fn captured(&self) -> impl Iterator<Item = (&String, &GroupSnapshot)> {
        self.groups.iter().filter(|(_, g)| g.outcome == "captured")
    }
}

impl PositionsDocument {
    /// The document's exact bytes: deterministic JSON, the bytes the receipt's
    /// `document.sha256` is over.
    ///
    /// # Errors
    ///
    /// The document does not serialise (it always does).
    pub fn to_bytes(&self) -> Result<Vec<u8>, String> {
        crate::det_json::to_deterministic_json(self)
            .map_err(|e| format!("the positions document does not serialise: {e}"))
    }

    /// The number of partitions of the named topics: what a captured group's
    /// entries and `no_committed_position` together account for.
    #[must_use]
    pub fn partition_count(&self) -> u64 {
        self.topics
            .values()
            .map(|t| t.partitions.len() as u64)
            .sum()
    }
}

/// One captured group's positions, counted by what they say about archived
/// data: the summary the receipt, the catalog point record, the catalog's
/// view, the product API and both receipt readers' lines carry.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct PositionCounts {
    /// Captured, with a coverage in [`RELATED`].
    pub related: u32,
    /// Captured, with any other coverage word.
    pub not_related: u32,
    /// No committed offset (the document's `no_committed_position`).
    pub never_committed: u32,
    /// `excluded: PositionBeyondEnd`.
    pub beyond_end: u32,
    /// `failed`.
    pub failed: u32,
    /// `notObserved`.
    pub not_observed: u32,
}

impl PositionCounts {
    /// The counts of one captured group's document entry.
    #[must_use]
    pub fn of(group: &GroupPositions) -> Self {
        let mut c = Self {
            never_committed: group.no_committed_position,
            ..Self::default()
        };
        for p in &group.positions {
            let slot = match p.status.as_str() {
                "captured" if p.coverage.as_deref().is_some_and(|w| RELATED.contains(&w)) => {
                    &mut c.related
                }
                "captured" => &mut c.not_related,
                "excluded" => &mut c.beyond_end,
                "failed" => &mut c.failed,
                _ => &mut c.not_observed,
            };
            *slot = slot.saturating_add(1);
        }
        c
    }

    /// Every partition counted: what a captured group accounts for.
    #[must_use]
    pub fn total(&self) -> u64 {
        [
            self.related,
            self.not_related,
            self.never_committed,
            self.beyond_end,
            self.failed,
            self.not_observed,
        ]
        .iter()
        .map(|n| u64::from(*n))
        .sum()
    }

    /// The counts as both readers' refusals and lines spell them.
    #[must_use]
    pub fn render(&self) -> String {
        format!(
            "{} related, {} not related, {} never committed, {} beyond the end, {} failed, {} \
             not observed",
            self.related,
            self.not_related,
            self.never_committed,
            self.beyond_end,
            self.failed,
            self.not_observed
        )
    }
}

/// Why a selection is refused before anything runs, or `None`: more than
/// [`MAX_SELECTED_GROUPS`] ids ([`SELECTION_TOO_LARGE`]); a blank id, an id
/// with a control character or longer than [`MAX_GROUP_ID_BYTES`] bytes
/// ([`SELECTION_ID_INVALID`]); the same id twice ([`SELECTION_REPEATED`]);
/// and a selection whose block could be over [`MAX_BLOCK_BYTES`] as JSON
/// writes it ([`SELECTION_TOO_LARGE`], [`worst_case_block_bytes`]). The
/// message starts with the refusal's name.
#[must_use]
pub fn refuse_selection(selected: &[String]) -> Option<String> {
    if selected.len() > MAX_SELECTED_GROUPS {
        return Some(format!(
            "{SELECTION_TOO_LARGE}: the backup selects {} consumer groups and at most \
             {MAX_SELECTED_GROUPS} may be selected",
            selected.len()
        ));
    }
    let mut seen = std::collections::BTreeSet::new();
    for id in selected {
        if id.trim().is_empty() {
            return Some(format!(
                "{SELECTION_ID_INVALID}: a selected consumer group id is blank"
            ));
        }
        if id.chars().any(char::is_control) || id.len() > MAX_GROUP_ID_BYTES {
            return Some(format!(
                "{SELECTION_ID_INVALID}: the selected consumer group id {:?} carries a control \
                 character or is longer than {MAX_GROUP_ID_BYTES} bytes",
                id.chars().take(64).collect::<String>()
            ));
        }
        if !seen.insert(id.as_str()) {
            return Some(format!(
                "{SELECTION_REPEATED}: the consumer group {id:?} is selected twice; each group is \
                 selected once"
            ));
        }
    }
    let encoded = worst_case_block_bytes(selected);
    if encoded > MAX_BLOCK_BYTES {
        return Some(format!(
            "{SELECTION_TOO_LARGE}: the receipt's summary of these {} consumer groups could be \
             {encoded} bytes as JSON writes it (each id with its escapes, every group \
             captured), over the {MAX_BLOCK_BYTES}-byte cap; select fewer groups or shorter ids",
            selected.len()
        ));
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    fn facts(ls: Option<i64>, hw: Option<i64>, archived: Option<(i64, i64)>) -> PartitionFacts {
        PartitionFacts {
            partition: 0,
            observed: true,
            log_start: ls,
            high_watermark: hw,
            log_start_after: None,
            high_watermark_after: None,
            archived_first: archived.map(|a| a.0),
            archived_last: archived.map(|a| a.1),
        }
    }

    /// Every row of the module doc's table, each boundary on both sides.
    #[test]
    fn the_relation_follows_the_table_at_every_boundary() {
        let f = facts(Some(5), Some(20), Some((8, 15)));
        let c = |w: &'static str| Relation::Coverage(w);
        assert_eq!(relation(21, &f), Relation::BeyondEnd);
        assert_eq!(
            relation(20, &f),
            c("beyondArchive"),
            "the end itself is not beyond it"
        );
        assert_eq!(relation(17, &f), c("beyondArchive"));
        assert_eq!(relation(16, &f), c("atArchiveEnd"));
        assert_eq!(relation(15, &f), c("withinArchive"));
        assert_eq!(relation(8, &f), c("withinArchive"));
        assert_eq!(relation(7, &f), c("beforeArchive"));
        assert_eq!(relation(5, &f), c("beforeArchive"));
        assert_eq!(relation(4, &f), c("beforeLogStart"));
        assert_eq!(relation(0, &f), c("beforeLogStart"));
        let empty = facts(Some(5), Some(20), None);
        assert_eq!(relation(10, &empty), c("noArchivedData"));
        assert_eq!(relation(4, &empty), c("beforeLogStart"));
        assert_eq!(relation(21, &empty), Relation::BeyondEnd);
        assert_eq!(
            relation(3, &facts(None, Some(20), None)),
            Relation::MarksNotRead
        );
        assert_eq!(
            relation(3, &facts(Some(0), None, None)),
            Relation::MarksNotRead
        );
        // A committed 0 on an empty partition (end 0): at the end, not beyond.
        assert_eq!(
            relation(0, &facts(Some(0), Some(0), None)),
            c("noArchivedData")
        );
        // Every word the rule can return is in the closed set, and only two relate.
        for w in COVERAGE_RELATIONS {
            assert_eq!(
                RELATED.contains(&w),
                w == "withinArchive" || w == "atArchiveEnd"
            );
        }
    }

    #[test]
    fn a_topic_changed_only_when_a_mark_regresses() {
        let mut p = facts(Some(5), Some(20), None);
        p.log_start_after = Some(5);
        p.high_watermark_after = Some(25);
        assert!(!changed_during_capture(std::slice::from_ref(&p)));
        p.high_watermark_after = Some(19);
        assert!(changed_during_capture(std::slice::from_ref(&p)));
        p.high_watermark_after = Some(20);
        p.log_start_after = Some(4);
        assert!(changed_during_capture(std::slice::from_ref(&p)));
        // An unread pair decides nothing.
        p.log_start_after = None;
        p.high_watermark_after = None;
        assert!(!changed_during_capture(std::slice::from_ref(&p)));
        p.log_start = None;
        p.high_watermark = None;
        p.log_start_after = Some(0);
        p.high_watermark_after = Some(0);
        assert!(!changed_during_capture(&[p]));
    }

    #[test]
    fn only_empty_and_dead_are_inactive_on_both_reads() {
        assert!(!active("Empty", "Empty"));
        assert!(!active("Dead", "Empty"));
        assert!(
            active("Stable", "Empty"),
            "a member joined after the listing"
        );
        assert!(
            active("Empty", "PreparingRebalance"),
            "a member left after the listing"
        );
        assert!(active("stateUnknownToClient", "Empty"));
        for s in GROUP_STATES {
            assert_eq!(active(s, s), !INACTIVE_STATES.contains(&s));
        }
    }

    /// M2: `Dead` with no member is the absent stand-in; `Dead` with a member
    /// and `Empty` with none are not.
    #[test]
    fn only_dead_with_no_member_is_a_vanished_group() {
        assert!(vanished("Dead", 0));
        assert!(!vanished("Dead", 1));
        assert!(!vanished("Empty", 0));
        assert!(!vanished("Stable", 0));
    }

    #[test]
    fn a_selection_is_refused_for_blank_control_long_repeated_or_too_many_ids_by_name() {
        let ids = |v: &[&str]| v.iter().map(|s| s.to_string()).collect::<Vec<_>>();
        assert_eq!(refuse_selection(&ids(&["a", "b"])), None);
        let named = |r: Option<String>, name: &str| {
            let r = r.expect("refused");
            assert!(r.starts_with(&format!("{name}: ")), "{r}");
        };
        named(refuse_selection(&ids(&[" "])), SELECTION_ID_INVALID);
        named(refuse_selection(&ids(&["a\nb"])), SELECTION_ID_INVALID);
        named(refuse_selection(&ids(&["a", "a"])), SELECTION_REPEATED);
        assert!(refuse_selection(&[("x".repeat(MAX_GROUP_ID_BYTES))]).is_none());
        named(
            refuse_selection(&[("x".repeat(MAX_GROUP_ID_BYTES + 1))]),
            SELECTION_ID_INVALID,
        );
        // Bytes, not characters: 64 four-byte characters are 256 bytes.
        named(
            refuse_selection(&["\u{1F600}".repeat(64)]),
            SELECTION_ID_INVALID,
        );
        assert!(refuse_selection(&["\u{1F600}".repeat(63)]).is_none());
        let many: Vec<String> = (0..=MAX_SELECTED_GROUPS).map(|i| format!("g{i}")).collect();
        named(refuse_selection(&many), SELECTION_TOO_LARGE);
        assert!(refuse_selection(&many[..MAX_SELECTED_GROUPS]).is_none());
    }

    /// **The cap is enforced on the ENCODED bytes (review N1).** Ids that need
    /// no escape fit at the most groups and the longest ids: 100 ids of 255
    /// bytes of four-byte characters (71,236 bytes at worst). Ids of `"` or
    /// `\` double as JSON writes them: the review's construction (100 ids of
    /// 255) is refused by name, and AT the cap the largest such selection (84
    /// ids) is accepted and one more id is refused — never truncated. Raw
    /// lengths would have admitted it. Every accepted selection's worst case
    /// is within the cap, a third of the catalog's read cap.
    #[test]
    fn the_cap_is_enforced_on_the_encoded_bytes_with_escape_heavy_ids_at_and_over_it() {
        let ids = |n: usize, ch: &str, per: usize| -> Vec<String> {
            (0..n)
                .map(|i| format!("{i:03}{}", ch.repeat((MAX_GROUP_ID_BYTES - 3) / per)))
                .collect()
        };
        // No escape: the most groups of the longest ids fit.
        let emoji = ids(MAX_SELECTED_GROUPS, "\u{1F600}", 4);
        assert!(emoji.iter().all(|i| i.len() <= MAX_GROUP_ID_BYTES));
        assert!(worst_case_block_bytes(&emoji) <= MAX_BLOCK_BYTES);
        assert_eq!(refuse_selection(&emoji), None);
        for ch in ["\"", "\\"] {
            // The review's construction: 100 ids of 255 escape-doubled bytes.
            let all = ids(MAX_SELECTED_GROUPS, ch, 1);
            assert!(all.iter().all(|i| i.len() == MAX_GROUP_ID_BYTES));
            let encoded = worst_case_block_bytes(&all);
            assert!(encoded > MAX_BLOCK_BYTES, "{encoded}");
            let refused = refuse_selection(&all).expect("refused");
            assert!(
                refused.starts_with(&format!("{SELECTION_TOO_LARGE}: ")),
                "{refused}"
            );
            assert!(
                refused.contains(&format!("{encoded} bytes as JSON writes it")),
                "{refused}"
            );
            // AT the cap: the largest such selection fits, one id more does
            // not.
            let at = (1..=MAX_SELECTED_GROUPS)
                .take_while(|&n| worst_case_block_bytes(&all[..n]) <= MAX_BLOCK_BYTES)
                .last()
                .expect("one id fits");
            assert_eq!(at, 84, "{ch}: the documented count");
            assert_eq!(refuse_selection(&all[..at]), None, "{ch} x {at} fits");
            let over = refuse_selection(&all[..=at]).expect("one more is refused");
            assert!(
                over.starts_with(&format!("{SELECTION_TOO_LARGE}: ")),
                "{over}"
            );
            // What the cap measures is the encoding: the raw bytes of the
            // refused selection are far under it.
            let raw: usize = all[..=at].iter().map(String::len).sum();
            assert!(raw < MAX_BLOCK_BYTES, "{raw}");
        }
        // A real block of the same ids is never larger than the worst case.
        let real = ConsumerPositions {
            observed_from: DateTime::<Utc>::from_timestamp(1_700_000_000, 123_456_789).unwrap(),
            observed_to: DateTime::<Utc>::from_timestamp(1_700_000_001, 0).unwrap(),
            listing: "complete".into(),
            document: DocumentRef {
                key: document_key("backup", "run"),
                sha256: format!("sha256:{}", "0".repeat(64)),
                bytes: 1,
            },
            groups: ids(3, "\"", 1)
                .into_iter()
                .map(|id| {
                    (
                        id,
                        GroupSnapshot {
                            outcome: "failed".into(),
                            reason: Some("GenerationChangedDuringCapture".into()),
                            group_type: None,
                            state: None,
                            listed_state: None,
                            members: None,
                            active: None,
                            counts: None,
                        },
                    )
                })
                .collect(),
        };
        let real_bytes = crate::det_json::to_deterministic_json(&real).unwrap().len();
        let real_ids: Vec<String> = real.groups.keys().cloned().collect();
        assert!(real_bytes <= worst_case_block_bytes(&real_ids));
        const { assert!(MAX_BLOCK_BYTES * 3 <= 256 * 1024) };
    }

    /// The counts of a sparse entry: the listed entries by status and
    /// coverage, and every unlisted partition never committed.
    #[test]
    fn counts_take_every_unlisted_partition_as_never_committed() {
        let e = |status: &str, coverage: Option<&str>| PositionEntry {
            topic: "t".into(),
            partition: 0,
            status: status.into(),
            position: None,
            reason: None,
            coverage: coverage.map(str::to_string),
        };
        let c = PositionCounts::of(&GroupPositions {
            positions: vec![
                e("captured", Some("withinArchive")),
                e("captured", Some("atArchiveEnd")),
                e("captured", Some("beyondArchive")),
                e("excluded", None),
                e("failed", None),
                e("notObserved", None),
            ],
            no_committed_position: 4,
        });
        assert_eq!(
            c,
            PositionCounts {
                related: 2,
                not_related: 1,
                never_committed: 4,
                beyond_end: 1,
                failed: 1,
                not_observed: 1
            }
        );
        assert_eq!(c.total(), 10);
        assert_eq!(
            c.render(),
            "2 related, 1 not related, 4 never committed, 1 beyond the end, 1 failed, 1 not \
             observed"
        );
    }
}
