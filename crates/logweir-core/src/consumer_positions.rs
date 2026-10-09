//! **PROD-04.1: consumer position evidence** — the backup receipt's
//! `consumer_positions` block (format 1.5.0), its closed vocabularies and the
//! pure rules both the writer and the receipt's arms 22–34 decide with.
//!
//! # What the block says
//!
//! A backup that names consumer groups (`source.consumer_groups`, the
//! `Backup`/`BackupSchedule` `spec.consumerGroups`, or `logweir backup run
//! --consumer-group`) records, for EVERY selected group, exactly one outcome:
//!
//! | `outcome` | `reason` | carries |
//! |---|---|---|
//! | `captured` | — | the group's type (`classic` or `consumer`), the state its listing and its description gave, its member count, whether it was `active`, and one position entry per partition of every named topic |
//! | `excluded` | `GroupTypeNotCaptured` (a share or streams group, a non-consumer protocol, or a type the client cannot name: every group on a broker below ListGroups v5) | `group_type: other` |
//! | `excluded` | `GroupNotFound` | nothing: the listings were complete, or a targeted describe answered the id as absent |
//! | `failed` | one of [`FAILED_REASONS`] | nothing: the group may exist and hold positions, and none is claimed |
//!
//! **Absence is never offset 0.** A group that is not `captured` carries no
//! position at all, and a captured group carries one entry for every
//! partition of every named topic, so a partition is never silently missing:
//! a partition with no committed offset is `noCommittedPosition`, never 0.
//!
//! # Which positions relate to archived data
//!
//! Each captured position is judged against the partition's facts, which the
//! block records ONCE per partition ([`PartitionFacts`]): the log start and
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
//! ([`RELATED`]). Both receipt readers re-derive every coverage word from the
//! recorded facts (arm 32), so a receipt cannot claim a relation its own
//! numbers do not support.
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
//! marks (a log start or a high watermark moved backwards: the topic was
//! recreated or truncated) is `changed_during_capture`, and no group may
//! claim a position on it: a group holding one is `failed:
//! GenerationChangedDuringCapture` (PROD-01.4 TI-04.1-3), never captured.
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

/// `GroupSnapshot::outcome`'s closed set (arm 27).
pub const GROUP_OUTCOMES: [&str; 3] = ["captured", "excluded", "failed"];

/// `GroupSnapshot::reason`'s closed set for an `excluded` group (arm 27).
pub const EXCLUDED_REASONS: [&str; 2] = ["GroupTypeNotCaptured", "GroupNotFound"];

/// `GroupSnapshot::reason`'s closed set for a `failed` group (arm 27).
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
///   that changed during the capture.
/// - `CaptureUnavailable`: the run's reader could not read consumer groups.
pub const FAILED_REASONS: [&str; 12] = [
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
];

/// The reason a group holding a position on a changed topic fails with.
pub const GENERATION_CHANGED: &str = "GenerationChangedDuringCapture";

/// The reason an excluded group of a type Logweir does not capture carries.
pub const GROUP_TYPE_NOT_CAPTURED: &str = "GroupTypeNotCaptured";

/// The group types a `captured` group carries (arm 28).
pub const CAPTURED_TYPES: [&str; 2] = ["classic", "consumer"];

/// The type an `excluded: GroupTypeNotCaptured` group carries (arm 28).
pub const OTHER_TYPE: &str = "other";

/// `GroupSnapshot::state` and `listed_state`'s closed set (arm 28): the states
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

/// `PositionEntry::status`'s closed set (arm 31).
pub const POSITION_STATUSES: [&str; 5] = [
    "captured",
    "noCommittedPosition",
    "excluded",
    "failed",
    "notObserved",
];

/// The reason an `excluded` position carries: its offset is above the
/// partition's high watermark at capture (PROD-01.4 TI-04.1-2).
pub const POSITION_BEYOND_END: &str = "PositionBeyondEnd";

/// `PositionEntry::reason`'s closed set for a `failed` position (arm 31).
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

/// `PositionEntry::reason`'s closed set for a `notObserved` position (arm 31):
/// the partition was added after the group capture read the topic's
/// partitions, or that read failed for the topic.
pub const NOT_OBSERVED_REASONS: [&str; 2] = ["PartitionAddedDuringCapture", "TopicNotObserved"];

/// `PositionEntry::coverage`'s closed set (arm 32), in [`relation`]'s order.
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

/// `ConsumerPositions::listing`'s closed set (arm 26).
pub const LISTING_VALUES: [&str; 2] = ["complete", "notComplete"];

/// The most groups one backup may select. A larger selection is refused at
/// phase −1 (exit 3): each captured group records one entry per partition of
/// every named topic, and the receipt is one signed document.
pub const MAX_SELECTED_GROUPS: usize = 100;

/// The longest group id a selection may name.
pub const MAX_GROUP_ID_CHARS: usize = 255;

/// **Receipt format 1.5.0.** The consumer position evidence of one backup.
///
/// Field order is byte order (`det_json`); appended fields only.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct ConsumerPositions {
    /// When the group capture started: before the listings, the
    /// descriptions and the positions.
    pub observed_from: DateTime<Utc>,
    /// When it ended, after the group-capture marks and BEFORE the engine
    /// started. At or after `observed_from` (arm 26).
    pub observed_to: DateTime<Utc>,
    /// `complete` when the group listings were complete (Describe on the
    /// cluster, no listing error), else `notComplete`: an unlisted id was then
    /// classified by a targeted describe (PROD-04.0 §5).
    pub listing: String,
    /// Per named topic, the facts of each partition. Exactly `source.topics`
    /// (arm 23).
    pub topics: BTreeMap<String, TopicPartitions>,
    /// One entry per selected group id, keyed by it. Never empty (arm 26): a
    /// backup that selects no group carries no block.
    pub groups: BTreeMap<String, GroupSnapshot>,
}

/// One named topic's partitions, as the capture saw them.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct TopicPartitions {
    /// One entry per partition, `partition` equal to its index (arm 24): the
    /// partitions the group capture read, then any the read after the engine
    /// or the archive shows beyond them.
    pub partitions: Vec<PartitionFacts>,
    /// Whether any partition's marks after the engine regress against its
    /// group-capture marks ([`changed_during_capture`], arm 25).
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

/// One selected group's outcome.
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
    /// have moved after they were read ([`active`], arm 29; captured only).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub active: Option<bool>,
    /// One entry per partition of every named topic, topics in name order
    /// (captured only; arm 30).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub positions: Option<Vec<PositionEntry>>,
}

/// One partition's position for one captured group.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct PositionEntry {
    pub topic: String,
    pub partition: u32,
    /// [`POSITION_STATUSES`].
    pub status: String,
    /// The committed next-to-consume offset: present exactly when `status` is
    /// `captured` or `excluded`. NEVER present for `noCommittedPosition`.
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
/// (module doc's table). Pure; the writer and arm 32 both decide with it, and
/// `docs/verify_scorecard.py` mirrors it.
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
/// §4.4).
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

impl ConsumerPositions {
    /// `sha256:<hex>` over the block's deterministic JSON: the digest the
    /// catalog point record binds (`consumer_positions.sha256`), recomputed
    /// by a reader from the verified receipt.
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

/// Why a selection is refused at phase −1, or `None`. A blank id, an id with
/// a control character or longer than [`MAX_GROUP_ID_CHARS`], the same id
/// twice, or more than [`MAX_SELECTED_GROUPS`] ids.
#[must_use]
pub fn refuse_selection(selected: &[String]) -> Option<String> {
    if selected.len() > MAX_SELECTED_GROUPS {
        return Some(format!(
            "the backup selects {} consumer groups and at most {MAX_SELECTED_GROUPS} may be \
             selected",
            selected.len()
        ));
    }
    let mut seen = std::collections::BTreeSet::new();
    for id in selected {
        if id.trim().is_empty() {
            return Some("a selected consumer group id is blank".to_string());
        }
        if id.chars().any(char::is_control) || id.chars().count() > MAX_GROUP_ID_CHARS {
            return Some(format!(
                "the selected consumer group id {:?} carries a control character or is longer \
                 than {MAX_GROUP_ID_CHARS} characters",
                id.chars().take(64).collect::<String>()
            ));
        }
        if !seen.insert(id.as_str()) {
            return Some(format!(
                "the consumer group {id:?} is selected twice; each group is selected once"
            ));
        }
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

    #[test]
    fn a_selection_is_refused_for_blank_control_long_repeated_or_too_many_ids() {
        let ids = |v: &[&str]| v.iter().map(|s| s.to_string()).collect::<Vec<_>>();
        assert_eq!(refuse_selection(&ids(&["a", "b"])), None);
        assert!(refuse_selection(&ids(&[" "])).is_some());
        assert!(refuse_selection(&ids(&["a\nb"])).is_some());
        assert!(refuse_selection(&ids(&["a", "a"])).is_some());
        assert!(refuse_selection(&[("x".repeat(MAX_GROUP_ID_CHARS))]).is_none());
        assert!(refuse_selection(&[("x".repeat(MAX_GROUP_ID_CHARS + 1))]).is_some());
        let many: Vec<String> = (0..=MAX_SELECTED_GROUPS).map(|i| format!("g{i}")).collect();
        assert!(refuse_selection(&many).is_some());
        assert!(refuse_selection(&many[..MAX_SELECTED_GROUPS]).is_none());
    }
}
