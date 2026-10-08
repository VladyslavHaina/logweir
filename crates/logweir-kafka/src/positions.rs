//! **PROD-04.0a: a consumer group's committed positions, read and written
//! through the safe consumer API.** The types and the decisions, with no
//! broker and no rdkafka, so they are available under
//! `--no-default-features` and every rule below is unit-testable. The
//! broker glue is `crate::rdkafka_positions` (feature `client`), reached
//! through [`RdKafkaReader::committed_positions`] and
//! [`RdKafkaReader::commit_positions`].
//!
//! The contract is PROD-04.0's (`docs/to-do/decisions/PROD-04.0-admin-path.md`),
//! §4.2, §4.3 and §5:
//!
//! - **Absence is never offset 0.** A partition the broker answers with "no
//!   committed offset" (wire −1, librdkafka [`NO_COMMITTED_OFFSET_RAW`],
//!   rust-rdkafka `Offset::Invalid`) is [`PartitionPosition::NoCommittedPosition`].
//!   An ABSENT group and a SHARE group answer exactly that too (K7), so a
//!   caller classifies the group (§5) before it reads anything into it.
//! - **One RequireStable fetch per group.** The handle reads with
//!   `isolation.level=read_committed`, so librdkafka sets RequireStable and
//!   retries a partition answering UNSTABLE_OFFSET_COMMIT until the bound
//!   expires. A pending transactional offset commit therefore surfaces as the
//!   WHOLE call timing out, with no partition named (§3.4), and the unit of
//!   [`PositionsError::PositionsUnstable`] is the group.
//! - **The same timeout means something else for a group no listing shows**
//!   (§5): [`PositionsError::NotVisibleOrUnreachable`]. The caller says which
//!   case it is in with [`GroupListing`]; the safe fetch cannot know.
//! - **A non-member commit.** Generation −1 and an empty member id, from a
//!   handle that never joins: the broker refuses it for the WHOLE request
//!   while the group has members (UNKNOWN_MEMBER_ID, K3), which is the atomic
//!   liveness guard PROD-04.2 relies on. Codes map as §4.3 says
//!   ([`commit_error`]).
//! - **Leader epochs.** Carried where the route exposes them: the safe API
//!   exposes none on a read (C3), so [`CommittedPosition::leader_epoch`] is
//!   `None` there, never a guessed −1; every commit carries
//!   [`COMMIT_LEADER_EPOCH`] (−1), never a captured source epoch (§4.3,
//!   AP-04.2-4).
//!
//! [`RdKafkaReader::committed_positions`]: crate::rdkafka_reader::RdKafkaReader::committed_positions
//! [`RdKafkaReader::commit_positions`]: crate::rdkafka_reader::RdKafkaReader::commit_positions
use std::collections::BTreeSet;
use std::time::Duration;

/// librdkafka's `RD_KAFKA_OFFSET_INVALID`: what it stores for a partition the
/// broker answered with offset −1, "no committed offset"
/// (`rdkafka_request.c:1288-1292` in rdkafka-sys 4.10.0+2.12.1), and what
/// `rd_kafka_committed` resets every requested partition to before it asks
/// (`rdkafka.c:3636-3637`). rust-rdkafka names it `Offset::Invalid`.
pub const NO_COMMITTED_OFFSET_RAW: i64 = -1001;

/// The leader epoch every commit carries: −1, "unknown". A source cluster's
/// leader epoch means nothing on a target cluster (§4.3), and −1 is also the
/// only value the safe API can send (C3: rust-rdkafka 0.36.2 has no setter;
/// librdkafka's default for a new list element is −1).
pub const COMMIT_LEADER_EPOCH: i32 = -1;

/// The metadata string every commit carries: Logweir's own marker (§4.3), so
/// a position Logweir wrote is recognisable in a readback. Never the captured
/// source metadata, which belongs to the source application.
pub const COMMIT_METADATA_MARKER: &str = "logweir:positions";

/// The default bound of one fetch or one commit wait: the 15 s PROD-04.0
/// measured its timeouts with (§3.4, §3.9).
pub const DEFAULT_POSITION_BOUND: Duration = Duration::from_secs(15);

/// The smallest bound a handle accepts. The commit's wait for a coordinator is
/// `session.timeout.ms` and its request timeout `socket.timeout.ms`, both set
/// to the bound; librdkafka warns when `socket.timeout.ms` is not at least
/// `fetch.wait.max.ms` (500) + 1000 (`rdkafka_conf.c:4613-4622`).
pub const MIN_POSITION_BOUND: Duration = Duration::from_secs(2);

/// The largest bound a handle accepts (librdkafka's `socket.timeout.ms`
/// ceiling is 300000).
pub const MAX_POSITION_BOUND: Duration = Duration::from_secs(300);

/// Kafka and librdkafka error codes this module decides on, as the integers
/// they are on the wire (librdkafka's `rdkafka.h`, rdkafka-sys
/// 4.10.0+2.12.1). Integers, not rust-rdkafka's enum, so the mapping is pure
/// and a code outside the enum (T12) still has a value to report.
pub mod code {
    /// `RD_KAFKA_RESP_ERR__TIMED_OUT` (`rdkafka.h:319`).
    pub const TIMED_OUT: i32 = -185;
    /// `RD_KAFKA_RESP_ERR__WAIT_COORD` (`rdkafka.h:329`): a commit deferred
    /// for a coordinator that never came, failed after `session.timeout.ms`
    /// (`rdkafka_cgrp.c:5809-5830`). Nothing was sent.
    pub const WAIT_COORD: i32 = -180;
    /// `RD_KAFKA_RESP_ERR__NO_OFFSET` (`rdkafka.h:353`): no valid offset to
    /// commit. [`super::commit_request`] refuses that input before it is sent.
    pub const NO_OFFSET: i32 = -168;
    /// `COORDINATOR_LOAD_IN_PROGRESS`.
    pub const COORDINATOR_LOAD_IN_PROGRESS: i32 = 14;
    /// `COORDINATOR_NOT_AVAILABLE`.
    pub const COORDINATOR_NOT_AVAILABLE: i32 = 15;
    /// `NOT_COORDINATOR`.
    pub const NOT_COORDINATOR: i32 = 16;
    /// `ILLEGAL_GENERATION`.
    pub const ILLEGAL_GENERATION: i32 = 22;
    /// `UNKNOWN_MEMBER_ID`: a non-member commit to a group with members (K3).
    pub const UNKNOWN_MEMBER_ID: i32 = 25;
    /// `REBALANCE_IN_PROGRESS`.
    pub const REBALANCE_IN_PROGRESS: i32 = 27;
    /// `TOPIC_AUTHORIZATION_FAILED`: per topic (T15).
    pub const TOPIC_AUTHORIZATION_FAILED: i32 = 29;
    /// `GROUP_AUTHORIZATION_FAILED`.
    pub const GROUP_AUTHORIZATION_FAILED: i32 = 30;
    /// `GROUP_ID_NOT_FOUND`: a share group refuses OffsetCommit with it (K4).
    pub const GROUP_ID_NOT_FOUND: i32 = 69;
    /// `FENCED_INSTANCE_ID`.
    pub const FENCED_INSTANCE_ID: i32 = 82;
    /// `UNSTABLE_OFFSET_COMMIT`: a pending transactional offset commit.
    pub const UNSTABLE_OFFSET_COMMIT: i32 = 88;
    /// `STALE_MEMBER_EPOCH` (`rdkafka.h:652`).
    pub const STALE_MEMBER_EPOCH: i32 = 113;
}

/// One partition of one topic.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TopicPartition {
    pub topic: String,
    pub partition: i32,
}

impl TopicPartition {
    pub fn new(topic: impl Into<String>, partition: i32) -> Self {
        Self {
            topic: topic.into(),
            partition,
        }
    }
}

impl std::fmt::Display for TopicPartition {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}:{}", self.topic, self.partition)
    }
}

/// A committed position: the next offset the group would consume.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommittedPosition {
    /// Always `>= 0`. "No committed offset" is never a position: it is
    /// [`PartitionPosition::NoCommittedPosition`].
    pub offset: i64,
    /// The committed leader epoch, where the route that read it exposes one.
    /// `None` means NOT EXPOSED, never "−1": rust-rdkafka 0.36.2's
    /// `TopicPartitionList` has no leader-epoch accessor (PROD-04.0 C3), so a
    /// read through the safe API is always `None`. A later route that reads
    /// it (O2's ListConsumerGroupOffsets) fills it, −1 included.
    pub leader_epoch: Option<i32>,
    /// The committed metadata string. `None` when it is not valid UTF-8:
    /// rust-rdkafka's `TopicPartitionListElem::metadata` panics on such bytes
    /// (`topic_partition_list.rs:141-145`), so the reader withholds it rather
    /// than guess. Apache Kafka returns metadata as a Java string, so its
    /// brokers always answer valid UTF-8; an empty or null string is `Some("")`.
    pub metadata: Option<String>,
}

/// What one requested partition's fetch answered.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PartitionPosition {
    /// The group has a committed position here.
    Committed(CommittedPosition),
    /// The broker answered "no committed offset" for this partition. NEVER
    /// offset 0. An absent group and a share group answer this for every
    /// partition (K7): classify the group before reading meaning into it.
    NoCommittedPosition,
    /// This partition's answer carried an error, or a value that is not a
    /// position.
    Failed(PartitionFailure),
}

/// Why one partition of a fetch has no position to report.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PartitionFailure {
    /// TOPIC_AUTHORIZATION_FAILED (29): the principal may not Describe the
    /// topic. The explicit-partition fetch names the refusal; an
    /// all-partitions fetch would drop the topic silently (T15).
    TopicNotAuthorized,
    /// UNSTABLE_OFFSET_COMMIT (88), answered for this partition after
    /// librdkafka's own retries: a transactional offset commit is pending.
    /// More usually the whole fetch times out instead
    /// ([`PositionsError::PositionsUnstable`]).
    Unstable,
    /// The broker answered a negative committed value other than "none": not
    /// a position, and not absence either.
    NotAPosition { raw: i64 },
    /// Any other per-partition code, as its integer and librdkafka's name.
    Other { code: i32, name: String },
}

/// What the caller's own group listing said about the group, the one fact a
/// safe fetch cannot supply (§5). It decides what a TIMEOUT means.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GroupListing {
    /// A listing the caller made shows this group id.
    Listed,
    /// No listing the caller made shows this id: the fetch is §5's targeted
    /// probe. An answer with no error means the id is describable and absent
    /// (or holds no position on these partitions); a timeout means it may
    /// exist and be hidden from this principal.
    NotListed,
}

/// One group's answer: one entry per requested partition, in request order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GroupPositions {
    pub group: String,
    pub partitions: Vec<(TopicPartition, PartitionPosition)>,
}

impl GroupPositions {
    /// The committed position of `tp`, if the answer holds one.
    #[must_use]
    pub fn committed(&self, tp: &TopicPartition) -> Option<&CommittedPosition> {
        self.partitions.iter().find_map(|(t, p)| match p {
            PartitionPosition::Committed(c) if t == tp => Some(c),
            _ => None,
        })
    }

    /// The answer for `tp`, if it was requested.
    #[must_use]
    pub fn get(&self, tp: &TopicPartition) -> Option<&PartitionPosition> {
        self.partitions
            .iter()
            .find_map(|(t, p)| (t == tp).then_some(p))
    }
}

/// Why a group's positions could not be read. Per group: every requested
/// partition of the group shares it (§4.2), and other groups are unaffected.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum PositionsError {
    /// The bounded RequireStable fetch of a LISTED group got no answer: a
    /// pending transactional offset commit, an unavailable coordinator, or a
    /// group this principal may not see. The safe route gives the same timeout
    /// for all three (§3.4, §3.9), so every requested partition of the group
    /// is unstable, and no position is reported, never the pre-transaction one.
    #[error(
        "{group}: positions unstable: no answer within {bound:?} (pending transactional \
         offsets, coordinator unavailable, or group not visible)"
    )]
    PositionsUnstable { group: String, bound: Duration },
    /// The bounded fetch of a group NO LISTING SHOWS got no answer: it may
    /// exist and be hidden from this principal, or its coordinator may be
    /// unreachable (§5). Never "not found".
    #[error(
        "{group}: not visible or unreachable: no answer within {bound:?} for a group no listing \
         shows (it may exist and be hidden from this principal)"
    )]
    NotVisibleOrUnreachable { group: String, bound: Duration },
    /// The broker refused this principal on the group (GROUP_AUTHORIZATION_FAILED,
    /// 30), as a fetch answer or as the coordinator lookup's refusal, on a
    /// group a listing shows (§5: "NotAuthorized (30 on a listed id)").
    #[error("{group}: not authorized on the group (GROUP_AUTHORIZATION_FAILED)")]
    NotAuthorized { group: String },
    /// As [`Self::NotAuthorized`], for a group no listing shows: it may exist,
    /// and this principal may not see it (§5). Never "not found".
    #[error(
        "{group}: not visible to this principal (GROUP_AUTHORIZATION_FAILED for a group no \
         listing shows)"
    )]
    NotVisibleToPrincipal { group: String },
    /// Any other error, as its integer code and librdkafka's name.
    #[error("{group}: positions not read: {name} ({code})")]
    Failed {
        group: String,
        code: i32,
        name: String,
    },
    /// Refused before anything was sent.
    #[error("positions request refused: {0}")]
    InvalidRequest(String),
    /// The handle could not be built.
    #[error("kafka: {0}")]
    Client(String),
}

/// Whether a failed commit can have changed any position.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CommitApplied {
    /// The code is one the broker returns for the WHOLE request (a group-level
    /// refusal, or a coordinator that never answered), or the request was never
    /// sent: no position changed.
    Nothing,
    /// The code may be one partition's: a synchronous commit reports the LAST
    /// failing partition's code even when other partitions were applied
    /// (`rdkafka_request.c:1733-1750`), and a request timeout may follow an
    /// applied commit. Some positions may have changed: read them back.
    Unknown,
}

/// Why a commit was not (or not wholly) applied.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum CommitError {
    /// UNKNOWN_MEMBER_ID (25): the group has members, so the broker refused
    /// the non-member commit for the whole request (K3, §3.3). Nothing changed.
    #[error("{group}: group active: the broker refused a non-member commit (UNKNOWN_MEMBER_ID)")]
    GroupActive { group: String },
    /// GROUP_ID_NOT_FOUND (69): the id is a group that takes no offset
    /// commits, such as a share group (K4). Nothing changed.
    #[error("{group}: not a consumer group: the broker refused the commit (GROUP_ID_NOT_FOUND)")]
    NotAConsumerGroup { group: String },
    /// GROUP_AUTHORIZATION_FAILED (30): OffsetCommit needs Read on the group;
    /// or the coordinator lookup itself was refused (no Describe on the
    /// group). Nothing changed.
    #[error("{group}: not authorized to commit on the group (GROUP_AUTHORIZATION_FAILED)")]
    NotAuthorized { group: String },
    /// No coordinator answered for the group within the bound (`_WAIT_COORD`):
    /// it may be hidden from this principal or unreachable. The commit was
    /// never sent, so nothing changed.
    #[error(
        "{group}: not visible or unreachable: no coordinator within {bound:?}; nothing was \
         committed"
    )]
    NotVisibleOrUnreachable { group: String, bound: Duration },
    /// Any other code, as its integer and librdkafka's name, and whether any
    /// position may have changed.
    #[error("{group}: commit failed: {name} ({code}); applied: {applied:?}")]
    Failed {
        group: String,
        code: i32,
        name: String,
        applied: CommitApplied,
    },
    /// Refused before anything was sent.
    #[error("commit refused before sending: {0}")]
    InvalidRequest(String),
    /// The handle could not be built. Nothing was sent.
    #[error("kafka: {0}")]
    Client(String),
}

impl CommitError {
    /// Whether this failure can have changed any committed position. Only
    /// [`CommitError::Failed`] with [`CommitApplied::Unknown`] can; a caller
    /// reads such a group back before it reports anything as refused.
    #[must_use]
    pub fn may_have_applied(&self) -> bool {
        matches!(
            self,
            CommitError::Failed {
                applied: CommitApplied::Unknown,
                ..
            }
        )
    }
}

/// What a fetch's partition answered, decided from the raw value librdkafka
/// left in the list element and its per-partition code (`None` for no error).
///
/// | code | raw | answer |
/// |---|---|---|
/// | 29 | any | `Failed(TopicNotAuthorized)` |
/// | 88 | any | `Failed(Unstable)` |
/// | other | any | `Failed(Other)` |
/// | none | `>= 0` | `Committed` |
/// | none | −1001 | `NoCommittedPosition` (never 0) |
/// | none | other negative | `Failed(NotAPosition)` |
#[must_use]
pub fn partition_answer(
    raw_offset: i64,
    error: Option<(i32, &str)>,
    metadata: Option<String>,
) -> PartitionPosition {
    if let Some((c, name)) = error {
        return PartitionPosition::Failed(match c {
            code::TOPIC_AUTHORIZATION_FAILED => PartitionFailure::TopicNotAuthorized,
            code::UNSTABLE_OFFSET_COMMIT => PartitionFailure::Unstable,
            other => PartitionFailure::Other {
                code: other,
                name: name.to_string(),
            },
        });
    }
    match raw_offset {
        n if n >= 0 => PartitionPosition::Committed(CommittedPosition {
            offset: n,
            // The safe API exposes no leader epoch (C3): not exposed, not −1.
            leader_epoch: None,
            metadata,
        }),
        NO_COMMITTED_OFFSET_RAW => PartitionPosition::NoCommittedPosition,
        raw => PartitionPosition::Failed(PartitionFailure::NotAPosition { raw }),
    }
}

/// What a fetch that returned an ERROR for the whole call means, from its
/// code, the caller's [`GroupListing`], and whether the handle's own queue
/// carried a GROUP_AUTHORIZATION_FAILED refusal (librdkafka posts a refused
/// coordinator lookup there once, `rdkafka_cgrp.c:797-807`, while the fetch
/// keeps waiting).
///
/// | code | refusal seen | listing | error |
/// |---|---|---|---|
/// | timeout | no | Listed | `PositionsUnstable` |
/// | timeout | no | NotListed | `NotVisibleOrUnreachable` |
/// | timeout, or 30 | yes, or code 30 | Listed | `NotAuthorized` |
/// | timeout, or 30 | yes, or code 30 | NotListed | `NotVisibleToPrincipal` |
/// | other | — | — | `Failed` with its integer |
#[must_use]
pub fn fetch_error(
    group: &str,
    listing: GroupListing,
    error_code: i32,
    error_name: &str,
    group_refusal_seen: bool,
    bound: Duration,
) -> PositionsError {
    let group = group.to_string();
    let refused = error_code == code::GROUP_AUTHORIZATION_FAILED
        || (error_code == code::TIMED_OUT && group_refusal_seen);
    if refused {
        return match listing {
            GroupListing::Listed => PositionsError::NotAuthorized { group },
            GroupListing::NotListed => PositionsError::NotVisibleToPrincipal { group },
        };
    }
    match (error_code, listing) {
        (code::TIMED_OUT, GroupListing::Listed) => {
            PositionsError::PositionsUnstable { group, bound }
        }
        (code::TIMED_OUT, GroupListing::NotListed) => {
            PositionsError::NotVisibleOrUnreachable { group, bound }
        }
        (c, _) => PositionsError::Failed {
            group,
            code: c,
            name: error_name.to_string(),
        },
    }
}

/// The codes a synchronous commit can return only when NO position changed:
/// the group-level refusals the broker returns for every partition of the
/// request, and librdkafka's own "never sent" codes.
const NOTHING_APPLIED: &[i32] = &[
    code::WAIT_COORD,
    code::NO_OFFSET,
    code::COORDINATOR_LOAD_IN_PROGRESS,
    code::COORDINATOR_NOT_AVAILABLE,
    code::NOT_COORDINATOR,
    code::ILLEGAL_GENERATION,
    code::UNKNOWN_MEMBER_ID,
    code::REBALANCE_IN_PROGRESS,
    code::GROUP_AUTHORIZATION_FAILED,
    code::GROUP_ID_NOT_FOUND,
    code::FENCED_INSTANCE_ID,
    code::STALE_MEMBER_EPOCH,
];

/// What a failed synchronous commit means (§4.3), from its code and whether
/// the handle's queue carried a GROUP_AUTHORIZATION_FAILED refusal of the
/// coordinator lookup.
///
/// | code | error |
/// |---|---|
/// | 25 UNKNOWN_MEMBER_ID | `GroupActive` |
/// | 69 GROUP_ID_NOT_FOUND | `NotAConsumerGroup` |
/// | 30 GROUP_AUTHORIZATION_FAILED, or `_WAIT_COORD` with the refusal seen | `NotAuthorized` |
/// | `_WAIT_COORD` | `NotVisibleOrUnreachable` |
/// | anything else | `Failed` with its integer, `applied: Nothing` only for a whole-request code |
#[must_use]
pub fn commit_error(
    group: &str,
    error_code: i32,
    error_name: &str,
    group_refusal_seen: bool,
    bound: Duration,
) -> CommitError {
    let group = group.to_string();
    match error_code {
        code::UNKNOWN_MEMBER_ID => CommitError::GroupActive { group },
        code::GROUP_ID_NOT_FOUND => CommitError::NotAConsumerGroup { group },
        code::GROUP_AUTHORIZATION_FAILED => CommitError::NotAuthorized { group },
        code::WAIT_COORD if group_refusal_seen => CommitError::NotAuthorized { group },
        code::WAIT_COORD => CommitError::NotVisibleOrUnreachable { group, bound },
        c => CommitError::Failed {
            group,
            code: c,
            name: error_name.to_string(),
            applied: if NOTHING_APPLIED.contains(&c) {
                CommitApplied::Nothing
            } else {
                CommitApplied::Unknown
            },
        },
    }
}

/// One entry of a commit request, exactly as it is sent.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommitEntry {
    pub tp: TopicPartition,
    pub offset: i64,
    /// Always [`COMMIT_LEADER_EPOCH`].
    pub leader_epoch: i32,
    /// Always [`COMMIT_METADATA_MARKER`].
    pub metadata: String,
}

/// The commit request for `positions`, refused before anything is sent when
/// it cannot mean what it says.
///
/// Only each position's OFFSET is committed. Its leader epoch is replaced by
/// [`COMMIT_LEADER_EPOCH`] and its metadata by [`COMMIT_METADATA_MARKER`]:
/// the positions a caller commits were usually captured on another cluster,
/// whose epochs and application metadata do not describe this one (§4.3).
///
/// Refused: an empty request (librdkafka would answer `_NO_OFFSET`); an empty
/// topic name; a negative partition; a negative offset (librdkafka silently
/// LEAVES OUT a partition whose offset is not absolute,
/// `rdkafka_request.c:1847-1863`, so the partition would read as committed
/// when nothing was sent); and the same partition twice (the request could
/// only apply one of them).
pub fn commit_request(
    positions: &[(TopicPartition, CommittedPosition)],
) -> Result<Vec<CommitEntry>, CommitError> {
    if positions.is_empty() {
        return Err(CommitError::InvalidRequest(
            "no positions to commit".to_string(),
        ));
    }
    let mut seen = BTreeSet::new();
    let mut out = Vec::with_capacity(positions.len());
    for (tp, p) in positions {
        if tp.topic.is_empty() {
            return Err(CommitError::InvalidRequest(
                "a position names an empty topic".to_string(),
            ));
        }
        if tp.partition < 0 {
            return Err(CommitError::InvalidRequest(format!(
                "{tp}: a partition is never negative"
            )));
        }
        if p.offset < 0 {
            return Err(CommitError::InvalidRequest(format!(
                "{tp}: offset {} is not a position (librdkafka would leave the partition out of \
                 the request without an error)",
                p.offset
            )));
        }
        if !seen.insert(tp.clone()) {
            return Err(CommitError::InvalidRequest(format!(
                "{tp} appears twice in one commit"
            )));
        }
        out.push(CommitEntry {
            tp: tp.clone(),
            offset: p.offset,
            leader_epoch: COMMIT_LEADER_EPOCH,
            metadata: COMMIT_METADATA_MARKER.to_string(),
        });
    }
    Ok(out)
}

/// The partitions of a fetch request, refused before anything is sent when
/// empty, nameless, negative or repeated.
pub fn fetch_request(partitions: &[TopicPartition]) -> Result<(), PositionsError> {
    if partitions.is_empty() {
        return Err(PositionsError::InvalidRequest(
            "no partitions to read".to_string(),
        ));
    }
    let mut seen = BTreeSet::new();
    for tp in partitions {
        if tp.topic.is_empty() {
            return Err(PositionsError::InvalidRequest(
                "a partition names an empty topic".to_string(),
            ));
        }
        if tp.partition < 0 {
            return Err(PositionsError::InvalidRequest(format!(
                "{tp}: a partition is never negative"
            )));
        }
        if !seen.insert(tp) {
            return Err(PositionsError::InvalidRequest(format!(
                "{tp} appears twice in one fetch"
            )));
        }
    }
    Ok(())
}

/// The group id a handle is bound to, refused when empty or blank: a
/// librdkafka consumer with an empty `group.id` has no group at all.
pub fn valid_group(group: &str) -> Result<(), String> {
    if group.trim().is_empty() {
        return Err("a group id is never empty".to_string());
    }
    Ok(())
}

/// The bound a handle uses, refused outside
/// [`MIN_POSITION_BOUND`]..=[`MAX_POSITION_BOUND`].
pub fn valid_bound(bound: Duration) -> Result<Duration, String> {
    if bound < MIN_POSITION_BOUND || bound > MAX_POSITION_BOUND {
        return Err(format!(
            "a position bound of {bound:?} is outside {MIN_POSITION_BOUND:?}..={MAX_POSITION_BOUND:?}"
        ));
    }
    Ok(bound)
}

#[cfg(test)]
mod tests {
    //! The PROD-04.0a mapping, row by row. Each test names the mutant it
    //! kills; `scratchpad/prod-04-0a/mutants.py` in the worker's report runs
    //! them.
    use super::*;

    const B: Duration = Duration::from_secs(9);

    fn tp(t: &str, p: i32) -> TopicPartition {
        TopicPartition::new(t, p)
    }

    fn pos(offset: i64, epoch: Option<i32>, meta: Option<&str>) -> CommittedPosition {
        CommittedPosition {
            offset,
            leader_epoch: epoch,
            metadata: meta.map(str::to_string),
        }
    }

    /// **Absence is never offset 0** (AP-04.1-2's mutant: "mapping
    /// `Offset::Invalid` (−1001) to 0"). −1001 is "no committed offset"; 0 is
    /// a real position and stays one, so the two can never be confused.
    #[test]
    fn no_committed_offset_is_never_position_zero() {
        assert_eq!(
            partition_answer(NO_COMMITTED_OFFSET_RAW, None, Some(String::new())),
            PartitionPosition::NoCommittedPosition
        );
        assert_eq!(
            partition_answer(0, None, Some(String::new())),
            PartitionPosition::Committed(pos(0, None, Some("")))
        );
        assert_eq!(
            partition_answer(9, None, Some("app".into())),
            PartitionPosition::Committed(pos(9, None, Some("app")))
        );
    }

    /// The safe route exposes no leader epoch (C3): a read says `None`, "not
    /// exposed", never a guessed −1 or 0.
    #[test]
    fn a_safe_read_never_invents_a_leader_epoch() {
        let PartitionPosition::Committed(c) = partition_answer(12, None, None) else {
            panic!("12 is a position");
        };
        assert_eq!(c.leader_epoch, None);
    }

    /// Any other negative value is neither a position nor absence.
    #[test]
    fn an_odd_negative_committed_value_is_not_a_position() {
        for raw in [-1, -2, -1000, -2000, -5000] {
            assert_eq!(
                partition_answer(raw, None, None),
                PartitionPosition::Failed(PartitionFailure::NotAPosition { raw }),
                "raw {raw}"
            );
        }
    }

    /// Per-partition codes win over whatever offset sits beside them: a
    /// refused topic (T15) and a pending transaction never read as positions.
    #[test]
    fn a_partition_error_is_never_read_as_a_position() {
        for raw in [7, NO_COMMITTED_OFFSET_RAW] {
            assert_eq!(
                partition_answer(raw, Some((code::TOPIC_AUTHORIZATION_FAILED, "x")), None),
                PartitionPosition::Failed(PartitionFailure::TopicNotAuthorized)
            );
            assert_eq!(
                partition_answer(raw, Some((code::UNSTABLE_OFFSET_COMMIT, "x")), None),
                PartitionPosition::Failed(PartitionFailure::Unstable)
            );
            assert_eq!(
                partition_answer(raw, Some((3, "Broker: Unknown topic or partition")), None),
                PartitionPosition::Failed(PartitionFailure::Other {
                    code: 3,
                    name: "Broker: Unknown topic or partition".into()
                })
            );
        }
    }

    /// §4.2 and §5: a timeout is `PositionsUnstable` for a listed group and
    /// `NotVisibleOrUnreachable` for an unlisted one; a group refusal is
    /// `NotAuthorized` or `NotVisibleToPrincipal`. Mutants: swapping the two
    /// listings, ignoring the refusal, or calling a timeout "not found" (an
    /// `Ok` would be the only way to say that, and this returns an error).
    #[test]
    fn a_fetch_timeout_is_named_by_the_callers_listing() {
        use GroupListing::{Listed, NotListed};
        let t = code::TIMED_OUT;
        assert_eq!(
            fetch_error("g", Listed, t, "timed out", false, B),
            PositionsError::PositionsUnstable {
                group: "g".into(),
                bound: B
            }
        );
        assert_eq!(
            fetch_error("g", NotListed, t, "timed out", false, B),
            PositionsError::NotVisibleOrUnreachable {
                group: "g".into(),
                bound: B
            }
        );
        assert_eq!(
            fetch_error("g", Listed, t, "timed out", true, B),
            PositionsError::NotAuthorized { group: "g".into() }
        );
        assert_eq!(
            fetch_error("g", NotListed, t, "timed out", true, B),
            PositionsError::NotVisibleToPrincipal { group: "g".into() }
        );
        let a = code::GROUP_AUTHORIZATION_FAILED;
        assert_eq!(
            fetch_error("g", Listed, a, "x", false, B),
            PositionsError::NotAuthorized { group: "g".into() }
        );
        assert_eq!(
            fetch_error("g", NotListed, a, "x", false, B),
            PositionsError::NotVisibleToPrincipal { group: "g".into() }
        );
    }

    /// Anything else keeps its integer, and a refusal seen on the queue never
    /// re-labels an error that is not a timeout.
    #[test]
    fn any_other_fetch_error_keeps_its_integer_code() {
        for listing in [GroupListing::Listed, GroupListing::NotListed] {
            for seen in [false, true] {
                assert_eq!(
                    fetch_error(
                        "g",
                        listing,
                        code::COORDINATOR_NOT_AVAILABLE,
                        "coord",
                        seen,
                        B
                    ),
                    PositionsError::Failed {
                        group: "g".into(),
                        code: 15,
                        name: "coord".into()
                    }
                );
                assert_eq!(
                    fetch_error("g", listing, 4242, "unlisted code", seen, B),
                    PositionsError::Failed {
                        group: "g".into(),
                        code: 4242,
                        name: "unlisted code".into()
                    },
                    "a code outside every enum survives as its integer (T12)"
                );
            }
        }
    }

    /// §4.3's table, exactly. Mutants: 25 to anything but `GroupActive`, 69 to
    /// anything but `NotAConsumerGroup`, 30 to anything but `NotAuthorized`,
    /// `_WAIT_COORD` to `Failed` or to "applied".
    #[test]
    fn commit_codes_map_as_section_4_3_says() {
        assert_eq!(
            commit_error("g", code::UNKNOWN_MEMBER_ID, "x", false, B),
            CommitError::GroupActive { group: "g".into() }
        );
        assert_eq!(
            commit_error("g", code::GROUP_ID_NOT_FOUND, "x", false, B),
            CommitError::NotAConsumerGroup { group: "g".into() }
        );
        assert_eq!(
            commit_error("g", code::GROUP_AUTHORIZATION_FAILED, "x", false, B),
            CommitError::NotAuthorized { group: "g".into() }
        );
        assert_eq!(
            commit_error("g", code::WAIT_COORD, "x", false, B),
            CommitError::NotVisibleOrUnreachable {
                group: "g".into(),
                bound: B
            }
        );
        assert_eq!(
            commit_error("g", code::WAIT_COORD, "x", true, B),
            CommitError::NotAuthorized { group: "g".into() }
        );
        // The refusal flag re-labels only the coordinator wait.
        assert_eq!(
            commit_error("g", code::UNKNOWN_MEMBER_ID, "x", true, B),
            CommitError::GroupActive { group: "g".into() }
        );
    }

    /// **A failed commit is "nothing applied" only for a whole-request code.**
    /// A sync commit returns the LAST failing partition's code even when the
    /// others were applied, so a per-partition code (29), a request timeout or
    /// a code nobody listed is `applied: Unknown`. Mutant: every `Failed` as
    /// `Nothing` (an audit would call a half-applied group refused).
    #[test]
    fn a_failed_commit_says_whether_anything_may_have_changed() {
        for c in [
            code::TOPIC_AUTHORIZATION_FAILED,
            code::TIMED_OUT,
            3,
            12,
            4242,
        ] {
            let e = commit_error("g", c, "n", false, B);
            assert_eq!(
                e,
                CommitError::Failed {
                    group: "g".into(),
                    code: c,
                    name: "n".into(),
                    applied: CommitApplied::Unknown
                },
                "code {c}"
            );
            assert!(e.may_have_applied(), "code {c}");
        }
        for c in [
            code::COORDINATOR_LOAD_IN_PROGRESS,
            code::COORDINATOR_NOT_AVAILABLE,
            code::NOT_COORDINATOR,
            code::ILLEGAL_GENERATION,
            code::REBALANCE_IN_PROGRESS,
            code::FENCED_INSTANCE_ID,
            code::STALE_MEMBER_EPOCH,
            code::NO_OFFSET,
        ] {
            let e = commit_error("g", c, "n", false, B);
            assert!(
                matches!(
                    e,
                    CommitError::Failed {
                        applied: CommitApplied::Nothing,
                        ..
                    }
                ),
                "code {c}: {e:?}"
            );
            assert!(!e.may_have_applied(), "code {c}");
        }
        for e in [
            commit_error("g", code::UNKNOWN_MEMBER_ID, "n", false, B),
            commit_error("g", code::GROUP_ID_NOT_FOUND, "n", false, B),
            commit_error("g", code::GROUP_AUTHORIZATION_FAILED, "n", false, B),
            commit_error("g", code::WAIT_COORD, "n", false, B),
        ] {
            assert!(!e.may_have_applied(), "{e:?}");
        }
    }

    /// **AP-04.2-4: every committed position carries leader epoch −1 and
    /// Logweir's marker, never the captured source epoch or metadata.**
    /// Mutants: copying the captured epoch (`7` here), or the captured
    /// metadata.
    #[test]
    fn a_commit_carries_epoch_minus_one_and_the_marker_never_the_captured_values() {
        let got = commit_request(&[
            (tp("orders", 0), pos(9, Some(7), Some("source-app-meta"))),
            (tp("orders", 2), pos(0, None, None)),
            (tp("audit", 1), pos(41, Some(-1), Some(""))),
        ])
        .expect("a valid request");
        assert_eq!(
            got,
            vec![
                CommitEntry {
                    tp: tp("orders", 0),
                    offset: 9,
                    leader_epoch: -1,
                    metadata: COMMIT_METADATA_MARKER.into()
                },
                CommitEntry {
                    tp: tp("orders", 2),
                    offset: 0,
                    leader_epoch: -1,
                    metadata: COMMIT_METADATA_MARKER.into()
                },
                CommitEntry {
                    tp: tp("audit", 1),
                    offset: 41,
                    leader_epoch: -1,
                    metadata: COMMIT_METADATA_MARKER.into()
                },
            ],
            "offsets kept in order; epoch and metadata replaced"
        );
        assert_eq!(COMMIT_LEADER_EPOCH, -1);
    }

    /// A request that cannot mean what it says is refused before sending: a
    /// negative offset would be LEFT OUT by librdkafka without an error.
    #[test]
    fn a_commit_request_that_cannot_mean_what_it_says_is_refused() {
        let ok = pos(1, None, None);
        for (bad, why) in [
            (vec![], "empty"),
            (vec![(tp("t", 0), pos(-1, None, None))], "negative offset"),
            (
                vec![(tp("t", 0), pos(NO_COMMITTED_OFFSET_RAW, None, None))],
                "absence is not a position to commit",
            ),
            (vec![(tp("t", -1), ok.clone())], "negative partition"),
            (vec![(tp("", 0), ok.clone())], "empty topic"),
            (
                vec![(tp("t", 0), ok.clone()), (tp("t", 0), pos(2, None, None))],
                "duplicate partition",
            ),
        ] {
            assert!(
                matches!(commit_request(&bad), Err(CommitError::InvalidRequest(_))),
                "{why}"
            );
        }
        assert!(commit_request(&[(tp("t", 0), ok)]).is_ok());
    }

    #[test]
    fn a_fetch_request_that_cannot_mean_what_it_says_is_refused() {
        for (bad, why) in [
            (vec![], "empty"),
            (vec![tp("t", -1)], "negative partition"),
            (vec![tp("", 0)], "empty topic"),
            (vec![tp("t", 0), tp("t", 0)], "duplicate"),
        ] {
            assert!(
                matches!(fetch_request(&bad), Err(PositionsError::InvalidRequest(_))),
                "{why}"
            );
        }
        assert!(fetch_request(&[tp("t", 0), tp("t", 1), tp("u", 0)]).is_ok());
    }

    #[test]
    fn the_bound_and_the_group_are_validated() {
        assert!(valid_bound(MIN_POSITION_BOUND).is_ok());
        assert!(valid_bound(MAX_POSITION_BOUND).is_ok());
        assert!(valid_bound(DEFAULT_POSITION_BOUND).is_ok());
        assert!(valid_bound(MIN_POSITION_BOUND - Duration::from_millis(1)).is_err());
        assert!(valid_bound(MAX_POSITION_BOUND + Duration::from_millis(1)).is_err());
        assert!(valid_group("g").is_ok());
        assert!(valid_group("").is_err());
        assert!(valid_group(" \t").is_err());
    }

    #[test]
    fn group_positions_answers_by_partition() {
        let g = GroupPositions {
            group: "g".into(),
            partitions: vec![
                (
                    tp("t", 0),
                    PartitionPosition::Committed(pos(5, None, Some(""))),
                ),
                (tp("t", 1), PartitionPosition::NoCommittedPosition),
            ],
        };
        assert_eq!(g.committed(&tp("t", 0)).map(|c| c.offset), Some(5));
        assert_eq!(g.committed(&tp("t", 1)), None);
        assert_eq!(
            g.get(&tp("t", 1)),
            Some(&PartitionPosition::NoCommittedPosition)
        );
        assert_eq!(g.get(&tp("t", 2)), None);
    }
}
