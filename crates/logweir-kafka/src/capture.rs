//! **PROD-04.1: one capture of the selected consumer groups**, as the reader
//! observed it, before anything about the archive is known. Pure values; the
//! broker glue is `crate::rdkafka_capture` (feature `client`), reached through
//! [`crate::reader::ClusterReader::observe_consumer_groups`] and
//! [`crate::reader::ClusterReader::partition_marks`].
//!
//! # The order of the reads, and why
//!
//! 1. **Classify** every selected id (PROD-04.0b's [`crate::groups`]): one
//!    verdict each, nothing dropped.
//! 2. **Describe** the capturable ones, refused unless the description agrees
//!    with the listing (T2): the state the description gives is fresher than
//!    the listing's, and a group that gained or lost members between the two
//!    reads shows it in the two states.
//! 3. **Read the named topics' partitions** (metadata), the set every
//!    position is asked for.
//! 4. **Fetch the positions** of every described group over exactly those
//!    partitions: one RequireStable fetch per group (PROD-04.0a), so a pending
//!    transactional offset commit never yields the pre-transaction position.
//! 5. **Read the marks** (log start and high watermark, READ_UNCOMMITTED) of
//!    exactly those partitions, AFTER the positions: a position the group
//!    committed between the fetch and the marks is then still at or below the
//!    high watermark, so `PositionBeyondEnd` is never a race against a
//!    consuming application.
//!
//! None of it is fatal: a failed read is a value the receipt records.
use crate::groups::{DescribeFailure, GroupDescription, GroupVerdict, ListingCompleteness};
use crate::positions::{GroupPositions, PositionsError};
use std::collections::BTreeMap;

/// One partition's marks: its log start offset and high watermark, read
/// together with READ_UNCOMMITTED isolation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Marks {
    pub log_start: i64,
    pub high_watermark: i64,
}

/// One named topic's partitions and their marks: the partitions the metadata
/// listed (`Err` when that read failed), each with its marks or why they were
/// not read.
pub type TopicMarks = Result<Vec<(i32, Result<Marks, String>)>, String>;

/// One selected group, as the capture observed it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObservedGroup {
    /// The selected id.
    pub group_id: String,
    /// PROD-04.0 §5's verdict.
    pub verdict: GroupVerdict,
    /// The description, for a [`GroupVerdict::Capture`] only.
    pub description: Option<Result<GroupDescription, DescribeFailure>>,
    /// The positions over every observed partition, for a described group
    /// only.
    pub positions: Option<Result<GroupPositions, PositionsError>>,
}

/// Everything one capture of the selected groups observed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GroupsObservation {
    /// Whether the listings were complete; `None` when no classification was
    /// made (the reader cannot read groups, or the selection was refused).
    pub completeness: Option<ListingCompleteness>,
    /// One entry per selected id, in selection order.
    pub groups: Vec<ObservedGroup>,
    /// Per named topic: the partitions the positions were asked for, with the
    /// marks read after them.
    pub topics: BTreeMap<String, TopicMarks>,
    /// Why nothing was observed, when nothing was.
    pub unavailable: Option<String>,
}

impl GroupsObservation {
    /// A capture that observed nothing, and why: every selected group is then
    /// recorded as failed `CaptureUnavailable`, never as absent and never as
    /// offset 0.
    #[must_use]
    pub fn unavailable(topics: &[String], why: impl Into<String>) -> Self {
        let why = why.into();
        Self {
            completeness: None,
            groups: Vec::new(),
            topics: topics
                .iter()
                .map(|t| (t.clone(), Err(why.clone())))
                .collect(),
            unavailable: Some(why),
        }
    }
}
