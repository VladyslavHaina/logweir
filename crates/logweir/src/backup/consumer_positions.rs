//! **PROD-04.1: the receipt's `consumer_positions` block**, built from what
//! the run observed of the selected consumer groups before the engine, the
//! partitions' marks after it, and the offsets the archive's manifest records.
//!
//! Pure: [`build`] is a function of its [`Capture`], so every rule below is a
//! unit row with no broker. The vocabulary and the coverage rule are
//! `logweir_core::consumer_positions`'s, the same ones the receipt's arms
//! 22-34 re-derive, so a block this module writes is one both readers accept.
//!
//! # The rules
//!
//! - **One outcome per selected id**, in the receipt's id order. An id the
//!   observation does not answer for — the reader could not read groups at
//!   all — is `failed: CaptureUnavailable`, never absent.
//! - **A captured group lists every partition of every named topic**: the
//!   partitions the capture read, then any the read after the engine or the
//!   archive shows beyond them, which are `notObserved`
//!   (`PartitionAddedDuringCapture`, or `TopicNotObserved` when the capture
//!   could not read the topic). A partition with no committed offset is
//!   `noCommittedPosition`: never offset 0.
//! - **A committed position is judged against its partition's facts**
//!   (`logweir_core::consumer_positions::relation`): `failed: MarksNotRead`
//!   without the marks, `excluded: PositionBeyondEnd` above the high
//!   watermark, otherwise `captured` with its coverage word.
//! - **A group holding a kept position on a topic that changed during the
//!   capture** is `failed: GenerationChangedDuringCapture`: its positions may
//!   be about records that no longer exist at those offsets.
//! - **Only well-formed facts are recorded**: a manifest range whose first
//!   offset is negative or above its last is NOT recorded (the partition then
//!   reads "nothing archived"), and the reader refuses marks that are not
//!   `0 <= log start <= high watermark`.
use chrono::{DateTime, Utc};
use logweir_core::consumer_positions::{
    self as model, ConsumerPositions, GroupSnapshot, PartitionFacts, PositionEntry, Relation,
    TopicPartitions,
};
use logweir_kafka::capture::{GroupsObservation, ObservedGroup, TopicMarks};
use logweir_kafka::groups::{
    DescribeFailure, Excluded, GroupFailure, GroupVerdict, ListingCompleteness,
};
use logweir_kafka::positions::{
    PartitionFailure, PartitionPosition, PositionsError, TopicPartition,
};
use std::collections::{BTreeMap, BTreeSet};

/// Per named topic, per partition with at least one segment: the lowest and
/// highest offsets the manifest records, inclusive.
pub type ArchivedRanges = BTreeMap<String, BTreeMap<i32, (i64, i64)>>;

/// One topic's archived range per partition, from the manifest's segments.
/// A partition with no segment, or whose range is not `0 <= first <= last`,
/// is left out: nothing archived, never `[0, 0]`.
#[must_use]
pub fn archived_ranges(
    partitions: &[logweir_core::engine::PartitionFacts],
) -> BTreeMap<i32, (i64, i64)> {
    partitions
        .iter()
        .filter_map(|p| {
            let first = p.segments.iter().map(|s| s.start_offset).min()?;
            let last = p.segments.iter().map(|s| s.end_offset).max()?;
            (0 <= first && first <= last && p.partition_id >= 0)
                .then_some((p.partition_id, (first, last)))
        })
        .collect()
}

/// Everything [`build`] decides from.
pub struct Capture<'a> {
    /// The selected group ids, in selection order (phase −1 refused blanks
    /// and repeats).
    pub selected: &'a [String],
    /// The plan's named topics.
    pub topics: &'a [String],
    /// When the group capture started.
    pub observed_from: DateTime<Utc>,
    /// When it ended, before the engine.
    pub observed_to: DateTime<Utc>,
    /// What it observed.
    pub observation: &'a GroupsObservation,
    /// The partitions' marks read after the engine.
    pub after: &'a BTreeMap<String, TopicMarks>,
    /// The archive's offsets.
    pub archived: &'a ArchivedRanges,
}

/// A topic's partitions as `partition -> marks`, when the read succeeded:
/// `None` for a failed metadata read, and a partition whose marks were not
/// read maps to `None`.
fn marks_by_partition(
    read: Option<&TopicMarks>,
) -> Option<BTreeMap<i32, Option<logweir_kafka::capture::Marks>>> {
    let list = read?.as_ref().ok()?;
    Some(
        list.iter()
            .map(|(p, m)| (*p, m.as_ref().ok().copied()))
            .collect(),
    )
}

/// The `topics` half of the block: each named topic's partition facts and
/// whether it changed during the capture.
fn topic_facts(c: &Capture<'_>) -> BTreeMap<String, TopicPartitions> {
    let names: BTreeSet<&String> = c.topics.iter().collect();
    names
        .into_iter()
        .map(|topic| {
            let at_capture = marks_by_partition(c.observation.topics.get(topic));
            let after = marks_by_partition(c.after.get(topic));
            let archived = c.archived.get(topic);
            // Every partition any of the three reads names, from 0.
            let highest = at_capture
                .iter()
                .flat_map(|m| m.keys())
                .chain(after.iter().flat_map(|m| m.keys()))
                .chain(archived.iter().flat_map(|m| m.keys()))
                .copied()
                .filter(|p| *p >= 0)
                .max();
            let partitions: Vec<PartitionFacts> = match highest {
                None => Vec::new(),
                Some(highest) => (0..=highest)
                    .map(|p| {
                        let seen = at_capture.as_ref().and_then(|m| m.get(&p));
                        let marks = seen.copied().flatten();
                        let marks_after = after.as_ref().and_then(|m| m.get(&p)).copied().flatten();
                        let range = archived.and_then(|m| m.get(&p)).copied();
                        PartitionFacts {
                            partition: u32::try_from(p).unwrap_or(u32::MAX),
                            observed: seen.is_some(),
                            log_start: marks.map(|m| m.log_start),
                            high_watermark: marks.map(|m| m.high_watermark),
                            log_start_after: marks_after.map(|m| m.log_start),
                            high_watermark_after: marks_after.map(|m| m.high_watermark),
                            archived_first: range.map(|r| r.0),
                            archived_last: range.map(|r| r.1),
                        }
                    })
                    .collect(),
            };
            let changed = model::changed_during_capture(&partitions);
            (
                topic.clone(),
                TopicPartitions {
                    partitions,
                    changed_during_capture: changed,
                },
            )
        })
        .collect()
}

/// A group that is not captured.
fn not_captured(outcome: &str, reason: &str) -> GroupSnapshot {
    GroupSnapshot {
        outcome: outcome.to_string(),
        reason: Some(reason.to_string()),
        group_type: (reason == model::GROUP_TYPE_NOT_CAPTURED)
            .then(|| model::OTHER_TYPE.to_string()),
        state: None,
        listed_state: None,
        members: None,
        active: None,
        positions: None,
    }
}

fn failed(reason: &str) -> GroupSnapshot {
    not_captured("failed", reason)
}

/// PROD-04.0b's classification failures, by name.
fn classification_failure(f: &GroupFailure) -> &'static str {
    match f {
        GroupFailure::NotVisibleToPrincipal => "NotVisibleToPrincipal",
        GroupFailure::ListingInconsistent(_) => "ListingInconsistent",
        GroupFailure::AbsenceUnproven(_) => "AbsenceUnproven",
        GroupFailure::TypeUnproven(_) => "TypeUnproven",
        GroupFailure::Unreachable { .. } => "Unreachable",
    }
}

fn description_failure(f: &DescribeFailure) -> &'static str {
    match f {
        DescribeFailure::NotAuthorized => "NotAuthorized",
        DescribeFailure::TypeDisagrees { .. } => "ListingInconsistent",
        DescribeFailure::NotRepresentable(_) | DescribeFailure::Failed { .. } => "DescribeFailed",
    }
}

fn positions_failure(e: &PositionsError) -> &'static str {
    match e {
        PositionsError::PositionsUnstable { .. } => "PositionsUnstable",
        PositionsError::NotVisibleOrUnreachable { .. } => "NotVisibleOrUnreachable",
        PositionsError::NotAuthorized { .. } => "NotAuthorized",
        PositionsError::NotVisibleToPrincipal { .. } => "NotVisibleToPrincipal",
        PositionsError::Failed { .. }
        | PositionsError::InvalidRequest(_)
        | PositionsError::Client(_) => "PositionsFailed",
    }
}

fn partition_failure(f: &PartitionFailure) -> &'static str {
    match f {
        PartitionFailure::TopicNotAuthorized => "TopicNotAuthorized",
        PartitionFailure::Unstable => "Unstable",
        PartitionFailure::NotAPosition { .. } => "NotAPosition",
        PartitionFailure::Other { .. } => "PartitionFailed",
    }
}

fn entry(topic: &str, p: u32, status: &str) -> PositionEntry {
    PositionEntry {
        topic: topic.to_string(),
        partition: p,
        status: status.to_string(),
        position: None,
        reason: None,
        coverage: None,
    }
}

/// One captured group's positions over every partition of every named topic.
fn position_entries(
    topics: &BTreeMap<String, TopicPartitions>,
    observation: &GroupsObservation,
    answered: &logweir_kafka::positions::GroupPositions,
) -> Vec<PositionEntry> {
    let mut out = Vec::new();
    for (topic, facts) in topics {
        // Whether the capture read the topic's partitions at all.
        let topic_read = matches!(observation.topics.get(topic), Some(Ok(_)));
        for f in &facts.partitions {
            let mut e = entry(topic, f.partition, "notObserved");
            if !f.observed {
                e.reason = Some(
                    if topic_read {
                        "PartitionAddedDuringCapture"
                    } else {
                        "TopicNotObserved"
                    }
                    .to_string(),
                );
                out.push(e);
                continue;
            }
            let tp = TopicPartition::new(topic.clone(), i32::try_from(f.partition).unwrap_or(-1));
            match answered.get(&tp) {
                Some(PartitionPosition::Committed(c)) => match model::relation(c.offset, f) {
                    Relation::MarksNotRead => {
                        e.status = "failed".into();
                        e.reason = Some(model::MARKS_NOT_READ.into());
                    }
                    Relation::BeyondEnd => {
                        e.status = "excluded".into();
                        e.position = Some(c.offset);
                        e.reason = Some(model::POSITION_BEYOND_END.into());
                    }
                    Relation::Coverage(word) => {
                        e.status = "captured".into();
                        e.position = Some(c.offset);
                        e.coverage = Some(word.into());
                    }
                },
                // NEVER offset 0 (PROD-04.0 T7).
                Some(PartitionPosition::NoCommittedPosition) => {
                    e.status = "noCommittedPosition".into();
                }
                Some(PartitionPosition::Failed(why)) => {
                    e.status = "failed".into();
                    e.reason = Some(partition_failure(why).into());
                }
                // The fetch was asked for every observed partition; one it did
                // not answer is a failure, never a missing entry.
                None => {
                    e.status = "failed".into();
                    e.reason = Some("PartitionFailed".into());
                }
            }
            out.push(e);
        }
    }
    out
}

/// One selected group's snapshot.
fn group_snapshot(
    observed: Option<&ObservedGroup>,
    topics: &BTreeMap<String, TopicPartitions>,
    observation: &GroupsObservation,
) -> GroupSnapshot {
    let Some(g) = observed else {
        return failed("CaptureUnavailable");
    };
    let capturable = match &g.verdict {
        GroupVerdict::Excluded(Excluded::GroupTypeNotCaptured { .. }) => {
            return not_captured("excluded", model::GROUP_TYPE_NOT_CAPTURED);
        }
        GroupVerdict::Excluded(Excluded::GroupNotFound { .. }) => {
            return not_captured("excluded", "GroupNotFound");
        }
        GroupVerdict::Failed(f) => return failed(classification_failure(f)),
        GroupVerdict::Capture(c) => c,
    };
    let description = match &g.description {
        None => return failed("DescribeFailed"),
        Some(Err(f)) => return failed(description_failure(f)),
        Some(Ok(d)) => d,
    };
    let answered = match &g.positions {
        None => return failed("PositionsFailed"),
        Some(Err(e)) => return failed(positions_failure(e)),
        Some(Ok(p)) => p,
    };
    let positions = position_entries(topics, observation, answered);
    // A kept position on a topic that changed during the capture.
    if positions.iter().any(|e| {
        e.position.is_some()
            && topics
                .get(&e.topic)
                .is_some_and(|t| t.changed_during_capture)
    }) {
        return failed(model::GENERATION_CHANGED);
    }
    let state = description.state.wire_name();
    let listed = capturable.state().wire_name();
    GroupSnapshot {
        outcome: "captured".to_string(),
        reason: None,
        group_type: Some(capturable.group_type().wire_name().to_string()),
        state: Some(state.to_string()),
        listed_state: Some(listed.to_string()),
        members: Some(u32::try_from(description.members.len()).unwrap_or(u32::MAX)),
        active: Some(model::active(state, listed)),
        positions: Some(positions),
    }
}

/// **The block.** See the module doc for the rules.
#[must_use]
pub fn build(c: &Capture<'_>) -> ConsumerPositions {
    let topics = topic_facts(c);
    let by_id: BTreeMap<&str, &ObservedGroup> = c
        .observation
        .groups
        .iter()
        .map(|g| (g.group_id.as_str(), g))
        .collect();
    let groups = c
        .selected
        .iter()
        .map(|id| {
            (
                id.clone(),
                group_snapshot(by_id.get(id.as_str()).copied(), &topics, c.observation),
            )
        })
        .collect();
    ConsumerPositions {
        observed_from: c.observed_from,
        observed_to: c.observed_to.max(c.observed_from),
        listing: match c.observation.completeness {
            Some(ListingCompleteness::Complete) => "complete",
            _ => "notComplete",
        }
        .to_string(),
        topics,
        groups,
    }
}

/// One log event per group, WARN for a group that is not captured, with what
/// the observation said: the receipt records the reason's NAME, and the log
/// says the rest.
pub fn log(block: &ConsumerPositions, observation: &GroupsObservation) {
    if let Some(why) = &observation.unavailable {
        tracing::warn!(
            reason = %why,
            "no consumer group was read, so every selected group is recorded failed \
             (CaptureUnavailable)"
        );
    }
    let detail: BTreeMap<&str, String> = observation
        .groups
        .iter()
        .map(|g| {
            let said = match (&g.verdict, &g.description, &g.positions) {
                (_, _, Some(Err(e))) => e.to_string(),
                (_, Some(Err(f)), _) => format!("{f:?}"),
                (v, _, _) => format!("{v:?}"),
            };
            (g.group_id.as_str(), said)
        })
        .collect();
    for (id, g) in &block.groups {
        let related = g
            .positions
            .iter()
            .flatten()
            .filter(|p| {
                p.coverage
                    .as_deref()
                    .is_some_and(|c| model::RELATED.contains(&c))
            })
            .count();
        match g.outcome.as_str() {
            "captured" => tracing::info!(
                group = %id,
                group_type = g.group_type.as_deref().unwrap_or(""),
                state = g.state.as_deref().unwrap_or(""),
                active = g.active.unwrap_or(true),
                positions_related_to_archive = related,
                "consumer group captured"
            ),
            outcome => tracing::warn!(
                group = %id,
                outcome,
                reason = g.reason.as_deref().unwrap_or(""),
                detail = detail.get(id.as_str()).map(String::as_str).unwrap_or(""),
                "consumer group not captured"
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    //! One row per rule; each names the mutant it kills. The block every row
    //! builds is also put through the receipt's own arms (`valid`), so a rule
    //! that wrote something the readers refuse fails here first.
    use super::*;
    use logweir_kafka::access::ClusterAccess;
    use logweir_kafka::capture::Marks;
    use logweir_kafka::groups::{
        code, GroupDescription, GroupListings, GroupState, GroupType, NameEntry, TypedEntry,
    };
    use logweir_kafka::positions::{CommittedPosition, GroupPositions};
    use std::time::Duration;

    const T: &str = "orders";

    fn marks(lo: i64, hi: i64) -> Result<Marks, String> {
        Ok(Marks {
            log_start: lo,
            high_watermark: hi,
        })
    }

    /// A capturable group of `ty` and listed `state`, from a real
    /// classification (only `classify` makes one).
    fn capturable(id: &str, ty: u32, state: u32) -> logweir_kafka::groups::CapturableGroup {
        let listings = GroupListings {
            typed: vec![TypedEntry {
                group_id: id.into(),
                is_simple: false,
                state,
                group_type: ty,
            }],
            typed_errors: vec![],
            names: vec![NameEntry {
                group_id: id.into(),
                error: 0,
            }],
            names_incomplete: None,
            access: ClusterAccess::Reported(vec![8]),
            unreadable_ids: 0,
        };
        match listings
            .classify(&[id.to_string()], &BTreeMap::new())
            .remove(0)
            .1
        {
            GroupVerdict::Capture(c) => c,
            other => panic!("not capturable: {other:?}"),
        }
    }

    fn described(id: &str, ty: GroupType, state: GroupState, members: usize) -> GroupDescription {
        GroupDescription {
            group_id: id.into(),
            group_type: ty,
            state,
            is_simple: false,
            partition_assignor: None,
            coordinator: Some(1),
            members: (0..members)
                .map(|_| logweir_kafka::groups::MemberDescription {
                    client_id: None,
                    consumer_id: None,
                    group_instance_id: None,
                    host: None,
                    assignment: vec![],
                    target_assignment: None,
                })
                .collect(),
        }
    }

    fn committed(offset: i64) -> PartitionPosition {
        PartitionPosition::Committed(CommittedPosition {
            offset,
            leader_epoch: None,
            metadata: Some(String::new()),
        })
    }

    /// A captured, Empty classic group answering `answers` for partitions 0..n.
    fn captured_group(id: &str, answers: Vec<PartitionPosition>) -> ObservedGroup {
        let c = capturable(id, code::TYPE_CLASSIC, code::STATE_EMPTY);
        ObservedGroup {
            group_id: id.into(),
            verdict: GroupVerdict::Capture(c),
            description: Some(Ok(described(id, GroupType::Classic, GroupState::Empty, 0))),
            positions: Some(Ok(GroupPositions {
                group: id.into(),
                partitions: answers
                    .into_iter()
                    .enumerate()
                    .map(|(p, a)| (TopicPartition::new(T, p as i32), a))
                    .collect(),
            })),
        }
    }

    fn observation(
        groups: Vec<ObservedGroup>,
        at_capture: Vec<(i32, Result<Marks, String>)>,
    ) -> GroupsObservation {
        GroupsObservation {
            completeness: Some(ListingCompleteness::Complete),
            groups,
            topics: [(T.to_string(), Ok(at_capture))].into_iter().collect(),
            unavailable: None,
        }
    }

    struct Fixture {
        selected: Vec<String>,
        topics: Vec<String>,
        observation: GroupsObservation,
        after: BTreeMap<String, TopicMarks>,
        archived: ArchivedRanges,
    }

    impl Fixture {
        fn new(observation: GroupsObservation, selected: &[&str]) -> Self {
            Self {
                selected: selected.iter().map(|s| s.to_string()).collect(),
                topics: vec![T.to_string()],
                observation,
                after: BTreeMap::new(),
                archived: BTreeMap::new(),
            }
        }
        fn build(&self) -> ConsumerPositions {
            let at = DateTime::<Utc>::from_timestamp(1_800_000_000, 0).expect("an instant");
            build(&Capture {
                selected: &self.selected,
                topics: &self.topics,
                observed_from: at,
                observed_to: at + chrono::Duration::milliseconds(250),
                observation: &self.observation,
                after: &self.after,
                archived: &self.archived,
            })
        }
    }

    /// The block inside a minimal 1.5.0 receipt satisfies every arm.
    fn valid(block: &ConsumerPositions) {
        let receipt: logweir_core::backup_receipt::BackupReceipt =
            serde_json::from_value(serde_json::json!({
                "format_version": "1.5.0",
                "run_id": "r", "backup_id": "b",
                "requested_at": "2027-01-15T08:00:00Z",
                "started_at": "2027-01-15T08:00:01Z",
                "finished_at": "2027-01-15T08:00:02Z",
                "exit_code": 0, "triggered_by": "",
                "source": {"cluster_id": "c", "bootstrap_servers": ["k:9092"],
                           "auth": {"mode": "plaintext", "username": null},
                           "topics": [T]},
                "engine": {"id": "oso-cli", "version": "v0.23.3", "digest": "sha256:00"},
                "archive": {"manifest_key": "m", "manifest_sha256": "sha256:00", "prefix": "p"},
                "records": {T: 1},
                "covered": {"from_ms": 1, "to_ms": 2},
                "consumer_positions": block,
            }))
            .expect("a receipt");
        assert_eq!(receipt.validate_invariants(), Ok(()), "{block:#?}");
    }

    fn group<'a>(b: &'a ConsumerPositions, id: &str) -> &'a GroupSnapshot {
        b.groups
            .get(id)
            .unwrap_or_else(|| panic!("no entry for {id}"))
    }

    /// Absent is never offset 0: a partition with no commit is
    /// `noCommittedPosition` with NO position, and every partition is listed.
    /// Kills: mapping NoCommittedPosition to a captured 0; dropping it.
    #[test]
    fn a_partition_without_a_commit_is_never_offset_zero_and_never_missing() {
        let mut f = Fixture::new(
            observation(
                vec![captured_group(
                    "g",
                    vec![committed(7), PartitionPosition::NoCommittedPosition],
                )],
                vec![(0, marks(0, 10)), (1, marks(0, 10))],
            ),
            &["g"],
        );
        f.archived
            .insert(T.into(), [(0, (0, 9)), (1, (0, 9))].into_iter().collect());
        let b = f.build();
        valid(&b);
        let positions = group(&b, "g").positions.as_ref().expect("captured");
        assert_eq!(positions.len(), 2);
        assert_eq!(positions[0].status, "captured");
        assert_eq!(positions[0].position, Some(7));
        assert_eq!(positions[0].coverage.as_deref(), Some("withinArchive"));
        assert_eq!(positions[1].status, "noCommittedPosition");
        assert_eq!(
            positions[1].position, None,
            "NEGATIVE CONTROL: absent read as 0"
        );
    }

    /// TI-04.1-2: a commit above the end is excluded `PositionBeyondEnd`, the
    /// end itself is captured. Kills: `>` -> `>=`, and dropping the check.
    #[test]
    fn a_position_beyond_the_end_is_excluded_and_the_end_itself_is_captured() {
        let f = Fixture::new(
            observation(
                vec![captured_group("g", vec![committed(10), committed(4)])],
                vec![(0, marks(0, 4)), (1, marks(0, 4))],
            ),
            &["g"],
        );
        let b = f.build();
        valid(&b);
        let p = group(&b, "g").positions.as_ref().expect("captured");
        assert_eq!(
            (p[0].status.as_str(), p[0].reason.as_deref(), p[0].position),
            ("excluded", Some("PositionBeyondEnd"), Some(10))
        );
        assert_eq!(
            (p[1].status.as_str(), p[1].coverage.as_deref()),
            ("captured", Some("noArchivedData"))
        );
    }

    /// Every outcome PROD-04.0 §5 names maps to the receipt's word, one entry
    /// per selected id, and an id the observation does not answer for is
    /// `CaptureUnavailable`. Kills: dropping an id; mapping a failure to
    /// `GroupNotFound`; mapping `GroupTypeNotCaptured` without `other`.
    #[test]
    fn every_verdict_maps_to_one_outcome_per_selected_id() {
        use logweir_kafka::groups::{Absence, OtherType};
        let excluded = |id: &str, e: Excluded| ObservedGroup {
            group_id: id.into(),
            verdict: GroupVerdict::Excluded(e),
            description: None,
            positions: None,
        };
        let failed_with = |id: &str, f: GroupFailure| ObservedGroup {
            group_id: id.into(),
            verdict: GroupVerdict::Failed(f),
            description: None,
            positions: None,
        };
        let mut unstable = captured_group("unstable", vec![]);
        unstable.positions = Some(Err(PositionsError::PositionsUnstable {
            group: "unstable".into(),
            bound: Duration::from_secs(2),
        }));
        let mut denied = captured_group("denied", vec![]);
        denied.description = Some(Err(DescribeFailure::NotAuthorized));
        denied.positions = None;
        let f = Fixture::new(
            observation(
                vec![
                    excluded(
                        "share",
                        Excluded::GroupTypeNotCaptured {
                            why: OtherType::NotInTypedListing,
                        },
                    ),
                    excluded(
                        "absent",
                        Excluded::GroupNotFound {
                            evidence: Absence::CompleteListing,
                        },
                    ),
                    failed_with("hidden", GroupFailure::NotVisibleToPrincipal),
                    unstable,
                    denied,
                ],
                vec![(0, marks(0, 1))],
            ),
            &[
                "share",
                "absent",
                "hidden",
                "unstable",
                "denied",
                "never-observed",
            ],
        );
        let b = f.build();
        valid(&b);
        let said = |id: &str| {
            let g = group(&b, id);
            (
                g.outcome.clone(),
                g.reason.clone(),
                g.group_type.clone(),
                g.positions.is_some(),
            )
        };
        let s = |x: &str| Some(x.to_string());
        assert_eq!(
            said("share"),
            (
                "excluded".into(),
                s("GroupTypeNotCaptured"),
                s("other"),
                false
            )
        );
        assert_eq!(
            said("absent"),
            ("excluded".into(), s("GroupNotFound"), None, false)
        );
        assert_eq!(
            said("hidden"),
            ("failed".into(), s("NotVisibleToPrincipal"), None, false)
        );
        assert_eq!(
            said("unstable"),
            ("failed".into(), s("PositionsUnstable"), None, false)
        );
        assert_eq!(
            said("denied"),
            ("failed".into(), s("NotAuthorized"), None, false)
        );
        assert_eq!(
            said("never-observed"),
            ("failed".into(), s("CaptureUnavailable"), None, false)
        );
        assert_eq!(b.groups.len(), 6, "exactly one entry per selected id");
    }

    /// A reader that observes nothing records every selected group failed,
    /// never absent. Kills: an empty `groups` map; a GroupNotFound default.
    #[test]
    fn an_unavailable_capture_fails_every_selected_group() {
        let topics = vec![T.to_string()];
        let mut f = Fixture::new(
            GroupsObservation::unavailable(&topics, "no reader"),
            &["a", "b"],
        );
        f.archived
            .insert(T.into(), [(0, (0, 3))].into_iter().collect());
        let b = f.build();
        valid(&b);
        assert_eq!(b.listing, "notComplete");
        for id in ["a", "b"] {
            assert_eq!(group(&b, id).reason.as_deref(), Some("CaptureUnavailable"));
        }
        // The archived partition is listed, unobserved.
        assert!(!b.topics[T].partitions[0].observed);
    }

    /// A partition the capture did not read (added during it) is
    /// `notObserved: PartitionAddedDuringCapture`, never dropped and never 0;
    /// a topic the capture could not read is `TopicNotObserved`. Kills:
    /// sizing the list from the capture-time partitions alone.
    #[test]
    fn a_partition_added_during_the_capture_is_listed_not_observed() {
        let mut f = Fixture::new(
            observation(
                vec![captured_group("g", vec![committed(3)])],
                vec![(0, marks(0, 5))],
            ),
            &["g"],
        );
        f.after
            .insert(T.into(), Ok(vec![(0, marks(0, 6)), (1, marks(0, 2))]));
        f.archived
            .insert(T.into(), [(0, (0, 5)), (1, (0, 1))].into_iter().collect());
        let b = f.build();
        valid(&b);
        let p = group(&b, "g").positions.as_ref().expect("captured");
        assert_eq!(p.len(), 2, "NEGATIVE CONTROL: the added partition dropped");
        assert_eq!(
            (p[1].status.as_str(), p[1].reason.as_deref(), p[1].position),
            ("notObserved", Some("PartitionAddedDuringCapture"), None)
        );
        assert!(b.topics[T].partitions[0].observed && !b.topics[T].partitions[1].observed);

        // The capture could not read the topic at all.
        let mut o = observation(vec![captured_group("g", vec![])], vec![]);
        o.topics.insert(T.into(), Err("metadata refused".into()));
        let mut f = Fixture::new(o, &["g"]);
        f.archived
            .insert(T.into(), [(0, (0, 5))].into_iter().collect());
        let b = f.build();
        valid(&b);
        let p = group(&b, "g").positions.as_ref().expect("captured");
        assert_eq!(p[0].reason.as_deref(), Some("TopicNotObserved"));
    }

    /// TI-04.1-3: marks that regress after the engine fail the groups that
    /// hold a kept position on the topic; stable marks capture them. Kills:
    /// ignoring `changed_during_capture`; failing a group with no position.
    #[test]
    fn a_topic_that_changed_during_the_capture_fails_the_groups_holding_positions_on_it() {
        let o = observation(
            vec![
                captured_group("holds", vec![committed(3)]),
                captured_group("holds-none", vec![PartitionPosition::NoCommittedPosition]),
            ],
            vec![(0, marks(0, 8))],
        );
        let mut f = Fixture::new(o, &["holds", "holds-none"]);
        f.after.insert(T.into(), Ok(vec![(0, marks(0, 2))]));
        let b = f.build();
        valid(&b);
        assert!(b.topics[T].changed_during_capture);
        assert_eq!(
            group(&b, "holds").reason.as_deref(),
            Some("GenerationChangedDuringCapture")
        );
        assert_eq!(group(&b, "holds-none").outcome, "captured");
        // The control: stable marks.
        f.after.insert(T.into(), Ok(vec![(0, marks(0, 9))]));
        let b = f.build();
        valid(&b);
        assert!(!b.topics[T].changed_during_capture);
        assert_eq!(group(&b, "holds").outcome, "captured");
    }

    /// Marks not read: the position is failed `MarksNotRead`, never judged
    /// against nothing. Kills: defaulting missing marks to 0 or to the
    /// position.
    #[test]
    fn a_position_whose_marks_were_not_read_is_never_judged() {
        let f = Fixture::new(
            observation(
                vec![captured_group("g", vec![committed(3)])],
                vec![(0, Err("timed out".into()))],
            ),
            &["g"],
        );
        let b = f.build();
        valid(&b);
        let p = &group(&b, "g").positions.as_ref().expect("captured")[0];
        assert_eq!(
            (p.status.as_str(), p.reason.as_deref(), p.position),
            ("failed", Some("MarksNotRead"), None)
        );
    }

    /// A group whose description shows a member the listing did not (a
    /// rebalance during the capture) is active. Kills: reading the listing's
    /// state alone.
    #[test]
    fn a_group_that_gained_a_member_during_the_capture_is_active() {
        let mut g = captured_group("g", vec![committed(1)]);
        g.description = Some(Ok(described(
            "g",
            GroupType::Classic,
            GroupState::PreparingRebalance,
            1,
        )));
        let f = Fixture::new(observation(vec![g], vec![(0, marks(0, 2))]), &["g"]);
        let b = f.build();
        valid(&b);
        let g = group(&b, "g");
        assert_eq!(g.listed_state.as_deref(), Some("Empty"));
        assert_eq!(g.state.as_deref(), Some("PreparingRebalance"));
        assert_eq!(g.members, Some(1));
        assert_eq!(
            g.active,
            Some(true),
            "NEGATIVE CONTROL: the listing's Empty"
        );
    }

    /// A partition the fetch did not answer for is `failed: PartitionFailed`,
    /// never `noCommittedPosition`: an unanswered partition says nothing about
    /// what the group committed. Kills: reading a missing answer as "no
    /// committed offset".
    #[test]
    fn a_partition_the_fetch_did_not_answer_is_failed_never_uncommitted() {
        let f = Fixture::new(
            observation(
                vec![captured_group("g", vec![committed(1)])],
                vec![(0, marks(0, 5)), (1, marks(0, 5))],
            ),
            &["g"],
        );
        let b = f.build();
        valid(&b);
        let p = &group(&b, "g").positions.as_ref().expect("captured")[1];
        assert_eq!(
            (p.status.as_str(), p.reason.as_deref(), p.position),
            ("failed", Some("PartitionFailed"), None)
        );
    }

    /// The manifest's ranges: only well-formed ones, per partition, from the
    /// lowest start to the highest end. Kills: min/max swapped; a negative
    /// range recorded (the receipt would then fail its own arm 25).
    #[test]
    fn archived_ranges_take_the_lowest_start_and_highest_end_and_drop_nonsense() {
        let seg = |s: i64, e: i64| logweir_core::engine::SegmentFacts {
            key: String::new(),
            start_offset: s,
            end_offset: e,
            start_timestamp: 0,
            end_timestamp: 0,
            record_count: 1,
            sha256: String::new(),
            uploaded_at: 0,
        };
        let part = |id: i32, segs: Vec<logweir_core::engine::SegmentFacts>| {
            logweir_core::engine::PartitionFacts {
                partition_id: id,
                segments: segs,
                gaps: vec![],
                pruned: vec![],
            }
        };
        let ranges = archived_ranges(&[
            part(0, vec![seg(10, 19), seg(0, 9), seg(20, 25)]),
            part(1, vec![]),
            part(2, vec![seg(-5, 3)]),
            part(3, vec![seg(9, 4)]),
        ]);
        assert_eq!(ranges, [(0, (0, 25))].into_iter().collect());
    }
}
