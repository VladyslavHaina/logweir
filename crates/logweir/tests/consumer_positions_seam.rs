//! **PROD-04.1, end to end through `backup run`'s in-process seam**: the
//! selection (plan and `--consumer-group`), phase −1's refusal, the capture
//! before the engine, the marks after it, the receipt's 1.5.0 block, the
//! catalog point's summary — with a `ClusterReader` double and no socket.
//!
//! | row | proves | negative control |
//! |---|---|---|
//! | `a_selected_group_is_recorded_in_the_signed_receipt_and_its_catalog_point` | one outcome per selected id (plan then CLI), a captured group's position judged against the archive (`withinArchive`), a share group `GroupTypeNotCaptured`, the receipt 1.5.0 and VALID, the catalog point's summary bound by the block's digest | the same run selecting nothing writes a 1.3.0 receipt that never names the block |
//! | `a_reader_that_cannot_read_groups_fails_every_selected_group` | the default `ClusterReader` (every double written before PROD-04.1) records each selected group `failed: CaptureUnavailable` | absence read as offset 0, or the group dropped, would fail arm 32 or the count |
//! | `a_selection_that_is_not_one_is_refused_before_anything_runs` | a group selected twice (plan and CLI), a blank id and a control character are refused at phase −1, exit 3, before the engine | a valid selection runs |
//! | `a_topic_recreated_during_the_capture_fails_the_groups_holding_positions_on_it` | marks after the engine below the group-capture marks (TI-04.1-3): the group is `GenerationChangedDuringCapture` | stable marks capture it |
#[path = "backup_seam/mod.rs"]
mod backup_seam;

use backup_seam::{Fixture, StubReader};
use logweir::exit::ExitCode;
use logweir_engine_oso::storage::Store;
use logweir_kafka::access::ClusterAccess;
use logweir_kafka::capture::{GroupsObservation, Marks, ObservedGroup, TopicMarks};
use logweir_kafka::groups::{
    code, Excluded, GroupDescription, GroupListings, GroupState, GroupType, GroupVerdict,
    NameEntry, OtherType, TypedEntry,
};
use logweir_kafka::positions::{
    CommittedPosition, GroupPositions, PartitionPosition, TopicPartition,
};
use logweir_kafka::reader::{ClusterReader, ConsumedRecord, KafkaError, TopicMeta};
use std::collections::BTreeMap;

/// A reader double that answers a capture: `captured` classic Empty groups
/// each committed at `position` on `orders` 0, `share` groups typed other,
/// and the marks given, at capture and after the engine.
struct GroupsReader {
    captured: Vec<&'static str>,
    share: Vec<&'static str>,
    position: i64,
    marks: (i64, i64),
    after: (i64, i64),
}

fn capturable(id: &str) -> logweir_kafka::groups::CapturableGroup {
    let listings = GroupListings {
        typed: vec![TypedEntry {
            group_id: id.into(),
            is_simple: false,
            state: code::STATE_EMPTY,
            group_type: code::TYPE_CLASSIC,
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
        other => panic!("{other:?}"),
    }
}

impl ClusterReader for GroupsReader {
    fn cluster_id(&self) -> Result<String, KafkaError> {
        StubReader.cluster_id()
    }
    fn list_topics(&self) -> Result<Vec<TopicMeta>, KafkaError> {
        Ok(vec![])
    }
    fn end_offsets(&self, _: &str) -> Result<Vec<(i32, i64)>, KafkaError> {
        Ok(vec![])
    }
    fn topic_configs(&self, _: &str) -> Result<BTreeMap<String, String>, KafkaError> {
        Ok(BTreeMap::new())
    }
    fn broker_configs(&self) -> Result<BTreeMap<String, String>, KafkaError> {
        Ok(BTreeMap::new())
    }
    fn consume_range(
        &self,
        _: &str,
        _: i32,
        _: i64,
        _: usize,
    ) -> Result<Vec<ConsumedRecord>, KafkaError> {
        Ok(vec![])
    }
    fn observe_consumer_groups(&self, selected: &[String], topics: &[String]) -> GroupsObservation {
        assert_eq!(topics, ["orders"]);
        let groups = selected
            .iter()
            .map(|id| {
                if self.captured.contains(&id.as_str()) {
                    ObservedGroup {
                        group_id: id.clone(),
                        verdict: GroupVerdict::Capture(capturable(id)),
                        description: Some(Ok(GroupDescription {
                            group_id: id.clone(),
                            group_type: GroupType::Classic,
                            state: GroupState::Empty,
                            is_simple: false,
                            partition_assignor: None,
                            coordinator: Some(1),
                            members: vec![],
                        })),
                        positions: Some(Ok(GroupPositions {
                            group: id.clone(),
                            partitions: vec![(
                                TopicPartition::new("orders", 0),
                                PartitionPosition::Committed(CommittedPosition {
                                    offset: self.position,
                                    leader_epoch: None,
                                    metadata: Some(String::new()),
                                }),
                            )],
                        })),
                    }
                } else if self.share.contains(&id.as_str()) {
                    ObservedGroup {
                        group_id: id.clone(),
                        verdict: GroupVerdict::Excluded(Excluded::GroupTypeNotCaptured {
                            why: OtherType::NotInTypedListing,
                        }),
                        description: None,
                        positions: None,
                    }
                } else {
                    panic!("the double knows no group {id}")
                }
            })
            .collect();
        GroupsObservation {
            completeness: Some(logweir_kafka::groups::ListingCompleteness::Complete),
            groups,
            topics: [("orders".to_string(), marks(self.marks))]
                .into_iter()
                .collect(),
            unavailable: None,
        }
    }
    fn partition_marks(&self, topics: &[String]) -> BTreeMap<String, TopicMarks> {
        topics
            .iter()
            .map(|t| (t.clone(), marks(self.after)))
            .collect()
    }
}

fn marks((log_start, high_watermark): (i64, i64)) -> TopicMarks {
    Ok(vec![(
        0,
        Ok(Marks {
            log_start,
            high_watermark,
        }),
    )])
}

fn evidence() -> Store {
    Store::in_memory("logweir/")
}

#[test]
fn a_selected_group_is_recorded_in_the_signed_receipt_and_its_catalog_point() {
    let f = Fixture::new();
    f.select_in_plan(&["billing", "share-1"]);
    let reader = GroupsReader {
        captured: vec!["billing", "cli-group"],
        share: vec!["share-1"],
        // The engine double archives offsets 0..=1233 of `orders` 0.
        position: 1000,
        marks: (0, 1234),
        after: (0, 1240),
    };
    let store = evidence();
    let outcome = f
        .execute_reading(&store, &reader, vec!["cli-group".into()])
        .expect("the backup succeeds");
    let block = outcome
        .consumer_positions
        .as_ref()
        .expect("a selecting run carries the block");
    let ids: Vec<&str> = block.groups.keys().map(String::as_str).collect();
    assert_eq!(
        ids,
        ["billing", "cli-group", "share-1"],
        "plan and CLI, one each"
    );
    let billing = &block.groups["billing"];
    assert_eq!(billing.outcome, "captured");
    let p = &billing.positions.as_ref().unwrap()[0];
    assert_eq!(
        (p.status.as_str(), p.position, p.coverage.as_deref()),
        ("captured", Some(1000), Some("withinArchive"))
    );
    assert_eq!(
        block.topics["orders"].partitions[0].archived_last,
        Some(1233)
    );
    assert_eq!(
        block.topics["orders"].partitions[0].high_watermark_after,
        Some(1240)
    );
    assert_eq!(
        block.groups["share-1"].reason.as_deref(),
        Some("GroupTypeNotCaptured")
    );

    // The SIGNED receipt carries it, as 1.5.0, and satisfies every arm.
    let (bytes, _) = store
        .get(&outcome.receipt_key)
        .expect("the receipt was put");
    let receipt: logweir_core::backup_receipt::BackupReceipt =
        serde_json::from_slice(&bytes).unwrap();
    assert_eq!(receipt.format_version, "1.5.0");
    assert_eq!(receipt.validate_invariants(), Ok(()));
    assert_eq!(receipt.consumer_positions.as_ref(), Some(block));

    // The catalog point binds it by digest.
    let key = outcome
        .catalog_key
        .as_ref()
        .expect("the catalog point was written");
    let (record, _) = store.get(key).unwrap();
    let point: logweir::catalog::record::CatalogPoint = serde_json::from_slice(&record).unwrap();
    let summary = point.consumer_positions.expect("the summary travels");
    assert_eq!(summary.sha256, block.digest().unwrap());
    assert_eq!(point.format_version, "1.5.0");

    // CONTROL: the same backup selecting nothing writes the receipt it wrote
    // before PROD-04.1.
    let f = Fixture::new();
    let store = evidence();
    let outcome = f
        .execute_reading(&store, &reader, Vec::new())
        .expect("the backup succeeds");
    assert!(outcome.consumer_positions.is_none());
    let (bytes, _) = store.get(&outcome.receipt_key).unwrap();
    let text = String::from_utf8(bytes).unwrap();
    assert!(!text.contains("consumer_positions"), "{text}");
    let plain: logweir_core::backup_receipt::BackupReceipt = serde_json::from_str(&text).unwrap();
    assert_eq!(plain.format_version, "1.3.0");
}

#[test]
fn a_reader_that_cannot_read_groups_fails_every_selected_group() {
    let f = Fixture::new();
    f.select_in_plan(&["billing", "audit"]);
    let store = evidence();
    let outcome = f
        .execute_reading(&store, &StubReader, Vec::new())
        .expect("the backup still succeeds: a capture is never fatal");
    let block = outcome.consumer_positions.expect("the block");
    assert_eq!(block.groups.len(), 2, "NEGATIVE CONTROL: a group dropped");
    for g in block.groups.values() {
        assert_eq!(
            (
                g.outcome.as_str(),
                g.reason.as_deref(),
                g.positions.is_none()
            ),
            ("failed", Some("CaptureUnavailable"), true)
        );
    }
    assert_eq!(block.listing, "notComplete");
}

#[test]
fn a_selection_that_is_not_one_is_refused_before_anything_runs() {
    let reader = GroupsReader {
        captured: vec!["billing"],
        share: vec![],
        position: 1,
        marks: (0, 2),
        after: (0, 2),
    };
    for (plan, cli) in [
        (vec!["billing"], vec!["billing".to_string()]),
        (vec!["billing"], vec![" ".to_string()]),
        (vec!["billing"], vec!["a\u{7}b".to_string()]),
    ] {
        let f = Fixture::new();
        f.select_in_plan(&plan);
        let store = evidence();
        match f.execute_reading(&store, &reader, cli.clone()) {
            Err(e) => {
                assert_eq!(e.exit_code(), ExitCode::GuardRefused, "{cli:?}: {e}");
                assert!(e.to_string().contains("consumer_groups"), "{e}");
            }
            Ok(_) => panic!("{cli:?} must be refused"),
        }
        assert!(
            store
                .list_page("logweir/backups/", None, 10)
                .unwrap()
                .0
                .is_empty(),
            "nothing was written"
        );
    }
    let f = Fixture::new();
    f.select_in_plan(&["billing"]);
    assert!(f.execute_reading(&evidence(), &reader, Vec::new()).is_ok());
}

#[test]
fn a_topic_recreated_during_the_capture_fails_the_groups_holding_positions_on_it() {
    let mut reader = GroupsReader {
        captured: vec!["billing"],
        share: vec![],
        position: 1000,
        marks: (0, 1234),
        // Recreated and refilled to 500: the end regressed.
        after: (0, 500),
    };
    let f = Fixture::new();
    f.select_in_plan(&["billing"]);
    let outcome = f.execute_reading(&evidence(), &reader, Vec::new()).unwrap();
    let block = outcome.consumer_positions.unwrap();
    assert!(block.topics["orders"].changed_during_capture);
    assert_eq!(
        block.groups["billing"].reason.as_deref(),
        Some("GenerationChangedDuringCapture")
    );
    // CONTROL: stable marks.
    reader.after = (0, 1234);
    let f = Fixture::new();
    f.select_in_plan(&["billing"]);
    let outcome = f.execute_reading(&evidence(), &reader, Vec::new()).unwrap();
    assert_eq!(
        outcome.consumer_positions.unwrap().groups["billing"].outcome,
        "captured"
    );
}
