//! **PROD-04.1, end to end through `backup run`'s in-process seam**: the
//! selection (plan and `--consumer-group`), phase −1's refusal, the capture
//! before the engine, the marks after it, the receipt's 1.7.0 block, the
//! catalog point's summary — with a `ClusterReader` double and no socket.
//!
//! | row | proves | negative control |
//! |---|---|---|
//! | `a_selected_group_is_recorded_in_the_signed_receipt_and_its_catalog_point` | one outcome per selected id (plan then CLI), a captured group's position — in the positions document put beside the receipt, which the receipt binds by digest — judged against the archive (`withinArchive`), a share group `GroupTypeNotCaptured`, the receipt 1.7.0 and VALID, the document VALID against it, the catalog point's summary bound by the block's digest | the same run selecting nothing writes a receipt that never names the block, and no document |
//! | `a_reader_that_cannot_read_groups_fails_every_selected_group` | the default `ClusterReader` (every double written before PROD-04.1) records each selected group `failed: CaptureUnavailable` | absence read as offset 0, or the group dropped, would fail arm 26, CP-11 or the count |
//! | `a_selection_that_is_not_one_is_refused_before_anything_runs` | a group selected twice (plan and CLI), a blank id and a control character are refused at phase −1, exit 3, before the engine | a valid selection runs |
//! | `a_topic_recreated_during_the_capture_fails_the_groups_holding_positions_on_it` | marks after the engine below the group-capture marks (TI-04.1-3): the group is `GenerationChangedDuringCapture` | stable marks capture it |
//! | `a_selection_whose_summary_could_exceed_the_cap_as_encoded_is_refused_by_name` | 85 ids of 255 `"` are refused at phase −1 by name, exit 3, nothing written (review N1) | 84 fit (core and builder rows) |
//! | `a_capture_that_asked_no_position_reads_no_marks_after_the_engine` | every selected group excluded: no mark is read after the engine (review L8) | one captured group: the marks are read once |
//! | `a_positions_document_over_the_read_cap_is_refused_naming_the_cap_and_nothing_is_read` | FX-31 over this row's document: read under `caps::SIGNED_DOCUMENT`, and one the store reports a byte over it is `TooLarge` naming the key and the cap, with no body byte taken and nothing held against the receipt | the same bytes in an honest store are read whole and hold against the signed receipt |
#[path = "backup_seam/mod.rs"]
mod backup_seam;

use backup_seam::{Fixture, StubReader};
use logweir::exit::ExitCode;
use logweir_core::backup_receipt::BackupReceipt;
use logweir_core::consumer_positions::PositionsDocument;
use logweir_engine_oso::storage::{caps, OverCap, Store, StoreError};
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

/// The positions document `receipt` binds, read back from `store` and held
/// against the signed receipt.
///
/// **FX-31.** It is read under `caps::SIGNED_DOCUMENT`, the cap a runner or
/// CLI reads a signed evidence document with: the document is not signed
/// itself, the receipt's signature covers its digest and length. Over the cap
/// the read is `TooLarge` and this returns before anything is parsed or held
/// against the receipt: there are no bytes to hand either.
fn positions_document(
    store: &Store,
    receipt: &BackupReceipt,
) -> Result<(Vec<u8>, PositionsDocument), StoreError> {
    let block = receipt
        .consumer_positions
        .as_ref()
        .expect("the receipt binds a positions document");
    let (bytes, _) = store.get_capped(&block.document.key, caps::SIGNED_DOCUMENT)?;
    let doc: PositionsDocument = serde_json::from_slice(&bytes).expect("a positions document");
    assert_eq!(
        receipt.validate_consumer_positions_document(&bytes, &doc),
        Ok(())
    );
    Ok((bytes, doc))
}

/// The minor of a `1.x.y` version.
fn minor_of(version: &str) -> u64 {
    version
        .split('.')
        .nth(1)
        .and_then(|m| m.parse().ok())
        .unwrap_or_else(|| panic!("{version} is not 1.x.y"))
}

/// Whether `version` is major 1 at a minor of at least `since`: the version
/// that defines a field, or a later one.
fn defines(version: &str, since: u64) -> bool {
    version.starts_with("1.") && minor_of(version) >= since
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
    assert_eq!(billing.counts.map(|c| c.related), Some(1));
    assert_eq!(
        block.groups["share-1"].reason.as_deref(),
        Some("GroupTypeNotCaptured")
    );

    // The SIGNED receipt carries it, as 1.7.0, and satisfies every arm.
    let (bytes, _) = store
        .get_capped(&outcome.receipt_key, caps::SIGNED_DOCUMENT)
        .expect("the receipt was put");
    let receipt: BackupReceipt = serde_json::from_slice(&bytes).unwrap();
    // AT LEAST the minor that defines the block: a later minor that also
    // defines it (a renumber at integration) is the same claim.
    assert!(
        defines(
            &receipt.format_version,
            logweir_core::backup_receipt::CONSUMER_POSITIONS_SINCE_MINOR
        ),
        "{}",
        receipt.format_version
    );
    assert_eq!(receipt.validate_invariants(), Ok(()));
    assert_eq!(receipt.consumer_positions.as_ref(), Some(block));

    // The positions are in the document beside it, bound by digest, and the
    // document holds against the signed receipt.
    assert_eq!(
        block.document.key,
        "logweir/backups/nightly-20260915/01J9X2QK7C4V0R8YB3ZP6MTS5A.consumer-positions.json"
    );
    let (doc_bytes, doc) =
        positions_document(&store, &receipt).expect("the positions document was put");
    assert_eq!(
        outcome.consumer_positions_document.as_deref(),
        Some(doc_bytes.as_slice())
    );
    let p = &doc.groups["billing"].positions[0];
    assert_eq!(
        (p.status.as_str(), p.position, p.coverage.as_deref()),
        ("captured", Some(1000), Some("withinArchive"))
    );
    assert_eq!(doc.topics["orders"].partitions[0].archived_last, Some(1233));
    assert_eq!(
        doc.topics["orders"].partitions[0].high_watermark_after,
        Some(1240)
    );
    assert!(!doc.groups.contains_key("share-1"), "only captured groups");

    // The catalog point binds it by digest.
    let key = outcome
        .catalog_key
        .as_ref()
        .expect("the catalog point was written");
    let (record, _) = store.get_capped(key, caps::SIGNED_DOCUMENT).unwrap();
    let point: logweir::catalog::record::CatalogPoint = serde_json::from_slice(&record).unwrap();
    let summary = point.consumer_positions.expect("the summary travels");
    assert_eq!(summary.sha256, block.digest().unwrap());
    assert!(
        defines(
            &point.format_version,
            minor_of(logweir::catalog::record::FORMAT_VERSION_WITH_CONSUMER_POSITIONS)
        ),
        "{}",
        point.format_version
    );

    // CONTROL: the same backup selecting nothing writes the receipt it would
    // without PROD-04.1.
    let f = Fixture::new();
    let store = evidence();
    let outcome = f
        .execute_reading(&store, &reader, Vec::new())
        .expect("the backup succeeds");
    assert!(outcome.consumer_positions.is_none());
    assert!(outcome.consumer_positions_document.is_none());
    let (bytes, _) = store
        .get_capped(&outcome.receipt_key, caps::SIGNED_DOCUMENT)
        .unwrap();
    let text = String::from_utf8(bytes).unwrap();
    assert!(!text.contains("consumer_positions"), "{text}");
    // No block and no document; the version is whatever this build writes
    // without a selection (its other blocks decide it), never pinned here.
    let plain: BackupReceipt = serde_json::from_str(&text).unwrap();
    assert!(plain.consumer_positions.is_none());
    assert_eq!(plain.validate_invariants(), Ok(()));
    let keys = store.list_page("logweir/backups/", None, 10).unwrap().0;
    assert!(
        keys.iter()
            .all(|k| !k.ends_with(".consumer-positions.json")),
        "no document without a selection: {keys:?}"
    );
}

/// **FX-31 over PROD-04.1's document (integrate-6).** The positions document
/// is read under `caps::SIGNED_DOCUMENT`, and one the store reports over that
/// cap is refused `TooLarge`, naming the key and the cap, before a body byte
/// is taken: nothing is parsed and nothing is held against the receipt.
///
/// The object IS the run's own valid document, the very bytes the control
/// reads and verifies, so the refusal is the cap's and not the document's.
///
/// KILLS: the read moved to a cap far too large (`u64::MAX` hands the bytes
/// back as `Ok`, and the document then holds against the receipt); a refusal
/// that still read the body.
#[test]
fn a_positions_document_over_the_read_cap_is_refused_naming_the_cap_and_nothing_is_read() {
    let f = Fixture::new();
    f.select_in_plan(&["billing"]);
    let reader = GroupsReader {
        captured: vec!["billing"],
        share: Vec::new(),
        position: 1000,
        marks: (0, 1234),
        after: (0, 1240),
    };
    let store = evidence();
    let outcome = f
        .execute_reading(&store, &reader, Vec::new())
        .expect("the backup succeeds");
    let (bytes, _) = store
        .get_capped(&outcome.receipt_key, caps::SIGNED_DOCUMENT)
        .expect("the receipt was put");
    let receipt: BackupReceipt = serde_json::from_slice(&bytes).unwrap();
    let key = receipt
        .consumer_positions
        .as_ref()
        .expect("a selecting run binds a document")
        .document
        .key
        .clone();

    // CONTROL: the document as the run put it is read whole under the cap and
    // holds against the signed receipt.
    let (document, _) =
        positions_document(&store, &receipt).expect("within the cap the document is read");
    assert_eq!(
        outcome.consumer_positions_document.as_deref(),
        Some(document.as_slice())
    );

    // The same bytes at the same key, in a store that reports the object one
    // byte over the cap.
    let over = caps::SIGNED_DOCUMENT + 1;
    let (reporting_over, meter) = Store::in_memory_misreporting_size("logweir/", over);
    reporting_over
        .put_create_only(&key, &document)
        .expect("the same document is planted");
    match positions_document(&reporting_over, &receipt) {
        Err(StoreError::TooLarge {
            key: refused,
            cap,
            observed,
        }) => {
            assert_eq!(refused, key);
            assert_eq!(cap, caps::SIGNED_DOCUMENT);
            assert_eq!(observed, OverCap::Reported(over));
        }
        Ok(_) => panic!("a positions document over the read cap was read and verified"),
        Err(other) => panic!("over the cap is TooLarge, got {other:?}"),
    }
    let message = positions_document(&reporting_over, &receipt)
        .map(|_| ())
        .unwrap_err()
        .to_string();
    assert!(
        message.contains(&format!(
            "is larger than the {}-byte read cap",
            caps::SIGNED_DOCUMENT
        )) && message.contains(&format!("the store reports {over} bytes"))
            && message.contains(&key),
        "the refusal names the key, the cap and the reported size: {message}"
    );
    assert_eq!(
        meter.streamed(),
        0,
        "not one body byte was taken, so nothing reached the receipt's check"
    );
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
            (g.outcome.as_str(), g.reason.as_deref(), g.counts.is_none()),
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

/// **Review N1 at phase −1.** 85 ids of 255 `"` — legal Kafka group ids, 85
/// of 100, each 255 bytes — would make a summary over the cap as JSON writes
/// it: the run is refused by name, exit 3, before anything runs, and nothing
/// is written. Never a truncated summary.
#[test]
fn a_selection_whose_summary_could_exceed_the_cap_as_encoded_is_refused_by_name() {
    let reader = GroupsReader {
        captured: vec![],
        share: vec![],
        position: 1,
        marks: (0, 2),
        after: (0, 2),
    };
    let ids: Vec<String> = (0..85)
        .map(|i| format!("{i:03}{}", "\"".repeat(252)))
        .collect();
    let f = Fixture::new();
    let store = evidence();
    match f.execute_reading(&store, &reader, ids) {
        Err(e) => {
            assert_eq!(e.exit_code(), ExitCode::GuardRefused, "{e}");
            assert!(
                e.to_string().contains("ConsumerGroupSelectionTooLarge"),
                "{e}"
            );
            assert!(e.to_string().contains("bytes as JSON writes it"), "{e}");
        }
        Ok(_) => panic!("an over-cap selection must be refused"),
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
    let doc: logweir_core::consumer_positions::PositionsDocument =
        serde_json::from_slice(&outcome.consumer_positions_document.unwrap()).unwrap();
    assert!(doc.topics["orders"].changed_during_capture);
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

/// A [`GroupsReader`] that counts the reads after the engine.
struct CountingReader {
    inner: GroupsReader,
    marks_reads: std::sync::atomic::AtomicUsize,
}

impl ClusterReader for CountingReader {
    fn cluster_id(&self) -> Result<String, KafkaError> {
        self.inner.cluster_id()
    }
    fn list_topics(&self) -> Result<Vec<TopicMeta>, KafkaError> {
        self.inner.list_topics()
    }
    fn end_offsets(&self, t: &str) -> Result<Vec<(i32, i64)>, KafkaError> {
        self.inner.end_offsets(t)
    }
    fn topic_configs(&self, t: &str) -> Result<BTreeMap<String, String>, KafkaError> {
        self.inner.topic_configs(t)
    }
    fn broker_configs(&self) -> Result<BTreeMap<String, String>, KafkaError> {
        self.inner.broker_configs()
    }
    fn consume_range(
        &self,
        t: &str,
        p: i32,
        o: i64,
        n: usize,
    ) -> Result<Vec<ConsumedRecord>, KafkaError> {
        self.inner.consume_range(t, p, o, n)
    }
    fn observe_consumer_groups(&self, selected: &[String], topics: &[String]) -> GroupsObservation {
        self.inner.observe_consumer_groups(selected, topics)
    }
    fn partition_marks(&self, topics: &[String]) -> BTreeMap<String, TopicMarks> {
        self.marks_reads
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        self.inner.partition_marks(topics)
    }
}

/// **Review L8.** A capture that described no group — every selected group
/// excluded, as on a broker that types none — asked no position, so the run
/// reads no marks after the engine: an unavailable leader costs it nothing.
/// The control: one captured group, and the marks are read once.
#[test]
fn a_capture_that_asked_no_position_reads_no_marks_after_the_engine() {
    let reader = CountingReader {
        inner: GroupsReader {
            captured: vec![],
            share: vec!["share-1"],
            position: 0,
            marks: (0, 1),
            after: (0, 1),
        },
        marks_reads: std::sync::atomic::AtomicUsize::new(0),
    };
    let f = Fixture::new();
    f.select_in_plan(&["share-1"]);
    let outcome = f.execute_reading(&evidence(), &reader, Vec::new()).unwrap();
    assert_eq!(
        outcome.consumer_positions.unwrap().groups["share-1"]
            .reason
            .as_deref(),
        Some("GroupTypeNotCaptured")
    );
    assert_eq!(
        reader.marks_reads.load(std::sync::atomic::Ordering::SeqCst),
        0,
        "no group was described, so no mark is read"
    );
    let reader = CountingReader {
        inner: GroupsReader {
            captured: vec!["billing"],
            share: vec![],
            position: 1000,
            marks: (0, 1234),
            after: (0, 1234),
        },
        marks_reads: std::sync::atomic::AtomicUsize::new(0),
    };
    let f = Fixture::new();
    f.select_in_plan(&["billing"]);
    f.execute_reading(&evidence(), &reader, Vec::new()).unwrap();
    assert_eq!(
        reader.marks_reads.load(std::sync::atomic::Ordering::SeqCst),
        1
    );
}
