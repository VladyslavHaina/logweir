use logweir_kafka::reader::{ClusterReader, ConsumedRecord, KafkaError, TopicMeta};

/// A hand-written fake proves the ORCHESTRATOR's contract without a broker,
/// and without letting phase tests only ever exercise the "everything is
/// healthy" path. A real `RdKafkaReader` can return
/// `KafkaError::TopicNotFound`, `KafkaError::NotAuthorized` and
/// `KafkaError::Unreachable` from every one of these methods; a mock that can
/// never produce them would let a phase test go green against a contract the
/// real implementation cannot uphold.
struct FakeReader {
    cluster_id: String,
    topics: Vec<TopicMeta>,
    /// When set, every method below returns this error instead of its
    /// normal behaviour — the single knob a phase test uses to exercise
    /// `TopicNotFound`/`NotAuthorized`/`Unreachable` end to end, with no
    /// broker required to produce any of the three.
    inject: Option<KafkaError>,
}

impl ClusterReader for FakeReader {
    fn cluster_id(&self) -> Result<String, KafkaError> {
        if let Some(e) = &self.inject {
            return Err(e.clone());
        }
        Ok(self.cluster_id.clone())
    }
    fn list_topics(&self) -> Result<Vec<TopicMeta>, KafkaError> {
        if let Some(e) = &self.inject {
            return Err(e.clone());
        }
        Ok(self.topics.clone())
    }
    fn end_offsets(&self, topic: &str) -> Result<Vec<(i32, i64)>, KafkaError> {
        if let Some(e) = &self.inject {
            return Err(e.clone());
        }
        match self.topics.iter().find(|t| t.name == topic) {
            // An unknown topic is an ERROR here, matching `RdKafkaReader`:
            // `Ok(vec![])` would be indistinguishable from a healthy empty
            // topic, which cannot exist in real Kafka (every topic has at
            // least one partition).
            None => Err(KafkaError::TopicNotFound(topic.to_string())),
            // A topic present but carrying `TopicMeta::error` (it stays IN
            // `list_topics`, with `partitions: 0`) must ALSO error here, or its
            // 0-partition range reads as `Ok(vec![])` too. This fake has only
            // the rendered message, not the broker code, so it reports a
            // generic `Client` error rather than guessing the variant back out
            // of text — still an `Err`, which is the property a phase test
            // depends on.
            Some(t) if t.error.is_some() => Err(KafkaError::Client(t.error.clone().unwrap())),
            Some(t) => Ok((0..t.partitions).map(|p| (p, 0i64)).collect()),
        }
    }
    fn topic_configs(
        &self,
        _topic: &str,
    ) -> Result<std::collections::BTreeMap<String, String>, KafkaError> {
        if let Some(e) = &self.inject {
            return Err(e.clone());
        }
        Ok(Default::default())
    }
    /// Guard **G-TS**. `inject` reaches this method too, for the same
    /// reason it reaches every other one: `RdKafkaReader::broker_configs` can
    /// return `Unreachable` (no metadata, or DescribeConfigs timed out), and a
    /// phase-0 test whose double could never produce that would let the
    /// preflight's exit-1-not-exit-3 mapping go unproven.
    fn broker_configs(&self) -> Result<std::collections::BTreeMap<String, String>, KafkaError> {
        if let Some(e) = &self.inject {
            return Err(e.clone());
        }
        Ok(Default::default())
    }
    fn consume_range(
        &self,
        _t: &str,
        _p: i32,
        _from: i64,
        _max: usize,
    ) -> Result<Vec<ConsumedRecord>, KafkaError> {
        if let Some(e) = &self.inject {
            return Err(e.clone());
        }
        Ok(vec![])
    }
}

#[test]
fn the_trait_is_object_safe_so_the_orchestrator_can_hold_a_dyn() {
    let r: Box<dyn ClusterReader> = Box::new(FakeReader {
        cluster_id: "MkU3OEVBNTcwNTJENDM2Qk".into(),
        topics: vec![TopicMeta::new("logweir.scratch", 1)],
        inject: None,
    });
    assert_eq!(r.cluster_id().unwrap(), "MkU3OEVBNTcwNTJENDM2Qk");
    assert!(r
        .list_topics()
        .unwrap()
        .iter()
        .any(|t| t.name == "logweir.scratch"));
}

#[test]
fn end_offsets_for_an_unknown_topic_is_an_error_not_an_empty_ok() {
    // An unknown topic is `TopicNotFound`, never `Ok(vec![])`: a healthy
    // zero-partition topic cannot exist on a real cluster, and a phase tested
    // against the fake must handle `RdKafkaReader::end_offsets`'s real
    // `TopicNotFound` return.
    let r = FakeReader {
        cluster_id: "c1".into(),
        topics: vec![TopicMeta::new("logweir.scratch", 1)],
        inject: None,
    };
    let err = r.end_offsets("does-not-exist").unwrap_err();
    assert!(matches!(err, KafkaError::TopicNotFound(t) if t == "does-not-exist"));
}

#[test]
fn end_offsets_consults_the_topic_level_error_not_just_partition_count() {
    // An errored, PRESENT topic (`TopicMeta::errored`: `partitions: 0`,
    // `error: Some(msg)`) must not turn its `0..0` range into `Ok(vec![])`
    // either: that is a second path to the "healthy empty topic" state. The
    // real `RdKafkaReader::end_offsets` returns `NotAuthorized`/`TopicNotFound`
    // for that metadata condition.
    let r = FakeReader {
        cluster_id: "c1".into(),
        topics: vec![
            TopicMeta::new("orders", 3),
            TopicMeta::errored("payments", "not authorized: payments"),
        ],
        inject: None,
    };
    assert_eq!(r.end_offsets("orders").unwrap().len(), 3);
    assert!(r.end_offsets("payments").is_err());
}

#[test]
fn the_fake_can_be_configured_to_return_each_new_error_variant() {
    // Proves the mock drill phases are tested against can produce every error
    // `RdKafkaReader` can — so a phase that only handles the happy path fails
    // its own tests here, before it ever reaches a real broker.
    for err in [
        KafkaError::TopicNotFound("orders".into()),
        KafkaError::NotAuthorized("orders".into()),
        KafkaError::Unreachable("no broker answered within 20s".into()),
    ] {
        let r = FakeReader {
            cluster_id: "c1".into(),
            topics: vec![],
            inject: Some(err.clone()),
        };
        assert!(r.cluster_id().is_err());
        assert!(r.list_topics().is_err());
        assert!(r.end_offsets("orders").is_err());
        assert!(r.topic_configs("orders").is_err());
        assert!(r.broker_configs().is_err());
        assert!(r.consume_range("orders", 0, 0, 1).is_err());
    }
}

/// Phase 9 drives teardown through this trait, so it must be testable
/// with no broker for the same reason `ClusterReader` is.
struct FakeDeleter {
    deleted: std::sync::Mutex<Vec<String>>,
}

impl logweir_kafka::reader::TopicDeleter for FakeDeleter {
    fn delete_topics(
        &self,
        names: &[String],
    ) -> Result<Vec<(String, Result<(), String>)>, KafkaError> {
        self.deleted.lock().unwrap().extend_from_slice(names);
        Ok(names.iter().map(|n| (n.clone(), Ok(()))).collect())
    }
}

#[test]
fn the_deleter_receives_exactly_the_named_topics_and_never_a_pattern() {
    use logweir_kafka::reader::TopicDeleter;
    let d = FakeDeleter {
        deleted: Default::default(),
    };
    let out = d
        .delete_topics(&["drill-orders".into(), "drill-payments".into()])
        .unwrap();
    assert_eq!(out.len(), 2);
    assert_eq!(
        *d.deleted.lock().unwrap(),
        vec!["drill-orders", "drill-payments"]
    );
}

/// The `x-original-offset` header value, spelled the way the engine writes it:
/// `record.offset.to_le_bytes()`, an 8-byte little-endian i64
/// [U:crates/kafka-backup-core/src/backup/engine.rs:1846] (guard **G-HDR**).
///
/// This crate's tests cannot reach `crates/logweir/tests/fixtures::le_offset`
/// — that is a test-only module of a different crate, not part of any library
/// — so this is its twin rather than a second encoding: both definitions have
/// the body `n.to_le_bytes().to_vec()` and
/// `no_test_fixture_encodes_an_offset_header_as_ascii`
/// (`crates/logweir/tests/windowed_reconciliation.rs`) reads both files and
/// asserts exactly that, so the two cannot drift.
fn le_offset(n: i64) -> Vec<u8> {
    n.to_le_bytes().to_vec()
}

#[test]
fn a_consumed_record_fingerprints_identically_to_the_archived_one() {
    use logweir_kafka::fingerprint::record_fingerprint;
    let headers = vec![("x-original-offset".to_string(), Some(le_offset(100)))];
    let a = record_fingerprint(Some(b"k0"), Some(b"v0"), &headers, 1_700_000_000_000);
    let rec = ConsumedRecord {
        partition: 0,
        offset: 0,
        timestamp_ms: 1_700_000_000_000,
        key: Some(b"k0".to_vec()),
        value: Some(b"v0".to_vec()),
        headers: headers.clone(),
    };
    assert_eq!(a, rec.fingerprint());
}

/// **Guard G-TS.** The `NewTopicSpec` -> `rdkafka::admin::NewTopic` conversion,
/// read back off `NewTopic`'s own public fields — so this asserts what the
/// CreateTopics request actually carries, not a restatement of it.
///
/// Three properties in one test, because they are one property:
///
/// 1. every `(k, v)` of `configs` reaches `.set`, and
/// 2. **in order** — `NewTopic::set` pushes onto `config`, so the vector below
///    is the order the request carries, and `TARGET_TOPIC_CONFIGS`'s own order
///    is therefore observable;
/// 3. the conversion holds the caller's owned `Vec` for the `NewTopic`'s
///    lifetime. That one **is proven by compilation**: `new_topics_for`
///    returns `Vec<NewTopic<'_>>` tied to its argument, and `NewTopic<'a>`
///    stores `&'a str` for the name and both halves of every entry
///    (`rdkafka-0.36.2/src/admin.rs:658`), so a version that formatted a local
///    `String` per entry could not be written.
#[cfg(feature = "client")]
#[test]
fn create_topics_sets_the_config_entries_it_was_given() {
    use logweir_kafka::reader::{NewTopicSpec, TARGET_TOPIC_CONFIGS};
    // The exact value the drill builds: the pinned set, owned, in order.
    let specs = vec![
        NewTopicSpec {
            name: "drill-orders".into(),
            num_partitions: 3,
            replication_factor: 1,
            configs: TARGET_TOPIC_CONFIGS
                .iter()
                .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
                .collect(),
        },
        NewTopicSpec {
            name: "drill-payments".into(),
            num_partitions: 1,
            replication_factor: 1,
            configs: TARGET_TOPIC_CONFIGS
                .iter()
                .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
                .collect(),
        },
    ];
    let built = logweir_kafka::rdkafka_reader::new_topics_for(&specs);
    assert_eq!(built.len(), 2);
    for (nt, spec) in built.iter().zip(&specs) {
        assert_eq!(nt.name, spec.name);
        assert_eq!(nt.num_partitions, spec.num_partitions);
        // `TopicReplication` has no `PartialEq`; `Debug` is the only readback
        // rdkafka 0.36 offers, and `Fixed(1)` vs `Variable(..)` is exactly the
        // distinction that matters here.
        assert_eq!(
            format!("{:?}", nt.replication),
            format!("Fixed({})", spec.replication_factor)
        );
        // THE ORDERED VECTOR, not a set.
        assert_eq!(
            nt.config,
            vec![
                ("message.timestamp.type", "CreateTime"),
                ("retention.ms", "-1")
            ],
            "{} did not receive TARGET_TOPIC_CONFIGS in order",
            spec.name
        );
    }
}

/// **Guard G-TS**, spelling. `ConfigResource` is the JAVA client's name for the
/// DescribeConfigs input and DOES NOT EXIST in rdkafka 0.36: the input type is
/// `ResourceSpecifier::Broker(i32)` and the result carries
/// `OwnedResourceSpecifier::Broker(i32)`. A doc comment or an implementation
/// that names `ConfigResource` is documenting an API this crate cannot call.
///
/// A source read, deliberately: the compiler already rejects a `ConfigResource`
/// *expression*, so the only place the wrong name can survive is prose — and
/// `ClusterReader::broker_configs`'s doc comment is what the next implementer
/// reads.
///
/// rdkafka 0.36.2 DOES have a `ConfigResource`: it is the RESULT struct
/// (`src/admin.rs:1019`, aliased `ConfigResourceResult` at `:958`), which the
/// reader names to read an answer's entries. What the guard exists for is the
/// INPUT, which is `ResourceSpecifier`. So the fully qualified result paths
/// `rdkafka::admin::ConfigResource` / `…ConfigResourceResult`, and librdkafka's
/// own C function names (`rd_kafka_ConfigResource_error`), are allowed; every
/// OTHER spelling — a bare `ConfigResource` standing for the input, as the
/// Java client spells it — still fails here.
#[test]
fn the_broker_resource_is_spelled_the_rdkafka_way() {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
    let mut saw_specifier = false;
    for rel in ["src/reader.rs", "src/rdkafka_reader.rs"] {
        let raw = std::fs::read_to_string(root.join(rel)).expect(rel);
        let body = raw
            .replace("rdkafka::admin::ConfigResource", "")
            .replace("rd_kafka_ConfigResource_", "");
        assert!(
            !body.contains("ConfigResource"),
            "{rel} names `ConfigResource`, which is the Java client's type name and does not \
             exist in rdkafka 0.36 — the input is ResourceSpecifier::Broker(i32) and the result \
             carries OwnedResourceSpecifier::Broker(i32)"
        );
        saw_specifier |= body.contains("ResourceSpecifier");
    }
    assert!(
        saw_specifier,
        "neither src/reader.rs nor src/rdkafka_reader.rs names `ResourceSpecifier`, so nothing \
         records which rdkafka type the broker-config read is issued against"
    );
}

// The pure rules every reader names an EMPTY DescribeConfigs answer by. The
// rdkafka-result half is `src/rdkafka_reader.rs`'s
// `an_empty_topic_answer_is_a_refusal_never_an_empty_override_set`.

#[test]
fn an_empty_topic_config_answer_is_named_from_the_topics_metadata_and_never_ok() {
    use logweir_kafka::reader::{empty_topic_config_answer, TopicVisibility};
    assert!(matches!(
        empty_topic_config_answer("orders", &TopicVisibility::Visible),
        KafkaError::NotAuthorized(ref m) if m.starts_with("orders")
    ));
    assert!(matches!(
        empty_topic_config_answer("orders", &TopicVisibility::NotAuthorized),
        KafkaError::NotAuthorized(_)
    ));
    assert!(matches!(
        empty_topic_config_answer("orders", &TopicVisibility::NotFound),
        KafkaError::TopicNotFound(ref t) if t == "orders"
    ));
    assert!(matches!(
        empty_topic_config_answer("orders", &TopicVisibility::Unread("timed out".into())),
        KafkaError::Client(ref m) if m.contains("timed out")
    ));
}

#[test]
fn an_empty_broker_config_answer_is_the_cluster_authorizers_refusal() {
    assert!(matches!(
        logweir_kafka::reader::empty_broker_config_answer(1001),
        KafkaError::NotAuthorized(ref m) if m.contains("broker 1001")
    ));
}

/// The source vocabulary the reader writes is EXACTLY the one the receipt's
/// arm 11 accepts — both directions, so neither side can grow alone.
#[test]
fn every_config_source_this_reader_names_is_one_the_receipt_defines() {
    use logweir_kafka::reader::ConfigSourceKind as K;
    let named: std::collections::BTreeSet<&str> = [
        K::DynamicTopicConfig,
        K::DynamicBrokerConfig,
        K::DynamicDefaultBrokerConfig,
        K::StaticBrokerConfig,
        K::DefaultConfig,
        K::Unknown,
    ]
    .into_iter()
    .map(K::wire_name)
    .collect();
    let defined: std::collections::BTreeSet<&str> = logweir_core::backup_receipt::CONFIG_SOURCES
        .into_iter()
        .collect();
    assert_eq!(named, defined);
}

/// A reader that does not implement the topic configuration read can only make
/// coverage WEAKER: the default answers "cannot", never an empty success.
#[test]
fn the_default_describe_topic_configs_reports_that_it_cannot_answer() {
    let r = FakeReader {
        cluster_id: "c".into(),
        topics: vec![],
        inject: None,
    };
    assert!(matches!(
        r.describe_topic_configs(&["orders".to_string()]),
        Err(KafkaError::Client(_))
    ));
}

// A topic read right after it was created.

use logweir_kafka::reader::{settle, Settling};
use std::time::{Duration, Instant};

/// A just-created topic as a reader sees it while the creation propagates:
/// its configuration read answers `TopicNotFound` for the first `lag` calls,
/// then the configuration. `refused` makes every call answer `NotAuthorized`
/// instead — a principal that may create the topic but not describe it.
struct JustCreated {
    lag: std::sync::atomic::AtomicUsize,
    calls: std::sync::atomic::AtomicUsize,
    refused: bool,
}

impl JustCreated {
    fn lagging(lag: usize) -> Self {
        Self {
            lag: lag.into(),
            calls: 0.into(),
            refused: false,
        }
    }
    fn calls(&self) -> usize {
        self.calls.load(std::sync::atomic::Ordering::SeqCst)
    }
}

impl ClusterReader for JustCreated {
    fn cluster_id(&self) -> Result<String, KafkaError> {
        Ok("c1".into())
    }
    fn list_topics(&self) -> Result<Vec<TopicMeta>, KafkaError> {
        Ok(vec![])
    }
    fn end_offsets(&self, _t: &str) -> Result<Vec<(i32, i64)>, KafkaError> {
        Ok(vec![])
    }
    fn topic_configs(
        &self,
        topic: &str,
    ) -> Result<std::collections::BTreeMap<String, String>, KafkaError> {
        use std::sync::atomic::Ordering::SeqCst;
        self.calls.fetch_add(1, SeqCst);
        if self.refused {
            return Err(KafkaError::NotAuthorized(topic.to_string()));
        }
        if self
            .lag
            .fetch_update(SeqCst, SeqCst, |n| n.checked_sub(1))
            .is_ok()
        {
            return Err(KafkaError::TopicNotFound(topic.to_string()));
        }
        Ok([(
            "message.timestamp.type".to_string(),
            "CreateTime".to_string(),
        )]
        .into())
    }
    fn broker_configs(&self) -> Result<std::collections::BTreeMap<String, String>, KafkaError> {
        Ok(Default::default())
    }
    fn consume_range(
        &self,
        _t: &str,
        _p: i32,
        _from: i64,
        _max: usize,
    ) -> Result<Vec<ConsumedRecord>, KafkaError> {
        Ok(vec![])
    }
}

/// The read after a create waits for the topic, and the plain read
/// does not.
///
/// NEGATIVE CONTROL, in the same row: the plain `topic_configs` on a topic
/// whose creation has not propagated fails with `TopicNotFound`, and
/// `created_topic_configs` on the same state answers. Take the wait out of
/// `created_topic_configs` (read once) and the second half fails.
#[test]
fn a_read_after_a_create_waits_for_the_topic_and_a_plain_read_does_not() {
    let plain = JustCreated::lagging(3);
    assert!(
        matches!(plain.topic_configs("t"), Err(KafkaError::TopicNotFound(_))),
        "the double must reproduce the race, or this row proves nothing"
    );

    let waited = JustCreated::lagging(3);
    let configs = waited
        .created_topic_configs("t", 1, Duration::from_secs(10))
        .expect("the wait outlasts the propagation");
    assert_eq!(configs["message.timestamp.type"], "CreateTime");
    assert_eq!(waited.calls(), 4, "three answers waited out, then the read");
}

/// A refusal is not propagation: the default `created_topic_configs` returns
/// `NotAuthorized` on the first answer, and does not spend
/// `CREATED_TOPIC_SETTLE` on it. (`RdKafkaReader` waits it out too, because
/// there it is inferred from an empty answer, "DescribeConfigs answered this
/// visible topic with no configuration"; its exact set is pinned by the unit
/// row `the_created_topic_configuration_read_waits_out_only_propagation`. No
/// broker row reproduces that race.)
#[test]
fn a_refusal_after_a_create_is_returned_at_once() {
    let refused = JustCreated {
        refused: true,
        ..JustCreated::lagging(0)
    };
    let started = Instant::now();
    let err = refused
        .created_topic_configs("t", 1, Duration::from_secs(30))
        .unwrap_err();
    assert!(matches!(err, KafkaError::NotAuthorized(_)), "{err:?}");
    assert_eq!(refused.calls(), 1);
    assert!(started.elapsed() < Duration::from_secs(5));
}

/// `settle` is a bounded poll: it returns the answer as soon as
/// there is one, a failure at once, and the last "not yet" at the deadline,
/// unchanged so the caller can still classify it.
#[test]
fn settle_returns_the_answer_a_failure_at_once_and_the_last_not_yet_at_the_deadline() {
    let mut attempts = 0;
    let answered = settle(Duration::from_secs(10), || {
        attempts += 1;
        if attempts < 3 {
            Settling::NotYet(KafkaError::TopicNotFound("t".into()))
        } else {
            Settling::Done(attempts)
        }
    });
    assert_eq!(answered.unwrap(), 3);

    let mut attempts = 0;
    let failed: Result<(), _> = settle(Duration::from_secs(10), || {
        attempts += 1;
        Settling::Failed(KafkaError::NotAuthorized("t".into()))
    });
    assert!(matches!(failed, Err(KafkaError::NotAuthorized(_))));
    assert_eq!(attempts, 1, "a failure is never retried");

    let started = Instant::now();
    let mut attempts = 0;
    let expired: Result<(), _> = settle(Duration::from_millis(300), || {
        attempts += 1;
        Settling::NotYet(KafkaError::TopicNotFound(format!("t{attempts}")))
    });
    let elapsed = started.elapsed();
    match expired {
        Err(KafkaError::TopicNotFound(t)) => assert_eq!(t, format!("t{attempts}"), "the LAST one"),
        other => panic!("{other:?}"),
    }
    assert!(elapsed >= Duration::from_millis(300), "{elapsed:?}");
    assert!(elapsed < Duration::from_secs(5), "{elapsed:?}");
    assert!(
        attempts >= 2,
        "it polled, it did not sleep once: {attempts}"
    );
}
