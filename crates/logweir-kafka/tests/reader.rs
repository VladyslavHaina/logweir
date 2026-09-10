use logweir_kafka::reader::{ClusterReader, ConsumedRecord, KafkaError, TopicMeta};

/// A hand-written fake proves the ORCHESTRATOR's contract without a broker,
/// which is what keeps Tasks 14-20 testable on a laptop with no compose
/// stack — and, per Task 10's post-review fix round, without letting those
/// phase tests only ever exercise the "everything is healthy" path. A real
/// `RdKafkaReader` can return `KafkaError::TopicNotFound`,
/// `KafkaError::NotAuthorized` and `KafkaError::Unreachable` from every one
/// of these methods; a mock that can never produce them would let a phase
/// test go green against a contract the real implementation cannot actually
/// uphold.
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
            // least one partition). The pre-fix-round version of this fake
            // returned `Ok(vec![])` via `.unwrap_or_default()` — precisely
            // the state Task 10's fix round declared impossible in the real
            // implementation, which would have let Tasks 16-21 write and
            // pass phase tests against a contract nothing real upholds.
            None => Err(KafkaError::TopicNotFound(topic.to_string())),
            // A topic present but carrying `TopicMeta::error` (round 2 added
            // this precisely so an errored topic stays IN `list_topics`, with
            // `partitions: 0`) must ALSO error here — the same 0-partition
            // range that made an unknown topic look like `Ok(vec![])` makes an
            // errored-but-present topic look exactly the same way if only
            // `t.partitions` is consulted. This fake only has the rendered
            // message string (not the original broker code `classify_topic_
            // error` used to build it in `RdKafkaReader`), so it reports a
            // generic `Client` error rather than guessing the original variant
            // back out of text — still an `Err`, which is the property a
            // phase test actually depends on.
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
    /// Task 8, guard **G-TS**. `inject` reaches this method too, for the same
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
    // The exact gap the post-review fix round found and closed: this used
    // to return `Ok(vec![])`, matching a real, healthy, zero-partition
    // topic — a state that cannot exist on a real cluster. A drill phase
    // that only ever exercised the fake here would never be forced to
    // handle `RdKafkaReader::end_offsets`'s real `TopicNotFound` return.
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
    // Round 2 added `TopicMeta::errored(name, msg)` precisely so an errored
    // topic stays present in `list_topics` with `partitions: 0` and
    // `error: Some(msg)`. But `end_offsets` computed its result from
    // `t.partitions` alone, so an errored, PRESENT topic's `0..0` range
    // silently produced `Ok(vec![])` -- the exact "healthy empty topic"
    // state the unknown-topic test above already proved `end_offsets` must
    // never produce, reachable through a second, un-covered path: a topic
    // this fake HAS, just with `error: Some(_)`, rather than one it lacks
    // entirely. The real `RdKafkaReader::end_offsets` returns
    // `NotAuthorized`/`TopicNotFound` for that same metadata condition, never
    // `Ok(vec![])`.
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
    // Proves the mock five later tasks (16-21) test drill phases against
    // can actually produce every error `RdKafkaReader` can — so a phase
    // that only handles the happy path fails its own tests here, before it
    // ever reaches a real broker.
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

/// Phase 9 (Task 20) drives teardown through this trait, so it must be testable
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

#[test]
fn a_consumed_record_fingerprints_identically_to_the_archived_one() {
    use logweir_kafka::fingerprint::record_fingerprint;
    let headers = vec![("x-original-offset".to_string(), Some(b"100".to_vec()))];
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
#[test]
fn the_broker_resource_is_spelled_the_rdkafka_way() {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
    let mut saw_specifier = false;
    for rel in ["src/reader.rs", "src/rdkafka_reader.rs"] {
        let body = std::fs::read_to_string(root.join(rel)).expect(rel);
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
