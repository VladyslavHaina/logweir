use logweir_kafka::reader::{ClusterReader, ConsumedRecord, KafkaError, TopicMeta};

/// A hand-written fake proves the ORCHESTRATOR's contract without a broker,
/// which is what keeps Tasks 14-20 testable on a laptop with no compose stack.
struct FakeReader {
    cluster_id: String,
    topics: Vec<TopicMeta>,
}

impl ClusterReader for FakeReader {
    fn cluster_id(&self) -> Result<String, KafkaError> {
        Ok(self.cluster_id.clone())
    }
    fn list_topics(&self) -> Result<Vec<TopicMeta>, KafkaError> {
        Ok(self.topics.clone())
    }
    fn end_offsets(&self, topic: &str) -> Result<Vec<(i32, i64)>, KafkaError> {
        Ok(self
            .topics
            .iter()
            .find(|t| t.name == topic)
            .map(|t| (0..t.partitions).map(|p| (p, 0i64)).collect())
            .unwrap_or_default())
    }
    fn topic_configs(
        &self,
        _topic: &str,
    ) -> Result<std::collections::BTreeMap<String, String>, KafkaError> {
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

#[test]
fn the_trait_is_object_safe_so_the_orchestrator_can_hold_a_dyn() {
    let r: Box<dyn ClusterReader> = Box::new(FakeReader {
        cluster_id: "MkU3OEVBNTcwNTJENDM2Qk".into(),
        topics: vec![TopicMeta {
            name: "logweir.scratch".into(),
            partitions: 1,
        }],
    });
    assert_eq!(r.cluster_id().unwrap(), "MkU3OEVBNTcwNTJENDM2Qk");
    assert!(r
        .list_topics()
        .unwrap()
        .iter()
        .any(|t| t.name == "logweir.scratch"));
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
