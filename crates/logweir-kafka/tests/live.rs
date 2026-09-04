#![cfg(feature = "e2e")]
use logweir_kafka::{rdkafka_reader::RdKafkaReader, reader::AuthConfig, reader::ClusterReader};

/// Requires `just e2e-up`. LOGWEIR_TEST_BOOTSTRAP defaults to localhost:9092.
fn reader() -> RdKafkaReader {
    let bs = std::env::var("LOGWEIR_TEST_BOOTSTRAP").unwrap_or_else(|_| "localhost:9092".into());
    RdKafkaReader::connect(&[bs], AuthConfig::Plaintext).unwrap()
}

#[test]
fn reads_a_real_cluster_id_and_the_marker_topic() {
    let r = reader();
    assert!(!r.cluster_id().unwrap().is_empty());
    assert!(r
        .list_topics()
        .unwrap()
        .iter()
        .any(|t| t.name == "logweir.scratch"));
}

#[test]
fn reads_end_offsets_and_topic_configs() {
    let r = reader();
    let offs = r.end_offsets("test-topic").unwrap();
    assert_eq!(
        offs.len(),
        3,
        "the compose stack creates test-topic with 3 partitions"
    );
    let cfg = r.topic_configs("test-topic").unwrap();
    assert!(cfg.contains_key("cleanup.policy"));
}
