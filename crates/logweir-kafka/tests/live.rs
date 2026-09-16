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

// ---------------------------------------------------------------------------
// D2 W3 — the check inventory, against the real broker
// ---------------------------------------------------------------------------

use logweir_core::check_contract::{CheckCode, ExpectedTopicState};
use logweir_kafka::inventory::{
    ConnectionSettings, InventoryProbe, InventoryRequest, KafkaInventory, ProbeTimeouts,
};
use logweir_kafka::reader::NewTopicSpec;
use std::time::Duration;

fn check_client() -> KafkaInventory {
    let bs = std::env::var("LOGWEIR_TEST_BOOTSTRAP").unwrap_or_else(|_| "localhost:9092".into());
    KafkaInventory::connect(&ConnectionSettings {
        bootstrap_servers: vec![bs],
        auth: AuthConfig::Plaintext,
        ca_file: None,
        timeouts: ProbeTimeouts::for_budget(Duration::from_secs(30)),
    })
    .expect("the compose broker is reachable when `just e2e-up` has run")
}

#[test]
fn the_check_inventory_reads_topics_partitions_and_the_broker_count() {
    let c = check_client();
    let req = InventoryRequest {
        include_internal: false,
        expected_topics: vec!["test-topic".to_string(), "no-such-topic-abcdef".to_string()],
        max_topics: 1000,
        relay_budget_bytes: 6 * 1024 * 1024,
    };
    let inv = c
        .inventory(&req, Duration::from_secs(12))
        .expect("an inventory against a live broker");
    assert!(
        inv.result.cluster_id.is_some(),
        "a live broker names a cluster id"
    );
    assert_eq!(
        inv.result.broker_count,
        Some(1),
        "the compose stack is one broker"
    );
    let t = inv
        .entries
        .iter()
        .find(|e| e.name == "test-topic")
        .expect("the compose stack creates test-topic");
    assert_eq!(t.partitions, 3);
    assert!(t.expected, "a named expected topic carries the flag");
    assert!(
        !inv.entries.iter().any(|e| e.internal),
        "internal topics are excluded by default"
    );
    assert!(
        inv.result.counts.internal_excluded > 0,
        "__consumer_offsets exists on a broker that has run a consumer, and is counted"
    );

    // The targeted probe, against a name the listing did not carry.
    let ghost = inv
        .result
        .expected_results
        .iter()
        .find(|r| r.name == "no-such-topic-abcdef")
        .expect("reported");
    assert_eq!(
        ghost.state,
        ExpectedTopicState::NotFound,
        "an absent name on an unauthenticated broker is notFound, not notAuthorized"
    );
    // AND IT WAS NOT AUTO-CREATED. `allow.auto.create.topics=false` is what
    // stops a metadata lookup from creating the very topic it asked about.
    let after = c.list_topics().expect("a second listing");
    assert!(
        !after
            .topics
            .iter()
            .any(|e| e.name == "no-such-topic-abcdef"),
        "the targeted metadata probe created the topic it asked about"
    );
}

#[test]
fn validate_only_create_topics_validates_and_creates_nothing() {
    let c = check_client();
    let fresh = format!(
        "lw-validate-only-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("a clock after 1970")
            .as_millis()
    );
    let specs = vec![
        NewTopicSpec {
            name: fresh.clone(),
            num_partitions: 1,
            replication_factor: 1,
            configs: vec![("retention.ms".to_string(), "-1".to_string())],
        },
        NewTopicSpec {
            name: "test-topic".to_string(),
            num_partitions: 3,
            replication_factor: 1,
            configs: Vec::new(),
        },
    ];
    let out = c
        .validate_create_topics(&specs)
        .expect("a validate-only CreateTopics round trip");
    let by = |n: &str| {
        out.iter()
            .find(|o| o.name == n)
            .unwrap_or_else(|| panic!("{n} has no result"))
            .code
    };
    assert_eq!(by(&fresh), CheckCode::TopicCreateValidated);
    assert_eq!(
        by("test-topic"),
        CheckCode::MappedTopicExists,
        "a name that already exists is the collision answer D2 §6.7(b) wants"
    );

    // THE POINT OF THE WHOLE TEST. A dropped `validate_only(true)` would have
    // created `fresh` on a real broker; nothing in this crate could then
    // remove it, because deletion is scoped to a drill's scratch prefix.
    let after = c.list_topics().expect("a listing after the validation");
    assert!(
        !after.topics.iter().any(|t| t.name == fresh),
        "the validate-only CreateTopics CREATED {fresh}; the flag was dropped"
    );
}

#[test]
fn describe_configs_reads_the_broker_and_a_topic() {
    let c = check_client();
    let topic = c
        .topic_configs("test-topic")
        .expect("DescribeConfigs on a topic resource");
    assert!(topic.contains_key("cleanup.policy"));
    let broker = c
        .broker_configs()
        .expect("DescribeConfigs on the broker metadata names");
    assert!(
        broker.contains_key("message.timestamp.type") || broker.contains_key("log.dirs"),
        "a broker config read returned nothing recognisable: {} entries",
        broker.len()
    );
}

/// **D2 §4.2 [VERIFY U5], against a real broker.**
///
/// A metadata request that a broker refuses on authentication grounds does NOT
/// fail with an authentication error: librdkafka retries until the caller's
/// deadline and the call returns a TIMEOUT. The refusal appears only in the
/// client's error callback. Without
/// `logweir_kafka::inventory::CapturingContext` this test's `code` is
/// `MetadataTimeout`, and an operator with a wrong password is sent to look at
/// the network.
///
/// The SASL listener published at `localhost:9097` authenticates against the
/// broker's SCRAM credential store. This test names a principal that is not in
/// it, so it needs no `scram-setup` step and cannot be made to pass by one.
#[test]
fn a_refused_scram_credential_is_authentication_failed_and_not_a_timeout() {
    let bs =
        std::env::var("LOGWEIR_TEST_SASL_BOOTSTRAP").unwrap_or_else(|_| "localhost:9097".into());
    let c = KafkaInventory::connect(&ConnectionSettings {
        bootstrap_servers: vec![bs],
        auth: AuthConfig::from_spec(
            &logweir_core::spec::AuthSpec::ScramSha512 {
                username: "no-such-principal".to_string(),
                tls: false,
            },
            Some("not-the-password".to_string()),
        )
        .expect("interface I1 builds the auth"),
        ca_file: None,
        timeouts: ProbeTimeouts::for_budget(Duration::from_secs(20)),
    })
    .expect("the client builds; nothing has dialled yet");

    let err = c
        .list_topics()
        .expect_err("a principal the broker does not know cannot list topics");
    assert_eq!(
        err.code,
        CheckCode::AuthenticationFailed,
        "the error callback's observation must beat the call's own outcome; got {err:?}"
    );
    assert!(
        err.message.contains("MetadataTimeout") || err.message.contains("BrokerUnreachable"),
        "the message should record what the CALL said as well as what was observed: {}",
        err.message
    );
    // And the raw broker reason never reaches the message verbatim.
    assert!(
        !err.message.contains("not-the-password"),
        "the projected password reached a check message: {}",
        err.message
    );
    assert!(
        c.faults()
            .codes()
            .contains(&CheckCode::AuthenticationFailed),
        "the fault log records what it saw: {:?}",
        c.faults().codes()
    );
}
