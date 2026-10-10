//! PROD-01.2 — the ApiVersions observation, against a REAL librdkafka
//! connection and no external broker.
//!
//! `logweir_kafka::api_versions` reads two line shapes out of librdkafka's
//! `feature` debug log. A log line is not an API: a librdkafka release may
//! reword it, and then every observation would quietly become "not observed".
//! These rows hold the shapes against the librdkafka this workspace LOCKS, on
//! every `cargo test`, by letting it connect to its own in-process mock
//! cluster (a loopback listener inside this process: no compose stack, no
//! external address).
//!
//! They also hold the observation to a WHOLE VIEW (review, M3): on a mock
//! cluster of three brokers it answers only when all three did, from one
//! bootstrap address as from three, and a broker that stays silent, or one
//! the cluster has dropped while its address is still named, makes it NOT
//! OBSERVED with the count.
#![cfg(feature = "client")]

use std::time::{Duration, Instant};

use logweir_core::check_contract::CheckCode;
use logweir_kafka::api_versions::{ApiVersions, LIST_GROUPS};
use logweir_kafka::inventory::{observe_api_versions, ClientConfig};
use rdkafka::mocking::MockCluster;
use rdkafka::producer::DefaultProducerContext;
use rdkafka::types::RDKafkaApiKey;

type Mock = MockCluster<'static, DefaultProducerContext>;

/// A configuration whose only broker is librdkafka's in-process mock.
fn mock_cluster() -> ClientConfig {
    let mut cfg = ClientConfig::new();
    cfg.set("test.mock.num.brokers", "1")
        .set("client.id", "logweir-kafka-api-versions-unit");
    cfg
}

/// A mock cluster of `brokers` brokers that the test holds, so it can stop,
/// slow or reconfigure one. Its broker ids are 1..=`brokers`.
fn mock_of(brokers: i32) -> Mock {
    MockCluster::new(brokers).expect("librdkafka's in-process mock cluster")
}

/// A configuration that reaches `mock` through the bootstrap addresses
/// `pick` chooses of its list.
fn reaching(mock: &Mock, pick: impl Fn(&[&str]) -> Vec<String>) -> ClientConfig {
    let all = mock.bootstrap_servers();
    let addresses: Vec<&str> = all.split(',').collect();
    let mut cfg = ClientConfig::new();
    cfg.set("bootstrap.servers", pick(&addresses).join(","))
        .set("client.id", "logweir-kafka-api-versions-unit");
    cfg
}

fn every_address(all: &[&str]) -> Vec<String> {
    all.iter().map(|s| s.to_string()).collect()
}

fn observed() -> ApiVersions {
    observe_api_versions(&mock_cluster(), Duration::from_secs(10))
        .expect("the locked librdkafka logs its mock broker's ApiVersions answer in the shapes api_versions.rs reads")
}

/// **The drift guard.** A connection to a broker yields a whole answer: the
/// APIs every Kafka broker serves are in it with a sane range. If librdkafka
/// rewords either line, this fails by name.
#[test]
fn the_shapes_are_the_ones_the_locked_librdkafka_prints() {
    let v = observed();
    assert_eq!(v.brokers(), 1, "{v:?}");
    assert!(!v.differ());
    for (api, key) in [
        ("Produce", 0),
        ("Fetch", 1),
        ("ListOffsets", 2),
        ("Metadata", 3),
        ("ApiVersions", 18),
    ] {
        let (min, max) = v
            .range(key)
            .unwrap_or_else(|| panic!("{api} ({key}) is missing from the observed answer: {v:?}"));
        assert!(
            (0..=max).contains(&min) && max < 100,
            "{api}: v{min}..v{max}"
        );
    }
    // The mock answers as a broker of its own vintage; whatever it serves for
    // ListGroups, the answer names the key or omits it, and `serves` agrees
    // with `range`.
    match v.range(LIST_GROUPS) {
        Some((min, max)) => assert!(v.serves(LIST_GROUPS, min) && v.serves(LIST_GROUPS, max)),
        None => assert!(!v.serves(LIST_GROUPS, 0)),
    }
    // An API key no broker has is not served.
    assert_eq!(v.range(29_999), None);
}

/// Two observations of one endpoint agree: the read is of the endpoint, not
/// of timing.
#[test]
fn two_observations_of_one_endpoint_agree() {
    let cfg = mock_cluster();
    // Each handle with `test.mock.num.brokers` gets its OWN mock cluster, of
    // the same librdkafka, so the two answers are the same ranges.
    let a = observe_api_versions(&cfg, Duration::from_secs(10)).expect("first");
    let b = observe_api_versions(&cfg, Duration::from_secs(10)).expect("second");
    for key in 0..80 {
        assert_eq!(a.range(key), b.range(key), "API key {key}");
    }
}

/// **NOT OBSERVED is an error, never an empty answer.** A client with no
/// broker to reach logs no ApiVersions answer; the observation says so with
/// its own code, within its budget.
#[test]
fn an_endpoint_that_never_answers_is_not_observed() {
    let mut cfg = ClientConfig::new();
    // No bootstrap server at all: the handle builds and has nothing to dial.
    cfg.set("client.id", "logweir-kafka-api-versions-unit");
    let started = Instant::now();
    let err = observe_api_versions(&cfg, Duration::from_secs(1))
        .expect_err("nothing answered, so nothing was observed");
    assert_eq!(err.code, CheckCode::ApiVersionsNotObserved, "{err}");
    assert!(
        started.elapsed() < Duration::from_secs(4),
        "the observation is bounded by its budget: {:?}",
        started.elapsed()
    );
}

/// With no budget left the endpoint is not dialled at all (review L9): the
/// refusal is immediate and says so.
#[test]
fn an_observation_with_no_budget_left_does_not_dial() {
    let started = Instant::now();
    let err = observe_api_versions(&mock_cluster(), Duration::from_millis(300))
        .expect_err("300 ms is not a budget");
    assert_eq!(err.code, CheckCode::ApiVersionsNotObserved, "{err}");
    assert!(err.message.contains("was not dialled"), "{err}");
    assert!(
        started.elapsed() < Duration::from_millis(200),
        "nothing was built or dialled: {:?}",
        started.elapsed()
    );
}

/// **Review M3: a cluster of three is three answers, every time.** From all
/// three bootstrap addresses and from ONE, the observation counts three
/// distinct brokers; the first version counted connections and saw one, two
/// or three brokers from run to run (measured on the compose stack).
///
/// Repeated, because the defect was a race: which brokers the client had
/// happened to dial.
#[test]
fn every_broker_of_a_three_broker_cluster_answers_from_one_address_or_three() {
    let mock = mock_of(3);
    for round in 0..6 {
        for (what, cfg) in [
            ("all three addresses", reaching(&mock, every_address)),
            (
                "one address",
                reaching(&mock, |all| vec![all[round % 3].to_string()]),
            ),
        ] {
            let v = observe_api_versions(&cfg, Duration::from_secs(10))
                .unwrap_or_else(|e| panic!("round {round}, {what}: {e}"));
            assert_eq!(v.brokers(), 3, "round {round}, {what}: {v:?}");
            assert!(!v.differ(), "one librdkafka, one answer: {v:?}");
            assert!(v.range(0).is_some(), "Produce is served: {v:?}");
        }
    }
}

/// **A broker that does not answer in time makes the view partial, and a
/// partial view is not an answer.** One of three mock brokers is given a
/// round trip longer than the budget: the cluster lists it, its connection
/// opens, and its ApiVersions answer does not come.
///
/// The observation waits out its budget and no longer, then says how many of
/// how many answered and which broker did not. It never returns the two
/// answers it has.
///
/// CONTROL: with the delay removed the same cluster is three of three.
#[test]
fn a_silent_broker_makes_the_view_partial_and_never_an_answer() {
    let mock = mock_of(3);
    mock.broker_round_trip_time(2, Duration::from_secs(30))
        .expect("the mock broker is slowed");
    for (what, cfg) in [
        ("all three addresses", reaching(&mock, every_address)),
        (
            "one address",
            reaching(&mock, |all| vec![all[0].to_string()]),
        ),
    ] {
        let started = Instant::now();
        let err = observe_api_versions(&cfg, Duration::from_secs(3))
            .expect_err("broker 2 never answered, so the cluster was not observed");
        let took = started.elapsed();
        assert_eq!(err.code, CheckCode::ApiVersionsNotObserved, "{what}: {err}");
        assert!(
            err.message.contains("2 of 3 broker(s)") && err.message.contains("broker 2 ("),
            "{what}: the reason names how many of how many, and which: {err}"
        );
        assert!(
            took >= Duration::from_secs(2) && took < Duration::from_secs(6),
            "{what}: it waits for the silent broker until its budget is spent, and stops: {took:?}"
        );
    }

    mock.broker_round_trip_time(2, Duration::ZERO)
        .expect("the delay is removed");
    let v = observe_api_versions(&reaching(&mock, every_address), Duration::from_secs(10))
        .expect("all three answer again");
    assert_eq!(v.brokers(), 3);
}

/// **A stopped broker whose address is still named is a broker nobody
/// asked.** The mock drops a downed broker from its metadata, as a real
/// cluster drops one it has fenced, so the cluster lists two brokers and both
/// answer. The connection still names three addresses: not a whole view.
///
/// From ONE live address the same cluster IS a whole view of two: nothing
/// names a third broker, and the count says two. That is the stated limit of
/// asking a cluster who its brokers are.
#[test]
fn a_stopped_broker_whose_address_is_still_named_is_not_a_whole_view() {
    let mock = mock_of(3);
    mock.broker_down(3).expect("the mock broker is stopped");

    let err = observe_api_versions(&reaching(&mock, every_address), Duration::from_secs(3))
        .expect_err("the third address did not answer");
    assert_eq!(err.code, CheckCode::ApiVersionsNotObserved, "{err}");
    assert!(
        err.message.contains("2 of 2 broker(s)")
            && err
                .message
                .contains("no answer from the bootstrap address(es) 127.0.0.1:"),
        "both listed brokers answered and a named address did not: {err}"
    );

    let v = observe_api_versions(
        &reaching(&mock, |all| vec![all[0].to_string()]),
        Duration::from_secs(10),
    )
    .expect("the cluster lists two brokers and both answered");
    assert_eq!(v.brokers(), 2, "the row prints how many the cluster listed");

    mock.broker_up(3).expect("the mock broker is started");
    let v = observe_api_versions(&reaching(&mock, every_address), Duration::from_secs(10))
        .expect("three again");
    assert_eq!(v.brokers(), 3);
}

/// The ranges are the cluster's own answer, not a table: narrowing what the
/// mock serves for Produce narrows what is observed.
#[test]
fn the_observed_ranges_are_what_the_cluster_answered() {
    let mock = mock_of(3);
    let cfg = reaching(&mock, every_address);
    let before = observe_api_versions(&cfg, Duration::from_secs(10)).expect("observed");
    let (_, max) = before.range(0).expect("Produce is served");
    assert!(max >= 8, "the mock serves the engine's Produce v8: v{max}");

    mock.apiversion(RDKafkaApiKey::Produce, Some(0), Some(7))
        .expect("the mock's Produce range is narrowed");
    let after = observe_api_versions(&cfg, Duration::from_secs(10)).expect("observed");
    assert_eq!(after.range(0), Some((0, 7)));
    assert!(!after.serves(0, 8), "as Redpanda v26.2.4 answers");
    assert_eq!(after.brokers(), 3);
}
