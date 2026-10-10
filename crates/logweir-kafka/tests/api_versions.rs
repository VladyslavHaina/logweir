//! PROD-01.2 — the ApiVersions observation, against a REAL librdkafka
//! connection and no external broker.
//!
//! `logweir_kafka::api_versions` reads two line shapes out of librdkafka's
//! `feature` debug log. A log line is not an API: a librdkafka release may
//! reword it, and then every observation would quietly become "not observed".
//! These rows hold the shapes against the librdkafka this workspace LOCKS, on
//! every `cargo test`, by letting it connect to its own in-process mock
//! cluster (`test.mock.num.brokers`, a loopback listener inside this process:
//! no compose stack, no external address, well under a second).
#![cfg(feature = "client")]

use std::time::Duration;

use logweir_core::check_contract::CheckCode;
use logweir_kafka::api_versions::{ApiVersions, LIST_GROUPS};
use logweir_kafka::inventory::{observe_api_versions, ClientConfig};

/// A configuration whose only broker is librdkafka's in-process mock.
fn mock_cluster() -> ClientConfig {
    let mut cfg = ClientConfig::new();
    cfg.set("test.mock.num.brokers", "1")
        .set("client.id", "logweir-kafka-api-versions-unit");
    cfg
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
    assert!(v.connections() >= 1, "{v:?}");
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
    let started = std::time::Instant::now();
    let err = observe_api_versions(&cfg, Duration::from_millis(300))
        .expect_err("nothing answered, so nothing was observed");
    assert_eq!(err.code, CheckCode::ApiVersionsNotObserved, "{err}");
    assert!(
        started.elapsed() < Duration::from_secs(4),
        "the observation is bounded by its budget: {:?}",
        started.elapsed()
    );
}
