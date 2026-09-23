//! PLAT-20.2: what the runner's half of a topic discovery costs at the sizes
//! the contract admits — a MEASUREMENT, not a check, and socketless.
//!
//! [`assemble`] is the pure step between the broker's all-topics metadata
//! answer and the relay: it builds an entry per listed topic, excludes and
//! counts the internal ones, sorts bytewise, truncates at `maxTopics` and the
//! relay budget, and digests the result. It is timed here over listings of
//! 10,000, 20,000 (the default `maxTopics`) and 50,000 (the installation
//! ceiling) topics. The broker round trip itself is not measured: it needs a
//! broker, and its cost is the cluster's, not Logweir's.
//!
//! `#[ignore]`d for the reason `crates/weirkeeper/tests/scale.rs` gives; the
//! numbers are in `docs/stability.md`, *Measured scale limits*.
//!
//! ```text
//! cargo test --release -p logweir-kafka --test scale -- --ignored --nocapture --test-threads=1
//! ```

use std::collections::BTreeMap;
use std::time::Instant;

use logweir_core::check_contract::{CheckCode, DEFAULT_RELAY_BUDGET_BYTES};
use logweir_kafka::inventory::{assemble, InventoryRequest, ListedTopic, Listing};

const RUNS: usize = 5;

/// A listing in the broker's own (unsorted) order: forty regional prefixes,
/// one in a hundred internal, one in a hundred refused.
fn listing(count: usize) -> Listing {
    let topics = (0..count)
        .map(|i| {
            // A stride that is coprime with `count` scatters the names, so the
            // sort has real work to do rather than an already-sorted input.
            let j = (i * 7_919) % count;
            ListedTopic {
                name: if j % 100 == 0 {
                    format!("__internal-{j:06}")
                } else {
                    format!("orders.region-{:02}.stream-{j:06}", j % 40)
                },
                partitions: 6,
                error: (j % 100 == 1).then_some(CheckCode::TopicAuthorizationFailed),
            }
        })
        .collect();
    Listing {
        broker_count: 3,
        topics,
    }
}

#[test]
#[ignore = "a measurement; run with --ignored --nocapture"]
fn measure_the_runner_half_of_a_discovery() {
    for (count, max_topics) in [(10_000usize, 20_000u32), (20_000, 20_000), (50_000, 50_000)] {
        let listed = listing(count);
        let request = InventoryRequest {
            include_internal: false,
            expected_topics: Vec::new(),
            max_topics,
            relay_budget_bytes: DEFAULT_RELAY_BUDGET_BYTES as u64,
        };
        let targeted = BTreeMap::new();
        let mut samples = Vec::with_capacity(RUNS);
        let mut last = None;
        for _ in 0..RUNS {
            let started = Instant::now();
            let inventory = assemble(&listed, &request, &targeted);
            samples.push(started.elapsed().as_secs_f64() * 1_000.0);
            last = Some(inventory);
        }
        samples.sort_by(|a, b| a.partial_cmp(b).expect("a finite time"));
        let inventory = last.expect("one run");
        println!(
            "MEASURE discovery (runner): {count} listed topics, maxTopics {max_topics}: \
             assemble_ms={:.2} (max {:.2}) returned={} internal_excluded={} truncated={}",
            samples[RUNS / 2],
            samples[RUNS - 1],
            inventory.result.counts.returned,
            inventory.result.counts.internal_excluded,
            inventory.result.truncated,
        );
    }
}
