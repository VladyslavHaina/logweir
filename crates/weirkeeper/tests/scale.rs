//! PLAT-20.2: what the controller's half of a topic discovery costs at the
//! sizes the contract admits — a MEASUREMENT, not a check.
//!
//! One discovery's inventory reaches the controller as a relay of topic frames
//! on the check pod's stdout. The controller decodes and verifies the frames,
//! splits the verified entries into `ConfigMap` chunks with an index for the
//! status, and — for a dynamic `Backup` — classifies them into the list the run
//! will freeze. Each of those is pure, so each is timed here over synthetic
//! inventories of 10,000, 20,000 (the default `maxTopics`) and 50,000 (the
//! installation ceiling, `MAX_TOPICS_CEILING`) names.
//!
//! Every row is `#[ignore]`d: the numbers and the machine they were taken on
//! are recorded in `docs/stability.md`, *Measured scale limits*, and a
//! wall-clock assertion on a shared host would be a flaky test rather than a
//! guard. The bounds these functions work inside (2,500 lines and 768 KiB a
//! chunk, 64 chunks in a status) are already asserted by
//! `topic_discovery_controller.rs` and `check_framework.rs`.
//!
//! ```text
//! cargo test --release -p weirkeeper --test scale -- --ignored --nocapture --test-threads=1
//! ```

use std::collections::BTreeMap;
use std::time::Instant;

use logweir_core::check_contract::{
    frames, CheckCode, CheckPlanKind, CheckResult, FrameExpectations, Stream, TopicEntry,
};
use weirkeeper::check::chunks;
use weirkeeper::controllers::backup_selection::{classify, Exclusions};
use weirkeeper::controllers::topic_discovery::chunk_index;

const PLAN_SHA: &str = "sha256:0000000000000000000000000000000000000000000000000000000000000000";
const SUBJECT_UID: &str = "00000000-0000-4000-8000-000000000001";
const RUNS: usize = 5;

/// `count` entries shaped like a real estate: forty regional prefixes, one in
/// a hundred internal, one in a hundred refused `TopicAuthorizationFailed`.
fn inventory(count: usize) -> Vec<TopicEntry> {
    let mut out: Vec<TopicEntry> = (0..count)
        .map(|i| {
            let name = if i % 100 == 0 {
                format!("__internal-{i:06}")
            } else {
                format!("orders.region-{:02}.stream-{i:06}", i % 40)
            };
            let mut e = TopicEntry::new(&name, 6);
            e.internal = TopicEntry::name_is_internal(&name);
            if i % 100 == 1 {
                e.error = Some(CheckCode::TopicAuthorizationFailed);
            }
            e
        })
        .collect();
    out.sort_by(|a, b| a.name.as_bytes().cmp(b.name.as_bytes()));
    out
}

/// The runner's relay for `entries`: one topic frame per entry, the result
/// document in part frames, and the end frame that pins both.
fn relay(entries: &[TopicEntry]) -> Vec<String> {
    let result = CheckResult::new(CheckPlanKind::TopicInventory);
    let bytes = result.to_canonical_json().expect("a serialisable result");
    let parts = frames::write_parts(Stream::Result, &bytes).expect("small enough to frame");
    let mut streams = BTreeMap::new();
    streams.insert(Stream::Result, (bytes.clone(), parts.len()));
    let end = frames::end_frame(PLAN_SHA, SUBJECT_UID, &streams, Some(entries));
    let mut out: Vec<String> = entries
        .iter()
        .map(|e| frames::write_topic_line(e).expect("a framable entry"))
        .collect();
    out.extend(parts);
    out.push(frames::write_end(&end).expect("a framable end"));
    out
}

fn median_ms<T>(mut f: impl FnMut() -> T) -> (f64, T) {
    let mut samples = Vec::with_capacity(RUNS);
    let mut last = None;
    for _ in 0..RUNS {
        let started = Instant::now();
        let out = f();
        samples.push(started.elapsed().as_secs_f64() * 1_000.0);
        last = Some(out);
    }
    samples.sort_by(|a, b| a.partial_cmp(b).expect("a finite time"));
    (samples[RUNS / 2], last.expect("at least one run"))
}

#[test]
#[ignore = "a measurement; run with --ignored --nocapture"]
fn measure_the_controller_half_of_a_discovery() {
    let exclusions = Exclusions {
        topics: (0..10)
            .map(|i| format!("orders.region-00.stream-{i:06}"))
            .collect(),
        prefixes: vec![
            "tmp-".into(),
            "scratch.".into(),
            "orders.region-39.".into(),
            "dlq.".into(),
            "test-".into(),
        ],
    };
    let expect = FrameExpectations {
        plan_sha256: PLAN_SHA.to_string(),
        subject_uid: SUBJECT_UID.to_string(),
    };
    for count in [10_000usize, 20_000, 50_000] {
        let entries = inventory(count);
        let lines = relay(&entries);
        let relay_bytes: usize = lines.iter().map(|l| l.len() + 1).sum();

        let (decode, relayed) = median_ms(|| {
            let mut decoder = frames::Decoder::new();
            for line in &lines {
                decoder.push_line(line).expect("a well-formed frame");
            }
            decoder.finish(&expect).expect("a verified relay")
        });
        assert_eq!(relayed.topics.len(), count);

        let (split, split_chunks) = median_ms(|| chunks::split(&relayed.topics));
        let (index, indexed) =
            median_ms(|| chunk_index(&relayed.topics, &split_chunks, "lwc-scale"));
        assert_eq!(indexed.len(), split_chunks.len());

        let (classified, classification) = median_ms(|| classify(&relayed.topics, &exclusions));
        assert_eq!(classification.visible, count);

        println!(
            "MEASURE discovery (controller): {count} topics, relay {relay_bytes} bytes: \
             decode+verify_ms={decode:.2} split_ms={split:.2} ({} chunks) index_ms={index:.2} \
             classify_ms={classified:.2} (resolved {}, internal {}, limited {}, excluded {})",
            split_chunks.len(),
            classification.resolved.len(),
            classification.internal.len(),
            classification.limited.len(),
            classification.excluded.len(),
        );
    }
}
