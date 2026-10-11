#![cfg(feature = "e2e")]
//! **PROD-01.5d — the `faultproxy` profile drops exactly the produce response
//! it is armed for, and forwards the rest.**
//!
//! `kafka-faulty` advertises the proxy's address, so a client bootstrapped on
//! `bootstrap_faultproxy()` sends every request through
//! `e2e/compose/profiles/faultproxy/faultproxy.py`. This row is the proxy's
//! own proof, the oracle that 07.1-R9 (PROD-07.3) and A-C5-1 (PROD-00.3d)
//! will lean on:
//!
//! 1. **drop**: armed with `arm drop 3`, five records sent one at a time, each
//!    its own Produce request, no retries. Records 1, 2, 4 and 5 are
//!    acknowledged at offsets 0, 1, 3 and 4; record 3 is never acknowledged
//!    (it times out) although the broker appended it at offset 2: the high
//!    watermark is 5. The proxy saw five Produce requests, forwarded four
//!    responses and dropped exactly one, the third.
//! 2. **hold**: `arm hold 3 3000` on the same topic: all five acknowledged, in
//!    order and contiguous, and the third no sooner than three seconds after
//!    it was sent.
//! 3. **CONTROL, the proxy off**: five records, five acknowledgements, nothing
//!    dropped or held; the proxy forwarded all five responses.
//!
//! The row is `#[ignore]`d because CI's e2e job does not start the profile:
//!
//! ```text
//! eval "$(e2e/compose/stack-env.sh --slot N --profiles faultproxy)"
//! just e2e-up
//! cargo test -p e2e --features e2e --test fault_proxy -- --include-ignored --test-threads=1
//! just e2e-down
//! ```

mod harness;
use harness::*;
use rdkafka::client::ClientContext;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::message::{DeliveryResult, Message};
use rdkafka::producer::{BaseProducer, BaseRecord, Producer, ProducerContext};
use serde_json::Value;
use std::io::{BufRead, BufReader, Write};
use std::net::TcpStream;
use std::sync::Mutex;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

/// One line to the proxy's control port, its one JSON line back.
fn control(cmd: &str) -> Value {
    let mut s = TcpStream::connect(faultproxy_control())
        .unwrap_or_else(|e| panic!("the proxy's control port: {e} — is `faultproxy` up?"));
    s.set_read_timeout(Some(Duration::from_secs(10))).unwrap();
    s.write_all(format!("{cmd}\n").as_bytes()).unwrap();
    let mut line = String::new();
    BufReader::new(s).read_line(&mut line).unwrap();
    serde_json::from_str(&line).unwrap_or_else(|e| panic!("{cmd}: {line:?}: {e}"))
}

/// Each delivery report: the offset, or the error.
#[derive(Default)]
struct Acks(Mutex<Vec<Result<i64, String>>>);

impl ClientContext for Acks {}

impl ProducerContext for Acks {
    type DeliveryOpaque = ();
    fn delivery(&self, r: &DeliveryResult<'_>, _: ()) {
        self.0.lock().unwrap().push(match r {
            Ok(m) => Ok(m.offset()),
            Err((e, _)) => Err(e.to_string()),
        });
    }
}

/// Sends `n` records through the proxy ONE AT A TIME (each waits for the
/// previous answer, so each is its own Produce request), with no retry and
/// no idempotence, so a lost response can only end as an error. Returns each
/// record's outcome and how long its answer took.
fn send_one_by_one(
    topic: &str,
    n: usize,
    request_timeout_ms: u64,
) -> Vec<(Result<i64, String>, Duration)> {
    let producer: BaseProducer<Acks> = ClientConfig::new()
        .set("bootstrap.servers", bootstrap_faultproxy())
        .set("enable.idempotence", "false")
        .set("acks", "all")
        .set("message.send.max.retries", "0")
        .set("max.in.flight.requests.per.connection", "1")
        .set("linger.ms", "0")
        .set("request.timeout.ms", request_timeout_ms.to_string())
        .set(
            "message.timeout.ms",
            (request_timeout_ms + 2_000).to_string(),
        )
        .create_with_context(Acks::default())
        .expect("a producer");
    let mut out = Vec::new();
    for i in 0..n {
        let payload = format!("faultproxy-{i}");
        let sent = Instant::now();
        producer
            .send(BaseRecord::to(topic).payload(&payload).key(&payload))
            .unwrap_or_else(|(e, _)| panic!("enqueue record {}: {e}", i + 1));
        let deadline = sent + Duration::from_millis(request_timeout_ms + 20_000);
        while producer.context().0.lock().unwrap().len() <= i {
            assert!(
                Instant::now() < deadline,
                "no delivery report for record {}",
                i + 1
            );
            producer.poll(Duration::from_millis(50));
        }
        let r = producer.context().0.lock().unwrap()[i].clone();
        out.push((r, sent.elapsed()));
    }
    producer.flush(Duration::from_secs(10)).ok();
    out
}

/// The partition's high watermark, read through the proxy (a ListOffsets
/// request, which the proxy never touches).
fn high_watermark(topic: &str) -> i64 {
    let c: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", bootstrap_faultproxy())
        .set("group.id", "faultproxy-watermarks")
        .create()
        .expect("a consumer");
    c.fetch_watermarks(topic, 0, Duration::from_secs(20))
        .expect("the watermarks")
        .1
}

#[test]
#[ignore = "needs the stack's `faultproxy` profile; see the module doc"]
fn the_fault_proxy_drops_exactly_the_chosen_produce_response() {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis();
    let topic = format!("faultproxy-{nonce}");
    let made = kafka_topics(&[
        "--bootstrap-server",
        "kafka-faulty:9094",
        "--create",
        "--topic",
        &topic,
        "--partitions",
        "1",
        "--replication-factor",
        "1",
    ]);
    assert!(made.status.success(), "create {topic}: {made:?}");
    await_created_on(&bootstrap_faultproxy(), &topic, 1);

    // --- 1. drop the third response ------------------------------------------
    let armed = control("arm drop 3");
    assert_eq!(armed["mode"], "drop", "{armed}");
    let drop = send_one_by_one(&topic, 5, 3_000);
    let got: Vec<&Result<i64, String>> = drop.iter().map(|(r, _)| r).collect();
    assert_eq!(got[0], &Ok(0), "{drop:?}");
    assert_eq!(got[1], &Ok(1), "{drop:?}");
    assert!(
        got[2].is_err(),
        "the third record's answer was dropped, yet it was acknowledged: {drop:?}"
    );
    assert_eq!(
        got[3],
        &Ok(3),
        "the fourth record lands after the unacknowledged third: {drop:?}"
    );
    assert_eq!(got[4], &Ok(4), "{drop:?}");
    assert_eq!(
        high_watermark(&topic),
        5,
        "the broker appended all five, the third included: only its RESPONSE was lost"
    );
    let s = control("stats");
    assert_eq!(s["produce_requests"], 5, "{s}");
    assert_eq!(s["produce_responses_forwarded"], 4, "{s}");
    let faulted = s["faulted"].as_array().expect("faulted");
    assert_eq!(faulted.len(), 1, "exactly one response was faulted: {s}");
    assert_eq!(faulted[0]["mode"], "drop", "{s}");
    assert_eq!(faulted[0]["produce_request"], 3, "{s}");

    // --- 2. hold the third response ------------------------------------------
    let armed = control("arm hold 3 3000");
    assert_eq!(armed["mode"], "hold", "{armed}");
    let hold = send_one_by_one(&topic, 5, 15_000);
    let offsets: Vec<Result<i64, String>> = hold.iter().map(|(r, _)| r.clone()).collect();
    assert_eq!(offsets, (5..10).map(Ok).collect::<Vec<_>>(), "{hold:?}");
    assert!(
        hold[2].1 >= Duration::from_millis(3_000),
        "the held answer came back after {:?}",
        hold[2].1
    );
    let s = control("stats");
    assert_eq!(s["produce_requests"], 5, "{s}");
    assert_eq!(s["produce_responses_forwarded"], 5, "{s}");
    assert_eq!(s["faulted"][0]["mode"], "hold", "{s}");
    assert_eq!(s["faulted"][0]["produce_request"], 3, "{s}");

    // --- 3. CONTROL: the proxy off, nothing dropped ---------------------------
    assert_eq!(control("off")["mode"], "off");
    let off = send_one_by_one(&topic, 5, 3_000);
    let offsets: Vec<Result<i64, String>> = off.iter().map(|(r, _)| r.clone()).collect();
    assert_eq!(offsets, (10..15).map(Ok).collect::<Vec<_>>(), "{off:?}");
    assert_eq!(high_watermark(&topic), 15);
    let s = control("stats");
    assert_eq!(s["produce_requests"], 5, "{s}");
    assert_eq!(s["produce_responses_forwarded"], 5, "{s}");
    assert_eq!(s["faulted"], serde_json::json!([]), "{s}");
    // A command the proxy does not know is refused, and changes nothing.
    assert!(control("arm drop 0")["error"].is_string());
    assert_eq!(control("stats")["mode"], "off");
    eprintln!(
        "[prod-01-5d] drop: {drop:?}; hold: third answer after {:?}; off: {off:?}",
        hold[2].1
    );
}
