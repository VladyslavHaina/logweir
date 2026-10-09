//! `rd_kafka_DescribeTopics`: the topic ID (KIP-516) of each named topic,
//! which rust-rdkafka 0.36.2's safe API does not wrap (PROD-01.4 §1.3 C1;
//! PROD-01.4a, OD-6 (a2)).
//!
//! Values only. The ID is handed back as its two 64-bit halves, exactly as
//! librdkafka holds them, and never through `rd_kafka_Uuid_base64str`: that
//! helper encodes with the STANDARD base64 alphabet while Kafka prints topic
//! IDs in the URL-safe one, so the same ID would read `Cf6zT/mcTNCoxuPmv1Ztxw`
//! here and `Cf6zT_mcTNCoxuPmv1Ztxw` in `kafka-topics.sh --describe`
//! (PROD-01.4 §1.3 C4, measured). What the halves MEAN — the all-zero ID is
//! Kafka's "no ID", the canonical text, a per-topic code read as "not found"
//! or "not authorized" — is decided in `logweir_kafka::topic_ids` and
//! `logweir_core::topic_identity`, which keep `#![forbid(unsafe_code)]` and
//! are unit-tested without a broker.
use crate::raw::{array, raw_error, run, text, Event};
use crate::{CText, CallError, RawError};
use rdkafka::bindings as rd;
use rdkafka::client::{Client, ClientContext};
use std::collections::BTreeSet;
use std::ffi::CString;
use std::os::raw::c_char;
use std::ptr::NonNull;
use std::time::Duration;

/// The most topic names one call accepts. librdkafka sends them all in ONE
/// Metadata request (`rdkafka_admin.c:9109-9135`), so this bounds the request
/// and the answer that one call holds in memory; a caller with more names
/// splits them.
pub const MAX_DESCRIBE_TOPICS: usize = 1000;

/// The longest topic name a call accepts, in bytes: Kafka's own limit
/// (`Topic.MAX_NAME_LENGTH`, 249). A longer name cannot name a topic, so it is
/// refused before anything is sent rather than costing a round trip.
pub const MAX_TOPIC_NAME_BYTES: usize = 249;

/// A topic ID as librdkafka holds it: the UUID's two 64-bit halves
/// (`rd_kafka_Uuid_most_significant_bits`, `…_least_significant_bits`).
/// Both zero is Kafka's "no ID" (a broker below inter-broker protocol 2.8
/// answers so); this crate does not interpret it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct TopicUuid {
    /// The most significant 64 bits.
    pub most_significant_bits: i64,
    /// The least significant 64 bits.
    pub least_significant_bits: i64,
}

/// One topic of a DescribeTopics answer, as librdkafka reported it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribedTopic {
    /// The topic name the answer carries.
    pub name: CText,
    /// The topic ID's two halves. `None` only if librdkafka returned a NULL
    /// ID pointer, which 2.12.1 never does (the ID is a field of the
    /// description, `rdkafka_admin.c:9070-9073`); kept apart from a zero ID so
    /// that case cannot be mistaken for the broker's answer.
    pub topic_id: Option<TopicUuid>,
    /// How many partitions the answer lists (0 beside an error).
    pub partition_count: usize,
    /// Whether the broker marks the topic internal.
    pub is_internal: bool,
    /// The per-topic error, with its code as an integer (T12):
    /// `UNKNOWN_TOPIC_OR_PART` (3) for a topic the broker does not hold,
    /// `TOPIC_AUTHORIZATION_FAILED` (29) for one the principal may not
    /// Describe. The ID beside an error is whatever the broker sent and means
    /// nothing.
    pub error: Option<RawError>,
}

/// The names a call sends, refused before anything is sent when the list is
/// empty or too long, or a name is blank, too long, carries a NUL, or is
/// repeated (librdkafka refuses a repeated name for the whole call,
/// `rdkafka_admin.c:9251-9268`, and an empty one at `:9271-9282`).
pub fn describe_request(names: &[&str]) -> Result<Vec<CString>, CallError> {
    if names.is_empty() {
        return Err(CallError::InvalidInput(
            "no topic names to describe".to_string(),
        ));
    }
    if names.len() > MAX_DESCRIBE_TOPICS {
        return Err(CallError::InvalidInput(format!(
            "{} topic names in one description; at most {MAX_DESCRIBE_TOPICS}",
            names.len()
        )));
    }
    let mut seen = BTreeSet::new();
    let mut out = Vec::with_capacity(names.len());
    for name in names {
        if name.trim().is_empty() {
            return Err(CallError::InvalidInput(
                "a topic name is never blank".to_string(),
            ));
        }
        if name.len() > MAX_TOPIC_NAME_BYTES {
            return Err(CallError::InvalidInput(format!(
                "a topic name of {} bytes is longer than Kafka's {MAX_TOPIC_NAME_BYTES}",
                name.len()
            )));
        }
        if !seen.insert(*name) {
            return Err(CallError::InvalidInput(format!("{name:?} appears twice")));
        }
        out.push(
            CString::new(*name)
                .map_err(|_| CallError::InvalidInput(format!("{name:?} carries a NUL byte")))?,
        );
    }
    Ok(out)
}

/// The request's topic collection, destroyed exactly once on drop.
struct Collection(NonNull<rd::rd_kafka_TopicCollection_t>);

impl Collection {
    /// A collection of `names`. librdkafka copies every name (`rd_strdup`,
    /// `rdkafka_admin.c:8842-8857`), so `names` need not outlive it.
    fn of(names: &[CString]) -> Result<Collection, CallError> {
        let mut pointers: Vec<*const c_char> = names.iter().map(|n| n.as_ptr()).collect();
        // SAFETY: `pointers` holds `pointers.len()` pointers to NUL-terminated
        // names in `names`, which outlive this call; librdkafka only reads
        // them (it copies each with `rd_strdup`) and never writes through the
        // array. With zero names it reads nothing. The result is a new
        // collection the caller owns, or NULL.
        let raw = unsafe {
            rd::rd_kafka_TopicCollection_of_topic_names(pointers.as_mut_ptr(), pointers.len())
        };
        NonNull::new(raw)
            .map(Collection)
            .ok_or_else(|| CallError::Options {
                code: -1,
                message: "rd_kafka_TopicCollection_of_topic_names returned NULL".to_string(),
            })
    }
}

impl Drop for Collection {
    fn drop(&mut self) {
        // SAFETY: created by `rd_kafka_TopicCollection_of_topic_names` and
        // owned by this guard alone. `rd_kafka_DescribeTopics` COPIES every
        // name into its request (`rd_strdup`, `rdkafka_admin.c:9245-9249`), so
        // no request refers to this object, and this is its only destroy.
        unsafe { rd::rd_kafka_TopicCollection_destroy(self.0.as_ptr()) }
    }
}

/// **Describes the named topics** (`rd_kafka_DescribeTopics`): one Metadata
/// request (v10 or later, which carries topic IDs, when the broker serves it)
/// with topic auto-creation OFF (`rdkafka_admin.c:1512-1521`), bounded by
/// `timeout` plus [`crate::POLL_MARGIN`].
///
/// One entry per topic the broker answered for, at the position of its name
/// in the request (`rdkafka_admin.c:9195-9213`). A name the broker did not
/// answer for is ABSENT from the result: the caller must not read a missing
/// entry as anything but "no answer".
///
/// # Errors
///
/// [`CallError::InvalidInput`] per [`describe_request`], before anything is
/// sent; otherwise [`CallError`] when the call as a whole failed (no broker
/// within the timeout is [`CallError::Call`] with `_TIMED_OUT`). A topic the
/// broker refused or does not hold is an entry whose
/// [`DescribedTopic::error`] is set, never an `Err`.
pub fn describe_topics<C: ClientContext>(
    client: &Client<C>,
    names: &[&str],
    timeout: Duration,
) -> Result<Vec<DescribedTopic>, CallError> {
    let request = describe_request(names)?;
    // The bound is checked here too, before `send` creates the collection, so
    // a refused timeout creates no librdkafka object at all (review L3).
    crate::raw::timeout_ms(timeout)?;
    send(client, &request, timeout)
}

/// The call itself, over names already validated (or deliberately not, by
/// the soak below, which needs librdkafka's own immediate refusals).
pub(crate) fn send<C: ClientContext>(
    client: &Client<C>,
    names: &[CString],
    timeout: Duration,
) -> Result<Vec<DescribedTopic>, CallError> {
    let collection = Collection::of(names)?;
    run(
        client,
        rd::rd_kafka_admin_op_t::RD_KAFKA_ADMIN_OP_DESCRIBETOPICS,
        rd::RD_KAFKA_EVENT_DESCRIBETOPICS_RESULT,
        timeout,
        |_| Ok(()),
        |rk, options, queue| {
            // SAFETY: `rk` is the handle `client` borrows for the whole of
            // `run`; `options` and `queue` are live guards owned by `run`
            // (librdkafka copies the options, `rdkafka_admin.c:637-640`, and
            // takes its own reference to the queue, `rdkafka_queue.h:733`);
            // `collection` is live until after `run` returns, and librdkafka
            // copies every name out of it before this call returns
            // (`rdkafka_admin.c:9245-9249`).
            unsafe {
                rd::rd_kafka_DescribeTopics(
                    rk,
                    collection.0.as_ptr(),
                    options.as_ptr(),
                    queue.as_ptr(),
                )
            }
        },
        read_descriptions,
    )
}

fn read_descriptions(event: &Event) -> Result<Vec<DescribedTopic>, CallError> {
    // SAFETY: `run` checked that the event is a DESCRIBETOPICS_RESULT without
    // a call error; this accessor returns the event itself, cast
    // (`rdkafka_event.c:392-397`), or NULL on a type mismatch.
    let result = unsafe { rd::rd_kafka_event_DescribeTopics_result(event.as_ptr()) };
    if result.is_null() {
        return Err(CallError::UnexpectedResult(
            "not a DescribeTopics result".to_string(),
        ));
    }
    let mut n = 0usize;
    // SAFETY: `result` is a live DescribeTopics result (the accessor's
    // `rd_assert` on the request type holds: `run` matched the event type);
    // it writes the count and returns the description array the event owns.
    // An element may be NULL where the broker skipped a requested name
    // (`rd_list_set` zero-fills, `rdlist.c:153-167`).
    let descriptions = unsafe { rd::rd_kafka_DescribeTopics_result_topics(result, &mut n) };
    // SAFETY: NULL or `n` description pointers owned by `event`, borrowed for
    // the slice's lifetime.
    let descriptions = unsafe { array(event, descriptions.cast_const(), n) };
    let mut out = Vec::with_capacity(descriptions.len());
    for &d in descriptions {
        if d.is_null() {
            continue;
        }
        // SAFETY: `d` is a non-NULL description owned by `event`; every
        // accessor below only reads it. The name is a string it owns, copied
        // by `text`; the ID accessor returns a pointer to a field of the
        // description (never NULL in 2.12.1, checked anyway) whose two halves
        // are read as `i64`; the partition accessor writes the count (the
        // array it returns is not read); the error is owned by the
        // description, copied by `raw_error` with its code read as an integer
        // (T12), and not destroyed here.
        let topic = unsafe {
            let id = rd::rd_kafka_TopicDescription_topic_id(d);
            let mut partitions = 0usize;
            let _ = rd::rd_kafka_TopicDescription_partitions(d, &mut partitions);
            DescribedTopic {
                name: text(event, rd::rd_kafka_TopicDescription_name(d)),
                topic_id: (!id.is_null()).then(|| TopicUuid {
                    most_significant_bits: rd::rd_kafka_Uuid_most_significant_bits(id),
                    least_significant_bits: rd::rd_kafka_Uuid_least_significant_bits(id),
                }),
                partition_count: partitions,
                is_internal: rd::rd_kafka_TopicDescription_is_internal(d) != 0,
                error: raw_error(event, rd::rd_kafka_TopicDescription_error(d)),
            }
        };
        out.push(topic);
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::code;
    use crate::test_support::offline_client;
    use rdkafka::producer::Producer;
    use std::time::Instant;

    #[test]
    fn a_topic_request_that_cannot_mean_what_it_says_is_refused_before_sending() {
        assert!(matches!(
            describe_request(&[]),
            Err(CallError::InvalidInput(m)) if m.contains("no topic names")
        ));
        assert!(matches!(
            describe_request(&[""]),
            Err(CallError::InvalidInput(m)) if m.contains("blank")
        ));
        assert!(matches!(
            describe_request(&["  "]),
            Err(CallError::InvalidInput(m)) if m.contains("blank")
        ));
        assert!(matches!(
            describe_request(&["orders", "payments", "orders"]),
            Err(CallError::InvalidInput(m)) if m.contains("twice")
        ));
        assert!(matches!(
            describe_request(&["or\0ders"]),
            Err(CallError::InvalidInput(m)) if m.contains("NUL")
        ));
        let long = "t".repeat(MAX_TOPIC_NAME_BYTES + 1);
        assert!(matches!(
            describe_request(&[long.as_str()]),
            Err(CallError::InvalidInput(m)) if m.contains("250 bytes")
        ));
        // Exactly Kafka's limit is a name.
        let longest = "t".repeat(MAX_TOPIC_NAME_BYTES);
        assert_eq!(
            describe_request(&[longest.as_str()]).expect("valid").len(),
            1
        );
        let many: Vec<String> = (0..=MAX_DESCRIBE_TOPICS).map(|i| format!("t{i}")).collect();
        let many: Vec<&str> = many.iter().map(String::as_str).collect();
        assert!(matches!(
            describe_request(&many),
            Err(CallError::InvalidInput(m)) if m.contains("1001 topic names")
        ));
        assert_eq!(
            describe_request(&many[..MAX_DESCRIBE_TOPICS])
                .expect("valid")
                .len(),
            MAX_DESCRIBE_TOPICS
        );
        let ok = describe_request(&["orders", "payments"]).expect("valid");
        assert_eq!(ok[1].as_bytes(), b"payments");
    }

    /// A refused request costs no call: an invalid name is refused first, and
    /// then the timeout bound, both before `describe_topics` creates any
    /// librdkafka object (`send` builds the collection only after both).
    #[test]
    fn a_refused_request_creates_nothing() {
        let p = offline_client();
        let started = Instant::now();
        assert!(matches!(
            describe_topics(p.client(), &["a", "a"], Duration::from_secs(1)),
            Err(CallError::InvalidInput(_))
        ));
        assert!(matches!(
            describe_topics(p.client(), &["a"], Duration::from_millis(10)),
            Err(CallError::InvalidInput(m)) if m.contains("request timeout")
        ));
        assert!(started.elapsed() < Duration::from_millis(500));
    }

    /// With no broker, the call returns within its timeout plus the margin,
    /// with librdkafka's `_TIMED_OUT` as an integer: a TRANSPORT failure,
    /// which the safe side keeps apart from "not found" and "not authorized".
    #[test]
    fn describe_topics_is_bounded_without_a_broker() {
        let p = offline_client();
        let t = Duration::from_secs(1);
        let started = Instant::now();
        let e = describe_topics(p.client(), &["pa-orders"], t).expect_err("no broker");
        assert!(
            matches!(
                e,
                CallError::Call {
                    code: code::TIMED_OUT,
                    ..
                }
            ),
            "{e}"
        );
        assert!(started.elapsed() < t + crate::POLL_MARGIN);
    }

    /// librdkafka's own immediate answers, no broker needed: a repeated name
    /// is refused for the whole call with `_INVALID_ARG`, and an empty
    /// collection is answered at once with no topic. The second is the
    /// success path of the reader (an event with a result list) with nothing
    /// in it.
    #[test]
    fn librdkafka_answers_a_repeated_name_and_an_empty_request_at_once() {
        let p = offline_client();
        let a = CString::new("pa-orders").expect("no NUL");
        let started = Instant::now();
        let e =
            send(p.client(), &[a.clone(), a], Duration::from_secs(1)).expect_err("a repeated name");
        assert!(
            matches!(
                e,
                CallError::Call {
                    code: code::INVALID_ARG,
                    ..
                }
            ),
            "{e}"
        );
        let none = send(p.client(), &[], Duration::from_secs(1)).expect("an empty answer");
        assert!(none.is_empty(), "{none:?}");
        assert!(
            started.elapsed() < Duration::from_secs(1),
            "both are immediate"
        );
    }

    /// **The DescribeTopics soak** (PROD-01.4 §6.1, PROD-04.0 §7.2): thousands
    /// of calls with a bounded resident set. Each iteration creates and
    /// destroys a topic collection with copied names, an options object, a
    /// queue, a request and an event, and alternates librdkafka's two
    /// immediate answers: the refusal (an error result whose message is
    /// copied) and the empty success (a result list that is read). A leak of
    /// one object per call is tens of bytes or more, so 100,000 calls would
    /// move the resident set by megabytes.
    #[test]
    fn a_hundred_thousand_describe_topics_calls_keep_the_resident_set_bounded() {
        let p = offline_client();
        let client = p.client();
        let id = CString::new("pa-orders").expect("no NUL");
        let twice = [id.clone(), id];
        let call = |i: usize| {
            if i % 2 == 0 {
                let none = send(client, &[], Duration::from_secs(1)).expect("an empty answer");
                assert!(none.is_empty());
            } else {
                match send(client, &twice, Duration::from_secs(1)) {
                    Err(CallError::Call { code: c, message }) => {
                        assert_eq!(c, code::INVALID_ARG, "{}", message.display());
                        assert!(
                            message.display().contains("Duplicate"),
                            "{}",
                            message.display()
                        );
                    }
                    other => panic!("unexpected {other:?}"),
                }
            }
        };
        for i in 0..2_000 {
            call(i);
        }
        let before = crate::test_support::rss_kib();
        let started = Instant::now();
        for i in 0..100_000 {
            call(i);
        }
        let took = started.elapsed();
        let after = crate::test_support::rss_kib();
        let grew = after.saturating_sub(before);
        eprintln!(
            "describe_topics soak: 100000 calls in {took:?}; resident set {before} KiB -> \
             {after} KiB (+{grew} KiB)"
        );
        assert!(
            grew < 2048,
            "100000 calls grew the resident set by {grew} KiB ({before} -> {after}): a leak"
        );
    }
}
