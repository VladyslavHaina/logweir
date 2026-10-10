//! **The `unsafe` perimeter: the one crate where Logweir calls librdkafka
//! directly** (OD-6, decided 2026-10-05 as (a2); ADR 0004's amendment in
//! `docs/architecture.md`; `docs/to-do/decisions/PROD-04.0-admin-path.md` §7).
//!
//! rust-rdkafka 0.36.2's safe API lacks the calls below, or offers them only
//! unsoundly (PROD-04.0 §1.2 C1, C2, T1). This crate makes exactly those calls
//! through `rdkafka::bindings` and hands back OWNED values: integers, `String`s,
//! `Vec`s. It interprets nothing: what a code, a state or a clamped enum MEANS
//! is decided in `logweir-kafka` (`groups`, `acls`), which keeps
//! `#![forbid(unsafe_code)]` and is unit-tested without a broker.
//!
//! | Call | librdkafka | Module |
//! |---|---|---|
//! | the typed group listing | `rd_kafka_ListConsumerGroups` | [`groups::list_consumer_groups`] |
//! | the name listing (every group type) | `rd_kafka_list_groups` | [`groups::list_group_names`] |
//! | group description, and the targeted visibility probe | `rd_kafka_DescribeConsumerGroups` | [`groups::describe_consumer_groups`] |
//! | the caller's authorized operations on the cluster | `rd_kafka_DescribeCluster` | [`cluster::describe_cluster`] |
//! | the broker's ACL bindings | `rd_kafka_DescribeAcls` | [`acls::describe_acls`] |
//! | topic IDs (KIP-516) of named topics (PROD-01.4a) | `rd_kafka_DescribeTopics` | [`topics::describe_topics`] |
//! | the client's own log lines, as events on a private queue (PROD-01.2: the broker's ApiVersions answer is only there) | `rd_kafka_set_log_queue`, `rd_kafka_event_log` | [`logs::drain_logs`] |
//!
//! # The obligations every call keeps
//!
//! These are the ones PROD-04.0 §7.2 and PROD-01.4 §6.1 measured, and each
//! `// SAFETY:` comment in this crate names the ones its block relies on.
//!
//! 1. **Every librdkafka object is destroyed exactly once.** Options, queues,
//!    events, ACL filters and the name listing are each held by one owning
//!    guard (`raw.rs`) whose `Drop` is the only destroy, so an early return or a
//!    panic cannot leak one or destroy it twice. Objects a result owns (a
//!    listing, a description, an error inside a description) are never
//!    destroyed here: the event's destroy frees them.
//! 2. **Nothing is read after its owner is destroyed.** Every pointer derived
//!    from a result is read while the guard that owns the result lives, through
//!    helpers whose returned slices BORROW that guard (`raw::array`), and
//!    every value that leaves a call is an owned copy. The reader closure of
//!    `raw::run` must return `T: Send + 'static`, which no raw pointer is, so
//!    the compiler refuses a pointer escaping the event.
//! 3. **Inputs carry no NUL, and are refused before anything is sent** when
//!    they cannot mean what they say (empty, repeated, a topic name longer
//!    than Kafka's 249 bytes) or are too many for one call: librdkafka would
//!    answer a repeated group id or topic name with an error for the whole
//!    call.
//! 4. **Every wait is bounded.** Each call takes a request timeout
//!    ([`MIN_TIMEOUT`]..=[`MAX_TIMEOUT`]) that librdkafka enforces itself, and
//!    the poll for its result waits that plus [`POLL_MARGIN`], never longer.
//! 5. **Error codes and C enums are read as integers (T12).** rdkafka-sys
//!    declares `rd_kafka_resp_err_t` and the group, ACL and resource enums as
//!    Rust enums; a value outside their variants (a broker code librdkafka
//!    passes through, a future enum member) would be an invalid discriminant,
//!    which is undefined behaviour the moment it is produced. `sys.rs` declares
//!    the same symbols returning C integers, and the one struct field typed as
//!    such an enum (`err` of a topic-partition and of a legacy group info) is
//!    never read as one: only through a pointer cast to `c_int`.
//! 6. **No Rust reference to a librdkafka struct with an enum field is ever
//!    formed.** Fields are read through raw places (`addr_of!((*p).field)`),
//!    because a reference to such a struct asserts its whole value is valid.
//! 7. **Strings are copied byte for byte** ([`CText`]): NULL stays `Null`,
//!    and bytes that are not UTF-8 stay bytes (`NotUtf8`), never a panic and
//!    never a lossy replacement that could turn one id into another (the
//!    T18 class: rust-rdkafka panics on such bytes).
//!
//! # Threading model
//!
//! Every function is synchronous and takes `&Client<C>`. The borrow is what
//! keeps the `rd_kafka_t` alive: `Client`'s `Drop` destroys the handle, and it
//! cannot run while a call holds the borrow. librdkafka's admin API may be
//! called from any thread on a shared handle. Each call creates its OWN
//! options object and its OWN result queue on the calling thread, polls that
//! queue on the same thread, copies the result out and destroys all three
//! before returning. The guards hold raw pointers, so they are neither `Send`
//! nor `Sync`: no librdkafka object made here can cross a thread boundary, and
//! no two threads ever share one. What a call returns is plain owned data,
//! `Send + Sync`, with no tie to librdkafka. A request that outlives its poll
//! bound (librdkafka never answered within the margin) is abandoned safely: the
//! request holds its own reference to the queue's internals, the destroyed
//! queue is disabled, and a late result is freed by librdkafka on arrival
//! (`rdkafka_queue.h:253`, `:440`, `:733` in rdkafka-sys 4.10.0+2.12.1).
//!
//! # Exit (ADR 0004)
//!
//! A call leaves this crate once a released rust-rdkafka offers it safely;
//! when none is left, the crate is deleted and `check-unsafe-scope.sh` loses
//! its perimeter lines. `rd_kafka_DescribeTopics` ([`topics`], PROD-01.4a)
//! leaves through PROD-01.4b: rust-rdkafka PR #721 or a successor, released,
//! with the URL-safe text form derived from the two halves.
#![deny(unsafe_op_in_unsafe_fn)]
#![deny(clippy::undocumented_unsafe_blocks)]
#![deny(missing_docs)]

pub mod acls;
pub mod cluster;
pub mod groups;
pub mod logs;
mod raw;
mod sys;
pub mod topics;

use std::time::Duration;

/// The shortest request timeout a call accepts.
pub const MIN_TIMEOUT: Duration = Duration::from_secs(1);

/// The longest request timeout a call accepts.
pub const MAX_TIMEOUT: Duration = Duration::from_secs(300);

/// How much longer than its request timeout a call polls for the result.
/// librdkafka fails a request on its own timeout and posts that failure as
/// the result, so the margin only absorbs scheduling; a call that sees no
/// result within it reports [`CallError::NoResult`] and abandons the request
/// safely (see "Threading model").
pub const POLL_MARGIN: Duration = Duration::from_secs(5);

/// librdkafka error codes this crate names. Every other code is passed on as
/// the integer it is (T12).
pub mod code {
    /// `RD_KAFKA_RESP_ERR__TIMED_OUT`.
    pub const TIMED_OUT: i32 = -185;
    /// `RD_KAFKA_RESP_ERR__PARTIAL`: `rd_kafka_list_groups` heard from some
    /// brokers but not all before its timeout.
    pub const PARTIAL: i32 = -158;
    /// `RD_KAFKA_RESP_ERR__INVALID_ARG`.
    pub const INVALID_ARG: i32 = -186;
    /// `RD_KAFKA_RESP_ERR__NOT_CONFIGURED`: `rd_kafka_set_log_queue` on a
    /// handle created without `log.queue=true`.
    pub const NOT_CONFIGURED: i32 = -145;
}

/// Text librdkafka returned, copied byte for byte before its owner was
/// destroyed. Never lossy: an id that is not UTF-8 is kept as bytes, so it can
/// never be mistaken for another id.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum CText {
    /// librdkafka returned a NULL pointer.
    Null,
    /// Valid UTF-8.
    Utf8(String),
    /// Bytes that are not UTF-8, kept as they were.
    NotUtf8(Vec<u8>),
}

impl CText {
    /// Classifies copied bytes.
    #[must_use]
    pub fn from_bytes(bytes: Vec<u8>) -> CText {
        match String::from_utf8(bytes) {
            Ok(s) => CText::Utf8(s),
            Err(e) => CText::NotUtf8(e.into_bytes()),
        }
    }

    /// The text, when it is UTF-8.
    #[must_use]
    pub fn as_str(&self) -> Option<&str> {
        match self {
            CText::Utf8(s) => Some(s),
            CText::Null | CText::NotUtf8(_) => None,
        }
    }

    /// A printable rendering for messages: the text, `(null)`, or the bytes
    /// escaped. Never use it as an identifier.
    #[must_use]
    pub fn display(&self) -> String {
        match self {
            CText::Utf8(s) => s.clone(),
            CText::Null => "(null)".to_string(),
            CText::NotUtf8(b) => b.escape_ascii().to_string(),
        }
    }
}

/// An error librdkafka reported for one item of a result (a broker of a
/// listing, a group of a description), with its code as an integer (T12).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RawError {
    /// The error code: a Kafka protocol code (`>= 0`) or one of librdkafka's
    /// own (`< 0`).
    pub code: i32,
    /// librdkafka's name for the code.
    pub name: CText,
    /// librdkafka's message.
    pub message: CText,
}

/// Why a whole call returned no result.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CallError {
    /// Refused before anything was sent.
    InvalidInput(String),
    /// librdkafka refused to build the request's options or inputs.
    Options {
        /// librdkafka's code, `-1` where it gave none (a NULL constructor).
        code: i32,
        /// Its message.
        message: String,
    },
    /// No result within the request timeout plus [`POLL_MARGIN`]. The request
    /// was abandoned safely; nothing about the cluster is known.
    NoResult {
        /// How long the call polled.
        waited: Duration,
    },
    /// The result carried an error for the whole call, as an integer code
    /// (T12), with librdkafka's message.
    Call {
        /// The error code.
        code: i32,
        /// librdkafka's message.
        message: CText,
    },
    /// The result was not the expected one. A private queue receives nothing
    /// else, so this means librdkafka broke its own contract.
    UnexpectedResult(String),
}

impl std::fmt::Display for CallError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CallError::InvalidInput(why) => write!(f, "refused before sending: {why}"),
            CallError::Options { code, message } => {
                write!(f, "librdkafka refused the request ({code}): {message}")
            }
            CallError::NoResult { waited } => write!(f, "no result within {waited:?}"),
            CallError::Call { code, message } => {
                write!(f, "the call failed ({code}): {}", message.display())
            }
            CallError::UnexpectedResult(why) => write!(f, "unexpected result: {why}"),
        }
    }
}

impl std::error::Error for CallError {}

#[cfg(test)]
pub(crate) mod test_support {
    use rdkafka::config::ClientConfig;
    use rdkafka::producer::BaseProducer;
    use std::io::Read;
    use std::process::{Command, Stdio};
    use std::time::{Duration, Instant};

    /// A client with NO bootstrap servers: librdkafka builds the handle and
    /// dials nothing, so a call made with it exercises the whole FFI path
    /// (options, queue, request, event, destroy) with no network at all, and
    /// fails on its own request timeout.
    pub(crate) fn offline_client() -> BaseProducer {
        ClientConfig::new()
            .set("client.id", "logweir-rdkafka-ffi-unit")
            .set("log_level", "0")
            .create()
            .expect("a handle with no brokers builds")
    }

    /// This process's resident set in KiB, from `ps`, bounded at 10 s.
    pub(crate) fn rss_kib() -> u64 {
        let mut child = Command::new("ps")
            .args(["-o", "rss=", "-p", &std::process::id().to_string()])
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()
            .expect("ps runs");
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            match child.try_wait().expect("ps can be waited on") {
                Some(_) => break,
                None if Instant::now() > deadline => {
                    let _ = child.kill();
                    let _ = child.wait();
                    panic!("ps did not answer within 10 s");
                }
                None => std::thread::sleep(Duration::from_millis(5)),
            }
        }
        let mut out = String::new();
        child
            .stdout
            .take()
            .expect("piped")
            .read_to_string(&mut out)
            .expect("ps output");
        out.trim().parse().expect("a number of KiB")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn text_is_never_lossy() {
        assert_eq!(
            CText::from_bytes(b"pa-orders".to_vec()),
            CText::Utf8("pa-orders".to_string())
        );
        // A byte sequence that is not UTF-8 stays those bytes: a lossy
        // conversion would turn two different ids into one U+FFFD string.
        let a = CText::from_bytes(vec![b'g', 0xff]);
        let b = CText::from_bytes(vec![b'g', 0xfe]);
        assert_eq!(a, CText::NotUtf8(vec![b'g', 0xff]));
        assert_ne!(a, b);
        assert_eq!(a.as_str(), None);
        assert_eq!(a.display(), "g\\xff");
        assert_eq!(CText::Null.as_str(), None);
        assert_eq!(CText::Null.display(), "(null)");
    }

    #[test]
    fn a_call_error_names_its_integer_code() {
        let e = CallError::Call {
            code: 131,
            message: CText::Utf8("a code rdkafka-sys has no variant for".to_string()),
        };
        assert_eq!(
            e.to_string(),
            "the call failed (131): a code rdkafka-sys has no variant for"
        );
    }
}
