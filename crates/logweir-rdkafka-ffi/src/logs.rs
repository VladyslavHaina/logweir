//! librdkafka's own log lines, read as events from a private queue
//! (`rd_kafka_set_log_queue`, `rd_kafka_event_log`; PROD-01.2).
//!
//! # Why Logweir reads a client's log at all
//!
//! librdkafka asks every broker it connects to for its ApiVersions, the
//! range of versions the broker serves for each Kafka API, and keeps the
//! answer private: no public call returns it. The only place it surfaces is
//! the `feature` debug context, one line per API key
//! (`rd_kafka_handle_ApiVersion`, `rdkafka_request.c:3194-3209` in rdkafka-sys
//! 4.10.0+2.12.1). The engine Logweir drives never negotiates and sends each
//! request at one fixed version, so that answer decides whether an operation
//! can run against an endpoint at all
//! (`docs/to-do/decisions/PROD-01.2-compatibility-contract.md`).
//!
//! rust-rdkafka 0.36.2 installs no log callback and does not enable the log
//! event on the queues it polls (`base_consumer.rs:66-72`), so those lines go
//! to librdkafka's default logger, which prints to stderr. With the
//! configuration property `log.queue=true` librdkafka queues them instead
//! (`rd_kafka_log_buf`, `rdkafka.c:253-277`), and [`drain_logs`] forwards
//! that queue to a private one and reads it. **No callback is installed:**
//! every line is read on the calling thread, as an event this crate owns, so
//! no librdkafka thread ever enters Rust code.
//!
//! This module copies lines out and interprets nothing. What a line means is
//! decided in `logweir-kafka` (`api_versions`).
use crate::raw::{text, Event, Queue};
use crate::{sys, CText, CallError};
use rdkafka::bindings as rd;
use rdkafka::client::{Client, ClientContext};
use std::os::raw::{c_char, c_int};
use std::time::{Duration, Instant};

/// The shortest quiet period [`drain_logs`] accepts.
pub const MIN_QUIET: Duration = Duration::from_millis(10);

/// The longest quiet period [`drain_logs`] accepts.
pub const MAX_QUIET: Duration = Duration::from_secs(5);

/// The longest a drain may run in all.
pub const MAX_DRAIN: Duration = Duration::from_secs(60);

/// The most lines one drain returns: a broker's ApiVersions answer is under a
/// hundred lines, so this is hundreds of connections' worth.
pub const MAX_LINES: usize = 50_000;

/// One line librdkafka logged.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogLine {
    /// The syslog level: 7 is debug.
    pub level: i32,
    /// The facility, for example `APIVERSION`.
    pub facility: CText,
    /// The message, with librdkafka's own thread and broker prefix.
    pub message: CText,
}

/// What one drain read, and how it ended.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Drained {
    /// Every log line read, in the order librdkafka queued them.
    pub lines: Vec<LogLine>,
    /// True when the drain ended because the queue stayed empty for the whole
    /// quiet period: nothing was being logged, so no group of lines was cut
    /// short. False when it stopped at its time or line bound, and the last
    /// lines read may be the start of a group whose rest was not read.
    pub quiet: bool,
}

/// The forwarding of a client's log queue to a private queue, undone exactly
/// once on drop: the logs go back to the main queue, librdkafka's own
/// default, before the private queue is destroyed.
struct Forwarding<'c, C: ClientContext> {
    client: &'c Client<C>,
    queue: Queue,
}

impl<C: ClientContext> Drop for Forwarding<'_, C> {
    fn drop(&mut self) {
        // SAFETY: `self.client` borrows the live `rd_kafka_t` for as long as
        // this guard exists. A NULL queue is the documented way to forward the
        // logs to the main queue again (`rdkafka.h`, `rd_kafka_set_log_queue`;
        // `rdkafka_queue.c:964-977`), which drops librdkafka's reference to
        // the private queue; `self.queue` is destroyed after this body, by its
        // own `Drop`. The return code is read as an integer (T12) and
        // ignored: the only failure is NOT_CONFIGURED, which `drain_logs`
        // already refused before this guard was built.
        let _ = unsafe { sys::set_log_queue(self.client.native_ptr(), std::ptr::null_mut()) };
    }
}

/// Copies the one log line a log event carries.
fn log_line(event: &Event) -> Option<LogLine> {
    let mut facility: *const c_char = std::ptr::null();
    let mut message: *const c_char = std::ptr::null();
    let mut level: c_int = 0;
    let (fac, msg, lvl) = (&mut facility, &mut message, &mut level);
    // SAFETY: the event is live (owned by `event`); the three out-pointers
    // point to initialised locals that outlive the call. The function writes
    // them and returns 0 only for a log event (`rdkafka_event.c`,
    // `rd_kafka_event_log`), and the two strings it hands back are owned by
    // the event.
    let rc = unsafe { rd::rd_kafka_event_log(event.as_ptr(), fac, msg, lvl) };
    if rc != 0 {
        return None;
    }
    // SAFETY: on success `facility` and `message` are NULL or NUL-terminated
    // strings the event owns; `text` copies each while `event` is borrowed
    // (obligations 2 and 7).
    unsafe {
        Some(LogLine {
            level,
            facility: text(event, facility),
            message: text(event, message),
        })
    }
}

/// **Reads the log lines `client` has queued, and those it queues while the
/// drain runs.**
///
/// The client must have been created with `log.queue=true`; without it
/// librdkafka refuses the forwarding and this returns
/// [`CallError::Call`] with [`crate::code::NOT_CONFIGURED`]. Lines queued
/// before the call are read too: forwarding moves them
/// (`rd_kafka_q_fwd_set0`, `rdkafka_queue.c:186-191`).
///
/// The drain ends when no line arrives for `quiet` (then
/// [`Drained::quiet`] is true), when `within` has passed, or at
/// [`MAX_LINES`]. On every path the logs are forwarded back to the main queue
/// and the private queue is destroyed.
///
/// # Errors
///
/// [`CallError::InvalidInput`] for a `quiet` outside
/// [`MIN_QUIET`]..=[`MAX_QUIET`] or a `within` above [`MAX_DRAIN`], before
/// anything is created; [`CallError::Call`] when librdkafka refuses the
/// forwarding.
pub fn drain_logs<C: ClientContext>(
    client: &Client<C>,
    quiet: Duration,
    within: Duration,
) -> Result<Drained, CallError> {
    if !(MIN_QUIET..=MAX_QUIET).contains(&quiet) {
        return Err(CallError::InvalidInput(format!(
            "a quiet period of {quiet:?} is outside {MIN_QUIET:?}..={MAX_QUIET:?}"
        )));
    }
    if within > MAX_DRAIN {
        return Err(CallError::InvalidInput(format!(
            "a drain of {within:?} is longer than {MAX_DRAIN:?}"
        )));
    }
    let quiet_ms = c_int::try_from(quiet.as_millis())
        .map_err(|_| CallError::InvalidInput(format!("{quiet:?} does not fit a C int")))?;
    let queue = Queue::new(client)?;
    // SAFETY: `client.native_ptr()` is the live `rd_kafka_t` that `client`
    // borrows for this whole call, and `queue` is a live queue of that
    // handle. librdkafka takes its own reference to the queue
    // (`rd_kafka_q_keep`, `rdkafka_queue.c:184`). The return code is read as
    // an integer (T12).
    let code = unsafe { sys::set_log_queue(client.native_ptr(), queue.as_ptr()) };
    if code != 0 {
        return Err(CallError::Call {
            code,
            message: CText::Utf8(
                "rd_kafka_set_log_queue refused: the client was not created with log.queue=true"
                    .to_string(),
            ),
        });
    }
    let forwarding = Forwarding { client, queue };
    let started = Instant::now();
    let mut lines = Vec::new();
    let quiet = loop {
        if lines.len() >= MAX_LINES || started.elapsed() >= within {
            break false;
        }
        match forwarding.queue.poll(quiet_ms) {
            None => break true,
            Some(event) => {
                if event.event_type() == rd::RD_KAFKA_EVENT_LOG {
                    lines.extend(log_line(&event));
                }
                // Any other event on a queue that only logs were forwarded to
                // is dropped with its guard.
            }
        }
    };
    drop(forwarding);
    Ok(Drained { lines, quiet })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::code;
    use crate::test_support::rss_kib;
    use rdkafka::config::{ClientConfig, RDKafkaLogLevel};
    use rdkafka::producer::{BaseProducer, Producer};

    /// A handle with no broker to reach whose logs are queued, at debug
    /// level, with the `generic` debug context on: librdkafka logs lines about
    /// its own start-up with no network at all.
    fn queued_logs_client() -> BaseProducer {
        ClientConfig::new()
            .set("client.id", "logweir-rdkafka-ffi-unit")
            .set("log.queue", "true")
            .set("debug", "generic")
            .set_log_level(RDKafkaLogLevel::Debug)
            .create()
            .expect("a handle with no brokers builds")
    }

    #[test]
    fn a_handle_without_a_log_queue_is_refused_and_nothing_is_read() {
        let p = crate::test_support::offline_client();
        match drain_logs(p.client(), MIN_QUIET, Duration::from_secs(1)) {
            Err(CallError::Call { code: c, .. }) => assert_eq!(c, code::NOT_CONFIGURED),
            other => panic!("expected NOT_CONFIGURED, got {other:?}"),
        }
    }

    #[test]
    fn the_bounds_are_checked_before_anything_is_created() {
        let p = queued_logs_client();
        for (quiet, within) in [
            (Duration::from_millis(9), Duration::from_secs(1)),
            (Duration::from_secs(6), Duration::from_secs(1)),
            (MIN_QUIET, Duration::from_secs(61)),
        ] {
            assert!(
                matches!(
                    drain_logs(p.client(), quiet, within),
                    Err(CallError::InvalidInput(_))
                ),
                "{quiet:?} / {within:?}"
            );
        }
    }

    /// The lines librdkafka logged BEFORE the drain are read (forwarding moves
    /// them), each with its level, facility and message, and the drain ends
    /// by quiet. A second drain reads nothing twice.
    #[test]
    fn queued_lines_are_read_once_with_their_facility() {
        let p = queued_logs_client();
        // Something to log, with no network: a metadata request on a handle
        // with no brokers fails at once and is logged under `generic`.
        let _ = p.client().fetch_metadata(None, Duration::from_millis(200));
        let first = drain_logs(
            p.client(),
            Duration::from_millis(100),
            Duration::from_secs(10),
        )
        .expect("the handle queues its logs");
        assert!(first.quiet, "a handle that logs nothing more goes quiet");
        assert!(
            !first.lines.is_empty(),
            "librdkafka logs its start-up under debug=generic"
        );
        for line in &first.lines {
            assert!((0..=7).contains(&line.level), "{line:?}");
            assert!(
                line.facility.as_str().is_some_and(|f| !f.is_empty()),
                "{line:?}"
            );
            assert!(line.message.as_str().is_some(), "{line:?}");
        }
        let second = drain_logs(
            p.client(),
            Duration::from_millis(100),
            Duration::from_secs(10),
        )
        .expect("a second drain is allowed");
        assert!(second.quiet);
        let repeated = second
            .lines
            .iter()
            .filter(|l| first.lines.contains(l))
            .count();
        assert!(
            repeated < first.lines.len(),
            "the second drain read the first drain's lines again: {repeated} of {}",
            first.lines.len()
        );
    }

    /// A drain that hits its time bound says it did not end by quiet.
    #[test]
    fn a_drain_cut_by_its_time_bound_is_not_quiet() {
        let p = queued_logs_client();
        let d = drain_logs(p.client(), Duration::from_millis(50), Duration::ZERO)
            .expect("a zero time bound is allowed");
        assert!(!d.quiet);
        assert!(d.lines.is_empty());
    }

    /// **Bounded resident set over thousands of drains** (obligation 1): each
    /// creates a queue, forwards the logs to it, polls it, forwards the logs
    /// back and destroys the queue.
    #[test]
    fn thousands_of_drains_keep_the_resident_set_bounded() {
        let p = queued_logs_client();
        let drain = || {
            drain_logs(p.client(), MIN_QUIET, Duration::from_secs(1)).expect("a drain");
        };
        for _ in 0..200 {
            drain();
        }
        let before = rss_kib();
        for _ in 0..1_500 {
            drain();
        }
        let after = rss_kib();
        let grew = after.saturating_sub(before);
        eprintln!("soak: 1500 drains; resident set {before} KiB -> {after} KiB (+{grew} KiB)");
        assert!(
            grew < 1024,
            "1500 drains grew the resident set by {grew} KiB ({before} -> {after}): a leak"
        );
    }
}
