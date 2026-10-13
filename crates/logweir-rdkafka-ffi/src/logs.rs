//! librdkafka's own log lines, read as events from a private queue
//! (`rd_kafka_set_log_queue`, `rd_kafka_event_log`).
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
//! can run against an endpoint at all.
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
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
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

/// How long the pump thread waits for one line before it looks at its stop
/// flag: the most a [`LogPumped`]'s drop waits for the thread.
const PUMP_POLL_MS: c_int = 100;

/// Where a pumped client's log lines go. A plain function: the caller decides
/// how a line is written (`logweir-kafka` escapes it onto one line, FX-43);
/// this crate interprets nothing.
pub type LogSink = fn(&LogLine);

/// A value that owns a librdkafka client handle: the clients
/// [`LogPumped`] wraps.
pub trait OwnsClient {
    /// The client's context.
    type Context: ClientContext;
    /// The client.
    fn native_client(&self) -> &Client<Self::Context>;
}

impl<C: ClientContext> OwnsClient for rdkafka::admin::AdminClient<C> {
    type Context = C;
    fn native_client(&self) -> &Client<C> {
        self.inner()
    }
}

impl<C: rdkafka::consumer::ConsumerContext> OwnsClient for rdkafka::consumer::BaseConsumer<C> {
    type Context = C;
    fn native_client(&self) -> &Client<C> {
        rdkafka::consumer::Consumer::client(self)
    }
}

impl<C: rdkafka::producer::ProducerContext> OwnsClient for rdkafka::producer::BaseProducer<C> {
    type Context = C;
    fn native_client(&self) -> &Client<C> {
        rdkafka::producer::Producer::client(self)
    }
}

/// The pump thread and the private queue it polls.
struct Pump {
    stop: Arc<AtomicBool>,
    thread: Option<std::thread::JoinHandle<()>>,
    /// Held for its `Drop` alone: destroyed after the thread is joined.
    _queue: PumpQueue,
}

/// The pump's private queue, held only so it is destroyed, once, after the
/// thread that polls it is joined. It has no method: nothing reaches the
/// queue through it.
struct PumpQueue(#[allow(dead_code)] Queue);

// SAFETY: a `PumpQueue` gives no access to its queue, so sharing a reference
// to it across threads shares nothing (`Sync`). Moving it to another thread
// moves only the right to destroy the queue (`Send`), which
// `rd_kafka_queue_destroy` allows from any thread: librdkafka's queues are
// thread-safe, and the one thread that polled it has been joined by then
// (`LogPumped`'s `Drop`). This is what lets a `LogPumped` client be held by a
// `Send + Sync` reader, as the clients it wraps are.
unsafe impl Send for PumpQueue {}
// SAFETY: see the `Send` impl above: no `&PumpQueue` method exists.
unsafe impl Sync for PumpQueue {}

/// **A client whose own log lines go to a [`LogSink`] and never to
/// librdkafka's default logger** (FX-43).
///
/// rust-rdkafka 0.36.2 installs no log callback, so a client's lines go to
/// librdkafka's default logger, which prints them RAW on the process's stderr:
/// a bootstrap address or a broker's message holding a line break then starts
/// a line of a pod log a controller reads by key. A client created with
/// `log.queue=true` queues its lines instead; this forwards that queue to a
/// private one and polls it on a thread this value owns, handing each line to
/// the sink. No callback is installed, so no librdkafka thread ever enters
/// Rust code.
///
/// Dropping it stops the thread (it drains what is queued first), joins it,
/// forwards the logs back to the main queue and destroys the private queue,
/// in that order, and only then drops the client: [`Drop`] here runs before
/// the fields drop. Lines the client logs while it is destroyed go to the main
/// queue and are freed with it.
pub struct LogPumped<T: OwnsClient> {
    pump: Option<Pump>,
    inner: T,
}

impl<T: OwnsClient> LogPumped<T> {
    /// Pump `inner`'s log lines to `sink`.
    ///
    /// # Errors
    ///
    /// [`CallError::Call`] with [`crate::code::NOT_CONFIGURED`] when the
    /// client was not created with `log.queue=true`, and
    /// [`CallError::Options`] when the queue or the thread could not be
    /// made. The client is dropped with the error.
    pub fn new(inner: T, sink: LogSink) -> Result<Self, CallError> {
        let queue = Queue::new(inner.native_client())?;
        // SAFETY: `inner.native_client().native_ptr()` is the live
        // `rd_kafka_t` that `inner` owns for this whole call, and `queue` is a
        // live queue of that handle; librdkafka takes its own reference to the
        // queue (`rd_kafka_q_keep`). The code is read as an integer (T12).
        let code =
            unsafe { sys::set_log_queue(inner.native_client().native_ptr(), queue.as_ptr()) };
        if code != 0 {
            return Err(CallError::Call {
                code,
                message: CText::Utf8(
                    "rd_kafka_set_log_queue refused: the client was not created with \
                     log.queue=true"
                        .to_string(),
                ),
            });
        }
        let stop = Arc::new(AtomicBool::new(false));
        let poller = queue.poller();
        let flag = Arc::clone(&stop);
        let spawned = std::thread::Builder::new()
            .name("rdkafka-log-pump".to_string())
            .spawn(move || loop {
                // SAFETY: the queue this poller came from is owned by the
                // `Pump` that owns this thread's handle, and `LogPumped`'s
                // `Drop` joins this thread before that queue is destroyed;
                // the wait is bounded by `PUMP_POLL_MS`.
                match unsafe { poller.poll(PUMP_POLL_MS) } {
                    Some(event) => {
                        if event.event_type() == rd::RD_KAFKA_EVENT_LOG {
                            if let Some(line) = log_line(&event) {
                                sink(&line);
                            }
                        }
                    }
                    None if flag.load(Ordering::Acquire) => break,
                    None => {}
                }
            });
        match spawned {
            Ok(thread) => Ok(Self {
                pump: Some(Pump {
                    stop,
                    thread: Some(thread),
                    _queue: PumpQueue(queue),
                }),
                inner,
            }),
            Err(e) => {
                // SAFETY: as above; NULL forwards the logs back to the main
                // queue before `queue` is destroyed at the end of this arm.
                let _ = unsafe {
                    sys::set_log_queue(inner.native_client().native_ptr(), std::ptr::null_mut())
                };
                Err(CallError::Options {
                    code: -1,
                    message: format!("the log pump thread could not start: {e}"),
                })
            }
        }
    }
}

impl<T: OwnsClient> std::ops::Deref for LogPumped<T> {
    type Target = T;
    fn deref(&self) -> &T {
        &self.inner
    }
}

impl<T: OwnsClient> Drop for LogPumped<T> {
    fn drop(&mut self) {
        let Some(mut pump) = self.pump.take() else {
            return;
        };
        pump.stop.store(true, Ordering::Release);
        if let Some(thread) = pump.thread.take() {
            // A sink that panicked ended the thread early; there is nothing
            // to do about it here but go on with the teardown.
            let _ = thread.join();
        }
        // SAFETY: `self.inner` still owns its live `rd_kafka_t` (fields drop
        // after this body). NULL forwards the logs back to the main queue,
        // dropping librdkafka's reference to the private queue, which `pump`
        // destroys when it drops at the end of this body; the thread that
        // polled it has been joined above.
        let _ = unsafe {
            sys::set_log_queue(
                self.inner.native_client().native_ptr(),
                std::ptr::null_mut(),
            )
        };
        drop(pump);
    }
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

    /// Every line a pumped client logs reaches the sink, and none reaches
    /// librdkafka's default logger; a handle without `log.queue=true` is
    /// refused. KILLS: a pump that never forwards the log queue (the sink sees
    /// nothing); one that drops the lines queued before it started.
    #[test]
    fn a_pumped_clients_lines_reach_the_sink() {
        use std::sync::Mutex;
        static SEEN: Mutex<Vec<LogLine>> = Mutex::new(Vec::new());
        fn sink(line: &LogLine) {
            SEEN.lock().expect("not poisoned").push(line.clone());
        }
        let p = LogPumped::new(queued_logs_client(), sink).expect("a queued-logs handle pumps");
        let _ = p.client().fetch_metadata(None, Duration::from_millis(200));
        drop(p);
        let seen = SEEN.lock().expect("not poisoned").clone();
        assert!(
            !seen.is_empty(),
            "librdkafka logs its start-up under debug=generic"
        );
        assert!(seen.iter().all(|l| (0..=7).contains(&l.level)), "{seen:?}");
        // NEGATIVE CONTROL: a handle that prints to the default logger cannot
        // be pumped.
        match LogPumped::new(crate::test_support::offline_client(), sink) {
            Err(CallError::Call { code: c, .. }) => assert_eq!(c, code::NOT_CONFIGURED),
            Err(other) => panic!("expected NOT_CONFIGURED, got {other:?}"),
            Ok(_) => panic!("a handle without log.queue was pumped"),
        }
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
