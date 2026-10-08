//! The shared half of every call: owning guards for librdkafka's options,
//! queue and event objects, the bounded runner that ties them together, and
//! the readers that copy a result's text and arrays out while their owner
//! lives (obligations 1, 2, 4 and 7 of the crate documentation).
//!
//! Every guard holds a `NonNull` raw pointer, so none of them is `Send` or
//! `Sync`, and each destroys its object in `Drop`, exactly once.
use crate::{sys, CText, CallError, RawError, MAX_TIMEOUT, MIN_TIMEOUT, POLL_MARGIN};
use rdkafka::bindings as rd;
use rdkafka::client::{Client, ClientContext};
use std::ffi::CStr;
use std::os::raw::{c_char, c_int};
use std::ptr::NonNull;
use std::time::{Duration, Instant};

/// The size of the error-string buffers librdkafka's option setters fill.
const ERRSTR_LEN: usize = 512;

/// A request timeout in milliseconds, refused outside
/// [`MIN_TIMEOUT`]..=[`MAX_TIMEOUT`] before anything is created.
pub(crate) fn timeout_ms(timeout: Duration) -> Result<c_int, CallError> {
    if !(MIN_TIMEOUT..=MAX_TIMEOUT).contains(&timeout) {
        return Err(CallError::InvalidInput(format!(
            "a request timeout of {timeout:?} is outside {MIN_TIMEOUT:?}..={MAX_TIMEOUT:?}"
        )));
    }
    c_int::try_from(timeout.as_millis())
        .map_err(|_| CallError::InvalidInput(format!("{timeout:?} does not fit a C int")))
}

/// The text a librdkafka error-string buffer holds: up to its first NUL, or
/// the whole buffer if librdkafka wrote none. Safe: it reads only the slice.
pub(crate) fn errstr_text(buf: &[c_char]) -> String {
    let bytes: Vec<u8> = buf
        .iter()
        .take_while(|c| **c != 0)
        .map(|c| *c as u8)
        .collect();
    String::from_utf8_lossy(&bytes).into_owned()
}

/// Copies a C string librdkafka returned.
///
/// # Safety
///
/// `ptr` is NULL, or points to a NUL-terminated string that stays valid and
/// unmodified while `_owner` is borrowed. `_owner` is the guard that owns the
/// string (an [`Event`], a [`GroupList`]), and the borrow ties the read to it.
pub(crate) unsafe fn text<O: ?Sized>(_owner: &O, ptr: *const c_char) -> CText {
    if ptr.is_null() {
        return CText::Null;
    }
    // SAFETY: by this function's contract `ptr` is non-NULL here and points to
    // a NUL-terminated string that `_owner` keeps alive and unmodified for the
    // whole read; `to_bytes().to_vec()` copies it before the borrow ends.
    let bytes = unsafe { CStr::from_ptr(ptr) }.to_bytes().to_vec();
    CText::from_bytes(bytes)
}

/// A slice over an array librdkafka returned, borrowing its owner.
///
/// # Safety
///
/// `ptr` is NULL (then the slice is empty, whatever `len` says), or points to
/// `len` initialised, properly aligned values of `T` that stay valid and
/// unmodified while `owner` is borrowed (the returned slice borrows it). librdkafka allocated them, so
/// `len * size_of::<T>()` cannot exceed `isize::MAX`.
pub(crate) unsafe fn array<O: ?Sized, T>(owner: &O, ptr: *const T, len: usize) -> &[T] {
    let _ = owner;
    if ptr.is_null() || len == 0 {
        return &[];
    }
    // SAFETY: by this function's contract `ptr` is non-NULL, aligned and
    // points to `len` initialised `T` owned by `owner`; the returned slice's
    // lifetime is the borrow of `owner`, so it cannot outlive the memory.
    unsafe { std::slice::from_raw_parts(ptr, len) }
}

/// Copies one `rd_kafka_error_t` librdkafka returned inside a result.
///
/// # Safety
///
/// `error` is NULL or a valid `rd_kafka_error_t` that `owner` keeps alive.
/// It is NOT destroyed here: the result that holds it frees it.
pub(crate) unsafe fn raw_error<O: ?Sized>(
    owner: &O,
    error: *const rd::rd_kafka_error_t,
) -> Option<RawError> {
    if error.is_null() {
        return None;
    }
    // SAFETY: `error` is a valid, live `rd_kafka_error_t` (this function's
    // contract). The code is read through the integer-typed declaration (T12);
    // the name and message accessors return strings the error owns, copied by
    // `text` while `owner` (which owns the error) is still borrowed.
    unsafe {
        Some(RawError {
            code: sys::error_code(error),
            name: text(owner, rd::rd_kafka_error_name(error)),
            message: text(owner, rd::rd_kafka_error_string(error)),
        })
    }
}

/// An error object librdkafka returned for the caller to destroy (the
/// `rd_kafka_error_t *` of an option setter): copied, then destroyed once.
struct OwnedError(NonNull<rd::rd_kafka_error_t>);

impl OwnedError {
    fn into_call_error(self) -> CallError {
        // SAFETY: `self.0` is a live error object this guard owns; `raw_error`
        // only reads it, and `self` (its owner) outlives the read.
        let e = unsafe { raw_error(&self, self.0.as_ptr()) };
        match e {
            Some(e) => CallError::Options {
                code: e.code,
                message: e.message.display(),
            },
            None => CallError::Options {
                code: -1,
                message: "an option setter returned an error object that reads as NULL".to_string(),
            },
        }
    }
}

impl Drop for OwnedError {
    fn drop(&mut self) {
        // SAFETY: librdkafka returned this error object to the caller to
        // destroy (`rdkafka.h`: "the returned error object ... must be
        // destroyed with rd_kafka_error_destroy()"); this guard is its only
        // owner and `drop` runs once, so it is destroyed exactly once.
        unsafe { rd::rd_kafka_error_destroy(self.0.as_ptr()) }
    }
}

/// An `rd_kafka_AdminOptions_t`, destroyed exactly once on drop.
pub(crate) struct Options(NonNull<rd::rd_kafka_AdminOptions_t>);

impl Options {
    /// Options for `op` with librdkafka's request timeout set to `timeout_ms`.
    fn new<C: ClientContext>(
        client: &Client<C>,
        op: rd::rd_kafka_admin_op_t,
        timeout_ms: c_int,
    ) -> Result<Options, CallError> {
        // SAFETY: `client.native_ptr()` is the live `rd_kafka_t` that `client`
        // borrows for this whole call; `op` is a valid variant of the enum the
        // C function takes. The result is a new object or NULL.
        let raw = unsafe { rd::rd_kafka_AdminOptions_new(client.native_ptr(), op) };
        let options = Options(NonNull::new(raw).ok_or_else(|| CallError::Options {
            code: -1,
            message: format!("rd_kafka_AdminOptions_new refused {op:?}"),
        })?);
        let mut errstr: [c_char; ERRSTR_LEN] = [0; ERRSTR_LEN];
        // SAFETY: the options object is live (owned by `options`); `errstr` is
        // a writable buffer of exactly `errstr.len()` bytes, which librdkafka
        // writes at most that many of, NUL-terminated. The return value is read
        // as an integer (T12).
        let code = unsafe {
            sys::set_request_timeout(
                options.as_ptr(),
                timeout_ms,
                errstr.as_mut_ptr(),
                errstr.len(),
            )
        };
        if code != 0 {
            return Err(CallError::Options {
                code,
                message: errstr_text(&errstr),
            });
        }
        Ok(options)
    }

    /// The raw pointer, for a call that reads (copies) the options.
    pub(crate) fn as_ptr(&self) -> *mut rd::rd_kafka_AdminOptions_t {
        self.0.as_ptr()
    }

    /// Asks the broker for the caller's authorized operations (KIP-430).
    pub(crate) fn include_authorized_operations(&self) -> Result<(), CallError> {
        // SAFETY: the options object is live (owned by `self`). The function
        // returns NULL or a new error object the caller must destroy, which
        // `OwnedError` takes ownership of at once.
        let e = unsafe {
            rd::rd_kafka_AdminOptions_set_include_authorized_operations(self.as_ptr(), 1)
        };
        match NonNull::new(e) {
            None => Ok(()),
            Some(e) => Err(OwnedError(e).into_call_error()),
        }
    }
}

impl Drop for Options {
    fn drop(&mut self) {
        // SAFETY: created by `rd_kafka_AdminOptions_new` and owned by this
        // guard alone. A request COPIES the options it is given
        // (`rd_kafka_AdminOptions_copy_to`, `rdkafka_admin.c:637-640`), so no
        // in-flight request refers to this object, and destroying it once here
        // is the only destroy.
        unsafe { rd::rd_kafka_AdminOptions_destroy(self.0.as_ptr()) }
    }
}

/// A private result queue, destroyed exactly once on drop.
pub(crate) struct Queue(NonNull<rd::rd_kafka_queue_t>);

impl Queue {
    fn new<C: ClientContext>(client: &Client<C>) -> Result<Queue, CallError> {
        // SAFETY: `client.native_ptr()` is the live `rd_kafka_t` that `client`
        // borrows for this whole call. The result is a new queue or NULL.
        let raw = unsafe { rd::rd_kafka_queue_new(client.native_ptr()) };
        Ok(Queue(NonNull::new(raw).ok_or_else(|| {
            CallError::Options {
                code: -1,
                message: "rd_kafka_queue_new returned NULL".to_string(),
            }
        })?))
    }

    /// The raw pointer, for the call that will post its result here.
    pub(crate) fn as_ptr(&self) -> *mut rd::rd_kafka_queue_t {
        self.0.as_ptr()
    }

    /// Waits at most `timeout_ms` for one event.
    fn poll(&self, timeout_ms: c_int) -> Option<Event> {
        // SAFETY: the queue is live (owned by `self`); `timeout_ms` is finite
        // and positive, so the wait is bounded. The result is NULL or an event
        // the application owns and must destroy, which `Event` takes at once.
        let raw = unsafe { rd::rd_kafka_queue_poll(self.as_ptr(), timeout_ms) };
        NonNull::new(raw).map(Event)
    }
}

impl Drop for Queue {
    fn drop(&mut self) {
        // SAFETY: created by `rd_kafka_queue_new` and owned by this guard
        // alone, so this is its only destroy. A request still in flight holds
        // its OWN reference to the underlying queue (`rd_kafka_set_replyq` takes
        // `rd_kafka_q_keep`, `rdkafka_queue.h:730-738`); destroying the owner
        // handle disables that queue (`rd_kafka_q_destroy_owner`, `:253-255`),
        // and a result arriving later is destroyed by librdkafka on enqueue
        // (`:440`). No pointer to the queue is kept anywhere else.
        unsafe { rd::rd_kafka_queue_destroy(self.0.as_ptr()) }
    }
}

/// One result event, destroyed exactly once on drop. Everything a result
/// accessor returns is owned by the event and is valid only while it lives.
pub(crate) struct Event(NonNull<rd::rd_kafka_event_t>);

impl Event {
    /// The raw pointer, for result accessors whose output this event owns.
    pub(crate) fn as_ptr(&self) -> *mut rd::rd_kafka_event_t {
        self.0.as_ptr()
    }

    fn event_type(&self) -> c_int {
        // SAFETY: the event is live (owned by `self`); the accessor only reads
        // its type field, an `int` in the binding.
        unsafe { rd::rd_kafka_event_type(self.as_ptr()) }
    }

    /// The error of the whole call, if any, with its message.
    fn error(&self) -> Option<(i32, CText)> {
        // SAFETY: the event is live (owned by `self`); the code is read through
        // the integer-typed declaration (T12), and the message is a string the
        // event owns, copied by `text` while `self` is borrowed.
        unsafe {
            let code = sys::event_error(self.as_ptr());
            (code != 0).then(|| {
                (
                    code,
                    text(self, rd::rd_kafka_event_error_string(self.as_ptr())),
                )
            })
        }
    }
}

impl Drop for Event {
    fn drop(&mut self) {
        // SAFETY: `rd_kafka_queue_poll` handed this event to the application,
        // which must destroy it (`rdkafka.h`, `rd_kafka_queue_poll`); this guard
        // is its only owner, so it is destroyed exactly once, after every read
        // of the results it owns (they borrow `self`).
        unsafe { rd::rd_kafka_event_destroy(self.0.as_ptr()) }
    }
}

/// **One admin call, bounded, with every object destroyed exactly once.**
///
/// Builds options for `op` with `timeout` as librdkafka's request timeout,
/// lets `configure` set more, creates a private queue, lets `send` enqueue the
/// request on it, polls for at most `timeout` + [`POLL_MARGIN`], checks that
/// the event is `expected` and carries no whole-call error, and hands it to
/// `read`, which copies the result out. Then it destroys the event, the queue
/// and the options, in that order, on every path (the guards' `Drop`).
///
/// `T: Send + 'static` is the compile-time half of obligation 2: a raw
/// pointer is neither, so nothing `read` returns can point into the event.
pub(crate) fn run<C, T>(
    client: &Client<C>,
    op: rd::rd_kafka_admin_op_t,
    expected: c_int,
    timeout: Duration,
    configure: impl FnOnce(&Options) -> Result<(), CallError>,
    send: impl FnOnce(*mut rd::rd_kafka_t, &Options, &Queue),
    read: impl FnOnce(&Event) -> Result<T, CallError>,
) -> Result<T, CallError>
where
    C: ClientContext,
    T: Send + 'static,
{
    let ms = timeout_ms(timeout)?;
    let options = Options::new(client, op, ms)?;
    configure(&options)?;
    let queue = Queue::new(client)?;
    send(client.native_ptr(), &options, &queue);
    let margin = c_int::try_from(POLL_MARGIN.as_millis()).unwrap_or(c_int::MAX);
    let started = Instant::now();
    let event = queue
        .poll(ms.saturating_add(margin))
        .ok_or(CallError::NoResult {
            waited: started.elapsed(),
        })?;
    if event.event_type() != expected {
        return Err(CallError::UnexpectedResult(format!(
            "event type {} where {expected} was expected",
            event.event_type()
        )));
    }
    if let Some((code, message)) = event.error() {
        return Err(CallError::Call { code, message });
    }
    let out = read(&event);
    drop(event);
    drop(queue);
    drop(options);
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_timeout_outside_the_bounds_is_refused_before_anything_is_created() {
        assert!(matches!(
            timeout_ms(Duration::from_millis(999)),
            Err(CallError::InvalidInput(_))
        ));
        assert!(matches!(
            timeout_ms(Duration::from_secs(301)),
            Err(CallError::InvalidInput(_))
        ));
        assert_eq!(timeout_ms(Duration::from_secs(1)), Ok(1000));
        assert_eq!(timeout_ms(Duration::from_secs(300)), Ok(300_000));
    }

    #[test]
    fn an_errstr_buffer_is_read_up_to_its_nul_and_never_past_its_end() {
        let mut buf: [c_char; 8] = [0; 8];
        for (i, b) in b"bad\0junk".iter().enumerate() {
            buf[i] = *b as c_char;
        }
        assert_eq!(errstr_text(&buf), "bad");
        // No NUL at all: the whole buffer, and not a byte beyond it.
        let full: [c_char; 4] = [
            b'a' as c_char,
            b'b' as c_char,
            b'c' as c_char,
            b'd' as c_char,
        ];
        assert_eq!(errstr_text(&full), "abcd");
    }

    #[test]
    fn the_array_reader_never_builds_a_slice_from_null() {
        let owner = ();
        // SAFETY: a NULL pointer, which the contract allows; nothing is read.
        let s: &[u32] = unsafe { array(&owner, std::ptr::null(), 5) };
        assert!(s.is_empty());
        let v = [7u32, 8, 9];
        // SAFETY: `v` is three initialised, aligned `u32` that outlive the
        // slice (`v` lives until the end of this test).
        let s = unsafe { array(&v, v.as_ptr(), 3) };
        assert_eq!(s, &[7, 8, 9]);
        // SAFETY: NULL again, so a NULL C string is `Null`, never a read.
        assert_eq!(unsafe { text(&owner, std::ptr::null()) }, CText::Null);
    }
}

/// **The broker-free soak** (PROD-04.0 §7.2: thousands of calls with a
/// bounded resident set). It drives [`run`] through librdkafka's own
/// immediate refusals, which need no broker: a description of NO groups and a
/// description that names one group twice are both answered at once with an
/// INVALID_ARG result (`rdkafka_admin.c:8756-8763`, `:8782-8791`), after
/// librdkafka copied the inputs and built its request. So each iteration
/// creates and destroys an options object, a queue, a request with copied
/// ids, an error result and an event, and copies the error's text out: every
/// allocation path of the runner. A leak of one object per call is tens of
/// bytes or more, so 100,000 calls would move the resident set by megabytes.
#[cfg(test)]
mod soak {
    use super::*;
    use crate::code;
    use rdkafka::producer::Producer;
    use std::io::Read;
    use std::process::{Command, Stdio};

    /// This process's resident set in KiB, from `ps`, bounded at 10 s.
    fn rss_kib() -> u64 {
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

    /// One call librdkafka refuses at once: `groups` names the ids to send
    /// (none, or one id twice).
    fn refused_at_once<C: ClientContext>(
        client: &Client<C>,
        groups: &[&std::ffi::CStr],
    ) -> CallError {
        let mut pointers: Vec<*const c_char> = groups.iter().map(|g| g.as_ptr()).collect();
        let r: Result<(), CallError> = run(
            client,
            rd::rd_kafka_admin_op_t::RD_KAFKA_ADMIN_OP_DESCRIBECONSUMERGROUPS,
            rd::RD_KAFKA_EVENT_DESCRIBECONSUMERGROUPS_RESULT,
            Duration::from_secs(1),
            |o| o.include_authorized_operations(),
            |rk, options, queue| {
                // SAFETY: `rk`, `options` and `queue` are live for the whole of
                // `run`; `pointers` holds `pointers.len()` pointers to
                // NUL-terminated ids that outlive the call (an empty array when
                // there are none, which librdkafka refuses before reading it).
                unsafe {
                    rd::rd_kafka_DescribeConsumerGroups(
                        rk,
                        pointers.as_mut_ptr(),
                        pointers.len(),
                        options.as_ptr(),
                        queue.as_ptr(),
                    )
                }
            },
            |_| Ok(()),
        );
        r.expect_err("librdkafka refuses these inputs")
    }

    #[test]
    fn a_hundred_thousand_calls_keep_the_resident_set_bounded() {
        let p = crate::test_support::offline_client();
        let client = p.client();
        let id = std::ffi::CString::new("pa-orders").expect("no NUL");
        let twice = [id.as_c_str(), id.as_c_str()];
        let call = |i: usize| {
            let e = if i % 2 == 0 {
                refused_at_once(client, &[])
            } else {
                refused_at_once(client, &twice)
            };
            match e {
                CallError::Call { code: c, message } => {
                    assert_eq!(c, code::INVALID_ARG, "{}", message.display());
                }
                other => panic!("unexpected {other}"),
            }
        };
        for i in 0..2_000 {
            call(i);
        }
        let before = rss_kib();
        let started = Instant::now();
        for i in 0..100_000 {
            call(i);
        }
        let took = started.elapsed();
        let after = rss_kib();
        let grew = after.saturating_sub(before);
        eprintln!("soak: 100000 calls in {took:?}; resident set {before} KiB -> {after} KiB (+{grew} KiB)");
        assert!(
            grew < 2048,
            "100000 calls grew the resident set by {grew} KiB ({before} -> {after}): a leak"
        );
    }
}
