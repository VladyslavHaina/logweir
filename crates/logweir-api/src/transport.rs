//! The stall deadline on every connection the listener serves (FX-24b).
//!
//! THE GAP. hyper's HTTP/1 server bounds the request HEAD (`main.rs`'s header
//! deadline) and nothing after it. A client that sends requests and stops
//! reading the answers leaves hyper parked in a write the kernel will not take,
//! and a client that sends a head and then stops sending its body leaves the
//! handler parked in a body read; neither has a timer, and each holds a
//! connection permit for as long as the client keeps the socket open. Measured
//! on the built binary (FX-24 review, 2026-10-08): 256 connections that
//! pipelined `GET /ui/render.js` and read nothing held every permit, and a real
//! request was still unanswered at 30 s.
//!
//! THE BOUND IS A STALL, NOT A TOTAL. Each guard here fails an operation that
//! has been PENDING, with no progress at all, for the stall limit, and forgets
//! the clock the moment one byte moves. A client that reads slowly but steadily
//! keeps its connection however long the answer takes, and a server-sent event
//! stream that is idle between heartbeats has no write pending at all, so its
//! clock never starts. What ends is the connection whose client stopped.
//!
//! TWO GUARDS, BECAUSE THE TRANSPORT CANNOT TELL A BODY READ FROM AN IDLE ONE.
//! - [`StallGuard`] wraps the connection's IO and times its OUTPUT: a pending
//!   `poll_write`, `poll_write_vectored`, `poll_flush` or `poll_shutdown`.
//! - [`StallBody`] wraps each request's body and times a body read the handler
//!   is waiting on.
//!
//! The IO's reads are deliberately NOT timed. While hyper writes an answer, and
//! while a handler works, it keeps a read pending on the socket to notice the
//! client going away (`mid_message_detect_eof`, hyper 1.11
//! `src/proto/h1/conn.rs:487-504`, reached from `poll_read_keep_alive`,
//! `:431-441`). That read is pending for the whole of every response, so a timer
//! on it would cut a five-minute event stream, and any handler slower than the
//! limit, at the limit. The head is already bounded by the header deadline; the
//! body is the one read worth timing, and only the body knows it is one.
//!
//! WHAT A FIRED GUARD DOES. Output: the write fails with
//! [`io::ErrorKind::TimedOut`], hyper ends the connection with that error, and
//! the task that held the connection permit ends with it, which returns the
//! permit. Body: the body yields a `TimedOut` error, `http::read_json` answers
//! `400 malformed_request`, and hyper, finding the body abandoned unfinished,
//! closes the read side and the connection after that answer
//! (`poll_drain_or_close_read`, hyper 1.11 `src/proto/h1/conn.rs:858-873`).

use std::future::Future as _;
use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

use bytes::Bytes;
use hyper::body::{Body, Frame, SizeHint};
use tokio::time::{Instant, Sleep};

/// The error type a [`StallBody`] yields: its inner body's error, boxed, or
/// the stall itself.
pub type BoxError = Box<dyn std::error::Error + Send + Sync>;

/// One stall clock: started by the first `Pending`, forgotten by any progress.
///
/// The timer is allocated on the first stall, not per connection: most
/// connections never wait on the kernel at all.
#[derive(Debug)]
struct StallClock {
    limit: Duration,
    sleep: Option<Pin<Box<Sleep>>>,
    running: bool,
}

impl StallClock {
    const fn new(limit: Duration) -> Self {
        Self {
            limit,
            sleep: None,
            running: false,
        }
    }

    /// The operation made progress (or failed on its own): forget the stall.
    fn progressed(&mut self) {
        self.running = false;
    }

    /// The operation is pending. Start the clock if it is not running, and say
    /// whether the limit has passed. Polling the timer registers the task's
    /// waker, so a connection that stays pending is woken at the limit.
    fn expired(&mut self, cx: &mut Context<'_>) -> bool {
        let deadline = Instant::now() + self.limit;
        let running = self.running;
        let sleep = self
            .sleep
            .get_or_insert_with(|| Box::pin(tokio::time::sleep_until(deadline)));
        if !running {
            sleep.as_mut().reset(deadline);
            self.running = true;
        }
        sleep.as_mut().poll(cx).is_ready()
    }

    fn observe<T>(
        &mut self,
        cx: &mut Context<'_>,
        result: Poll<io::Result<T>>,
        what: &str,
    ) -> Poll<io::Result<T>> {
        match result {
            Poll::Ready(result) => {
                self.progressed();
                Poll::Ready(result)
            }
            Poll::Pending if self.expired(cx) => Poll::Ready(Err(stalled(what, self.limit))),
            Poll::Pending => Poll::Pending,
        }
    }
}

/// The `TimedOut` error a guard fails with.
fn stalled(what: &str, limit: Duration) -> io::Error {
    io::Error::new(
        io::ErrorKind::TimedOut,
        format!("{what} made no progress for {} s", limit.as_secs_f64()),
    )
}

/// A connection's IO with a stall deadline on its output.
///
/// Reads pass through untouched; see the module documentation for why they
/// must.
#[derive(Debug)]
pub struct StallGuard<T> {
    inner: T,
    output: StallClock,
}

impl<T> StallGuard<T> {
    /// Wrap `inner`: an output operation pending for `limit` with no progress
    /// fails with [`io::ErrorKind::TimedOut`].
    pub const fn new(inner: T, limit: Duration) -> Self {
        Self {
            inner,
            output: StallClock::new(limit),
        }
    }
}

impl<T: hyper::rt::Read + Unpin> hyper::rt::Read for StallGuard<T> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: hyper::rt::ReadBufCursor<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_read(cx, buf)
    }
}

const OUTPUT: &str = "the connection's output";

impl<T: hyper::rt::Write + Unpin> hyper::rt::Write for StallGuard<T> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        let result = Pin::new(&mut this.inner).poll_write(cx, buf);
        this.output.observe(cx, result, OUTPUT)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        let result = Pin::new(&mut this.inner).poll_flush(cx);
        this.output.observe(cx, result, OUTPUT)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        let result = Pin::new(&mut this.inner).poll_shutdown(cx);
        this.output.observe(cx, result, OUTPUT)
    }

    fn is_write_vectored(&self) -> bool {
        self.inner.is_write_vectored()
    }

    fn poll_write_vectored(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[io::IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        let result = Pin::new(&mut this.inner).poll_write_vectored(cx, bufs);
        this.output.observe(cx, result, OUTPUT)
    }
}

/// A request body with a stall deadline on the reads a handler waits on.
///
/// The clock runs only while the body is POLLED and pending: a handler that
/// has not started reading (it is still authorizing, say) is not waiting on
/// the client, and neither is a body nobody reads.
#[derive(Debug)]
pub struct StallBody<B> {
    inner: B,
    clock: StallClock,
}

impl<B> StallBody<B> {
    /// Wrap `inner`: a frame read pending for `limit` with no frame arriving
    /// yields a [`io::ErrorKind::TimedOut`] error.
    pub const fn new(inner: B, limit: Duration) -> Self {
        Self {
            inner,
            clock: StallClock::new(limit),
        }
    }
}

impl<B> Body for StallBody<B>
where
    B: Body<Data = Bytes> + Unpin,
    B::Error: Into<BoxError>,
{
    type Data = Bytes;
    type Error = BoxError;

    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, BoxError>>> {
        let this = self.get_mut();
        match Pin::new(&mut this.inner).poll_frame(cx) {
            Poll::Ready(frame) => {
                this.clock.progressed();
                Poll::Ready(frame.map(|frame| frame.map_err(Into::into)))
            }
            Poll::Pending if this.clock.expired(cx) => Poll::Ready(Some(Err(Box::new(stalled(
                "the request body",
                this.clock.limit,
            ))))),
            Poll::Pending => Poll::Pending,
        }
    }

    fn is_end_stream(&self) -> bool {
        self.inner.is_end_stream()
    }

    fn size_hint(&self) -> SizeHint {
        self.inner.size_hint()
    }
}

/// Whether `error`, or anything in its source chain, is a stall this module
/// raised (or any other `TimedOut`).
#[must_use]
pub fn is_timeout(error: &(dyn std::error::Error + 'static)) -> bool {
    let mut current = Some(error);
    while let Some(error) = current {
        if error
            .downcast_ref::<io::Error>()
            .is_some_and(|io| io.kind() == io::ErrorKind::TimedOut)
        {
            return true;
        }
        current = error.source();
    }
    false
}

#[cfg(test)]
mod tests {
    //! The guards on real loopback sockets and on stub IO, at a 300 ms limit.
    //!
    //! Each row can fail: the non-reader and the stuck-IO rows fail without
    //! the output clock; the steady-reader and idle rows fail if the clock is a
    //! total rather than a stall; the pending-read row fails if reads are
    //! timed at the transport; the body rows fail without the body clock, or
    //! with one that never resets.

    use super::*;
    use hyper::rt::{Read as _, Write as _};
    use hyper_util::rt::TokioIo;
    use std::io::Read as _;
    use std::time::Instant as StdInstant;

    const LIMIT: Duration = Duration::from_millis(300);
    /// A generous margin for a loaded host: the clock is the server's own and
    /// fires on time, but the test thread can be scheduled late.
    const SLACK: Duration = Duration::from_millis(1500);
    /// Both kernel buffers, kept small so a client that stops reading stops
    /// the writer after a few hundred KiB rather than after several MiB.
    const SOCKET_BUFFER: u32 = 64 * 1024;

    type Server = StallGuard<TokioIo<tokio::net::TcpStream>>;

    /// A connected pair: the server side guarded at `limit`, and a blocking
    /// std client.
    async fn pair_at(limit: Duration) -> (Server, std::net::TcpStream) {
        let listening = tokio::net::TcpSocket::new_v4().unwrap();
        listening.set_send_buffer_size(SOCKET_BUFFER).unwrap();
        listening.bind("127.0.0.1:0".parse().unwrap()).unwrap();
        let listener = listening.listen(8).unwrap();
        let address = listener.local_addr().unwrap();
        let connecting = tokio::net::TcpSocket::new_v4().unwrap();
        connecting.set_recv_buffer_size(SOCKET_BUFFER).unwrap();
        let (client, accepted) = tokio::join!(connecting.connect(address), listener.accept());
        let client = client.unwrap().into_std().unwrap();
        client.set_nonblocking(false).unwrap();
        let (server, _) = accepted.unwrap();
        (StallGuard::new(TokioIo::new(server), limit), client)
    }

    async fn pair() -> (Server, std::net::TcpStream) {
        pair_at(LIMIT).await
    }

    async fn write(io: &mut Server, buf: &[u8]) -> io::Result<usize> {
        std::future::poll_fn(|cx| Pin::new(&mut *io).poll_write(cx, buf)).await
    }

    /// Write `total` bytes in 16 KiB pieces, or fail with the first error.
    async fn write_all(io: &mut Server, total: usize) -> io::Result<()> {
        let chunk = [0x5au8; 16 * 1024];
        let mut sent = 0;
        while sent < total {
            sent += write(io, &chunk[..chunk.len().min(total - sent)]).await?;
        }
        Ok(())
    }

    fn assert_at_the_limit(elapsed: Duration, what: &str) {
        assert!(
            elapsed >= LIMIT && elapsed < LIMIT + SLACK,
            "{what} failed after {elapsed:?}, not at the {LIMIT:?} limit"
        );
    }

    #[tokio::test]
    async fn a_client_that_stops_reading_fails_the_write_at_the_limit() {
        let (mut server, client) = pair().await;
        let started = StdInstant::now();
        // Far more than the two kernel buffers hold; the client never reads.
        let error = write_all(&mut server, 64 * 1024 * 1024)
            .await
            .expect_err("a write to a client that never reads must fail");
        assert_eq!(error.kind(), io::ErrorKind::TimedOut, "{error}");
        assert!(is_timeout(&error));
        assert_at_the_limit(started.elapsed(), "the stalled write");
        drop(client);
    }

    /// IO whose output never completes and never wakes anyone: the guard's
    /// own timer is the only thing that can end the wait.
    struct Stuck;

    impl hyper::rt::Write for Stuck {
        fn poll_write(
            self: Pin<&mut Self>,
            _: &mut Context<'_>,
            _: &[u8],
        ) -> Poll<io::Result<usize>> {
            Poll::Pending
        }
        fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Pending
        }
        fn poll_shutdown(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Pending
        }
        fn is_write_vectored(&self) -> bool {
            true
        }
        fn poll_write_vectored(
            self: Pin<&mut Self>,
            _: &mut Context<'_>,
            _: &[io::IoSlice<'_>],
        ) -> Poll<io::Result<usize>> {
            Poll::Pending
        }
    }

    #[tokio::test]
    async fn every_output_operation_is_timed() {
        let mut io = StallGuard::new(Stuck, LIMIT);
        assert!(hyper::rt::Write::is_write_vectored(&io));

        let started = StdInstant::now();
        let flushed = std::future::poll_fn(|cx| Pin::new(&mut io).poll_flush(cx)).await;
        assert_eq!(flushed.unwrap_err().kind(), io::ErrorKind::TimedOut);
        assert_at_the_limit(started.elapsed(), "a stuck flush");

        let mut io = StallGuard::new(Stuck, LIMIT);
        let started = StdInstant::now();
        let shut = std::future::poll_fn(|cx| Pin::new(&mut io).poll_shutdown(cx)).await;
        assert_eq!(shut.unwrap_err().kind(), io::ErrorKind::TimedOut);
        assert_at_the_limit(started.elapsed(), "a stuck shutdown");

        let mut io = StallGuard::new(Stuck, LIMIT);
        let started = StdInstant::now();
        let slices = [io::IoSlice::new(b"a"), io::IoSlice::new(b"b")];
        let written =
            std::future::poll_fn(|cx| Pin::new(&mut io).poll_write_vectored(cx, &slices)).await;
        assert_eq!(written.unwrap_err().kind(), io::ErrorKind::TimedOut);
        assert_at_the_limit(started.elapsed(), "a stuck vectored write");
    }

    #[tokio::test]
    async fn a_slow_but_steady_reader_is_never_cut() {
        // ITS OWN, LONGER LIMIT. How often a reading client lets the writer
        // progress is the receiver's window-update policy, not the reader's
        // pace: on macOS a reader taking 64 KiB every 75 ms still left the
        // writer waiting 315 ms at a time (a probe, 2026-10-08). One second is
        // several times the gaps a steady reader leaves, so a cut here is the
        // guard misjudging progress, not the kernel.
        const STEADY_LIMIT: Duration = Duration::from_secs(1);
        const TOTAL: usize = 8 * 1024 * 1024;
        let (mut server, mut client) = pair_at(STEADY_LIMIT).await;
        // At most 128 KiB every 50 ms: the writer waits on the reader nearly
        // all the time, for several limits in all, but never for a limit.
        let reader = std::thread::spawn(move || {
            client
                .set_read_timeout(Some(Duration::from_secs(10)))
                .unwrap();
            let mut buffer = vec![0u8; 128 * 1024];
            let mut total = 0usize;
            loop {
                std::thread::sleep(Duration::from_millis(50));
                match client.read(&mut buffer) {
                    Ok(0) | Err(_) => break total,
                    Ok(n) => total += n,
                }
            }
        });
        let started = StdInstant::now();
        let result = write_all(&mut server, TOTAL).await;
        let elapsed = started.elapsed();
        assert!(
            result.is_ok(),
            "a steady reader was cut after {elapsed:?}: {result:?}"
        );
        assert!(
            elapsed > STEADY_LIMIT * 2,
            "the writer finished in {elapsed:?}, so a total deadline at the limit would not \
             have cut it either: the row proves nothing about a stall"
        );
        drop(server);
        assert_eq!(reader.join().unwrap(), TOTAL);
    }

    #[tokio::test]
    async fn an_idle_connection_with_nothing_to_write_is_not_cut() {
        let (mut server, mut client) = pair().await;
        write(&mut server, b"first").await.unwrap();
        // Three limits with no write pending at all: a heartbeat's gap.
        tokio::time::sleep(LIMIT * 3).await;
        write(&mut server, b"second").await.unwrap();
        drop(server);
        let mut text = String::new();
        client.read_to_string(&mut text).unwrap();
        assert_eq!(text, "firstsecond");
    }

    #[tokio::test]
    async fn a_pending_read_on_the_connection_is_not_timed() {
        let (mut server, mut client) = pair().await;
        // hyper keeps a read pending for the whole of every answer; that read
        // must outlive the limit. Wait three limits for a read nothing feeds.
        let mut storage = [0u8; 16];
        let mut buf = hyper::rt::ReadBuf::new(&mut storage);
        let read = tokio::time::timeout(
            LIMIT * 3,
            std::future::poll_fn(|cx| Pin::new(&mut server).poll_read(cx, buf.unfilled())),
        )
        .await;
        assert!(
            read.is_err(),
            "the read ended before the client sent anything: {read:?}"
        );
        // And the connection still works.
        std::io::Write::write_all(&mut client, b"ping").unwrap();
        let read =
            std::future::poll_fn(|cx| Pin::new(&mut server).poll_read(cx, buf.unfilled())).await;
        assert!(read.is_ok(), "{read:?}");
        assert_eq!(buf.filled(), b"ping");
    }

    /// A body that is never ready and never wakes anyone.
    struct Pending;

    impl Body for Pending {
        type Data = Bytes;
        type Error = std::convert::Infallible;
        fn poll_frame(
            self: Pin<&mut Self>,
            _: &mut Context<'_>,
        ) -> Poll<Option<Result<Frame<Bytes>, Self::Error>>> {
            Poll::Pending
        }
    }

    /// A body that yields one byte every `gap`, `left` times.
    struct Trickle {
        gap: Duration,
        left: usize,
        next: Pin<Box<Sleep>>,
    }

    impl Trickle {
        fn new(gap: Duration, count: usize) -> Self {
            Self {
                gap,
                left: count,
                next: Box::pin(tokio::time::sleep(gap)),
            }
        }
    }

    impl Body for Trickle {
        type Data = Bytes;
        type Error = std::convert::Infallible;
        fn poll_frame(
            mut self: Pin<&mut Self>,
            cx: &mut Context<'_>,
        ) -> Poll<Option<Result<Frame<Bytes>, Self::Error>>> {
            if self.left == 0 {
                return Poll::Ready(None);
            }
            if self.next.as_mut().poll(cx).is_pending() {
                return Poll::Pending;
            }
            self.left -= 1;
            let next = Instant::now() + self.gap;
            self.next.as_mut().reset(next);
            Poll::Ready(Some(Ok(Frame::data(Bytes::from_static(b"x")))))
        }
    }

    #[tokio::test]
    async fn a_body_that_stops_fails_at_the_limit() {
        use http_body_util::BodyExt as _;
        let started = StdInstant::now();
        let error = StallBody::new(Pending, LIMIT)
            .collect()
            .await
            .expect_err("a body that never arrives must fail");
        assert!(is_timeout(error.as_ref()), "{error}");
        assert_at_the_limit(started.elapsed(), "the stalled body");
    }

    #[tokio::test]
    async fn a_body_that_keeps_arriving_is_read_whole() {
        use http_body_util::BodyExt as _;
        // A byte every half limit, six times: three limits in all, never one
        // limit without a byte.
        let started = StdInstant::now();
        let body = StallBody::new(Trickle::new(LIMIT / 2, 6), LIMIT)
            .collect()
            .await
            .expect("a body that keeps arriving is read whole")
            .to_bytes();
        assert_eq!(&body[..], b"xxxxxx");
        assert!(started.elapsed() >= LIMIT * 2);
    }

    #[test]
    fn a_timeout_is_found_through_the_error_chain() {
        #[derive(Debug)]
        struct Wrapper(io::Error);
        impl std::fmt::Display for Wrapper {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str("wrapped")
            }
        }
        impl std::error::Error for Wrapper {
            fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
                Some(&self.0)
            }
        }
        assert!(is_timeout(&Wrapper(stalled("x", LIMIT))));
        assert!(!is_timeout(&Wrapper(io::Error::from(
            io::ErrorKind::ConnectionReset
        ))));
    }
}
