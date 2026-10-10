//! The limits on every connection the listener serves: a progress window on
//! each connection's output and on each request body (FX-24b), a rate floor on
//! the body's window, and a cap on the connections one peer may hold (FX-24c).
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
//! THE BOUND IS A WINDOW, NOT A TOTAL. Each guard here opens a window when an
//! operation goes PENDING, and fails the operation if the window's limit
//! passes before a FLOOR of bytes has moved in it; progress past the floor, or
//! the operation settling, closes the window. A client that reads slowly but
//! steadily keeps its connection however long the answer takes; what ends is
//! the connection whose client stopped, or, for a body, trickles.
//!
//! THE OUTPUT'S FLOOR IS ONE BYTE, AND THAT IS MEASURED, NOT ASSUMED (FX-24c).
//! The server sees a reader's progress only when the kernel lets it write
//! again, and the kernel does that in bursts the size of a send buffer: on
//! macOS a client draining a steady 16 KiB/s woke the writer with 167 KB, then
//! 12 KB five seconds later, then not at all for 25 s, and a client reading one
//! byte every twenty seconds got exactly the same first thirty seconds
//! (instrumented binary, 2026-10-09). A window that wanted 32 KiB cut that
//! steady reader at 32.5 s, which the one-byte stall served to the end; and the
//! stall already ends the one-byte reader, at 35.1 s on the same host, because
//! the kernel stops waking a writer whose client takes almost nothing. So the
//! output keeps the stall; slow-RATE readers that keep the kernel moving are
//! the peer cap's to bound, below.
//!
//! THE BODY'S FLOOR IS REAL (FX-24c). hyper reads a body as it arrives, with no
//! buffer between the client's rate and the frames the handler sees, so a
//! window on a body measures the client. A body still arriving at the end of a
//! window must have brought the floor in it; a body that ends inside its
//! window is never cut, however small.
//!
//! WHAT CLOSES A WINDOW, ON THE OUTPUT. Any byte the kernel accepts, or the
//! output CATCHING UP: hyper flushes the IO itself only once its own write
//! buffer is empty (hyper 1.11 `src/proto/h1/io.rs:271-304`,
//! `Buffered::poll_flush`), so a ready `poll_flush` here means everything hyper
//! had to write has been written. A server-sent event stream that is being read
//! catches up on every heartbeat, and between heartbeats it has no write
//! pending at all.
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
//!
//! THE PEER CAP ([`PeerLimit`]) bounds what one address can hold whatever its
//! rate: a client that reads fast enough to keep the kernel waking the writer
//! keeps each connection however long its pipelined answers take, so no window
//! can bound it and only a count can. Peers inside the trusted-proxy set are
//! never capped, because the ingress multiplexes every browser behind it onto
//! its own address; and with no trusted-proxy set configured nothing is capped
//! at all, because the console cannot then tell its ingress from any other
//! peer.

use std::collections::HashMap;
use std::future::Future as _;
use std::io;
use std::net::IpAddr;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::Duration;

use bytes::Bytes;
use hyper::body::{Body, Frame, SizeHint};
use tokio::time::{Instant, Sleep};

use crate::trusted_proxy::TrustedProxies;

/// The error type a [`StallBody`] yields: its inner body's error, boxed, or
/// the stall itself.
pub type BoxError = Box<dyn std::error::Error + Send + Sync>;

/// One progress window: opened by the first `Pending`, closed by `floor`
/// bytes of progress or by the operation settling, failed at `limit`.
///
/// With a floor of one byte this is FX-24b's stall clock: any progress closes
/// the window. The timer is allocated on the first window, not per connection:
/// most connections never wait on the kernel at all.
#[derive(Debug)]
struct StallClock {
    limit: Duration,
    /// The least progress, in bytes, that closes an open window (FX-24c).
    floor: usize,
    sleep: Option<Pin<Box<Sleep>>>,
    /// Bytes moved since the window opened; `None` while no window is open.
    window: Option<usize>,
}

impl StallClock {
    const fn new(limit: Duration, floor: usize) -> Self {
        Self {
            limit,
            floor,
            sleep: None,
            window: None,
        }
    }

    /// The operation settled: the output caught up, the body ended, or the
    /// operation failed on its own. Close the window.
    fn settled(&mut self) {
        self.window = None;
    }

    /// `bytes` moved. An open window closes once it has seen the floor; a
    /// byte moved while no window is open is not waiting on anyone.
    fn moved(&mut self, bytes: usize) {
        if let Some(moved) = self.window.as_mut() {
            *moved = moved.saturating_add(bytes);
            if *moved >= self.floor {
                self.window = None;
            }
        }
    }

    /// The operation is pending. Open a window if none is open, and say
    /// whether its limit has passed. Polling the timer registers the task's
    /// waker, so a connection that stays pending is woken at the limit.
    fn expired(&mut self, cx: &mut Context<'_>) -> bool {
        let deadline = Instant::now() + self.limit;
        let sleep = self
            .sleep
            .get_or_insert_with(|| Box::pin(tokio::time::sleep_until(deadline)));
        if self.window.is_none() {
            sleep.as_mut().reset(deadline);
            self.window = Some(0);
        }
        sleep.as_mut().poll(cx).is_ready()
    }

    /// The `TimedOut` error a window that expired fails with.
    fn stalled(&self, what: &str) -> io::Error {
        let message = if self.floor <= 1 {
            format!("{what} made no progress for {} s", self.limit.as_secs_f64())
        } else {
            format!(
                "{what} moved {} of the {} bytes it must move in {} s",
                self.window.unwrap_or(0),
                self.floor,
                self.limit.as_secs_f64()
            )
        };
        io::Error::new(io::ErrorKind::TimedOut, message)
    }

    /// A write: its bytes count toward the window.
    fn observe_write(
        &mut self,
        cx: &mut Context<'_>,
        result: Poll<io::Result<usize>>,
        what: &str,
    ) -> Poll<io::Result<usize>> {
        match result {
            Poll::Ready(Ok(written)) => {
                self.moved(written);
                Poll::Ready(Ok(written))
            }
            Poll::Ready(Err(error)) => {
                self.settled();
                Poll::Ready(Err(error))
            }
            Poll::Pending if self.expired(cx) => Poll::Ready(Err(self.stalled(what))),
            Poll::Pending => Poll::Pending,
        }
    }

    /// A flush or a shutdown: done, it settles the window; pending, it is
    /// timed like a write that moves nothing.
    fn observe_settling(
        &mut self,
        cx: &mut Context<'_>,
        result: Poll<io::Result<()>>,
        what: &str,
    ) -> Poll<io::Result<()>> {
        match result {
            Poll::Ready(result) => {
                self.settled();
                Poll::Ready(result)
            }
            Poll::Pending if self.expired(cx) => Poll::Ready(Err(self.stalled(what))),
            Poll::Pending => Poll::Pending,
        }
    }
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
    /// at all fails with [`io::ErrorKind::TimedOut`]. The output's floor is
    /// one byte; the module documentation says why it is not higher.
    pub const fn new(inner: T, limit: Duration) -> Self {
        Self {
            inner,
            output: StallClock::new(limit, 1),
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
        this.output.observe_write(cx, result, OUTPUT)
    }

    /// hyper flushes the IO only once its own write buffer is empty, so a
    /// ready flush is the output caught up, and it closes the window.
    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        let result = Pin::new(&mut this.inner).poll_flush(cx);
        this.output.observe_settling(cx, result, OUTPUT)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        let result = Pin::new(&mut this.inner).poll_shutdown(cx);
        this.output.observe_settling(cx, result, OUTPUT)
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
        this.output.observe_write(cx, result, OUTPUT)
    }
}

/// A request body with a progress window on the reads a handler waits on.
///
/// The window opens only while the body is POLLED and pending: a handler that
/// has not started reading (it is still authorizing, say) is not waiting on
/// the client, and neither is a body nobody reads. A body that ends inside
/// its window is never cut, however few bytes it had.
#[derive(Debug)]
pub struct StallBody<B> {
    inner: B,
    clock: StallClock,
}

impl<B> StallBody<B> {
    /// Wrap `inner`: a body read that stays pending for `limit` without
    /// `floor` bytes arriving yields a [`io::ErrorKind::TimedOut`] error. A
    /// `floor` of one byte is a pure stall.
    pub const fn new(inner: B, limit: Duration, floor: usize) -> Self {
        Self {
            inner,
            clock: StallClock::new(limit, floor),
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
            Poll::Ready(Some(Ok(frame))) => {
                this.clock.moved(frame.data_ref().map_or(0, Bytes::len));
                Poll::Ready(Some(Ok(frame)))
            }
            Poll::Ready(other) => {
                this.clock.settled();
                Poll::Ready(other.map(|frame| frame.map_err(Into::into)))
            }
            Poll::Pending if this.clock.expired(cx) => {
                Poll::Ready(Some(Err(Box::new(this.clock.stalled("the request body")))))
            }
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

/// The cap on the connections one peer outside the trusted-proxy set may hold
/// at once (FX-24c).
///
/// WHO IS CAPPED. A peer is one socket address's IP (an IPv4-mapped IPv6
/// address is the IPv4 address it maps to). A peer the trusted-proxy set
/// contains when its connection is accepted is NEVER capped: that is the
/// ingress, and every browser behind it arrives from its address. So the cap
/// exists only where a trusted-proxy set does ([`PeerLimit::outside`]); without
/// one the console cannot tell its ingress from anyone else, and capping the
/// ingress as one peer would turn one busy ingress pod into an outage.
///
/// WHAT IT BOUNDS. One address outside the set can hold at most `per_peer` of
/// the listener's permits, whatever its rate, so holding all of them takes
/// `MAX_CONNECTIONS / per_peer` addresses. A connection over the cap is closed
/// the moment it is accepted, before anything is read from it, so it costs its
/// permit for no longer than an accept.
#[derive(Debug)]
pub struct PeerLimit {
    per_peer: usize,
    trusted: Arc<TrustedProxies>,
    held: Mutex<HashMap<IpAddr, usize>>,
    warned: Mutex<Option<std::time::Instant>>,
    refused: std::sync::atomic::AtomicU64,
}

/// What [`PeerLimit::admit`] decided for one accepted connection.
#[derive(Debug)]
pub enum Admission {
    /// The peer is a trusted proxy: not counted.
    Trusted,
    /// Counted against the peer's share; the slot returns it when dropped.
    Counted(PeerSlot),
    /// The peer already holds its share: close the connection.
    Refused,
}

/// One connection's place in its peer's share. Dropping it gives the place
/// back.
#[derive(Debug)]
pub struct PeerSlot {
    limit: Arc<PeerLimit>,
    peer: IpAddr,
}

/// How often at most a refusal is logged. A peer over its cap is usually one
/// that keeps reconnecting, and a warning per connection would be a log flood
/// it could cause at will.
const REFUSAL_WARNING_EVERY: Duration = Duration::from_secs(10);

impl PeerLimit {
    /// The cap for peers outside `trusted`, or `None` when `trusted` names no
    /// proxy at all: then nothing is capped (see the type's documentation).
    #[must_use]
    pub fn outside(trusted: &Arc<TrustedProxies>, per_peer: usize) -> Option<Arc<Self>> {
        trusted.is_configured().then(|| {
            Arc::new(Self {
                per_peer,
                trusted: Arc::clone(trusted),
                held: Mutex::new(HashMap::new()),
                warned: Mutex::new(None),
                refused: std::sync::atomic::AtomicU64::new(0),
            })
        })
    }

    /// Decide one accepted connection from `peer`.
    #[must_use]
    pub fn admit(self: &Arc<Self>, peer: IpAddr) -> Admission {
        let peer = crate::trusted_proxy::canonical(peer);
        if self.trusted.contains(peer) {
            return Admission::Trusted;
        }
        let mut held = self
            .held
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let count = held.entry(peer).or_insert(0);
        if *count >= self.per_peer {
            drop(held);
            self.note_refusal(peer);
            return Admission::Refused;
        }
        *count += 1;
        Admission::Counted(PeerSlot {
            limit: Arc::clone(self),
            peer,
        })
    }

    /// How many connections `peer` holds right now.
    #[must_use]
    pub fn held_by(&self, peer: IpAddr) -> usize {
        self.held
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(&crate::trusted_proxy::canonical(peer))
            .copied()
            .unwrap_or(0)
    }

    fn note_refusal(&self, peer: IpAddr) {
        use std::sync::atomic::Ordering;
        let refused = self.refused.fetch_add(1, Ordering::Relaxed) + 1;
        let mut warned = self
            .warned
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if warned.is_some_and(|at| at.elapsed() < REFUSAL_WARNING_EVERY) {
            return;
        }
        *warned = Some(std::time::Instant::now());
        self.refused.store(0, Ordering::Relaxed);
        tracing::warn!(
            %peer,
            per_peer = self.per_peer,
            refused_since_last_warning = refused,
            "closed a connection at once: this peer already holds every connection one address \
             outside the trusted-proxy set may hold. A proxy that carries many clients belongs \
             in trustedProxyCidrs or trustedProxyService"
        );
    }
}

impl Drop for PeerSlot {
    fn drop(&mut self) {
        let mut held = self
            .limit
            .held
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(count) = held.get_mut(&self.peer) {
            *count = count.saturating_sub(1);
            if *count == 0 {
                held.remove(&self.peer);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    //! The guards on real loopback sockets and on stub IO, at a 300 ms limit
    //! (and a 4 KiB floor for bodies), and the peer cap over plain addresses.
    //!
    //! Each row can fail: the non-reader and the stuck-IO rows fail without
    //! the output clock; the steady-reader and idle rows fail if the clock is a
    //! total rather than a window; the pending-read row fails if reads are
    //! timed at the transport; the trickling-reader row fails if the output is
    //! given a floor above one byte (FX-24c measured why it must not be); the
    //! clock row fails when progress below the floor closes a window, or when a
    //! settled operation leaves its window open; the body floor rows fail when
    //! any byte closes a body's window, or when the floor is far too high; the
    //! body rows fail without the body clock, or with one that never closes;
    //! the cap rows fail with no cap, with the trusted proxy capped, with a cap
    //! where no trusted proxy is configured, or with places that never come
    //! back.

    use super::*;
    use crate::config::{Cidr, ServiceRef};
    use hyper::rt::{Read as _, Write as _};
    use hyper_util::rt::TokioIo;
    use std::io::Read as _;
    use std::task::Waker;
    use std::time::Instant as StdInstant;

    const LIMIT: Duration = Duration::from_millis(300);
    /// The body floor these rows use: 4 KiB per 300 ms window, about 13 KiB/s.
    const FLOOR: usize = 4 * 1024;
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

    async fn write<T: hyper::rt::Write + Unpin>(io: &mut T, buf: &[u8]) -> io::Result<usize> {
        std::future::poll_fn(|cx| Pin::new(&mut *io).poll_write(cx, buf)).await
    }

    /// Write `total` bytes in 16 KiB pieces, or fail with the first error.
    async fn write_all<T: hyper::rt::Write + Unpin>(io: &mut T, total: usize) -> io::Result<()> {
        let chunk = [0x5au8; 16 * 1024];
        let mut sent = 0;
        while sent < total {
            sent += write(io, &chunk[..chunk.len().min(total - sent)]).await?;
        }
        Ok(())
    }

    /// Every wait in these rows has its own deadline: a mutant that drops a
    /// guard must fail the row, not hang the suite.
    async fn bounded<F: std::future::Future>(within: Duration, what: &str, future: F) -> F::Output {
        tokio::time::timeout(within, future)
            .await
            .unwrap_or_else(|_| panic!("{what} was still pending after {within:?}"))
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
        let error = bounded(
            LIMIT + SLACK,
            "a write to a client that never reads",
            write_all(&mut server, 64 * 1024 * 1024),
        )
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
        let flushed = bounded(
            LIMIT + SLACK,
            "a stuck flush",
            std::future::poll_fn(|cx| Pin::new(&mut io).poll_flush(cx)),
        )
        .await;
        assert_eq!(flushed.unwrap_err().kind(), io::ErrorKind::TimedOut);
        assert_at_the_limit(started.elapsed(), "a stuck flush");

        let mut io = StallGuard::new(Stuck, LIMIT);
        let started = StdInstant::now();
        let shut = bounded(
            LIMIT + SLACK,
            "a stuck shutdown",
            std::future::poll_fn(|cx| Pin::new(&mut io).poll_shutdown(cx)),
        )
        .await;
        assert_eq!(shut.unwrap_err().kind(), io::ErrorKind::TimedOut);
        assert_at_the_limit(started.elapsed(), "a stuck shutdown");

        let mut io = StallGuard::new(Stuck, LIMIT);
        let started = StdInstant::now();
        let slices = [io::IoSlice::new(b"a"), io::IoSlice::new(b"b")];
        let written = bounded(
            LIMIT + SLACK,
            "a stuck vectored write",
            std::future::poll_fn(|cx| Pin::new(&mut io).poll_write_vectored(cx, &slices)),
        )
        .await;
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
        let result = bounded(
            Duration::from_secs(60),
            "a write to a steady reader",
            write_all(&mut server, TOTAL),
        )
        .await;
        let elapsed = started.elapsed();
        assert!(
            result.is_ok(),
            "a steady reader was cut after {elapsed:?}: {result:?}"
        );
        assert!(
            elapsed > STEADY_LIMIT * 2,
            "the writer finished in {elapsed:?}, so a total deadline at the limit would not \
             have cut it either: the row proves nothing about a window"
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
        let read = bounded(
            Duration::from_secs(5),
            "a read after the client wrote",
            std::future::poll_fn(|cx| Pin::new(&mut server).poll_read(cx, buf.unfilled())),
        )
        .await;
        assert!(read.is_ok(), "{read:?}");
        assert_eq!(buf.filled(), b"ping");
    }

    /// Output a client drains at its own pace: `chunk` bytes accepted every
    /// `gap`, pending in between, woken by its own timer as a socket is by the
    /// kernel. Flushes are always done: nothing is buffered here.
    struct Drip {
        chunk: usize,
        gap: Duration,
        next: Pin<Box<Sleep>>,
    }

    impl Drip {
        fn new(chunk: usize, gap: Duration) -> Self {
            Self {
                chunk,
                gap,
                next: Box::pin(tokio::time::sleep(gap)),
            }
        }
    }

    impl hyper::rt::Write for Drip {
        fn poll_write(
            mut self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            buf: &[u8],
        ) -> Poll<io::Result<usize>> {
            if self.next.as_mut().poll(cx).is_pending() {
                return Poll::Pending;
            }
            let next = Instant::now() + self.gap;
            self.next.as_mut().reset(next);
            Poll::Ready(Ok(buf.len().min(self.chunk)))
        }
        fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }
        fn poll_shutdown(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }
    }

    /// **The output is judged by the stall alone: a reader that keeps the
    /// kernel taking bytes, however few, is not cut (FX-24c's decision).**
    ///
    /// 64 bytes every 50 ms is 384 bytes a window, a tenth of the body floor.
    /// On the output that is not the client's rate but the kernel's: it wakes
    /// a writer in bursts the size of its send buffer, so a steady 16 KiB/s
    /// reader showed the server 12 KB in thirty seconds on macOS, and a window
    /// floor cut it where the stall served it to the end (the module
    /// documentation). A floor added to the output fails here first.
    #[tokio::test]
    async fn the_output_is_judged_by_the_stall_alone() {
        const TOTAL: usize = 6 * 1024;
        let mut io = StallGuard::new(Drip::new(64, LIMIT / 6), LIMIT);
        let started = StdInstant::now();
        let result = bounded(
            Duration::from_secs(60),
            "a write to a client that takes 64 bytes every 50 ms",
            write_all(&mut io, TOTAL),
        )
        .await;
        let elapsed = started.elapsed();
        assert!(
            result.is_ok(),
            "a reader the kernel kept taking bytes from was cut after {elapsed:?}: {result:?}"
        );
        assert!(
            elapsed > LIMIT * 5,
            "the transfer took only {elapsed:?}: too few windows to prove anything"
        );
    }

    /// **Progress below the floor keeps a window open; the floor's worth, or
    /// the operation settling, closes it (FX-24c).** The clock itself, polled
    /// by hand with a waker nobody listens to.
    ///
    /// - A window opens; 100 bytes move, far short of a 4 KiB floor; one limit
    ///   after the window opened it has expired. FX-24b's clock, which any byte
    ///   closed, would have opened a fresh window there instead.
    /// - The floor's worth closes the window: a limit later, a new pending
    ///   operation opens a new one, not an expired one.
    /// - So does settling — the output caught up, the body ended: two limits
    ///   later the next pending operation is judged by a new window. A window
    ///   that outlived the operation it was opened for would cut the next
    ///   answer at once, or an event stream's next frame.
    #[tokio::test]
    async fn progress_below_the_floor_keeps_the_window_and_settling_closes_it() {
        let mut cx = Context::from_waker(Waker::noop());

        let mut clock = StallClock::new(LIMIT, FLOOR);
        assert!(!clock.expired(&mut cx), "opens a window");
        tokio::time::sleep(LIMIT / 2).await;
        clock.moved(100);
        assert!(!clock.expired(&mut cx), "still inside the window");
        tokio::time::sleep(LIMIT / 2 + LIMIT / 10).await;
        assert!(
            clock.expired(&mut cx),
            "100 bytes in a window whose floor is {FLOOR} kept it open past its limit"
        );
        assert!(
            clock.stalled("x").to_string().contains("moved 100 of the"),
            "{}",
            clock.stalled("x")
        );

        let mut clock = StallClock::new(LIMIT, FLOOR);
        assert!(!clock.expired(&mut cx), "opens a window");
        tokio::time::sleep(LIMIT / 2).await;
        clock.moved(FLOOR);
        tokio::time::sleep(LIMIT / 2 + LIMIT / 10).await;
        assert!(
            !clock.expired(&mut cx),
            "a window that saw its floor was still judged by its old limit"
        );

        let mut clock = StallClock::new(LIMIT, FLOOR);
        assert!(!clock.expired(&mut cx), "opens a window");
        tokio::time::sleep(LIMIT / 2).await;
        clock.moved(100);
        clock.settled();
        tokio::time::sleep(LIMIT * 2).await;
        assert!(
            !clock.expired(&mut cx),
            "an operation two limits after the last one settled was judged by the old window"
        );
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

    /// A body that yields `chunk` bytes every `gap`, `left` times.
    struct Trickle {
        chunk: usize,
        gap: Duration,
        left: usize,
        next: Pin<Box<Sleep>>,
    }

    impl Trickle {
        fn new(chunk: usize, gap: Duration, count: usize) -> Self {
            Self {
                chunk,
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
            Poll::Ready(Some(Ok(Frame::data(Bytes::from(vec![b' '; self.chunk])))))
        }
    }

    #[tokio::test]
    async fn a_body_that_stops_fails_at_the_limit() {
        use http_body_util::BodyExt as _;
        let started = StdInstant::now();
        let error = bounded(
            LIMIT + SLACK,
            "a body that never arrives",
            StallBody::new(Pending, LIMIT, FLOOR).collect(),
        )
        .await
        .expect_err("a body that never arrives must fail");
        assert!(is_timeout(error.as_ref()), "{error}");
        assert_at_the_limit(started.elapsed(), "the stalled body");
    }

    #[tokio::test]
    async fn a_body_that_keeps_arriving_is_read_whole() {
        use http_body_util::BodyExt as _;
        // A floor's worth every half limit, six times: three limits in all,
        // never a window without its floor.
        let started = StdInstant::now();
        let body = bounded(
            LIMIT * 10,
            "a body that keeps arriving",
            StallBody::new(Trickle::new(FLOOR, LIMIT / 2, 6), LIMIT, FLOOR).collect(),
        )
        .await
        .expect("a body that keeps arriving is read whole")
        .to_bytes();
        assert_eq!(body.len(), 6 * FLOOR);
        assert!(started.elapsed() >= LIMIT * 2);
    }

    /// **A body that keeps arriving below the floor fails at the limit
    /// (FX-24c).** A byte every quarter limit never stalls, so FX-24b's
    /// clock read it whole, however long it took.
    #[tokio::test]
    async fn a_body_below_the_floor_fails_at_the_limit() {
        use http_body_util::BodyExt as _;
        let started = StdInstant::now();
        let error = bounded(
            LIMIT + SLACK,
            "a body below the floor",
            StallBody::new(Trickle::new(1, LIMIT / 4, 1000), LIMIT, FLOOR).collect(),
        )
        .await
        .expect_err("a body below the floor must fail");
        assert!(is_timeout(error.as_ref()), "{error}");
        assert_at_the_limit(started.elapsed(), "the body below the floor");
    }

    /// **A small body that ends inside its window is read whole, however
    /// slowly (FX-24c).** Ten bytes a byte at a time are far below the floor,
    /// but the body is over before its window is: the floor judges a body
    /// still arriving, never one that has arrived. Its own one-second window,
    /// five times the trickle, so a late timer on a loaded host cannot push
    /// the body past it.
    #[tokio::test]
    async fn a_small_body_that_ends_inside_its_window_is_read_whole() {
        use http_body_util::BodyExt as _;
        const WINDOW: Duration = Duration::from_secs(1);
        let body = bounded(
            WINDOW * 10,
            "a small, slow body",
            StallBody::new(
                Trickle::new(1, Duration::from_millis(20), 10),
                WINDOW,
                FLOOR,
            )
            .collect(),
        )
        .await
        .expect("a small body that ends inside its window is read whole")
        .to_bytes();
        assert_eq!(body.len(), 10);
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
        let clock = StallClock::new(LIMIT, FLOOR);
        assert!(is_timeout(&Wrapper(clock.stalled("x"))));
        assert!(!is_timeout(&Wrapper(io::Error::from(
            io::ErrorKind::ConnectionReset
        ))));
    }

    // ------------------------------------------------------------ the cap

    fn ip(s: &str) -> IpAddr {
        s.parse().unwrap()
    }

    fn trusting(cidrs: &[&str]) -> Arc<TrustedProxies> {
        Arc::new(TrustedProxies::from_cidrs(
            cidrs.iter().map(|c| Cidr::parse(c).unwrap()).collect(),
        ))
    }

    fn counted(admission: Admission) -> PeerSlot {
        match admission {
            Admission::Counted(slot) => slot,
            other => panic!("expected a counted slot, got {other:?}"),
        }
    }

    /// **A peer outside the trusted set is capped, its neighbours are not, and
    /// its places come back (FX-24c).**
    #[test]
    fn a_peer_outside_the_trusted_set_is_capped_and_its_places_come_back() {
        let limit = PeerLimit::outside(&trusting(&["10.42.0.0/16"]), 3).expect("a set is named");
        let pod = ip("10.43.0.9");
        let mut slots: Vec<PeerSlot> = (0..3).map(|_| counted(limit.admit(pod))).collect();
        assert_eq!(limit.held_by(pod), 3);
        assert!(
            matches!(limit.admit(pod), Admission::Refused),
            "a fourth connection from a peer capped at three was admitted"
        );
        assert_eq!(limit.held_by(pod), 3, "a refusal takes no place");
        // Another address is its own peer.
        let neighbour = counted(limit.admit(ip("10.43.0.10")));
        // A connection that ends gives its place back.
        slots.pop();
        assert_eq!(limit.held_by(pod), 2);
        slots.push(counted(limit.admit(pod)));
        drop(slots);
        drop(neighbour);
        assert_eq!(limit.held_by(pod), 0);
        assert!(
            limit.held.lock().unwrap().is_empty(),
            "a peer with no connection left is forgotten"
        );
    }

    /// **The trusted proxy is never capped as one peer (FX-24c).** Every
    /// browser behind the ingress arrives from its address.
    #[test]
    fn the_trusted_proxy_is_never_capped_as_one_peer() {
        let limit = PeerLimit::outside(&trusting(&["10.42.0.0/16"]), 3).expect("a set is named");
        let ingress = ip("10.42.7.1");
        for i in 0..50 {
            assert!(
                matches!(limit.admit(ingress), Admission::Trusted),
                "the ingress was counted as one peer at connection {i}"
            );
        }
        assert_eq!(limit.held_by(ingress), 0);
    }

    /// **Without a trusted-proxy set nothing is capped (FX-24c).** The
    /// console cannot then tell its ingress from any other peer.
    #[test]
    fn without_a_trusted_proxy_set_nothing_is_capped() {
        assert!(PeerLimit::outside(&trusting(&[]), 3).is_none());
        let service = Arc::new(TrustedProxies::new(
            Vec::new(),
            Some(ServiceRef {
                namespace: "traefik".into(),
                name: "traefik".into(),
            }),
        ));
        assert!(
            PeerLimit::outside(&service, 3).is_some(),
            "a trustedProxyService alone is a set"
        );
    }

    /// **The Service source's endpoints are trusted while fresh, and capped
    /// once stale (FX-24c).** A stale set trusts nobody (`/readyz` is false
    /// then), so the cap fails closed with it.
    #[test]
    fn a_trusted_service_endpoint_is_not_capped_while_its_set_is_fresh() {
        let trusted = Arc::new(TrustedProxies::new(
            Vec::new(),
            Some(ServiceRef {
                namespace: "traefik".into(),
                name: "traefik".into(),
            }),
        ));
        let limit = PeerLimit::outside(&trusted, 2).expect("a set is named");
        let ingress = ip("10.1.0.7");
        trusted.replace_at([ingress], StdInstant::now());
        for _ in 0..10 {
            assert!(matches!(limit.admit(ingress), Admission::Trusted));
        }
        let stale = StdInstant::now()
            .checked_sub(crate::trusted_proxy::MAX_AGE + Duration::from_secs(1))
            .expect("the clock is past the window");
        trusted.replace_at([ingress], stale);
        let _a = counted(limit.admit(ingress));
        let _b = counted(limit.admit(ingress));
        assert!(matches!(limit.admit(ingress), Admission::Refused));
    }

    /// **An IPv4-mapped IPv6 peer is the IPv4 peer (FX-24c).** A dual-stack
    /// listener sees IPv4 clients in the mapped form; one client must not get
    /// two shares by connecting both ways.
    #[test]
    fn an_ipv4_mapped_peer_shares_its_ipv4_share() {
        let limit = PeerLimit::outside(&trusting(&["10.42.0.0/16"]), 2).expect("a set is named");
        let _a = counted(limit.admit(ip("192.0.2.9")));
        let _b = counted(limit.admit(ip("::ffff:192.0.2.9")));
        assert!(matches!(limit.admit(ip("192.0.2.9")), Admission::Refused));
        assert!(matches!(
            limit.admit(ip("::ffff:192.0.2.9")),
            Admission::Refused
        ));
        assert_eq!(limit.held_by(ip("::ffff:192.0.2.9")), 2);
        // A trusted mapped address is trusted.
        assert!(matches!(
            limit.admit(ip("::ffff:10.42.0.1")),
            Admission::Trusted
        ));
    }
}
