//! Rate and connection limits for the unauthenticated login surface and for
//! streams.
//!
//! WHY THE LOGIN SURFACE NEEDS ITS OWN LIMIT. `/auth/login` and
//! `/auth/callback` are the only routes an unauthenticated caller can reach
//! that do work: a login mints randomness and may fetch discovery, and a
//! callback makes an outbound request to the identity provider. Without a limit
//! a single client can turn one cheap request into one provider request
//! forever, which is a denial of service aimed at the IdP through this service.
//!
//! THE KEY IS THE CLIENT AS THE TRUSTED PROXY SAW IT, AND OTHERWISE THE PEER
//! (FX-13). Behind the shared console's ingress every request has the same
//! socket peer — the ingress — so a limit keyed on the peer alone is one
//! global budget: about ten sign-ins a minute for everyone, and one
//! unauthenticated client can spend it and block every sign-in for a minute.
//! So `crate::http::login_rate_key` chooses the bucket:
//!
//! - when the immediate peer is a trusted proxy — the same
//!   `TrustedProxies::contains` decision the entry point's `requireTrustedProxy`
//!   gate makes, so a Service source that is not read yet or older than its
//!   `MAX_AGE` trusts nobody here either — the key is the rightmost
//!   `X-Forwarded-For` hop that is not itself a trusted proxy: the address the
//!   outermost trusted proxy received the request from, which a client cannot
//!   choose (everything left of it is client-sent and is never read);
//! - otherwise, and whenever that hop is absent or is not an IP address, or the
//!   chain names only trusted proxies, the key is the socket peer.
//!
//! `X-Forwarded-For` is the only header read, because it is the one the
//! supported ingress writes: Traefik (`deploy/poc/traefik.values.yaml`,
//! `forwardedHeaders.trustedIPs: []`) deletes a client's `X-Forwarded-*` and
//! appends the client's socket address, while it copies an RFC 7239
//! `Forwarded` header through untouched — so `Forwarded` is whatever the
//! client wrote, and is never read.
//!
//! A BUCKET IS NOT AN IDENTITY. D0 allows forwarded values for transport
//! facts only — never for an identity or a grant (amended 2026-10-07 for this
//! key). The forwarded address chooses which counter a request is charged to,
//! and a counter can only refuse: it authenticates no one, authorizes nothing
//! and is never an audit actor. A forged header from an untrusted peer is not
//! read at all, so it cannot move a request out of its peer's bucket.
//!
//! The table is still bounded by [`MAX_TRACKED_PEERS`], whatever chooses the
//! keys: a spray of forwarded addresses through a trusted proxy grows it no
//! further than a spray of socket addresses did. The windows are per console
//! process, as they always were: behind an ingress that spreads requests over
//! N replicas, one client may be served up to N times the allowance.
//!
//! STREAM SLOTS BOUND THE EVENT STREAM. `routes::operations::events` takes one
//! per open stream, keyed by principal and namespace, and answers `429` when
//! the principal already holds its share; the slot is released when the
//! stream ends.

use std::collections::HashMap;
use std::net::IpAddr;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// The login window.
pub const LOGIN_WINDOW: Duration = Duration::from_secs(60);
/// The most login or callback requests one key — a client behind a trusted
/// proxy, or else a socket peer — may make per window.
pub const LOGIN_PER_WINDOW: u32 = 20;
/// The most peers tracked at once. Past this the oldest windows are dropped,
/// so a spray of source addresses cannot grow this map without bound.
pub const MAX_TRACKED_PEERS: usize = 4096;

struct Window {
    started: Instant,
    count: u32,
}

/// A fixed-window counter keyed by address: the forwarded client behind a
/// trusted proxy, or the socket peer (see the module documentation).
pub struct RateLimiter {
    window: Duration,
    per_window: u32,
    state: Mutex<HashMap<IpAddr, Window>>,
}

/// What a limiter decided.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Decision {
    /// Under the limit.
    Allowed,
    /// Over the limit; retry after this many seconds.
    Limited {
        /// Seconds until the window resets.
        retry_after_seconds: u64,
    },
}

impl RateLimiter {
    /// A limiter over a window and a per-window ceiling.
    #[must_use]
    pub fn new(window: Duration, per_window: u32) -> Self {
        Self {
            window,
            per_window,
            state: Mutex::new(HashMap::new()),
        }
    }

    /// The login-surface limiter.
    #[must_use]
    pub fn for_login() -> Self {
        Self::new(LOGIN_WINDOW, LOGIN_PER_WINDOW)
    }

    /// Count one request against `peer`'s window and decide.
    pub fn check(&self, peer: IpAddr) -> Decision {
        self.check_at(peer, Instant::now())
    }

    /// Count one request at an explicit instant, for tests.
    pub fn check_at(&self, peer: IpAddr, now: Instant) -> Decision {
        let mut state = self
            .state
            .lock()
            .expect("the limiter lock is never poisoned");
        if state.len() >= MAX_TRACKED_PEERS {
            state.retain(|_, w| now.duration_since(w.started) < self.window);
            if state.len() >= MAX_TRACKED_PEERS {
                // Still full of live windows: refuse rather than grow.
                return Decision::Limited {
                    retry_after_seconds: self.window.as_secs().max(1),
                };
            }
        }
        let window = state.entry(peer).or_insert(Window {
            started: now,
            count: 0,
        });
        if now.duration_since(window.started) >= self.window {
            window.started = now;
            window.count = 0;
        }
        window.count += 1;
        if window.count > self.per_window {
            let elapsed = now.duration_since(window.started);
            let remaining = self.window.saturating_sub(elapsed);
            Decision::Limited {
                retry_after_seconds: remaining.as_secs().max(1),
            }
        } else {
            Decision::Allowed
        }
    }
}

impl RateLimiter {
    /// How many keys hold a window right now. Never more than
    /// [`MAX_TRACKED_PEERS`].
    #[must_use]
    pub fn tracked(&self) -> usize {
        self.state
            .lock()
            .expect("the limiter lock is never poisoned")
            .len()
    }
}

impl std::fmt::Debug for RateLimiter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "RateLimiter({} per {:?})", self.per_window, self.window)
    }
}

/// The most concurrent streams one actor may hold, per namespace.
pub const STREAMS_PER_ACTOR_NAMESPACE: usize = 4;

/// Concurrent-stream accounting, keyed by `(actor id, namespace)`.
#[derive(Default)]
pub struct StreamSlots {
    held: Mutex<HashMap<(String, String), usize>>,
    per_key: usize,
}

/// A held stream slot. Dropping it releases the slot, so a dropped connection
/// — the normal way a browser leaves — cannot leak one.
pub struct StreamSlot {
    slots: Arc<StreamSlots>,
    key: (String, String),
}

impl Drop for StreamSlot {
    fn drop(&mut self) {
        let mut held = self
            .slots
            .held
            .lock()
            .expect("the stream lock is never poisoned");
        if let Some(count) = held.get_mut(&self.key) {
            *count = count.saturating_sub(1);
            if *count == 0 {
                held.remove(&self.key);
            }
        }
    }
}

impl StreamSlots {
    /// Accounting with the default ceiling.
    #[must_use]
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            held: Mutex::new(HashMap::new()),
            per_key: STREAMS_PER_ACTOR_NAMESPACE,
        })
    }

    /// Accounting with an explicit ceiling, for tests.
    #[must_use]
    pub fn with_limit(per_key: usize) -> Arc<Self> {
        Arc::new(Self {
            held: Mutex::new(HashMap::new()),
            per_key,
        })
    }

    /// Take a slot for one actor in one namespace, or `None` at the ceiling.
    pub fn acquire(self: &Arc<Self>, actor_id: &str, namespace: &str) -> Option<StreamSlot> {
        let key = (actor_id.to_string(), namespace.to_string());
        let mut held = self.held.lock().expect("the stream lock is never poisoned");
        let count = held.entry(key.clone()).or_insert(0);
        if *count >= self.per_key {
            if *count == 0 {
                held.remove(&key);
            }
            return None;
        }
        *count += 1;
        Some(StreamSlot {
            slots: Arc::clone(self),
            key,
        })
    }

    /// How many slots one actor holds in one namespace.
    #[must_use]
    pub fn held(&self, actor_id: &str, namespace: &str) -> usize {
        self.held
            .lock()
            .expect("the stream lock is never poisoned")
            .get(&(actor_id.to_string(), namespace.to_string()))
            .copied()
            .unwrap_or(0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ip(last: u8) -> IpAddr {
        IpAddr::from([127, 0, 0, last])
    }

    #[test]
    fn a_peer_is_limited_after_its_window_allowance_and_recovers() {
        let limiter = RateLimiter::new(Duration::from_secs(60), 3);
        let start = Instant::now();
        for i in 0..3 {
            assert_eq!(limiter.check_at(ip(1), start), Decision::Allowed, "{i}");
        }
        let Decision::Limited {
            retry_after_seconds,
        } = limiter.check_at(ip(1), start)
        else {
            panic!("the fourth request in the window must be limited");
        };
        assert!((1..=60).contains(&retry_after_seconds));

        // A different peer has its own window.
        assert_eq!(limiter.check_at(ip(2), start), Decision::Allowed);

        // Past the window the allowance returns.
        let later = start + Duration::from_secs(61);
        assert_eq!(limiter.check_at(ip(1), later), Decision::Allowed);
    }

    #[test]
    fn stream_slots_are_per_actor_and_namespace_and_are_released_on_drop() {
        let slots = StreamSlots::with_limit(2);
        let a1 = slots.acquire("actor#1", "team-a").unwrap();
        let a2 = slots.acquire("actor#1", "team-a").unwrap();
        assert_eq!(slots.held("actor#1", "team-a"), 2);
        assert!(
            slots.acquire("actor#1", "team-a").is_none(),
            "the ceiling holds"
        );
        // The same actor in another namespace, and another actor here, are
        // separate.
        assert!(slots.acquire("actor#1", "team-b").is_some());
        assert!(slots.acquire("actor#2", "team-a").is_some());

        drop(a1);
        assert_eq!(slots.held("actor#1", "team-a"), 1);
        assert!(slots.acquire("actor#1", "team-a").is_some());
        drop(a2);
    }

    #[test]
    fn the_peer_table_does_not_grow_without_bound() {
        let limiter = RateLimiter::new(Duration::from_millis(1), 1);
        let start = Instant::now();
        for i in 0..(MAX_TRACKED_PEERS + 200) {
            let octets = (i as u32).to_be_bytes();
            limiter.check_at(
                IpAddr::from(octets),
                start + Duration::from_millis(i as u64),
            );
        }
        let tracked = limiter
            .state
            .lock()
            .expect("the limiter lock is never poisoned")
            .len();
        assert!(
            tracked <= MAX_TRACKED_PEERS,
            "the limiter tracked {tracked} peers"
        );
    }
}
