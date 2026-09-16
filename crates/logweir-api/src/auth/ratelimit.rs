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
//! THE KEY IS THE IMMEDIATE PEER, NOT A HEADER. `X-Forwarded-For` is attacker-
//! controlled unless the immediate peer is a trusted proxy, and D0 allows
//! forwarded values for transport LOGGING only — never for a decision. So the
//! bucket key is the socket peer address, which is the reverse proxy's address
//! in a shared deployment. That is a deliberate trade: behind one ingress the
//! limit is a global limit, which is the correct conservative behaviour for a
//! service whose per-user limits live behind authentication.
//!
//! STREAM SLOTS ARE A SEAM. No route streams yet ([`crate::authz::Action`]
//! `StreamOperationEvents` has no route and is advertised `false`), but the
//! limit the streams will need is implemented and tested here so that adding
//! the route is adding a route, not inventing a limiter.

use std::collections::HashMap;
use std::net::IpAddr;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// The login window.
pub const LOGIN_WINDOW: Duration = Duration::from_secs(60);
/// The most login or callback requests one peer may make per window.
pub const LOGIN_PER_WINDOW: u32 = 20;
/// The most peers tracked at once. Past this the oldest windows are dropped,
/// so a spray of source addresses cannot grow this map without bound.
pub const MAX_TRACKED_PEERS: usize = 4096;

struct Window {
    started: Instant,
    count: u32,
}

/// A fixed-window counter keyed by peer address.
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

    /// Count one request from `peer` and decide.
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
