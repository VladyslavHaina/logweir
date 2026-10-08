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
//! AN IPv6 KEY IS ITS `/64`, AND A CEILING BACKS EVERY KEY (the FX-13 security
//! review). A per-client key is only as good as the client's inability to
//! change it, and an IPv6 client holds a whole `/64` at least: keyed per
//! address it could take a fresh budget for every request, so IPv6 keys are
//! folded to their `/64` ([`IPV6_KEY_PREFIX`] says why `/64` and not wider).
//! And the old global key, for all its harm, bounded the provider's load at
//! 20 requests a minute; per-client keys alone lose that bound for anyone who
//! holds many addresses. So [`LOGIN_CEILING_PER_WINDOW`] caps every key
//! together, high enough that ordinary use never meets it. A request is
//! charged to both only when both allow it: a client refused by its own budget
//! does not spend the ceiling, and one refused by the ceiling keeps its
//! budget. The audit note `loginRateLimit` says which limit refused (`key`,
//! `ceiling`, or `trackedKeys` for the table bound below).
//!
//! The table is still bounded by [`MAX_TRACKED_PEERS`], whatever chooses the
//! keys: a spray of forwarded addresses through a trusted proxy grows it no
//! further than a spray of socket addresses did, and only expired windows are
//! ever dropped, so a spray cannot reset a live count. With the login ceiling
//! the bound is not even reached: a key is added only by a request the ceiling
//! allows. The windows are per console process, as they always were: behind an
//! ingress that spreads requests over N replicas, one client may be served up
//! to N times its allowance, and the provider may see N times the ceiling.
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
/// proxy, or else a socket peer, an IPv6 one folded to its `/64` — may make
/// per window.
pub const LOGIN_PER_WINDOW: u32 = 20;
/// The most login and callback requests ALL keys together may make per
/// window, per console process: the backstop that keeps the provider's load
/// bounded when a client holds many addresses (FX-13 security review).
///
/// WHY 600. A sign-in is two requests (`/auth/login`, then `/auth/callback`),
/// so this is 300 sign-ins a minute per process. A session lasts at most
/// 15 minutes (`sessionMaxAgeSeconds`, 900 by default), so it is more than
/// 4,000 people signing in again every quarter hour on one replica, or all of
/// them at once after a key rotation drains in a minute — ordinary use of a
/// team console never meets it. It is also 30 keys' worth of the per-key
/// budget, so the one global budget the per-client key replaced comes back
/// only for someone holding 30 addresses (30 `/64`s for IPv6) and spending
/// them all, and even then the provider sees at most ten requests a second
/// from each console process — the bound the old peer key gave at a thirtieth
/// of the rate, without making one client everyone's limit.
pub const LOGIN_CEILING_PER_WINDOW: u32 = 600;
/// The prefix an IPv6 key is folded to.
///
/// WHY `/64`. It is the smallest subnet an IPv6 site or host is given: SLAAC
/// needs one (RFC 4291 §2.5.1, RFC 7421), a host on it may use any of its
/// 2^64 addresses, and privacy addresses (RFC 8981) rotate through them on
/// their own. Keyed per address, one client could take a fresh budget for
/// every request. `/64` is also as wide as is safe to go: some networks give
/// each customer or handset exactly one `/64`, so a `/56` or `/48` key would
/// put unrelated clients in one budget. A client that holds a `/56` therefore
/// still has 256 budgets — [`LOGIN_CEILING_PER_WINDOW`] is the bound on that.
/// The cost is the same as IPv4 NAT: hosts sharing one `/64` share one budget.
/// IPv4 keys, IPv4-mapped IPv6 included, stay single addresses.
pub const IPV6_KEY_PREFIX: u32 = 64;
/// The most keys tracked at once. When the table is full, the windows that
/// have expired are dropped; a live window is never dropped, so a spray of
/// source addresses cannot grow this map without bound, and cannot reset any
/// key's live count either. A new key that finds it still full is refused.
pub const MAX_TRACKED_PEERS: usize = 4096;

/// The bucket an address is counted in: an IPv4-mapped IPv6 address as its
/// IPv4 address, any other IPv6 address as its [`IPV6_KEY_PREFIX`] network,
/// an IPv4 address as itself.
#[must_use]
pub fn bucket_of(address: IpAddr) -> IpAddr {
    match address.to_canonical() {
        IpAddr::V6(v6) => {
            let mask = u128::MAX << (128 - IPV6_KEY_PREFIX);
            IpAddr::V6(std::net::Ipv6Addr::from(u128::from(v6) & mask))
        }
        v4 @ IpAddr::V4(_) => v4,
    }
}

#[derive(Clone, Copy)]
struct Window {
    started: Instant,
    count: u32,
}

impl Window {
    fn fresh(now: Instant) -> Self {
        Self {
            started: now,
            count: 0,
        }
    }

    fn live(&self, now: Instant, length: Duration) -> bool {
        now.duration_since(self.started) < length
    }

    fn retry_after(&self, now: Instant, length: Duration) -> u64 {
        length
            .saturating_sub(now.duration_since(self.started))
            .as_secs()
            .max(1)
    }
}

struct Table {
    keys: HashMap<IpAddr, Window>,
    all: Window,
}

/// A fixed-window counter keyed by address — the forwarded client behind a
/// trusted proxy, or the socket peer (see the module documentation) — with an
/// optional ceiling over all keys together.
pub struct RateLimiter {
    window: Duration,
    per_window: u32,
    ceiling: Option<u32>,
    state: Mutex<Table>,
}

/// Which limit refused a request.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Limit {
    /// The key's own budget.
    Key,
    /// The ceiling over all keys together.
    Ceiling,
    /// The table of keys is full of live windows and this key is new.
    TrackedKeys,
}

impl Limit {
    /// The audit note's value.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Key => "key",
            Self::Ceiling => "ceiling",
            Self::TrackedKeys => "trackedKeys",
        }
    }
}

/// What a limiter decided.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Decision {
    /// Under every limit; the request was counted.
    Allowed,
    /// Over a limit; retry after this many seconds. Nothing was counted.
    Limited {
        /// Seconds until the refusing window resets.
        retry_after_seconds: u64,
        /// Which limit refused.
        limit: Limit,
    },
}

impl RateLimiter {
    /// A limiter over a window and a per-key budget, with no ceiling over all
    /// keys.
    #[must_use]
    pub fn new(window: Duration, per_window: u32) -> Self {
        Self {
            window,
            per_window,
            ceiling: None,
            state: Mutex::new(Table {
                keys: HashMap::new(),
                all: Window::fresh(Instant::now()),
            }),
        }
    }

    /// The same limiter with a ceiling over all keys together per window.
    #[must_use]
    pub fn with_ceiling(mut self, total_per_window: u32) -> Self {
        self.ceiling = Some(total_per_window);
        self
    }

    /// The login-surface limiter: [`LOGIN_PER_WINDOW`] per key and
    /// [`LOGIN_CEILING_PER_WINDOW`] in all, per [`LOGIN_WINDOW`].
    #[must_use]
    pub fn for_login() -> Self {
        Self::new(LOGIN_WINDOW, LOGIN_PER_WINDOW).with_ceiling(LOGIN_CEILING_PER_WINDOW)
    }

    /// Count one request against `address`'s bucket ([`bucket_of`]) and
    /// decide.
    pub fn check(&self, address: IpAddr) -> Decision {
        self.check_at(address, Instant::now())
    }

    /// Count one request at an explicit instant, for tests.
    ///
    /// THE ORDER IS THE POINT. The key's own budget is checked first, and a
    /// request it refuses is not charged to the ceiling — so one client that
    /// keeps knocking after its budget cannot spend everyone's. A request the
    /// ceiling refuses is not charged to its key either, so the key's budget is
    /// intact when the ceiling's window turns. A request is counted, on both,
    /// only when it is allowed.
    pub fn check_at(&self, address: IpAddr, now: Instant) -> Decision {
        let bucket = bucket_of(address);
        let length = self.window;
        let mut table = self
            .state
            .lock()
            .expect("the limiter lock is never poisoned");
        if !table.all.live(now, length) {
            table.all = Window::fresh(now);
        }
        let current = table
            .keys
            .get(&bucket)
            .copied()
            .filter(|w| w.live(now, length));
        if let Some(window) = current.filter(|w| w.count >= self.per_window) {
            return Decision::Limited {
                retry_after_seconds: window.retry_after(now, length),
                limit: Limit::Key,
            };
        }
        if let Some(ceiling) = self.ceiling {
            if table.all.count >= ceiling {
                return Decision::Limited {
                    retry_after_seconds: table.all.retry_after(now, length),
                    limit: Limit::Ceiling,
                };
            }
        }
        if !table.keys.contains_key(&bucket) && table.keys.len() >= MAX_TRACKED_PEERS {
            // Only windows that have expired are dropped: they hold nothing a
            // fresh window would not.
            table.keys.retain(|_, w| w.live(now, length));
            if table.keys.len() >= MAX_TRACKED_PEERS {
                // Still full of live windows: refuse the new key rather than
                // grow, or than drop a live count.
                return Decision::Limited {
                    retry_after_seconds: length.as_secs().max(1),
                    limit: Limit::TrackedKeys,
                };
            }
        }
        let window = table
            .keys
            .entry(bucket)
            .or_insert_with(|| Window::fresh(now));
        if !window.live(now, length) {
            *window = Window::fresh(now);
        }
        window.count += 1;
        table.all.count += 1;
        Decision::Allowed
    }

    /// How many keys hold a window right now, live or expired. Never more
    /// than [`MAX_TRACKED_PEERS`].
    #[must_use]
    pub fn tracked(&self) -> usize {
        self.state
            .lock()
            .expect("the limiter lock is never poisoned")
            .keys
            .len()
    }
}

impl std::fmt::Debug for RateLimiter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "RateLimiter({} per {:?}, ceiling {:?})",
            self.per_window, self.window, self.ceiling
        )
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
            limit: Limit::Key,
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
        let tracked = limiter.tracked();
        assert!(
            tracked <= MAX_TRACKED_PEERS,
            "the limiter tracked {tracked} peers"
        );
    }

    fn v6(s: &str) -> IpAddr {
        s.parse().unwrap()
    }

    fn limited(decision: Decision) -> Option<Limit> {
        match decision {
            Decision::Allowed => None,
            Decision::Limited { limit, .. } => Some(limit),
        }
    }

    /// **An IPv6 key is its `/64`; an IPv4 key, mapped or not, is its
    /// address.** A client rotating through its own `/64` — what privacy
    /// addresses do unasked — spends one budget. NEGATIVE CONTROL: the next
    /// `/64` over, and the next IPv4 address over, are budgets of their own,
    /// so the fold is exactly `/64` and not wider.
    #[test]
    fn ipv6_keys_are_folded_to_their_64_and_ipv4_keys_are_not() {
        assert_eq!(bucket_of(v6("2001:db8:1:2::1")), v6("2001:db8:1:2::"));
        assert_eq!(
            bucket_of(v6("2001:db8:1:2:ffff:ffff:ffff:ffff")),
            v6("2001:db8:1:2::")
        );
        assert_ne!(bucket_of(v6("2001:db8:1:3::1")), v6("2001:db8:1:2::"));
        assert_eq!(bucket_of(v6("::ffff:203.0.113.5")), v6("203.0.113.5"));
        assert_ne!(bucket_of(v6("::ffff:203.0.113.6")), v6("203.0.113.5"));
        assert_eq!(bucket_of(v6("203.0.113.5")), v6("203.0.113.5"));

        let limiter = RateLimiter::new(Duration::from_secs(60), 3);
        let start = Instant::now();
        for host in 1..=3 {
            let rotated = v6(&format!("2001:db8:1:2::{host:x}"));
            assert_eq!(limiter.check_at(rotated, start), Decision::Allowed);
        }
        assert_eq!(
            limited(limiter.check_at(v6("2001:db8:1:2:dead:beef:0:4"), start)),
            Some(Limit::Key),
            "a fresh address in the same /64 is the same, spent, budget"
        );
        assert_eq!(
            limiter.check_at(v6("2001:db8:1:3::1"), start),
            Decision::Allowed
        );
        for last in 1..=3 {
            assert_eq!(
                limiter.check_at(v6(&format!("::ffff:198.51.100.{last}")), start),
                Decision::Allowed,
                "IPv4 neighbours are separate budgets, mapped or not"
            );
        }
    }

    /// **The ceiling bounds every key together.** Ten keys, one request each,
    /// spend a ceiling of ten; the eleventh key is refused by the CEILING, with
    /// a `Retry-After` inside the window, and the window turning lifts it.
    /// NEGATIVE CONTROL: the same eleven requests against the same limiter
    /// without a ceiling are all served.
    #[test]
    fn the_ceiling_bounds_all_keys_together() {
        let start = Instant::now();
        let key = |i: u8| IpAddr::from([192, 0, 2, i]);
        let capped = RateLimiter::new(Duration::from_secs(60), 3).with_ceiling(10);
        let uncapped = RateLimiter::new(Duration::from_secs(60), 3);
        for i in 0..10 {
            assert_eq!(capped.check_at(key(i), start), Decision::Allowed);
            assert_eq!(uncapped.check_at(key(i), start), Decision::Allowed);
        }
        let at = start + Duration::from_secs(20);
        let Decision::Limited {
            retry_after_seconds,
            limit: Limit::Ceiling,
        } = capped.check_at(key(10), at)
        else {
            panic!("the eleventh key must meet the ceiling");
        };
        assert!(
            (1..=40).contains(&retry_after_seconds),
            "{retry_after_seconds}"
        );
        assert_eq!(
            limited(capped.check_at(key(0), at)),
            Some(Limit::Ceiling),
            "a key with budget left meets it too"
        );
        assert_eq!(uncapped.check_at(key(10), at), Decision::Allowed);
        let later = start + Duration::from_secs(61);
        assert_eq!(capped.check_at(key(10), later), Decision::Allowed);
    }

    /// **A key over its own budget does not spend the ceiling.** One client
    /// that keeps knocking after its three is refused by its KEY, and the
    /// other keys still find the whole remaining ceiling. NEGATIVE CONTROL:
    /// the ceiling still holds — the request past it is refused.
    #[test]
    fn a_key_over_its_budget_does_not_spend_the_ceiling() {
        let start = Instant::now();
        let limiter = RateLimiter::new(Duration::from_secs(60), 3).with_ceiling(10);
        let noisy = IpAddr::from([192, 0, 2, 200]);
        let mut refused = 0;
        for _ in 0..50 {
            match limiter.check_at(noisy, start) {
                Decision::Allowed => {}
                Decision::Limited { limit, .. } => {
                    assert_eq!(limit, Limit::Key);
                    refused += 1;
                }
            }
        }
        assert_eq!(refused, 47);
        for i in 0..7 {
            assert_eq!(
                limiter.check_at(IpAddr::from([192, 0, 2, i]), start),
                Decision::Allowed,
                "key {i}: the ceiling has 7 left after the noisy key's 3"
            );
        }
        assert_eq!(
            limited(limiter.check_at(IpAddr::from([192, 0, 2, 7]), start)),
            Some(Limit::Ceiling)
        );
    }

    /// **A request the ceiling refuses is not charged to its key.** A key
    /// refused five times by the ceiling at t+30s still has its whole budget
    /// when the ceiling's window turns at t+60s. NEGATIVE CONTROL: the budget
    /// is real — the request after it is refused by the key.
    #[test]
    fn a_request_the_ceiling_refuses_is_not_charged_to_its_key() {
        let start = Instant::now();
        let limiter = RateLimiter::new(Duration::from_secs(60), 3).with_ceiling(3);
        for i in 0..3 {
            assert_eq!(
                limiter.check_at(IpAddr::from([192, 0, 2, i]), start),
                Decision::Allowed
            );
        }
        let late = IpAddr::from([192, 0, 2, 99]);
        for _ in 0..5 {
            assert_eq!(
                limited(limiter.check_at(late, start + Duration::from_secs(30))),
                Some(Limit::Ceiling)
            );
        }
        let turned = start + Duration::from_secs(61);
        for _ in 0..3 {
            assert_eq!(limiter.check_at(late, turned), Decision::Allowed);
        }
        assert_eq!(limited(limiter.check_at(late, turned)), Some(Limit::Key));
    }

    /// **A spray of new keys cannot reset a live window, nor lock out a
    /// tracked key, nor grow the table.** The table fills with live windows;
    /// a spent key stays spent (eviction drops only EXPIRED windows, which
    /// hold nothing a fresh window would not), a tracked key with budget left
    /// is still served, and only the new keys are refused, as
    /// `TrackedKeys`. NEGATIVE CONTROL: once the windows expire, the next new
    /// key evicts them and is served, and the table is still bounded.
    #[test]
    fn a_spray_cannot_reset_a_live_window_or_grow_the_table() {
        let start = Instant::now();
        let limiter = RateLimiter::new(Duration::from_secs(60), 2);
        let spent = IpAddr::from([192, 0, 2, 1]);
        let tracked = IpAddr::from([192, 0, 2, 2]);
        assert_eq!(limiter.check_at(spent, start), Decision::Allowed);
        assert_eq!(limiter.check_at(spent, start), Decision::Allowed);
        assert_eq!(limiter.check_at(tracked, start), Decision::Allowed);
        let spray = |i: usize| {
            let [_, b, c, d] = (i as u32).to_be_bytes();
            IpAddr::from([10, b, c, d])
        };
        let mut refused_new = 0;
        for i in 0..(MAX_TRACKED_PEERS + 100) {
            match limiter.check_at(spray(i), start + Duration::from_secs(1)) {
                Decision::Allowed => {}
                Decision::Limited { limit, .. } => {
                    assert_eq!(limit, Limit::TrackedKeys);
                    refused_new += 1;
                }
            }
        }
        assert_eq!(limiter.tracked(), MAX_TRACKED_PEERS);
        assert_eq!(refused_new, 102, "the two earlier keys hold two places");
        let during = start + Duration::from_secs(2);
        assert_eq!(limited(limiter.check_at(spent, during)), Some(Limit::Key));
        assert_eq!(limiter.check_at(tracked, during), Decision::Allowed);

        let after = start + Duration::from_secs(62);
        assert_eq!(
            limiter.check_at(IpAddr::from([192, 0, 2, 3]), after),
            Decision::Allowed
        );
        assert!(limiter.tracked() <= MAX_TRACKED_PEERS);
        assert_eq!(limiter.check_at(spent, after), Decision::Allowed);
    }
}
