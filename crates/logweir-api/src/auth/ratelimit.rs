//! Rate and connection limits for the unauthenticated login surface and for
//! streams.
//!
//! WHAT THE LOGIN LIMIT IS FOR. `/auth/login` and `/auth/callback` are the
//! only routes an unauthenticated caller can reach that do work: a login mints
//! randomness and a login cookie, and a callback that carries a login cookie
//! whose `state` matches makes one request to the identity provider. The
//! limit keeps one client from spending this service's work, and the
//! provider's, as fast as it can send.
//!
//! IT IS PER CLIENT, AND THERE IS NO GLOBAL BUDGET (FX-13). A global budget —
//! which the old peer key became behind an ingress, where every request has
//! the ingress as its peer — lets one unauthenticated client lock everyone out
//! of the console; for a recovery tool, being able to sign in during an
//! incident outranks everything this limit protects. A ceiling over all
//! clients, however high, is the same lockout at a higher threshold (30
//! addresses, or one IPv6 customer's `/56`, at 600 a minute), so there is
//! none: N clients each within their budget are all served, whatever N is.
//!
//! WHY NO GLOBAL BUDGET IS NEEDED: THERE IS NO AMPLIFICATION. Each inbound
//! request makes at most one provider request once the caches are warm, which
//! readiness ensures before a replica takes traffic (`oidc.rs:357`, discovery
//! then JWKS):
//!
//! - `/auth/login` makes none while the discovery document is cached
//!   (`DISCOVERY_MAX_AGE`, one hour, `oidc.rs:40`; the cache read at
//!   `oidc.rs:412`), and one `GET` of it when the hour has lapsed
//!   (`oidc.rs:426`; called at `login.rs:117`);
//! - `/auth/callback` reaches the provider only after its login cookie
//!   decrypts and its `state` matches (`login.rs:214`, `login.rs:225`), then
//!   makes exactly one token `POST` (`login.rs:231` → `oidc.rs:577`,
//!   `oidc.rs:604`). The keys come from the JWKS cache (`JWKS_MAX_AGE`, a day,
//!   `oidc.rs:45`; `oidc.rs:642`); an unknown `kid` in the provider's OWN ID
//!   token forces at most one refetch per `JWKS_MIN_REFETCH` per process
//!   (60 s, `oidc.rs:43`, `oidc.rs:505`, `oidc.rs:644`).
//!
//! On a cold or expired cache a request adds at most the discovery `GET` and a
//! JWKS `GET` (three requests at most), and the result serves every later
//! request. That is one-to-one, not amplification, and a provider has to limit
//! its own traffic anyway; this service only promises not to multiply it.
//!
//! THE KEY IS THE CLIENT AS THE TRUSTED PROXY SAW IT, AND OTHERWISE THE PEER.
//! `crate::http::login_rate_key` chooses the bucket:
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
//! AN IPv6 KEY IS ITS `/64` ([`bucket_of`]). A per-client key is only as good
//! as the client's inability to change it, and an IPv6 client holds a whole
//! `/64` at least: keyed per address it could take a fresh budget for every
//! request. [`IPV6_KEY_PREFIX`] says why `/64` and not wider.
//!
//! THE RESIDUAL, STATED. A client holding many addresses — many IPv4
//! addresses, or many `/64`s (a `/56` is 256 of them) — holds that many
//! budgets. Each is bounded at [`LOGIN_PER_WINDOW`], none can spend another
//! client's, and each request costs the provider at most one request (above).
//!
//! A BUCKET IS NOT AN IDENTITY. D0 allows forwarded values for transport
//! facts only — never for an identity or a grant (amended 2026-10-07 for this
//! key). The forwarded address chooses which counter a request is charged to,
//! and a counter can only refuse: it authenticates no one, authorizes nothing
//! and is never an audit actor. A forged header from an untrusted peer is not
//! read at all, so it cannot move a request out of its peer's bucket.
//!
//! THE TABLE IS BOUNDED, AND A FULL ONE LOCKS NO ONE OUT. At most
//! [`MAX_TRACKED_PEERS`] keys hold a window, whatever chooses the keys. When a
//! new key finds the table full, the windows that have expired are dropped —
//! they hold nothing a fresh window would not, so a spray of addresses cannot
//! reset a live count — and if it is still full of live windows, the new key
//! is SERVED WITHOUT A WINDOW rather than refused: refusing it would be the
//! global lockout again, for whoever holds `MAX_TRACKED_PEERS` keys. Tracked
//! keys keep their windows meanwhile, and the audit notes `loginRateUntracked`
//! so an operator can see it happen. The windows are per console process, as
//! they always were: behind an ingress that spreads requests over N replicas,
//! one client may be served up to N times its allowance.
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
/// The prefix an IPv6 key is folded to.
///
/// WHY `/64`. It is the smallest subnet an IPv6 site or host is given: SLAAC
/// needs one (RFC 4291 §2.5.1, RFC 7421), a host on it may use any of its
/// 2^64 addresses, and privacy addresses (RFC 8981) rotate through them on
/// their own. Keyed per address, one client could take a fresh budget for
/// every request. `/64` is also as wide as is safe to go: some networks give
/// each customer or handset exactly one `/64`, so a `/56` or `/48` key would
/// put unrelated clients in one budget. A client that holds a `/56` therefore
/// still has 256 budgets, each bounded, none able to spend another client's
/// (the residual the module documentation states). The cost is the same as
/// IPv4 NAT: hosts sharing one `/64` share one budget.
/// IPv4 keys, IPv4-mapped IPv6 included, stay single addresses.
pub const IPV6_KEY_PREFIX: u32 = 64;
/// The most keys tracked at once. When the table is full, the windows that
/// have expired are dropped; a live window is never dropped, so a spray of
/// source addresses cannot grow this map without bound, and cannot reset any
/// key's live count either. A new key that finds it still full is served
/// without a window ([`Decision::AllowedUntracked`]), never refused.
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

/// A fixed-window counter per key — the forwarded client behind a trusted
/// proxy, or the socket peer (see the module documentation). There is no
/// budget over all keys together, on purpose.
pub struct RateLimiter {
    window: Duration,
    per_window: u32,
    state: Mutex<HashMap<IpAddr, Window>>,
}

/// What a limiter decided.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Decision {
    /// Under the key's budget; the request was counted.
    Allowed,
    /// The table of keys is full of live windows and this key is new: served,
    /// without a window, rather than locked out (see the module
    /// documentation).
    AllowedUntracked,
    /// Over the key's budget; retry after this many seconds. Nothing was
    /// counted.
    Limited {
        /// Seconds until the key's window resets.
        retry_after_seconds: u64,
    },
}

impl RateLimiter {
    /// A limiter over a window and a per-key budget.
    #[must_use]
    pub fn new(window: Duration, per_window: u32) -> Self {
        Self {
            window,
            per_window,
            state: Mutex::new(HashMap::new()),
        }
    }

    /// The login-surface limiter: [`LOGIN_PER_WINDOW`] per key per
    /// [`LOGIN_WINDOW`].
    #[must_use]
    pub fn for_login() -> Self {
        Self::new(LOGIN_WINDOW, LOGIN_PER_WINDOW)
    }

    /// Count one request against `address`'s bucket ([`bucket_of`]) and
    /// decide.
    pub fn check(&self, address: IpAddr) -> Decision {
        self.check_at(address, Instant::now())
    }

    /// Count one request at an explicit instant, for tests.
    pub fn check_at(&self, address: IpAddr, now: Instant) -> Decision {
        let bucket = bucket_of(address);
        let length = self.window;
        let mut keys = self
            .state
            .lock()
            .expect("the limiter lock is never poisoned");
        if let Some(window) = keys
            .get(&bucket)
            .filter(|w| w.live(now, length) && w.count >= self.per_window)
        {
            return Decision::Limited {
                retry_after_seconds: window.retry_after(now, length),
            };
        }
        if !keys.contains_key(&bucket) && keys.len() >= MAX_TRACKED_PEERS {
            // Only windows that have expired are dropped: they hold nothing a
            // fresh window would not.
            keys.retain(|_, w| w.live(now, length));
            if keys.len() >= MAX_TRACKED_PEERS {
                // Still full of live windows: serve the new key without one,
                // rather than grow, drop a live count, or lock it out.
                return Decision::AllowedUntracked;
            }
        }
        let window = keys.entry(bucket).or_insert_with(|| Window::fresh(now));
        if !window.live(now, length) {
            *window = Window::fresh(now);
        }
        window.count += 1;
        Decision::Allowed
    }

    /// How many keys hold a window right now, live or expired. Never more
    /// than [`MAX_TRACKED_PEERS`].
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
        let tracked = limiter.tracked();
        assert!(
            tracked <= MAX_TRACKED_PEERS,
            "the limiter tracked {tracked} peers"
        );
    }

    fn v6(s: &str) -> IpAddr {
        s.parse().unwrap()
    }

    fn limited(decision: Decision) -> bool {
        matches!(decision, Decision::Limited { .. })
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
        assert!(
            limited(limiter.check_at(v6("2001:db8:1:2:dead:beef:0:4"), start)),
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

    /// **There is no global budget.** Forty distinct clients, each spending
    /// exactly its own twenty inside one window — 800 requests, more than any
    /// ceiling of 600 a minute would admit — are every one served by the
    /// shipped login limiter. NEGATIVE CONTROL: each client's twenty-first is
    /// refused, so the limiter is limiting, per client.
    #[test]
    fn no_global_bucket_exists() {
        let limiter = RateLimiter::for_login();
        let start = Instant::now();
        let clients: Vec<IpAddr> = (0..40u8).map(|i| IpAddr::from([192, 0, 2, i])).collect();
        let total = clients.len() as u32 * LOGIN_PER_WINDOW;
        assert!(total > 600, "the row must exceed the reviewed ceiling");
        for round in 0..LOGIN_PER_WINDOW {
            for (i, client) in clients.iter().enumerate() {
                assert_eq!(
                    limiter.check_at(*client, start + Duration::from_millis(u64::from(round))),
                    Decision::Allowed,
                    "client {i}, request {round}: a client within its budget is served"
                );
            }
        }
        for client in &clients {
            assert!(limited(limiter.check_at(*client, start)));
        }
    }

    /// **A spray of new keys cannot reset a live window, grow the table, or
    /// lock anyone out.** The table fills with live windows; a spent key stays
    /// spent (eviction drops only EXPIRED windows, which hold nothing a fresh
    /// window would not), a tracked key with budget left is still served, and
    /// the new keys past the bound are served WITHOUT a window
    /// (`AllowedUntracked`), never refused. NEGATIVE CONTROL: once the windows
    /// expire, the next new key evicts them and is tracked again, and the
    /// table is still bounded.
    #[test]
    fn a_spray_cannot_reset_a_live_window_grow_the_table_or_lock_anyone_out() {
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
        let mut untracked = 0;
        for i in 0..(MAX_TRACKED_PEERS + 100) {
            match limiter.check_at(spray(i), start + Duration::from_secs(1)) {
                Decision::Allowed => {}
                Decision::AllowedUntracked => untracked += 1,
                Decision::Limited { .. } => panic!("sprayed key {i} was locked out"),
            }
        }
        assert_eq!(limiter.tracked(), MAX_TRACKED_PEERS);
        assert_eq!(untracked, 102, "the two earlier keys hold two places");
        let during = start + Duration::from_secs(2);
        assert!(
            limited(limiter.check_at(spent, during)),
            "the spray did not reset the spent key's live window"
        );
        assert_eq!(limiter.check_at(tracked, during), Decision::Allowed);
        assert_eq!(
            limiter.check_at(IpAddr::from([192, 0, 2, 3]), during),
            Decision::AllowedUntracked,
            "a new client is served while the table is full"
        );

        let after = start + Duration::from_secs(62);
        assert_eq!(
            limiter.check_at(IpAddr::from([192, 0, 2, 3]), after),
            Decision::Allowed
        );
        assert!(limiter.tracked() <= MAX_TRACKED_PEERS);
        assert_eq!(limiter.check_at(spent, after), Decision::Allowed);
    }
}
