//! Rate and connection limits for the unauthenticated login surface and for
//! streams.
//!
//! THE SIGN-IN LIMIT, AND THE THREAT IT ANSWERS (FX-13, settled after three
//! security reviews; the design record is the FX-13 result's *Security review
//! follow-up*). `/auth/login` and `/auth/callback` are the only routes an
//! unauthenticated caller can reach that do work. The limit is per client, it
//! bounds LOAD, and it has no global budget. In full:
//!
//! 1. IT BOUNDS LOAD, NOT CREDENTIALS. There is nothing to guess here. An OIDC
//!    sign-in takes no password at this service. A callback reaches the
//!    provider only with a login cookie this service minted, whose `state`
//!    must match (`login.rs:214`, `login.rs:225`), and the code it carries is
//!    exchanged with that cookie's PKCE verifier at the provider's token
//!    endpoint (`login.rs:231` → `oidc.rs:577`, `oidc.rs:604`): the provider
//!    makes authorization codes single-use, short-lived and unguessable
//!    (RFC 6749 §4.1.2 and §10.10) and binds each to the verifier (RFC 7636).
//!    More attempts buy an attacker nothing but load. THE LOGIN COOKIE IS NOT
//!    SINGLE-USE HERE: it is sealed and stateless, it opens for
//!    `LOGIN_STATE_SECONDS` (600 s, `session.rs:36`; checked for authenticity
//!    and age only, `session.rs:241-256`), and nothing records a used
//!    `state`, so one `/auth/login` arms any number of callbacks inside the
//!    callback key's own budget — each one a token request (point 3).
//!
//! 2. NO AMPLIFICATION: AT MOST ONE PROVIDER REQUEST PER REQUEST. Readiness
//!    warms the discovery and JWKS caches before a replica takes traffic
//!    (`oidc.rs:357`). Then `/auth/login` makes none while the discovery
//!    document is cached (`DISCOVERY_MAX_AGE`, an hour, `oidc.rs:40`; the
//!    cache read at `oidc.rs:412`, called at `login.rs:117`) and one `GET`
//!    when the hour has lapsed (`oidc.rs:426`); `/auth/callback` makes exactly
//!    one token `POST` (`oidc.rs:604`), the keys coming from the JWKS cache
//!    (`JWKS_MAX_AGE`, a day, `oidc.rs:45`; read at `oidc.rs:642`), and an
//!    unknown `kid` in the provider's OWN ID token forces at most one refetch
//!    per `JWKS_MIN_REFETCH` per process (a minute, `oidc.rs:43`,
//!    `oidc.rs:505`, `oidc.rs:644`). A cold or expired cache adds at most the
//!    discovery `GET` and a JWKS `GET`; a fetch that SUCCEEDS then serves
//!    every later request, while a failed one is not cached, so during a
//!    provider outage each request tries again — still at most three. A
//!    constant per request is no amplification.
//!
//! 3. BUT IT IS CONCENTRATION: THE PROVIDER SEES THIS SERVICE, NOT THE
//!    ATTACKER. Each key is held to [`LOGIN_PER_WINDOW`] and none can spend
//!    another's; past [`MAX_TRACKED_PEERS`] live keys a new key is served
//!    without a window ([`Decision::AllowedUntracked`]). An attacker with N
//!    keys therefore gets N bounded budgets, and its callbacks reach the
//!    provider AUTHENTICATED AS THIS CLIENT (`client_secret`, `oidc.rs:585-597`)
//!    AND FROM THIS SERVICE'S ADDRESS — not as the unauthenticated requests
//!    from N addresses it could send the provider directly. A provider that
//!    throttles per client or per source can therefore refuse this service's
//!    sign-ins under a many-address attack: a sign-in outage AT THE PROVIDER,
//!    for every operator, while it lasts.
//!
//! 4. A DISTRIBUTED ATTACKER CAN CAUSE A SIGN-IN OUTAGE EITHER WAY, AND
//!    AVAILABILITY DECIDES WHERE. With no budget over all clients, it takes
//!    enough addresses to spend the provider's quota for this client. With
//!    one, it takes far fewer: a global cap here GUARANTEES the outage at a
//!    volume far below any provider's quota — the old peer key was such a
//!    cap, and one client locked everyone out; a 600-a-minute ceiling is 30
//!    addresses, or one IPv6 customer's `/56`. For a recovery tool, operators
//!    signing in during an incident outrank a tighter bound, so there is NO
//!    GLOBAL BUDGET and nothing refuses a client that is within its own. For
//!    the same reason a FULL TABLE NEVER REFUSES: refusing new keys there is
//!    a cheap lockout too — an IPv6 `/52` is 4,096 `/64`s — which is why the
//!    table is 65,536 keys and serves a new key untracked when it is full.
//!    What the operator does about the residual (`docs/api.md`, *Rate
//!    limits*): set the provider's per-client limits for this client
//!    generously; alert on `loginRateUntracked` and on refused exchanges
//!    whose log line says `the provider answered HTTP 429`; and keep a
//!    break-glass path that does not sign in through the provider — the
//!    in-cluster administrator mode, reached by `kubectl port-forward`.
//!
//! THE KEY IS THE CLIENT AS THE TRUSTED PROXY SAW IT, AND OTHERWISE THE PEER.
//! Behind the shared console's ingress every request has the ingress as its
//! socket peer, so `crate::http::login_rate_key` chooses the bucket:
//!
//! - when the immediate peer is a trusted proxy — the same
//!   `TrustedProxies::contains` decision the entry point's `requireTrustedProxy`
//!   gate makes, so a Service source that is not read yet or older than its
//!   `MAX_AGE` trusts nobody here either — the key is the rightmost
//!   `X-Forwarded-For` hop that is not itself a trusted proxy: the address the
//!   outermost trusted proxy received the request from, which a client cannot
//!   choose (everything left of it is client-sent and is never read);
//! - otherwise, and whenever that hop is absent or is not an address (a bare
//!   IP, or `ip:port` with the port dropped), or the chain names only trusted
//!   proxies, the key is the socket peer.
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
//! request. [`IPV6_KEY_PREFIX`] says why `/64` and not wider. An address in
//! the well-known NAT64 prefix `64:ff9b::/96` is keyed as the IPv4 client it
//! embeds instead, so IPv4 clients behind a translator are not one budget.
//!
//! A BUCKET IS NOT AN IDENTITY. D0 allows forwarded values for transport
//! facts only — never for an identity or a grant (amended 2026-10-07 for this
//! key). The forwarded address chooses which counter a request is charged to,
//! and a counter can only refuse: it authenticates no one, authorizes nothing
//! and is never an audit actor. A forged header from an untrusted peer is not
//! read at all, so it cannot move a request out of its peer's bucket.
//!
//! THE TABLE IS BOUNDED, AND A SPRAY RESETS NOTHING. At most
//! [`MAX_TRACKED_PEERS`] keys hold a window (a few MB at worst; the constant
//! says how many). When a new key finds the table full, the windows that have
//! expired are swept — they hold nothing a fresh window would not, so a spray
//! of addresses cannot reset a live count — at most [`SWEEPS_PER_WINDOW`]
//! times a window and never before the oldest window can have expired, so a
//! full table does not turn every request into a 65,536-entry scan. If it is
//! still full, the new key is served untracked: its request carries the audit
//! note `loginRateUntracked`, and the first one in a window logs one WARN line
//! (never one per request). The windows are per console process, as they
//! always were: behind an ingress that spreads requests over N replicas, one
//! client may be served up to N times its allowance.
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
/// The most keys tracked at once, per console process.
///
/// WHY 65,536. Filling the table takes that many keys live at once — a whole
/// IPv6 `/48` of `/64`s, or 65,536 IPv4 addresses — where 4,096 was one `/52`.
/// Past it a new key is served untracked, never refused (the module
/// documentation's point 4).
///
/// WORST-CASE MEMORY, 6.1 MiB steady. A slot is the `(IpAddr, Window)` pair:
/// a 17-byte `IpAddr` and a 24-byte `Window` (a 16-byte `Instant` and a
/// `u32`), 48 bytes aligned — measured on macOS arm64, and the same on Linux
/// x86_64, where an `Instant` is also 16 bytes. The table's `HashMap` keeps at
/// most 7/8 of its buckets full and rounds the bucket count up to a power of
/// two, so 65,536 keys take 131,072 buckets at 49 bytes each (the slot and one
/// control byte): 6,422,528 bytes, plus one 16-byte control group. The last
/// growth briefly holds the old table of 65,536 buckets beside the new one,
/// 9.2 MiB at the peak. A swept table keeps its capacity, so that is also
/// the most it ever holds. `tests::the_table_bound_costs_a_few_mib` recomputes
/// this from the real sizes.
pub const MAX_TRACKED_PEERS: usize = 65_536;
/// How many times a window a full table may be swept for expired windows.
pub const SWEEPS_PER_WINDOW: u32 = 16;

/// The RFC 6052 well-known NAT64 prefix, `64:ff9b::/96`, as its top 96 bits.
const NAT64_WELL_KNOWN_PREFIX: u128 = 0x0064_ff9b_0000_0000_0000_0000;

/// The bucket an address is counted in:
///
/// - an IPv4-mapped IPv6 address (`::ffff:0:0/96`) as its IPv4 address;
/// - an address in the well-known NAT64 prefix `64:ff9b::/96` (RFC 6052) as
///   the IPv4 address it embeds: behind a server-side NAT64 or SIIT
///   translator every IPv4 client arrives in that one `/96`, and folding it to
///   its `/64` would put them all in one budget (FX-13 review L1). A client
///   cannot pick such a source address without the translator answering for
///   it. A network-specific prefix (RFC 8215's `64:ff9b:1::/48`, or any
///   other) places the IPv4 bits by its own length, which this service does
///   not know, so it folds to its `/64` like any IPv6 address — the pre-FX-13
///   single budget for those clients, never a bypass;
/// - any other IPv6 address as its [`IPV6_KEY_PREFIX`] network;
/// - an IPv4 address as itself.
#[must_use]
pub fn bucket_of(address: IpAddr) -> IpAddr {
    match address.to_canonical() {
        IpAddr::V6(v6) => {
            let bits = u128::from(v6);
            if bits >> 32 == NAT64_WELL_KNOWN_PREFIX {
                // The low 32 bits are the IPv4 address; truncation is the point.
                #[allow(clippy::cast_possible_truncation)]
                return IpAddr::V4(std::net::Ipv4Addr::from(bits as u32));
            }
            let mask = u128::MAX << (128 - IPV6_KEY_PREFIX);
            IpAddr::V6(std::net::Ipv6Addr::from(bits & mask))
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
    /// No sweep before this: the oldest window cannot have expired sooner, or
    /// the last sweep was less than a sweep interval ago.
    next_sweep: Option<Instant>,
    /// When the last untracked admission was announced in the log.
    untracked_announced: Option<Instant>,
}

/// A fixed-window counter per key — the forwarded client behind a trusted
/// proxy, or the socket peer (see the module documentation). There is no
/// budget over all keys together, on purpose.
pub struct RateLimiter {
    window: Duration,
    per_window: u32,
    max_tracked: usize,
    state: Mutex<Table>,
}

/// What a limiter decided.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Decision {
    /// Under the key's budget; the request was counted.
    Allowed,
    /// The table of keys is full of live windows and this key is new: served,
    /// without a window, rather than locked out (the module documentation's
    /// point 4).
    AllowedUntracked {
        /// The first untracked admission in a window: log it, once.
        announce: bool,
    },
    /// Over the key's budget; retry after this many seconds. Nothing was
    /// counted.
    Limited {
        /// Seconds until the key's window resets.
        retry_after_seconds: u64,
    },
}

impl RateLimiter {
    /// A limiter over a window and a per-key budget, tracking at most
    /// [`MAX_TRACKED_PEERS`] keys.
    #[must_use]
    pub fn new(window: Duration, per_window: u32) -> Self {
        Self {
            window,
            per_window,
            max_tracked: MAX_TRACKED_PEERS,
            state: Mutex::new(Table {
                keys: HashMap::new(),
                next_sweep: None,
                untracked_announced: None,
            }),
        }
    }

    /// The same limiter with a smaller table, so a test can fill it in a few
    /// requests. Production uses [`MAX_TRACKED_PEERS`].
    #[must_use]
    pub fn with_max_tracked(mut self, max_tracked: usize) -> Self {
        self.max_tracked = max_tracked;
        self
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
        let mut table = self
            .state
            .lock()
            .expect("the limiter lock is never poisoned");
        if let Some(window) = table
            .keys
            .get(&bucket)
            .filter(|w| w.live(now, length) && w.count >= self.per_window)
        {
            return Decision::Limited {
                retry_after_seconds: window.retry_after(now, length),
            };
        }
        if !table.keys.contains_key(&bucket) && table.keys.len() >= self.max_tracked {
            if table.next_sweep.is_none_or(|at| now >= at) {
                // Only windows that have expired are dropped: they hold
                // nothing a fresh window would not.
                let mut oldest: Option<Instant> = None;
                table.keys.retain(|_, w| {
                    let live = w.live(now, length);
                    if live {
                        oldest = Some(oldest.map_or(w.started, |o| o.min(w.started)));
                    }
                    live
                });
                // A window's start only ever moves later, so none expires
                // before the oldest one does.
                let interval = length / SWEEPS_PER_WINDOW;
                let expiry = oldest.map_or(now, |o| o + length);
                table.next_sweep = Some(expiry.max(now + interval));
            }
            if table.keys.len() >= self.max_tracked {
                // Still full of live windows: serve the new key without one,
                // rather than grow, drop a live count, or lock it out.
                let announce = table
                    .untracked_announced
                    .is_none_or(|at| now.duration_since(at) >= length);
                if announce {
                    table.untracked_announced = Some(now);
                }
                return Decision::AllowedUntracked { announce };
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
        Decision::Allowed
    }

    /// How many keys hold a window right now, live or expired. Never more
    /// than the table's bound ([`MAX_TRACKED_PEERS`] in production).
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
            "RateLimiter({} per {:?}, {} keys)",
            self.per_window, self.window, self.max_tracked
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
        } = limiter.check_at(ip(1), start)
        else {
            panic!("the fourth request in the window must be limited");
        };
        assert!((1..=60).contains(&retry_after_seconds));

        // A different peer has its own window.
        assert_eq!(limiter.check_at(ip(2), start), Decision::Allowed);

        // Past the window the allowance returns — and only the allowance:
        // the renewed window is limited again (review, informational: a
        // window that was never renewed left a key unlimited after its first
        // minute, and only an unrelated row noticed).
        let later = start + Duration::from_secs(61);
        for i in 0..3 {
            assert_eq!(limiter.check_at(ip(1), later), Decision::Allowed, "{i}");
        }
        assert!(matches!(
            limiter.check_at(ip(1), later),
            Decision::Limited { .. }
        ));
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
        // NAT64's well-known prefix embeds the IPv4 client (review L1): two
        // IPv4 clients behind the translator are two buckets, and one of them
        // is the same bucket as that client arriving natively. A
        // network-specific prefix is an ordinary /64.
        assert_eq!(bucket_of(v6("64:ff9b::c000:205")), v6("192.0.2.5"));
        assert_eq!(bucket_of(v6("64:ff9b::192.0.2.6")), v6("192.0.2.6"));
        assert_ne!(
            bucket_of(v6("64:ff9b::c000:205")),
            bucket_of(v6("64:ff9b::c000:206"))
        );
        assert_eq!(
            bucket_of(v6("64:ff9b:1::c000:205")),
            v6("64:ff9b:1::"),
            "RFC 8215's local-use prefix is not unwrapped"
        );
        assert_eq!(
            bucket_of(v6("64:ff9b:0:0:1::5")),
            v6("64:ff9b::"),
            "only the /96 is the well-known prefix"
        );

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
        let mut announced = 0;
        for i in 0..(MAX_TRACKED_PEERS + 100) {
            match limiter.check_at(spray(i), start + Duration::from_secs(1)) {
                Decision::Allowed => {}
                Decision::AllowedUntracked { announce } => {
                    untracked += 1;
                    announced += usize::from(announce);
                }
                Decision::Limited { .. } => panic!("sprayed key {i} was locked out"),
            }
        }
        assert_eq!(limiter.tracked(), MAX_TRACKED_PEERS);
        assert_eq!(untracked, 102, "the two earlier keys hold two places");
        assert_eq!(announced, 1, "announced once, not once per request");
        let during = start + Duration::from_secs(2);
        assert!(
            limited(limiter.check_at(spent, during)),
            "the spray did not reset the spent key's live window"
        );
        assert_eq!(limiter.check_at(tracked, during), Decision::Allowed);
        assert_eq!(
            limiter.check_at(IpAddr::from([192, 0, 2, 3]), during),
            Decision::AllowedUntracked { announce: false },
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

    /// **Untracked service is announced once a window, not once a request**,
    /// and announced again in a later window if the table is still full.
    /// NEGATIVE CONTROL: the second window's first untracked admission does
    /// announce, so the once-a-window rule is not "once ever".
    #[test]
    fn untracked_service_is_announced_once_a_window() {
        let start = Instant::now();
        let limiter = RateLimiter::new(Duration::from_secs(60), 5).with_max_tracked(2);
        let key = |i: u8| IpAddr::from([192, 0, 2, i]);
        assert_eq!(limiter.check_at(key(1), start), Decision::Allowed);
        assert_eq!(limiter.check_at(key(2), start), Decision::Allowed);
        let first = start + Duration::from_secs(1);
        assert_eq!(
            limiter.check_at(key(3), first),
            Decision::AllowedUntracked { announce: true }
        );
        for i in 4..9 {
            assert_eq!(
                limiter.check_at(key(i), first),
                Decision::AllowedUntracked { announce: false }
            );
        }
        // The two tracked keys come back in the next window, so the table is
        // full of LIVE windows again.
        let next = start + Duration::from_secs(61);
        assert_eq!(limiter.check_at(key(1), next), Decision::Allowed);
        assert_eq!(limiter.check_at(key(2), next), Decision::Allowed);
        let later = start + Duration::from_secs(62);
        assert_eq!(
            limiter.check_at(key(9), later),
            Decision::AllowedUntracked { announce: true }
        );
        assert_eq!(
            limiter.check_at(key(10), later),
            Decision::AllowedUntracked { announce: false }
        );
    }

    /// **A full table is swept at most `SWEEPS_PER_WINDOW` times a window**,
    /// so a spray of new keys against a full table does not make every
    /// request scan it. A sweep at t+60.2s drops the one expired window; the
    /// window that expires half a second later is NOT swept by the next new
    /// key at t+61s (served untracked), only by one after the sweep interval
    /// (60s / 16 = 3.75s). NEGATIVE CONTROL: that later key IS tracked, so the
    /// sweep still happens, just not on every request.
    #[test]
    fn a_full_table_is_swept_at_most_sixteen_times_a_window() {
        let start = Instant::now();
        let at = |ms: u64| start + Duration::from_millis(ms);
        let limiter = RateLimiter::new(Duration::from_secs(60), 5).with_max_tracked(2);
        let key = |i: u8| IpAddr::from([192, 0, 2, i]);
        assert_eq!(limiter.check_at(key(1), at(0)), Decision::Allowed);
        assert_eq!(limiter.check_at(key(2), at(500)), Decision::Allowed);
        // key 1 expired at 60.0s: the sweep drops it and key 3 takes its
        // place.
        assert_eq!(limiter.check_at(key(3), at(60_200)), Decision::Allowed);
        // key 2 expired at 60.5s, but the last sweep was 0.8s ago.
        assert!(matches!(
            limiter.check_at(key(4), at(61_000)),
            Decision::AllowedUntracked { .. }
        ));
        assert_eq!(limiter.tracked(), 2);
        // 3.75s after the sweep, the next new key sweeps key 2 out.
        assert_eq!(limiter.check_at(key(5), at(64_000)), Decision::Allowed);
        assert_eq!(limiter.tracked(), 2);
    }

    /// **The table's worst case is a few MiB**, recomputed from the real
    /// sizes the way `MAX_TRACKED_PEERS`'s documentation computes it: the
    /// bucket count the `HashMap` needs for that many keys (at most 7/8 full,
    /// a power of two) times the slot and its control byte. A `Window` or a
    /// bound that grew past 8 MiB steady fails here, and the documentation's
    /// figure with it.
    #[test]
    fn the_table_bound_costs_a_few_mib() {
        let slot = std::mem::size_of::<(IpAddr, Window)>();
        let buckets = (MAX_TRACKED_PEERS * 8 / 7).next_power_of_two();
        assert_eq!(buckets, 131_072);
        let steady = buckets * (slot + 1);
        assert!(slot <= 48, "a slot is {slot} bytes");
        assert!(steady <= 8 << 20, "the full table costs {steady} bytes");
        // The last growth holds the previous table (half the buckets) too.
        let peak = steady + buckets / 2 * (slot + 1);
        assert!(peak <= 12 << 20, "the growth peak is {peak} bytes");
    }
}
