//! Which socket peers the entry point treats as its trusted proxy.
//!
//! TWO SOURCES, ONE ANSWER. `trustedProxyCidrs` is the static list the
//! administrator writes (PLAT-17.2, with its `/16` and `/48` width floors under
//! `requireTrustedProxy`). `trustedProxyService` names the INGRESS
//! CONTROLLER'S Service, and the addresses of its serving endpoints are trusted
//! beside the list — chart gap G6: a `/32` for the ingress pod is wrong the
//! moment that pod is recreated, and a range wide enough to survive that
//! trusts every other pod the node schedules.
//!
//! WHAT THE SERVICE SOURCE TRUSTS, EXACTLY. Every address in an
//! `EndpointSlice` labelled `kubernetes.io/service-name=<name>` in `<namespace>`
//! whose endpoint is SERVING, each as a single host (`/32` or `/128`). That is
//! the ingress controller's own pods, today's, and nothing else — narrower than
//! any CIDR the width floors allow.
//!
//! WHO CAN WIDEN IT. Whoever can edit that Service's selector, write an
//! `EndpointSlice` in that namespace, or create or relabel a Pod (or a
//! Deployment) matching the Service's selector there — the EndpointSlice
//! controller then lists it — can add an address. That is the ingress
//! controller's own namespace, whose administrators already terminate the
//! console's TLS and could read or rewrite every request anyway; the console
//! grants no one a power they did not hold. It is still the reason the
//! namespace is named, not discovered, and the reason the chart's grant is a
//! `list` of `endpointslices` in THAT namespace only.
//!
//! A `hostNetwork` INGRESS PUBLISHES THE NODE'S ADDRESS. Its endpoint is the
//! node, so this source then trusts every hostNetwork pod and node process on
//! that node and anything the CNI masquerades to the node address — not "the
//! ingress pods and no other pod". Accept that knowingly, or decide a
//! `trustedProxyCidrs` `/32` instead.
//!
//! STALENESS FAILS CLOSED. The set is refreshed every [`REFRESH_EVERY`]. A
//! refresh that fails keeps the last complete set, but only for
//! [`MAX_AGE`] after it was read: past that the service source trusts nobody,
//! requests from the ingress are answered `421`, and `/readyz` reports not
//! ready — an outage an operator can see, never a stale grant nobody can. The
//! window an address stays trusted after its pod is gone is therefore at most
//! [`MAX_AGE`], and in the ordinary case one refresh: the endpoint leaves the
//! slice when the pod stops serving, before the pod's address is released for
//! reuse, and IP allocators do not hand a released address straight back.
//!
//! NOTHING HERE IS AN IDENTITY INPUT. As with the static list, a trusted peer
//! may only have its forwarded headers RECORDED and satisfy the entry-point
//! gate; no authentication, authorization, redirect or callback decision reads
//! a forwarded header (D0).

use std::collections::BTreeSet;
use std::net::IpAddr;
use std::sync::RwLock;
use std::time::{Duration, Instant};

use crate::config::{Cidr, ServiceRef};
use crate::kube::KubeAdapter;

/// How often the Service's endpoints are read.
pub const REFRESH_EVERY: Duration = Duration::from_secs(5);

/// How long the last complete read stays trusted when refreshes fail.
pub const MAX_AGE: Duration = Duration::from_secs(30);

/// The trusted proxy peers: the static ranges, and the Service's endpoints.
#[derive(Debug)]
pub struct TrustedProxies {
    cidrs: Vec<Cidr>,
    service: Option<ServiceRef>,
    endpoints: RwLock<Option<(BTreeSet<IpAddr>, Instant)>>,
    every: Duration,
    max_age: Duration,
}

impl TrustedProxies {
    /// The two sources, with no endpoint read yet.
    #[must_use]
    pub fn new(cidrs: Vec<Cidr>, service: Option<ServiceRef>) -> Self {
        Self {
            cidrs,
            service,
            endpoints: RwLock::new(None),
            every: REFRESH_EVERY,
            max_age: MAX_AGE,
        }
    }

    /// The same sources with a shorter refresh period and staleness window —
    /// so a test can drive [`TrustedProxies::run`] itself through a failed
    /// refresh and watch the set age out in milliseconds rather than thirty
    /// seconds. Production uses [`REFRESH_EVERY`] and [`MAX_AGE`].
    #[must_use]
    pub fn with_timing(mut self, every: Duration, max_age: Duration) -> Self {
        self.every = every;
        self.max_age = max_age;
        self
    }

    /// The static ranges alone.
    #[must_use]
    pub fn from_cidrs(cidrs: Vec<Cidr>) -> Self {
        Self::new(cidrs, None)
    }

    /// The configured Service, if any.
    #[must_use]
    pub fn service(&self) -> Option<&ServiceRef> {
        self.service.as_ref()
    }

    /// Whether `peer` is a trusted proxy right now.
    ///
    /// An IPv4-mapped IPv6 peer is the IPv4 address it maps to, for both
    /// sources.
    #[must_use]
    pub fn contains(&self, peer: IpAddr) -> bool {
        let peer = canonical(peer);
        if self.cidrs.iter().any(|c| c.contains(peer)) {
            return true;
        }
        self.current().is_some_and(|set| set.contains(&peer))
    }

    /// Whether the Service source, if configured, holds a set young enough to
    /// use. Always true without one. `/readyz` reads this.
    #[must_use]
    pub fn ready(&self) -> bool {
        self.service.is_none() || self.current().is_some()
    }

    /// The Service's addresses, if the last complete read is within
    /// [`MAX_AGE`].
    #[must_use]
    pub fn current(&self) -> Option<BTreeSet<IpAddr>> {
        let guard = self
            .endpoints
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        match &*guard {
            Some((set, read_at)) if read_at.elapsed() <= self.max_age => Some(set.clone()),
            _ => None,
        }
    }

    /// Replace the Service's addresses with a complete read taken at `at`.
    /// Returns whether the set changed.
    pub fn replace_at(&self, addresses: impl IntoIterator<Item = IpAddr>, at: Instant) -> bool {
        let next: BTreeSet<IpAddr> = addresses.into_iter().map(canonical).collect();
        let mut guard = self
            .endpoints
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let changed = guard.as_ref().is_none_or(|(set, _)| *set != next);
        *guard = Some((next, at));
        changed
    }

    /// One refresh: read the Service's serving endpoints and replace the set.
    /// A failed read changes nothing, so the last complete set ages out on its
    /// own.
    ///
    /// # Errors
    ///
    /// [`crate::kube::KubeFailure`] from the read; `Ok(None)` without a
    /// configured Service.
    pub async fn refresh(
        &self,
        kube: &KubeAdapter,
    ) -> Result<Option<usize>, crate::kube::KubeFailure> {
        let Some(service) = &self.service else {
            return Ok(None);
        };
        let addresses = kube
            .list_service_endpoints(&service.namespace, &service.name)
            .await?;
        let count = addresses.len();
        if self.replace_at(addresses.iter().copied(), Instant::now()) {
            tracing::info!(
                namespace = %service.namespace,
                service = %service.name,
                addresses = %addresses
                    .iter()
                    .map(ToString::to_string)
                    .collect::<Vec<_>>()
                    .join(","),
                "the trusted proxy set changed: these ingress endpoints are the entry point's peers"
            );
        }
        if count == 0 {
            tracing::warn!(
                namespace = %service.namespace,
                service = %service.name,
                "the trusted proxy Service has no serving endpoint; the entry point refuses every \
                 request until it has one"
            );
        }
        Ok(Some(count))
    }

    /// Refresh every [`REFRESH_EVERY`], forever. Spawned once by `main` when a
    /// Service is configured; the first read happens immediately.
    ///
    /// A FAILED REFRESH TOUCHES NOTHING. Only [`TrustedProxies::refresh`]'s
    /// successful read stamps the set; this loop logs a failure and sleeps, so
    /// the last complete set ages out on its own [`MAX_AGE`] clock.
    /// `tests/entry_point.rs` drives this loop through failures to pin that.
    pub async fn run(self: std::sync::Arc<Self>, kube: KubeAdapter) {
        loop {
            if let Err(failure) = self.refresh(&kube).await {
                tracing::warn!(
                    ?failure,
                    max_age_seconds = self.max_age.as_secs(),
                    "could not read the trusted proxy Service's endpoints; the last complete set \
                     is kept until it is older than max_age_seconds"
                );
            }
            tokio::time::sleep(self.every).await;
        }
    }
}

fn canonical(ip: IpAddr) -> IpAddr {
    match ip {
        IpAddr::V6(v6) => v6.to_ipv4_mapped().map_or(ip, IpAddr::V4),
        IpAddr::V4(_) => ip,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ip(s: &str) -> IpAddr {
        s.parse().unwrap()
    }

    fn service() -> Option<ServiceRef> {
        Some(ServiceRef {
            namespace: "traefik".into(),
            name: "traefik".into(),
        })
    }

    #[test]
    fn the_static_ranges_alone_behave_as_before() {
        let t = TrustedProxies::from_cidrs(vec![Cidr::parse("10.42.0.0/16").unwrap()]);
        assert!(t.contains(ip("10.42.3.4")));
        assert!(t.contains(ip("::ffff:10.42.3.4")));
        assert!(!t.contains(ip("10.43.0.1")));
        assert!(t.ready(), "no Service source is always ready");
    }

    #[test]
    fn the_service_source_trusts_exactly_its_serving_addresses() {
        let t = TrustedProxies::new(Vec::new(), service());
        assert!(!t.ready(), "not ready before the first read");
        assert!(!t.contains(ip("10.1.0.7")), "nothing before the first read");
        assert!(t.replace_at([ip("10.1.0.7"), ip("fd00::7")], Instant::now()));
        assert!(t.ready());
        assert!(t.contains(ip("10.1.0.7")));
        assert!(t.contains(ip("::ffff:10.1.0.7")));
        assert!(t.contains(ip("fd00::7")));
        assert!(!t.contains(ip("10.1.0.8")), "a neighbour is not trusted");
        // The ingress pod is recreated with a new address: the old one is
        // distrusted at once, the new one trusted.
        assert!(t.replace_at([ip("10.1.0.9")], Instant::now()));
        assert!(!t.contains(ip("10.1.0.7")));
        assert!(t.contains(ip("10.1.0.9")));
        assert!(!t.replace_at([ip("10.1.0.9")], Instant::now()), "unchanged");
    }

    #[test]
    fn a_set_older_than_the_max_age_trusts_nobody() {
        let t = TrustedProxies::new(Vec::new(), service());
        let old = Instant::now()
            .checked_sub(MAX_AGE + Duration::from_secs(1))
            .expect("the clock is past the window");
        t.replace_at([ip("10.1.0.7")], old);
        assert!(
            !t.contains(ip("10.1.0.7")),
            "a stale read is not a grant: the failure is a visible 421, never a silent trust"
        );
        assert!(!t.ready(), "and /readyz says so");
        // The static ranges are not aged: they are configuration.
        let both = TrustedProxies::new(vec![Cidr::parse("192.0.2.0/24").unwrap()], service());
        both.replace_at([ip("10.1.0.7")], old);
        assert!(both.contains(ip("192.0.2.9")));
        assert!(!both.contains(ip("10.1.0.7")));
    }
}
