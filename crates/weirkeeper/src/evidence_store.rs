//! The controller's `ControllerIdentity` evidence handles (D2 §3.10, §3.8
//! option **E**).
//!
//! The global read-only store is built from `LOGWEIR_ARCHIVE_URL` and the
//! controller's own environment, so on an installation with two destinations
//! it would read the second one's evidence from the wrong bucket or as the
//! wrong principal, and the `NotAttempted` would look like "no evidence". A
//! handle built here takes its bucket, prefix, region, endpoint, addressing and
//! transport from the destination's own [`DestinationLocation`] and its CA from
//! the destination's bundle; only the credential is ambient, which is what
//! `ControllerIdentity` means.
//!
//! # Opt-in and allowlisted by the installation
//!
//! The default is option **C** (D2 §3.8): an evidence-fetch Job in the object's
//! namespace with the destination's own `evidenceRead` credential, so the
//! controller holds no credential. Option **E** is one principal reading
//! several locations, bounded only by the list it may read, and that list
//! lives in the installation policy `ConfigMap` in the release namespace
//! ([`crate::check::policy`]): a namespace operator can only ask, and an
//! unlisted destination is refused with
//! [`CheckCode::ControllerIdentityNotAllowlisted`] and verification is
//! `NotAttempted`.
//!
//! # Interface I13, and the one sanctioned construction site
//!
//! `Store` drives its own current-thread runtime, and `Runtime::block_on` from
//! a thread already driving one panics with *Cannot start a runtime from within
//! a runtime*, having compiled cleanly. `main.rs` builds the global handle
//! before the runtime exists; [`StoreCache::get_or_build`] is the only other
//! site, and it constructs inside `tokio::task::spawn_blocking`.
//! `tests/retention.rs::no_store_call_is_made_outside_spawn_blocking` pins
//! both.
//!
//! # Bounded
//!
//! Thirty-two handles, each with a connection pool and a tokio runtime. An
//! unbounded map keyed by `(uid, generation, caSha256)` would grow with every
//! CA rotation or destination edit and never shrink, ending in an OOM weeks
//! later. Eviction is least-recently-used, so the destination a busy namespace
//! verifies against every few minutes stays.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

use logweir_core::check_contract::CheckCode;
use logweir_store::{CredentialSource, Store, StoreOptions};

use crate::check::policy::Policy;
use crate::destination::{
    controller_identity_allowed, DestinationRefusal, ResolvedDestination, ResolvedGrant,
};

/// How many read-only evidence handles are kept (D2 §3.10).
pub const MAX_CACHED_STORES: usize = 32;

/// What one cached handle is keyed by.
///
/// # All three parts, and what each would leak without
///
/// * **`uid`**, not the name: a destination deleted and recreated under the
///   same name is a different object, possibly a different bucket.
/// * **`generation`**: the CA reference and anything a later revision adds can
///   change, and keying on the generation makes every edit a new handle by
///   construction rather than by remembering to invalidate.
/// * **`ca_sha256`**, the bundle's content: a CA rotated in place changes the
///   `ConfigMap`, not the destination's generation, so a rotated root would
///   otherwise be invisible until a restart.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct CacheKey {
    /// The destination's `metadata.uid`.
    pub uid: String,
    /// The destination's `metadata.generation`.
    pub generation: i64,
    /// `sha256:<hex>` over the CA bytes, or `None` for the platform store.
    pub ca_sha256: Option<String>,
}

impl CacheKey {
    #[must_use]
    pub fn of(resolved: &ResolvedDestination) -> Self {
        Self {
            uid: resolved.uid.clone(),
            generation: resolved.generation,
            ca_sha256: resolved.ca_sha256.clone(),
        }
    }
}

/// A bounded, least-recently-used set of read-only evidence handles.
///
/// `Mutex<VecDeque<…>>`, not a third-party LRU crate: Global Constraint 38
/// closes the workspace graph, and a linear scan of thirty-two entries is
/// cheaper than hashing the key.
pub struct StoreCache {
    entries: Mutex<VecDeque<(CacheKey, Arc<Store>)>>,
    capacity: usize,
}

/// Hand-written, because `Store` is not `Debug` and stays so: a derived `Debug`
/// on a store handle would put its configuration, and one day its credential
/// provider, into whatever log line formatted it. This prints the keys and the
/// bound.
impl std::fmt::Debug for StoreCache {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let keys: Vec<CacheKey> = self
            .entries
            .lock()
            .map(|e| e.iter().map(|(k, _)| k.clone()).collect())
            .unwrap_or_default();
        f.debug_struct("StoreCache")
            .field("capacity", &self.capacity)
            .field("keys", &keys)
            .finish()
    }
}

impl Default for StoreCache {
    fn default() -> Self {
        Self::new()
    }
}

impl StoreCache {
    /// A cache holding [`MAX_CACHED_STORES`] handles.
    #[must_use]
    pub fn new() -> Self {
        Self::with_capacity(MAX_CACHED_STORES)
    }

    /// A cache of an explicit size (for the test that proves the bound). `0` is
    /// raised to `1`: a cache that held nothing would build a `Store`, and so a
    /// tokio runtime, on every verification.
    #[must_use]
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            entries: Mutex::new(VecDeque::new()),
            capacity: capacity.max(1),
        }
    }

    /// How many handles are held right now.
    #[must_use]
    pub fn len(&self) -> usize {
        self.entries.lock().map_or(0, |e| e.len())
    }

    /// Whether the cache holds nothing.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// The bound this cache was built with.
    #[must_use]
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// Whether a key is held, without promoting it (for tests that assert which
    /// entry was evicted).
    #[must_use]
    pub fn holds(&self, key: &CacheKey) -> bool {
        self.entries
            .lock()
            .is_ok_and(|e| e.iter().any(|(k, _)| k == key))
    }

    /// The cached handle for `key`, promoted to most-recently-used.
    fn take_cached(&self, key: &CacheKey) -> Option<Arc<Store>> {
        let mut entries = self.entries.lock().ok()?;
        let at = entries.iter().position(|(k, _)| k == key)?;
        let entry = entries.remove(at)?;
        let store = Arc::clone(&entry.1);
        entries.push_back(entry);
        Some(store)
    }

    /// Record a handle, evicting the least recently used while over capacity.
    ///
    /// **The evicted handles are returned, not dropped here.** `Store` owns a
    /// `tokio::runtime::Runtime`, and dropping one on a thread driving a
    /// runtime panics (*Cannot drop a runtime in a context where blocking is not
    /// allowed*), so [`StoreCache::get_or_build`] drops them inside
    /// `spawn_blocking`.
    #[must_use]
    fn record(&self, key: CacheKey, store: &Arc<Store>) -> Vec<Arc<Store>> {
        let Ok(mut entries) = self.entries.lock() else {
            return Vec::new();
        };
        let mut evicted = Vec::new();
        // A concurrent build of the same key may lose the race: both are
        // read-only handles on the same location, and keeping one is enough.
        if let Some(at) = entries.iter().position(|(k, _)| *k == key) {
            if let Some((_, old)) = entries.remove(at) {
                evicted.push(old);
            }
        }
        entries.push_back((key, Arc::clone(store)));
        while entries.len() > self.capacity {
            if let Some((_, old)) = entries.pop_front() {
                evicted.push(old);
            }
        }
        evicted
    }

    /// Release every handle, from a blocking thread.
    ///
    /// Not `Drop`: dropping a `Store` on a runtime thread panics (see
    /// [`StoreCache::record`]) and a `Drop` impl cannot `await`, so a caller
    /// invokes this at shutdown or at the end of a test. Never calling it is
    /// not a leak: the handles live as long as the process.
    pub async fn clear(&self) {
        let taken: Vec<(CacheKey, Arc<Store>)> = match self.entries.lock() {
            Ok(mut entries) => entries.drain(..).collect(),
            Err(_) => return,
        };
        if taken.is_empty() {
            return;
        }
        let _ = tokio::task::spawn_blocking(move || drop(taken)).await;
    }

    /// The read-only evidence handle for one resolved destination, built once
    /// and shared.
    ///
    /// # The four refusals, in order
    ///
    /// 1. The grant is not [`ResolvedGrant::ControllerIdentity`]: a
    ///    `SecretKeys` or `WorkloadIdentity` evidence read is an evidence-fetch
    ///    Job (D2 §3.9), because the controller may not read the Secret those
    ///    grants name.
    /// 2. The destination's location is not in the installation policy's
    ///    `evidence.controllerIdentityLocations`.
    /// 3. A hit: the cached handle, promoted.
    /// 4. A build, inside `spawn_blocking`.
    ///
    /// # `CredentialSource::Ambient`
    ///
    /// Only the credential comes from the controller pod's environment, through
    /// object_store's documented order. `StoreOptions`' `Ambient` source reads
    /// the named credential variables and never `AWS_ENDPOINT_URL`,
    /// `AWS_REGION`, `AWS_ALLOW_HTTP` or `AWS_VIRTUAL_HOSTED_STYLE_REQUEST`, so
    /// location, region, addressing and transport come from the destination.
    ///
    /// # Errors
    ///
    /// [`DestinationRefusal`] carrying
    /// [`CheckCode::ControllerIdentityNotAllowlisted`],
    /// [`CheckCode::DestinationRoleNotConfigured`], or the
    /// [`logweir_store::StoreErrorClass`] of a build failure.
    pub async fn get_or_build(
        &self,
        resolved: &ResolvedDestination,
        policy: &Policy,
    ) -> Result<Arc<Store>, DestinationRefusal> {
        if !matches!(resolved.grant, ResolvedGrant::ControllerIdentity) {
            return Err(refusal(
                CheckCode::DestinationRoleNotConfigured,
                "spec.access.evidenceRead.mode",
                format!(
                    "BackupDestination {}/{} does not ask for ControllerIdentity evidence \
                     reads, so the controller builds no handle for it; a SecretKeys or \
                     WorkloadIdentity grant is read by an evidence-fetch Job in the object's \
                     own namespace (D2 §3.9)",
                    resolved.namespace, resolved.name
                ),
            ));
        }
        if !controller_identity_allowed(policy, &resolved.location) {
            return Err(refusal(
                CheckCode::ControllerIdentityNotAllowlisted,
                "spec.access.evidenceRead.mode",
                format!(
                    "the installation policy's evidence.controllerIdentityLocations does not \
                     list {} (BackupDestination {}/{}); only a chart or cluster administrator \
                     can add it",
                    resolved.canonical_url, resolved.namespace, resolved.name
                ),
            ));
        }

        let key = CacheKey::of(resolved);
        if let Some(store) = self.take_cached(&key) {
            return Ok(store);
        }

        // The evidence location, not the archive one: `logweir/` is Global
        // Constraint 6's evidence root, which `Store`'s prefix guard requires.
        let location = resolved.evidence_storage();
        let mut options = StoreOptions {
            credentials: CredentialSource::Ambient,
            ..StoreOptions::default()
        };
        if let Some(pem) = resolved.ca_pem.clone() {
            options = options.with_root_certificate(pem);
        }
        let description = resolved.canonical_url.clone();

        // Inside `spawn_blocking` because the constructor builds a
        // current-thread runtime and calls `block_on`, which panics on the
        // reconciler's runtime thread (interface I13). `read_only_with`, never
        // `from_url_with` (guard G-RET): the handle cannot put.
        let built = tokio::task::spawn_blocking(move || Store::read_only_with(&location, &options))
            .await
            .map_err(|e| {
                refusal(
                    CheckCode::StoreErrorUnclassified,
                    "status.evidence",
                    format!("building the evidence handle for {description} did not complete: {e}"),
                )
            })?;

        match built {
            Ok(store) => {
                let store = Arc::new(store);
                let evicted = self.record(key, &store);
                if !evicted.is_empty() {
                    // See `record`: an evicted `Store` owns a runtime, and a
                    // runtime dropped on a runtime thread panics.
                    let _ = tokio::task::spawn_blocking(move || drop(evicted)).await;
                }
                Ok(store)
            }
            Err(e) => {
                let class = logweir_store::StoreErrorClass::classify(&e);
                let code =
                    CheckCode::parse(class.as_str()).unwrap_or(CheckCode::StoreErrorUnclassified);
                Err(refusal(
                    code,
                    "status.evidence",
                    format!(
                        "the controller could not build a read-only evidence handle for \
                         BackupDestination {}/{}: {}",
                        resolved.namespace,
                        resolved.name,
                        // Redacted: an object-store error can quote an S3 XML
                        // body, a presigned URL or a request header.
                        logweir_core::check_contract::redact(&e.to_string())
                    ),
                ))
            }
        }
    }
}

fn refusal(code: CheckCode, field: &str, message: String) -> DestinationRefusal {
    DestinationRefusal {
        code,
        field: field.to_string(),
        message,
    }
}
