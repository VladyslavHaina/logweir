//! The controller's `ControllerIdentity` evidence handles — D2 §3.10, §3.8
//! option **E**.
//!
//! # The defect this closes
//!
//! Grounding **G2**: today the controller builds ONE read-only store from
//! `LOGWEIR_ARCHIVE_URL`, and every region, endpoint and credential in it comes
//! from the controller's own process environment. On an installation with two
//! destinations, evidence for the second one is read from the wrong bucket, or
//! with the wrong principal, and the resulting `NotAttempted` looks like "no
//! evidence" rather than "wrong bucket". That is the tracker's *global
//! configuration leakage*.
//!
//! A handle built HERE takes its bucket, prefix, region, endpoint, addressing
//! and transport from the destination's own [`DestinationLocation`], and its CA
//! from the destination's own bundle. Only the CREDENTIAL is ambient, which is
//! what `ControllerIdentity` means.
//!
//! # Why this is opt-in and allowlisted, and who holds the allowlist
//!
//! D2 §3.8's honest comparison: the default is option **C**, an evidence-fetch
//! Job that runs in the object's own namespace with the destination's own
//! `evidenceRead` credential. That keeps the controller holding no credential
//! at all. Option **E** — this module — is one principal reading several
//! locations, and its blast radius is bounded only by the list of locations it
//! may read. That list lives in the installation policy `ConfigMap` in the
//! RELEASE namespace ([`crate::check::policy`]), so a namespace operator cannot
//! point the controller's principal anywhere: they can only ask, and an
//! unlisted destination is refused with
//! [`CheckCode::ControllerIdentityNotAllowlisted`] and verification is
//! `NotAttempted`.
//!
//! # Interface I13, and the ONE sanctioned construction site
//!
//! `Store` drives its own current-thread runtime, and `Runtime::block_on` from
//! a thread already driving one panics with *Cannot start a runtime from within
//! a runtime* — having compiled cleanly. Until this module,
//! `crates/weirkeeper/src/main.rs` was the ONLY place in the crate that built a
//! `Store`, and `tests/retention.rs::no_store_call_is_made_outside_spawn_blocking`
//! asserted exactly that.
//!
//! [`StoreCache::get_or_build`] is the SECOND and LAST site, and the I13 guard
//! now names it explicitly. The construction is inside
//! `tokio::task::spawn_blocking`, for the same reason every `Store` CALL is:
//! the constructor is where `Store` builds and drives its runtime. A mutant
//! that moves it out of the closure fails that guard.
//!
//! # And it is BOUNDED
//!
//! Thirty-two handles. Each holds a connection pool and a tokio runtime; an
//! unbounded map keyed by `(uid, generation, caSha256)` grows by one entry every
//! time an operator rotates a CA or edits a destination, and never shrinks —
//! a slow leak whose symptom is a controller OOM weeks later. The eviction is
//! least-recently-USED and not least-recently-inserted, so the destination a
//! busy namespace verifies against every few minutes is the one that stays.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

use logweir_core::check_contract::CheckCode;
use logweir_store::{CredentialSource, Store, StoreOptions};

use crate::check::policy::Policy;
use crate::destination::{
    controller_identity_allowed, DestinationRefusal, ResolvedDestination, ResolvedGrant,
};

/// How many read-only evidence handles are kept — D2 §3.10.
pub const MAX_CACHED_STORES: usize = 32;

/// What one cached handle is keyed by.
///
/// # All three parts, and what each of them would leak without
///
/// * **`uid`** — not the name. A destination deleted and recreated under the
///   same name is a different object, possibly a different bucket; a
///   name-keyed cache would serve the OLD store to the NEW destination.
/// * **`generation`** — the spec version. Location and transport are immutable,
///   but the CA reference is not, and neither is anything a later revision adds;
///   keying on the generation makes every edit a new handle by construction
///   rather than by someone remembering to invalidate.
/// * **`ca_sha256`** — the bundle's CONTENT. A CA rotated in place does not
///   change the destination's generation at all (the `ConfigMap` changed, not
///   the `BackupDestination`), so without this part a rotated root would be
///   invisible until the controller restarted.
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
    /// The key one resolved destination is cached under.
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
/// `Mutex<VecDeque<…>>` and not a third-party LRU crate: Global Constraint 38
/// closes the workspace graph, and thirty-two entries with a linear scan is
/// cheaper than the hash of the key would be.
pub struct StoreCache {
    entries: Mutex<VecDeque<(CacheKey, Arc<Store>)>>,
    capacity: usize,
}

/// HAND-WRITTEN, because `Store` is not `Debug` — and it stays that way on
/// purpose: a derived `Debug` on a store handle would put its configuration,
/// and one day its credential provider, into whatever log line formatted it.
/// What a reader of a cache dump needs is the KEYS and the bound, which is what
/// this prints.
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

    /// A cache of an explicit size — for the test that proves the bound is a
    /// bound. `0` is raised to `1`: a cache that could hold nothing would
    /// rebuild a `Store`, and therefore a tokio runtime, on every verification.
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

    /// Whether a key is currently held, WITHOUT promoting it — for tests that
    /// assert which entry was evicted.
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
    /// **THE EVICTED HANDLES ARE RETURNED AND NOT DROPPED HERE.** `Store` owns
    /// a `tokio::runtime::Runtime`, and dropping a runtime from a thread that
    /// is driving one panics with *Cannot drop a runtime in a context where
    /// blocking is not allowed* — the same class of defect as interface I13's
    /// nested-runtime panic, at the other end of the handle's life. So an
    /// eviction hands the handles back and [`StoreCache::get_or_build`] drops
    /// them inside `spawn_blocking`.
    #[must_use]
    fn record(&self, key: CacheKey, store: &Arc<Store>) -> Vec<Arc<Store>> {
        let Ok(mut entries) = self.entries.lock() else {
            return Vec::new();
        };
        let mut evicted = Vec::new();
        // A concurrent build of the same key is a race this cache is allowed to
        // lose: both handles are read-only handles on the same location, and
        // keeping one of them is the whole requirement.
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
    /// # Why this is not `Drop`
    ///
    /// See [`StoreCache::record`]: dropping a `Store` drops a
    /// `tokio::runtime::Runtime`, and doing that on a runtime thread panics. A
    /// `Drop` impl cannot `await`, so the release has to be a method a caller
    /// invokes — at shutdown, or at the end of a test that built handles.
    ///
    /// A controller that never calls it is not leaking: the handles live as
    /// long as the process, which is what the cache is for.
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
    /// 1. The grant is not [`ResolvedGrant::ControllerIdentity`] — this cache is
    ///    the `ControllerIdentity` path and nothing else. A `SecretKeys` or
    ///    `WorkloadIdentity` evidence read is an evidence-fetch JOB (D2 §3.9),
    ///    because the controller may not read the Secret those grants name.
    /// 2. The destination's location is not in the installation policy's
    ///    `evidence.controllerIdentityLocations`.
    /// 3. A hit: the cached handle, promoted.
    /// 4. A build, inside `spawn_blocking`.
    ///
    /// # `CredentialSource::Ambient`, and what that does and does not read
    ///
    /// The credential — and ONLY the credential — comes from the controller
    /// pod's own environment, through object_store's documented order. Location,
    /// region, addressing and transport come from the destination:
    /// `StoreOptions`' `Ambient` source reads the NAMED credential variables and
    /// never `AWS_ENDPOINT_URL`, `AWS_REGION`, `AWS_ALLOW_HTTP` or
    /// `AWS_VIRTUAL_HOSTED_STYLE_REQUEST` (W2's `s3_effective`). That is the
    /// difference between this handle and the global one G2 describes.
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

        // THE EVIDENCE LOCATION, NOT THE ARCHIVE ONE. `logweir/` is Global
        // Constraint 6's evidence root, and `Store`'s own prefix guard requires
        // exactly it.
        let location = resolved.evidence_storage();
        let mut options = StoreOptions {
            credentials: CredentialSource::Ambient,
            ..StoreOptions::default()
        };
        if let Some(pem) = resolved.ca_pem.clone() {
            options = options.with_root_certificate(pem);
        }
        let description = resolved.canonical_url.clone();

        // INSIDE `spawn_blocking`, AND THE CONSTRUCTOR IS WHY. `Store::new_rt`
        // builds a current-thread runtime and `build_backend_with` calls
        // `rt.block_on`; doing that on the reconciler's own runtime thread is
        // the *Cannot start a runtime from within a runtime* panic that
        // interface I13 exists to prevent. `read_only_with` and never
        // `from_url_with`: guard G-RET, and the handle this returns physically
        // cannot put.
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
                        // REDACTED: an object-store error can quote an S3 XML
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
