//! The only place Logweir touches object storage. object_store 0.14 with
//! features ["aws","azure","gcp","http"] — the same crate, version and feature
//! set OSO uses (Global Constraint 9), so a bucket OSO can read, we can read.
//!
//! CREDENTIALS: `AmazonS3Builder::from_env()` applies object_store's OWN chain
//! (static keys, then web identity / IRSA, ECS, EKS Pod Identity, IMDS). That is
//! NOT the AWS SDK chain: `~/.aws/credentials` profiles, `AWS_PROFILE` and SSO
//! are unsupported. docs/stability.md states this in one sentence, because an
//! adopter discovering it at drill time is a support ticket.
use logweir_core::engine::{BackupSetRef, EngineError, StorageUrl};
use object_store::path::Path as OPath;
use object_store::{ObjectStore, ObjectStoreExt as _, PutMode, PutOptions};
use std::sync::Arc;

#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    #[error("object already exists at {0} — refusing to overwrite evidence")]
    AlreadyExists(String),
    /// Distinguished from `Io` so a caller can tell "definitely absent" from
    /// "could not tell" (permission denied, a timeout, a truncated read, a
    /// transient network error). Task 12 fix: `describe()`'s sibling
    /// consumer-groups-snapshot read used to collapse every `get` failure
    /// into `None` ("no snapshot"), which reported a 403 or a dropped
    /// connection identically to genuine absence — a positive claim
    /// (`consumer_group_snapshot_sha256: None`) the store never actually
    /// established, which then flows into the signed scorecard.
    #[error("object not found at {0}")]
    NotFound(String),
    #[error("storage: {0}")]
    Io(String),
    #[error("unsupported storage backend `{0}`")]
    Backend(String),
    /// Controller amendment: the handle returned by `read_only_from_url`
    /// physically cannot put. Every put method checks this before it checks
    /// anything else — including before the `LOGWEIR_ROOT` assertion — so a
    /// read-only handle constructed over the OSO archive prefix (which is
    /// exactly the case `read_only_from_url` exists to allow) can never reach
    /// a codepath that writes.
    #[error("store is read-only — refusing to put {0}")]
    ReadOnly(String),
}

impl From<StoreError> for EngineError {
    fn from(e: StoreError) -> Self {
        EngineError::Operational(e.to_string())
    }
}

#[derive(Debug, Clone)]
pub struct PutOutcome {
    pub version_id: Option<String>,
    /// false when the backend answered `Unsupported` to `PutMode::Create` and we
    /// fell back to HEAD-then-PUT. Recorded honestly into
    /// `evidence.create_only_enforced`; never assumed true.
    pub create_only_enforced: bool,
}

/// What a provider actually reported about an object's WORM retention. Only
/// ever constructed from a readback — never inferred from bucket settings, and
/// never defaulted — so `evidence.immutable` in a signed scorecard can be
/// `true` only when a provider said so (spec §6 C3).
#[derive(Debug, Clone)]
pub struct LockInfo {
    pub immutable: bool,
    pub retain_until: Option<chrono::DateTime<chrono::Utc>>,
}

/// The ONLY key root Logweir may write under (Global Constraint 6). Fixed in
/// code, never taken from a spec, so `put_create_only`'s guard cannot be
/// widened by an adopter's configuration.
pub const LOGWEIR_ROOT: &str = "logweir/";

pub struct Store {
    inner: Arc<dyn ObjectStore>,
    prefix: String,
    /// Set false by `in_memory_without_conditional_put` and by the runtime
    /// fallback, so the flag the scorecard publishes is observed, not declared.
    conditional_put: bool,
    /// Built once per store and reused. See `new_rt` for why per-call runtimes
    /// break connection reuse on every network backend.
    rt: Arc<tokio::runtime::Runtime>,
    /// Controller amendment: true only for handles returned by
    /// `read_only_from_url`. A handle that physically cannot put is a
    /// stronger guarantee than a prefix check, and it keeps the write path's
    /// `LOGWEIR_ROOT` guard (in `from_url` and in `put_create_only`) intact —
    /// this flag never widens or bypasses that guard, it only ever adds an
    /// earlier, unconditional refusal in front of it.
    read_only: bool,
}

impl Store {
    /// One arm per `StorageUrl` variant, matching upstream's own
    /// `StorageBackendConfig` shape [VERIFIED
    /// U/kafka-backup/crates/kafka-backup-core/src/storage/config.rs:14-105].
    /// The client is built INSIDE `rt.block_on`, so the client and its
    /// connection pool belong to the runtime that will later drive them.
    /// Shared by `from_url` and `read_only_from_url` — the two constructors
    /// differ only in whether the `LOGWEIR_ROOT` guard runs, never in how the
    /// backend itself is built.
    fn build_backend(
        u: &StorageUrl,
        rt: &tokio::runtime::Runtime,
    ) -> Result<Arc<dyn ObjectStore>, StoreError> {
        rt.block_on(async {
            let r: Result<Arc<dyn ObjectStore>, StoreError> = match u {
                StorageUrl::S3 {
                    bucket,
                    region,
                    endpoint,
                    path_style,
                    allow_http,
                    ..
                } => {
                    let mut b = object_store::aws::AmazonS3Builder::from_env()
                        .with_bucket_name(bucket)
                        .with_virtual_hosted_style_request(!*path_style)
                        .with_allow_http(*allow_http);
                    if let Some(r) = region {
                        b = b.with_region(r);
                    }
                    if let Some(e) = endpoint {
                        b = b.with_endpoint(e);
                    }
                    Ok(Arc::new(
                        b.build().map_err(|e| StoreError::Io(e.to_string()))?,
                    ))
                }
                StorageUrl::Azure {
                    account_name,
                    container_name,
                    ..
                } => Ok(Arc::new(
                    object_store::azure::MicrosoftAzureBuilder::from_env()
                        .with_account(account_name)
                        .with_container_name(container_name)
                        .build()
                        .map_err(|e| StoreError::Io(e.to_string()))?,
                )),
                StorageUrl::Gcs { bucket, .. } => Ok(Arc::new(
                    object_store::gcp::GoogleCloudStorageBuilder::from_env()
                        .with_bucket_name(bucket)
                        .build()
                        .map_err(|e| StoreError::Io(e.to_string()))?,
                )),
                StorageUrl::Filesystem { path } => Ok(Arc::new(
                    object_store::local::LocalFileSystem::new_with_prefix(path)
                        .map_err(|e| StoreError::Io(e.to_string()))?,
                )),
            };
            r
        })
    }

    /// The write-path constructor. Enforces Global Constraint 6 at
    /// construction rather than only at the put: a spec naming a prefix
    /// outside `logweir/` is a phase-0 refusal, not a panic in the middle of
    /// a signed upload.
    pub fn from_url(u: &StorageUrl) -> Result<Self, StoreError> {
        let rt = Self::new_rt();
        let prefix = u.prefix().to_string();
        if !prefix.starts_with(LOGWEIR_ROOT) && !matches!(u, StorageUrl::Filesystem { .. }) {
            return Err(StoreError::Backend(format!(
                "evidence prefix `{prefix}` must start with `{LOGWEIR_ROOT}` (Global Constraint 6)"
            )));
        }
        let inner = Self::build_backend(u, &rt)?;
        Ok(Self {
            inner,
            prefix,
            conditional_put: true,
            rt,
            read_only: false,
        })
    }

    /// Controller amendment (added after the addenda pass): the read-path
    /// constructor for reading the OSO archive. Skips the `LOGWEIR_ROOT`
    /// prefix guard — the archive prefix (e.g. `kafka-backups/daily`) is never
    /// under `logweir/`, and `from_url` would refuse to construct a `Store`
    /// over it, which would make `list_backup_sets`/`describe`/
    /// `segment_keys_for` unable to run at all.
    ///
    /// The handle this returns physically cannot put: `read_only` is set
    /// `true` here and `put_create_only` checks it FIRST, before the
    /// `LOGWEIR_ROOT` assertion, so this constructor never becomes a second
    /// way to write outside `logweir/`. `from_url`'s guard is unchanged and
    /// stays the only way to build a *writable* store.
    pub fn read_only_from_url(u: &StorageUrl) -> Result<Self, StoreError> {
        let rt = Self::new_rt();
        let prefix = u.prefix().to_string();
        let inner = Self::build_backend(u, &rt)?;
        Ok(Self {
            inner,
            prefix,
            conditional_put: true,
            rt,
            read_only: true,
        })
    }

    pub fn in_memory(prefix: &str) -> Self {
        Self {
            inner: Arc::new(object_store::memory::InMemory::new()),
            prefix: prefix.to_string(),
            conditional_put: true,
            rt: Self::new_rt(),
            read_only: false,
        }
    }

    pub fn in_memory_without_conditional_put(prefix: &str) -> Self {
        Self {
            conditional_put: false,
            ..Self::in_memory(prefix)
        }
    }

    /// ONE runtime for the life of the store, built in every constructor and
    /// held in `self.rt`. `object_store` 0.14's AmazonS3/MicrosoftAzure/
    /// GoogleCloudStorage backends hold an HTTP client whose connection pool and
    /// hyper background tasks are bound to the runtime that drove them; building
    /// and DROPPING a runtime per call orphans those pooled connections, so
    /// every later call pays a fresh TCP+TLS handshake and can observe
    /// `connection closed before message completed` on a reused pool entry.
    /// The InMemory suite cannot surface this — it does no I/O — which is why
    /// the assertion lives in Task 20 step 0's MinIO leg as well.
    fn new_rt() -> Arc<tokio::runtime::Runtime> {
        Arc::new(
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("current-thread runtime"),
        )
    }

    /// Returns `StoreError::NotFound` specifically when the object genuinely
    /// does not exist, distinct from every other failure mode (`Io`) — see
    /// `StoreError::NotFound`'s doc comment for why the distinction exists.
    /// `EngineError: From<StoreError>` makes every existing `?`-based caller
    /// of this method (which all want a plain operational failure) unaffected
    /// by this signature; `describe()`'s sibling-snapshot read is the one
    /// caller that inspects the variant directly.
    pub fn get(&self, key: &str) -> Result<(Vec<u8>, Option<String>), StoreError> {
        let rt = &self.rt;
        rt.block_on(async {
            let r = self
                .inner
                .get(&OPath::from(key))
                .await
                .map_err(|e| match e {
                    object_store::Error::NotFound { .. } => StoreError::NotFound(key.to_string()),
                    other => StoreError::Io(format!("{key}: {other}")),
                })?;
            let vid = r.meta.version.clone();
            let b = r
                .bytes()
                .await
                .map_err(|e| StoreError::Io(format!("{key}: {e}")))?;
            Ok((b.to_vec(), vid))
        })
    }

    /// Every key under `prefix`, sorted, with no filter of any kind. Added for
    /// Task 20 phase 8, which reads the engine's own validation report out of
    /// the per-run prefix Logweir itself set in `validation.yaml`:
    /// `list_manifest_keys` cannot serve that read because its
    /// `/manifest.json` filter would return nothing there.
    ///
    /// Sorted so a caller taking `keys[0]` gets a deterministic answer rather
    /// than whatever order the backend happened to stream.
    ///
    /// The sort is DEFENSIVE and, honestly, unproven: `object_store` 0.14
    /// contracts no list ordering across backends, but both in-process
    /// backends this workspace can build (`InMemory`, which is a `BTreeMap`,
    /// and `LocalFileSystem`) happen to return keys already ordered. So no
    /// test here can distinguish "sorted by this line" from "sorted by the
    /// backend" — deleting `out.sort()` leaves the suite green (Task 20 fix
    /// round 1, mutant Z4). It is kept because phase 8 takes `keys[0]` and a
    /// backend that streamed in arbitrary order would otherwise make WHICH
    /// engine report is retained non-deterministic; it is documented as
    /// unproven rather than asserted as a tested guarantee.
    pub fn list_keys(&self, prefix: &str) -> Result<Vec<String>, EngineError> {
        use futures::StreamExt as _;
        let rt = &self.rt;
        rt.block_on(async {
            let mut out = Vec::new();
            let mut st = self.inner.list(Some(&OPath::from(prefix)));
            while let Some(m) = st.next().await {
                let m = m.map_err(|e| EngineError::Operational(e.to_string()))?;
                out.push(m.location.to_string());
            }
            out.sort();
            Ok(out)
        })
    }

    /// Every key under `prefix` ending `/manifest.json` — exactly what the CLI's
    /// `list` scans (GT-10). Expressed as `list_keys` plus the filter, so the
    /// two can never disagree about what "under this prefix" means.
    pub fn list_manifest_keys(&self, prefix: &str) -> Result<Vec<String>, EngineError> {
        Ok(self
            .list_keys(prefix)?
            .into_iter()
            .filter(|k| k.ends_with("/manifest.json"))
            .collect())
    }

    /// The provider's Object Lock state for `key`, or `None` when the backend
    /// exposes no such readback.
    ///
    /// `object_store` 0.14 — the crate, version and feature set Global
    /// Constraint 9 fixes — models no Object Lock / WORM retention API at all,
    /// on any of its `aws`/`azure`/`gcp`/`http` backends. So this returns
    /// `None` on every backend Logweir can currently build, and phase 8
    /// therefore publishes `evidence.immutable: false`. That is the point:
    /// spec §6 C3 allows `immutable: true` ONLY after a provider readback, so
    /// the absence of a readback must produce an honest `false` rather than an
    /// optimistic guess derived from the bucket's configuration or the
    /// adopter's say-so. If a future object_store exposes the readback, this
    /// method is the one place that changes.
    pub fn object_lock_readback(&self, key: &str) -> Option<LockInfo> {
        let _ = key;
        None
    }

    pub fn list_manifests(&self, loc: &StorageUrl) -> Result<Vec<BackupSetRef>, EngineError> {
        Ok(self
            .list_manifest_keys(loc.prefix())?
            .into_iter()
            .map(|k| BackupSetRef {
                backup_id: k
                    .trim_end_matches("/manifest.json")
                    .rsplit('/')
                    .next()
                    .unwrap_or("")
                    .to_string(),
                manifest_key: k,
            })
            .collect())
    }

    /// Pure window filter, extracted so it is testable with no backend.
    pub fn segment_keys_from(&self, segs: &[(String, i64, i64)], w: (i64, i64)) -> Vec<String> {
        segs.iter()
            .filter(|(_, t0, t1)| *t0 <= w.1 && *t1 >= w.0)
            .map(|(k, _, _)| k.clone())
            .collect()
    }

    /// Qualifies a manifest-relative key into the fully-qualified key
    /// `get`/`list_manifest_keys` operate on.
    ///
    /// Upstream stores every key a manifest carries — `manifest_key` itself
    /// (`{backup_id}/manifest.json` [VERIFIED
    /// U/kafka-backup/crates/kafka-backup-core/src/backup/engine.rs:1599]) and
    /// every segment key (`{backup_id}/topics/{topic}/partition={n}/
    /// segment-{offset:020}.bin{ext}` [VERIFIED .../backup/engine.rs:1436-1442])
    /// — RELATIVE to the configured prefix, prepending that prefix only at the
    /// storage boundary (`S3Backend::full_path` [VERIFIED
    /// .../storage/s3.rs:101-104]: `format!("{}/{}", prefix.trim_end_matches('/'),
    /// key)`). `list_manifest_keys` and `get` in THIS file already operate in
    /// the fully-qualified space — the `prefix` handed to `object_store::list`
    /// IS the search root — so a key read out of a manifest BODY must be
    /// qualified the same way before `get` can resolve it. Fixed after a
    /// review found `segment_keys_for` passing the manifest's relative key
    /// straight through: a real archive at `s3://bucket` with a non-empty
    /// `prefix` would have every returned segment key 404 in `get`, reporting
    /// a present backup's segments as missing.
    fn qualify(&self, relative_key: &str) -> String {
        if self.prefix.is_empty() {
            relative_key.to_string()
        } else {
            format!("{}/{}", self.prefix.trim_end_matches('/'), relative_key)
        }
    }

    /// Resolves `topic`/`partition` against ONE manifest's body and returns
    /// its (qualified key, start_timestamp, end_timestamp) triples,
    /// unfiltered by any time window. Shared by `segment_keys_for` (scans
    /// every manifest under the prefix — kept for callers that genuinely want
    /// every backup set, none as of this crate) and `segment_keys_for_set`
    /// (reads exactly the one manifest a caller names — what
    /// `OsoCliEngine::fingerprints` uses, per the Task 12 fix that closed the
    /// cross-set merging hole: two backup sets sharing a topic/partition with
    /// an overlapping window used to merge silently here).
    ///
    /// Parsed as `serde_json::Value` rather than `crate::vendored::manifest::
    /// BackupManifest`: Task 12b executes BEFORE Task 12, so the vendored types
    /// do not exist yet. The three field paths read here — topics[].name,
    /// .partitions[].partition_id, .segments[].{key,start_timestamp,
    /// end_timestamp} — are the same ones Task 12's `describe()` maps. Each
    /// `key` is manifest-relative (see `qualify`) and is qualified into this
    /// store's key space before being returned, so the caller can `get` it
    /// directly.
    fn segments_in_manifest(
        &self,
        manifest_key: &str,
        topic: &str,
        partition: i32,
    ) -> Result<Vec<(String, i64, i64)>, EngineError> {
        let mut segs: Vec<(String, i64, i64)> = Vec::new();
        let (bytes, _) = self.get(manifest_key)?;
        let v: serde_json::Value = serde_json::from_slice(&bytes)
            .map_err(|e| EngineError::Operational(format!("{manifest_key}: {e}")))?;
        let topics = v
            .get("topics")
            .and_then(|t| t.as_array())
            .map(|a| a.as_slice())
            .unwrap_or(&[]);
        for t in topics {
            if t.get("name").and_then(|n| n.as_str()) != Some(topic) {
                continue;
            }
            let parts = t
                .get("partitions")
                .and_then(|p| p.as_array())
                .map(|a| a.as_slice())
                .unwrap_or(&[]);
            for p in parts {
                if p.get("partition_id").and_then(|i| i.as_i64()) != Some(partition as i64) {
                    continue;
                }
                let ss = p
                    .get("segments")
                    .and_then(|s| s.as_array())
                    .map(|a| a.as_slice())
                    .unwrap_or(&[]);
                for s in ss {
                    let (Some(k), Some(t0), Some(t1)) = (
                        s.get("key").and_then(|k| k.as_str()),
                        s.get("start_timestamp").and_then(|x| x.as_i64()),
                        s.get("end_timestamp").and_then(|x| x.as_i64()),
                    ) else {
                        return Err(EngineError::Operational(format!(
                            "{manifest_key}: segment entry missing key/start_timestamp/end_timestamp"
                        )));
                    };
                    segs.push((self.qualify(k), t0, t1));
                }
            }
        }
        Ok(segs)
    }

    /// The Interfaces-block method from Task 12b's brief. Resolves
    /// `topic`/`partition` against EVERY manifest under this store's prefix.
    /// No caller in this workspace uses this anymore as of the Task 12 fix
    /// (see `segment_keys_for_set`) — kept because it is part of Task 12b's
    /// committed, tested Interfaces contract, and a future caller that
    /// genuinely wants a cross-set view (e.g. an archive-wide audit) has a
    /// real use for it. `OsoCliEngine::fingerprints` MUST NOT call this one.
    pub fn segment_keys_for(
        &self,
        topic: &str,
        partition: i32,
        window: (i64, i64),
    ) -> Result<Vec<String>, EngineError> {
        let mut segs: Vec<(String, i64, i64)> = Vec::new();
        for mk in self.list_manifest_keys(&self.prefix)? {
            segs.extend(self.segments_in_manifest(&mk, topic, partition)?);
        }
        let mut out = self.segment_keys_from(&segs, window);
        out.sort();
        out.dedup();
        Ok(out)
    }

    /// Task 12 fix (post-review): the set-scoped sibling of `segment_keys_for`.
    /// Reads exactly the ONE manifest named by `manifest_key` — never lists or
    /// touches any other manifest under this store's prefix — so two backup
    /// sets sharing a topic/partition with an overlapping window can no
    /// longer merge: `fingerprints()` calls this, passing
    /// `sel.set.manifest_key`, instead of `segment_keys_for`.
    pub fn segment_keys_for_set(
        &self,
        manifest_key: &str,
        topic: &str,
        partition: i32,
        window: (i64, i64),
    ) -> Result<Vec<String>, EngineError> {
        let segs = self.segments_in_manifest(manifest_key, topic, partition)?;
        let mut out = self.segment_keys_from(&segs, window);
        out.sort();
        out.dedup();
        Ok(out)
    }

    /// Global Constraint 6: `PutMode::Create` everywhere, under `logweir/` only.
    pub fn put_create_only(&self, key: &str, bytes: &[u8]) -> Result<PutOutcome, StoreError> {
        // Controller amendment: checked before anything else, including the
        // LOGWEIR_ROOT assertion below. A handle from `read_only_from_url` is
        // built over the OSO archive prefix precisely so it CAN read that
        // prefix, so it must never reach an assertion that would panic on
        // that same prefix — it must simply refuse to put, every time.
        if self.read_only {
            return Err(StoreError::ReadOnly(key.to_string()));
        }
        // NOT `key.starts_with(&self.prefix)` as the only test: `self.prefix`
        // is caller-supplied from the evidence StorageUrl, so that disjunct is
        // satisfied by ANY prefix a spec names — including the OSO archive
        // prefix, which is the one outcome Global Constraint 6 exists to
        // prevent. The sanctioned root is fixed in code and checked FIRST.
        assert!(
            key.starts_with(LOGWEIR_ROOT),
            "Global Constraint 6: logweir writes only under `{LOGWEIR_ROOT}`, got `{key}`"
        );
        assert!(
            key.starts_with(&self.prefix),
            "key `{key}` escapes this store's configured prefix `{}`",
            self.prefix
        );
        let rt = &self.rt;
        rt.block_on(async {
            let p = OPath::from(key);
            let payload = object_store::PutPayload::from(bytes.to_vec());
            if self.conditional_put {
                let opts = PutOptions {
                    mode: PutMode::Create,
                    ..Default::default()
                };
                match self.inner.put_opts(&p, payload.clone(), opts).await {
                    Ok(r) => {
                        return Ok(PutOutcome {
                            version_id: r.version,
                            create_only_enforced: true,
                        })
                    }
                    Err(object_store::Error::AlreadyExists { .. }) => {
                        return Err(StoreError::AlreadyExists(key.to_string()))
                    }
                    // The backend does not implement conditional put. Fall
                    // through to HEAD-then-PUT and RECORD that we did.
                    //
                    // Two distinct object_store variants mean this, not one:
                    // `NotSupported` is what a backend that never implements
                    // conditional put at all would return (object_store 0.14.1
                    // only actually produces it for copy-if-not-exists
                    // [VERIFIED object_store-0.14.1/src/aws/mod.rs:399]).
                    // `NotImplemented` is what `AmazonS3` ACTUALLY returns for
                    // `PutMode::Create` when `AWS_CONDITIONAL_PUT=disabled` (or
                    // the equivalent builder config) — reachable through
                    // `AmazonS3Builder::from_env()` on a real S3-compatible
                    // endpoint [VERIFIED
                    // object_store-0.14.1/src/aws/mod.rs:186-192]. Without this
                    // arm the put fails closed with `StoreError::Io` instead of
                    // falling back — safe, but it means
                    // `create_only_enforced: false` could only ever be
                    // observed against the in-memory test double, never
                    // against the real backend this fallback exists for.
                    Err(object_store::Error::NotSupported { .. })
                    | Err(object_store::Error::NotImplemented { .. }) => {}
                    Err(e) => return Err(StoreError::Io(e.to_string())),
                }
            }
            if self.inner.head(&p).await.is_ok() {
                return Err(StoreError::AlreadyExists(key.to_string()));
            }
            let r = self
                .inner
                .put(&p, payload)
                .await
                .map_err(|e| StoreError::Io(e.to_string()))?;
            Ok(PutOutcome {
                version_id: r.version,
                create_only_enforced: false,
            })
        })
    }
}
