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

    pub fn get(&self, key: &str) -> Result<(Vec<u8>, Option<String>), EngineError> {
        let rt = &self.rt;
        rt.block_on(async {
            let r = self
                .inner
                .get(&OPath::from(key))
                .await
                .map_err(|e| EngineError::Operational(format!("{key}: {e}")))?;
            let vid = r.meta.version.clone();
            let b = r
                .bytes()
                .await
                .map_err(|e| EngineError::Operational(format!("{key}: {e}")))?;
            Ok((b.to_vec(), vid))
        })
    }

    /// Every key under `prefix` ending `/manifest.json` — exactly what the CLI's
    /// `list` scans (GT-10).
    pub fn list_manifest_keys(&self, prefix: &str) -> Result<Vec<String>, EngineError> {
        use futures::StreamExt as _;
        let rt = &self.rt;
        rt.block_on(async {
            let mut out = Vec::new();
            let mut st = self.inner.list(Some(&OPath::from(prefix)));
            while let Some(m) = st.next().await {
                let m = m.map_err(|e| EngineError::Operational(e.to_string()))?;
                let k = m.location.to_string();
                if k.ends_with("/manifest.json") {
                    out.push(k);
                }
            }
            Ok(out)
        })
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

    /// The Interfaces-block method Task 12's `fingerprints()` calls. Resolves
    /// `topic`/`partition` against every manifest under this store's prefix and
    /// returns the segment keys whose [start_timestamp, end_timestamp] overlaps
    /// `window`, delegating the overlap test to `segment_keys_from`.
    ///
    /// Parsed as `serde_json::Value` rather than `crate::vendored::manifest::
    /// BackupManifest`: Task 12b executes BEFORE Task 12, so the vendored types
    /// do not exist yet. The three field paths read here — topics[].name,
    /// .partitions[].partition_id, .segments[].{key,start_timestamp,
    /// end_timestamp} — are the same ones Task 12's `describe()` maps.
    pub fn segment_keys_for(
        &self,
        topic: &str,
        partition: i32,
        window: (i64, i64),
    ) -> Result<Vec<String>, EngineError> {
        let mut segs: Vec<(String, i64, i64)> = Vec::new();
        for mk in self.list_manifest_keys(&self.prefix)? {
            let (bytes, _) = self.get(&mk)?;
            let v: serde_json::Value = serde_json::from_slice(&bytes)
                .map_err(|e| EngineError::Operational(format!("{mk}: {e}")))?;
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
                                "{mk}: segment entry missing key/start_timestamp/end_timestamp"
                            )));
                        };
                        segs.push((k.to_string(), t0, t1));
                    }
                }
            }
        }
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
                    Err(object_store::Error::NotSupported { .. }) => {}
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
