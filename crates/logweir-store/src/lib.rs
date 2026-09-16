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
    /// The object at this key parsed as JSON but is not a backup manifest:
    /// it declares no `topics` array at all.
    ///
    /// TASK 13 REVIEW CARRY, DISCHARGED HERE. `manifest_facts` used to answer
    /// `{"hello":"world"}` and `{"topics":5}` with
    /// `Backend("… declares no segment, so it bounds no window")` — the same
    /// error a REAL manifest describing an empty backup set gets. Those two
    /// facts are not the same fact, and collapsing them is the
    /// `NotFound`-versus-`Io` defect this enum's own doc comments already
    /// argue about: "this is not a manifest" is a configuration error (the
    /// prefix points at the wrong thing, or a sibling JSON object was picked
    /// up by the `/manifest.json` filter), while "this manifest bounds no
    /// window" is a fact about a backup set that really exists. A caller —
    /// and a reader of a controller log — needs to be able to tell them
    /// apart, so `manifest_facts` now returns THIS for the first and keeps
    /// `Backend` for the second.
    ///
    /// The second field carries what was wrong, so the message names the key
    /// AND the reason rather than only one of them.
    #[error("{0} is not a backup manifest: {1}")]
    NotAManifest(String, String),
}

/// The `backup_id` a manifest key belongs to: the key's parent directory.
///
/// TASK 13 REVIEW CARRY, DISCHARGED HERE. This derivation was copy-pasted in
/// two places — `Store::list_manifests` and `Store::manifest_facts` — and the
/// second one's doc comment promised it was "derived exactly as
/// `list_manifests` derives it, so the two can never disagree about which
/// backup set a manifest belongs to". A promise kept by two copies of four
/// chained iterator calls is a promise one edit breaks silently, and
/// `manifest_facts`'s `backup_id` is the string the retention report's
/// rendered `aws s3 rm` command names. It is one function now, both callers
/// go through it, and `the_backup_id_derivation_is_one_function` asserts the
/// two agree over every shape that reaches either.
///
/// `pub` because `weirkeeper`'s retention reconciler lists manifest KEYS
/// (`list_manifest_keys`, the same call `list_manifests` is written on top of)
/// and must name the sets by the same rule.
#[must_use]
pub fn backup_id_from_manifest_key(key: &str) -> String {
    key.trim_end_matches("/manifest.json")
        .rsplit('/')
        .next()
        .unwrap_or("")
        .to_string()
}

/// The JSON type name of `v`, for an error message that says what was found
/// rather than only what was expected. The VALUE is never quoted: a manifest
/// body is an adopter's data and this string reaches a controller log.
fn kind_of(v: &serde_json::Value) -> &'static str {
    match v {
        serde_json::Value::Null => "null",
        serde_json::Value::Bool(_) => "a boolean",
        serde_json::Value::Number(_) => "a number",
        serde_json::Value::String(_) => "a string",
        serde_json::Value::Array(_) => "an array",
        serde_json::Value::Object(_) => "an object",
    }
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

/// The covered window one manifest declares, plus the backup set it belongs
/// to. Interface **I12**, the only new type in the `logweir-store`
/// extraction.
///
/// It exists because `list_manifests` returns `Vec<BackupSetRef>` and
/// `BackupSetRef` is `{ backup_id, manifest_key }` — derived from the key
/// string alone, with no object read, so **there is no timestamp anywhere in
/// the returned type**. The only structure carrying one is `BackupSetFacts`,
/// produced by `OsoCliEngine::describe`, in the crate this extraction exists
/// to keep out of the control plane. A retention reconciler needs a window
/// and must not link that crate to get one.
#[derive(Debug, Clone)]
pub struct ManifestFacts {
    pub backup_id: String,
    pub newest_record_ms: i64,
    pub oldest_record_ms: i64,
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
    ///
    /// THE PREFIX MUST BE EXACTLY `logweir/`, not merely start with it, and
    /// that is a fix. `starts_with` admitted `logweir/prod/` — a legal-looking
    /// prefix that `docs/quickstart.md` and `examples/drill.yaml` both already
    /// described as refused — while every key builder in this crate is
    /// hard-coded to `logweir/drills/…`. The result was `assert!(key
    /// .starts_with(&self.prefix))` firing in `put_create_only`, AFTER the
    /// restore had already run: a panic, exit 101, outside the five-code exit
    /// contract entirely, on the one path where the drill had already touched
    /// the operator's cluster. Refusing at construction puts it back inside
    /// the contract, and this constructor's own doc comment above already
    /// promised exactly that.
    ///
    /// `Filesystem` is exempt because it HAS no prefix — `StorageUrl::prefix`
    /// returns `""` for it, since upstream's variant carries only `path` — and
    /// its keys still go through the `LOGWEIR_ROOT` assertion in
    /// `put_create_only`, so objects still land under `<path>/logweir/`. The
    /// exemption is about a field that does not exist, not about the rule.
    pub fn from_url(u: &StorageUrl) -> Result<Self, StoreError> {
        let rt = Self::new_rt();
        let prefix = u.prefix().to_string();
        Self::assert_evidence_prefix(u, &prefix)?;
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

    /// The EXPLICIT write-path constructor (D2 W2). Same `LOGWEIR_ROOT`
    /// guard as [`Store::from_url`], and the same handle; what differs is
    /// that every addressing, transport and credential decision comes from
    /// the arguments rather than from the process environment.
    pub fn from_url_with(u: &StorageUrl, opts: &StoreOptions) -> Result<Self, StoreError> {
        let rt = Self::new_rt();
        let prefix = u.prefix().to_string();
        Self::assert_evidence_prefix(u, &prefix)?;
        let inner = Self::build_backend_with(u, opts, &rt)?;
        Ok(Self {
            inner,
            prefix,
            conditional_put: true,
            rt,
            read_only: false,
        })
    }

    /// The EXPLICIT read-path constructor (D2 W2), the sibling of
    /// [`Store::read_only_from_url`]. The handle it returns physically cannot
    /// put, for the same reason and by the same flag.
    ///
    /// This is what the controller's allowlisted `ControllerIdentity`
    /// evidence cache builds (D2 §3.10) and what a check Job's archive and
    /// evidence reads use.
    pub fn read_only_with(u: &StorageUrl, opts: &StoreOptions) -> Result<Self, StoreError> {
        let rt = Self::new_rt();
        let prefix = u.prefix().to_string();
        let inner = Self::build_backend_with(u, opts, &rt)?;
        Ok(Self {
            inner,
            prefix,
            conditional_put: true,
            rt,
            read_only: true,
        })
    }

    /// ONE builder, driven by [`s3_effective`]'s decision.
    ///
    /// Every value below is READ OFF `eff`, never recomputed here, so the
    /// configuration a test inspects with [`s3_effective`] is the
    /// configuration the client gets. `AmazonS3Builder::from_env()` is called
    /// ONLY for [`CredentialSource::Ambient`]; every other source starts from
    /// `AmazonS3Builder::new()`, so no `AWS_*` variable can reach the client
    /// except the named ones `s3_effective` itself resolved.
    fn build_backend_with(
        u: &StorageUrl,
        opts: &StoreOptions,
        rt: &tokio::runtime::Runtime,
    ) -> Result<Arc<dyn ObjectStore>, StoreError> {
        let eff = s3_effective(u, opts)?;
        let mut client = object_store::ClientOptions::default().with_allow_http(eff.allow_http);
        for pem in &opts.root_certificates {
            for cert in object_store::Certificate::from_pem_bundle(pem)
                .map_err(|e| StoreError::Backend(format!("root certificate: {e}")))?
            {
                client = client.with_root_certificate(cert);
            }
        }
        if let Some(t) = eff.request_timeout {
            client = client.with_timeout(t).with_connect_timeout(t);
        }
        rt.block_on(async move {
            let mut b = match &opts.credentials {
                CredentialSource::Ambient => object_store::aws::AmazonS3Builder::from_env(),
                _ => object_store::aws::AmazonS3Builder::new(),
            };
            b = b
                .with_bucket_name(&eff.bucket)
                // Applied AFTER `from_env`, so an explicit value wins over
                // `AWS_ALLOW_HTTP` / `AWS_VIRTUAL_HOSTED_STYLE_REQUEST` in the
                // environment (defect SEC-ENVHTTP, D-SEAMS S5).
                .with_virtual_hosted_style_request(eff.virtual_hosted_style)
                .with_allow_http(eff.allow_http)
                .with_client_options(client);
            if let Some(r) = &eff.region {
                b = b.with_region(r);
            }
            if let Some(e) = &eff.endpoint {
                b = b.with_endpoint(e);
            }
            if let Some(m) = &eff.metadata_endpoint {
                b = b.with_metadata_endpoint(m);
            }
            if let Some(n) = eff.max_retries {
                b = b.with_retry(object_store::RetryConfig {
                    max_retries: n,
                    retry_timeout: eff
                        .request_timeout
                        .unwrap_or(std::time::Duration::from_secs(30)),
                    ..Default::default()
                });
            }
            b = match &opts.credentials {
                CredentialSource::Ambient => b,
                CredentialSource::Static {
                    access_key_id,
                    secret_access_key,
                    session_token,
                } => {
                    let mut b = b
                        .with_access_key_id(access_key_id)
                        .with_secret_access_key(secret_access_key);
                    if let Some(t) = session_token {
                        b = b.with_token(t);
                    }
                    b
                }
                CredentialSource::StaticFromEnv => {
                    // `s3_effective` already established that both are set.
                    let mut b = b
                        .with_access_key_id(std::env::var("AWS_ACCESS_KEY_ID").unwrap_or_default())
                        .with_secret_access_key(
                            std::env::var("AWS_SECRET_ACCESS_KEY").unwrap_or_default(),
                        );
                    if let Ok(t) = std::env::var("AWS_SESSION_TOKEN") {
                        if !t.trim().is_empty() {
                            b = b.with_token(t);
                        }
                    }
                    b
                }
                CredentialSource::WorkloadIdentity => match eff
                    .workload_identity
                    .clone()
                    .expect("s3_effective refuses this source without an injected identity")
                {
                    WorkloadIdentity::WebIdentity {
                        token_file,
                        role_arn,
                        session_name,
                        sts_endpoint,
                    } => {
                        let mut b = b
                            .with_config(
                                object_store::aws::AmazonS3ConfigKey::WebIdentityTokenFile,
                                token_file,
                            )
                            .with_config(object_store::aws::AmazonS3ConfigKey::RoleArn, role_arn);
                        if let Some(n) = session_name {
                            b = b.with_config(
                                object_store::aws::AmazonS3ConfigKey::RoleSessionName,
                                n,
                            );
                        }
                        if let Some(e) = sts_endpoint {
                            b = b.with_config(object_store::aws::AmazonS3ConfigKey::StsEndpoint, e);
                        }
                        b
                    }
                    WorkloadIdentity::ContainerFullUri { uri, token_file } => b
                        .with_config(
                            object_store::aws::AmazonS3ConfigKey::ContainerCredentialsFullUri,
                            uri,
                        )
                        .with_config(
                            object_store::aws::AmazonS3ConfigKey::ContainerAuthorizationTokenFile,
                            token_file,
                        ),
                    WorkloadIdentity::ContainerRelativeUri { uri } => b.with_config(
                        object_store::aws::AmazonS3ConfigKey::ContainerCredentialsRelativeUri,
                        uri,
                    ),
                },
            };
            let s3: Arc<dyn ObjectStore> =
                Arc::new(b.build().map_err(|e| StoreError::Io(e.to_string()))?);
            Ok(s3)
        })
    }

    /// Global Constraint 6's construction-time guard, extracted so
    /// [`Store::from_url`] and [`Store::from_url_with`] enforce it by calling
    /// ONE function rather than by carrying two copies of the same message.
    fn assert_evidence_prefix(u: &StorageUrl, prefix: &str) -> Result<(), StoreError> {
        if prefix != LOGWEIR_ROOT && !matches!(u, StorageUrl::Filesystem { .. }) {
            return Err(StoreError::Backend(format!(
                "evidence prefix `{prefix}` must be exactly `{LOGWEIR_ROOT}` (Global \
                 Constraint 6). Logweir builds every evidence key as \
                 `{LOGWEIR_ROOT}drills/<run_id>…`, so a deeper prefix such as \
                 `{LOGWEIR_ROOT}prod/` names a location nothing would ever be written \
                 to; put the environment in the BUCKET, not in the prefix."
            )));
        }
        Ok(())
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
                backup_id: backup_id_from_manifest_key(&k),
                manifest_key: k,
            })
            .collect())
    }

    /// The covered window ONE manifest declares, read out of the manifest
    /// BODY. Interface **I12**, for the retention reconciler (spec §5, guard
    /// G-RET), which runs in `weirkeeper` and therefore cannot call
    /// `OsoCliEngine::describe`.
    ///
    /// An untyped `serde_json::Value` read of the manifest body's
    /// covered-window fields. Links NO vendored upstream struct — the same
    /// untyped read, for the same reason, that `segments_in_manifest` states
    /// in its own doc comment further down this file. The field paths are
    /// `topics[].partitions[].segments[].{start_timestamp,end_timestamp}`,
    /// two of the three that method already reads.
    ///
    /// `newest_record_ms` is the MAXIMUM `end_timestamp` and `oldest_record_ms`
    /// the MINIMUM `start_timestamp` over every segment of every partition of
    /// every topic — the whole manifest, unfiltered, because a backup set's
    /// window is the union of its segments' windows and not any one topic's.
    /// Both come from the BODY: the key carries no timestamp, so deriving
    /// either from the key string would report `0`.
    ///
    /// `backup_id` is the manifest key's parent directory, derived by the ONE
    /// function [`backup_id_from_manifest_key`] that `list_manifests` also
    /// calls, so the two cannot disagree about which backup set a manifest
    /// belongs to. (Task 13 review carry: it used to be two copies of the same
    /// four chained calls, under a doc comment promising they agreed.)
    ///
    /// TWO DIFFERENT FAILURES, TWO DIFFERENT ERRORS (Task 13 review carry).
    /// A body that carries no `topics` array — `{"hello":"world"}`, or
    /// `{"topics":5}` where the key exists but is not an array — is not a
    /// manifest at all, and answers [`StoreError::NotAManifest`]. A body that
    /// IS manifest-shaped but whose segments bound no window answers
    /// `Backend`, because "this backup set declares no segment" is a fact
    /// about a real set and not a configuration mistake. Collapsing the two,
    /// which is what this method did, reported a prefix pointing at the wrong
    /// objects identically to an empty backup set.
    ///
    /// A manifest with no segment bounds no window, and saying so is the only
    /// honest answer: an empty min/max would be published as a real window.
    pub fn manifest_facts(&self, key: &str) -> Result<ManifestFacts, StoreError> {
        let (bytes, _) = self.get(key)?;
        let v: serde_json::Value =
            serde_json::from_slice(&bytes).map_err(|e| StoreError::Io(format!("{key}: {e}")))?;
        let mut newest: Option<i64> = None;
        let mut oldest: Option<i64> = None;
        let Some(topics) = v.get("topics").map(|t| {
            t.as_array()
                .map(|a| a.as_slice())
                .ok_or_else(|| format!("`topics` is {}, not an array", kind_of(t)))
        }) else {
            return Err(StoreError::NotAManifest(
                key.to_string(),
                "it declares no `topics` key".to_string(),
            ));
        };
        let topics = topics.map_err(|why| StoreError::NotAManifest(key.to_string(), why))?;
        for t in topics {
            let parts = t
                .get("partitions")
                .and_then(|p| p.as_array())
                .map(|a| a.as_slice())
                .unwrap_or(&[]);
            for p in parts {
                let ss = p
                    .get("segments")
                    .and_then(|s| s.as_array())
                    .map(|a| a.as_slice())
                    .unwrap_or(&[]);
                for s in ss {
                    let (Some(t0), Some(t1)) = (
                        s.get("start_timestamp").and_then(|x| x.as_i64()),
                        s.get("end_timestamp").and_then(|x| x.as_i64()),
                    ) else {
                        return Err(StoreError::Backend(format!(
                            "{key}: segment entry missing start_timestamp/end_timestamp"
                        )));
                    };
                    oldest = Some(oldest.map_or(t0, |o: i64| o.min(t0)));
                    newest = Some(newest.map_or(t1, |n: i64| n.max(t1)));
                }
            }
        }
        let (Some(oldest_record_ms), Some(newest_record_ms)) = (oldest, newest) else {
            return Err(StoreError::Backend(format!(
                "{key}: manifest declares no segment, so it bounds no window"
            )));
        };
        Ok(ManifestFacts {
            backup_id: backup_id_from_manifest_key(key),
            newest_record_ms,
            oldest_record_ms,
        })
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
    ///
    /// `pub` since Task 21c: `OsoCliEngine::describe` must qualify the segment
    /// keys it lifts out of a manifest body for exactly the same reason
    /// `segments_in_manifest` does, and it lives in a different module.
    pub fn qualify(&self, relative_key: &str) -> String {
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

// ===========================================================================
// EXPLICIT STORE CONSTRUCTION (decision D2 W2)
// ===========================================================================
// Explicit store construction (decision D2 W2, §3.5, §3.10, §4.2).
//
// # Why this module exists
//
// `AmazonS3Builder::from_env()` evaluates EVERY `AWS_*` variable in the
// process environment (object_store 0.14.1 `aws/builder.rs:606-617`). That is
// the right default for a single-destination installation and the wrong one
// for anything else, and it is the mechanism behind tracker defect
// **SEC-ENVHTTP**: a forwarded `AWS_ALLOW_HTTP=true` enables plaintext
// transport even when the approved plan says `allow_http: false`. A global
// setting must never override approved execution inputs (D-SEAMS S5).
//
// So this module adds a SECOND way to build a store, beside the existing
// `from_url` / `read_only_from_url` (which are unchanged, still read the
// environment, and still serve legacy inline objects):
//
// * [`CredentialSource`] says exactly where the credential comes from —
//   explicit static values, static values read from three NAMED variables and
//   nothing else, an injected workload identity ONLY, or today's ambient
//   chain.
// * every addressing and transport value comes from the [`StorageUrl`] the
//   caller passes, and overrides whatever the environment says.
// * [`StoreOptions::pin_instance_metadata`] points the instance-metadata
//   endpoint at a dead loopback address, so a missing workload identity can
//   never fall back to the node's instance role (D2 G16).
// * [`StoreOptions::root_certificates`] carries a destination's private CA.
//
// # How the guarantee is testable without a socket
//
// [`s3_effective`] is the ONE function that decides what a store will be
// built with, and [`Store::from_url_with`] builds the client by
// consuming its output. A test can therefore read the decision directly
// rather than modelling it a second time — a parallel model is exactly how a
// test comes to assert something the production path does not do.

use std::time::Duration;

/// The message prefix a refusal carries when a `WorkloadIdentity` store finds
/// no injected identity. D2 §3.5: the runner fails CLOSED rather than falling
/// through to a node role.
pub const WORKLOAD_IDENTITY_NOT_INJECTED: &str = "WorkloadIdentityNotInjected";

/// A dead loopback address. Port 1 is `tcpmux`, which nothing in a runner or
/// controller image listens on, so a credential chain that reaches instance
/// metadata gets an immediate connection refusal instead of a node role.
pub const DEAD_METADATA_ENDPOINT: &str = "http://127.0.0.1:1";

/// Where the object-store credential comes from.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub enum CredentialSource {
    /// object_store's own chain over the whole `AWS_*` environment: static
    /// keys, then web identity, then container credentials, then instance
    /// metadata. What `from_url` and `read_only_from_url` have always used,
    /// and what the controller's own allowlisted `ControllerIdentity` reads
    /// use (D2 §3.10).
    #[default]
    Ambient,
    /// Explicit values the caller already holds — the shape the restore
    /// runner's evidence store uses when its grant differs from the archive
    /// grant (`LOGWEIR_EVIDENCE_AWS_*`, D2 §3.5).
    Static {
        access_key_id: String,
        secret_access_key: String,
        session_token: Option<String>,
    },
    /// The three NAMED variables `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`
    /// and `AWS_SESSION_TOKEN`, and nothing else. This is the destination
    /// -backed Job's archive credential: the kubelet projects those three from
    /// the grant's Secret, and no other `AWS_*` variable may influence the
    /// store.
    StaticFromEnv,
    /// An injected workload identity ONLY: `AWS_WEB_IDENTITY_TOKEN_FILE` plus
    /// `AWS_ROLE_ARN`, or `AWS_CONTAINER_CREDENTIALS_FULL_URI` /
    /// `AWS_CONTAINER_CREDENTIALS_RELATIVE_URI` plus its token file. Static
    /// keys present in the environment are IGNORED — otherwise they would take
    /// precedence over the identity the operator asked for (object_store's
    /// chain puts static first) — and an absent injection is a refusal.
    WorkloadIdentity,
}

/// Which credential provider a built store will use. The observable half of
/// [`CredentialSource`], with the secret removed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CredentialKind {
    Ambient,
    Static,
    WorkloadIdentity,
}

/// Everything about a store that is NOT its location.
#[derive(Debug, Clone)]
pub struct StoreOptions {
    pub credentials: CredentialSource,
    /// PEM bundles to trust IN ADDITION to the platform store. Each is parsed
    /// with `object_store::Certificate::from_pem_bundle`, so a file holding a
    /// chain works.
    pub root_certificates: Vec<Vec<u8>>,
    /// Point the instance-metadata endpoint at [`DEAD_METADATA_ENDPOINT`].
    /// Defaults to `true` for every explicit credential source and `false` for
    /// [`CredentialSource::Ambient`], which is the one case where an instance
    /// role may legitimately be what the operator configured.
    pub pin_instance_metadata: Option<bool>,
    /// An overall request timeout. A check has a budget; without one,
    /// object_store's default retry window is three minutes.
    pub request_timeout: Option<Duration>,
    /// `Some(0)` disables retries. `None` keeps object_store's default.
    pub max_retries: Option<usize>,
}

impl Default for StoreOptions {
    fn default() -> Self {
        Self {
            credentials: CredentialSource::Ambient,
            root_certificates: Vec::new(),
            pin_instance_metadata: None,
            request_timeout: None,
            max_retries: None,
        }
    }
}

impl StoreOptions {
    /// Today's behaviour, named: the ambient chain over the whole `AWS_*`
    /// environment.
    #[must_use]
    pub fn ambient() -> Self {
        Self::default()
    }

    /// The three projected variables and nothing else.
    #[must_use]
    pub fn static_from_env() -> Self {
        Self {
            credentials: CredentialSource::StaticFromEnv,
            ..Self::default()
        }
    }

    /// An injected workload identity only.
    #[must_use]
    pub fn workload_identity() -> Self {
        Self {
            credentials: CredentialSource::WorkloadIdentity,
            ..Self::default()
        }
    }

    /// Explicit values.
    #[must_use]
    pub fn static_keys(
        access_key_id: impl Into<String>,
        secret_access_key: impl Into<String>,
        session_token: Option<String>,
    ) -> Self {
        Self {
            credentials: CredentialSource::Static {
                access_key_id: access_key_id.into(),
                secret_access_key: secret_access_key.into(),
                session_token,
            },
            ..Self::default()
        }
    }

    #[must_use]
    pub fn with_root_certificate(mut self, pem: Vec<u8>) -> Self {
        self.root_certificates.push(pem);
        self
    }

    #[must_use]
    pub fn with_request_timeout(mut self, d: Duration) -> Self {
        self.request_timeout = Some(d);
        self
    }

    #[must_use]
    pub fn with_max_retries(mut self, n: usize) -> Self {
        self.max_retries = Some(n);
        self
    }

    /// Whether the instance-metadata endpoint is pinned, after defaulting.
    #[must_use]
    pub fn pins_instance_metadata(&self) -> bool {
        self.pin_instance_metadata
            .unwrap_or(!matches!(self.credentials, CredentialSource::Ambient))
    }

    /// Whether building a store with these options reads the process
    /// environment at all, and if so, which variables. `Ambient` reads every
    /// `AWS_*` variable; the explicit sources read a NAMED list; `Static`
    /// reads none.
    #[must_use]
    pub fn reads_environment(&self) -> bool {
        !matches!(self.credentials, CredentialSource::Static { .. })
    }
}

/// An injected workload identity, as found in the environment.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WorkloadIdentity {
    /// IRSA: `AWS_WEB_IDENTITY_TOKEN_FILE` + `AWS_ROLE_ARN`.
    WebIdentity {
        token_file: String,
        role_arn: String,
        session_name: Option<String>,
        sts_endpoint: Option<String>,
    },
    /// EKS Pod Identity: `AWS_CONTAINER_CREDENTIALS_FULL_URI` +
    /// `AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE`.
    ContainerFullUri { uri: String, token_file: String },
    /// ECS task role: `AWS_CONTAINER_CREDENTIALS_RELATIVE_URI`.
    ContainerRelativeUri { uri: String },
}

/// EXACTLY what a store will be built with. Read by a test, and consumed by
/// [`Store::from_url_with`] — one decision, two readers, so a test
/// cannot assert something the production path does not do.
///
/// The SECRET is deliberately absent: `access_key_id` is the public half of a
/// static credential (it appears in every signed request), and nothing here
/// carries the secret access key or a session token value.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct S3Effective {
    pub bucket: String,
    pub prefix: String,
    pub region: Option<String>,
    pub endpoint: Option<String>,
    /// `true` renders `AWS_VIRTUAL_HOSTED_STYLE_REQUEST=true`; it is the
    /// NEGATION of the `StorageUrl`'s `path_style`, and it comes from there
    /// and from nowhere else.
    pub virtual_hosted_style: bool,
    /// Comes from the `StorageUrl`'s `allow_http`, which a destination derives
    /// from `transport.security` alone. NEVER from `AWS_ALLOW_HTTP` and never
    /// from the addressing style.
    pub allow_http: bool,
    pub credentials: CredentialKind,
    /// The public half of a static credential, for a log line that says WHICH
    /// principal was used. `None` for every other source.
    pub access_key_id: Option<String>,
    pub session_token_present: bool,
    pub workload_identity: Option<WorkloadIdentity>,
    pub metadata_endpoint: Option<String>,
    pub root_certificate_count: usize,
    pub request_timeout: Option<Duration>,
    pub max_retries: Option<usize>,
    /// `true` when construction consults the process environment.
    pub reads_environment: bool,
}

fn env_value(name: &str) -> Option<String> {
    std::env::var(name)
        .ok()
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty())
}

/// The injected identity, or `None`. The NAMED list is the whole list: a
/// variable not on it cannot influence a `WorkloadIdentity` store.
#[must_use]
pub fn workload_identity_from_env() -> Option<WorkloadIdentity> {
    if let (Some(token_file), Some(role_arn)) = (
        env_value("AWS_WEB_IDENTITY_TOKEN_FILE"),
        env_value("AWS_ROLE_ARN"),
    ) {
        return Some(WorkloadIdentity::WebIdentity {
            token_file,
            role_arn,
            session_name: env_value("AWS_ROLE_SESSION_NAME"),
            sts_endpoint: env_value("AWS_ENDPOINT_URL_STS"),
        });
    }
    if let (Some(uri), Some(token_file)) = (
        env_value("AWS_CONTAINER_CREDENTIALS_FULL_URI"),
        env_value("AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE"),
    ) {
        return Some(WorkloadIdentity::ContainerFullUri { uri, token_file });
    }
    env_value("AWS_CONTAINER_CREDENTIALS_RELATIVE_URI")
        .map(|uri| WorkloadIdentity::ContainerRelativeUri { uri })
}

/// Resolves a `StorageUrl` plus [`StoreOptions`] into the exact configuration
/// a store will carry.
///
/// Refuses a non-S3 `StorageUrl` with an explicit credential source: a
/// `BackupDestination` is S3-only (D2 §3.1 `provider: S3`), and silently
/// ignoring an explicit credential on an Azure or filesystem URL would build a
/// store with a credential nobody asked for.
pub fn s3_effective(u: &StorageUrl, opts: &StoreOptions) -> Result<S3Effective, StoreError> {
    let StorageUrl::S3 {
        bucket,
        prefix,
        region,
        endpoint,
        path_style,
        allow_http,
    } = u
    else {
        return Err(StoreError::Backend(format!(
            "explicit store options apply to `s3://` locations only; a destination is \
             `provider: S3` (got {})",
            backend_name(u)
        )));
    };

    let (credentials, access_key_id, session_token_present, workload_identity) =
        match &opts.credentials {
            CredentialSource::Ambient => (CredentialKind::Ambient, None, false, None),
            CredentialSource::Static {
                access_key_id,
                session_token,
                ..
            } => (
                CredentialKind::Static,
                Some(access_key_id.clone()),
                session_token.is_some(),
                None,
            ),
            CredentialSource::StaticFromEnv => {
                let Some(key_id) = env_value("AWS_ACCESS_KEY_ID") else {
                    return Err(StoreError::Backend(
                        "credential mode `static` needs AWS_ACCESS_KEY_ID and \
                         AWS_SECRET_ACCESS_KEY projected into this process; neither is set"
                            .to_string(),
                    ));
                };
                if env_value("AWS_SECRET_ACCESS_KEY").is_none() {
                    return Err(StoreError::Backend(
                        "credential mode `static` has AWS_ACCESS_KEY_ID but no \
                         AWS_SECRET_ACCESS_KEY; check the Secret key names on the grant"
                            .to_string(),
                    ));
                }
                (
                    CredentialKind::Static,
                    Some(key_id),
                    env_value("AWS_SESSION_TOKEN").is_some(),
                    None,
                )
            }
            CredentialSource::WorkloadIdentity => {
                let Some(id) = workload_identity_from_env() else {
                    return Err(StoreError::Backend(format!(
                        "{WORKLOAD_IDENTITY_NOT_INJECTED}: credential mode \
                         `workloadIdentity` found neither AWS_WEB_IDENTITY_TOKEN_FILE plus \
                         AWS_ROLE_ARN nor AWS_CONTAINER_CREDENTIALS_FULL_URI plus its token \
                         file. Refusing rather than falling back to a node instance role, \
                         which is not a supported destination mode"
                    )));
                };
                (CredentialKind::WorkloadIdentity, None, false, Some(id))
            }
        };

    Ok(S3Effective {
        bucket: bucket.clone(),
        prefix: prefix.clone(),
        region: region.clone(),
        endpoint: endpoint.clone(),
        virtual_hosted_style: !*path_style,
        allow_http: *allow_http,
        credentials,
        access_key_id,
        session_token_present,
        workload_identity,
        metadata_endpoint: opts
            .pins_instance_metadata()
            .then(|| DEAD_METADATA_ENDPOINT.to_string()),
        root_certificate_count: opts.root_certificates.len(),
        request_timeout: opts.request_timeout,
        max_retries: opts.max_retries,
        reads_environment: opts.reads_environment(),
    })
}

fn backend_name(u: &StorageUrl) -> &'static str {
    match u {
        StorageUrl::S3 { .. } => "s3",
        StorageUrl::Azure { .. } => "azure",
        StorageUrl::Gcs { .. } => "gcs",
        StorageUrl::Filesystem { .. } => "filesystem",
    }
}

// ------------------------------------------------------- error classification

/// The CLOSED store-side code vocabulary of D2 §4.2, so a caller can map a
/// failure to a check code without ever printing the raw error.
///
/// The spellings are the ones `logweir_core::check_contract::CheckCode` uses;
/// `the_class_names_are_check_codes` in `tests/options.rs` asserts the two
/// tables agree, so a rename in either is a failing test rather than a check
/// result with a reason string no UI has text for.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum StoreErrorClass {
    /// The principal is authenticated and not authorized.
    AccessDenied,
    /// The credential itself is wrong, expired or not signed correctly:
    /// `InvalidAccessKeyId`, `SignatureDoesNotMatch`, `ExpiredToken`.
    InvalidCredentials,
    BucketNotFound,
    ObjectNotFound,
    EndpointUnreachable,
    TlsTrustFailed,
    RegionMismatch,
    Timeout,
    /// Deliberately NOT a fallback that pretends to know: "I could not
    /// classify this" is a different fact from any of the above, and a caller
    /// that showed a wrong remedy would send an operator after the wrong
    /// problem.
    StoreErrorUnclassified,
}

impl StoreErrorClass {
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::AccessDenied => "AccessDenied",
            Self::InvalidCredentials => "InvalidCredentials",
            Self::BucketNotFound => "BucketNotFound",
            Self::ObjectNotFound => "ObjectNotFound",
            Self::EndpointUnreachable => "EndpointUnreachable",
            Self::TlsTrustFailed => "TlsTrustFailed",
            Self::RegionMismatch => "RegionMismatch",
            Self::Timeout => "Timeout",
            Self::StoreErrorUnclassified => "StoreErrorUnclassified",
        }
    }

    pub const ALL: [Self; 9] = [
        Self::AccessDenied,
        Self::InvalidCredentials,
        Self::BucketNotFound,
        Self::ObjectNotFound,
        Self::EndpointUnreachable,
        Self::TlsTrustFailed,
        Self::RegionMismatch,
        Self::Timeout,
        Self::StoreErrorUnclassified,
    ];

    /// Classifies a [`StoreError`].
    ///
    /// # What this is, honestly
    ///
    /// The variant carries the reliable half: [`StoreError::NotFound`] is a
    /// genuine absence, established by the backend, and never a denial (that
    /// distinction is what `StoreError::NotFound`'s own doc comment exists
    /// for). Everything else arrives as [`StoreError::Io`], whose message is
    /// `object_store::Error`'s `Display` — which carries the structured
    /// variant's wording, the HTTP status and, for S3, the `<Code>` element of
    /// the XML body. So the rest is a TOKEN SCAN over that text.
    ///
    /// A token scan is a tripwire on spellings, not a proof. What makes it
    /// trustworthy enough to act on is that every token below is pinned by a
    /// REAL message in `tests/options.rs::the_classifier_table`, and that the
    /// unmatched case is its own answer rather than a guess.
    #[must_use]
    pub fn classify(e: &StoreError) -> Self {
        match e {
            StoreError::NotFound(_) => Self::ObjectNotFound,
            StoreError::AlreadyExists(_) | StoreError::ReadOnly(_) => Self::StoreErrorUnclassified,
            StoreError::NotAManifest(_, _) => Self::StoreErrorUnclassified,
            StoreError::Backend(m) | StoreError::Io(m) => classify_text(m),
        }
    }

    /// Classifies a raw `object_store::Error`, using its STRUCTURED variants
    /// where they exist and the token scan otherwise. Callers that still hold
    /// the raw error should prefer this.
    #[must_use]
    pub fn classify_object_store(e: &object_store::Error) -> Self {
        match e {
            object_store::Error::NotFound { source, .. } => {
                // A `NoSuchBucket` arrives as NotFound too, and "the bucket is
                // not there" is a different remedy from "the key is not
                // there".
                let text = source.to_string();
                if text.contains("NoSuchBucket") || text.contains("bucket does not exist") {
                    Self::BucketNotFound
                } else {
                    Self::ObjectNotFound
                }
            }
            object_store::Error::PermissionDenied { source, .. } => {
                let c = classify_text(&source.to_string());
                if c == Self::StoreErrorUnclassified {
                    Self::AccessDenied
                } else {
                    c
                }
            }
            object_store::Error::Unauthenticated { source, .. } => {
                let c = classify_text(&source.to_string());
                if c == Self::StoreErrorUnclassified {
                    Self::InvalidCredentials
                } else {
                    c
                }
            }
            other => classify_text(&other.to_string()),
        }
    }
}

/// The token table. Order matters: the credential codes are checked before the
/// generic `AccessDenied`, because `SignatureDoesNotMatch` arrives with a 403
/// and a wrong key is not the same problem as a missing grant.
fn classify_text(text: &str) -> StoreErrorClass {
    let lower = text.to_ascii_lowercase();

    // A private CA that the process does not trust. Checked first: a TLS
    // failure can also carry the word "connect", and "add the CA bundle" is a
    // very different remedy from "open the port".
    const TLS: [&str; 6] = [
        "invalid peer certificate",
        "certificate verify failed",
        "unknownissuer",
        "self-signed certificate",
        "self signed certificate",
        "certificate is not trusted",
    ];
    if TLS.iter().any(|t| lower.contains(t)) {
        return StoreErrorClass::TlsTrustFailed;
    }

    const CREDENTIAL: [&str; 6] = [
        "invalidaccesskeyid",
        "signaturedoesnotmatch",
        "expiredtoken",
        "tokenrefreshrequired",
        "invalidsecurity",
        "lacked valid authentication credentials",
    ];
    if CREDENTIAL.iter().any(|t| lower.contains(t)) {
        return StoreErrorClass::InvalidCredentials;
    }

    // A wrong region answers 301 `PermanentRedirect` (path-style) or 400
    // `AuthorizationHeaderMalformed` naming the expected region.
    const REGION: [&str; 4] = [
        "permanentredirect",
        "authorizationheadermalformed",
        "the region",
        "illegallocationconstraint",
    ];
    if REGION.iter().any(|t| lower.contains(t)) {
        return StoreErrorClass::RegionMismatch;
    }

    const BUCKET: [&str; 2] = ["nosuchbucket", "bucket does not exist"];
    if BUCKET.iter().any(|t| lower.contains(t)) {
        return StoreErrorClass::BucketNotFound;
    }

    const DENIED: [&str; 5] = [
        "accessdenied",
        "lacked the necessary privileges",
        "all access to this object has been disabled",
        "403 forbidden",
        "status: 403",
    ];
    if DENIED.iter().any(|t| lower.contains(t)) {
        return StoreErrorClass::AccessDenied;
    }

    const NOT_FOUND: [&str; 3] = ["nosuchkey", "not found: ", "status: 404"];
    if NOT_FOUND.iter().any(|t| lower.contains(t)) {
        return StoreErrorClass::ObjectNotFound;
    }

    const TIMEOUT: [&str; 4] = [
        "timed out",
        "timeout",
        "operation timed out",
        "deadline has elapsed",
    ];
    if TIMEOUT.iter().any(|t| lower.contains(t)) {
        return StoreErrorClass::Timeout;
    }

    const UNREACHABLE: [&str; 8] = [
        "connection refused",
        "dns error",
        "failed to lookup address",
        "no route to host",
        "network is unreachable",
        "error sending request",
        "connection reset",
        "url scheme is not allowed",
    ];
    if UNREACHABLE.iter().any(|t| lower.contains(t)) {
        return StoreErrorClass::EndpointUnreachable;
    }

    StoreErrorClass::StoreErrorUnclassified
}

/// `true` when this refusal is D2 §3.5's fail-closed "no injected identity".
#[must_use]
pub fn is_workload_identity_not_injected(e: &StoreError) -> bool {
    matches!(e, StoreError::Backend(m) if m.starts_with(WORKLOAD_IDENTITY_NOT_INJECTED))
}
