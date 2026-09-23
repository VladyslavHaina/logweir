//! The credential-scoped handle that actually removes an object.
//!
//! # This file is the whole of the delete capability
//!
//! Everything else in this workspace is delete-free, and
//! `scripts/check-no-archive-write.sh` proves it two ways: a source grep over
//! `crates/weirkeeper/src` and `crates/logweir-store/src`, and a dependency walk
//! showing the crates reaching `logweir-reaper` are exactly
//! `{logweir-retention}`. The whole point of putting the call in one small file
//! behind one trait method is that "what can delete" is answerable by reading
//! it.
//!
//! # The credential never reaches a log, a status or a spec
//!
//! [`Credentials::from_env`] reads three NAMED variables the Job projects with
//! `valueFrom.secretKeyRef` and nothing else — never a sweep of `AWS_*`, which
//! is what `AmazonS3Builder::from_env()` does and what would let a controller's
//! ambient environment relocate a deletion (D-SEAMS **S5**). The struct has a
//! hand-written [`std::fmt::Debug`] that prints the access key id's length and
//! nothing of the secret, because a `#[derive(Debug)]` on a credential is one
//! `tracing` field away from a pod log.

use std::sync::Arc;

use object_store::{ObjectStore, ObjectStoreExt as _};

use logweir_core::engine::StorageUrl;

use crate::{DeleteError, Deleter, Versioning};

/// The three variables a retention Job projects from its delete-capable Secret.
pub const ACCESS_KEY_ID_ENV: &str = "AWS_ACCESS_KEY_ID";
/// The secret half.
pub const SECRET_ACCESS_KEY_ENV: &str = "AWS_SECRET_ACCESS_KEY";
/// The optional session token.
pub const SESSION_TOKEN_ENV: &str = "AWS_SESSION_TOKEN";

/// A static object-store credential, by value, read from named variables.
#[derive(Clone, PartialEq, Eq)]
pub struct Credentials {
    /// The public half.
    pub access_key_id: String,
    /// The secret half. Never printed.
    pub secret_access_key: String,
    /// The session token, when one is projected.
    pub session_token: Option<String>,
}

impl std::fmt::Debug for Credentials {
    /// Lengths and presence, never values. A credential in a `Debug` output is
    /// a credential in a pod log.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Credentials")
            .field("access_key_id_len", &self.access_key_id.len())
            .field("secret_access_key_len", &self.secret_access_key.len())
            .field("session_token", &self.session_token.is_some())
            .finish()
    }
}

impl Credentials {
    /// Read the three named variables.
    ///
    /// **A variable that is present and blank is not a credential.**
    /// `std::env::var` returns `Ok("")` — not `Err(NotPresent)` — for a
    /// Kubernetes `env:` entry with an empty `value:`, and a `secretKeyRef` to
    /// a key that exists and is blank projects the same thing. Treating that as
    /// a credential produces an `AccessDenied` at 04:17 instead of a legible
    /// refusal at startup.
    #[must_use]
    pub fn from_env() -> Option<Self> {
        let non_empty = |name: &str| std::env::var(name).ok().filter(|v| !v.is_empty());
        Some(Self {
            access_key_id: non_empty(ACCESS_KEY_ID_ENV)?,
            secret_access_key: non_empty(SECRET_ACCESS_KEY_ENV)?,
            session_token: non_empty(SESSION_TOKEN_ENV),
        })
    }
}

/// Why a handle could not be built.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum BuildError {
    /// The backend refused the configuration. Carries the backend's own
    /// message, which names no credential value.
    #[error("the object-store handle could not be built: {0}")]
    Backend(String),
    /// The runtime could not be built.
    #[error("the reaper's runtime could not be built: {0}")]
    Runtime(String),
    /// A provider this crate has no EXPLICIT-credential path for.
    ///
    /// Review `d3w9` M7. The S3 arm is written with `AmazonS3Builder::new()`
    /// precisely so that no ambient variable can relocate a deletion (D-SEAMS
    /// **S5**); the Azure and GCS builders have no equivalent explicit path in
    /// this shape, and the first landing reached for `from_env()` on both —
    /// putting the process environment, rather than the frozen `StorageUrl`, in
    /// charge of where a delete lands. Refusing is the only honest answer until
    /// an explicit path exists: neither provider is advertised for enforcement,
    /// and a `RetentionPolicy` over one is usable in `Report` and
    /// `ExternalLifecycle` exactly as before.
    #[error(
        "the retention worker has no explicit-credential path for {0}, and it will not build a          DELETE-capable handle from ambient process environment (D-SEAMS S5). Use `mode: Report`          or `mode: ExternalLifecycle` for this destination."
    )]
    UnsupportedProvider(&'static str),
}

/// A handle that can remove an object, and do nothing else.
///
/// It exposes exactly one operation. There is no put, no list-and-delete, no
/// copy: a method that read a prefix and removed what it found would make the
/// explicit key list in the plan decorative.
pub struct ArchiveReaper {
    inner: Arc<dyn ObjectStore>,
    rt: tokio::runtime::Runtime,
}

impl std::fmt::Debug for ArchiveReaper {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("ArchiveReaper")
    }
}

impl ArchiveReaper {
    /// Build the handle from a location and an explicit credential.
    ///
    /// **`AmazonS3Builder::new()`, never `from_env()`** — the same ruling
    /// `logweir_store::Store::s3_builder` records: `from_env()` sweeps every
    /// `AWS_*` variable including `AWS_ENDPOINT_URL` and `AWS_REGION`, which
    /// would silently relocate a deletion to a destination the plan does not
    /// name. Every addressing decision here comes from the `StorageUrl` the
    /// controller froze into the Job's environment.
    ///
    /// # Errors
    ///
    /// [`BuildError`].
    pub fn new(
        location: &StorageUrl,
        credentials: Option<&Credentials>,
        allow_http: bool,
    ) -> Result<Self, BuildError> {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| BuildError::Runtime(e.to_string()))?;
        let inner = rt.block_on(async { build(location, credentials, allow_http) })?;
        Ok(Self { inner, rt })
    }
}

fn build(
    location: &StorageUrl,
    credentials: Option<&Credentials>,
    allow_http: bool,
) -> Result<Arc<dyn ObjectStore>, BuildError> {
    match location {
        StorageUrl::S3 {
            bucket,
            region,
            endpoint,
            path_style,
            allow_http: url_allow_http,
            ..
        } => {
            let http = allow_http || *url_allow_http;
            let client = object_store::ClientOptions::default().with_allow_http(http);
            let mut b = object_store::aws::AmazonS3Builder::new()
                .with_bucket_name(bucket)
                .with_virtual_hosted_style_request(!*path_style)
                .with_client_options(client);
            if let Some(r) = region {
                b = b.with_region(r);
            }
            if let Some(e) = endpoint {
                b = b.with_endpoint(e);
            }
            if let Some(c) = credentials {
                b = b
                    .with_access_key_id(&c.access_key_id)
                    .with_secret_access_key(&c.secret_access_key);
                if let Some(t) = &c.session_token {
                    b = b.with_token(t);
                }
            }
            Ok(Arc::new(
                b.build().map_err(|e| BuildError::Backend(e.to_string()))?,
            ))
        }
        // NO `from_env()` IN THE DELETER, ON ANY PROVIDER (review `d3w9` M7).
        StorageUrl::Azure { .. } => Err(BuildError::UnsupportedProvider("Azure Blob Storage")),
        StorageUrl::Gcs { .. } => Err(BuildError::UnsupportedProvider("Google Cloud Storage")),
        StorageUrl::Filesystem { path } => Ok(Arc::new(
            object_store::local::LocalFileSystem::new_with_prefix(path)
                .map_err(|e| BuildError::Backend(e.to_string()))?,
        )),
    }
}

impl Deleter for ArchiveReaper {
    /// **The one object-store delete in this workspace.**
    ///
    /// The key is checked against its own NORMALISED form first — review
    /// `d3w9` M8. `object_store::path::Path::from` drops empty segments and
    /// percent-encodes `.`, `..`, `%`, `#`, `<`, `>`, `?`, `*` and the control
    /// set, so the string every rail above validated is not necessarily the
    /// path this would delete. Two consequences, both closed here:
    ///
    /// * a key like `/logweir/x` normalises to `logweir/x`, i.e. INTO the
    ///   evidence root that `validate_plan` proved it was outside of;
    /// * a key legitimately containing `?`, `#` or `%` is encoded into a
    ///   different object, the delete hits nothing, the backend answers
    ///   `NotFound`, and `attempt` treats that as success — reporting a point
    ///   `Deleted` with its objects still in the bucket.
    ///
    /// A key whose normalised form differs from the plan's is therefore a
    /// **refusal**, not a delete of the normalised one.
    fn delete_exact(&self, key: &str) -> Result<(), DeleteError> {
        let path = normalise(key)?;
        self.rt
            .block_on(async { self.inner.delete(&path).await })
            .map_err(|e| classify(&e))
    }

    /// A HEAD of the key, read for `ObjectMeta::version`.
    ///
    /// The S3 client fills `version` from `x-amz-version-id`, which a provider
    /// sends only for an object stored under versioning — so `Some` is exactly
    /// "a delete by key would write a marker" (see [`Versioning`]). The local
    /// filesystem store never versions. The same normalisation rail as
    /// [`Deleter::delete_exact`]: the probe asks about the path the delete
    /// would address, or refuses.
    fn probe_versioning(&self, key: &str) -> Result<Versioning, DeleteError> {
        let path = normalise(key)?;
        let meta = self
            .rt
            .block_on(async { self.inner.head(&path).await })
            .map_err(|e| classify(&e))?;
        Ok(versioning_of(&meta))
    }
}

/// `ObjectMeta` → [`Versioning`], the one rule, separate so a test can drive it
/// without a bucket.
///
/// ANY version id — including the literal `null` some providers send for a
/// null version in a bucket that has been versioned — counts as versioned: the
/// conservative reading, since the answer that deletes is the other one.
#[must_use]
pub fn versioning_of(meta: &object_store::ObjectMeta) -> Versioning {
    if meta.version.as_deref().is_some_and(|v| !v.is_empty()) {
        Versioning::Versioned
    } else {
        Versioning::Unversioned
    }
}

/// The key as the object store will address it, or a refusal.
///
/// # Errors
///
/// [`DeleteError::Unclassified`] when normalisation would change the key, or
/// when the normalised form reaches the evidence root. Both are "this worker
/// will not act on a path it did not validate", which is a refusal and not a
/// transport failure, so neither is retried.
pub fn normalise(key: &str) -> Result<object_store::path::Path, DeleteError> {
    let path = object_store::path::Path::from(key);
    if path.as_ref() != key {
        return Err(DeleteError::Unclassified);
    }
    if path.as_ref().starts_with(crate::EVIDENCE_ROOT) {
        return Err(DeleteError::Unclassified);
    }
    Ok(path)
}

/// An `object_store::Error` in the closed vocabulary D3 §6.5 names.
///
/// The variants are matched rather than the message parsed, except for the one
/// case `object_store` folds into `Generic`: an S3 `AccessDenied` arrives as a
/// generic error whose source carries the code. That one substring match is
/// named here rather than hidden, and `access_denied_is_classified_and_not_
/// retried` is the row that fails if the spelling moves.
#[must_use]
pub fn classify(e: &object_store::Error) -> DeleteError {
    use object_store::Error as E;
    match e {
        E::NotFound { .. } => DeleteError::NotFound,
        E::Precondition { .. } | E::AlreadyExists { .. } => DeleteError::PreconditionFailed,
        E::NotModified { .. } => DeleteError::PreconditionFailed,
        E::PermissionDenied { .. } | E::Unauthenticated { .. } => DeleteError::AccessDenied,
        E::NotSupported { .. } | E::NotImplemented { .. } => DeleteError::Unclassified,
        other => classify_text(&other.to_string()),
    }
}

/// The fallback: classify by the codes providers actually return.
///
/// Kept separate and public so a test can drive every branch without
/// constructing an `object_store::Error` for each, and so the list of codes is
/// readable in one place.
#[must_use]
pub fn classify_text(message: &str) -> DeleteError {
    let m = message.to_ascii_lowercase();
    // ORDER MATTERS: a WORM refusal often also says "access denied", and the
    // hold is the more specific and more consequential fact.
    if m.contains("objectlock")
        || m.contains("object lock")
        || m.contains("legalhold")
        || m.contains("legal hold")
        || m.contains("worm")
        || m.contains("retention period")
    {
        return DeleteError::Locked;
    }
    if m.contains("accessdenied")
        || m.contains("access denied")
        || m.contains("forbidden")
        || m.contains("403")
    {
        return DeleteError::AccessDenied;
    }
    if m.contains("timed out") || m.contains("timeout") || m.contains("connection") {
        return DeleteError::Timeout;
    }
    if m.contains("500")
        || m.contains("502")
        || m.contains("503")
        || m.contains("504")
        || m.contains("internalerror")
        || m.contains("slowdown")
        || m.contains("serviceunavailable")
    {
        return DeleteError::ServerError;
    }
    DeleteError::Unclassified
}

impl crate::Lister for ArchiveReaper {
    /// Every key under one set directory.
    ///
    /// **Bounded.** `MAX_LISTED_KEYS` is the ceiling; a set directory holding
    /// more than that is refused rather than truncated, because a truncated
    /// listing would produce a point this run reports as `Deleted` while
    /// leaving objects behind — which is exactly the half-deleted set the
    /// manifest-first ordering exists to make impossible.
    fn list_exact(&self, prefix: &str) -> Result<Vec<String>, DeleteError> {
        use futures::StreamExt as _;
        let path = object_store::path::Path::from(prefix);
        self.rt.block_on(async {
            let mut out = Vec::new();
            let mut stream = self.inner.list(Some(&path));
            while let Some(item) = stream.next().await {
                match item {
                    Ok(meta) => {
                        if out.len() >= MAX_LISTED_KEYS {
                            return Err(DeleteError::Unclassified);
                        }
                        out.push(meta.location.to_string());
                    }
                    Err(e) => return Err(classify(&e)),
                }
            }
            Ok(out)
        })
    }
}

/// The per-set listing ceiling. A backup set of a million objects is a set this
/// worker refuses rather than half-removes.
pub const MAX_LISTED_KEYS: usize = 100_000;
