//! The controller half of a saved destination: resolve one `BackupDestination`
//! into the COMPLETE, EXPLICIT object-store configuration one operation needs
//! — decision D2 §3.4, §3.5, §3.7, §3.10.
//!
//! # What the pure half already decided, and what is left here
//!
//! [`logweir_core::destination`] owns everything that is a function of the
//! location alone: the closed enums, R3–R6, the engine-compatibility refusal,
//! the canonical URL, the location digest and the two `StorageUrl` renderings.
//! It is re-exported here rather than re-derived, because two spellings of
//! "where the archive is" is exactly the defect PLAT-08 removes.
//!
//! What is left is everything that needs the OBJECT rather than the location:
//! which of the four grants a role resolves to after defaulting, whether the
//! object's own status says it is `Valid`, what a Job's environment is, and
//! what a run freezes.
//!
//! # THE ENVIRONMENT THIS MODULE RENDERS IS COMPLETE AND CLOSED
//!
//! Defect **SEC-ENVHTTP** (D-SEAMS **S5**): today's controller FORWARDS
//! `AWS_ENDPOINT_URL`, `AWS_REGION`, `AWS_ALLOW_HTTP` and
//! `AWS_VIRTUAL_HOSTED_STYLE_REQUEST` from its own process environment into
//! every runner Job ([`crate::controllers::backup::archive_addressing_env`]),
//! and the engine calls `AmazonS3Builder::from_env()`, which reads every
//! `AWS_*` variable it finds. A controller started with `AWS_ALLOW_HTTP=true`
//! therefore enables plaintext HTTP in a runner **whose approved plan says
//! `allow_http: false`**.
//!
//! Not one function in this module reads `std::env::var`. Every value in
//! [`DestinationEnv`] is computed from the `BackupDestination` it was handed,
//! and `AWS_ALLOW_HTTP` is computed from
//! [`TransportSecurity::allows_plaintext_http`] and from nothing else — not
//! from the addressing style (defect **UI-HTTPDOWNGRADE**'s server-side twin),
//! not from the endpoint scheme, and not from the process. `AWS_ENDPOINT_URL`
//! is ABSENT BY CONSTRUCTION: the endpoint reaches the runner inside the plan's
//! own `storage` block, which W2's `StoreOptions` path reads explicitly, so a
//! variable that could relocate a store never exists in the pod at all.
//!
//! # No credential VALUE passes through this module
//!
//! A grant resolves to a Secret NAME and DATA KEYS. The kubelet projects the
//! value, in the pod's own namespace, from the reference the controller wrote.
//! The controller holds no verb on `secrets`
//! (`tests/linkage.rs::the_controller_never_reads_a_secret`), which is also why
//! "the Secret exists" is NOT something this module can assert: see
//! [`ResolvedDestination::check_job_namespace`] for what it asserts instead.
//!
//! # Wiring
//!
//! W7 owns the resolver, the `BackupDestination` reconciler and the evidence
//! store cache. **W10** wires [`ResolvedDestination::job_env`] and
//! [`ResolvedDestination::plan_storage`] into `controllers/backup.rs` and
//! `controllers/restore.rs`, freezes [`ResolvedDestinationSnapshot`] into
//! PLAT-06.1's `execution-inputs.json` (seam **S4**), and applies
//! [`retention_scope`] in `controllers/backup_schedule.rs` (D2 §3.10, G14).

use std::collections::BTreeSet;

use chrono::{DateTime, Utc};
use k8s_openapi::api::core::v1::ConfigMap;
use kube::api::Api;
use kube::ResourceExt as _;
use serde::{Deserialize, Serialize};

use logweir_core::check_contract::CheckCode;
use logweir_core::destination::{
    engine_compatible, validate, validate_ca_bundle, Addressing, DestinationLocation,
    StorageProvider, TransportSecurity,
};
use logweir_core::engine::StorageUrl;
use logweir_core::ids::sha256_prefixed;

use crate::check::policy::Policy;
use crate::crds::backup_destination::{
    AccessGrant, AccessMode, BackupDestination, EvidenceReadMode, S3SecretKeysRef,
};
use crate::job::EnvFromSecret;

/// **`DestinationRole` LIVES IN THE PURE LAYER AND IS RE-EXPORTED HERE.**
///
/// The check plan (`logweir_core::check_contract`), this resolver and the API
/// DTOs must all name the same four roles; a second enum in this crate would
/// be a second vocabulary that drifts at the first added role. Consumers write
/// `weirkeeper::destination::DestinationRole` or the core path — both resolve
/// to one type.
pub use logweir_core::destination::DestinationRole;

// ---------------------------------------------------------------------------
// The version-skew handshake (D2 §3.5)
// ---------------------------------------------------------------------------

/// The store-contract version a destination-backed Job is driven at.
///
/// A RUNNER WITHOUT THE CONTRACT MUST REFUSE, NOT IMPROVISE. The same technique
/// as `--execution-contract-version`: the flag below reaches the runner's argv
/// allowlist, and a runner that does not know it exits before dispatch. Without
/// the handshake a new controller could drive an old runner that silently built
/// its evidence store from ambient credentials.
pub const STORE_CONTRACT_VERSION: &str = "1";
/// The argv flag carrying [`STORE_CONTRACT_VERSION`].
pub const STORE_CONTRACT_VERSION_ARG: &str = "--store-contract-version";
/// The environment variable carrying [`STORE_CONTRACT_VERSION`].
pub const STORE_CONTRACT_VERSION_ENV: &str = "LOGWEIR_STORE_CONTRACT_VERSION";

// ---------------------------------------------------------------------------
// The variable names one resolved destination renders (D2 §3.5)
// ---------------------------------------------------------------------------

/// Which credential provider the runner builds its ARCHIVE store with.
pub const ARCHIVE_CREDENTIALS_ENV: &str = "LOGWEIR_ARCHIVE_CREDENTIALS";
/// Which credential provider the runner builds its EVIDENCE store with.
pub const EVIDENCE_CREDENTIALS_ENV: &str = "LOGWEIR_EVIDENCE_CREDENTIALS";
/// `LOGWEIR_*_CREDENTIALS` for the three projected `AWS_*` key variables.
pub const CREDENTIALS_STATIC: &str = "static";
/// `LOGWEIR_*_CREDENTIALS` for an injected workload identity.
pub const CREDENTIALS_WORKLOAD_IDENTITY: &str = "workloadIdentity";
/// `LOGWEIR_EVIDENCE_CREDENTIALS` when the evidence grant IS the archive grant.
pub const CREDENTIALS_ARCHIVE: &str = "archive";

/// The file the runner's archive store trusts in addition to the platform
/// store. The bytes are frozen into the run's own plan `ConfigMap`.
pub const ARCHIVE_CA_FILE_ENV: &str = "LOGWEIR_ARCHIVE_CA_FILE";
/// The evidence store's twin of [`ARCHIVE_CA_FILE_ENV`].
pub const EVIDENCE_CA_FILE_ENV: &str = "LOGWEIR_EVIDENCE_CA_FILE";
/// The plan `ConfigMap` key holding the archive destination's CA bundle.
pub const ARCHIVE_CA_PLAN_KEY: &str = "archive-ca.pem";
/// The plan `ConfigMap` key holding the evidence destination's CA bundle.
pub const EVIDENCE_CA_PLAN_KEY: &str = "evidence-ca.pem";
/// Where the plan `ConfigMap` is mounted in a runner pod.
pub const PLAN_MOUNT_PATH: &str = "/plan";

/// The three variables a `SecretKeys` archive grant projects.
pub const AWS_ACCESS_KEY_ID_ENV: &str = "AWS_ACCESS_KEY_ID";
/// See [`AWS_ACCESS_KEY_ID_ENV`].
pub const AWS_SECRET_ACCESS_KEY_ENV: &str = "AWS_SECRET_ACCESS_KEY";
/// See [`AWS_ACCESS_KEY_ID_ENV`].
pub const AWS_SESSION_TOKEN_ENV: &str = "AWS_SESSION_TOKEN";

/// The evidence twins of the three above, read by the runner's evidence store
/// only — so a restore whose evidence grant differs from its archive grant can
/// carry both without either shadowing the other.
pub const EVIDENCE_ACCESS_KEY_ID_ENV: &str = "LOGWEIR_EVIDENCE_AWS_ACCESS_KEY_ID";
/// See [`EVIDENCE_ACCESS_KEY_ID_ENV`].
pub const EVIDENCE_SECRET_ACCESS_KEY_ENV: &str = "LOGWEIR_EVIDENCE_AWS_SECRET_ACCESS_KEY";
/// See [`EVIDENCE_ACCESS_KEY_ID_ENV`].
pub const EVIDENCE_SESSION_TOKEN_ENV: &str = "LOGWEIR_EVIDENCE_AWS_SESSION_TOKEN";

/// `true` only for [`TransportSecurity::InsecureHttp`].
pub const AWS_ALLOW_HTTP_ENV: &str = "AWS_ALLOW_HTTP";
/// `true` only for [`Addressing::VirtualHosted`].
pub const AWS_VIRTUAL_HOSTED_ENV: &str = "AWS_VIRTUAL_HOSTED_STYLE_REQUEST";
/// Pinned at [`logweir_store::DEAD_METADATA_ENDPOINT`] on every
/// destination-backed Job (D2 G16): a missing workload identity must be a
/// refusal, never a silent fall-back to the node's instance role.
pub const AWS_METADATA_ENDPOINT_ENV: &str = "AWS_METADATA_ENDPOINT";
/// Rendered only when `spec.storage.region` is set.
pub const AWS_REGION_ENV: &str = "AWS_REGION";

/// The variable this module NEVER renders.
///
/// Named as a constant so the guard
/// `no_rendered_environment_carries_an_endpoint_variable` can assert its
/// absence by the same spelling the leak would use. The endpoint travels in the
/// plan's `storage` block, which the runner reads explicitly; a variable that
/// `AmazonS3Builder::from_env()` would sweep up is a second, silent answer to
/// "where is the bucket".
pub const AWS_ENDPOINT_URL_ENV: &str = "AWS_ENDPOINT_URL";

/// A CA bundle larger than this is refused with [`CheckCode::CaBundleTooLarge`].
///
/// 64 KiB is roughly two hundred certificates. A `ConfigMap` may hold a
/// megabyte, and every byte of it would be copied into every run's immutable
/// plan `ConfigMap` and parsed on every store build.
pub const CA_BUNDLE_MAX_BYTES: usize = 64 * 1024;

/// The condition type the `BackupDestination` reconciler owns.
pub const CONDITION_VALID: &str = "Valid";

// ---------------------------------------------------------------------------
// Refusals
// ---------------------------------------------------------------------------

/// Why a destination cannot be resolved, or cannot be used for this operation.
///
/// THE CODE IS A [`CheckCode`], NOT A LOCAL STRING. D2's check catalogue, this
/// resolver and the `BackupDestination` status all report the same closed
/// vocabulary; a private `&'static str` set here would be a fourth spelling
/// nobody could compare against the other three.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DestinationRefusal {
    /// The closed code. Also the condition `reason` a caller writes.
    pub code: CheckCode,
    /// The offending field, as a dotted path rooted at `spec`, or the object
    /// path for a reference that did not resolve.
    pub field: String,
    /// What is wrong and what to do. Names fields, objects and keys; never a
    /// credential value — this resolver has none to name.
    pub message: String,
}

impl DestinationRefusal {
    fn new(code: CheckCode, field: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            code,
            field: field.into(),
            message: message.into(),
        }
    }

    /// The condition `reason` this refusal is written as.
    #[must_use]
    pub fn reason(&self) -> &'static str {
        self.code.as_str()
    }

    /// Whether a caller should HOLD (requeue and re-read) rather than fail the
    /// object terminally — D2 §3.6.
    ///
    /// **TWO CODES AND NO MORE.** A destination that is absent or not yet
    /// `Valid` is a race an operator resolves by creating or fixing the object,
    /// and a run that failed terminally on it could never be retried. Everything
    /// else — a role nobody configured, two ServiceAccounts in one pod, an
    /// addressing the engine cannot honour — is a decision, and a decision that
    /// is retried forever is a decision nobody sees.
    #[must_use]
    pub fn is_hold(&self) -> bool {
        matches!(
            self.code,
            CheckCode::DestinationNotFound | CheckCode::DestinationNotValid
        )
    }
}

impl std::fmt::Display for DestinationRefusal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.code.as_str(), self.message)
    }
}

impl std::error::Error for DestinationRefusal {}

/// A refusal, or the API server would not answer.
///
/// SEPARATE FROM [`DestinationRefusal`] ON PURPOSE. A 503 is not a verdict
/// about the destination: a caller requeues on it and writes no condition,
/// exactly as [`crate::controllers::approval::ReconcileError::Api`] does.
#[derive(Debug)]
pub enum ResolveError {
    /// The destination was read and refused.
    Refused(Box<DestinationRefusal>),
    /// The API server could not be talked to.
    Api(kube::Error),
}

impl From<DestinationRefusal> for ResolveError {
    fn from(r: DestinationRefusal) -> Self {
        Self::Refused(Box::new(r))
    }
}

impl From<kube::Error> for ResolveError {
    fn from(e: kube::Error) -> Self {
        Self::Api(e)
    }
}

impl std::fmt::Display for ResolveError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Refused(r) => write!(f, "{r}"),
            Self::Api(e) => write!(f, "kubernetes API error: {e}"),
        }
    }
}

impl std::error::Error for ResolveError {}

// ---------------------------------------------------------------------------
// The resolved grant
// ---------------------------------------------------------------------------

/// One role's credential, after defaulting — REFERENCES ONLY.
/// **`rename_all_fields`, NOT JUST `rename_all`.** On an ENUM, `rename_all`
/// renames the VARIANTS and leaves struct-variant FIELDS alone — so the first
/// version of this type serialised `{"mode":"secretKeys","access_key_id_key":…}`
/// inside a snapshot that is camelCase everywhere else, against D2 §3.7's
/// `{mode, secretName?, keys?, serviceAccountName?}`. That block is frozen into
/// `execution-inputs.json` under `deny_unknown_fields` and re-encoded byte for
/// byte on every later pass, so renaming a key after W10 lands turns every
/// running `Backup` into a `PlanConfigMapConflict`. `the_snapshot_key_spellings_are_pinned`
/// is the golden-bytes test that keeps it.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// The VARIANT names are left PascalCase deliberately, so the snapshot's `mode`
/// reads exactly as `spec.access.<role>.mode` reads on the object it was
/// resolved from — `SecretKeys`, `WorkloadIdentity`, `ControllerIdentity`. One
/// vocabulary, one spelling; a camelCased tag here would be a second.
#[serde(rename_all_fields = "camelCase", tag = "mode")]
pub enum ResolvedGrant {
    /// Keys the kubelet projects from a Secret in the destination's own
    /// namespace.
    SecretKeys {
        /// The Secret's `metadata.name`. No namespace: see
        /// [`ResolvedDestination::check_job_namespace`].
        secret: String,
        /// The data key holding the access key id.
        access_key_id_key: String,
        /// The data key holding the secret access key.
        secret_access_key_key: String,
        /// The data key holding a session token, when one is configured.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        session_token_key: Option<String>,
    },
    /// The pod's own ServiceAccount identity.
    WorkloadIdentity {
        /// The ServiceAccount the Job must run as.
        service_account_name: String,
    },
    /// The controller's own allowlisted read-only handle. `EvidenceRead` only.
    ControllerIdentity,
    /// `evidenceRead` is absent: verification is `NotAttempted` and says so.
    NotConfigured,
}

impl ResolvedGrant {
    /// The `LOGWEIR_*_CREDENTIALS` value a runner reads, or `None` for a grant
    /// no runner ever sees.
    #[must_use]
    pub fn credentials_mode(&self) -> Option<&'static str> {
        match self {
            Self::SecretKeys { .. } => Some(CREDENTIALS_STATIC),
            Self::WorkloadIdentity { .. } => Some(CREDENTIALS_WORKLOAD_IDENTITY),
            Self::ControllerIdentity | Self::NotConfigured => None,
        }
    }

    /// The ServiceAccount this grant requires the pod to run as, if any.
    #[must_use]
    pub fn service_account_name(&self) -> Option<&str> {
        match self {
            Self::WorkloadIdentity {
                service_account_name,
            } => Some(service_account_name),
            _ => None,
        }
    }
}

// ---------------------------------------------------------------------------
// The resolved destination
// ---------------------------------------------------------------------------

/// A `BackupDestination`, resolved for ONE role.
///
/// SERIALISABLE AND VALUE-FREE, like [`crate::connection::ResolvedConnection`]:
/// every field is a setting or a reference, so the whole thing can be frozen
/// into a run's immutable inputs and returned by a product API.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ResolvedDestination {
    /// The destination's `metadata.name`.
    pub name: String,
    /// The destination's `metadata.namespace` — the ONE namespace in which
    /// every reference below resolves to the intended object.
    pub namespace: String,
    /// The destination's `metadata.uid`.
    pub uid: String,
    /// The `metadata.generation` this resolution was produced from.
    pub generation: i64,
    /// What this resolution is for.
    pub role: DestinationRole,
    /// The immutable location and transport.
    pub location: DestinationLocation,
    /// `sha256:<hex>` over the canonical location — the value a frozen plan is
    /// compared against and a recovery point is indexed by.
    pub location_digest: String,
    /// `s3://<bucket>[/<prefix>]`.
    pub canonical_url: String,
    /// The CA bundle's `ConfigMap` name and key, when one is declared.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ca_bundle: Option<CaBundleReference>,
    /// `sha256:<hex>` over the CA bytes, once they have been read.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ca_sha256: Option<String>,
    /// The CA bytes themselves, once they have been read. Public material: a
    /// certificate authority certificate is published by construction.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ca_pem: Option<Vec<u8>>,
    /// The grant for [`ResolvedDestination::role`], after defaulting.
    pub grant: ResolvedGrant,
}

/// Where a CA bundle is, as a reference.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CaBundleReference {
    /// The `ConfigMap`'s `metadata.name`, in the destination's namespace.
    pub config_map_name: String,
    /// The data key holding the PEM bundle.
    pub key: String,
}

/// What one destination contributes to one runner Job.
///
/// THREE LISTS, AND THE THIRD IS NOT A LIST. Literal variables and
/// `secretKeyRef` variables are appended by the caller wherever it already
/// appends its own; the ServiceAccount is a single Job field, which is why two
/// grants that want different ones is an [`CheckCode::ExecutionContextConflict`]
/// and not a merge.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct DestinationEnv {
    /// Literal `name=value` variables, in a stable order.
    pub literals: Vec<(String, String)>,
    /// Variables the kubelet projects from a Secret in the Job's namespace.
    pub from_secret: Vec<EnvFromSecret>,
    /// The ServiceAccount the Job must run as, for a workload-identity grant.
    pub service_account_name: Option<String>,
}

impl DestinationEnv {
    /// Every literal variable's name, for a guard that asserts what is ABSENT.
    #[must_use]
    pub fn literal_names(&self) -> BTreeSet<&str> {
        self.literals.iter().map(|(n, _)| n.as_str()).collect()
    }

    /// One literal's value.
    #[must_use]
    pub fn literal(&self, name: &str) -> Option<&str> {
        self.literals
            .iter()
            .find(|(n, _)| n == name)
            .map(|(_, v)| v.as_str())
    }

    /// Every variable name this contributes, projected and literal alike.
    #[must_use]
    pub fn names(&self) -> BTreeSet<&str> {
        let mut out = self.literal_names();
        out.extend(self.from_secret.iter().map(|e| e.name.as_str()));
        out
    }
}

// ---------------------------------------------------------------------------
// Reading the spec
// ---------------------------------------------------------------------------

/// The pure [`DestinationLocation`] a `BackupDestination.spec` names.
///
/// The ONE place the CRD's `storage` block and `transport.security` become the
/// pure layer's value. Everything that reasons about the location — the digest,
/// the two `StorageUrl`s, R3–R6 — goes through here, so there is no second
/// reading of the same fields.
#[must_use]
pub fn location_of(dest: &BackupDestination) -> DestinationLocation {
    let storage = &dest.spec.storage;
    DestinationLocation {
        provider: storage.provider,
        bucket: storage.bucket.clone(),
        prefix: storage.prefix.clone(),
        region: storage.region.clone(),
        endpoint: storage.endpoint.clone(),
        addressing: storage.addressing,
        transport: dest.spec.transport.security,
    }
}

/// Whether a persisted `BackupDestination` carries a `Valid=True` condition
/// computed from its CURRENT generation.
///
/// **GENERATION-AWARE ON PURPOSE.** A `Valid=True` left over from the previous
/// generation says nothing about the spec that is there now — an edit that
/// added an unreadable CA bundle would be used by every run that started before
/// the reconciler caught up. `observedGeneration` is what makes the answer
/// about the object a caller is holding.
#[must_use]
pub fn is_valid_now(dest: &BackupDestination) -> bool {
    let Some(status) = dest.status.as_ref() else {
        return false;
    };
    if status.observed_generation != dest.metadata.generation {
        return false;
    }
    status.conditions.as_ref().is_some_and(|cs| {
        cs.iter()
            .any(|c| c.r#type == CONDITION_VALID && c.status == "True")
    })
}

// ---------------------------------------------------------------------------
// The resolver
// ---------------------------------------------------------------------------

/// Resolve one `BackupDestination` for one role — D2 §3.4.
///
/// # The order of the checks, and why it is this order
///
/// 1. **Identity.** Namespace, UID and generation. Every reference below is
///    resolved in that namespace, and the freeze records that UID: a
///    deleted-and-recreated destination of the same name is a different
///    destination.
/// 2. **The location re-validated.** R3–R6 and the bucket and region patterns
///    are re-evaluated HERE, over what the API server actually stored, and not
///    trusted from CEL. An object admitted by an older CRD revision — installed
///    before a rule existed, or by an administrator who applied the CRD by hand
///    — is still refused, and the operator gets the field name.
/// 3. **Engine compatibility** (G4). `VirtualHosted` with a custom endpoint is
///    a setting engine 0.21.0 cannot honour, so it is refused rather than
///    silently served as path-style (defect **ENGINE-PATHSTYLE**).
/// 4. **The object's own verdict.** `Valid=True` at the current generation.
///    AFTER the two checks above, so a destination whose reconciler has not run
///    yet still reports the real problem rather than "not valid yet".
/// 5. **The grant**, after defaulting.
///
/// # The role defaulting, in one place
///
/// * `ArchiveRead` falls back to `archiveWrite`.
/// * `EvidenceWrite` falls back to `archiveWrite`.
/// * `EvidenceRead` resolves `ArchiveReadGrant` to the EXPLICIT `archiveRead`
///   (R9 refuses the mode without one, and this refuses it again).
/// * `EvidenceRead` absent is [`ResolvedGrant::NotConfigured`] — a defined
///   answer, never a fall-back to a wider grant.
/// * `ArchiveWrite` is required; it cannot default to anything.
///
/// # Errors
///
/// [`DestinationRefusal`], whose [`DestinationRefusal::is_hold`] says whether
/// the caller should requeue or fail terminally.
pub fn resolve(
    dest: &BackupDestination,
    role: DestinationRole,
    policy: &Policy,
) -> Result<ResolvedDestination, DestinationRefusal> {
    let name = dest.name_any();
    let namespace = dest.namespace().filter(|n| !n.is_empty()).ok_or_else(|| {
        DestinationRefusal::new(
            CheckCode::DestinationNotValid,
            "metadata.namespace",
            format!(
                "BackupDestination {name} carries no metadata.namespace, so there is no \
                 namespace to resolve its Secret and ConfigMap references in"
            ),
        )
    })?;
    let uid = dest.uid().filter(|u| !u.is_empty()).ok_or_else(|| {
        DestinationRefusal::new(
            CheckCode::DestinationNotValid,
            "metadata.uid",
            format!(
                "BackupDestination {namespace}/{name} carries no metadata.uid; a run freezes the \
                 UID so that a deleted-and-recreated destination of the same name is a different \
                 destination"
            ),
        )
    })?;
    let generation = dest.metadata.generation.unwrap_or(0);

    let location = location_of(dest);
    if let Err(errors) = validate(&location) {
        let first = errors.first().expect("validate returns a non-empty Vec");
        return Err(DestinationRefusal::new(
            CheckCode::DestinationNotValid,
            first.field.clone(),
            format!(
                "BackupDestination {namespace}/{name} is not a usable location ({}): {}",
                first.rule, first.message
            ),
        ));
    }
    if let Err(message) = validate_ca_bundle(
        dest.spec.transport.security,
        dest.spec.transport.ca_bundle.is_some(),
    ) {
        return Err(DestinationRefusal::new(
            CheckCode::DestinationNotValid,
            message.field,
            format!("BackupDestination {namespace}/{name}: {}", message.message),
        ));
    }
    if let Err(why) = engine_compatible(&location) {
        return Err(DestinationRefusal::new(
            CheckCode::AddressingUnsupportedByEngine,
            "spec.storage.addressing",
            format!("BackupDestination {namespace}/{name}: {why}"),
        ));
    }
    if !is_valid_now(dest) {
        return Err(DestinationRefusal::new(
            CheckCode::DestinationNotValid,
            "status.conditions[type=Valid]",
            format!(
                "BackupDestination {namespace}/{name} has no Valid=True condition for \
                 generation {generation}; nothing is created while the destination's own \
                 verdict is missing or stale"
            ),
        ));
    }

    let grant = resolve_grant(dest, role, policy, &namespace, &name, &location)?;

    Ok(ResolvedDestination {
        name,
        namespace,
        uid,
        generation,
        role,
        location_digest: location.location_digest(),
        canonical_url: location.canonical_url(),
        location,
        ca_bundle: dest
            .spec
            .transport
            .ca_bundle
            .as_ref()
            .map(|c| CaBundleReference {
                config_map_name: c.config_map_name.clone(),
                key: c.key.clone(),
            }),
        ca_sha256: None,
        ca_pem: None,
        grant,
    })
}

fn resolve_grant(
    dest: &BackupDestination,
    role: DestinationRole,
    policy: &Policy,
    namespace: &str,
    name: &str,
    location: &DestinationLocation,
) -> Result<ResolvedGrant, DestinationRefusal> {
    let access = &dest.spec.access;
    match role {
        DestinationRole::ArchiveWrite => grant_from(
            &access.archive_write,
            "spec.access.archiveWrite",
            namespace,
            name,
        ),
        // DEFAULTING IS A FALL-BACK TO THE WRITE GRANT AND NOT TO "ANYTHING
        // AVAILABLE": an absent read grant means the operator did not separate
        // the principals, which is the pre-destination status quo, not a
        // widening.
        DestinationRole::ArchiveRead => {
            let (grant, field) = match access.archive_read.as_ref() {
                Some(g) => (g, "spec.access.archiveRead"),
                None => (&access.archive_write, "spec.access.archiveWrite"),
            };
            grant_from(grant, field, namespace, name)
        }
        DestinationRole::EvidenceWrite => {
            let (grant, field) = match access.evidence_write.as_ref() {
                Some(g) => (g, "spec.access.evidenceWrite"),
                None => (&access.archive_write, "spec.access.archiveWrite"),
            };
            grant_from(grant, field, namespace, name)
        }
        DestinationRole::EvidenceRead => {
            evidence_read_grant(dest, policy, namespace, name, location)
        }
    }
}

fn evidence_read_grant(
    dest: &BackupDestination,
    policy: &Policy,
    namespace: &str,
    name: &str,
    location: &DestinationLocation,
) -> Result<ResolvedGrant, DestinationRefusal> {
    let access = &dest.spec.access;
    // ABSENT IS AN ANSWER. Verification reports `NotAttempted` with a detail
    // naming this field, which is a truthful "nobody configured a reader" —
    // not a green badge and not an error.
    let Some(read) = access.evidence_read.as_ref() else {
        return Ok(ResolvedGrant::NotConfigured);
    };
    match read.mode {
        EvidenceReadMode::SecretKeys => secret_grant(
            read.secret.as_ref(),
            "spec.access.evidenceRead",
            namespace,
            name,
        ),
        EvidenceReadMode::WorkloadIdentity => Ok(ResolvedGrant::WorkloadIdentity {
            service_account_name: workload_identity_name(
                read.workload_identity
                    .as_ref()
                    .map(|w| w.service_account_name.as_str()),
                "spec.access.evidenceRead",
                namespace,
                name,
            )?,
        }),
        // R9 IS RE-EVALUATED HERE, for the same reason R3-R6 are: a write grant
        // must never be reused to read evidence back, and an object admitted
        // before R9 existed would otherwise verify its own writer's bucket with
        // its own writer's key.
        EvidenceReadMode::ArchiveReadGrant => {
            let Some(archive_read) = access.archive_read.as_ref() else {
                return Err(DestinationRefusal::new(
                    CheckCode::DestinationRoleNotConfigured,
                    "spec.access.evidenceRead.mode",
                    format!(
                        "BackupDestination {namespace}/{name} sets evidenceRead mode \
                         ArchiveReadGrant with no spec.access.archiveRead; a write grant is \
                         never reused for verification (rule R9)"
                    ),
                ));
            };
            grant_from(archive_read, "spec.access.archiveRead", namespace, name)
        }
        EvidenceReadMode::ControllerIdentity => {
            if controller_identity_allowed(policy, location) {
                Ok(ResolvedGrant::ControllerIdentity)
            } else {
                Err(DestinationRefusal::new(
                    CheckCode::ControllerIdentityNotAllowlisted,
                    "spec.access.evidenceRead.mode",
                    format!(
                        "BackupDestination {namespace}/{name} asks the controller's own \
                         identity to read evidence at {}, which the installation policy's \
                         evidence.controllerIdentityLocations does not list. Only a chart or \
                         cluster administrator can add it; verification is NotAttempted \
                         meanwhile",
                        location.canonical_url()
                    ),
                ))
            }
        }
    }
}

/// Whether the installation policy allows the controller's own identity to read
/// evidence at this location — D2 §4.4's `evidence` block.
///
/// **BUCKET, ENDPOINT AND REGION, ALL THREE.** A bucket name is not an identity:
/// `lw-b` at `minio-a` and `lw-b` at `minio-b` are two different stores, and an
/// allowlist that matched on the bucket alone would let an operator point the
/// controller's principal at a bucket of that name on any endpoint they can
/// name. The comparison uses [`DestinationLocation::host_identity`] so that
/// `https://minio-b.ns.svc:9000` and `minio-b.ns.svc:9000` are the same host.
#[must_use]
pub fn controller_identity_allowed(policy: &Policy, location: &DestinationLocation) -> bool {
    let host = location.host_identity();
    let region = location.region.clone().unwrap_or_default();
    policy
        .evidence
        .controller_identity_locations
        .iter()
        .any(|entry| {
            let entry_host = if entry.endpoint.trim().is_empty() {
                format!("aws/{}", entry.region.trim())
            } else {
                entry
                    .endpoint
                    .trim()
                    .split_once("://")
                    .map_or(entry.endpoint.trim(), |(_, rest)| rest)
                    .trim_end_matches('/')
                    .to_ascii_lowercase()
            };
            entry_host == host
                && entry.region.trim() == region
                && entry.bucket.trim() == location.bucket
        })
}

fn grant_from(
    grant: &AccessGrant,
    field: &str,
    namespace: &str,
    name: &str,
) -> Result<ResolvedGrant, DestinationRefusal> {
    match grant.mode {
        AccessMode::SecretKeys => secret_grant(grant.secret.as_ref(), field, namespace, name),
        AccessMode::WorkloadIdentity => Ok(ResolvedGrant::WorkloadIdentity {
            service_account_name: workload_identity_name(
                grant
                    .workload_identity
                    .as_ref()
                    .map(|w| w.service_account_name.as_str()),
                field,
                namespace,
                name,
            )?,
        }),
    }
}

fn secret_grant(
    secret: Option<&S3SecretKeysRef>,
    field: &str,
    namespace: &str,
    name: &str,
) -> Result<ResolvedGrant, DestinationRefusal> {
    let Some(secret) = secret else {
        return Err(DestinationRefusal::new(
            CheckCode::DestinationRoleNotConfigured,
            format!("{field}.secret"),
            format!(
                "BackupDestination {namespace}/{name} sets {field}.mode SecretKeys with no \
                 {field}.secret (rule R7)"
            ),
        ));
    };
    // NAMESPACE ISOLATION, ENFORCED ON THE SPELLING. A `S3SecretKeysRef` has no
    // `namespace` field, so a cross-namespace reference cannot be EXPRESSED —
    // but `name: "team-b/logweir-s3"` is a name-shaped attempt at one, and a
    // kubelet would reject it in a way an operator reads as "Secret not found"
    // rather than as "you may not reach another namespace". Refusing it here
    // names the real rule. (`the_controller_never_reads_a_secret` is why this
    // is a SPELLING check and not a `get`: the controller holds no verb on
    // Secrets, and a design that gave it one is D2 §3.8 option B, rejected.)
    check_object_name(
        &secret.name,
        &format!("{field}.secret.name"),
        namespace,
        name,
    )?;
    for (value, key_field) in [
        (&secret.access_key_id_key, "accessKeyIdKey"),
        (&secret.secret_access_key_key, "secretAccessKeyKey"),
    ] {
        check_data_key(
            value,
            &format!("{field}.secret.{key_field}"),
            namespace,
            name,
        )?;
    }
    if let Some(token) = secret.session_token_key.as_ref() {
        check_data_key(
            token,
            &format!("{field}.secret.sessionTokenKey"),
            namespace,
            name,
        )?;
    }
    Ok(ResolvedGrant::SecretKeys {
        secret: secret.name.clone(),
        access_key_id_key: secret.access_key_id_key.clone(),
        secret_access_key_key: secret.secret_access_key_key.clone(),
        session_token_key: secret.session_token_key.clone(),
    })
}

fn workload_identity_name(
    configured: Option<&str>,
    field: &str,
    namespace: &str,
    name: &str,
) -> Result<String, DestinationRefusal> {
    let sa = configured
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .unwrap_or(crate::crds::backup_destination::DEFAULT_RUNNER_SERVICE_ACCOUNT);
    check_object_name(
        sa,
        &format!("{field}.workloadIdentity.serviceAccountName"),
        namespace,
        name,
    )?;
    Ok(sa.to_string())
}

fn check_object_name(
    value: &str,
    field: &str,
    namespace: &str,
    name: &str,
) -> Result<(), DestinationRefusal> {
    if crate::connection::is_dns1123_subdomain(value) {
        return Ok(());
    }
    Err(DestinationRefusal::new(
        CheckCode::DestinationRoleNotConfigured,
        field,
        format!(
            "BackupDestination {namespace}/{name}: `{field}` is `{value}`, which is not a \
             DNS-1123 subdomain. Every reference on a BackupDestination names an object in \
             {namespace} and in no other namespace; a `<namespace>/<name>` spelling is a \
             cross-namespace reference and is refused rather than resolved"
        ),
    ))
}

fn check_data_key(
    value: &str,
    field: &str,
    namespace: &str,
    name: &str,
) -> Result<(), DestinationRefusal> {
    if crate::connection::is_data_key(value) {
        return Ok(());
    }
    Err(DestinationRefusal::new(
        CheckCode::DestinationRoleNotConfigured,
        field,
        format!(
            "BackupDestination {namespace}/{name}: `{field}` is `{value}`, which is not a legal \
             Secret data key (`^[-._a-zA-Z0-9]{{1,253}}$`)"
        ),
    ))
}

// ---------------------------------------------------------------------------
// The CA bundle
// ---------------------------------------------------------------------------

/// What one read of the CA `ConfigMap` observed.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CaObservation {
    /// The destination declares no CA bundle.
    NotDeclared,
    /// The `ConfigMap` is absent.
    NotFound,
    /// The `ConfigMap` is there and carries no such key.
    KeyMissing,
    /// The bytes, verbatim.
    Present(Vec<u8>),
}

/// A CA bundle's bytes checked and digested, or the code that refuses them.
///
/// # What "parseable" means here, and what it deliberately does not
///
/// At least one `BEGIN CERTIFICATE` / `END CERTIFICATE` block whose body is
/// valid standard base64 decoding to a DER SEQUENCE (tag `0x30`). That is
/// enough to catch the failures an operator actually makes — pasting a PRIVATE
/// KEY, pasting a truncated block, pasting a `.der` file, or leaving the value
/// empty — and it is the same shape `object_store::Certificate::from_pem_bundle`
/// will reject later, so a bundle accepted here is one a store build has a
/// chance with. It is NOT certificate validation: expiry, chain and key usage
/// are the TLS handshake's business, and a controller that pre-judged them
/// would refuse a bundle that works.
///
/// # Errors
///
/// [`CheckCode::CaBundleTooLarge`] over [`CA_BUNDLE_MAX_BYTES`], or
/// [`CheckCode::CaBundleInvalid`].
pub fn check_ca_bundle(bytes: &[u8]) -> Result<String, CheckCode> {
    if bytes.len() > CA_BUNDLE_MAX_BYTES {
        return Err(CheckCode::CaBundleTooLarge);
    }
    if certificate_count(bytes) == 0 {
        return Err(CheckCode::CaBundleInvalid);
    }
    Ok(sha256_prefixed(bytes))
}

/// How many PEM certificates a bundle holds, by the rule [`check_ca_bundle`]
/// documents. `0` for anything that is not a bundle.
#[must_use]
pub fn certificate_count(bytes: &[u8]) -> usize {
    const BEGIN: &str = "-----BEGIN CERTIFICATE-----";
    const END: &str = "-----END CERTIFICATE-----";
    let Ok(text) = std::str::from_utf8(bytes) else {
        return 0;
    };
    let mut rest = text;
    let mut found = 0usize;
    while let Some(start) = rest.find(BEGIN) {
        rest = &rest[start + BEGIN.len()..];
        let Some(end) = rest.find(END) else {
            return found;
        };
        let body: String = rest[..end]
            .chars()
            .filter(|c| !c.is_ascii_whitespace())
            .collect();
        rest = &rest[end + END.len()..];
        match decode_base64(&body) {
            // A DER certificate is a SEQUENCE: tag 0x30. Anything else is a
            // different object that happened to be wrapped in CERTIFICATE
            // markers — a private key, a CSR, a base64 of nothing.
            Some(der) if der.first() == Some(&0x30) => found += 1,
            _ => return found,
        }
    }
    found
}

/// Standard base64 with padding, hand-written because this crate declares no
/// base64 dependency and Global Constraint 38 closes the workspace graph for a
/// twenty-line decoder.
fn decode_base64(s: &str) -> Option<Vec<u8>> {
    fn value(c: u8) -> Option<u32> {
        match c {
            b'A'..=b'Z' => Some(u32::from(c - b'A')),
            b'a'..=b'z' => Some(u32::from(c - b'a') + 26),
            b'0'..=b'9' => Some(u32::from(c - b'0') + 52),
            b'+' => Some(62),
            b'/' => Some(63),
            _ => None,
        }
    }
    let bytes = s.as_bytes();
    if bytes.is_empty() || bytes.len() % 4 != 0 {
        return None;
    }
    let mut out = Vec::with_capacity(bytes.len() / 4 * 3);
    let last = bytes.len() / 4 - 1;
    for (group, chunk) in bytes.chunks(4).enumerate() {
        // PADDING ONLY IN THE FINAL GROUP. Counting it per group accepted
        // `AA==AAAA`, which is not base64 at all — and a body that is not base64
        // is a body `object_store::Certificate::from_pem_bundle` will refuse
        // later, so accepting it here only made `CaBundleInvalid` fire at the
        // wrong moment.
        let pad = chunk.iter().rev().take_while(|c| **c == b'=').count();
        if pad > 2 || (pad > 0 && group != last) {
            return None;
        }
        let mut acc = 0u32;
        for (i, c) in chunk.iter().enumerate() {
            let v = if *c == b'=' {
                if i < 4 - pad {
                    return None;
                }
                0
            } else {
                value(*c)?
            };
            acc = (acc << 6) | v;
        }
        let triple = acc.to_be_bytes();
        out.push(triple[1]);
        if pad < 2 {
            out.push(triple[2]);
        }
        if pad < 1 {
            out.push(triple[3]);
        }
    }
    Some(out)
}

impl ResolvedDestination {
    /// Attach the CA bytes this destination declares, after checking them.
    ///
    /// # Errors
    ///
    /// A [`DestinationRefusal`] carrying the same code the reconciler writes on
    /// `status`, so a run and the object agree about why the bundle is unusable.
    pub fn with_ca(mut self, observation: &CaObservation) -> Result<Self, DestinationRefusal> {
        let Some(reference) = self.ca_bundle.clone() else {
            return Ok(self);
        };
        let where_ = format!(
            "BackupDestination {}/{} names spec.transport.caBundle ConfigMap {} key {}",
            self.namespace, self.name, reference.config_map_name, reference.key
        );
        match observation {
            CaObservation::NotDeclared => Ok(self),
            CaObservation::NotFound => Err(DestinationRefusal::new(
                CheckCode::CaBundleNotFound,
                "spec.transport.caBundle.configMapName",
                format!(
                    "{where_}, and that ConfigMap does not exist in {}",
                    self.namespace
                ),
            )),
            CaObservation::KeyMissing => Err(DestinationRefusal::new(
                CheckCode::CaBundleKeyMissing,
                "spec.transport.caBundle.key",
                format!("{where_}, and that ConfigMap carries no such key"),
            )),
            CaObservation::Present(bytes) => match check_ca_bundle(bytes) {
                Ok(digest) => {
                    self.ca_sha256 = Some(digest);
                    self.ca_pem = Some(bytes.clone());
                    Ok(self)
                }
                Err(code) => Err(DestinationRefusal::new(
                    code,
                    "spec.transport.caBundle.key",
                    format!(
                        "{where_}, and its {} bytes are not a usable PEM bundle ({})",
                        bytes.len(),
                        code.as_str()
                    ),
                )),
            },
        }
    }

    /// The archive `StorageUrl` a plan block and a `Store` are built from.
    #[must_use]
    pub fn plan_storage(&self) -> StorageUrl {
        self.location.archive_storage_url()
    }

    /// The evidence `StorageUrl`, rooted at Global Constraint 6's `logweir/`.
    #[must_use]
    pub fn evidence_storage(&self) -> StorageUrl {
        self.location.evidence_storage_url()
    }

    /// A Job built from this resolution may run in the destination's own
    /// namespace and in no other.
    ///
    /// THE PROPERTY THE RESOLUTION CANNOT ENFORCE BY ITSELF. Every reference in
    /// [`ResolvedDestination`] is a bare object name, and the kubelet resolves a
    /// bare name in the POD's namespace. A Job placed in another namespace
    /// therefore projects whatever object happens to carry that name THERE — a
    /// silent credential substitution, not an error. The caller that chooses the
    /// namespace calls this; `check_job_namespace` is the same contract
    /// [`crate::connection::ResolvedConnection::check_job_namespace`] holds for
    /// a connection.
    ///
    /// # Errors
    ///
    /// [`CheckCode::ExecutionContextConflict`], naming both namespaces.
    pub fn check_job_namespace(&self, job_namespace: &str) -> Result<(), DestinationRefusal> {
        if job_namespace == self.namespace {
            return Ok(());
        }
        Err(DestinationRefusal::new(
            CheckCode::ExecutionContextConflict,
            "metadata.namespace",
            format!(
                "BackupDestination {}/{} resolves its Secret and ConfigMap references in \
                 {}, and this Job would run in {job_namespace}: a bare reference is resolved \
                 by the kubelet in the POD's namespace, so running there would project \
                 whichever objects happen to carry those names in {job_namespace}",
                self.namespace, self.name, self.namespace
            ),
        ))
    }

    /// The COMPLETE, EXPLICIT archive environment this destination contributes
    /// to a runner Job — D2 §3.5.
    ///
    /// # Every variable, and why it is here
    ///
    /// * `LOGWEIR_STORE_CONTRACT_VERSION` — the handshake. A runner that does
    ///   not know the contract refuses before it dispatches.
    /// * `LOGWEIR_ARCHIVE_CREDENTIALS` — which provider the runner builds with.
    /// * `AWS_ALLOW_HTTP` — `"true"` if and only if
    ///   `spec.transport.security == InsecureHTTP`. ALWAYS RENDERED, including
    ///   the `"false"` case, because an ABSENT variable is one an ambient
    ///   `AWS_ALLOW_HTTP=true` in the pod's environment could still answer.
    /// * `AWS_VIRTUAL_HOSTED_STYLE_REQUEST` — from `spec.storage.addressing`
    ///   alone, and it does not touch the line above. Two fields, two answers.
    /// * `AWS_METADATA_ENDPOINT` — a dead loopback, so a missing workload
    ///   identity cannot fall through to the node's instance role (G16).
    /// * `AWS_REGION` — only when the destination names one.
    /// * `LOGWEIR_ARCHIVE_CA_FILE` — only with a CA bundle; the bytes are frozen
    ///   into the run's own plan `ConfigMap`, never re-read at Job time.
    ///
    /// `AWS_ENDPOINT_URL` is absent by construction: see
    /// [`AWS_ENDPOINT_URL_ENV`].
    #[must_use]
    pub fn job_env(&self) -> DestinationEnv {
        let mut env = DestinationEnv {
            literals: vec![
                (
                    STORE_CONTRACT_VERSION_ENV.to_string(),
                    STORE_CONTRACT_VERSION.to_string(),
                ),
                (
                    AWS_ALLOW_HTTP_ENV.to_string(),
                    // FROM THE TRANSPORT AND FROM NOTHING ELSE (D-SEAMS S5).
                    bool_env(self.location.transport.allows_plaintext_http()),
                ),
                (
                    AWS_VIRTUAL_HOSTED_ENV.to_string(),
                    // FROM THE ADDRESSING AND FROM NOTHING ELSE.
                    bool_env(!self.location.addressing.is_path_style()),
                ),
                (
                    AWS_METADATA_ENDPOINT_ENV.to_string(),
                    logweir_store::DEAD_METADATA_ENDPOINT.to_string(),
                ),
            ],
            from_secret: Vec::new(),
            service_account_name: None,
        };
        if let Some(region) = self.location.region.as_ref() {
            env.literals
                .push((AWS_REGION_ENV.to_string(), region.clone()));
        }
        if self.ca_bundle.is_some() {
            env.literals.push((
                ARCHIVE_CA_FILE_ENV.to_string(),
                format!("{PLAN_MOUNT_PATH}/{ARCHIVE_CA_PLAN_KEY}"),
            ));
        }
        match &self.grant {
            ResolvedGrant::SecretKeys {
                secret,
                access_key_id_key,
                secret_access_key_key,
                session_token_key,
            } => {
                env.literals.push((
                    ARCHIVE_CREDENTIALS_ENV.to_string(),
                    CREDENTIALS_STATIC.to_string(),
                ));
                env.from_secret.push(EnvFromSecret {
                    name: AWS_ACCESS_KEY_ID_ENV.to_string(),
                    secret_name: secret.clone(),
                    key: access_key_id_key.clone(),
                });
                env.from_secret.push(EnvFromSecret {
                    name: AWS_SECRET_ACCESS_KEY_ENV.to_string(),
                    secret_name: secret.clone(),
                    key: secret_access_key_key.clone(),
                });
                if let Some(token) = session_token_key {
                    env.from_secret.push(EnvFromSecret {
                        name: AWS_SESSION_TOKEN_ENV.to_string(),
                        secret_name: secret.clone(),
                        key: token.clone(),
                    });
                }
            }
            ResolvedGrant::WorkloadIdentity {
                service_account_name,
            } => {
                env.literals.push((
                    ARCHIVE_CREDENTIALS_ENV.to_string(),
                    CREDENTIALS_WORKLOAD_IDENTITY.to_string(),
                ));
                env.service_account_name = Some(service_account_name.clone());
            }
            // Neither reaches a runner Job: `ControllerIdentity` is read by the
            // controller's own cache and `NotConfigured` is read by nobody.
            ResolvedGrant::ControllerIdentity | ResolvedGrant::NotConfigured => {}
        }
        env.literals.sort_by(|a, b| a.0.cmp(&b.0));
        env
    }

    /// The EVIDENCE-side environment for a Job that already carries `archive`'s
    /// [`ResolvedDestination::job_env`] — D2 §3.5's restore paragraph.
    ///
    /// # The three cases, and the one refusal
    ///
    /// * The evidence grant IS the archive grant (same Secret and keys, or the
    ///   same ServiceAccount): `LOGWEIR_EVIDENCE_CREDENTIALS=archive`, and no
    ///   second credential is projected.
    /// * A different Secret: `=static`, plus the three `LOGWEIR_EVIDENCE_AWS_*`
    ///   variables. They are SEPARATELY NAMED so neither store's credential can
    ///   shadow the other's — `AWS_*` is the archive's.
    /// * A workload identity beside a static archive grant: `=workloadIdentity`,
    ///   and the runner builds the evidence store WITHOUT `from_env`, copying
    ///   only the web-identity and container-credential variables. Otherwise
    ///   object_store's chain would put the static archive keys first (G16).
    /// * Two DIFFERENT workload-identity ServiceAccounts in one pod:
    ///   [`CheckCode::ExecutionContextConflict`]. A pod has exactly one
    ///   ServiceAccount, so this is a fact about Kubernetes and not a policy.
    ///
    /// # Errors
    ///
    /// [`CheckCode::ExecutionContextConflict`] for two ServiceAccounts in one
    /// pod, or for two destinations in two namespaces.
    pub fn evidence_env(
        &self,
        archive: &ResolvedDestination,
    ) -> Result<DestinationEnv, DestinationRefusal> {
        archive.check_job_namespace(&self.namespace)?;
        let mut env = DestinationEnv::default();
        if self.ca_bundle.is_some() {
            env.literals.push((
                EVIDENCE_CA_FILE_ENV.to_string(),
                format!("{PLAN_MOUNT_PATH}/{EVIDENCE_CA_PLAN_KEY}"),
            ));
        }
        match (&self.grant, &archive.grant) {
            (a, b) if a == b => {
                env.literals.push((
                    EVIDENCE_CREDENTIALS_ENV.to_string(),
                    CREDENTIALS_ARCHIVE.to_string(),
                ));
            }
            (
                ResolvedGrant::SecretKeys {
                    secret,
                    access_key_id_key,
                    secret_access_key_key,
                    session_token_key,
                },
                _,
            ) => {
                env.literals.push((
                    EVIDENCE_CREDENTIALS_ENV.to_string(),
                    CREDENTIALS_STATIC.to_string(),
                ));
                env.from_secret.push(EnvFromSecret {
                    name: EVIDENCE_ACCESS_KEY_ID_ENV.to_string(),
                    secret_name: secret.clone(),
                    key: access_key_id_key.clone(),
                });
                env.from_secret.push(EnvFromSecret {
                    name: EVIDENCE_SECRET_ACCESS_KEY_ENV.to_string(),
                    secret_name: secret.clone(),
                    key: secret_access_key_key.clone(),
                });
                if let Some(token) = session_token_key {
                    env.from_secret.push(EnvFromSecret {
                        name: EVIDENCE_SESSION_TOKEN_ENV.to_string(),
                        secret_name: secret.clone(),
                        key: token.clone(),
                    });
                }
            }
            (
                ResolvedGrant::WorkloadIdentity {
                    service_account_name,
                },
                other,
            ) => {
                if let Some(archive_sa) = other.service_account_name() {
                    if archive_sa != service_account_name {
                        return Err(DestinationRefusal::new(
                            CheckCode::ExecutionContextConflict,
                            "spec.access",
                            format!(
                                "the archive grant of BackupDestination {}/{} runs as \
                                 ServiceAccount {archive_sa} and the evidence grant of \
                                 BackupDestination {}/{} runs as {service_account_name}; one \
                                 pod has one ServiceAccount, so these two grants cannot be \
                                 satisfied by one Job",
                                archive.namespace, archive.name, self.namespace, self.name
                            ),
                        ));
                    }
                }
                env.literals.push((
                    EVIDENCE_CREDENTIALS_ENV.to_string(),
                    CREDENTIALS_WORKLOAD_IDENTITY.to_string(),
                ));
                env.service_account_name = Some(service_account_name.clone());
            }
            (ResolvedGrant::ControllerIdentity | ResolvedGrant::NotConfigured, _) => {
                return Err(DestinationRefusal::new(
                    CheckCode::DestinationRoleNotConfigured,
                    "spec.access.evidenceWrite",
                    format!(
                        "BackupDestination {}/{} has no evidence-write grant a Job could use \
                         ({:?})",
                        self.namespace, self.name, self.grant
                    ),
                ));
            }
        }
        env.literals.sort_by(|a, b| a.0.cmp(&b.0));
        Ok(env)
    }

    /// The value a run FREEZES — seam **S4**.
    #[must_use]
    pub fn snapshot(&self) -> ResolvedDestinationSnapshot {
        ResolvedDestinationSnapshot {
            name: self.name.clone(),
            uid: self.uid.clone(),
            generation: self.generation,
            location_digest: self.location_digest.clone(),
            archive_storage: self.plan_storage(),
            evidence_storage: self.evidence_storage(),
            transport: self.location.transport,
            addressing: self.location.addressing,
            ca_sha256: self.ca_sha256.clone(),
            grant: self.grant.clone(),
        }
    }
}

fn bool_env(value: bool) -> String {
    if value { "true" } else { "false" }.to_string()
}

// ---------------------------------------------------------------------------
// The frozen snapshot (seam S4)
// ---------------------------------------------------------------------------

/// The resolved destination as one run freezes it — D2 §3.7, seam **S4**.
///
/// # BEFORE W10 FREEZES THIS, READ THESE TWO LINES
///
/// 1. **The key spellings are settled and pinned.** They were not, in the first
///    version of this type: `#[serde(rename_all)]` on an enum renames VARIANTS
///    and not struct-variant FIELDS, so `grant` carried `access_key_id_key`
///    inside a camelCase document. `rename_all_fields` fixes it and
///    `tests/destination_controller.rs::the_snapshot_key_spellings_are_pinned`
///    is the golden-bytes row that keeps it. Do not freeze a build without that
///    row green: once these bytes are in an immutable `ConfigMap` that is
///    re-encoded and compared on every later pass, renaming one key turns every
///    running `Backup` into a `TERMINAL_STATE_PLAN_CONFIG_MAP_CONFLICT`.
/// 2. **Rollback is an open ruling, not an oversight.** Adding this block under
///    the existing [`crate::backup_execution::INPUTS_VERSION`] with
///    `#[serde(default)]` keeps FORWARD compatibility — a `v1` document with no
///    `destination` still loads. It does NOT give backward compatibility: every
///    struct in that module is `deny_unknown_fields` and the loader requires the
///    version string exactly, so an OLDER controller handed a document carrying
///    `destination` refuses the run. That is fail-closed and probably right, but
///    it is a rollback behaviour nobody has written down. W10 (with the
///    orchestrator) decides between "additive under the current version, with the
///    refusal recorded in `docs/stability.md`" and a version bump. This type is
///    fit for either; nothing here presumes one.
///
/// # It is a BLOCK inside PLAT-06.1's document, not a second freeze
///
/// Seam S4 is explicit: `execution-inputs.json` is the single frozen grammar
/// and the same immutable `ConfigMap`. **W10** adds
/// `destination: Option<ResolvedDestinationSnapshot>` to
/// [`crate::backup_execution::BackupExecutionInputs`] additively, keeps every
/// snapshot written without it loadable, and extends that module's existing
/// validation rather than adding a parallel one. This type and
/// [`ResolvedDestinationSnapshot::canonical_bytes`] are what it adds; nothing
/// here writes a `ConfigMap`.
///
/// # What it carries, and what it deliberately does not
///
/// It carries the UID and generation (so a destination deleted and recreated
/// under the same name is a different input), the location digest (so a
/// recovery point can be matched to a destination later), both `StorageUrl`s
/// (so the Job is rendered from the snapshot and never from the live object),
/// the CA DIGEST and the grant REFERENCES.
///
/// It carries NO credential value and no CA bytes. The bytes live beside
/// `backup.yaml` in the same immutable plan `ConfigMap` under
/// [`ARCHIVE_CA_PLAN_KEY`]; this digest is what binds them, so a rotated CA
/// cannot be swapped into a run that already exists without the snapshot
/// disagreeing.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ResolvedDestinationSnapshot {
    /// The destination's `metadata.name`.
    pub name: String,
    /// Its `metadata.uid`.
    pub uid: String,
    /// Its `metadata.generation` at resolution time.
    pub generation: i64,
    /// `sha256:<hex>` over the canonical location.
    pub location_digest: String,
    /// The archive storage block the plan carries.
    pub archive_storage: StorageUrl,
    /// The evidence storage block, rooted at `logweir/`.
    pub evidence_storage: StorageUrl,
    /// The declared transport security.
    pub transport: TransportSecurity,
    /// The declared addressing style.
    pub addressing: Addressing,
    /// `sha256:<hex>` over the CA bytes frozen beside this snapshot.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ca_sha256: Option<String>,
    /// The grant for the role this snapshot was resolved for.
    pub grant: ResolvedGrant,
}

impl ResolvedDestinationSnapshot {
    /// The ONE canonical encoding of this block — `logweir_core::det_json`, the
    /// same encoder [`crate::backup_execution::canonical_inputs`] uses for the
    /// document this block sits inside.
    ///
    /// ONE ENCODER AND NOT TWO. The frozen document is re-encoded and compared
    /// byte for byte on every later pass; a block with its own serialiser would
    /// re-encode differently the first time a field was added and turn every
    /// running Backup into a `PlanConfigMapConflict`.
    ///
    /// # Errors
    ///
    /// The encoder's own error, which a typed value built by this module cannot
    /// produce.
    pub fn canonical_bytes(&self) -> Result<Vec<u8>, logweir_core::det_json::DetJsonError> {
        logweir_core::det_json::to_deterministic_json(self)
    }

    /// `sha256:<hex>` over [`ResolvedDestinationSnapshot::canonical_bytes`].
    ///
    /// # Errors
    ///
    /// See [`ResolvedDestinationSnapshot::canonical_bytes`].
    pub fn digest(&self) -> Result<String, logweir_core::det_json::DetJsonError> {
        Ok(sha256_prefixed(&self.canonical_bytes()?))
    }
}

// ---------------------------------------------------------------------------
// The retention guard (D2 §3.10, G14)
// ---------------------------------------------------------------------------

/// Whether the controller's ONE global archive handle may be used to report
/// retention for a schedule — D2 §3.10, grounding **G14**.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RetentionScope {
    /// The schedule's archive is the global handle's archive. Report as today.
    GlobalHandleApplies,
    /// The schedule names a DIFFERENT bucket. No report, and one INFO line
    /// naming both buckets.
    WrongBucket {
        /// The bucket the schedule writes to.
        schedule_bucket: String,
        /// The bucket the controller's handle reads.
        handle_bucket: String,
    },
    /// The schedule is destination-backed. No report until PLAT-16.1 adds the
    /// per-destination archive-inventory check kind.
    DestinationBacked,
    /// No global handle is configured at all.
    NoHandle,
}

impl RetentionScope {
    /// Whether a retention report may be written.
    #[must_use]
    pub fn reports(&self) -> bool {
        matches!(self, Self::GlobalHandleApplies)
    }
}

/// Decide whether a schedule's retention report may be computed through the
/// global handle — the guard defect **RET-WRONGBUCKET** needs.
///
/// # The defect, exactly
///
/// `controllers/backup_schedule.rs` lists manifests through the controller's
/// ONE global handle while rendering `aws s3 rm` / `mc rm` commands for the
/// SCHEDULE's own `archive.url`. On an installation with two buckets that is a
/// report about bucket A printed as if it described bucket B — and the commands
/// it prints name keys in B that were listed in A. An operator who runs them
/// deletes the wrong objects, or nothing, and either way the report was never
/// about their catalogue.
///
/// # Why the comparison is the BUCKET and not the whole URL
///
/// A schedule's `archive.url` carries a prefix; the handle's URL carries the
/// root the operator configured. Two schedules writing `s3://kb/team-a` and
/// `s3://kb/team-b` through a handle on `s3://kb` are both describable by that
/// handle — the listing is prefix-scoped by the report itself. A different
/// BUCKET is the case where the listing cannot be about the schedule at all.
///
/// # Arguments
///
/// `schedule_archive_url` is `BackupSchedule.spec.archive.url`;
/// `handle_archive_url` is [`crate::retention::ARCHIVE_URL_ENV`]'s value, or
/// `None` when this controller holds no handle; `destination_backed` is whether
/// the schedule carries a `destinationRef`.
///
/// **W10 wires this into `controllers/backup_schedule.rs`.** It is a pure
/// function here so the property has a test that names a bucket rather than a
/// route table.
#[must_use]
pub fn retention_scope(
    schedule_archive_url: &str,
    handle_archive_url: Option<&str>,
    destination_backed: bool,
) -> RetentionScope {
    if destination_backed {
        return RetentionScope::DestinationBacked;
    }
    let Some(handle) = handle_archive_url else {
        return RetentionScope::NoHandle;
    };
    let schedule_bucket = bucket_of(schedule_archive_url);
    let handle_bucket = bucket_of(handle);
    if !schedule_bucket.is_empty() && schedule_bucket == handle_bucket {
        RetentionScope::GlobalHandleApplies
    } else {
        RetentionScope::WrongBucket {
            schedule_bucket,
            handle_bucket,
        }
    }
}

/// The bucket (or container, or filesystem root) an object-store URL names.
///
/// Derived from [`crate::retention::storage_url_for`] rather than re-parsed, so
/// the two cannot disagree about which bucket a URL names — which is the exact
/// failure class this guard exists to close.
fn bucket_of(url: &str) -> String {
    match crate::retention::storage_url_for(url) {
        Ok(StorageUrl::S3 { bucket, .. } | StorageUrl::Gcs { bucket, .. }) => bucket,
        Ok(StorageUrl::Azure {
            account_name,
            container_name,
            ..
        }) => format!("{account_name}/{container_name}"),
        Ok(StorageUrl::Filesystem { path }) => path.to_string_lossy().into_owned(),
        // AN UNREADABLE URL IS NOT THE SAME BUCKET AS ANYTHING. Returning the
        // empty string here makes `retention_scope` answer `WrongBucket`, which
        // withholds the report — the safe direction. The `Backup` path reports
        // the URL itself as `ArchiveUrlUnreadable`; a retention REPORT is not
        // the place to raise it a second time.
        Err(_) => String::new(),
    }
}

// ---------------------------------------------------------------------------
// The API-server half
// ---------------------------------------------------------------------------

/// Read one `BackupDestination` by name, resolve it for `role`, and attach its
/// CA bundle.
///
/// NAMESPACE-LOCAL BY CONSTRUCTION: `Api::namespaced` over the namespace the
/// caller is reconciling in. A `destinationRef` is a [`crate::crds::LocalRef`]
/// with no namespace field, and this is the call that makes that structural
/// fact a runtime one.
///
/// # Errors
///
/// [`ResolveError::Refused`] with [`CheckCode::DestinationNotFound`] for an
/// absent object, whatever [`resolve`] refuses, or [`ResolveError::Api`].
pub async fn resolve_ref(
    client: &kube::Client,
    namespace: &str,
    name: &str,
    role: DestinationRole,
    policy: &Policy,
) -> Result<ResolvedDestination, ResolveError> {
    let api: Api<BackupDestination> = Api::namespaced(client.clone(), namespace);
    let Some(dest) = api.get_opt(name).await? else {
        return Err(DestinationRefusal::new(
            CheckCode::DestinationNotFound,
            "spec.destinationRef.name",
            format!(
                "namespace {namespace} has no BackupDestination named {name}; a destinationRef \
                 is namespace-local and is never resolved in another namespace"
            ),
        )
        .into());
    };
    let resolved = resolve(&dest, role, policy)?;
    let observation = read_ca_bundle(client, &resolved).await?;
    Ok(resolved.with_ca(&observation)?)
}

/// Read the CA bundle a resolved destination declares.
///
/// # Errors
///
/// [`kube::Error`] from the `get`. An absent `ConfigMap` is
/// [`CaObservation::NotFound`] and not an error.
pub async fn read_ca_bundle(
    client: &kube::Client,
    resolved: &ResolvedDestination,
) -> Result<CaObservation, kube::Error> {
    let Some(reference) = resolved.ca_bundle.as_ref() else {
        return Ok(CaObservation::NotDeclared);
    };
    let maps: Api<ConfigMap> = Api::namespaced(client.clone(), &resolved.namespace);
    let Some(cm) = maps.get_opt(&reference.config_map_name).await? else {
        return Ok(CaObservation::NotFound);
    };
    if let Some(pem) = cm.data.as_ref().and_then(|d| d.get(&reference.key)) {
        return Ok(CaObservation::Present(pem.clone().into_bytes()));
    }
    // `binaryData` IS READ TOO, AND IT IS NOT A CONVENIENCE. The API server puts
    // a key in `binaryData` and not `data` whenever its value is not valid
    // UTF-8, which is exactly what `kubectl create configmap ca --from-file=ca.crt=ca.der`
    // produces. Reading `data` alone answered `CaBundleKeyMissing` — "that
    // ConfigMap carries no such key" — for a key the operator can see in
    // `kubectl get cm -o yaml`, and sent them looking for the wrong thing. Read
    // here, the bytes reach `check_ca_bundle`, which refuses them as
    // `CaBundleInvalid` with the message that names the real problem: a DER file
    // is not a PEM bundle.
    if let Some(bytes) = cm.binary_data.as_ref().and_then(|d| d.get(&reference.key)) {
        return Ok(CaObservation::Present(bytes.0.clone()));
    }
    Ok(CaObservation::KeyMissing)
}

// ---------------------------------------------------------------------------
// The status the reconciler writes (D2 §3.3)
// ---------------------------------------------------------------------------

/// One `BackupDestination` verdict — the pure half of the reconciler.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DestinationVerdict {
    /// Whether the `Valid` condition is `True`.
    pub valid: bool,
    /// The condition `reason`, which is also `status.reason`.
    pub reason: &'static str,
    /// The condition `message`. Names fields and objects, never a value.
    pub message: String,
    /// `s3://<bucket>[/<prefix>]`.
    pub canonical_url: String,
    /// `sha256:<hex>` over the canonical location.
    pub location_digest: String,
    /// `sha256:<hex>` over the CA bytes, when they were readable.
    pub ca_bundle_sha256: Option<String>,
}

/// The `Valid` reason for a destination whose every check passed.
pub const REASON_VALID: &str = "Valid";

/// Every reason [`evaluate`] can write, for a guard that asserts the set is
/// closed and for the CRD's own documentation.
///
/// A LITERAL TABLE, deliberately derived from no `match` the evaluator reads: a
/// list computed from the same code it checks moves with a mutant and asserts
/// nothing.
pub const DESTINATION_CONDITION_REASONS: [&str; 7] = [
    REASON_VALID,
    "AddressingUnsupportedByEngine",
    "CaBundleNotFound",
    "CaBundleKeyMissing",
    "CaBundleTooLarge",
    "CaBundleInvalid",
    "DestinationNotValid",
];

/// Decide one `BackupDestination`'s `Valid` condition — D2 §3.3.
///
/// A PURE FUNCTION OF THE OBJECT AND ONE OBSERVATION, for the reason every
/// evaluator in `controllers/` is one: the verdict is the property, and a test
/// that must build a route table to reach it is a test about `kube`.
///
/// # What it checks that CEL does not, and why
///
/// CEL rules R0–R9 are compiled by the API server and cannot change without a
/// CRD upgrade. These four can:
///
/// * **`AddressingUnsupportedByEngine`** depends on the ENGINE VERSION. Engine
///   0.21.0 forces path-style whenever an endpoint is set, so `VirtualHosted`
///   with an endpoint is unhonourable; a later engine may honour it, and that
///   must not require a new CRD.
/// * **The CA bundle's existence, key, size and content** depend on another
///   object, which CEL cannot read at all.
///
/// It ALSO re-evaluates R3–R6 through `logweir_core::destination::validate`, so
/// an object admitted by an older CRD revision is reported here rather than
/// discovered by a run.
#[must_use]
pub fn evaluate(dest: &BackupDestination, ca: &CaObservation) -> DestinationVerdict {
    let location = location_of(dest);
    let canonical_url = location.canonical_url();
    let location_digest = location.location_digest();
    let refuse = |reason: &'static str, message: String| DestinationVerdict {
        valid: false,
        reason,
        message,
        canonical_url: canonical_url.clone(),
        location_digest: location_digest.clone(),
        ca_bundle_sha256: None,
    };

    if let Err(errors) = validate(&location) {
        let names: Vec<String> = errors
            .iter()
            .map(|e| format!("{} ({}): {}", e.field, e.rule, e.message))
            .collect();
        return refuse(
            CheckCode::DestinationNotValid.as_str(),
            format!(
                "this BackupDestination was admitted by the API server but does not satisfy the \
                 location rules this controller enforces: {}",
                names.join("; ")
            ),
        );
    }
    if let Err(e) = validate_ca_bundle(
        dest.spec.transport.security,
        dest.spec.transport.ca_bundle.is_some(),
    ) {
        return refuse(CheckCode::DestinationNotValid.as_str(), e.message);
    }
    if let Err(why) = engine_compatible(&location) {
        return refuse(
            CheckCode::AddressingUnsupportedByEngine.as_str(),
            why.to_string(),
        );
    }

    let declared = dest.spec.transport.ca_bundle.as_ref();
    let ca_sha256 = match (declared, ca) {
        (None, _) => None,
        (Some(reference), CaObservation::NotDeclared | CaObservation::NotFound) => {
            return refuse(
                CheckCode::CaBundleNotFound.as_str(),
                format!(
                    "spec.transport.caBundle names ConfigMap {} in this namespace, and it does \
                     not exist. A CA bundle is PUBLIC material and is read from a ConfigMap \
                     rather than a Secret; create it, or remove spec.transport.caBundle to use \
                     the platform trust store",
                    reference.config_map_name
                ),
            );
        }
        (Some(reference), CaObservation::KeyMissing) => {
            return refuse(
                CheckCode::CaBundleKeyMissing.as_str(),
                format!(
                    "ConfigMap {} carries no key `{}`",
                    reference.config_map_name, reference.key
                ),
            );
        }
        (Some(reference), CaObservation::Present(bytes)) => match check_ca_bundle(bytes) {
            Ok(digest) => Some(digest),
            Err(code @ CheckCode::CaBundleTooLarge) => {
                return refuse(
                    code.as_str(),
                    format!(
                        "ConfigMap {} key `{}` holds {} bytes; a destination CA bundle is at \
                         most {CA_BUNDLE_MAX_BYTES} bytes, because every byte is copied into \
                         every run's immutable plan ConfigMap",
                        reference.config_map_name,
                        reference.key,
                        bytes.len()
                    ),
                );
            }
            Err(code) => {
                return refuse(
                    code.as_str(),
                    format!(
                        "ConfigMap {} key `{}` holds no parseable PEM certificate. A CA bundle \
                         is one or more `-----BEGIN CERTIFICATE-----` blocks; a private key, a \
                         DER file or a truncated block is refused here rather than at the \
                         first TLS handshake of a run",
                        reference.config_map_name, reference.key
                    ),
                );
            }
        },
    };

    DestinationVerdict {
        valid: true,
        reason: REASON_VALID,
        message: format!(
            "{canonical_url} over {}, {} addressing{}",
            dest.spec.transport.security.as_str(),
            dest.spec.storage.addressing.as_str(),
            match &ca_sha256 {
                Some(_) => ", with a private CA",
                None => ", platform trust store",
            }
        ),
        canonical_url,
        location_digest,
        ca_bundle_sha256: ca_sha256,
    }
}

/// The `observedAt` a verdict takes — `now` for a NEW verdict, the stored value
/// for an unchanged one (erratum E11(d)).
#[must_use]
pub fn observed_at_for(
    previous: Option<&crate::crds::backup_destination::BackupDestinationStatus>,
    verdict: &DestinationVerdict,
    now: DateTime<Utc>,
) -> DateTime<Utc> {
    match previous {
        Some(p)
            if p.reason.as_deref() == Some(verdict.reason)
                && p.location_digest.as_deref() == Some(verdict.location_digest.as_str())
                && p.ca_bundle_sha256.as_deref() == verdict.ca_bundle_sha256.as_deref() =>
        {
            p.observed_at.unwrap_or(now)
        }
        _ => now,
    }
}

/// The provider every destination in this build names, re-exported so a caller
/// that must spell it does not reach for the CRD module.
pub const PROVIDER: StorageProvider = StorageProvider::S3;
