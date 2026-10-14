//! The controller half of a saved destination: resolve one `BackupDestination`
//! into the complete, explicit object-store configuration one operation needs
//! (decision D2 §3.4, §3.5, §3.7, §3.10).
//!
//! [`logweir_core::destination`] owns everything that is a function of the
//! location alone: the closed enums, R3–R6, the engine-compatibility refusal,
//! the canonical URL, the location digest and the two `StorageUrl` renderings.
//! It is re-exported here rather than re-derived, so there is one spelling of
//! "where the archive is". This module adds what needs the object: which grant
//! a role resolves to after defaulting, whether the object's own status says it
//! is `Valid`, what a Job's environment is, and what a run freezes.
//!
//! # The environment this module renders is complete and closed
//!
//! The engine calls `AmazonS3Builder::from_env()`, which reads every `AWS_*`
//! variable it finds, so a variable forwarded from the controller's own
//! process ([`crate::controllers::backup::archive_addressing_env`]) such as
//! `AWS_ALLOW_HTTP=true` would enable plaintext HTTP in a runner whose approved
//! plan says `allow_http: false` (seam **S5**). Not one function in this module
//! reads `std::env::var`. Every value in [`DestinationEnv`] is computed from the
//! `BackupDestination` it was handed, and `AWS_ALLOW_HTTP` from
//! [`TransportSecurity::allows_plaintext_http`] alone: not from the addressing
//! style, the endpoint scheme or the process. `AWS_ENDPOINT_URL` is absent by
//! construction; the endpoint reaches the runner in the plan's own `storage`
//! block.
//!
//! # No credential value passes through this module
//!
//! A grant resolves to a Secret name and data keys; the kubelet projects the
//! value in the pod's own namespace. The controller holds no verb on `secrets`
//! (`tests/linkage.rs::the_controller_never_reads_a_secret`), so "the Secret
//! exists" is not something this module can assert: see
//! [`ResolvedDestination::check_job_namespace`] for what it asserts instead.

use std::collections::BTreeSet;

use chrono::{DateTime, Utc};
use k8s_openapi::api::core::v1::ConfigMap;
use kube::api::Api;
use kube::ResourceExt as _;
use serde::{Deserialize, Serialize};

use logweir_core::check_contract::CheckCode;
use logweir_core::credential_binding;
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

/// Re-exported from the pure layer: the check plan, this resolver and the API
/// DTOs must all name the same four roles, and a second enum here would drift
/// at the first added role.
pub use logweir_core::destination::DestinationRole;

/// The store-contract version a destination-backed Job is driven at.
///
/// It reaches the runner's argv allowlist, and a runner that does not know it
/// exits before dispatch, so a new controller cannot drive an old runner that
/// would build its evidence store from ambient credentials.
pub const STORE_CONTRACT_VERSION: &str = "1";
/// The argv flag carrying [`STORE_CONTRACT_VERSION`].
pub const STORE_CONTRACT_VERSION_ARG: &str = "--store-contract-version";
/// The environment variable carrying [`STORE_CONTRACT_VERSION`].
pub const STORE_CONTRACT_VERSION_ENV: &str = "LOGWEIR_STORE_CONTRACT_VERSION";

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

/// The variable this module never renders: the endpoint travels in the plan's
/// `storage` block, and a variable `AmazonS3Builder::from_env()` would sweep up
/// is a second, silent answer to "where is the bucket". The guard
/// `no_rendered_environment_carries_an_endpoint_variable` asserts its absence
/// by this spelling.
pub const AWS_ENDPOINT_URL_ENV: &str = "AWS_ENDPOINT_URL";

/// A CA bundle larger than this is refused with [`CheckCode::CaBundleTooLarge`]:
/// 64 KiB is roughly two hundred certificates, and every byte is copied into
/// every run's immutable plan `ConfigMap` and parsed on every store build.
pub const CA_BUNDLE_MAX_BYTES: usize = 64 * 1024;

/// The condition type the `BackupDestination` reconciler owns.
pub const CONDITION_VALID: &str = "Valid";

/// Why a destination cannot be resolved, or cannot be used for this operation.
///
/// The code is a [`CheckCode`] so the check catalogue, this resolver and the
/// `BackupDestination` status report one closed vocabulary.
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

    /// Whether a caller should hold (requeue and re-read) rather than fail the
    /// object terminally (D2 §3.6).
    ///
    /// Only an absent or not-yet-`Valid` destination holds: that is a race an
    /// operator resolves by creating or fixing the object. Everything else is a
    /// decision, and a decision retried forever is one nobody sees.
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
/// Separate from [`DestinationRefusal`] because a 503 is not a verdict about
/// the destination: a caller requeues on it and writes no condition.
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

/// One role's credential, after defaulting: references only.
///
/// `rename_all_fields`, not just `rename_all`: on an enum `rename_all` renames
/// only the variants. This block is frozen into `execution-inputs.json` under
/// `deny_unknown_fields` and re-encoded byte for byte on every later pass, so a
/// renamed key turns every running `Backup` into a `PlanConfigMapConflict`;
/// `the_snapshot_key_spellings_are_pinned` pins the bytes.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// The variant names stay PascalCase so the snapshot's `mode` reads exactly as
/// `spec.access.<role>.mode` does on the object it was resolved from.
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

/// A `BackupDestination`, resolved for one role.
///
/// Value-free, like [`crate::connection::ResolvedConnection`]: every field is a
/// setting or a reference, so it can be frozen into a run's immutable inputs
/// and returned by a product API.
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
    /// This resolution stands for an inline archive (a legacy `secretRef` read
    /// where an approved plan or an inline `archive.url` says), not for a
    /// `BackupDestination` object. Its Secret is bound to the location
    /// ([`logweir_core::credential_binding::archive_location_binding`]) because
    /// there is no object UID to bind it to. Omitted when `false`, so older
    /// serialised resolutions read the same.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub inline_archive: bool,
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
/// The caller appends the literals and `secretKeyRef` variables beside its own;
/// the ServiceAccount is a single Job field, which is why two grants that want
/// different ones are an [`CheckCode::ExecutionContextConflict`] and not a
/// merge.
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

/// The pure [`DestinationLocation`] a `BackupDestination.spec` names: the one
/// place the CRD's `storage` block and `transport.security` are read, so the
/// digest, the two `StorageUrl`s and R3–R6 all see the same reading.
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
/// computed from its current generation. A `Valid=True` from an earlier
/// generation says nothing about the spec there now.
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

/// Whether this destination opted in to the create-only readiness marker
/// (`spec.readiness.writeProbe: CreateOnlyMarker`). Absent `spec.readiness` and
/// absent `writeProbe` both mean `Disabled`: Global Constraint 6's create-only
/// boundary left untouched.
#[must_use]
pub fn write_probe_enabled(dest: &BackupDestination) -> bool {
    dest.spec.readiness.as_ref().is_some_and(|r| {
        r.write_probe == crate::crds::backup_destination::WriteProbe::CreateOnlyMarker
    })
}

/// Resolve one `BackupDestination` for one role (D2 §3.4).
///
/// The checks run in this order:
///
/// 1. **Identity.** Every reference is resolved in the object's namespace, and
///    the freeze records its UID: a deleted-and-recreated destination of the
///    same name is a different destination.
/// 2. **The location re-validated.** R3–R6 and the bucket and region patterns
///    are re-evaluated over what the API server stored, so an object admitted
///    by an older CRD revision is still refused, with the field name.
/// 3. **Engine compatibility** (G4). The pinned engine forces path-style
///    whenever an endpoint is set, so `VirtualHosted` with a custom endpoint is
///    refused rather than silently served as path-style.
/// 4. **The object's own verdict**, `Valid=True` at the current generation;
///    after the two above, so a destination whose reconciler has not run yet
///    still reports the real problem.
/// 5. **The grant**, after defaulting: `ArchiveRead` and `EvidenceWrite` fall
///    back to `archiveWrite`; `EvidenceRead` with `ArchiveReadGrant` needs an
///    explicit `archiveRead` (R9, refused again here); an absent `evidenceRead`
///    is [`ResolvedGrant::NotConfigured`], never a wider grant; `archiveWrite`
///    is required.
///
/// # Errors
///
/// [`DestinationRefusal`]; [`DestinationRefusal::is_hold`] says whether to
/// requeue or fail terminally.
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
        inline_archive: false,
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
        // An absent read grant falls back to the write grant, never to
        // "anything available": the operator did not separate the principals.
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
    // Absent is an answer: verification reports `NotAttempted` naming this
    // field, not a green badge and not an error.
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
        // R9 re-evaluated, as R3-R6 are: a write grant must never read evidence
        // back, even on an object admitted before R9 existed.
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
/// evidence at this location (D2 §4.4's `evidence` block).
///
/// Matched on bucket, endpoint and region: a bucket name alone is not an
/// identity (`lw-b` at `minio-a` and at `minio-b` are two stores). The host is
/// compared through [`DestinationLocation::host_identity`], so
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
    // Namespace isolation, enforced on the spelling: `name: "team-b/x"` is a
    // name-shaped cross-namespace reference that a kubelet would report as
    // "Secret not found". The controller holds no verb on Secrets, so this is a
    // spelling check and not a `get`.
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
/// "Parseable" means at least one `BEGIN CERTIFICATE`/`END CERTIFICATE` block
/// whose body is valid standard base64 decoding to a DER SEQUENCE (tag `0x30`):
/// enough to catch a pasted private key, a truncated block, a `.der` file or an
/// empty value, the same shape `object_store::Certificate::from_pem_bundle`
/// rejects. It is not certificate validation: expiry, chain and key usage are
/// the TLS handshake's business.
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
            // A DER certificate is a SEQUENCE (tag 0x30); anything else in
            // CERTIFICATE markers is a private key, a CSR or nothing.
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
        // Padding only in the final group: `AA==AAAA` is not base64, and
        // `object_store::Certificate::from_pem_bundle` would refuse it later.
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
    /// Every reference here is a bare object name, which the kubelet resolves in
    /// the pod's namespace, so a Job placed elsewhere would silently project
    /// whatever object carries that name there. The caller that chooses the
    /// namespace calls this, as it does
    /// [`crate::connection::ResolvedConnection::check_job_namespace`].
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

    /// The complete, explicit archive environment this destination contributes
    /// to a runner Job (D2 §3.5):
    ///
    /// * `LOGWEIR_STORE_CONTRACT_VERSION`: the handshake. A runner that does
    ///   not know the contract refuses before it dispatches.
    /// * `LOGWEIR_ARCHIVE_CREDENTIALS`: which provider the runner builds with.
    /// * `AWS_ALLOW_HTTP`: `"true"` if and only if
    ///   `spec.transport.security == InsecureHTTP`. Always rendered, including
    ///   `"false"`, because an absent variable is one an ambient
    ///   `AWS_ALLOW_HTTP=true` in the pod's environment could still answer.
    /// * `AWS_VIRTUAL_HOSTED_STYLE_REQUEST`: from `spec.storage.addressing`
    ///   alone; it does not touch the line above.
    /// * `AWS_METADATA_ENDPOINT`: a dead loopback, so a missing workload
    ///   identity cannot fall through to the node's instance role (G16).
    /// * `AWS_REGION`: only when the destination names one.
    /// * `LOGWEIR_ARCHIVE_CA_FILE`: only with a CA bundle; the bytes are frozen
    ///   into the run's own plan `ConfigMap`, never re-read at Job time.
    ///
    /// `AWS_ENDPOINT_URL` is absent by construction: see
    /// [`AWS_ENDPOINT_URL_ENV`].
    #[must_use]
    pub fn job_env(&self) -> DestinationEnv {
        render_job_env(
            self.location.transport,
            self.location.addressing,
            self.location.region.as_deref(),
            self.ca_bundle.is_some(),
            &self.grant,
            &self.credential_binding(),
        )
    }

    /// The binding this destination's `SecretKeys` Secrets must carry under
    /// `logweir-binding`: its UID and archive route
    /// ([`logweir_core::credential_binding::destination_binding`]), or the
    /// fail-closed `unbound:no-uid` for an object with no UID. It is what
    /// `status.credentialBinding` publishes, what the console writes into the
    /// Secrets it creates, and what every Job built from this destination
    /// expects.
    #[must_use]
    pub fn credential_binding(&self) -> String {
        if self.inline_archive {
            return credential_binding::archive_location_binding(&self.plan_storage());
        }
        binding_for(&self.uid, &self.plan_storage())
    }

    /// The evidence-side environment for a Job that already carries `archive`'s
    /// [`ResolvedDestination::job_env`] (D2 §3.5's restore paragraph).
    ///
    /// * The evidence grant is the archive grant (same Secret and keys, or the
    ///   same ServiceAccount): `LOGWEIR_EVIDENCE_CREDENTIALS=archive`, and no
    ///   second credential is projected.
    /// * A different Secret: `=static`, plus the three `LOGWEIR_EVIDENCE_AWS_*`
    ///   variables, named apart so neither store's credential can shadow the
    ///   other's (`AWS_*` is the archive's).
    /// * A workload identity beside a static archive grant: `=workloadIdentity`,
    ///   and the runner builds the evidence store without `from_env`, copying
    ///   only the web-identity and container-credential variables; otherwise
    ///   object_store's chain would put the static archive keys first (G16).
    ///
    /// # Errors
    ///
    /// [`CheckCode::ExecutionContextConflict`] for two different
    /// workload-identity ServiceAccounts in one pod (a pod has exactly one), or
    /// for two destinations in two namespaces.
    pub fn evidence_env(
        &self,
        archive: &ResolvedDestination,
    ) -> Result<DestinationEnv, DestinationRefusal> {
        archive.check_job_namespace(&self.namespace)?;
        // The one refusal that needs the objects, because its message names
        // both destinations; everything below is `render_evidence_env`, a pure
        // function of the two grants.
        if let (
            ResolvedGrant::WorkloadIdentity {
                service_account_name,
            },
            Some(archive_sa),
        ) = (&self.grant, archive.grant.service_account_name())
        {
            if archive_sa != service_account_name {
                return Err(DestinationRefusal::new(
                    CheckCode::ExecutionContextConflict,
                    "spec.access",
                    format!(
                        "the archive grant of BackupDestination {}/{} runs as ServiceAccount \
                         {archive_sa} and the evidence grant of BackupDestination {}/{} runs as \
                         {service_account_name}; one pod has one ServiceAccount, so these two \
                         grants cannot be satisfied by one Job",
                        archive.namespace, archive.name, self.namespace, self.name
                    ),
                ));
            }
        }
        if matches!(
            self.grant,
            ResolvedGrant::ControllerIdentity | ResolvedGrant::NotConfigured
        ) {
            return Err(DestinationRefusal::new(
                CheckCode::DestinationRoleNotConfigured,
                "spec.access.evidenceWrite",
                format!(
                    "BackupDestination {}/{} has no evidence-write grant a Job could use ({:?})",
                    self.namespace, self.name, self.grant
                ),
            ));
        }
        // The evidence destination's own root, in the `archive` arm too: the
        // two may be different objects with different bundles even when they
        // share a credential, and the runner adds this root to the archive's.
        let ca_file = self
            .ca_bundle
            .is_some()
            .then(|| format!("{PLAN_MOUNT_PATH}/{EVIDENCE_CA_PLAN_KEY}"));
        // The evidence destination's own binding, in the `archive` arm too: a
        // shared Secret presents the archive credential to this destination's
        // route, so it must be bound to this destination as well.
        Ok(render_evidence_env(
            &self.grant,
            &archive.grant,
            ca_file,
            EvidenceBindings {
                evidence: &self.credential_binding(),
                archive: &archive.credential_binding(),
            },
        ))
    }

    /// The evidence-write environment a check Job adds so the create-only
    /// readiness marker is written as this destination's `evidenceWrite`
    /// principal.
    ///
    /// `self` is the resolution the check plan's destination credential came
    /// from (`ArchiveWrite` for a Backup readiness check, the primary requested
    /// role for a `DestinationAccess` check); `evidence_write` is the same
    /// object's `evidenceWrite` grant.
    ///
    /// * `Ok(None)`: the two are one grant, so nothing second is projected.
    /// * `Ok(Some(env))`: a different Secret or a workload identity, rendered by
    ///   the renderer [`ResolvedDestination::evidence_env`] uses, so a check pod
    ///   and a Restore pod project the grant the same way. No
    ///   `LOGWEIR_EVIDENCE_CA_FILE`: one destination, and the check plan names
    ///   its trust bundle itself.
    ///
    /// # Errors
    ///
    /// [`CheckCode::ExecutionContextConflict`] when both grants are workload
    /// identities on different ServiceAccounts, and
    /// [`CheckCode::DestinationRoleNotConfigured`] for a grant no Job can use.
    pub fn check_evidence_write_env(
        &self,
        evidence_write: &ResolvedGrant,
    ) -> Result<Option<DestinationEnv>, DestinationRefusal> {
        if evidence_write == &self.grant {
            return Ok(None);
        }
        if let (
            ResolvedGrant::WorkloadIdentity {
                service_account_name,
            },
            Some(primary_sa),
        ) = (evidence_write, self.grant.service_account_name())
        {
            if primary_sa != service_account_name {
                return Err(DestinationRefusal::new(
                    CheckCode::ExecutionContextConflict,
                    "spec.access.evidenceWrite",
                    format!(
                        "BackupDestination {}/{} runs its checked grant as ServiceAccount \
                         {primary_sa} and its evidence-write grant as {service_account_name}; \
                         one pod has one ServiceAccount, so one check cannot exercise both",
                        self.namespace, self.name
                    ),
                ));
            }
        }
        if matches!(
            evidence_write,
            ResolvedGrant::ControllerIdentity | ResolvedGrant::NotConfigured
        ) {
            return Err(DestinationRefusal::new(
                CheckCode::DestinationRoleNotConfigured,
                "spec.access.evidenceWrite",
                format!(
                    "BackupDestination {}/{} has no evidence-write grant a Job could use ({:?})",
                    self.namespace, self.name, evidence_write
                ),
            ));
        }
        let binding = self.credential_binding();
        Ok(Some(render_evidence_env(
            evidence_write,
            &self.grant,
            None,
            EvidenceBindings {
                evidence: &binding,
                archive: &binding,
            },
        )))
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
            evidence_grant: None,
        }
    }

    /// [`ResolvedDestination::snapshot`] with the `evidenceWrite` grant this
    /// run's evidence store is built from (D2 §3.5).
    ///
    /// A backup run writes the archive through `archiveWrite` and its signed
    /// receipt through `evidenceWrite`, and the runner refuses a run that does
    /// not say which credential the second store uses. `evidence` is recorded
    /// only when it differs from [`ResolvedDestination::grant`]; see
    /// [`ResolvedDestinationSnapshot::evidence_grant`].
    #[must_use]
    pub fn snapshot_with_evidence(&self, evidence: &ResolvedGrant) -> ResolvedDestinationSnapshot {
        let mut snapshot = self.snapshot();
        if *evidence != self.grant {
            snapshot.evidence_grant = Some(evidence.clone());
        }
        snapshot
    }
}

/// **The store environment of a Job that READS a run's evidence and writes
/// beside it** (theme A's PoC defect D1): the `Switchover` Job (it reads the
/// restore's scorecard, sidecar, mapping and a rollback's record, and writes
/// the switchover record) and a TTL clone's cleanup Job (it reads the
/// scorecard and writes the cleanup attestation).
///
/// Neither reads an archive, so the ARCHIVE slot ([`ResolvedDestination::
/// job_env`]: `AWS_*`, `LOGWEIR_ARCHIVE_CREDENTIALS`, the archive binding
/// pair) carries the evidence destination's `evidenceRead` grant, and the
/// EVIDENCE slot ([`ResolvedDestination::evidence_env`]) its `evidenceWrite`
/// grant, `archive` when the two are one. The runner reads with the first and
/// writes with the second (`logweir::evidence_read`).
///
/// An `evidenceRead` no Job can hold (`ControllerIdentity`, not configured,
/// not resolvable, or a workload identity on another ServiceAccount than
/// `evidenceWrite`'s) falls back to reading with the write grant, as these Jobs
/// did before, and [`EvidenceAccessEnv::read_grant`] says so: a write grant
/// that cannot read then fails by name, never as a verdict.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EvidenceAccessEnv {
    /// The archive slot: the grant the Job reads evidence with.
    pub read: DestinationEnv,
    /// The evidence slot: the grant it writes with.
    pub write: DestinationEnv,
    /// The read grant as a failure names it: `evidenceRead of
    /// BackupDestination <ns>/<name>`, or the fallback with its reason.
    pub read_grant: String,
}

impl EvidenceAccessEnv {
    /// The ServiceAccount a workload-identity grant requires, if either does
    /// (one pod, one ServiceAccount: [`ResolvedDestination::evidence_env`]
    /// refused two different ones).
    #[must_use]
    pub fn service_account_name(&self) -> Option<&str> {
        self.read
            .service_account_name
            .as_deref()
            .or(self.write.service_account_name.as_deref())
    }
}

/// Every variable [`ResolvedDestination::job_env`] and
/// [`ResolvedDestination::evidence_env`] can render: what a Job copied from
/// another's pod drops before it takes an [`EvidenceAccessEnv`].
/// `every_rendered_store_variable_is_listed` holds the list to the renderers.
pub const STORE_ENV_NAMES: [&str; 19] = [
    STORE_CONTRACT_VERSION_ENV,
    AWS_ALLOW_HTTP_ENV,
    AWS_VIRTUAL_HOSTED_ENV,
    AWS_METADATA_ENDPOINT_ENV,
    AWS_REGION_ENV,
    ARCHIVE_CA_FILE_ENV,
    ARCHIVE_CREDENTIALS_ENV,
    AWS_ACCESS_KEY_ID_ENV,
    AWS_SECRET_ACCESS_KEY_ENV,
    AWS_SESSION_TOKEN_ENV,
    credential_binding::ARCHIVE_CREDENTIAL_BINDING_ENV,
    credential_binding::ARCHIVE_CREDENTIAL_BINDING_EXPECTED_ENV,
    EVIDENCE_CA_FILE_ENV,
    EVIDENCE_CREDENTIALS_ENV,
    EVIDENCE_ACCESS_KEY_ID_ENV,
    EVIDENCE_SECRET_ACCESS_KEY_ENV,
    EVIDENCE_SESSION_TOKEN_ENV,
    credential_binding::EVIDENCE_CREDENTIAL_BINDING_ENV,
    credential_binding::EVIDENCE_CREDENTIAL_BINDING_EXPECTED_ENV,
];

/// **The read and the write grant of an evidence-reading Job**, pure. `write`
/// is the evidence destination resolved for `evidenceWrite`; `read` the same
/// object resolved for `evidenceRead`, or `None` when that did not resolve.
///
/// # Errors
///
/// [`ResolvedDestination::evidence_env`]'s refusals of the write grant.
pub fn evidence_access_env(
    write: &ResolvedDestination,
    read: Option<&ResolvedDestination>,
) -> Result<EvidenceAccessEnv, DestinationRefusal> {
    let why_not = match read {
        None => "does not resolve".to_string(),
        Some(r) if r.uid != write.uid => "is another object's".to_string(),
        Some(r) => match &r.grant {
            ResolvedGrant::SecretKeys { .. } | ResolvedGrant::WorkloadIdentity { .. } => {
                match write.evidence_env(r) {
                    Ok(write_env) => {
                        return Ok(EvidenceAccessEnv {
                            read: read_slot(r),
                            write: write_env,
                            read_grant: format!(
                                "evidenceRead of BackupDestination {}/{}",
                                r.namespace, r.name
                            ),
                        })
                    }
                    Err(refusal) => format!(
                        "cannot share a pod with evidenceWrite ({})",
                        refusal.message
                    ),
                }
            }
            ResolvedGrant::ControllerIdentity => {
                "is the controller's own handle, which no Job holds".to_string()
            }
            ResolvedGrant::NotConfigured => "is not configured".to_string(),
        },
    };
    Ok(EvidenceAccessEnv {
        read: read_slot(write),
        write: write.evidence_env(write)?,
        read_grant: format!(
            "evidenceWrite of BackupDestination {}/{} (its evidenceRead {why_not})",
            write.namespace, write.name
        ),
    })
}

/// `dest`'s grant rendered into the archive slot, its CA at the evidence
/// destination's key in the run's plan `ConfigMap` (where a Restore froze it).
fn read_slot(dest: &ResolvedDestination) -> DestinationEnv {
    let mut env = dest.job_env();
    for (name, value) in &mut env.literals {
        if name == ARCHIVE_CA_FILE_ENV {
            *value = format!("{PLAN_MOUNT_PATH}/{EVIDENCE_CA_PLAN_KEY}");
        }
    }
    env
}

/// The archive environment one resolved location and grant contribute to a
/// runner Job; [`ResolvedDestination::job_env`] lists every variable.
///
/// One renderer for two callers: [`ResolvedDestination::job_env`] renders from
/// the live object at the freeze, [`ResolvedDestinationSnapshot::job_env`] from
/// the frozen block on every later pass. Two implementations would drift the
/// first time a variable was added; `the_snapshot_renders_the_same_job_env`
/// pins the equality.
///
/// A `SecretKeys` grant brings its binding pair:
/// `LOGWEIR_ARCHIVE_CREDENTIAL_BINDING` as an optional `secretKeyRef` to the
/// same Secret's `logweir-binding` key, and
/// `LOGWEIR_ARCHIVE_CREDENTIAL_BINDING_EXPECTED` = `binding` as a literal, so
/// every runner refuses a Secret written for another destination before it
/// builds a store.
#[must_use]
fn render_job_env(
    transport: TransportSecurity,
    addressing: Addressing,
    region: Option<&str>,
    has_ca: bool,
    grant: &ResolvedGrant,
    binding: &str,
) -> DestinationEnv {
    let mut env = DestinationEnv {
        literals: vec![
            (
                STORE_CONTRACT_VERSION_ENV.to_string(),
                STORE_CONTRACT_VERSION.to_string(),
            ),
            (
                AWS_ALLOW_HTTP_ENV.to_string(),
                // From the transport alone (seam S5).
                bool_env(transport.allows_plaintext_http()),
            ),
            (
                AWS_VIRTUAL_HOSTED_ENV.to_string(),
                // From the addressing alone.
                bool_env(!addressing.is_path_style()),
            ),
            (
                AWS_METADATA_ENDPOINT_ENV.to_string(),
                logweir_store::DEAD_METADATA_ENDPOINT.to_string(),
            ),
        ],
        from_secret: Vec::new(),
        service_account_name: None,
    };
    if let Some(region) = region {
        env.literals
            .push((AWS_REGION_ENV.to_string(), region.to_string()));
    }
    if has_ca {
        env.literals.push((
            ARCHIVE_CA_FILE_ENV.to_string(),
            format!("{PLAN_MOUNT_PATH}/{ARCHIVE_CA_PLAN_KEY}"),
        ));
    }
    match grant {
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
                optional: false,
                key: access_key_id_key.clone(),
            });
            env.from_secret.push(EnvFromSecret {
                name: AWS_SECRET_ACCESS_KEY_ENV.to_string(),
                secret_name: secret.clone(),
                optional: false,
                key: secret_access_key_key.clone(),
            });
            if let Some(token) = session_token_key {
                env.from_secret.push(EnvFromSecret {
                    name: AWS_SESSION_TOKEN_ENV.to_string(),
                    secret_name: secret.clone(),
                    optional: false,
                    key: token.clone(),
                });
            }
            push_binding_pair(
                &mut env,
                secret,
                credential_binding::ARCHIVE_CREDENTIAL_BINDING_ENV,
                credential_binding::ARCHIVE_CREDENTIAL_BINDING_EXPECTED_ENV,
                binding,
            );
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

/// The evidence-side environment one grant pair contributes to a runner Job;
/// [`ResolvedDestination::evidence_env`] lists the cases.
///
/// One renderer for every caller, for [`render_job_env`]'s reason: a second
/// implementation would be a second answer to "which credential writes this
/// run's evidence". On the Backup path the two grants are usually one, because
/// `evidenceWrite` falls back to `archiveWrite` (D2 §3.4).
///
/// `ca_file` is the plan path of a root the evidence store needs of its own,
/// `None` where it inherits the archive's.
///
/// It cannot refuse: the refusals need the objects to name and are made before
/// this is reached. A `ControllerIdentity` or `NotConfigured` grant that
/// arrives anyway renders nothing, and the runner then refuses at exit 3 with
/// the variable named, which beats a controller that panics.
#[must_use]
fn render_evidence_env(
    evidence_grant: &ResolvedGrant,
    archive_grant: &ResolvedGrant,
    ca_file: Option<String>,
    bindings: EvidenceBindings<'_>,
) -> DestinationEnv {
    let mut env = DestinationEnv::default();
    if let Some(path) = ca_file {
        env.literals.push((EVIDENCE_CA_FILE_ENV.to_string(), path));
    }
    // A Secret-backed evidence credential carries its binding pair against the
    // evidence destination's binding. In the `archive` arm the archive pair
    // already checks the Secret, so the pair is added only when the two
    // destinations' bindings differ (a Restore whose evidence destination is
    // another object); a Backup's one destination projects nothing twice.
    let pair_needed = match evidence_grant {
        _ if evidence_grant == archive_grant => bindings.evidence != bindings.archive,
        ResolvedGrant::SecretKeys { .. } => true,
        _ => false,
    };
    match evidence_grant {
        _ if evidence_grant == archive_grant => {
            env.literals.push((
                EVIDENCE_CREDENTIALS_ENV.to_string(),
                CREDENTIALS_ARCHIVE.to_string(),
            ));
        }
        ResolvedGrant::SecretKeys {
            secret,
            access_key_id_key,
            secret_access_key_key,
            session_token_key,
        } => {
            env.literals.push((
                EVIDENCE_CREDENTIALS_ENV.to_string(),
                CREDENTIALS_STATIC.to_string(),
            ));
            env.from_secret.push(EnvFromSecret {
                name: EVIDENCE_ACCESS_KEY_ID_ENV.to_string(),
                secret_name: secret.clone(),
                optional: false,
                key: access_key_id_key.clone(),
            });
            env.from_secret.push(EnvFromSecret {
                name: EVIDENCE_SECRET_ACCESS_KEY_ENV.to_string(),
                secret_name: secret.clone(),
                optional: false,
                key: secret_access_key_key.clone(),
            });
            if let Some(token) = session_token_key {
                env.from_secret.push(EnvFromSecret {
                    name: EVIDENCE_SESSION_TOKEN_ENV.to_string(),
                    secret_name: secret.clone(),
                    optional: false,
                    key: token.clone(),
                });
            }
        }
        ResolvedGrant::WorkloadIdentity {
            service_account_name,
        } => {
            env.literals.push((
                EVIDENCE_CREDENTIALS_ENV.to_string(),
                CREDENTIALS_WORKLOAD_IDENTITY.to_string(),
            ));
            env.service_account_name = Some(service_account_name.clone());
        }
        // Refused upstream; rendering nothing is the least harmful reaction.
        ResolvedGrant::ControllerIdentity | ResolvedGrant::NotConfigured => {}
    }
    if let (true, ResolvedGrant::SecretKeys { secret, .. }) = (pair_needed, evidence_grant) {
        push_binding_pair(
            &mut env,
            secret,
            credential_binding::EVIDENCE_CREDENTIAL_BINDING_ENV,
            credential_binding::EVIDENCE_CREDENTIAL_BINDING_EXPECTED_ENV,
            bindings.evidence,
        );
    }
    env.literals.sort_by(|a, b| a.0.cmp(&b.0));
    env
}

/// The two bindings an evidence environment is rendered against: equal for a
/// Backup (one destination), possibly different for a Restore.
#[derive(Clone, Copy)]
struct EvidenceBindings<'a> {
    evidence: &'a str,
    archive: &'a str,
}

/// The binding pair one Secret-backed credential contributes: the Secret's
/// `logweir-binding` key as an optional `secretKeyRef` (an unbound Secret still
/// reaches the runner, which refuses it by name) and the expectation as a
/// literal.
fn push_binding_pair(
    env: &mut DestinationEnv,
    secret: &str,
    projected: &str,
    expected_env: &str,
    expected: &str,
) {
    env.literals
        .push((expected_env.to_string(), expected.to_string()));
    env.from_secret.push(EnvFromSecret {
        name: projected.to_string(),
        secret_name: secret.to_string(),
        optional: true,
        key: credential_binding::CREDENTIAL_BINDING_KEY.to_string(),
    });
}

/// What an inline archive's `secretRef` contributes to a runner Job: the
/// binding pair for its `AWS_*` credential over the location the runner dials
/// (scheme, bucket and endpoint). When the run also writes evidence with that
/// credential elsewhere (an inline `Restore` whose plan names another bucket or
/// endpoint for `evidence:`), a second pair from the same Secret expects the
/// evidence location's binding, which one Secret cannot carry beside the
/// first, so the run is refused rather than present the credential where it
/// was never bound. The caller appends these beside the
/// `AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY` pair it already projects.
#[must_use]
pub fn inline_archive_binding_env(
    secret: &str,
    archive: &StorageUrl,
    evidence: Option<&StorageUrl>,
) -> DestinationEnv {
    let mut env = DestinationEnv::default();
    let archive_binding = credential_binding::archive_location_binding(archive);
    push_binding_pair(
        &mut env,
        secret,
        credential_binding::ARCHIVE_CREDENTIAL_BINDING_ENV,
        credential_binding::ARCHIVE_CREDENTIAL_BINDING_EXPECTED_ENV,
        &archive_binding,
    );
    if let Some(evidence) = evidence {
        let evidence_binding = credential_binding::archive_location_binding(evidence);
        if evidence_binding != archive_binding {
            push_binding_pair(
                &mut env,
                secret,
                credential_binding::EVIDENCE_CREDENTIAL_BINDING_ENV,
                credential_binding::EVIDENCE_CREDENTIAL_BINDING_EXPECTED_ENV,
                &evidence_binding,
            );
        }
    }
    env
}

/// The location an inline-archive runner actually dials: the plan's (or the
/// frozen) storage block, with an S3 endpoint and region the block leaves out
/// filled from the `AWS_ENDPOINT_URL` and `AWS_REGION` the Job is handed
/// (`object_store`'s own fall-back). The binding then covers the host the
/// request really goes to, and a `Restore` whose plan omits the region expects
/// the same binding as a `Backup` at that location, whose frozen block already
/// carries the controller's `AWS_REGION` (`crate::retention::storage_url_for`).
#[must_use]
pub fn effective_inline_storage(
    storage: &StorageUrl,
    addressing: &[(String, String)],
) -> StorageUrl {
    let forwarded = |var: &str| {
        addressing
            .iter()
            .find(|(name, value)| name == var && !value.trim().is_empty())
            .map(|(_, value)| value.clone())
    };
    let mut effective = storage.clone();
    if let StorageUrl::S3 {
        endpoint, region, ..
    } = &mut effective
    {
        if endpoint.as_deref().is_none_or(|e| e.trim().is_empty()) {
            *endpoint = forwarded(AWS_ENDPOINT_URL_ENV);
        }
        if region.is_none() {
            *region = forwarded(AWS_REGION_ENV);
        }
    }
    effective
}

/// A destination's binding, or the fail-closed expectation for an object with
/// no UID.
fn binding_for(uid: &str, archive: &StorageUrl) -> String {
    if uid.trim().is_empty() {
        return credential_binding::UNBOUND_NO_UID.to_string();
    }
    credential_binding::destination_binding(uid, archive)
}

/// The region one frozen archive `StorageUrl` names, or `None`.
///
/// S3 IS THE ONLY BACKEND WITH ONE. A destination is `StorageProvider::S3` by
/// construction ([`PROVIDER`]), and the other variants exist because
/// `StorageUrl` is shared with the legacy inline path.
fn region_of(storage: &StorageUrl) -> Option<&str> {
    match storage {
        StorageUrl::S3 { region, .. } => region.as_deref(),
        _ => None,
    }
}

fn bool_env(value: bool) -> String {
    if value { "true" } else { "false" }.to_string()
}

/// The resolved destination as one run freezes it (D2 §3.7, seam **S4**).
///
/// A block inside `execution-inputs.json`
/// ([`crate::backup_execution::BackupExecutionInputs`]), not a second freeze.
/// It carries the UID and generation (a destination deleted and recreated
/// under the same name is a different input), the location digest (to match a
/// recovery point to a destination later), both `StorageUrl`s (the Job is
/// rendered from the snapshot, never from the live object), the CA digest and
/// the grant references. It carries no credential value and no CA bytes: the
/// bytes live in the same immutable plan `ConfigMap` under
/// [`ARCHIVE_CA_PLAN_KEY`], and this digest binds them, so a rotated CA cannot
/// be swapped into a run that already exists.
///
/// The key spellings are pinned by
/// `tests/destination_controller.rs::the_snapshot_key_spellings_are_pinned`:
/// the frozen bytes are re-encoded and compared on every later pass, so
/// renaming one key turns every running `Backup` into a
/// `TERMINAL_STATE_PLAN_CONFIG_MAP_CONFLICT`.
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
    pub transport: TransportSecurity,
    pub addressing: Addressing,
    /// `sha256:<hex>` over the CA bytes frozen beside this snapshot.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ca_sha256: Option<String>,
    /// The grant for the role this snapshot was resolved for.
    pub grant: ResolvedGrant,
    /// The `evidenceWrite` grant, when it differs from [`Self::grant`].
    ///
    /// `None` means the same grant: `evidenceWrite` falls back to `archiveWrite`
    /// (D2 §3.4), and the runner is told `LOGWEIR_EVIDENCE_CREDENTIALS=archive`.
    /// Last in declaration order and omitted when absent, because
    /// `logweir_core::det_json` emits fields in declaration order: a snapshot
    /// without it re-encodes to the bytes it had before this field existed, so
    /// every frozen plan stays re-verifiable.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub evidence_grant: Option<ResolvedGrant>,
}

impl ResolvedDestinationSnapshot {
    /// The archive environment a Job rendered from this frozen block carries
    /// (D2 §3.7: the Job is rendered only from the snapshot).
    ///
    /// A runner Job may be re-created (garbage collected, a node lost, a
    /// controller restarted mid-run). Rendering it from the destination as it
    /// reads now would let a CA or access-key rotation change what an approved,
    /// half-written run addresses and trusts. It is [`render_job_env`], the
    /// renderer [`ResolvedDestination::job_env`] uses.
    #[must_use]
    pub fn job_env(&self) -> DestinationEnv {
        render_job_env(
            self.transport,
            self.addressing,
            region_of(&self.archive_storage),
            self.ca_sha256.is_some(),
            &self.grant,
            &self.credential_binding(),
        )
    }

    /// [`ResolvedDestination::credential_binding`] over the frozen block, so a
    /// Job re-created from the snapshot expects exactly what the first one did.
    #[must_use]
    pub fn credential_binding(&self) -> String {
        binding_for(&self.uid, &self.archive_storage)
    }

    /// The evidence-side environment a Job rendered from this frozen block
    /// carries (D2 §3.5).
    ///
    /// A Backup names one destination, so the archive and receipt stores trust
    /// the same root, frozen at [`ARCHIVE_CA_PLAN_KEY`]. When the evidence grant
    /// is the archive grant the runner clones the archive's options, which
    /// already carry that root, so no file is named; when the grants differ the
    /// runner builds fresh options and the root is named at the archive's key.
    /// It is [`render_evidence_env`], the renderer
    /// [`ResolvedDestination::evidence_env`] uses.
    #[must_use]
    pub fn evidence_job_env(&self) -> DestinationEnv {
        let evidence = self.evidence_grant.as_ref().unwrap_or(&self.grant);
        let ca_file = (self.ca_sha256.is_some() && *evidence != self.grant)
            .then(|| format!("{PLAN_MOUNT_PATH}/{ARCHIVE_CA_PLAN_KEY}"));
        let binding = self.credential_binding();
        render_evidence_env(
            evidence,
            &self.grant,
            ca_file,
            EvidenceBindings {
                evidence: &binding,
                archive: &binding,
            },
        )
    }

    /// The one canonical encoding of this block: `logweir_core::det_json`, the
    /// encoder [`crate::backup_execution::canonical_inputs`] uses for the
    /// document this block sits inside. A second serialiser would re-encode
    /// differently the first time a field was added and turn every running
    /// Backup into a `PlanConfigMapConflict`.
    ///
    /// # Errors
    ///
    /// The encoder's own error, which a typed value built here cannot produce.
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

/// `(backend, bucket)` of a parsed location. The backend matters: `s3://kb` and
/// `gs://kb` are two buckets that share a name.
fn storage_bucket(u: &StorageUrl) -> (&'static str, String) {
    match u {
        StorageUrl::S3 { bucket, .. } => ("s3", bucket.clone()),
        StorageUrl::Gcs { bucket, .. } => ("gs", bucket.clone()),
        StorageUrl::Azure {
            account_name,
            container_name,
            ..
        } => ("az", format!("{account_name}/{container_name}")),
        StorageUrl::Filesystem { path } => ("file", path.to_string_lossy().into_owned()),
    }
}

/// Whether the controller's one global handle can read an inline-archive run's
/// evidence.
///
/// An inline-archive `Restore` writes its signed scorecard wherever its
/// approved plan's `evidence:` block says, and the runner prints only
/// bucket-relative keys, which the controller reads through the handle `main`
/// built from `LOGWEIR_ARCHIVE_URL`. Read in a bucket the document is not in, a
/// key finds nothing, or someone else's object; so a run whose evidence
/// location differs from the handle's is not read at all, and says so.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum LegacyEvidenceScope {
    /// The caller did not state where the handle points (the reconcile entry
    /// points tests drive with in-memory oracles). Read like
    /// [`Self::GlobalHandleApplies`]: in the shipped binary a handle exists only
    /// when `LOGWEIR_ARCHIVE_URL` names one, and then the caller passes it.
    HandleLocationUnstated,
    /// The evidence is in the handle's bucket: read it through the handle.
    GlobalHandleApplies,
    /// The evidence is somewhere the handle cannot see.
    Elsewhere {
        /// `<backend>://<bucket>` the run's evidence was written to — the
        /// tenant's own plan, and safe to publish.
        evidence: String,
        /// The handle's `LOGWEIR_ARCHIVE_URL`. Installation configuration: it
        /// goes to the controller's log and never into a status a tenant reads.
        handle: String,
    },
}

/// How a tenant-visible sentence names the controller's archive handle: by
/// ROLE, never by value. The handle's URL is installation configuration, and a
/// status, a Preflight message or an event is readable by every operator of the
/// namespace — in shared mode by every tenant of the console. The value is in
/// the controller's log, where an administrator reads it.
pub const ARCHIVE_HANDLE_LABEL: &str = "the controller's archive handle (the installation's \
     LOGWEIR_ARCHIVE_URL, which an administrator can read in the controller's configuration)";

impl LegacyEvidenceScope {
    /// The `NotAttempted` detail for [`Self::Elsewhere`]: the run's OWN
    /// evidence location, the handle by [`ARCHIVE_HANDLE_LABEL`] only, and the
    /// command that verifies the run without the controller; `None` for every
    /// scope the handle may read.
    #[must_use]
    pub fn not_attempted_detail(&self, document: &str) -> Option<String> {
        match self {
            Self::Elsewhere { evidence, .. } => Some(format!(
                "this run wrote its {document} to {evidence}, which is not the bucket of \
                 {ARCHIVE_HANDLE_LABEL}; an inline-archive run's evidence is read only through \
                 that handle, so nothing was read and nothing is verified. Write the evidence \
                 to the handle's bucket (on the chart's default install, the recovery point's \
                 own archive bucket, which the console starts the field on), or run the \
                 printed logweir drill verify command"
            )),
            Self::HandleLocationUnstated | Self::GlobalHandleApplies => None,
        }
    }
}

/// Decide [`LegacyEvidenceScope`] for an inline-archive run whose evidence is
/// at `evidence`: a restore plan's `evidence:` block, or a backup's archive
/// (its receipt goes under the archive's own bucket, D2 grounding G7).
///
/// Compared on backend and bucket, since every key a runner prints is
/// bucket-relative. The endpoint is not compared (the handle's comes from the
/// controller's own environment), so a same-named bucket on another endpoint is
/// read and found empty, which the unread-evidence verdict names: the safe
/// direction. `handle_archive_url` is
/// [`crate::retention::configured_archive_url`]'s answer, or `None`
/// ([`LegacyEvidenceScope::HandleLocationUnstated`]).
#[must_use]
pub fn legacy_evidence_scope(
    evidence: &StorageUrl,
    handle_archive_url: Option<&str>,
) -> LegacyEvidenceScope {
    let Some(handle) = handle_archive_url.filter(|u| !u.is_empty()) else {
        return LegacyEvidenceScope::HandleLocationUnstated;
    };
    let (backend, bucket) = storage_bucket(evidence);
    let evidence_at = format!("{backend}://{bucket}");
    let handle_at = crate::retention::storage_url_for(handle)
        .ok()
        .map(|u| storage_bucket(&u));
    match handle_at {
        Some((b, name)) if b == backend && !name.is_empty() && name == bucket => {
            LegacyEvidenceScope::GlobalHandleApplies
        }
        // AN UNREADABLE HANDLE URL IS NOT THE SAME BUCKET AS ANYTHING — the
        // safe direction.
        _ => LegacyEvidenceScope::Elsewhere {
            evidence: evidence_at,
            handle: handle.to_string(),
        },
    }
}

/// [`legacy_evidence_scope`] for an inline-archive `Backup`: its receipt is
/// written under its own archive's bucket, so the archive URL IS the evidence
/// location. An archive URL that does not parse is `Elsewhere`: the run was
/// refused as `ArchiveUrlUnreadable` before any Job, and nothing it could have
/// written is in the handle's bucket.
#[must_use]
pub fn legacy_backup_evidence_scope(
    archive_url: &str,
    handle_archive_url: Option<&str>,
) -> LegacyEvidenceScope {
    match crate::retention::storage_url_for(archive_url) {
        Ok(location) => legacy_evidence_scope(&location, handle_archive_url),
        Err(_) => match handle_archive_url.filter(|u| !u.is_empty()) {
            None => LegacyEvidenceScope::HandleLocationUnstated,
            Some(handle) => LegacyEvidenceScope::Elsewhere {
                evidence: archive_url.to_string(),
                handle: handle.to_string(),
            },
        },
    }
}

/// Read one `BackupDestination` by name, resolve it for `role`, and attach its
/// CA bundle. Namespace-local by construction (`Api::namespaced`): a
/// `destinationRef` is a [`crate::crds::LocalRef`] with no namespace field.
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

/// One `BackupDestination` read ONCE and resolved for TWO roles — what
/// [`resolve_ref_roles`] returns.
#[derive(Debug)]
pub struct ResolvedRoles {
    /// The first role's resolution. The location, the digest and the canonical
    /// URL are the same on both; only [`ResolvedDestination::grant`] and
    /// [`ResolvedDestination::role`] differ.
    pub primary: ResolvedDestination,
    /// The second role's resolution from the same read, or the refusal its
    /// grant produced. Not an error of the whole call, because only the caller
    /// knows when the second role matters (retention needs it only in
    /// `Enforce`); every check before the grant is role-independent and has
    /// already passed for `primary`.
    pub secondary: Result<ResolvedDestination, Box<DestinationRefusal>>,
    /// Whether the object DECLARES the second role's own grant, or
    /// [`secondary`](Self::secondary) is the documented fall-back — see
    /// [`declares`], which says what this is and is not for.
    pub secondary_declared: bool,
}

/// Whether `dest` declares `role`'s own grant, rather than reaching one through
/// the defaulting [`resolve`] documents.
///
/// It does not decide whether the default is allowed (the defaulting is the
/// documented contract: absent grants do not widen). It decides which field a
/// refusal names, so an operator is sent to the line they wrote rather than a
/// line their object does not contain.
#[must_use]
pub fn declares(dest: &BackupDestination, role: DestinationRole) -> bool {
    let access = &dest.spec.access;
    match role {
        // Required by the CRD; there is nothing for it to fall back to.
        DestinationRole::ArchiveWrite => true,
        DestinationRole::ArchiveRead => access.archive_read.is_some(),
        DestinationRole::EvidenceWrite => access.evidence_write.is_some(),
        DestinationRole::EvidenceRead => access.evidence_read.is_some(),
    }
}

/// Read one `BackupDestination` by name and resolve it for two roles from one
/// read and one CA-bundle fetch: two reads of a moving object could pair a
/// location from one generation with a grant from another, and the pair is
/// what a caller freezes into one Job.
///
/// # Errors
///
/// Whatever [`resolve_ref`] refuses for the primary role; the secondary's
/// refusal is in [`ResolvedRoles::secondary`].
pub async fn resolve_ref_roles(
    client: &kube::Client,
    namespace: &str,
    name: &str,
    primary: DestinationRole,
    secondary: DestinationRole,
    policy: &Policy,
) -> Result<ResolvedRoles, ResolveError> {
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
    let first = resolve(&dest, primary, policy)?;
    let second = resolve(&dest, secondary, policy);
    let observation = read_ca_bundle(client, &first).await?;
    Ok(ResolvedRoles {
        primary: first.with_ca(&observation)?,
        secondary: second
            .and_then(|r| r.with_ca(&observation))
            .map_err(Box::new),
        secondary_declared: declares(&dest, secondary),
    })
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
    // `binaryData` too: the API server stores a non-UTF-8 value there, which is
    // what `kubectl create configmap ca --from-file=ca.crt=ca.der` produces.
    // Read here, a DER file reaches `check_ca_bundle` and is refused as
    // `CaBundleInvalid` (a DER file is not a PEM bundle) instead of a false
    // "no such key".
    if let Some(bytes) = cm.binary_data.as_ref().and_then(|d| d.get(&reference.key)) {
        return Ok(CaObservation::Present(bytes.0.clone()));
    }
    Ok(CaObservation::KeyMissing)
}

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
/// closed. A literal table derived from no `match` the evaluator reads: a list
/// computed from the code it checks moves with a mutant.
pub const DESTINATION_CONDITION_REASONS: [&str; 7] = [
    REASON_VALID,
    "AddressingUnsupportedByEngine",
    "CaBundleNotFound",
    "CaBundleKeyMissing",
    "CaBundleTooLarge",
    "CaBundleInvalid",
    "DestinationNotValid",
];

/// Decide one `BackupDestination`'s `Valid` condition (D2 §3.3), as a pure
/// function of the object and one observation.
///
/// It checks what CEL rules R0–R9 cannot:
///
/// * **`AddressingUnsupportedByEngine`** depends on the engine version: the
///   pinned engine forces path-style whenever an endpoint is set, and a later
///   one may not, which must not require a new CRD.
/// * **The CA bundle's existence, key, size and content** depend on another
///   object, which CEL cannot read.
///
/// It also re-evaluates R3–R6 through `logweir_core::destination::validate`, so
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

/// The `observedAt` a verdict takes: `now` for a new verdict, the stored value
/// for an unchanged one.
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
