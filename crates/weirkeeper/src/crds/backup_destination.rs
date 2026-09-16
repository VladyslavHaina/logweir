//! `BackupDestination` — where archives live, saved once and referenced, with
//! no credential value anywhere in it.
//!
//! # Why a kind and not a field (ADR 0008 Amendment F)
//!
//! A destination is shared by schedules, manual backups, restores and catalog
//! import. Copying the same bucket, endpoint and credential reference into
//! every one of them is the defect PLAT-08 removes: four spellings of one
//! location is how a controller and a runner come to disagree about which
//! archive they read. A `ConfigMap` convention would carry no structural
//! schema, no CEL, no status, no printer columns and no RBAC separation.
//!
//! # The two halves, and why only one of them is immutable
//!
//! `spec.storage` and `spec.transport.security` are **immutable** (R1, R2): a
//! different location is a different destination, and a transport that could
//! be changed in place would let an edit silently move an existing archive's
//! traffic onto plaintext HTTP. Everything else — the description, the CA
//! reference, every credential reference — is **mutable**, because rotation is
//! an operational routine and a rotation that required recreating the object
//! would require re-pointing every schedule that names it.
//!
//! Executions never read this object at run time: PLAT-06.1's freeze copies a
//! resolved snapshot into the run's immutable inputs, so a later edit cannot
//! change a run that already exists.
//!
//! # Addressing is never a transport choice
//!
//! `storage.addressing` (`PathStyle` / `VirtualHosted`) says how a request
//! names the bucket. It says NOTHING about whether the connection is
//! encrypted, in either direction. R3 is the rule that states the one real
//! coupling — the endpoint scheme must match the declared transport — and it
//! names the endpoint, never the addressing. This is the UI defect G5 written
//! into the schema so it cannot come back.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use logweir_core::destination::{Addressing, StorageProvider, TransportSecurity};

use super::{Condition, Time};

/// The scheme a `Backup`, `BackupSchedule` or `Restore` uses in its
/// `archive.url` when it delegates the location to a `BackupDestination`.
///
/// A SENTINEL, NOT AN ABSENT FIELD. `archive` stays required so an older
/// controller can still DESERIALIZE the object — a missing required field
/// fails the whole list/watch decode in `kube-runtime`'s reflector, which
/// would stall every `Backup` reconcile after a rollback rather than refusing
/// one object. With the sentinel the old controller decodes the object, fails
/// to parse the unknown scheme and writes the terminal `ArchiveUrlUnreadable`
/// before any POST.
pub const DESTINATION_URL_SCHEME: &str = "logweir-destination://";

/// A DNS-1123 subdomain — what a Secret or ConfigMap `metadata.name` must be.
pub const OBJECT_NAME_PATTERN: &str = super::kafka_cluster::OBJECT_NAME_PATTERN;

/// The API server's own rule for a `data` key in a Secret or ConfigMap.
pub const DATA_KEY_PATTERN: &str = super::kafka_cluster::DATA_KEY_PATTERN;

/// `spec.storage.bucket` (D2 §3.1).
pub const BUCKET_PATTERN: &str = r"^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$";

/// `spec.storage.region` (D2 §3.1).
pub const REGION_PATTERN: &str = r"^[a-z0-9-]{1,32}$";

// ---------------------------------------------------------------------------
// The CEL rules of D2 §3.2, verbatim, each beside the message it travels with.
// ---------------------------------------------------------------------------

/// R0 — the name budget, on the schema ROOT because that is the one place a
/// rule may read `self.metadata.name`.
pub const R0_NAME_RULE: &str = "size(self.metadata.name) <= 63";
/// R0's message.
pub const R0_NAME_MESSAGE: &str = "BackupDestination names are at most 63 characters";

/// R1 — the whole location is immutable.
pub const R1_STORAGE_IMMUTABLE_RULE: &str = "self.storage == oldSelf.storage";
/// R1's message.
pub const R1_STORAGE_IMMUTABLE_MESSAGE: &str =
    "spec.storage is immutable: a different location is a different BackupDestination";

/// R2 — transport security is immutable.
pub const R2_TRANSPORT_IMMUTABLE_RULE: &str =
    "self.transport.security == oldSelf.transport.security";
/// R2's message.
pub const R2_TRANSPORT_IMMUTABLE_MESSAGE: &str =
    "spec.transport.security is immutable: transport can never be changed in place";

/// R3 — the endpoint scheme and the declared transport must agree. Addressing
/// appears nowhere in it, in either direction.
pub const R3_TRANSPORT_SCHEME_RULE: &str = "self.transport.security == 'InsecureHTTP' ? (has(self.storage.endpoint) && self.storage.endpoint.startsWith('http://')) : (!has(self.storage.endpoint) || self.storage.endpoint.startsWith('https://'))";
/// R3's message.
pub const R3_TRANSPORT_SCHEME_MESSAGE: &str = "transport.security must match the endpoint scheme: TLS needs an https:// endpoint or none; InsecureHTTP needs an explicit http:// endpoint. storage.addressing never changes transport";

/// R4 — a CA bundle is trust material for TLS and means nothing without it.
pub const R4_CA_REQUIRES_TLS_RULE: &str =
    "!has(self.transport.caBundle) || self.transport.security == 'TLS'";
/// R4's message.
pub const R4_CA_REQUIRES_TLS_MESSAGE: &str = "transport.caBundle requires transport.security TLS";

// R5 is regex-only ON PURPOSE. The CEL URL library cannot be proved present on
// the 1.29 floor Global Constraint 25 fixes, and a rule the API server refuses
// to compile is a CRD that does not install at all. Scheme, host and optional
// port; no userinfo, path, query or fragment. `logweir_core::destination`'s
// `endpoint_shape_ok` is the same shape hand-written for the pure layer.
/// R5 — `spec.storage.endpoint` is an http(s) ORIGIN and nothing more.
pub const R5_ENDPOINT_RULE: &str = concat!(
    "!has(self.endpoint) || self.endpoint.matches('",
    r"^https?://([A-Za-z0-9]([A-Za-z0-9-]*[A-Za-z0-9])?(\\.[A-Za-z0-9]([A-Za-z0-9-]*[A-Za-z0-9])?)*|\\[[0-9A-Fa-f:.]+\\])(:[0-9]{1,5})?/?$",
    "')"
);
/// R5's message.
pub const R5_ENDPOINT_MESSAGE: &str = "storage.endpoint must be an http(s) origin: scheme, host and optional port only (no userinfo, path, query or fragment)";

/// R6 — the prefix is relative, has no empty, `.` or `..` segment, and may
/// never be Logweir's own evidence root.
pub const R6_PREFIX_RULE: &str = concat!(
    "self.prefix == '' || (self.prefix.matches(\"",
    r"^[A-Za-z0-9!_.*'()-]+(/[A-Za-z0-9!_.*'()-]+)*$",
    "\") && !self.prefix.matches('",
    r"(^|/)[.]{1,2}(/|$)",
    "') && !self.prefix.matches('",
    r"^logweir(/|$)",
    "'))"
);
/// R6's message.
pub const R6_PREFIX_MESSAGE: &str = "storage.prefix is relative with no empty, '.' or '..' segment, and may not be the reserved evidence root logweir/";

/// R7 — a grant's fields must match its mode.
pub const R7_GRANT_SHAPE_RULE: &str =
    "self.mode == 'SecretKeys' ? (has(self.secret) && !has(self.workloadIdentity)) : !has(self.secret)";
/// R7's message.
pub const R7_GRANT_SHAPE_MESSAGE: &str =
    "SecretKeys needs secret and no workloadIdentity; WorkloadIdentity takes no secret";

/// R8 — the evidence-read grant has two more modes, and neither takes a
/// reference.
pub const R8_EVIDENCE_READ_SHAPE_RULE: &str = "self.mode == 'SecretKeys' ? (has(self.secret) && !has(self.workloadIdentity)) : (!has(self.secret) && (self.mode == 'WorkloadIdentity' || !has(self.workloadIdentity)))";
/// R8's message.
pub const R8_EVIDENCE_READ_SHAPE_MESSAGE: &str = "evidenceRead fields must match its mode";

/// R9 — reusing the archive grant for verification requires an explicit
/// READ-ONLY archive grant; a write grant is never reused to read evidence.
pub const R9_ARCHIVE_READ_GRANT_RULE: &str =
    "!has(self.evidenceRead) || self.evidenceRead.mode != 'ArchiveReadGrant' || has(self.archiveRead)";
/// R9's message.
pub const R9_ARCHIVE_READ_GRANT_MESSAGE: &str = "evidenceRead ArchiveReadGrant requires an explicit read-only archiveRead grant; a write grant is never reused for verification";

/// The four rules that sit on `.spec` itself, in R-order.
///
/// R1 and R2 name `oldSelf`, so the placement is load-bearing for the reason
/// [`super::backup_schedule::SUSPEND_ONLY_RULE`]'s is: a per-field transition
/// rule does not fire on an absent → present transition at the 1.29 floor.
pub const SPEC_RULES: [super::SpecRule; 4] = [
    super::SpecRule::new(R1_STORAGE_IMMUTABLE_RULE, R1_STORAGE_IMMUTABLE_MESSAGE),
    super::SpecRule::new(R2_TRANSPORT_IMMUTABLE_RULE, R2_TRANSPORT_IMMUTABLE_MESSAGE),
    super::SpecRule::new(R3_TRANSPORT_SCHEME_RULE, R3_TRANSPORT_SCHEME_MESSAGE),
    super::SpecRule::new(R4_CA_REQUIRES_TLS_RULE, R4_CA_REQUIRES_TLS_MESSAGE),
];

/// The rules attached BELOW `.spec`, each at the node whose fields it reads.
///
/// None of them names `oldSelf`: a cross-field validation rule is evaluated
/// exactly when the object it is attached to exists, which is exactly when it
/// has something to say.
pub const NESTED_RULES: [(&[&str], &str, &str); 7] = [
    (&["storage"], R5_ENDPOINT_RULE, R5_ENDPOINT_MESSAGE),
    (&["storage"], R6_PREFIX_RULE, R6_PREFIX_MESSAGE),
    (
        &["access", "archiveWrite"],
        R7_GRANT_SHAPE_RULE,
        R7_GRANT_SHAPE_MESSAGE,
    ),
    (
        &["access", "archiveRead"],
        R7_GRANT_SHAPE_RULE,
        R7_GRANT_SHAPE_MESSAGE,
    ),
    (
        &["access", "evidenceWrite"],
        R7_GRANT_SHAPE_RULE,
        R7_GRANT_SHAPE_MESSAGE,
    ),
    (
        &["access", "evidenceRead"],
        R8_EVIDENCE_READ_SHAPE_RULE,
        R8_EVIDENCE_READ_SHAPE_MESSAGE,
    ),
    (
        &["access"],
        R9_ARCHIVE_READ_GRANT_RULE,
        R9_ARCHIVE_READ_GRANT_MESSAGE,
    ),
];

/// The default `accessKeyId` data key — today's `ARCHIVE_ACCESS_KEY`.
pub const DEFAULT_ACCESS_KEY_ID_KEY: &str = "access-key-id";
/// The default `secretAccessKey` data key — today's `ARCHIVE_SECRET_KEY`.
pub const DEFAULT_SECRET_ACCESS_KEY_KEY: &str = "secret-access-key";
/// The default CA `ConfigMap` key.
pub const DEFAULT_CA_KEY: &str = "ca.crt";
/// The runner ServiceAccount a workload-identity grant assumes.
pub const DEFAULT_RUNNER_SERVICE_ACCOUNT: &str = "logweir-runner";

fn default_access_key_id_key() -> String {
    DEFAULT_ACCESS_KEY_ID_KEY.to_string()
}
fn default_secret_access_key_key() -> String {
    DEFAULT_SECRET_ACCESS_KEY_KEY.to_string()
}
fn default_ca_key() -> String {
    DEFAULT_CA_KEY.to_string()
}
fn default_runner_service_account() -> String {
    DEFAULT_RUNNER_SERVICE_ACCOUNT.to_string()
}

/// Where the archive root is. **Immutable** (R1).
#[derive(Deserialize, Serialize, Clone, Debug, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct StorageLocation {
    /// The object-store provider. One value, so a second one is a reviewable
    /// event with its own validation rules rather than free text.
    pub provider: StorageProvider,
    /// The bucket.
    #[schemars(regex(path = "BUCKET_PATTERN"), length(min = 3, max = 63))]
    pub bucket: String,
    /// The key prefix inside the bucket, relative, `""` for the bucket root.
    /// Never `logweir` or anything under it: that is Logweir's own evidence
    /// root (Global Constraint 6) and R6 refuses it.
    #[serde(default)]
    #[schemars(length(max = 512))]
    pub prefix: String,
    /// The region, when the provider needs one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(regex(path = "REGION_PATTERN"))]
    pub region: Option<String>,
    /// An http(s) ORIGIN. Absent means AWS S3.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(length(max = 2048))]
    pub endpoint: Option<String>,
    /// How a request names the bucket. **Never** a transport choice: see the
    /// module header and R3.
    pub addressing: Addressing,
}

/// A CA bundle in a `ConfigMap` in this namespace.
///
/// A `ConfigMap` AND NEVER A SECRET. A certificate authority certificate is
/// public material by construction; putting it in a Secret would widen the set
/// of objects the runner's ServiceAccount must be able to read for no gain.
#[derive(Deserialize, Serialize, Clone, Debug, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct CaBundleRef {
    /// The `ConfigMap` name, in this namespace.
    #[schemars(regex(path = "OBJECT_NAME_PATTERN"), length(min = 1, max = 253))]
    pub config_map_name: String,
    /// The data key holding the PEM bundle.
    #[serde(default = "default_ca_key")]
    #[schemars(regex(path = "DATA_KEY_PATTERN"), length(min = 1, max = 253))]
    pub key: String,
}

/// Transport security, and the trust material it may be given.
#[derive(Deserialize, Serialize, Clone, Debug, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct TransportBlock {
    /// `TLS` or `InsecureHTTP`. **Immutable** (R2), and the ONE field that
    /// decides whether plaintext is permitted.
    pub security: TransportSecurity,
    /// A private CA for `TLS`. Mutable, because a CA rotates.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ca_bundle: Option<CaBundleRef>,
}

/// How a role's credential is obtained.
#[derive(Deserialize, Serialize, Clone, Copy, Debug, JsonSchema, PartialEq, Eq)]
pub enum AccessMode {
    /// Keys read from a Secret in this namespace, projected into the Job.
    SecretKeys,
    /// The pod's own ServiceAccount identity; no Secret is projected.
    WorkloadIdentity,
}

/// How the evidence-read role's credential is obtained.
///
/// Two more values than [`AccessMode`], and neither takes a reference:
/// `ControllerIdentity` is the controller's own read-only handle, and
/// `ArchiveReadGrant` reuses the destination's explicit read-only archive
/// grant (R9 refuses reusing a write grant).
#[derive(Deserialize, Serialize, Clone, Copy, Debug, JsonSchema, PartialEq, Eq)]
pub enum EvidenceReadMode {
    /// Keys read from a Secret in this namespace.
    SecretKeys,
    /// The pod's own ServiceAccount identity.
    WorkloadIdentity,
    /// The controller's own read-only object-store handle.
    ControllerIdentity,
    /// The destination's explicit read-only `archiveRead` grant.
    ArchiveReadGrant,
}

/// A Secret in this namespace, and the keys inside it. **References only.**
#[derive(Deserialize, Serialize, Clone, Debug, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct S3SecretKeysRef {
    /// The Secret name, in this namespace.
    #[schemars(regex(path = "OBJECT_NAME_PATTERN"), length(min = 1, max = 253))]
    pub name: String,
    /// The data key holding the access key id.
    #[serde(default = "default_access_key_id_key")]
    #[schemars(regex(path = "DATA_KEY_PATTERN"), length(min = 1, max = 253))]
    pub access_key_id_key: String,
    /// The data key holding the secret access key.
    #[serde(default = "default_secret_access_key_key")]
    #[schemars(regex(path = "DATA_KEY_PATTERN"), length(min = 1, max = 253))]
    pub secret_access_key_key: String,
    /// The data key holding a session token, when the credential is temporary.
    /// No default: a session token nobody configured is a token that does not
    /// exist.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(regex(path = "DATA_KEY_PATTERN"), length(min = 1, max = 253))]
    pub session_token_key: Option<String>,
}

/// The ServiceAccount a workload-identity grant runs as.
#[derive(Deserialize, Serialize, Clone, Debug, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct WorkloadIdentityRef {
    /// The ServiceAccount name, in this namespace.
    #[serde(default = "default_runner_service_account")]
    #[schemars(regex(path = "OBJECT_NAME_PATTERN"), length(min = 1, max = 253))]
    pub service_account_name: String,
}

/// One role's grant. R7 ties the reference shape to `mode`.
#[derive(Deserialize, Serialize, Clone, Debug, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct AccessGrant {
    /// How the credential is obtained.
    pub mode: AccessMode,
    /// The Secret and its keys, for `SecretKeys`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub secret: Option<S3SecretKeysRef>,
    /// The ServiceAccount, for `WorkloadIdentity`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workload_identity: Option<WorkloadIdentityRef>,
}

/// The evidence-read grant. R8 ties the reference shape to `mode`.
#[derive(Deserialize, Serialize, Clone, Debug, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct EvidenceReadAccessGrant {
    /// How the credential is obtained.
    pub mode: EvidenceReadMode,
    /// The Secret and its keys, for `SecretKeys`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub secret: Option<S3SecretKeysRef>,
    /// The ServiceAccount, for `WorkloadIdentity`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workload_identity: Option<WorkloadIdentityRef>,
}

/// The four grants. **Mutable**, because credentials rotate.
///
/// ABSENT IS A DEFINED ANSWER, NOT A FALLBACK TO SOMETHING WIDER:
/// `archiveRead` and `evidenceWrite` absent mean "use `archiveWrite`", and
/// `evidenceRead` absent means verification is `NotAttempted` and says so.
/// Nothing here ever silently widens a grant.
#[derive(Deserialize, Serialize, Clone, Debug, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct AccessBlock {
    /// The grant execution Jobs write the archive with. Required: a
    /// destination nothing may write to is not a backup destination.
    pub archive_write: AccessGrant,
    /// The read-only archive grant. Absent means `archiveWrite` is used.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub archive_read: Option<AccessGrant>,
    /// The grant that writes evidence under `logweir/`. Absent means
    /// `archiveWrite` is used.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub evidence_write: Option<AccessGrant>,
    /// The grant that reads evidence back for verification. Absent means
    /// verification is `NotAttempted`, with a detail naming this field.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub evidence_read: Option<EvidenceReadAccessGrant>,
}

/// Whether an explicit readiness test may write a marker object.
#[derive(Deserialize, Serialize, Clone, Copy, Debug, Default, JsonSchema, PartialEq, Eq)]
pub enum WriteProbe {
    /// No object is ever written by a readiness test. The default, and the
    /// only value that keeps Global Constraint 6's create-only boundary
    /// untouched for a destination nobody has opted in.
    #[default]
    Disabled,
    /// A `Preflight` with `operation: DestinationAccess` may create ONE marker
    /// object under the destination's own prefix. It is never deleted.
    CreateOnlyMarker,
}

/// Explicit readiness settings. There is **no periodic health probe**: a
/// destination is exercised by operations and by an explicit `Preflight`,
/// because cached health rendered as readiness is the defect PLAT-03 names.
#[derive(Deserialize, Serialize, Clone, Debug, Default, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ReadinessBlock {
    /// Whether a readiness test may write a marker. Absent means `Disabled`.
    #[serde(default)]
    pub write_probe: WriteProbe,
}

/// `BackupDestination.spec`.
#[derive(kube::CustomResource, Deserialize, Serialize, Clone, Debug, JsonSchema)]
#[kube(
    group = "logweir.dev",
    version = "v1alpha1",
    kind = "BackupDestination",
    doc = "Where archives live, saved once and referenced by name. `spec.storage` and `spec.transport.security` are immutable — a different location or transport is a different destination — while the description, the CA reference and every credential reference are mutable so that rotation needs no new object. It holds NO credential value; executions freeze a resolved snapshot, so a later edit never changes a run that already exists.",
    plural = "backupdestinations",
    singular = "backupdestination",
    namespaced,
    status = "BackupDestinationStatus",
    printcolumn = r#"{"name":"BUCKET","type":"string","jsonPath":".spec.storage.bucket"}"#,
    printcolumn = r#"{"name":"ENDPOINT","type":"string","jsonPath":".spec.storage.endpoint","description":"absent means AWS S3"}"#,
    printcolumn = r#"{"name":"TRANSPORT","type":"string","jsonPath":".spec.transport.security","description":"TLS or InsecureHTTP; never derived from addressing"}"#,
    printcolumn = r#"{"name":"VALID","type":"string","jsonPath":".status.conditions[?(@.type==\"Valid\")].status"}"#,
    printcolumn = r#"{"name":"AGE","type":"date","jsonPath":".metadata.creationTimestamp"}"#
)]
#[serde(rename_all = "camelCase")]
pub struct BackupDestinationSpec {
    /// What this destination is, for a human reading `kubectl get`. Mutable.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(length(max = 256))]
    pub description: Option<String>,
    /// The location. **Immutable** (R1).
    pub storage: StorageLocation,
    /// Transport security and its trust material. `security` is **immutable**
    /// (R2); `caBundle` is not.
    pub transport: TransportBlock,
    /// The four credential references. Mutable, and references only.
    pub access: AccessBlock,
    /// Explicit readiness settings. Absent means `writeProbe: Disabled`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub readiness: Option<ReadinessBlock>,
}

/// `BackupDestination.status`.
#[derive(Deserialize, Serialize, Clone, Debug, Default, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct BackupDestinationStatus {
    /// The `metadata.generation` the verdict below was computed from.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub observed_generation: Option<i64>,
    /// The `Valid` condition's reason, promoted to a scalar — `Valid`,
    /// `AddressingUnsupportedByEngine`, `CaBundleNotFound`,
    /// `CaBundleKeyMissing`, `CaBundleTooLarge`, `CaBundleInvalid`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    /// The archive root as one URL — `s3://<bucket>/<prefix>` — so an operator
    /// and a plan never spell the same location two ways.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub canonical_url: Option<String>,
    /// `sha256:<lowercase hex>` over the canonical location, as
    /// `logweir_core::destination::DestinationLocation::location_digest`
    /// computes it. What a frozen plan is compared against.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub location_digest: Option<String>,
    /// `sha256:<lowercase hex>` over the CA bytes read from the `ConfigMap`,
    /// so a rotation is visible without reading the bundle again.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ca_bundle_sha256: Option<String>,
    /// When the controller last reached this verdict. A new verdict takes a
    /// new instant; an unchanged one keeps the stored one (erratum E11(d)).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub observed_at: Option<Time>,
    /// The condition set. One type, `Valid`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub conditions: Option<Vec<Condition>>,
}
