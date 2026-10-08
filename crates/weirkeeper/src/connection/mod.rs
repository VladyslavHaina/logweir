//! The saved-connection resolver — PLAT-07.1's ONE answer to "what does a
//! runner Job need to dial this `KafkaCluster`".
//!
//! # One resolution, every path
//!
//! [`resolve`] turns a `KafkaCluster` into a [`ResolvedConnection`]: the public
//! settings a plan or an argv carries (bootstrap servers, auth mode, username,
//! TLS) and the REFERENCES the kubelet resolves in the Job's namespace (the
//! password's Secret and key, the CA's Secret or ConfigMap and key). The probe
//! (`controllers::kafka_cluster`), the backup (`controllers::backup`) and the
//! restore (`controllers::restore`) Jobs are all built from it through
//! [`ResolvedConnection::project`], and future discovery and preflight check
//! Jobs are meant to be too — a check Job built any other way would be testing
//! a connection the backup does not use.
//!
//! Before this module each path assembled its own credential environment:
//! the probe and the backup from `kafka_cluster::source_password_env`, the
//! restore from an inline copy of the same match. Two copies of one rule is
//! how a probe comes to report `reachable: true` for settings a backup then
//! dials differently.
//!
//! # The controller reads no credential, and this module is why that is enough
//!
//! Spec §9: there is no `get` on Secrets anywhere in the control plane. The
//! resolver never needs one. It validates the SHAPE of a reference — present,
//! a DNS-1123 name, a legal data key — and writes the reference into a Job as
//! `valueFrom.secretKeyRef` or a projected volume. Whether the named object
//! exists is a fact only the kubelet (and a future PLAT-03.1 check Job) can
//! establish; the refusal vocabulary below says so rather than pretending.
//!
//! # Rotation, and what is bound by a hash
//!
//! Nothing here copies a value, so a rotated password or CA is picked up by
//! the NEXT Job the controller creates, with no edit to the `KafkaCluster`: the
//! kubelet resolves `secretKeyRef` at container start and projects the CA file
//! at pod start. A Job already running keeps the environment it started with.
//! The username, the bootstrap servers and the TLS switch are identity, not
//! credentials: a restore plan binds them in `planBytes` (and so in the
//! approval's plan hash), and a backup receipt records the username. The
//! password and the CA are never bound by any hash — binding them would turn
//! every rotation into a re-approval.
//!
//! # Refusals happen before any Job, ConfigMap or plan exists
//!
//! [`ConnectionRefusal`] carries a CamelCase terminal state from
//! [`crate::conditions`] and a message that names fields, objects and keys and
//! never a value. The three controllers convert it into their own terminal
//! status before their first `POST`.

use kube::ResourceExt as _;
use serde::{Deserialize, Serialize};

use crate::conditions::{
    TERMINAL_STATE_CONNECTION_CONFIG_INVALID, TERMINAL_STATE_CONNECTION_FIELD_UNSUPPORTED,
    TERMINAL_STATE_CONNECTION_PLAN_MISMATCH, TERMINAL_STATE_CONNECTION_REFERENCE_INVALID,
    TERMINAL_STATE_CREDENTIAL_NOT_RENDERABLE, TERMINAL_STATE_PLAIN_WITHOUT_TLS,
};
use crate::crds::kafka_cluster::{
    AuthBlock, AuthMode, KafkaCluster, KafkaClusterSpec, ObjectKeyRef, UnrecognizedFields,
    DEFAULT_CLIENT_CERTIFICATE_KEY, DEFAULT_CLIENT_PRIVATE_KEY_KEY, DEFAULT_PASSWORD_KEY,
};
use crate::job::{ConfigMapMount, EnvFromSecret, SecretMount};
use logweir_core::spec::AuthSpec;

pub mod credential;

pub use logweir_core::connection::{
    CONTRACT_VERSION, SOURCE_TLS_CA_FILE_ENV, SOURCE_TLS_CERT_FILE_ENV, SOURCE_TLS_KEY_FILE_ENV,
    TARGET_TLS_CA_FILE_ENV, TARGET_TLS_CERT_FILE_ENV, TARGET_TLS_KEY_FILE_ENV,
};

/// The file name a projected CA certificate has inside its volume, whatever
/// data key it came from. One name, so the path the runner is handed does not
/// depend on the adopter's key.
pub const CA_FILE_NAME: &str = "ca.crt";

/// The file names a projected `mtls` client certificate and key have inside
/// their volume, whatever data keys they came from (PROD-01.3).
pub const CLIENT_CERT_FILE_NAME: &str = "tls.crt";
/// See [`CLIENT_CERT_FILE_NAME`].
pub const CLIENT_KEY_FILE_NAME: &str = "tls.key";

/// WHAT a connection is being resolved for.
///
/// **THE SECOND ARGUMENT OF [`resolve`], AND NOT DECORATION.** It decides the
/// side a resolution projects under ([`ConnectionUse::side`]) and the
/// ServiceAccount the Job runs as ([`ConnectionUse::service_account_name`]), so
/// a caller cannot pick a variable family that disagrees with the run it is
/// building — `project` takes no side argument for exactly that reason.
///
/// The refusal rules do NOT vary by use, and that is the point: a discovery or
/// preflight check that accepted a connection a backup would refuse would be a
/// green light for a run that cannot happen. Any new use is added here, and
/// gets the same answer.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum ConnectionUse {
    /// `logweir cluster-probe`, from the `KafkaCluster` controller.
    Probe,
    /// `logweir backup run` — the cluster is the run's SOURCE.
    BackupSource,
    /// `logweir restore run` / `logweir drill run` — the cluster is the TARGET.
    RestoreTarget,
    /// A topic-discovery check Job (D2 W8). Reads the SOURCE side.
    Discovery,
    /// A preflight check Job (D2 W9) for a `Backup` or a `DestinationAccess`
    /// — the side a backup would dial.
    PreflightSource,
    /// A preflight check Job (D2 W9) for a `Restore` — the side a restore
    /// would dial. Separate from [`ConnectionUse::PreflightSource`] because the
    /// two project different variable families, and a preflight that checked
    /// the wrong one would pass on a connection the run never uses.
    PreflightTarget,
}

impl ConnectionUse {
    /// The runner variable family this use projects under.
    #[must_use]
    pub const fn side(self) -> Side {
        match self {
            ConnectionUse::Probe
            | ConnectionUse::BackupSource
            | ConnectionUse::Discovery
            | ConnectionUse::PreflightSource => Side::Source,
            ConnectionUse::RestoreTarget | ConnectionUse::PreflightTarget => Side::Target,
        }
    }

    /// The ServiceAccount a Job for this use runs as.
    ///
    /// ONE ACCOUNT FOR EVERY USE TODAY (`logweir-runner`), stated as a function
    /// rather than a constant so a check Job that later needs a narrower
    /// account changes this line and not six Job builders.
    #[must_use]
    pub const fn service_account_name(self) -> &'static str {
        crate::controllers::backup::RUNNER_SERVICE_ACCOUNT
    }

    /// The use as a stable lowercase token, for a status field or a log line.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            ConnectionUse::Probe => "probe",
            ConnectionUse::BackupSource => "backupSource",
            ConnectionUse::RestoreTarget => "restoreTarget",
            ConnectionUse::Discovery => "discovery",
            ConnectionUse::PreflightSource => "preflightSource",
            ConnectionUse::PreflightTarget => "preflightTarget",
        }
    }

    /// Every use, so a test can assert a property across all of them.
    pub const ALL: [ConnectionUse; 6] = [
        ConnectionUse::Probe,
        ConnectionUse::BackupSource,
        ConnectionUse::RestoreTarget,
        ConnectionUse::Discovery,
        ConnectionUse::PreflightSource,
        ConnectionUse::PreflightTarget,
    ];
}

/// Where a Job built from a resolution runs, and as whom.
///
/// The NETWORK EXECUTION CONTEXT the saved-connection contract owes its
/// callers: a discovery or preflight Job that checked a connection from a
/// different ServiceAccount, or in a different namespace, would be checking a
/// path the real run never takes. There is deliberately no node placement
/// field: `KafkaCluster` has no placement setting in contract v1, and an
/// always-`None` field would read as a control that exists.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ExecutionContext {
    /// The namespace the Job runs in — the `KafkaCluster`'s own, and the only
    /// one in which its references resolve to the intended objects.
    pub namespace: String,
    /// The ServiceAccount the runner pod uses.
    pub service_account_name: String,
    /// The variable family the connection projects under.
    pub side: Side,
    /// Always `false`: a runner reaches no API server (`job::build`).
    pub automount_service_account_token: bool,
}

/// Which runner variable family a connection is projected under.
///
/// A PROBE AND A BACKUP ARE THE SOURCE SIDE, A RESTORE IS THE TARGET SIDE. The
/// runner has one password variable per side of a drill
/// (`LOGWEIR_SOURCE_PASSWORD` / `LOGWEIR_TARGET_PASSWORD`) and the probe reads
/// the source one whatever `spec.role` says; the CA variables and volumes
/// follow the same split. The REFERENCES a side projects are identical for one
/// `KafkaCluster` — only the variable and volume names differ, which
/// `tests/connection.rs` asserts across all three Jobs.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum Side {
    /// `logweir cluster-probe` and `logweir backup run`.
    Source,
    /// `logweir restore run`.
    Target,
}

impl Side {
    /// The environment variable the SASL password is projected into.
    #[must_use]
    pub const fn password_env(self) -> &'static str {
        match self {
            Side::Source => crate::controllers::kafka_cluster::SOURCE_PASSWORD_ENV,
            Side::Target => crate::controllers::restore::TARGET_PASSWORD_ENV,
        }
    }

    /// The environment variable naming the projected CA file.
    #[must_use]
    pub const fn tls_ca_file_env(self) -> &'static str {
        match self {
            Side::Source => SOURCE_TLS_CA_FILE_ENV,
            Side::Target => TARGET_TLS_CA_FILE_ENV,
        }
    }

    /// The pod volume the CA is projected as. Unique beside every volume
    /// `job::build` and the three controllers add (`signing`, `approval`,
    /// `plan`, `work`).
    #[must_use]
    pub const fn ca_volume(self) -> &'static str {
        match self {
            Side::Source => "source-ca",
            Side::Target => "target-ca",
        }
    }

    /// Where [`Side::ca_volume`] is mounted, read-only.
    #[must_use]
    pub const fn ca_mount_path(self) -> &'static str {
        match self {
            Side::Source => "/connection/source-ca",
            Side::Target => "/connection/target-ca",
        }
    }

    /// The CA file's full path inside the pod — the value of
    /// [`Side::tls_ca_file_env`].
    #[must_use]
    pub fn ca_file_path(self) -> String {
        format!("{}/{CA_FILE_NAME}", self.ca_mount_path())
    }

    /// The variable the credential Secret's `logweir-binding` key is projected
    /// into (optional), PROD-01.3 security follow-up.
    #[must_use]
    pub const fn credential_binding_env(self) -> &'static str {
        match self {
            Side::Source => logweir_core::connection::SOURCE_CREDENTIAL_BINDING_ENV,
            Side::Target => logweir_core::connection::TARGET_CREDENTIAL_BINDING_ENV,
        }
    }

    /// The variable carrying the binding the runner must find — a literal.
    #[must_use]
    pub const fn credential_binding_expected_env(self) -> &'static str {
        match self {
            Side::Source => logweir_core::connection::SOURCE_CREDENTIAL_BINDING_EXPECTED_ENV,
            Side::Target => logweir_core::connection::TARGET_CREDENTIAL_BINDING_EXPECTED_ENV,
        }
    }

    /// The variable naming the projected `mtls` client certificate (PROD-01.3).
    #[must_use]
    pub const fn tls_cert_file_env(self) -> &'static str {
        match self {
            Side::Source => SOURCE_TLS_CERT_FILE_ENV,
            Side::Target => TARGET_TLS_CERT_FILE_ENV,
        }
    }

    /// The variable naming the projected `mtls` client key (PROD-01.3).
    #[must_use]
    pub const fn tls_key_file_env(self) -> &'static str {
        match self {
            Side::Source => SOURCE_TLS_KEY_FILE_ENV,
            Side::Target => TARGET_TLS_KEY_FILE_ENV,
        }
    }

    /// The pod volume the client certificate Secret is projected as —
    /// unique beside [`Side::ca_volume`] and every other runner volume.
    #[must_use]
    pub const fn client_cert_volume(self) -> &'static str {
        match self {
            Side::Source => "source-client-cert",
            Side::Target => "target-client-cert",
        }
    }

    /// Where [`Side::client_cert_volume`] is mounted, read-only.
    #[must_use]
    pub const fn client_cert_mount_path(self) -> &'static str {
        match self {
            Side::Source => "/connection/source-client-cert",
            Side::Target => "/connection/target-client-cert",
        }
    }

    /// The client certificate file's full path inside the pod.
    #[must_use]
    pub fn client_cert_file_path(self) -> String {
        format!("{}/{CLIENT_CERT_FILE_NAME}", self.client_cert_mount_path())
    }

    /// The client key file's full path inside the pod.
    #[must_use]
    pub fn client_key_file_path(self) -> String {
        format!("{}/{CLIENT_KEY_FILE_NAME}", self.client_cert_mount_path())
    }
}

/// The Secret, and the two keys in it, an `mtls` connection's client
/// certificate and private key are projected from (PROD-01.3). A reference:
/// the controller never reads the Secret.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ClientCertificateReference {
    /// The Secret's `metadata.name`.
    pub name: String,
    /// The data key of the PEM certificate.
    pub certificate_key: String,
    /// The data key of the PEM private key.
    pub private_key_key: String,
}

/// A key of a Secret in the connection's namespace.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SecretKeyReference {
    /// The Secret's `metadata.name`.
    pub name: String,
    /// The data key.
    pub key: String,
}

/// Which kind of object a CA certificate is read from.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum CaSourceKind {
    /// A `Secret`, projected at mode `0440` with the pod's `fsGroup`.
    Secret,
    /// A `ConfigMap` — a CA certificate is public.
    ConfigMap,
}

/// A key of a Secret or ConfigMap holding PEM CA certificate(s).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CaReference {
    /// Secret or ConfigMap.
    pub kind: CaSourceKind,
    /// The object's `metadata.name`.
    pub name: String,
    /// The data key.
    pub key: String,
}

/// One `KafkaCluster`, resolved: everything a runner Job needs to dial it, as
/// public settings and references, and nothing else.
///
/// SERIALISABLE AND VALUE-FREE ON PURPOSE. It is the shape an immutable run
/// snapshot can freeze (PLAT-06.1) and a product API can return (PLAT-17.1):
/// every field is a setting or a reference, never a credential.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ResolvedConnection {
    /// [`CONTRACT_VERSION`] at resolution time.
    pub contract_version: String,
    /// What this resolution is for — [`resolve`]'s second argument.
    pub connection_use: ConnectionUse,
    /// The namespace every reference below is resolved in — the
    /// `KafkaCluster`'s own, and the only one a Job built from it may run in.
    pub namespace: String,
    /// The `KafkaCluster`'s `metadata.name`.
    pub cluster_name: String,
    /// The `KafkaCluster`'s `metadata.uid`, when the object came from an API
    /// server.
    ///
    /// AN OPTION, NOT A `String`: `plan_backup_spec` resolves a connection to
    /// build a plan document, and a plan is a function of the SPEC — it must
    /// not depend on whether the caller had a stored object. A consumer that
    /// needs identity (a check result naming the object it checked) requires
    /// `Some` itself and says so; `None` never reaches a live path, because
    /// every controller that builds a Job reads the UID for the owner
    /// reference first.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub uid: Option<String>,
    /// The `KafkaCluster`'s `metadata.generation` — the spec version this
    /// resolution was produced from, for a consumer recording "checked at
    /// generation N" (D2 freshness). `None` for an object with no generation,
    /// as above.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub generation: Option<i64>,
    /// Where the Job runs and as whom.
    pub execution: ExecutionContext,
    /// The Kafka principal this connection authenticates as:
    /// `User:<username>` for `scramSha512`, `User:ANONYMOUS` for `plaintext`.
    ///
    /// The string an adopter writes in an ACL, so a refusal or a check result
    /// can name the identity whose permissions are missing. Derived from
    /// `auth.username`, which is public identity and never a credential.
    pub principal: String,
    /// `spec.bootstrapServers`, verbatim and in order.
    pub bootstrap_servers: Vec<String>,
    /// A digest of the bootstrap list AS A SET — trimmed entries, deduplicated,
    /// sorted, newline-joined, `sha256:`-prefixed.
    ///
    /// AS A SET, because that is how [`ResolvedConnection::check_restore_plan`]
    /// compares a plan's address list with a connection's: reordering the same
    /// addresses is not a different cluster. A consumer (a check result, a
    /// cache key, a "this is the same connection" comparison) gets one short
    /// token instead of re-implementing that rule.
    pub bootstrap_sha256: String,
    /// Mode, username and TLS, in the plan grammar the runner parses. This is
    /// the value a backup plan's `source.auth` is and a restore plan's
    /// `target.auth` must equal.
    pub auth: AuthSpec,
    /// Where the SASL password is, for `scramSha512`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub password: Option<SecretKeyReference>,
    /// Where the private CA is, when the connection names one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tls_ca: Option<CaReference>,
    /// Where the `mtls` client certificate and key are (PROD-01.3). Absent for
    /// every other mode, so a resolution of any object that predates `mtls`
    /// serialises exactly as it did.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub client_certificate: Option<ClientCertificateReference>,
}

/// What a resolution contributes to one runner Job.
///
/// FOUR LISTS, EACH APPENDED BY ITS CALLER. The three controllers already own
/// Job fields of the same four kinds (the signing key, the archive credential,
/// the approval bundle), and each keeps its own ordering: a connection's items
/// are placed where the controller placed them before this module existed, so
/// a `KafkaCluster` that names no new field produces a byte-identical Job.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ConnectionProjection {
    /// The password, as `valueFrom.secretKeyRef`. Empty for `plaintext`.
    pub env_from_secret: Vec<EnvFromSecret>,
    /// The CA file's path. Empty when no CA is named.
    pub env_literal: Vec<(String, String)>,
    /// The CA volume, when it comes from a Secret.
    pub secret_mounts: Vec<SecretMount>,
    /// The CA volume, when it comes from a ConfigMap.
    pub config_map_mounts: Vec<ConfigMapMount>,
}

impl ConnectionProjection {
    /// Every literal environment entry the projection adds — the CA file and,
    /// for `mtls`, the client certificate and key files. PATHS only.
    #[must_use]
    pub fn literal_names(&self) -> Vec<&str> {
        self.env_literal.iter().map(|(n, _)| n.as_str()).collect()
    }
}

/// Why a connection cannot be resolved, or cannot be used for a run.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ConnectionRefusal {
    /// A terminal state from [`crate::conditions::TERMINAL_STATES`].
    pub reason: &'static str,
    /// The offending field, as a dotted path (`spec.auth.tlsCa`).
    pub field: String,
    /// What is wrong and what to do. Names fields, objects and keys; never a
    /// credential value (the resolver has none to name).
    pub message: String,
}

impl std::fmt::Display for ConnectionRefusal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.reason, self.message)
    }
}

impl std::error::Error for ConnectionRefusal {}

impl From<ConnectionRefusal> for crate::controllers::backup::BackupError {
    fn from(refusal: ConnectionRefusal) -> Self {
        Self::Refused(refusal.reason, refusal.message)
    }
}

impl From<ConnectionRefusal> for crate::controllers::restore::RestoreError {
    fn from(refusal: ConnectionRefusal) -> Self {
        Self::Refused(refusal.reason, refusal.message)
    }
}

impl From<ConnectionRefusal> for crate::controllers::kafka_cluster::KafkaClusterError {
    fn from(refusal: ConnectionRefusal) -> Self {
        Self::Refused(refusal.reason, refusal.message)
    }
}

fn refusal(reason: &'static str, field: &str, message: String) -> ConnectionRefusal {
    ConnectionRefusal {
        reason,
        field: field.to_string(),
        message,
    }
}

/// Resolve one `KafkaCluster` into the connection every runner Job uses.
///
/// `connection_use` says WHAT the connection is for; it selects the runner
/// variable family and the execution context ([`ConnectionUse`]) and changes
/// no refusal — a connection a preflight accepts is one a backup can run.
///
/// # The order of the checks
///
/// 1. `metadata.namespace` must exist — every reference is resolved in it.
/// 2. No field this controller does not implement (see the CRD module's
///    header): [`TERMINAL_STATE_CONNECTION_FIELD_UNSUPPORTED`], naming them
///    all. First, because every later check would otherwise be answering a
///    question about an object whose meaning this build does not know.
/// 3. `spec.bootstrapServers` must name at least one address, and no entry may
///    be empty or carry a comma or whitespace — the probe joins the list with
///    commas and a backup plan keeps it as a list, so such an entry is two
///    different dials on two paths.
/// 4. The mode:
///    * `plaintext` with `tls: true` — refused, not downgraded
///      ([`TERMINAL_STATE_CONNECTION_CONFIG_INVALID`]); `plaintext` with a CA
///      likewise. A `secretRef` left on a plaintext object is ignored and not
///      projected, exactly as before this module.
///    * `scramSha512` needs a non-blank `username` and a `secretRef` with a
///      non-blank name ([`TERMINAL_STATE_CREDENTIAL_NOT_RENDERABLE`], the
///      state a backup reported for the same object before), a DNS-1123 name
///      and a legal `passwordKey` ([`TERMINAL_STATE_CONNECTION_REFERENCE_INVALID`]).
/// 5. `tlsCa`, when named, needs `tls: true`, exactly one source, and a legal
///    name and key.
///
/// # Errors
///
/// [`ConnectionRefusal`] for each of the above.
pub fn resolve(
    cluster: &KafkaCluster,
    connection_use: ConnectionUse,
) -> Result<ResolvedConnection, ConnectionRefusal> {
    let name = cluster.name_any();
    let namespace = cluster.namespace().ok_or_else(|| {
        refusal(
            TERMINAL_STATE_CONNECTION_REFERENCE_INVALID,
            "metadata.namespace",
            format!(
                "KafkaCluster {name} carries no metadata.namespace, so there is no namespace to \
                 resolve its references in"
            ),
        )
    })?;
    let spec = &cluster.spec;

    let unrecognized = unrecognized_paths(spec);
    if let Some(first) = unrecognized.first() {
        return Err(refusal(
            TERMINAL_STATE_CONNECTION_FIELD_UNSUPPORTED,
            first,
            format!(
                "KafkaCluster {namespace}/{name} sets {} this controller does not implement \
                 (saved-connection contract {CONTRACT_VERSION}): {}. The connection is refused \
                 rather than resolved without {}; run the controller release whose CRD declares \
                 {}, or recreate the object without {}",
                if unrecognized.len() == 1 {
                    "a field"
                } else {
                    "fields"
                },
                unrecognized.join(", "),
                pronoun(unrecognized.len()),
                pronoun(unrecognized.len()),
                pronoun(unrecognized.len()),
            ),
        ));
    }

    let bootstrap_servers = checked_bootstrap_servers(&namespace, &name, &spec.bootstrap_servers)?;
    let auth = &spec.auth;

    // PROD-01.3: a client certificate belongs to `mtls` and to nothing else.
    // The CRD's own rule refuses the shape at admission; this is the resolver's
    // half, for an object admitted by a CRD that did not carry the rule.
    if auth.client_certificate.is_some() && auth.mode != AuthMode::Mtls {
        return Err(refusal(
            TERMINAL_STATE_CONNECTION_CONFIG_INVALID,
            "spec.auth.clientCertificate",
            format!(
                "KafkaCluster {namespace}/{name} names auth.clientCertificate with auth.mode \
                 {}; only mtls presents a client certificate, and one the connection would \
                 silently not present is refused instead",
                auth_mode_str(auth.mode)
            ),
        ));
    }

    let (resolved_auth, password, tls_ca, client_certificate) = match auth.mode {
        AuthMode::Plaintext => {
            if auth.tls {
                return Err(refusal(
                    TERMINAL_STATE_CONNECTION_CONFIG_INVALID,
                    "spec.auth.tls",
                    format!(
                        "KafkaCluster {namespace}/{name} sets auth.mode plaintext with auth.tls: \
                         true — TLS without SASL — which saved-connection contract \
                         {CONTRACT_VERSION} does not support. It is refused rather than dialled \
                         without TLS (releases before this one dialled it in the clear). Use \
                         auth.mode scramSha512 over TLS, mtls for a client certificate, or \
                         create an object with tls: false for a plaintext listener"
                    ),
                ));
            }
            if auth.tls_ca.is_some() {
                return Err(refusal(
                    TERMINAL_STATE_CONNECTION_CONFIG_INVALID,
                    "spec.auth.tlsCa",
                    format!(
                        "KafkaCluster {namespace}/{name} names auth.tlsCa with auth.tls: false; a \
                         CA verifies a TLS transport, so it requires auth.tls: true"
                    ),
                ));
            }
            (AuthSpec::Plaintext, None, None, None)
        }
        AuthMode::ScramSha512 | AuthMode::ScramSha256 | AuthMode::Plain => {
            let mode = auth_mode_str(auth.mode);
            // PROD-01.3: SASL/PLAIN only over TLS — the named reason
            // `PlainWithoutTls`, before anything else about the object is
            // judged, because it is the one shape that would send a password
            // in the clear.
            if auth.mode == AuthMode::Plain && !auth.tls {
                return Err(refusal(
                    TERMINAL_STATE_PLAIN_WITHOUT_TLS,
                    "spec.auth.tls",
                    format!(
                        "KafkaCluster {namespace}/{name}: {}",
                        logweir_core::connection::PlainWithoutTls
                    ),
                ));
            }
            let username = auth
                .username
                .clone()
                .filter(|u| !u.trim().is_empty())
                .ok_or_else(|| {
                    refusal(
                        TERMINAL_STATE_CREDENTIAL_NOT_RENDERABLE,
                        "spec.auth.username",
                        format!(
                            "KafkaCluster {namespace}/{name} names auth mode {mode} and no \
                             auth.username, so no run can name the identity it presents; \
                             recreate it with the SASL principal in auth.username"
                        ),
                    )
                })?;
            let secret = auth
                .secret_ref
                .as_ref()
                .filter(|s| !s.name.trim().is_empty())
                .ok_or_else(|| {
                    refusal(
                        TERMINAL_STATE_CREDENTIAL_NOT_RENDERABLE,
                        "spec.auth.secretRef.name",
                        format!(
                            "KafkaCluster {namespace}/{name} uses {mode} but has no \
                             non-empty auth.secretRef.name; reference a Secret in namespace \
                             {namespace} holding the password"
                        ),
                    )
                })?;
            check_object_name(&namespace, &name, "spec.auth.secretRef.name", &secret.name)?;
            let key = secret
                .password_key
                .clone()
                .unwrap_or_else(|| DEFAULT_PASSWORD_KEY.to_string());
            check_data_key(&namespace, &name, "spec.auth.secretRef.passwordKey", &key)?;
            let tls_ca = tls_ca_of(&namespace, &name, auth)?;
            let resolved = match auth.mode {
                AuthMode::ScramSha512 => AuthSpec::ScramSha512 {
                    username,
                    tls: auth.tls,
                },
                AuthMode::ScramSha256 => AuthSpec::ScramSha256 {
                    username,
                    tls: auth.tls,
                },
                _ => AuthSpec::Plain {
                    username,
                    tls: auth.tls,
                },
            };
            (
                resolved,
                Some(SecretKeyReference {
                    name: secret.name.clone(),
                    key,
                }),
                tls_ca,
                None,
            )
        }
        AuthMode::Mtls => {
            if !auth.tls {
                return Err(refusal(
                    TERMINAL_STATE_CONNECTION_CONFIG_INVALID,
                    "spec.auth.tls",
                    format!(
                        "KafkaCluster {namespace}/{name}: {}",
                        logweir_core::connection::MtlsWithoutTls
                    ),
                ));
            }
            let reference = auth
                .client_certificate
                .as_ref()
                .filter(|c| !c.name.trim().is_empty())
                .ok_or_else(|| {
                    refusal(
                        TERMINAL_STATE_CREDENTIAL_NOT_RENDERABLE,
                        "spec.auth.clientCertificate.name",
                        format!(
                            "KafkaCluster {namespace}/{name} uses mtls but names no \
                             auth.clientCertificate Secret; reference a Secret in namespace \
                             {namespace} holding the client certificate and its private key \
                             (`kubectl create secret tls`)"
                        ),
                    )
                })?;
            check_object_name(
                &namespace,
                &name,
                "spec.auth.clientCertificate.name",
                &reference.name,
            )?;
            let certificate_key = reference
                .certificate_key
                .clone()
                .unwrap_or_else(|| DEFAULT_CLIENT_CERTIFICATE_KEY.to_string());
            check_data_key(
                &namespace,
                &name,
                "spec.auth.clientCertificate.certificateKey",
                &certificate_key,
            )?;
            let private_key_key = reference
                .private_key_key
                .clone()
                .unwrap_or_else(|| DEFAULT_CLIENT_PRIVATE_KEY_KEY.to_string());
            check_data_key(
                &namespace,
                &name,
                "spec.auth.clientCertificate.privateKeyKey",
                &private_key_key,
            )?;
            if certificate_key == private_key_key {
                return Err(refusal(
                    TERMINAL_STATE_CONNECTION_REFERENCE_INVALID,
                    "spec.auth.clientCertificate",
                    format!(
                        "KafkaCluster {namespace}/{name} names the same data key \
                         {certificate_key:?} for the client certificate and its private key; \
                         they are two files, so they are two keys"
                    ),
                ));
            }
            let tls_ca = tls_ca_of(&namespace, &name, auth)?;
            (
                AuthSpec::Mtls { tls: true },
                None,
                tls_ca,
                Some(ClientCertificateReference {
                    name: reference.name.clone(),
                    certificate_key,
                    private_key_key,
                }),
            )
        }
    };

    Ok(ResolvedConnection {
        contract_version: CONTRACT_VERSION.to_string(),
        connection_use,
        uid: cluster.uid(),
        generation: cluster.metadata.generation,
        execution: ExecutionContext {
            namespace: namespace.clone(),
            service_account_name: connection_use.service_account_name().to_string(),
            side: connection_use.side(),
            // `job::build` sets `automountServiceAccountToken: false` on every
            // runner pod; stating it here is what lets a check Job assert it
            // is running the same isolation the real run does.
            automount_service_account_token: false,
        },
        principal: principal_of(&resolved_auth, client_certificate.as_ref()),
        bootstrap_sha256: bootstrap_digest(&bootstrap_servers),
        namespace,
        cluster_name: name,
        bootstrap_servers,
        auth: resolved_auth,
        password,
        tls_ca,
        client_certificate,
    })
}

/// The mode as the documents spell it — `AuthSpec::mode_str`'s twin for the
/// CRD enum, for messages.
#[must_use]
pub fn auth_mode_str(mode: AuthMode) -> &'static str {
    use logweir_core::connection as c;
    match mode {
        AuthMode::Plaintext => c::AUTH_MODE_PLAINTEXT,
        AuthMode::ScramSha512 => c::AUTH_MODE_SCRAM_SHA_512,
        AuthMode::ScramSha256 => c::AUTH_MODE_SCRAM_SHA_256,
        AuthMode::Plain => c::AUTH_MODE_PLAIN,
        AuthMode::Mtls => c::AUTH_MODE_MTLS,
    }
}

/// `tlsCa`, when named: it needs `tls: true`, exactly one source, and a legal
/// name and key. Shared by every TLS-capable mode.
fn tls_ca_of(
    namespace: &str,
    name: &str,
    auth: &AuthBlock,
) -> Result<Option<CaReference>, ConnectionRefusal> {
    match auth.tls_ca.as_ref() {
        None => Ok(None),
        Some(_) if !auth.tls => Err(refusal(
            TERMINAL_STATE_CONNECTION_CONFIG_INVALID,
            "spec.auth.tlsCa",
            format!(
                "KafkaCluster {namespace}/{name} names auth.tlsCa with auth.tls: false; a CA \
                 verifies a TLS transport, so it requires auth.tls: true"
            ),
        )),
        Some(source) => Ok(Some(ca_reference(
            namespace,
            name,
            source.secret_key_ref.as_ref(),
            source.config_map_key_ref.as_ref(),
        )?)),
    }
}

/// The Kafka principal an [`AuthSpec`] authenticates as — see
/// [`ResolvedConnection::principal`].
///
/// For `mtls` the Kafka principal is the client certificate's subject, which
/// the controller cannot know — it reads no Secret. The principal is then
/// `mtls:secret/<name>`: not an ACL string, but a stable name for where the
/// identity lives, so an attestation or a check result can still say which
/// identity it was about.
fn principal_of(
    auth: &AuthSpec,
    client_certificate: Option<&ClientCertificateReference>,
) -> String {
    match auth {
        // What a broker with no SASL records for an unauthenticated
        // connection, and what an adopter writes in an ACL for one.
        AuthSpec::Plaintext => "User:ANONYMOUS".to_string(),
        AuthSpec::ScramSha512 { username, .. }
        | AuthSpec::ScramSha256 { username, .. }
        | AuthSpec::Plain { username, .. } => format!("User:{username}"),
        AuthSpec::Mtls { .. } => format!(
            "mtls:secret/{}",
            client_certificate.map_or("<none>", |c| c.name.as_str())
        ),
    }
}

/// See [`ResolvedConnection::bootstrap_sha256`].
fn bootstrap_digest(servers: &[String]) -> String {
    let canonical = servers
        .iter()
        .map(|s| s.trim())
        .collect::<std::collections::BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>()
        .join("\n");
    logweir_core::ids::sha256_prefixed(canonical.as_bytes())
}

fn pronoun(count: usize) -> &'static str {
    if count == 1 {
        "it"
    } else {
        "them"
    }
}

/// Every undeclared key an object carried, as dotted paths, sorted.
fn unrecognized_paths(spec: &KafkaClusterSpec) -> Vec<String> {
    fn push(out: &mut Vec<String>, at: &str, fields: &UnrecognizedFields) {
        // THE KEY, NEVER THE VALUE. An undeclared key is the adopter's own text
        // and may sit beside anything; naming it is the whole diagnostic.
        out.extend(fields.keys().map(|k| format!("{at}.{k}")));
    }
    let mut out = Vec::new();
    push(&mut out, "spec", &spec.unrecognized_fields);
    push(&mut out, "spec.auth", &spec.auth.unrecognized_fields);
    if let Some(secret) = spec.auth.secret_ref.as_ref() {
        push(&mut out, "spec.auth.secretRef", &secret.unrecognized_fields);
    }
    if let Some(cert) = spec.auth.client_certificate.as_ref() {
        push(
            &mut out,
            "spec.auth.clientCertificate",
            &cert.unrecognized_fields,
        );
    }
    if let Some(ca) = spec.auth.tls_ca.as_ref() {
        push(&mut out, "spec.auth.tlsCa", &ca.unrecognized_fields);
        for (label, reference) in [
            ("secretKeyRef", ca.secret_key_ref.as_ref()),
            ("configMapKeyRef", ca.config_map_key_ref.as_ref()),
        ] {
            if let Some(reference) = reference {
                push(
                    &mut out,
                    &format!("spec.auth.tlsCa.{label}"),
                    &reference.unrecognized_fields,
                );
            }
        }
    }
    out.sort();
    out
}

fn checked_bootstrap_servers(
    namespace: &str,
    name: &str,
    servers: &[String],
) -> Result<Vec<String>, ConnectionRefusal> {
    if servers.is_empty() {
        return Err(refusal(
            TERMINAL_STATE_CONNECTION_CONFIG_INVALID,
            "spec.bootstrapServers",
            format!(
                "KafkaCluster {namespace}/{name} names no bootstrap server; give at least one \
                 host:port"
            ),
        ));
    }
    for (i, server) in servers.iter().enumerate() {
        if server.trim().is_empty() || server.contains(',') || server.contains(char::is_whitespace)
        {
            return Err(refusal(
                TERMINAL_STATE_CONNECTION_CONFIG_INVALID,
                &format!("spec.bootstrapServers[{i}]"),
                format!(
                    "KafkaCluster {namespace}/{name} has bootstrap entry {i} ({server:?}), which \
                     is empty or carries a comma or whitespace; each entry is one host:port, and \
                     such an entry would be dialled differently by the probe (one \
                     comma-separated argument) and by a backup plan (a list)"
                ),
            ));
        }
    }
    Ok(servers.to_vec())
}

fn ca_reference(
    namespace: &str,
    name: &str,
    secret: Option<&ObjectKeyRef>,
    config_map: Option<&ObjectKeyRef>,
) -> Result<CaReference, ConnectionRefusal> {
    let (kind, reference, field) = match (secret, config_map) {
        (Some(s), None) => (CaSourceKind::Secret, s, "spec.auth.tlsCa.secretKeyRef"),
        (None, Some(c)) => (
            CaSourceKind::ConfigMap,
            c,
            "spec.auth.tlsCa.configMapKeyRef",
        ),
        (Some(_), Some(_)) | (None, None) => {
            return Err(refusal(
                TERMINAL_STATE_CONNECTION_CONFIG_INVALID,
                "spec.auth.tlsCa",
                format!(
                    "KafkaCluster {namespace}/{name} must name exactly one of \
                     auth.tlsCa.secretKeyRef or auth.tlsCa.configMapKeyRef"
                ),
            ))
        }
    };
    check_object_name(namespace, name, &format!("{field}.name"), &reference.name)?;
    check_data_key(namespace, name, &format!("{field}.key"), &reference.key)?;
    Ok(CaReference {
        kind,
        name: reference.name.clone(),
        key: reference.key.clone(),
    })
}

/// Whether `value` is a DNS-1123 subdomain — the name rule for Secrets and
/// ConfigMaps. Hand-written rather than a regex dependency: lowercase
/// alphanumerics, `-` and `.`, at most 253 characters, every dot-separated
/// label non-empty and starting and ending alphanumeric.
#[must_use]
pub fn is_dns1123_subdomain(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 253
        && value.split('.').all(|label| {
            let bytes = label.as_bytes();
            !bytes.is_empty()
                && bytes
                    .iter()
                    .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || *b == b'-')
                && bytes.first().is_some_and(u8::is_ascii_alphanumeric)
                && bytes.last().is_some_and(u8::is_ascii_alphanumeric)
        })
}

/// Whether `value` is a legal Secret/ConfigMap data key — the API server's
/// rule: `[-._a-zA-Z0-9]+`, at most 253 characters, and not `.`, `..` or
/// anything starting with `..`.
#[must_use]
pub fn is_data_key(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 253
        && value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'.' | b'_'))
        && value != "."
        && !value.starts_with("..")
}

fn check_object_name(
    namespace: &str,
    name: &str,
    field: &str,
    value: &str,
) -> Result<(), ConnectionRefusal> {
    if is_dns1123_subdomain(value) {
        return Ok(());
    }
    Err(refusal(
        TERMINAL_STATE_CONNECTION_REFERENCE_INVALID,
        field,
        format!(
            "KafkaCluster {namespace}/{name} has {field} {value:?}, which is not a DNS-1123 \
             subdomain, so no object in namespace {namespace} can carry that name and the kubelet \
             could never resolve it"
        ),
    ))
}

fn check_data_key(
    namespace: &str,
    name: &str,
    field: &str,
    value: &str,
) -> Result<(), ConnectionRefusal> {
    if is_data_key(value) {
        return Ok(());
    }
    Err(refusal(
        TERMINAL_STATE_CONNECTION_REFERENCE_INVALID,
        field,
        format!(
            "KafkaCluster {namespace}/{name} has {field} {value:?}, which is not a legal data key \
             ([-._a-zA-Z0-9]+, at most 253 characters, not `.` or `..`)"
        ),
    ))
}

impl ResolvedConnection {
    /// The credential BINDING this connection's credential Secret must carry
    /// under `logweir-binding` (PROD-01.3 security follow-up):
    /// [`logweir_core::connection::credential_binding`] over this object's UID
    /// and its endpoint — the bootstrap set, the mode, the username, the TLS
    /// switch and the CA reference.
    ///
    /// `None` without a UID (a resolution of a spec, never a stored object);
    /// [`ResolvedConnection::project`] then projects a value no Secret can
    /// carry, so a Job built from such a resolution refuses its credential
    /// rather than skipping the check.
    #[must_use]
    pub fn credential_binding(&self) -> Option<String> {
        let uid = self.uid.as_deref().filter(|u| !u.is_empty())?;
        let ca = self.tls_ca.as_ref().map(|ca| {
            format!(
                "{}/{}/{}",
                match ca.kind {
                    CaSourceKind::Secret => "secret",
                    CaSourceKind::ConfigMap => "configMap",
                },
                ca.name,
                ca.key
            )
        });
        Some(logweir_core::connection::credential_binding(
            uid,
            &self.bootstrap_servers,
            &self.auth,
            ca.as_deref(),
        ))
    }

    /// The Secret the connection's CREDENTIAL is in — the SASL password's or
    /// the `mtls` client certificate's — when it has one.
    #[must_use]
    pub fn credential_secret(&self) -> Option<&str> {
        self.password
            .as_ref()
            .map(|p| p.name.as_str())
            .or(self.client_certificate.as_ref().map(|c| c.name.as_str()))
    }

    /// The environment and mounts this connection contributes to its Job.
    /// References only: see this module's header.
    ///
    /// **The binding pair (PROD-01.3 security follow-up).** A connection with
    /// a credential also projects the EXPECTED binding as a literal and the
    /// Secret's own `logweir-binding` key as an OPTIONAL `secretKeyRef`, and
    /// the runner refuses the credential (`CredentialBindingMismatch`) unless
    /// the two are equal — so a `KafkaCluster` that names another connection's
    /// Secret cannot make the runner present that credential anywhere.
    ///
    /// THE SIDE IS NOT AN ARGUMENT. It is `execution.side`, fixed by the
    /// [`ConnectionUse`] the caller resolved with, so a restore cannot be
    /// handed the source side's variables by a caller that passed the wrong
    /// constant.
    #[must_use]
    pub fn project(&self) -> ConnectionProjection {
        let side = self.execution.side;
        let mut projection = ConnectionProjection::default();
        if let Some(password) = self.password.as_ref() {
            projection.env_from_secret.push(EnvFromSecret {
                name: side.password_env().to_string(),
                secret_name: password.name.clone(),
                optional: false,
                key: password.key.clone(),
            });
        }
        if let Some(secret) = self.credential_secret() {
            projection.env_literal.push((
                side.credential_binding_expected_env().to_string(),
                // FAIL CLOSED: no UID, no binding a Secret could carry.
                self.credential_binding()
                    .unwrap_or_else(|| "unbound:no-uid".to_string()),
            ));
            projection.env_from_secret.push(EnvFromSecret {
                name: side.credential_binding_env().to_string(),
                secret_name: secret.to_string(),
                key: logweir_core::connection::CREDENTIAL_BINDING_KEY.to_string(),
                optional: true,
            });
        }
        if let Some(ca) = self.tls_ca.as_ref() {
            projection
                .env_literal
                .push((side.tls_ca_file_env().to_string(), side.ca_file_path()));
            let items = vec![(ca.key.clone(), CA_FILE_NAME.to_string())];
            match ca.kind {
                CaSourceKind::Secret => projection.secret_mounts.push(SecretMount {
                    volume: side.ca_volume().to_string(),
                    secret_name: ca.name.clone(),
                    mount_path: side.ca_mount_path().to_string(),
                    items,
                }),
                CaSourceKind::ConfigMap => projection.config_map_mounts.push(ConfigMapMount {
                    volume: side.ca_volume().to_string(),
                    config_map_name: ca.name.clone(),
                    mount_path: side.ca_mount_path().to_string(),
                    items,
                }),
            }
        }
        // PROD-01.3: the `mtls` pair, from ONE Secret, as two files in one
        // read-only volume at the runner's `0440` Secret mode. The two
        // variables carry PATHS; the kubelet resolves the Secret.
        if let Some(cert) = self.client_certificate.as_ref() {
            projection.env_literal.push((
                side.tls_cert_file_env().to_string(),
                side.client_cert_file_path(),
            ));
            projection.env_literal.push((
                side.tls_key_file_env().to_string(),
                side.client_key_file_path(),
            ));
            projection.secret_mounts.push(SecretMount {
                volume: side.client_cert_volume().to_string(),
                secret_name: cert.name.clone(),
                mount_path: side.client_cert_mount_path().to_string(),
                items: vec![
                    (
                        cert.certificate_key.clone(),
                        CLIENT_CERT_FILE_NAME.to_string(),
                    ),
                    (
                        cert.private_key_key.clone(),
                        CLIENT_KEY_FILE_NAME.to_string(),
                    ),
                ],
            });
        }
        projection
    }

    /// The probe's connection flags — interface **I14**'s `--bootstrap`,
    /// `--auth-mode`, `--username` and `--tls`, in that order.
    ///
    /// `--tls` is emitted only when the transport is TLS: a boolean flag has no
    /// false form. The CA is not a flag; it reaches the probe through
    /// [`Side::tls_ca_file_env`] like every other runner.
    #[must_use]
    pub fn probe_args(&self) -> Vec<String> {
        let mut argv = vec![
            "--bootstrap".to_string(),
            self.bootstrap_servers.join(","),
            "--auth-mode".to_string(),
            self.auth.mode_str().to_string(),
        ];
        if let Some(username) = self.auth.username() {
            argv.push("--username".to_string());
            argv.push(username.to_string());
        }
        if self.tls() {
            argv.push("--tls".to_string());
        }
        argv
    }

    /// Whether the transport is TLS.
    #[must_use]
    pub fn tls(&self) -> bool {
        self.auth.tls()
    }

    /// Refuse to project this connection into a Job in another namespace.
    ///
    /// Every reference is resolved by the kubelet in the JOB's namespace, so a
    /// Job elsewhere would silently read a different Secret of the same name.
    /// The three controllers always build Jobs in the namespace they read the
    /// `KafkaCluster` from; this makes that a checked property instead of a
    /// coincidence of two lookups.
    ///
    /// # Errors
    ///
    /// [`TERMINAL_STATE_CONNECTION_REFERENCE_INVALID`] when they differ.
    pub fn check_job_namespace(&self, job_namespace: &str) -> Result<(), ConnectionRefusal> {
        if job_namespace == self.namespace {
            return Ok(());
        }
        Err(refusal(
            TERMINAL_STATE_CONNECTION_REFERENCE_INVALID,
            "metadata.namespace",
            format!(
                "the KafkaCluster {}/{} cannot be used by a Job in namespace {job_namespace}: \
                 connection references are resolved only in their own namespace",
                self.namespace, self.cluster_name
            ),
        ))
    }

    /// Refuse a restore whose approved plan names a different target than this
    /// connection.
    ///
    /// The runner dials the PLAN's `target.bootstrap_servers` with the plan's
    /// `target.auth`, and authenticates with THIS connection's password and CA.
    /// So the two must describe one connection: a different address is a
    /// credential sent somewhere the saved connection does not name, a
    /// different username is a password presented for another principal, and
    /// `tls: false` against a TLS connection is a downgrade. Bootstrap servers
    /// are compared as a set (order and duplicates are not a different cluster);
    /// mode, username and TLS exactly.
    ///
    /// Reads only `target.bootstrap_servers` and `target.auth`, with the
    /// runner's own `AuthSpec` grammar (absent `auth` is `plaintext`). A plan
    /// with no readable `target` block is refused here too: the runner could
    /// not run it either, and a check that passed on a block it never read
    /// would be the wrong kind of green.
    ///
    /// # Errors
    ///
    /// [`TERMINAL_STATE_CONNECTION_PLAN_MISMATCH`].
    pub fn check_restore_plan(&self, plan_bytes: &str) -> Result<(), ConnectionRefusal> {
        #[derive(Deserialize)]
        struct PlanView {
            target: TargetView,
        }
        #[derive(Deserialize)]
        struct TargetView {
            #[serde(default)]
            bootstrap_servers: Vec<String>,
            #[serde(default)]
            auth: AuthSpec,
        }
        let view: PlanView = serde_yaml::from_str(plan_bytes).map_err(|e| {
            refusal(
                TERMINAL_STATE_CONNECTION_PLAN_MISMATCH,
                "spec.planBytes",
                format!(
                    "spec.planBytes has no readable target block ({e}), so it cannot be compared \
                     with the saved connection KafkaCluster {}/{}",
                    self.namespace, self.cluster_name
                ),
            )
        })?;
        let as_set = |servers: &[String]| {
            servers
                .iter()
                .map(|s| s.trim().to_string())
                .collect::<std::collections::BTreeSet<_>>()
        };
        if as_set(&view.target.bootstrap_servers) != as_set(&self.bootstrap_servers) {
            return Err(refusal(
                TERMINAL_STATE_CONNECTION_PLAN_MISMATCH,
                "spec.planBytes.target.bootstrap_servers",
                format!(
                    "the approved plan dials {:?} but spec.target.clusterRef resolves to \
                     KafkaCluster {}/{} at {:?}; the runner would present that connection's \
                     credential to the plan's address. Build a plan from the saved connection \
                     and approve it",
                    view.target.bootstrap_servers,
                    self.namespace,
                    self.cluster_name,
                    self.bootstrap_servers
                ),
            ));
        }
        if view.target.auth != self.auth {
            return Err(refusal(
                TERMINAL_STATE_CONNECTION_PLAN_MISMATCH,
                "spec.planBytes.target.auth",
                format!(
                    "the approved plan's target.auth is {} but KafkaCluster {}/{} resolves to {}; \
                     the runner authenticates with the connection's credential and CA, so the \
                     plan must name the same mode, username and TLS. Build a plan from the saved \
                     connection and approve it",
                    describe_auth(&view.target.auth),
                    self.namespace,
                    self.cluster_name,
                    describe_auth(&self.auth)
                ),
            ));
        }
        Ok(())
    }
}

/// Mode, username and TLS as prose — public settings only.
fn describe_auth(auth: &AuthSpec) -> String {
    let transport = if auth.tls() {
        "over TLS"
    } else {
        "without TLS"
    };
    match auth {
        AuthSpec::Plaintext => "plaintext (no TLS)".to_string(),
        AuthSpec::Mtls { .. } => format!("mtls {transport}"),
        _ => format!(
            "{} as {:?} {transport}",
            auth.mode_str(),
            auth.username().unwrap_or_default()
        ),
    }
}
