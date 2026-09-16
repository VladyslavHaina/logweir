//! `KafkaCluster` — a cluster connection as an object, not a Secret
//! convention.
//!
//! The Secret carries only the password; this resource carries the bootstrap
//! servers, the auth mode, the **username**, the TLS flag, the role and —
//! after the controller's probe — the *observed* `clusterId`. Phase 0 reads
//! the cluster id from the broker and never from a spec
//! (`crates/logweir/src/drill/phase0_admit.rs`), so `status.clusterId` is an
//! observation and `spec` says nothing about it.
//!
//! # The saved-connection contract, version 1 (PLAT-07.1)
//!
//! Every field this file declares is resolved by exactly one function,
//! [`crate::connection::resolve`], and every runner Job that dials this
//! cluster — probe, backup, restore — is built from that one resolution. The
//! contract is additive over the fields that shipped before it: an object that
//! names none of `auth.secretRef.passwordKey` or `auth.tlsCa` resolves exactly
//! as it did (the fixtures under `tests/fixtures/connection/legacy` are the
//! measured proof).
//!
//! THE TLS SWITCH STAYS `auth.tls`. A CA reference is an input that can only
//! ADD trust material to a transport that is already TLS; it never turns TLS
//! on. That split is what keeps a rollback honest: an older controller that
//! does not know `auth.tlsCa` still reads `auth.tls` and still dials TLS, so
//! the field it ignores can only make verification fail (a private CA is not
//! in the image's public roots), never downgrade the transport.
//!
//! # Unrecognised fields are kept, so they can be refused
//!
//! The API server prunes what the INSTALLED CRD does not declare, so a field
//! reaches this controller undeclared only when the CRD is newer than the
//! controller — a rollback, or a CRD applied ahead of its controller. Each
//! connection struct therefore captures undeclared keys
//! (`unrecognized_fields`, never published in the schema) and
//! [`crate::connection::resolve`] refuses an object carrying one, naming it,
//! instead of silently resolving the connection without a setting its author
//! asked for.

use std::collections::BTreeMap;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use super::{Condition, Time};

/// The data key a credential Secret carries the SASL password under when
/// `auth.secretRef.passwordKey` is absent — the only key earlier releases
/// projected, which is what keeps an object written before the field existed
/// resolving to the same `secretKeyRef`.
pub const DEFAULT_PASSWORD_KEY: &str = "password";

/// The pattern a Secret or ConfigMap data key must match — the API server's own
/// rule for `data` keys (`[-._a-zA-Z0-9]+`), stated in the schema of the new
/// key fields so a key the kubelet could never project is refused at admission
/// rather than as a pod stuck in `CreateContainerConfigError`.
pub const DATA_KEY_PATTERN: &str = r"^[-._a-zA-Z0-9]+$";

/// A DNS-1123 subdomain — what a Secret or ConfigMap `metadata.name` must be.
pub const OBJECT_NAME_PATTERN: &str =
    r"^[a-z0-9]([-a-z0-9]*[a-z0-9])?(\.[a-z0-9]([-a-z0-9]*[a-z0-9])?)*$";

/// Keys an object carried that this controller's schema does not declare.
///
/// Never published in the CRD (`#[schemars(skip)]`), so the installed schema is
/// unchanged by it; see this module's header for why it exists.
pub type UnrecognizedFields = BTreeMap<String, serde_json::Value>;

/// `auth.mode`'s enum, fixed byte for byte at this task.
///
/// THE SPELLINGS ARE THE CONTRACT. They are exactly `["plaintext",
/// "scramSha512"]`, in that order, and Task 6's late-binding agreement test
/// `the_crd_auth_mode_enum_and_auth_spec_agree` (which lives in
/// `tests/crd_shape.rs` and is owned by that task, not this one) asserts BYTE
/// equality between this enum and `logweir_core::spec::AuthSpec`'s serde
/// spellings. This task consumes nothing from Task 6 — the Rust `AuthSpec`
/// lands in a later slot and must match what is here.
///
/// NO THIRD MODE. `mtls`, `gssapi`, `oauthbearer` and `scramSha256` are not in
/// tag 1; a `KafkaCluster` naming one is refused by the CRD schema rather than
/// by a controller branch, and `RenderError::UnsupportedAuthMode` (Task 3) is
/// the CLI half of the same refusal.
#[derive(Deserialize, Serialize, Clone, Copy, Debug, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
#[schemars(rename_all = "camelCase")]
pub enum AuthMode {
    /// No SASL. `security.protocol` is `PLAINTEXT`. Connection contract v1
    /// refuses this mode with `tls: true` (TLS without SASL, `SSL`): the signed
    /// restore plan grammar has no TLS field for it, and earlier releases
    /// dialled such an object without TLS.
    Plaintext,
    /// SASL/SCRAM-SHA-512. `security.protocol` is `SASL_PLAINTEXT` or
    /// `SASL_SSL` depending on `tls`.
    ScramSha512,
}

/// How Logweir authenticates to this cluster, and how the transport is
/// protected.
///
/// `tls` IS SEPARATE FROM `mode` ON PURPOSE. SASL/SCRAM over PLAINTEXT and
/// SASL/SCRAM over SSL are two different `security.protocol` values for one
/// mechanism. The runner has two TLS clients with two trust stores (Global
/// Constraint 29): the engine falls back to bundled roots unless its own
/// `ssl_ca_location` is set, and Logweir's own client uses the image's
/// `ca-certificates`. `tlsCa` configures both from one reference.
///
/// NO PASSWORD FIELD, AT ANY MODE. The credential reaches the Job through
/// `secretRef` and the environment, never through this object and never
/// through a rendered document. `planBytes` binds `auth.username`, not the
/// password, which is why the Secret named below is mutable and covered by no
/// hash — spec §4's "the approval binds the identity, not just the address".
#[derive(Deserialize, Serialize, Clone, Debug, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct AuthBlock {
    /// The SASL mechanism, or `plaintext` for none.
    pub mode: AuthMode,
    /// The SASL principal. Required when `mode` is `scramSha512`; it is what
    /// `planBytes` binds, so changing it after an approval invalidates that
    /// approval's plan hash. Never read from a Secret.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub username: Option<String>,
    /// A Secret in this namespace holding the SASL password, and the key it is
    /// under. Required when `mode` is `scramSha512`. Logweir never reads its
    /// value into a status field, a log line or a rendered document.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub secret_ref: Option<CredentialSecretRef>,
    /// Whether the transport is TLS — the only switch that turns TLS on.
    /// Independent of `mode`; in connection contract v1 TLS is supported with
    /// `scramSha512` (SASL_SSL), and `plaintext` with `tls: true` is refused
    /// rather than dialled without TLS.
    #[serde(default)]
    pub tls: bool,
    /// The certificate authority that signs the brokers' certificates, when it
    /// is not one the runner image already trusts: exactly one key of a Secret
    /// or a ConfigMap in this namespace. It replaces the default trust store
    /// for this connection in BOTH clients (the engine's `ssl_ca_location` and
    /// librdkafka's `ssl.ca.location`). Requires `tls: true`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tls_ca: Option<TlsCaSource>,
    #[serde(flatten)]
    #[schemars(skip)]
    pub unrecognized_fields: UnrecognizedFields,
}

/// The credential Secret a SASL connection names.
///
/// SAME NAMESPACE BY CONSTRUCTION: there is no `namespace` field, because a
/// cross-namespace reference is a privilege-escalation surface (the referrer's
/// RBAC does not cover the referent's namespace).
#[derive(Deserialize, Serialize, Clone, Debug, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct CredentialSecretRef {
    /// The Secret's `metadata.name`, in this namespace.
    pub name: String,
    /// The data key holding the password. Absent means `password`, the key
    /// every earlier release projected.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(regex(path = "DATA_KEY_PATTERN"), length(min = 1, max = 253))]
    pub password_key: Option<String>,
    #[serde(flatten)]
    #[schemars(skip)]
    pub unrecognized_fields: UnrecognizedFields,
}

/// Where a connection's CA certificate is: exactly one of the two sources.
#[derive(Deserialize, Serialize, Clone, Debug, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct TlsCaSource {
    /// A key of a Secret in this namespace holding PEM CA certificate(s).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub secret_key_ref: Option<ObjectKeyRef>,
    /// A key of a ConfigMap in this namespace holding PEM CA certificate(s). A
    /// CA certificate is public, so a ConfigMap is an ordinary home for it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub config_map_key_ref: Option<ObjectKeyRef>,
    #[serde(flatten)]
    #[schemars(skip)]
    pub unrecognized_fields: UnrecognizedFields,
}

/// One data key of a Secret or ConfigMap in THIS namespace.
#[derive(Deserialize, Serialize, Clone, Debug, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ObjectKeyRef {
    /// The object's `metadata.name`, in this namespace.
    #[schemars(regex(path = "OBJECT_NAME_PATTERN"), length(min = 1, max = 253))]
    pub name: String,
    /// The data key.
    #[schemars(regex(path = "DATA_KEY_PATTERN"), length(min = 1, max = 253))]
    pub key: String,
    #[serde(flatten)]
    #[schemars(skip)]
    pub unrecognized_fields: UnrecognizedFields,
}

/// The CEL rule on `spec.auth`: a CA reference requires TLS.
///
/// NOT A TRANSITION RULE — it never names `oldSelf` — so it is evaluated on
/// every write the way any schema constraint is, and it is vacuously true for
/// every object that names no `tlsCa`, which is every object written before
/// the field existed. [`crate::connection::resolve`] refuses the same shape
/// independently, for an object admitted by a CRD that did not carry the rule.
pub const TLS_CA_REQUIRES_TLS_RULE: &str = "!has(self.tlsCa) || self.tls";
/// The message paired with [`TLS_CA_REQUIRES_TLS_RULE`].
pub const TLS_CA_REQUIRES_TLS_MESSAGE: &str =
    "auth.tlsCa names a CA for a TLS transport, so it requires auth.tls: true";

/// The CEL rule on `spec.auth.tlsCa`: exactly one source.
pub const TLS_CA_EXACTLY_ONE_SOURCE_RULE: &str =
    "has(self.secretKeyRef) != has(self.configMapKeyRef)";
/// The message paired with [`TLS_CA_EXACTLY_ONE_SOURCE_RULE`].
pub const TLS_CA_EXACTLY_ONE_SOURCE_MESSAGE: &str =
    "auth.tlsCa names exactly one of secretKeyRef or configMapKeyRef";

/// The connection contract's CEL rules, as `(schema path under .spec, rule,
/// message)`. Injected by `crds::render_all`, the only caller.
pub const CONNECTION_RULES: [(&[&str], &str, &str); 2] = [
    (
        &["auth"],
        TLS_CA_REQUIRES_TLS_RULE,
        TLS_CA_REQUIRES_TLS_MESSAGE,
    ),
    (
        &["auth", "tlsCa"],
        TLS_CA_EXACTLY_ONE_SOURCE_RULE,
        TLS_CA_EXACTLY_ONE_SOURCE_MESSAGE,
    ),
];

/// `KafkaCluster.spec`.
#[derive(kube::CustomResource, Deserialize, Serialize, Clone, Debug, JsonSchema)]
#[kube(
    group = "logweir.dev",
    version = "v1alpha1",
    kind = "KafkaCluster",
    doc = "A Kafka cluster Logweir connects to (saved-connection contract v1): bootstrap servers, auth mode and username, the TLS switch and an optional private CA reference, role, and the marker topic that proves a scratch target. The password lives in a Secret and the CA in a Secret or ConfigMap, both in this namespace and both referenced, never copied; `status.clusterId` is read from the broker and never from this spec. `spec` is immutable.",
    plural = "kafkaclusters",
    singular = "kafkacluster",
    namespaced,
    status = "KafkaClusterStatus",
    printcolumn = r#"{"name":"ROLE","type":"string","jsonPath":".spec.role","description":"source or target"}"#,
    printcolumn = r#"{"name":"REACHABLE","type":"string","jsonPath":".status.reachable","description":"the controller's last probe"}"#,
    printcolumn = r#"{"name":"CLUSTER-ID","type":"string","jsonPath":".status.clusterId","description":"read from the broker, never from the spec"}"#,
    printcolumn = r#"{"name":"AGE","type":"date","jsonPath":".metadata.creationTimestamp"}"#
)]
#[serde(rename_all = "camelCase")]
pub struct KafkaClusterSpec {
    /// The broker bootstrap addresses, `host:port`.
    pub bootstrap_servers: Vec<String>,
    /// How Logweir authenticates here.
    pub auth: AuthBlock,
    /// What this cluster is to Logweir — `source` or `target`. A free-form
    /// string rather than an enum: the value is a label the adopter picks and
    /// the controller reports, and `allowedClusterIds` on the cluster-scoped
    /// `TrustRoster` is what actually authorises a target, never a role
    /// written next to the address it authorises.
    pub role: String,
    /// The marker topic that proves a target is a scratch cluster. v0.1 proves
    /// scratch-ness with a marker topic because a guard needing a kubeconfig
    /// cannot run in the unit suite; absent here, no marker proof is offered
    /// and a `mode: scratch` restore against this cluster is refused.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub marker_topic: Option<String>,
    #[serde(flatten)]
    #[schemars(skip)]
    pub unrecognized_fields: UnrecognizedFields,
}

/// `KafkaCluster.status` — entirely observations.
#[derive(Deserialize, Serialize, Clone, Debug, Default, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct KafkaClusterStatus {
    /// Whether the controller's last probe reached a broker.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reachable: Option<bool>,
    /// The cluster id READ FROM THE BROKER. Never copied from a spec, and
    /// re-asserted `!= target` before a backup runs (Global Constraint 18's
    /// fourth rail).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cluster_id: Option<String>,
    /// When the probe above was performed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub observed_at: Option<Time>,
    /// **Why `reachable` says what it says** — `Reachable`,
    /// `ProbeReportedUnreachable`, `ProbeOutputUnreadable`, `ProbeRunning`,
    /// `NoExitCode`, `PodUnschedulable`, `DisruptedMidDrill` or `NameTooLong`:
    /// the `Reachable` condition's own `reason`, promoted to a scalar. Added by
    /// Task 15c.
    ///
    /// WHY A SCALAR BESIDE THE CONDITION. `reachable` is a tri-state in
    /// practice — `true`, `false`, and ABSENT — and the absent case has five
    /// causes an operator has to tell apart: the probe pod printed no parseable
    /// contract line, the Job finished with no exit code at all, the pod never
    /// scheduled, the node went away mid-probe, or a probe is simply in flight.
    /// With no scalar all of those read identically through `-o jsonpath` and
    /// through any `custom-columns` view, and the specific state exists only
    /// inside `status.conditions` — which is review finding **M2** on the
    /// `Restore` path, reached here from the other direction.
    ///
    /// **`status` IS STRUCTURAL AND PRUNES WHAT IT DOES NOT DECLARE**, so this
    /// is not a field a controller could have written without the CRD saying
    /// so: a merge patch carrying `status.reason` against the previous schema
    /// was dropped by the API server silently.
    ///
    /// It is not a second vocabulary: it is always **verbatim** the `reason` of
    /// the one `Reachable` condition the same patch writes, which makes it
    /// CamelCase everywhere by errata **E5b**.
    //
    // NO NEW PRINTER COLUMN, and this half is a `//` comment because it is
    // build rationale rather than something `kubectl explain` should print.
    // `KafkaCluster`'s four columns (ROLE/REACHABLE/CLUSTER-ID/AGE) are Task
    // 15b's pinned interface, asserted against a deliberately non-derived
    // literal in `tests/crd_shape.rs` — mutant MA of that task dropped a column
    // and 16 of 16 tests passed — so repointing or extending them is that
    // task's decision, not this one's. The scalar is reachable through
    // `-o jsonpath={.status.reason}` and through `custom-columns` meanwhile.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    /// `Reachable`, and whatever else the controller reports.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub conditions: Option<Vec<Condition>>,
}
