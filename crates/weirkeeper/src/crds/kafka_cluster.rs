//! `KafkaCluster` — a cluster connection as an object, not a Secret
//! convention.
//!
//! The Secret carries only the password; this resource carries the bootstrap
//! servers, the auth mode, the **username**, the TLS flag, the role and —
//! after the controller's probe — the *observed* `clusterId`. Phase 0 reads
//! the cluster id from the broker and never from a spec
//! (`crates/logweir/src/drill/phase0_admit.rs`), so `status.clusterId` is an
//! observation and `spec` says nothing about it.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use super::{Condition, LocalRef, Time};

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
    /// No SASL. `security.protocol` is `PLAINTEXT` or `SSL` depending on
    /// `tls`.
    Plaintext,
    /// SASL/SCRAM-SHA-512. `security.protocol` is `SASL_PLAINTEXT` or
    /// `SASL_SSL` depending on `tls`.
    ScramSha512,
}

/// How Logweir authenticates to this cluster.
///
/// `tls` IS SEPARATE FROM `mode` ON PURPOSE. SASL/SCRAM over PLAINTEXT and
/// SASL/SCRAM over SSL are two different `security.protocol` values for one
/// mechanism, and an adopter with a private CA has to configure BOTH trust
/// stores (Global Constraint 29): the engine falls back to bundled roots
/// unless its own `ssl_ca_location` is set, while Logweir's own client uses
/// the image's `ca-certificates`.
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
    /// The SASL principal. Required in practice when `mode` is
    /// `scramSha512`; it is what `planBytes` binds, so changing it after an
    /// approval invalidates that approval's plan hash.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub username: Option<String>,
    /// A Secret in this namespace holding the SASL password. Logweir never
    /// reads its value into a status field, a log line or a rendered
    /// document.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub secret_ref: Option<LocalRef>,
    /// Whether the transport is TLS. Independent of `mode` — see the type's
    /// own note.
    #[serde(default)]
    pub tls: bool,
}

/// `KafkaCluster.spec`.
#[derive(kube::CustomResource, Deserialize, Serialize, Clone, Debug, JsonSchema)]
#[kube(
    group = "logweir.dev",
    version = "v1alpha1",
    kind = "KafkaCluster",
    doc = "A Kafka cluster Logweir connects to: bootstrap servers, auth mode and username, role, and the marker topic that proves a scratch target. The password lives in a Secret; `status.clusterId` is read from the broker and never from this spec. `spec` is immutable.",
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
