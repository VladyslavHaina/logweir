//! `TrustRoster` — the one cluster-scoped kind, carrying key material for
//! **both** lists.
//!
//! # Why `signingKeys[]` and not `signingKeyIds[]`
//!
//! Interface **I17**, and spec amendment 3. A `signingKeyIds: Vec<String>`
//! shape — ids with no key material — is UNBUILDABLE: Task 24's
//! `verify_evidence` resolves a runner's signing key from this roster and
//! calls `verify_detached`, and an id alone gives it nothing to verify
//! against. Every verification would return `NotAttempted`,
//! `status.evidence.verification.result` could never be `Valid`, and Phase B's
//! exit criterion would be unreachable. **A `signingKeyIds: [string]` shape is
//! a CRD schema error, not a degraded mode.**
//!
//! [`KeyEntry`] therefore carries the same four fields for both lists, with
//! `keyId` and `spkiPem` both REQUIRED. This is discovered at Task 24 and
//! fixed here, at slot 5, because after Task 21 renders `logweir.yaml`, after
//! Task 23 pins both images, and with `.spec` sealed by CEL, the retrofit
//! reopens six landed tasks in a strictly serial chain.
//!
//! # Two lists, two principals
//!
//! `approverKeys` **authorises**; `signingKeys` **attests**. They stay
//! separate: a runner that could also authorise its own restore would make the
//! approval a formality. An empty `signingKeys` is `NotAttempted` naming
//! itself — *the TrustRoster lists no signing key material; add the runner's
//! public key to spec.signingKeys* — never a silent `Invalid`.
//!
//! # Why `allowedClusterIds` lives here
//!
//! Never on a plan. Today's CLI rule is "a SEPARATE file argument, never read
//! from the drill spec, so an edited spec cannot widen its own allowlist"
//! (`crates/logweir-core/src/spec.rs`); in Kubernetes the stronger form of
//! "separate file" is "different RBAC subject", which is what cluster scope
//! buys.
//!
//! # No private key material, here or anywhere
//!
//! `spkiPem` is a **public** key in SubjectPublicKeyInfo PEM form. Nothing in
//! this group has a field a private key could go in, and
//! `tests/linkage.rs::the_controller_never_reads_a_secret` keeps the
//! controller's source away from the Secret API.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use super::{Condition, Time};

/// One key on the roster. The same four fields for approvers and for signers.
#[derive(Deserialize, Serialize, Clone, Debug, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct KeyEntry {
    /// The key id an evidence sidecar names, and the id
    /// `status.matchedKeyId` reports. **Required.**
    pub key_id: String,
    /// The **public** key, SubjectPublicKeyInfo in PEM form. **Required** —
    /// this is the field whose absence would make every verification
    /// `NotAttempted`. Never a private key.
    pub spki_pem: String,
    /// Who or what this key belongs to, for display.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub subject: Option<String>,
    /// When this key stops being accepted. A key past `notAfter` is reported
    /// in `status.expiredKeyIds` and does not verify.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub not_after: Option<Time>,
}

/// `TrustRoster.spec`. **Cluster-scoped** — see the module header.
#[derive(kube::CustomResource, Deserialize, Serialize, Clone, Debug, JsonSchema)]
#[kube(
    group = "logweir.dev",
    version = "v1alpha1",
    kind = "TrustRoster",
    doc = "DEPRECATED in favour of `TrustPolicy` (ADR 0008 Amendment G), and still served and reconciled: a cluster with a roster keeps working, and with no TrustPolicy present the controller synthesises `legacy-roster-v1` from this object. What it cannot express is a key LIFECYCLE — retirement, revocation and a usage split — so a rotation here makes every archive the old key signed unverifiable, which is why the replacement exists. Cluster-scoped. The keys that may authorise (`approverKeys`) and the keys that may attest (`signingKeys`), both carrying public key material, plus the cluster ids a restore may target. `spec` is immutable.",
    plural = "trustrosters",
    singular = "trustroster",
    status = "TrustRosterStatus",
    printcolumn = r#"{"name":"KEYS","type":"string","jsonPath":".spec.approverKeys[*].keyId","description":"the approver key ids; the signing key ids are in spec.signingKeys"}"#,
    printcolumn = r#"{"name":"LOADED","type":"string","jsonPath":".status.loaded"}"#,
    printcolumn = r#"{"name":"EXPIRED","type":"string","jsonPath":".status.expiredKeyIds[*]"}"#,
    printcolumn = r#"{"name":"AGE","type":"date","jsonPath":".metadata.creationTimestamp"}"#
)]
#[serde(rename_all = "camelCase")]
pub struct TrustRosterSpec {
    /// The keys that may **authorise** a restore or a backup. An `Approval`
    /// verifies against one of these.
    pub approver_keys: Vec<KeyEntry>,
    /// The keys that may **attest** — the runners' own signing keys, whose
    /// public halves verify a scorecard, a backup receipt or a teardown
    /// record. Interface **I17**: key material, not ids.
    pub signing_keys: Vec<KeyEntry>,
    /// The cluster ids a restore may target. Read from here and never from a
    /// plan, so an edited spec cannot widen its own allowlist.
    pub allowed_cluster_ids: Vec<String>,
}

/// `TrustRoster.status`.
#[derive(Deserialize, Serialize, Clone, Debug, Default, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct TrustRosterStatus {
    /// Whether the controller parsed every entry on this roster.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub loaded: Option<bool>,
    /// The `keyId`s past their `notAfter`, from both lists. Declared here so
    /// no consumer has to derive expiry itself from a clock it does not
    /// share with the controller.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expired_key_ids: Option<Vec<String>>,
    /// `Loaded`, per-key `Expired`, and whatever else the controller reports.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub conditions: Option<Vec<Condition>>,
}
