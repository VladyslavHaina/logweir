//! `Approval` — four spec fields, and a create form that must supply all four.
//!
//! A page that posts only `approvalBytes` and `sidecarBytes` posts an object
//! the CRD schema REJECTS (critique C H3). `subjectRef` and `planHash` come
//! from the route the wizard navigated to; the controller recomputes the hash
//! from the referent's own bytes regardless, so a wrong `planHash` is a
//! refusal and never a silent acceptance.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use super::Condition;

/// What an `Approval` can be about.
///
/// EXACTLY TWO VALUES. `Switchover` is tag 2 and is **not** in this enum, so
/// an `Approval` cannot name one: the subject kind is part of the bytes the
/// approval binds, and without that an approved restore in a namespace would
/// double as "a valid signature by a rostered key exists here", which any
/// approved restore already produced.
#[derive(Deserialize, Serialize, Clone, Copy, Debug, JsonSchema, PartialEq, Eq)]
pub enum SubjectKind {
    /// A `Restore` — including a drill, which is a `Restore` with
    /// `spec.target.mode: scratch`.
    Restore,
    /// A `Backup`.
    Backup,
}

/// The object this approval is about.
#[derive(Deserialize, Serialize, Clone, Debug, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct SubjectRef {
    /// `Restore` or `Backup`. Comes from the route the wizard navigated to.
    pub kind: SubjectKind,
    /// The subject's `metadata.name`, in this namespace.
    pub name: String,
}

/// `Approval.spec` — **four fields, all required**.
#[derive(kube::CustomResource, Deserialize, Serialize, Clone, Debug, JsonSchema)]
#[kube(
    group = "logweir.dev",
    version = "v1alpha1",
    kind = "Approval",
    doc = "A DSSE-signed authorisation for one Restore or Backup. All four spec fields are required: a create form that posts only the documents posts an object the schema rejects. `spec` is immutable.",
    plural = "approvals",
    singular = "approval",
    namespaced,
    status = "ApprovalStatus",
    printcolumn = r#"{"name":"SUBJECT","type":"string","jsonPath":".spec.subjectRef.name"}"#,
    printcolumn = r#"{"name":"VERIFIED","type":"string","jsonPath":".status.verified"}"#,
    printcolumn = r#"{"name":"APPROVER","type":"string","jsonPath":".status.approver"}"#,
    printcolumn = r#"{"name":"KEY-ID","type":"string","jsonPath":".status.matchedKeyId"}"#,
    printcolumn = r#"{"name":"AGE","type":"date","jsonPath":".metadata.creationTimestamp"}"#
)]
#[serde(rename_all = "camelCase")]
pub struct ApprovalSpec {
    /// The object this approval is about. Comes from the route the wizard
    /// navigated to; it is part of the bytes the approval binds, so an
    /// `Approval` whose `planHash` matches a `Restore` is never accepted for
    /// anything else.
    pub subject_ref: SubjectRef,
    /// The sha256 of the subject's `planBytes`, lowercase hex. Supplied by the
    /// create form for the operator to compare, and RECOMPUTED by the
    /// controller from the referent's own bytes regardless — a mismatch is a
    /// refusal.
    pub plan_hash: String,
    /// The approval document, as **the UTF-8 document text, verbatim, never
    /// base64**.
    ///
    /// The UI pastes exactly what `logweir drill approve` wrote and the
    /// controller hashes exactly those bytes. Base64 would insert an encoding
    /// step between the approver's file and the hashed bytes, which is the
    /// class of transformation `planBytes` exists to forbid. This field
    /// therefore declares no `format: byte`.
    pub approval_bytes: String,
    /// The detached DSSE sidecar for `approvalBytes`, as **the UTF-8 document
    /// text, verbatim, never base64**.
    ///
    /// Same reason as `approvalBytes`: the bytes the approver's tool wrote are
    /// the bytes the controller verifies, with no encoding step in between.
    /// This field declares no `format: byte`.
    pub sidecar_bytes: String,
}

/// `Approval.status`.
#[derive(Deserialize, Serialize, Clone, Debug, Default, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ApprovalStatus {
    /// Whether the DSSE signature over `approvalBytes` verified against a
    /// `TrustRoster` approver key, and the recomputed plan hash matched.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub verified: Option<bool>,
    /// The `TrustRoster.spec.approverKeys[].keyId` that verified it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub matched_key_id: Option<String>,
    /// The approver's subject, read from the matched roster entry.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub approver: Option<String>,
    /// The change ticket the approval document names, when it names one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ticket: Option<String>,
    /// Whether the approver and the requester are the same principal —
    /// `false` means only that the two matched key ids differ.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub self_attested_risk: Option<bool>,
    /// `Verified`, and whatever else the controller reports.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub conditions: Option<Vec<Condition>>,
}
