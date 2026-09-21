//! `Approval` — four spec fields, and a create form that must supply all four.
//!
//! A page that posts only `approvalBytes` and `sidecarBytes` posts an object
//! the CRD schema REJECTS (critique C H3). `subjectRef` and `planHash` come
//! from the route the wizard navigated to; the controller recomputes the hash
//! from the referent's own bytes regardless, so a wrong `planHash` is a
//! refusal and never a silent acceptance.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use super::{Condition, Time};

/// What an `Approval` can be about.
///
/// `Switchover` is tag 2 and is **not** in this enum, so an `Approval` cannot
/// name one: the subject kind is part of the bytes the approval binds, and
/// without that an approved restore in a namespace would double as "a valid
/// signature by a rostered key exists here", which any approved restore
/// already produced.
///
/// THE THIRD VALUE IS ADDITIVE AND NARROW. `RehearsalSchedule` (ADR 0008
/// Amendment G) is what carries a STANDING authorization: one signed document
/// whose `planHash` is a digest of a sealed `RehearsalSchedule.spec`, checked
/// again every slot. It is a different referent for the same rule, not a new
/// rule — the approval controller's checks 7 and 8 become "the recomputed
/// subject digest" and "the signed subject kind" — and it exists because a
/// rehearsal that needed a human every week is a rehearsal nobody runs, while
/// a controller that could mint its own authorization would be the bypass
/// PLAT-19.2 exists to prevent.
#[derive(Deserialize, Serialize, Clone, Copy, Debug, JsonSchema, PartialEq, Eq)]
pub enum SubjectKind {
    /// A `Restore` — including a drill, which is a `Restore` with
    /// `spec.target.mode: scratch`.
    Restore,
    /// A `Backup`.
    Backup,
    /// A `RehearsalSchedule`, carrying a standing rehearsal authorization.
    RehearsalSchedule,
}

impl SubjectKind {
    /// The kind as the string check 8 compares.
    ///
    /// WHY A STRING AND NOT AN ENUM COMPARISON. The fifth check
    /// ([`crate::controllers::approval::evaluate`] step 8) compares the
    /// `subject_kind` inside the **signed bytes** with the referent's kind, and
    /// the signed bytes carry a string that `logweir drill approve` wrote — not
    /// a `SubjectKind`. Deserialising it into this enum first would turn "the
    /// approval binds a kind this build has never heard of" (a `Switchover`
    /// approval replayed against a tag-1 controller) into a parse error two
    /// checks earlier, reported as a bad signature. Comparing strings keeps
    /// that case where it belongs: `SubjectKindMismatch`, naming both sides.
    ///
    /// These spellings are the serde spellings of the variants — the enum
    /// takes no `rename_all`, so `Restore`, `Backup` and `RehearsalSchedule`
    /// are what the wire carries — and
    /// `the_subject_kind_strings_are_the_wire_spellings` asserts that by
    /// round-tripping each through `serde_json`.
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Restore => "Restore",
            Self::Backup => "Backup",
            Self::RehearsalSchedule => "RehearsalSchedule",
        }
    }
}

impl std::fmt::Display for SubjectKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
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

/// The exact Kubernetes object whose bytes were verified.
///
/// This is controller-produced provenance, not another user assertion.  In
/// particular, `uid` prevents a verified Approval from silently following a
/// delete/recreate of a same-named Restore.
#[derive(Deserialize, Serialize, Clone, Debug, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct VerifiedSubjectRef {
    /// The referent's API version at verification time.
    pub api_version: String,
    /// The referent's kind at verification time.
    pub kind: SubjectKind,
    /// The referent's name at verification time.
    pub name: String,
    /// The referent's namespace at verification time.
    pub namespace: String,
    /// The referent's immutable Kubernetes UID at verification time.
    pub uid: String,
}

/// `Approval.spec` — **four fields, all required**.
#[derive(kube::CustomResource, Deserialize, Serialize, Clone, Debug, JsonSchema)]
#[kube(
    group = "logweir.dev",
    version = "v1alpha1",
    kind = "Approval",
    doc = "A DSSE-signed authorisation for one Restore or Backup. All four spec fields are required: a create form that posts only the documents posts an object the schema rejects. `spec` is immutable. `subjectRef.kind` gained `RehearsalSchedule` in ADR 0008 Amendment G: the enum only GROWS, but the decode is closed, so a controller image that predates that amendment cannot deserialize such an object and its whole Approval watch stalls. Nothing in this build creates one; before anything does, every rollback target must already understand the value (docs/kubernetes.md, `Upgrade, rollback and legacy Jobs`).",
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

/// The matched approver key's declared validity window, published beside the
/// `Verified` condition.
///
/// # Why a status field, when the condition already carries the verdict
///
/// Defect **APPROVAL-KEY-WINDOW-UNPUBLISHED**. Since PREFLIGHT-APPROVAL-ROSTER
/// the restore preflight's two `approval.*` rows RELAY this object's verdict
/// and no longer resolve the approver key themselves — the roster they used to
/// read is not the authority the namespace's `TrustPolicy` is. That removed the
/// only place the key's window was known, and took two D2 §6.3 behaviours with
/// it: `ApproverKeyExpiresBeforeDeadline`, which warns ahead of time about a
/// key that expires inside a restore's own deadline, and the
/// `min(10 m, notAfter)` re-check cap on both rows. Neither is a verdict about
/// this `Approval`; both are questions about the KEY, and only the controller
/// that resolved the key can answer them.
///
/// So the window is published here, by the one process that resolved it,
/// derived from the same [`crate::trust::ResolvedTrust`] the `Verified`
/// condition is derived from and written in the same preconditioned patch. A
/// reader that finds it can compare it; a reader that does not MUST read
/// `unknown` and never `valid` (PLAT-19.1's acceptance: unevaluated or stale
/// expiry information is unknown, not valid).
///
/// IT IS NOT A SECOND VERDICT. The window is what the resolved trust DECLARES
/// for the key; whether the key may authorise anything now is the `Verified`
/// condition's answer and nothing here overrides it. A retired or revoked key
/// has an open window and authorises nothing, which is exactly why this type
/// carries three facts and no state.
#[derive(Deserialize, Serialize, Clone, Debug, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ApproverKeyWindow {
    /// The `keyId` this window belongs to — the key the signature actually
    /// verified under, the same value [`ApprovalStatus::matched_key_id`]
    /// carries.
    ///
    /// REPEATED HERE ON PURPOSE. A window beside a key id is two fields a
    /// merge patch can leave in disagreement; a window that NAMES its key is
    /// one fact, and a reader can refuse a window that is about some other
    /// key rather than compare a deadline against it.
    pub key_id: String,
    /// That key's declared `notBefore`.
    pub not_before: Time,
    /// That key's declared `notAfter` — the instant this approval stops being
    /// true on its own, and the value the preflight caps its re-check at.
    pub not_after: Time,
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
    /// The matched approver key's declared validity window — defect
    /// APPROVAL-KEY-WINDOW-UNPUBLISHED; see [`ApproverKeyWindow`].
    ///
    /// **ABSENT WHEN NO KEY MATCHED**, and absent means UNKNOWN to every
    /// reader, never valid. It is cleared by an explicit `null` on every
    /// refusal (`controllers::approval::CLEARABLE_STATUS_FIELDS`) rather than
    /// merely omitted, because a JSON merge patch that omits a key LEAVES IT —
    /// and a window left behind by a verdict that has since been withdrawn is
    /// the one shape a reader would read as "this key is good until then".
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub approver_key_window: Option<ApproverKeyWindow>,
    /// The exact referent identity used for the first successful verification.
    /// Once present it is retained across later failures so a same-named,
    /// recreated object can never acquire this Approval on a later reconcile.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub verified_subject_ref: Option<VerifiedSubjectRef>,
    /// `Verified`, and whatever else the controller reports.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub conditions: Option<Vec<Condition>>,
}
