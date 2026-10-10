//! PLAT-19.2 — ordinary confirmation and governed approval: the pure contract.
//!
//! # What lives here
//!
//! Everything about the two approval modes that is decidable without a clock
//! read, a key, a cluster or a file: the installation's policy set and how a
//! namespace resolves to one policy; the policy SNAPSHOT (the exact bytes a
//! run freezes, and the digest every signed document names); the
//! authorization document v2 that both modes produce (decision D0,
//! "Authorization document v2"); and the binding checks every boundary makes
//! over it — the Approval controller, Restore admission and the runner. The
//! signatures are checked by the crates that link a verifier; this crate
//! links no crypto (`scripts/check-pure-core.sh`).
//!
//! # Where the policy lives, and why it is not a namespaced object
//!
//! D0 names an immutable `ApprovalPolicy` "or the exact PLAT-19.1 policy
//! resource", bound to a namespace by INSTALLATION configuration that also
//! carries the `allowOrdinaryConfirmation` floor, and says "selecting a
//! different policy is an explicit installation-admin rollout and audit event,
//! not a namespace operator edit". This build keeps the policies AND the
//! binding in that one installation document, which the chart renders once and
//! mounts into both the controller and `logweir-api`, so the two consume the
//! same bytes and therefore the same digest (D0: "API and controller consume
//! the same content hash"). A namespaced policy object would be writable by
//! whoever holds namespace RBAC — the "a namespace names its own authority"
//! shape D3 §7.1 forbids for trust — and a new cluster-scoped kind would be a
//! second writable authority beside `TrustPolicy`. Immutability comes from the
//! content address: a signed document names the policy's `digest`, so ANY edit
//! to a policy is a different policy to every outstanding document, which is
//! D0's "binding/policy mismatch requires re-confirmation/re-approval".
//!
//! # The two modes, and the one that is never synthesised
//!
//! * [`ApprovalMode::Governed`] — the console's confirmation signature attests
//!   the requester, AND a human approver countersigns the same bytes with a
//!   `GovernedApproval` key whose principal differs from the requester.
//! * [`ApprovalMode::Ordinary`] — the console's confirmation signature alone:
//!   an authorised operator confirming their own operation.
//!
//! A namespace with no binding resolves to [`EffectivePolicy::Legacy`]
//! (`legacy-governed-v1`), which is TODAY'S behaviour byte for byte: a v1
//! approval document signed by a `GovernedApproval` key — UNLESS the
//! installation's unbound default says `confirm` (PROD-16.1, below).
//!
//! # PROD-16.1: the unbound default, and why an upgrade never reaches it
//!
//! OD-8 (2026-10-07) amends D0: a FRESH install starts with one-person
//! confirmation in the console for every namespace without a binding, so the
//! first restore needs no personal key. An unbound namespace then resolves to
//! [`default_confirm_policy`] (`default-confirm-v1`, `Ordinary`), a concrete,
//! content-addressed policy like any other: the console signs a v2 document
//! naming it, and every checkpoint compares that name and digest exactly as for
//! an explicit binding.
//!
//! What decides it, in this order ([`ApprovalPolicySet::unbound_basis`]):
//!
//! 1. `defaultMode` in the installation document (`approvalPolicy.default` in
//!    the chart): `confirm` or `strict`, an explicit installation-admin
//!    rollout — and the way an operator of an OLDER install opts in;
//! 2. otherwise the FRESH-INSTALL MARKER ([`InstallationMarker`]): a
//!    [`MarkerClaim`] that `logweir identity bootstrap` writes in the one run
//!    that GENERATES the installation identity, naming the default
//!    `TrustPolicy` the same run created for the identity and the console
//!    key. [`verify_marker`] — the one decision both readers make — honours
//!    it only beside that exact, hook-made trust entry. An upgraded install's
//!    identity already existed, so the hook never marks it and never trusts
//!    its console key, and an annotation patched in later names no such
//!    entry: it stays `legacy-governed-v1` (D0: "Ordinary mode cannot be
//!    enabled merely by upgrading");
//! 3. otherwise `legacy-governed-v1`.
//!
//! The marker is runtime state (the API and the controller read it from the
//! cluster), so [`ApprovalPolicySet::parse`] yields an UNMARKED set and the
//! reader applies what it observed with [`ApprovalPolicySet::with_installation`].
//! An unread or unreadable marker is [`InstallationMarker::Unmarked`]: the
//! strict side.
//!
//! # The three modes the operator sees
//!
//! [`OperatorMode`] is the ONE mapping between the internal names, which stay
//! because they are inside signed bytes and snapshots (`Ordinary`, `Governed`,
//! `legacy-governed-v1`), and the names the chart values, the console and the
//! docs use: `confirm`, `two-person` (PROD-16.2: `Governed` with
//! `approverSignature: Console`) and `strict`.
//!
//! # PROD-16.2: two-person approval in the console
//!
//! A `Governed` policy may say WHO signs the approval:
//! [`ApproverSignature::Console`] (`approverSignature: Console`; absent is the
//! personal `GovernedApproval` key, today's behaviour byte for byte). Under
//! it the approver signs in to the shared console and clicks Approve, and the
//! console signs the SAME authorization document with two optional fields
//! added, `approver {issuer, subject}` and `approvedAt`, under document
//! format 2.2.0 ([`RESTORE_AUTHORIZATION_FORMAT_VERSION_APPROVER`]). The
//! setting is inside the policy snapshot, so inside the digest every signed
//! document names: a request made under one setting cannot be approved under
//! the other.
//!
//! What this crate decides, for every reader alike (the console before it
//! signs, the Approval controller, Restore admission, the runner):
//!
//! * the two fields are refused under a version older than 2.2.0, one without
//!   the other, and under any policy that is not `Console`
//!   ([`RestoreAuthorization::check_approver_fields`], inside
//!   [`check_binding`]);
//! * under a `Console` policy a document WITHOUT them authorises nothing — it
//!   is the pending request ([`AuthorizationRefusal::ApprovalRequired`]);
//! * WITH them, the approver must be a second person
//!   ([`console_separation`]: comparable principals, no local administrator,
//!   no service account, the same issuer, another subject) and `approvedAt`
//!   must lie inside the request's own window ([`check_console_approval`]).

use std::collections::BTreeMap;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::ids::sha256_prefixed;
use crate::trust::KeyUsage;

/// The name the synthesised policy of an unbound namespace carries.
pub const LEGACY_GOVERNED_POLICY_NAME: &str = "legacy-governed-v1";

/// PROD-16.1: the name of the policy an unbound namespace resolves to when the
/// installation's unbound default is `confirm` ([`default_confirm_policy`]).
/// Reserved: no declared policy may carry it.
pub const DEFAULT_CONFIRM_POLICY_NAME: &str = "default-confirm-v1";

/// PROD-16.1: the annotation that records a fresh install's `confirm` unbound
/// default. It appears in TWO places, both written by the identity hook in the
/// one run that GENERATES the installation identity, and both are required:
///
/// * on the public identity ConfigMap, as a [`MarkerClaim`] naming the
///   installation `TrustPolicy` the same run created (name and UID) and the
///   two key ids it trusts;
/// * on that `TrustPolicy`, as the bare value [`APPROVAL_DEFAULT_CONFIRM`].
///
/// [`verify_marker`] honours the claim only when the policy it names exists
/// with that UID, the hook's provenance, `default: true`, this annotation, and
/// exactly those two keys with their one usage each. Anything else is
/// [`InstallationMarker::Unmarked`] — `legacy-governed-v1`.
pub const APPROVAL_DEFAULT_ANNOTATION: &str = "logweir.dev/approval-default";

/// The value the hook-made `TrustPolicy` carries under
/// [`APPROVAL_DEFAULT_ANNOTATION`], and the first field of a [`MarkerClaim`].
pub const APPROVAL_DEFAULT_CONFIRM: &str = "confirm";

/// PROD-16.1: the provenance annotation on the installation `TrustPolicy`.
pub const CREATED_BY_ANNOTATION: &str = "logweir.dev/created-by";

/// Its value: the identity hook.
pub const CREATED_BY_IDENTITY_BOOTSTRAP: &str = "identity-bootstrap";

/// The policy snapshot's own format version.
pub const POLICY_SNAPSHOT_FORMAT_VERSION: &str = "1";

/// The snapshot's `kind`, so a snapshot can never be confused with another
/// small JSON document mounted beside it.
pub const POLICY_SNAPSHOT_KIND: &str = "ApprovalPolicySnapshot";

/// The shortest document lifetime a policy may declare.
pub const MIN_MAX_AGE_SECONDS: i64 = 60;

/// The installation maximum D0 requires ("`maxAgeSeconds` with a bounded
/// installation maximum"): seven days.
pub const MAX_MAX_AGE_SECONDS: i64 = 7 * 86_400;

/// The default lifetime of an ordinary confirmation. Short on purpose: an
/// ordinary confirmation is admitted within seconds of being signed, and a long
/// window is only a longer replay window for an unadmitted request.
pub const DEFAULT_ORDINARY_MAX_AGE_SECONDS: i64 = 900;

/// The default lifetime of a governed request: a day for a human to approve.
pub const DEFAULT_GOVERNED_MAX_AGE_SECONDS: i64 = 86_400;

/// How far a document's `issuedAt` may lie ahead of the verifier's clock. The
/// console and the controller read two different clocks; without a bound here
/// a skew of one millisecond would refuse a fresh confirmation until the
/// Approval controller's next heartbeat.
pub const MAX_ISSUED_AT_SKEW_SECONDS: i64 = 60;

/// The longest change ticket a document carries.
pub const MAX_TICKET_LEN: usize = 128;

/// The most policies one installation document may declare.
pub const MAX_POLICIES: usize = 64;

/// The most namespace bindings one installation document may declare.
pub const MAX_BINDINGS: usize = 256;

/// The DSSE `payloadType` of an authorization document v2.
///
/// ITS OWN PAYLOAD TYPE, and that is the version boundary: a v1 approval
/// signature cannot be replayed as a v2 document or the reverse, because the
/// payload type is inside what the signature covers (DSSE PAE). An old
/// controller reached by rollback refuses every v2 document as a
/// `PayloadTypeMismatch` — fail closed, which is D0's rollback rule.
pub const PAYLOAD_TYPE_RESTORE_AUTHORIZATION: &str =
    "application/vnd.logweir.restore-authorization+json;version=2.0.0";

/// The document's `formatVersion`: what this build reads (major 2), and the
/// version every document that carries neither `approvalSubject` nor
/// `originalNameConfirmation` is written at — so every such document is byte
/// for byte what it was. [`restore_authorization_format_version_for`] picks
/// [`RESTORE_AUTHORIZATION_FORMAT_VERSION_SUBJECT`] for one that does.
pub const RESTORE_AUTHORIZATION_FORMAT_VERSION: &str = "2.0.0";

/// **PROD-15.1.** The minor that defines `approvalSubject` and
/// `originalNameConfirmation`. A document declaring an older minor that
/// carries either is refused: the fields change WHAT the document
/// authorises (a restore under the original topic names), and a document
/// that predates them cannot have said so. The standing authorization's
/// 1.1.0 is the same pattern
/// (`crate::execution_contract::STANDING_AUTHORIZATION_COVERAGE_SINCE_MINOR`).
pub const RESTORE_AUTHORIZATION_SUBJECT_SINCE_MINOR: u64 = 1;

/// **PROD-15.1.** The version a document that carries `approvalSubject` or
/// `originalNameConfirmation` is written at.
pub const RESTORE_AUTHORIZATION_FORMAT_VERSION_SUBJECT: &str = "2.1.0";

/// **PROD-16.2.** The minor that defines `approver` and `approvedAt`, the
/// two fields a console approval adds. A document declaring an older minor
/// that carries either is refused: they say a SECOND PERSON approved, and a
/// document that predates them cannot have said so.
pub const RESTORE_AUTHORIZATION_APPROVER_SINCE_MINOR: u64 = 2;

/// **PROD-16.2.** The version a document that carries `approver` or
/// `approvedAt` is written at.
pub const RESTORE_AUTHORIZATION_FORMAT_VERSION_APPROVER: &str = "2.2.0";

/// The `formatVersion` an authorization document v2 WITHOUT a console
/// approver is written at: 2.1.0 when it carries the approval subject or the
/// typed confirmation, 2.0.0 otherwise. A request (and a one-person
/// confirmation) takes its version from here;
/// [`restore_authorization_format_version`] is the whole rule.
#[must_use]
pub fn restore_authorization_format_version_for(
    approval_subject: Option<&str>,
    original_name_confirmation: Option<&crate::original_name::OriginalNameConfirmation>,
) -> &'static str {
    restore_authorization_format_version(approval_subject, original_name_confirmation, false)
}

/// The `formatVersion` an authorization document v2 is written at — THE ONE
/// PLACE a writer takes the version from:
///
/// * **2.2.0** when it carries the console approver (`approver`,
///   `approvedAt`; PROD-16.2);
/// * else **2.1.0** when it carries the approval subject or the typed
///   confirmation (PROD-15.1);
/// * else **2.0.0**.
///
/// Each step is taken exactly when its fields are present, so a document
/// that does not need a field is byte for byte what it was before the field
/// existed.
#[must_use]
pub fn restore_authorization_format_version(
    approval_subject: Option<&str>,
    original_name_confirmation: Option<&crate::original_name::OriginalNameConfirmation>,
    console_approver: bool,
) -> &'static str {
    if console_approver {
        RESTORE_AUTHORIZATION_FORMAT_VERSION_APPROVER
    } else if approval_subject.is_some() || original_name_confirmation.is_some() {
        RESTORE_AUTHORIZATION_FORMAT_VERSION_SUBJECT
    } else {
        RESTORE_AUTHORIZATION_FORMAT_VERSION
    }
}

/// The issuer of the in-cluster administrator console's one identity
/// (`localAdmin` mode). PROD-16.2: never a party to a console approval — its
/// one identity cannot be two people — at any reader.
pub const LOCAL_ADMIN_ISSUER: &str = "urn:logweir:local-admin";

/// Kubernetes' reserved subject prefix (`system:serviceaccount:…`,
/// `system:node:…`, `system:admin`). PROD-16.2: a subject under it is a
/// machine, never one of the two people of a console approval.
pub const KUBERNETES_SYSTEM_SUBJECT_PREFIX: &str = "system:";

/// The longest subject a console approval compares: OpenID Connect Core's
/// bound on `sub` ("MUST NOT exceed 255 ASCII characters").
pub const MAX_COMPARABLE_SUBJECT_LEN: usize = 255;

/// The longest issuer a console approval compares.
pub const MAX_COMPARABLE_ISSUER_LEN: usize = 2048;

/// The document's `kind`.
pub const RESTORE_AUTHORIZATION_KIND: &str = "RestoreAuthorization";

/// The API version every Logweir subject carries.
pub const SUBJECT_API_VERSION: &str = "logweir.dev/v1alpha1";

/// The one subject kind a v2 document may authorise in this build.
pub const SUBJECT_KIND_RESTORE: &str = "Restore";

/// The two approval modes.
#[derive(Serialize, Deserialize, Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum ApprovalMode {
    /// A console-attested requester plus an independent human approver.
    Governed,
    /// A console-attested requester confirming their own operation.
    Ordinary,
}

impl ApprovalMode {
    /// The wire spelling.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Governed => "Governed",
            Self::Ordinary => "Ordinary",
        }
    }

    /// The key usages whose signatures a v2 document in this mode requires,
    /// in the order they are checked.
    ///
    /// The console's `ConsoleConfirmation` signature is required in BOTH modes
    /// (D0: "the console signature attests the verified requester in both
    /// modes"); `Governed` adds the approver's. A `GovernedApproval`
    /// signature is never sufficient on its own for a v2 document, because
    /// without the console's attestation nothing says who the requester was
    /// and separation of duties is not checkable.
    #[must_use]
    pub const fn required_usages(self) -> &'static [KeyUsage] {
        match self {
            Self::Governed => &[KeyUsage::ConsoleConfirmation, KeyUsage::GovernedApproval],
            Self::Ordinary => &[KeyUsage::ConsoleConfirmation],
        }
    }
}

impl std::fmt::Display for ApprovalMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// **PROD-16.2 — who signs a `Governed` policy's approval.**
///
/// A property of the POLICY, frozen into its snapshot and so into the digest
/// every signed document names: a request confirmed under one value is a
/// document for another policy under the other.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub enum ApproverSignature {
    /// A personal `GovernedApproval` key countersigns the console's
    /// confirmation (`strict`). ABSENT on the wire: every policy written
    /// before PROD-16.2 is this, with the snapshot bytes it always had.
    #[default]
    PersonalKey,
    /// The console signs for an approver who signed in and clicked Approve
    /// and who is not the requester (`two-person`). `approverSignature:
    /// Console` on the wire.
    Console,
}

impl ApproverSignature {
    /// The wire spelling of [`Self::Console`], the only one ever written.
    pub const CONSOLE: &'static str = "Console";

    /// The words a message uses.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::PersonalKey => "a personal GovernedApproval key",
            Self::Console => "the console (approverSignature: Console)",
        }
    }
}

/// The one value `approverSignature` has on the wire. A second spelling for
/// the default would be a second snapshot, and a second digest, for one
/// policy.
#[derive(Serialize, Deserialize, Clone, Copy, Debug, PartialEq, Eq)]
enum ConsoleWord {
    Console,
}

/// PROD-16.1 — the three approval modes an OPERATOR sees, and the one place
/// they are mapped onto the internal names.
///
/// | operator | internal | who approves |
/// |---|---|---|
/// | `confirm` | `Ordinary` (v2, the console's signature) | the requester, one click in the console |
/// | `two-person` | `Governed` with `approverSignature: Console` (PROD-16.2) | a second person, one click in the shared console |
/// | `strict` | `Governed` (v2) or `legacy-governed-v1` (v1) | a human approver's personal key |
///
/// The internal names stay: they are inside signed documents and policy
/// snapshots, and renaming them would change signed bytes.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum OperatorMode {
    /// One-person confirmation in the console (`Ordinary`).
    Confirm,
    /// Two-person approval in the console — PROD-16.2: a `Governed` policy
    /// whose `approverSignature` is `Console`. A second person signs in to
    /// the shared console and clicks Approve; no personal key.
    TwoPerson,
    /// A personal-key approval: `Governed` (the console confirms, an approver
    /// countersigns with a `GovernedApproval` key) or `legacy-governed-v1`.
    Strict,
}

impl OperatorMode {
    /// The operator-facing spelling.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Confirm => "confirm",
            Self::TwoPerson => "two-person",
            Self::Strict => "strict",
        }
    }

    /// Parse an operator-facing spelling.
    #[must_use]
    pub fn from_operator_name(name: &str) -> Option<Self> {
        match name {
            "confirm" => Some(Self::Confirm),
            "two-person" => Some(Self::TwoPerson),
            "strict" => Some(Self::Strict),
            _ => None,
        }
    }

    /// What an effective policy is, in the operator's words.
    #[must_use]
    pub fn of(effective: &EffectivePolicy) -> Self {
        match effective {
            EffectivePolicy::Legacy => Self::Strict,
            EffectivePolicy::Bound(policy) => match (policy.mode, policy.approver_signature) {
                (ApprovalMode::Ordinary, _) => Self::Confirm,
                (ApprovalMode::Governed, ApproverSignature::Console) => Self::TwoPerson,
                (ApprovalMode::Governed, ApproverSignature::PersonalKey) => Self::Strict,
            },
        }
    }

    /// Whether the in-cluster administrator console (`localAdmin`) may serve
    /// this mode. `confirm` may (PROD-16.1: the confirming principal is
    /// `urn:logweir:local-admin#admin`, and whoever can reach that console can
    /// confirm — stated in SECURITY.md); `strict` may, because an independent
    /// approver key still decides; `two-person` may NOT, because the console's
    /// one identity cannot be two people.
    #[must_use]
    pub const fn allowed_in_local_admin(self) -> bool {
        !matches!(self, Self::TwoPerson)
    }
}

impl std::fmt::Display for OperatorMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// PROD-16.1 — what the installation's fresh-install marker says, as one of
/// its readers observed it. Never parsed from the policy document: the marker
/// lives on the installation identity ([`APPROVAL_DEFAULT_ANNOTATION`]).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub enum InstallationMarker {
    /// No marker, an unreadable one, or one not yet read: the strict side.
    #[default]
    Unmarked,
    /// The identity bootstrap generated this installation's identity and
    /// recorded that a fresh install starts in `confirm`.
    FreshInstallConfirm,
}

/// PROD-16.1 — the fresh-install claim the identity hook writes on the public
/// identity ConfigMap:
/// `confirm;policy=<name>;uid=<uid>;signing=<keyId>;console=<keyId>`.
///
/// It BINDS the marker to what the same run created: the installation
/// `TrustPolicy` by name AND by the UID the API server assigned it (a policy
/// deleted and re-created, or one an administrator wrote, has another), the
/// installation identity's key id, and the console key's.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MarkerClaim {
    /// The installation `TrustPolicy`'s name.
    pub policy_name: String,
    /// Its `metadata.uid` as the API server answered the hook's create.
    pub policy_uid: String,
    /// The installation identity's key id (`EvidenceSigning`).
    pub signing_key_id: String,
    /// The console key's id (`ConsoleConfirmation`).
    pub console_key_id: String,
}

impl MarkerClaim {
    /// The annotation value.
    #[must_use]
    pub fn to_annotation(&self) -> String {
        format!(
            "{APPROVAL_DEFAULT_CONFIRM};policy={};uid={};signing={};console={}",
            self.policy_name, self.policy_uid, self.signing_key_id, self.console_key_id
        )
    }

    /// Parse an annotation value. Exactly the five fields, in order, each
    /// non-empty; anything else — the bare word `confirm` included — is not a
    /// claim.
    #[must_use]
    pub fn parse(value: &str) -> Option<Self> {
        let mut parts = value.split(';');
        if parts.next() != Some(APPROVAL_DEFAULT_CONFIRM) {
            return None;
        }
        let mut field = |name: &str| {
            parts
                .next()
                .and_then(|p| p.strip_prefix(name))
                .and_then(|p| p.strip_prefix('='))
                .filter(|v| !v.is_empty() && v.trim() == *v)
                .map(str::to_string)
        };
        let claim = Self {
            policy_name: field("policy")?,
            policy_uid: field("uid")?,
            signing_key_id: field("signing")?,
            console_key_id: field("console")?,
        };
        parts.next().is_none().then_some(claim)
    }
}

/// One key of a `TrustPolicy`, as a reader extracted it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TrustKeyFacts {
    /// `keyId`.
    pub key_id: String,
    /// `usages`, as written.
    pub usages: Vec<String>,
    /// `principal.id`.
    pub principal_id: String,
}

/// What a reader read of the `TrustPolicy` a [`MarkerClaim`] names.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TrustPolicyFacts {
    /// `metadata.uid`.
    pub uid: String,
    /// `spec.default`.
    pub default: bool,
    /// The [`CREATED_BY_ANNOTATION`] value.
    pub created_by: Option<String>,
    /// The [`APPROVAL_DEFAULT_ANNOTATION`] value.
    pub approval_default: Option<String>,
    /// `spec.keys`.
    pub keys: Vec<TrustKeyFacts>,
}

/// Why a fresh-install marker was not honoured. Every arm is the strict side
/// (`legacy-governed-v1`), and every arm is logged as a WARN by both readers.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MarkerRefusal {
    /// The annotation is not a [`MarkerClaim`] (a bare `confirm`, a hand
    /// edit, a truncated value).
    NotAClaim,
    /// The claim names another installation identity than the ConfigMap it
    /// is on.
    OtherIdentity,
    /// The `TrustPolicy` it names does not exist.
    PolicyMissing,
    /// It exists with another UID: deleted and re-created, or written by
    /// someone else.
    PolicyReplaced,
    /// It does not carry the hook's provenance or the confirm annotation.
    NotHookMade,
    /// It is no longer the default policy, so it no longer governs unbound
    /// namespaces.
    NotDefault,
    /// It does not declare the claimed keys with exactly their one usage each
    /// and the hook's principals.
    KeysDiffer,
}

impl MarkerRefusal {
    /// The sentence a WARN carries.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::NotAClaim => "the annotation is not the claim the identity hook writes",
            Self::OtherIdentity => "the claim names another installation identity",
            Self::PolicyMissing => "the TrustPolicy the claim names does not exist",
            Self::PolicyReplaced => "the TrustPolicy the claim names has another UID",
            Self::NotHookMade => {
                "the TrustPolicy the claim names lacks the identity hook's provenance or its \
                 confirm annotation"
            }
            Self::NotDefault => "the TrustPolicy the claim names is no longer default: true",
            Self::KeysDiffer => {
                "the TrustPolicy the claim names does not declare the claimed keys with their \
                 one usage each"
            }
        }
    }
}

/// PROD-16.1 — THE ONE DECISION both readers (the console and the controller)
/// make over the fresh-install marker, so they cannot disagree about it.
///
/// * `annotation`: the [`APPROVAL_DEFAULT_ANNOTATION`] value on the public
///   identity ConfigMap, if any;
/// * `identity_key_id`: that ConfigMap's `key-id`;
/// * `installation_namespace`: its namespace;
/// * `policy`: the `TrustPolicy` the claim names, as read (`None`: absent).
///
/// WHY THE BINDING (the PROD-16.1 security review). Before PROD-16.1, turning
/// a governed namespace into one-person confirmation took two separate gates:
/// an approval-policy rollout, and a trust administrator trusting the console
/// key. An annotation alone would have been ONE ConfigMap patch. Bound, the
/// marker is honoured only beside a trust entry the identity hook made in the
/// same fresh-install run — so on an install that existed before, patching
/// the annotation in changes nothing, and forging the trust entry is the
/// trust administrator's gate, as before.
///
/// # Errors
///
/// `Ok(Unmarked)` for no annotation; `Err` for an annotation that is not
/// honoured — which the caller ALSO treats as unmarked, and logs as a WARN.
pub fn verify_marker(
    annotation: Option<&str>,
    identity_key_id: Option<&str>,
    installation_namespace: &str,
    policy: Option<&TrustPolicyFacts>,
) -> Result<InstallationMarker, MarkerRefusal> {
    let Some(annotation) = annotation else {
        return Ok(InstallationMarker::Unmarked);
    };
    let claim = MarkerClaim::parse(annotation).ok_or(MarkerRefusal::NotAClaim)?;
    if identity_key_id != Some(claim.signing_key_id.as_str()) {
        return Err(MarkerRefusal::OtherIdentity);
    }
    let policy = policy.ok_or(MarkerRefusal::PolicyMissing)?;
    if policy.uid != claim.policy_uid {
        return Err(MarkerRefusal::PolicyReplaced);
    }
    if policy.created_by.as_deref() != Some(CREATED_BY_IDENTITY_BOOTSTRAP)
        || policy.approval_default.as_deref() != Some(APPROVAL_DEFAULT_CONFIRM)
    {
        return Err(MarkerRefusal::NotHookMade);
    }
    if !policy.default {
        return Err(MarkerRefusal::NotDefault);
    }
    let carries = |key_id: &str, usage: &str, principal_prefix: &str| {
        policy.keys.iter().any(|k| {
            k.key_id == key_id
                && k.usages.len() == 1
                && k.usages[0] == usage
                && k.principal_id.starts_with(principal_prefix)
        })
    };
    if !carries(
        &claim.signing_key_id,
        "EvidenceSigning",
        &format!("install:{installation_namespace}/"),
    ) || !carries(
        &claim.console_key_id,
        "ConsoleConfirmation",
        &format!("console:{installation_namespace}/"),
    ) {
        return Err(MarkerRefusal::KeysDiffer);
    }
    Ok(InstallationMarker::FreshInstallConfirm)
}

/// The installation document's explicit unbound default (`defaultMode`).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum UnboundDefault {
    /// `defaultMode: confirm` — every unbound namespace is `default-confirm-v1`.
    Confirm,
    /// `defaultMode: strict` — every unbound namespace is `legacy-governed-v1`,
    /// whatever the marker says.
    Strict,
}

/// WHY an unbound namespace resolves the way it does — what the console shows
/// beside the mode.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum UnboundBasis {
    /// `defaultMode` in the installation document.
    Configured(UnboundDefault),
    /// No `defaultMode`, and the fresh-install marker.
    FreshInstall,
    /// No `defaultMode` and no marker: `legacy-governed-v1`.
    Legacy,
}

impl UnboundBasis {
    /// The wire spelling.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Configured(_) => "configured",
            Self::FreshInstall => "freshInstall",
            Self::Legacy => "legacy",
        }
    }
}

/// PROD-16.1 — the policy an unbound namespace resolves to under a `confirm`
/// unbound default: `Ordinary`, the ordinary default lifetime, no approver.
///
/// A CONSTANT, so the console and the controller compute the same snapshot and
/// digest without sharing anything but this build.
#[must_use]
pub fn default_confirm_policy() -> ApprovalPolicy {
    ApprovalPolicy {
        name: DEFAULT_CONFIRM_POLICY_NAME.to_string(),
        mode: ApprovalMode::Ordinary,
        max_age_seconds: DEFAULT_ORDINARY_MAX_AGE_SECONDS,
        require_distinct_principal: false,
        approver_signature: ApproverSignature::PersonalKey,
    }
}

/// One named, content-addressed approval policy.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ApprovalPolicy {
    /// The installation-unique name.
    pub name: String,
    /// Which of the two modes.
    pub mode: ApprovalMode,
    /// The longest `expiresAt - issuedAt` a document under this policy may
    /// declare.
    pub max_age_seconds: i64,
    /// Whether the governed approver's principal must differ from the
    /// requester's. Always `true` for `Governed` (D0: "must be true for
    /// Governed in the supported baseline"); meaningless and `false` for
    /// `Ordinary`, which has no approver.
    pub require_distinct_principal: bool,
    /// **PROD-16.2.** Who signs the approval of a `Governed` policy: a
    /// personal key (the default, and the only value an `Ordinary` policy
    /// has) or the console.
    pub approver_signature: ApproverSignature,
}

/// The snapshot as serialised: fixed field order, so the bytes — and
/// therefore the digest — are a function of the policy alone.
///
/// PROD-16.2: `approverSignature` is the LAST key and is written only when
/// it is `Console`, so every policy without it keeps the bytes and the digest
/// it always had. A runner built before the key refuses a snapshot that
/// carries it (`deny_unknown_fields`): it never runs a two-person policy as
/// a personal-key one.
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct Snapshot {
    format_version: String,
    kind: String,
    name: String,
    mode: ApprovalMode,
    max_age_seconds: i64,
    require_distinct_principal: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    approver_signature: Option<ConsoleWord>,
}

impl ApprovalPolicy {
    /// The exact snapshot bytes a run freezes into its bundle.
    #[must_use]
    pub fn snapshot_bytes(&self) -> Vec<u8> {
        let snapshot = Snapshot {
            format_version: POLICY_SNAPSHOT_FORMAT_VERSION.to_string(),
            kind: POLICY_SNAPSHOT_KIND.to_string(),
            name: self.name.clone(),
            mode: self.mode,
            max_age_seconds: self.max_age_seconds,
            require_distinct_principal: self.require_distinct_principal,
            approver_signature: match self.approver_signature {
                ApproverSignature::PersonalKey => None,
                ApproverSignature::Console => Some(ConsoleWord::Console),
            },
        };
        // A struct of strings, an enum, an integer and a bool cannot fail to
        // serialise; an empty vector would hash to a digest no document names.
        serde_json::to_vec(&snapshot).unwrap_or_default()
    }

    /// `sha256:<hex>` over [`Self::snapshot_bytes`] — the value a signed
    /// document names as `policy.digest`.
    #[must_use]
    pub fn digest(&self) -> String {
        sha256_prefixed(&self.snapshot_bytes())
    }

    /// Read a snapshot back — the runner's direction. The snapshot must be the
    /// CANONICAL rendering of what it parses to, so a reader cannot be handed
    /// equivalent-but-different bytes that hash to another digest.
    ///
    /// # Errors
    ///
    /// A message naming what is wrong with the bytes.
    pub fn from_snapshot_bytes(bytes: &[u8]) -> Result<Self, String> {
        let snapshot: Snapshot = serde_json::from_slice(bytes)
            .map_err(|e| format!("the approval-policy snapshot does not parse: {e}"))?;
        if snapshot.format_version != POLICY_SNAPSHOT_FORMAT_VERSION
            || snapshot.kind != POLICY_SNAPSHOT_KIND
        {
            return Err(format!(
                "the approval-policy snapshot declares formatVersion {:?} and kind {:?}; this \
                 build reads {POLICY_SNAPSHOT_FORMAT_VERSION:?} and {POLICY_SNAPSHOT_KIND:?}",
                snapshot.format_version, snapshot.kind
            ));
        }
        let policy = Self {
            name: snapshot.name,
            mode: snapshot.mode,
            max_age_seconds: snapshot.max_age_seconds,
            require_distinct_principal: snapshot.require_distinct_principal,
            approver_signature: match snapshot.approver_signature {
                None => ApproverSignature::PersonalKey,
                Some(ConsoleWord::Console) => ApproverSignature::Console,
            },
        };
        // PROD-16.2: only a Governed policy has an approver, so only it can
        // say who signs for one. A snapshot that says so of an Ordinary
        // policy was not rendered by `ApprovalPolicySet::parse`.
        if policy.approver_signature == ApproverSignature::Console
            && policy.mode != ApprovalMode::Governed
        {
            return Err(format!(
                "the approval-policy snapshot says approverSignature: Console on a {} policy; \
                 only a Governed policy has an approver",
                policy.mode
            ));
        }
        if policy.snapshot_bytes() != bytes {
            return Err(
                "the approval-policy snapshot is not in canonical form; the bytes a run \
                 freezes are exactly the bytes the controller rendered"
                    .to_string(),
            );
        }
        Ok(policy)
    }
}

/// What a namespace resolves to.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum EffectivePolicy {
    /// No binding and a `strict` (or unmarked) unbound default:
    /// `legacy-governed-v1`, today's behaviour. Accepts v1 approval documents
    /// signed by a `GovernedApproval` key and nothing else.
    Legacy,
    /// An explicit installation binding, or (PROD-16.1) an unbound namespace
    /// under a `confirm` unbound default, which is the concrete
    /// [`default_confirm_policy`]. Accepts authorization document v2 naming
    /// exactly this policy, and nothing else.
    Bound(ApprovalPolicy),
}

impl EffectivePolicy {
    /// The policy name — [`LEGACY_GOVERNED_POLICY_NAME`] for the synthesis.
    #[must_use]
    pub fn name(&self) -> &str {
        match self {
            Self::Legacy => LEGACY_GOVERNED_POLICY_NAME,
            Self::Bound(policy) => &policy.name,
        }
    }

    /// The mode. The synthesis is governed; it is never ordinary.
    #[must_use]
    pub fn mode(&self) -> ApprovalMode {
        match self {
            Self::Legacy => ApprovalMode::Governed,
            Self::Bound(policy) => policy.mode,
        }
    }

    /// The explicit policy, when there is one.
    #[must_use]
    pub fn bound(&self) -> Option<&ApprovalPolicy> {
        match self {
            Self::Legacy => None,
            Self::Bound(policy) => Some(policy),
        }
    }

    /// Whether this is the synthesised legacy policy.
    #[must_use]
    pub fn is_legacy(&self) -> bool {
        matches!(self, Self::Legacy)
    }

    /// The digest a reader can compare — `None` for the synthesis, which has
    /// no snapshot because no v2 document may name it.
    #[must_use]
    pub fn digest(&self) -> Option<String> {
        self.bound().map(ApprovalPolicy::digest)
    }
}

/// A policy's `mode` as the installation document may spell it: the internal
/// names, or (PROD-16.1) the operator's. The chart renders the internal names,
/// so an image-only rollback reads the same document it always did; a
/// hand-written document may use either. Mapped onto [`ApprovalMode`] in ONE
/// place, [`ModeSpelling::mode`].
#[derive(Deserialize, Clone, Copy, Debug, PartialEq, Eq)]
enum ModeSpelling {
    Governed,
    Ordinary,
    #[serde(rename = "confirm")]
    Confirm,
    #[serde(rename = "strict")]
    Strict,
    #[serde(rename = "two-person")]
    TwoPerson,
}

impl ModeSpelling {
    /// The internal mode. `two-person` (PROD-16.2) is `Governed`, with the
    /// console as the approver's signature ([`Self::implies_console`]).
    fn mode(self) -> ApprovalMode {
        match self {
            Self::Governed | Self::Strict | Self::TwoPerson => ApprovalMode::Governed,
            Self::Ordinary | Self::Confirm => ApprovalMode::Ordinary,
        }
    }

    /// Whether the spelling itself says the console signs the approval.
    fn implies_console(self) -> bool {
        matches!(self, Self::TwoPerson)
    }

    /// Whether the spelling says a PERSONAL key signs it (`strict`): the
    /// operator's word for exactly that, which `approverSignature: Console`
    /// beside it would contradict. The internal `Governed` says nothing
    /// about who signs.
    fn says_personal_key(self) -> bool {
        matches!(self, Self::Strict)
    }
}

/// A policy as the installation document writes it.
#[derive(Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct PolicyEntry {
    name: String,
    mode: ModeSpelling,
    #[serde(default)]
    max_age_seconds: Option<i64>,
    #[serde(default)]
    require_distinct_principal: Option<bool>,
    /// PROD-16.2: `Console`, or absent (a personal key). A field an OLDER
    /// binary does not know, so it refuses the whole document at start
    /// (`deny_unknown_fields`) rather than run a two-person namespace as a
    /// strict one, or the reverse.
    #[serde(default)]
    approver_signature: Option<String>,
}

/// The installation document as written.
///
/// `defaultMode` (PROD-16.1) is a field an OLDER binary does not know, and
/// `deny_unknown_fields` makes such a binary refuse the whole document at
/// start — fail closed, never a silent fall back to an unbound default it
/// cannot represent. The chart renders it only when it is set.
#[derive(Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct PolicySetDocument {
    #[serde(default)]
    allow_ordinary_confirmation: bool,
    #[serde(default)]
    default_mode: Option<String>,
    #[serde(default)]
    policies: Vec<PolicyEntry>,
    #[serde(default)]
    namespaces: BTreeMap<String, String>,
}

/// The installation's approval policies and namespace bindings.
///
/// `Default` is the installation that configured nothing and is not marked:
/// every namespace resolves to [`EffectivePolicy::Legacy`], which is what an
/// upgrade without a policy document must mean (D0: "existing installations
/// retain their approval requirement until explicitly changed"). A fresh
/// install's readers apply the marker they observed with
/// [`Self::with_installation`].
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ApprovalPolicySet {
    allow_ordinary_confirmation: bool,
    policies: BTreeMap<String, ApprovalPolicy>,
    bindings: BTreeMap<String, String>,
    /// `defaultMode`, when the document sets it.
    default_mode: Option<UnboundDefault>,
    /// The fresh-install marker as the reader observed it.
    installation: InstallationMarker,
}

/// Why an installation document was refused. Every refusal names the field.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PolicyConfigError(pub String);

impl std::fmt::Display for PolicyConfigError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "approval policy configuration: {}", self.0)
    }
}

impl std::error::Error for PolicyConfigError {}

fn is_dns_label(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 63
        && value
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
        && !value.starts_with('-')
        && !value.ends_with('-')
}

impl ApprovalPolicySet {
    /// Parse and validate an installation document (YAML or JSON).
    ///
    /// # The refusals
    ///
    /// * an unknown field anywhere (`deny_unknown_fields`);
    /// * a policy name that is not a DNS label, repeated, or the reserved
    ///   [`LEGACY_GOVERNED_POLICY_NAME`];
    /// * an `Ordinary` policy while `allowOrdinaryConfirmation` is not `true` —
    ///   D0's installation floor, refused rather than silently demoted, because
    ///   an operator who bound `Ordinary` and got `Governed` would be told
    ///   nothing;
    /// * `requireDistinctPrincipal: false` on a `Governed` policy (D0's
    ///   supported baseline) and `requireDistinctPrincipal: true` on an
    ///   `Ordinary` one, which has no approver to compare;
    /// * `maxAgeSeconds` outside [`MIN_MAX_AGE_SECONDS`]..=[`MAX_MAX_AGE_SECONDS`];
    /// * a binding naming an invalid namespace or an undeclared policy;
    /// * more than [`MAX_POLICIES`] policies or [`MAX_BINDINGS`] bindings.
    ///
    /// # Errors
    ///
    /// [`PolicyConfigError`] naming the field.
    pub fn parse(text: &str) -> Result<Self, PolicyConfigError> {
        if text.trim().is_empty() {
            return Ok(Self::default());
        }
        let document: PolicySetDocument = serde_yaml::from_str(text)
            .map_err(|e| PolicyConfigError(format!("the document does not parse: {e}")))?;
        let default_mode = match document.default_mode.as_deref() {
            None => None,
            // D0's floor, kept for the explicit opt-in (PROD-16.1 security
            // review): an Ordinary unbound default is an Ordinary policy, and
            // a document that does not also say allowOrdinaryConfirmation
            // never enables one.
            Some("confirm") if !document.allow_ordinary_confirmation => {
                return Err(PolicyConfigError(
                    "defaultMode confirm is ordinary confirmation for every unbound namespace, \
                     and allowOrdinaryConfirmation is not true; ordinary confirmation is an \
                     explicit installation decision (D0) and is never enabled by one field alone"
                        .to_string(),
                ));
            }
            Some("confirm") => Some(UnboundDefault::Confirm),
            Some("strict") => Some(UnboundDefault::Strict),
            Some("two-person") => {
                return Err(PolicyConfigError(
                    "defaultMode two-person is not a default: two-person approval in the console \
                     is set per namespace. Declare a policy with mode: two-person (Governed with \
                     approverSignature: Console) and bind each namespace to it; defaultMode is \
                     confirm or strict"
                        .to_string(),
                ));
            }
            Some(other) => {
                return Err(PolicyConfigError(format!(
                    "defaultMode {other:?} is not a mode; it is confirm or strict (or absent: the \
                     fresh-install marker decides)"
                )));
            }
        };
        if document.policies.len() > MAX_POLICIES {
            return Err(PolicyConfigError(format!(
                "`policies` declares {} policies; at most {MAX_POLICIES}",
                document.policies.len()
            )));
        }
        if document.namespaces.len() > MAX_BINDINGS {
            return Err(PolicyConfigError(format!(
                "`namespaces` declares {} bindings; at most {MAX_BINDINGS}",
                document.namespaces.len()
            )));
        }
        let mut policies = BTreeMap::new();
        for (index, entry) in document.policies.into_iter().enumerate() {
            let field = format!("policies[{index}]");
            if !is_dns_label(&entry.name) {
                return Err(PolicyConfigError(format!(
                    "{field}.name {:?} must be a DNS label: lowercase letters, digits and '-'",
                    entry.name
                )));
            }
            if entry.name == LEGACY_GOVERNED_POLICY_NAME
                || entry.name == DEFAULT_CONFIRM_POLICY_NAME
            {
                return Err(PolicyConfigError(format!(
                    "{field}.name {:?} is reserved for the policy an unbound namespace resolves \
                     to; leave the namespace unbound instead",
                    entry.name
                )));
            }
            let mode = entry.mode.mode();
            // PROD-16.2: who signs the approval. `mode: two-person` says the
            // console; `approverSignature: Console` on a Governed policy says
            // the same thing in the internal words (what the chart renders).
            let approver_signature = match entry.approver_signature.as_deref() {
                None if entry.mode.implies_console() => ApproverSignature::Console,
                None => ApproverSignature::PersonalKey,
                Some(ApproverSignature::CONSOLE) if mode == ApprovalMode::Ordinary => {
                    return Err(PolicyConfigError(format!(
                        "{field}.approverSignature is Console on an Ordinary (confirm) policy, \
                         which has no approver; two-person approval in the console is mode: \
                         two-person (Governed with approverSignature: Console)"
                    )))
                }
                Some(ApproverSignature::CONSOLE) if entry.mode.says_personal_key() => {
                    return Err(PolicyConfigError(format!(
                        "{field}.approverSignature is Console and its mode is strict, which is a \
                         personal-key approval; say mode: two-person (or Governed) for approval \
                         in the console, or drop approverSignature for a personal key"
                    )))
                }
                Some(ApproverSignature::CONSOLE) => ApproverSignature::Console,
                Some(other) => {
                    return Err(PolicyConfigError(format!(
                        "{field}.approverSignature is {other:?}; it is Console (a second person \
                         approves in the console) or absent (a personal GovernedApproval key)"
                    )))
                }
            };
            if mode == ApprovalMode::Ordinary && !document.allow_ordinary_confirmation {
                return Err(PolicyConfigError(format!(
                    "{field} ({}) is Ordinary but allowOrdinaryConfirmation is not true; ordinary \
                     confirmation is an explicit installation decision (D0) and is never enabled \
                     by declaring a policy alone",
                    entry.name
                )));
            }
            let require_distinct_principal = match (mode, entry.require_distinct_principal) {
                (ApprovalMode::Governed, None | Some(true)) => true,
                (ApprovalMode::Governed, Some(false)) => {
                    return Err(PolicyConfigError(format!(
                        "{field}.requireDistinctPrincipal is false on a Governed policy; the \
                         supported baseline requires the approver's principal to differ from \
                         the requester's"
                    )))
                }
                (ApprovalMode::Ordinary, None | Some(false)) => false,
                (ApprovalMode::Ordinary, Some(true)) => {
                    return Err(PolicyConfigError(format!(
                        "{field}.requireDistinctPrincipal is true on an Ordinary policy, which has \
                         no approver to compare; use a Governed policy"
                    )))
                }
            };
            let max_age_seconds = entry.max_age_seconds.unwrap_or(match mode {
                ApprovalMode::Governed => DEFAULT_GOVERNED_MAX_AGE_SECONDS,
                ApprovalMode::Ordinary => DEFAULT_ORDINARY_MAX_AGE_SECONDS,
            });
            if !(MIN_MAX_AGE_SECONDS..=MAX_MAX_AGE_SECONDS).contains(&max_age_seconds) {
                return Err(PolicyConfigError(format!(
                    "{field}.maxAgeSeconds is {max_age_seconds}; it must be from \
                     {MIN_MAX_AGE_SECONDS} to {MAX_MAX_AGE_SECONDS}"
                )));
            }
            let policy = ApprovalPolicy {
                name: entry.name.clone(),
                mode,
                max_age_seconds,
                require_distinct_principal,
                approver_signature,
            };
            if policies.insert(entry.name.clone(), policy).is_some() {
                return Err(PolicyConfigError(format!(
                    "{field}.name {:?} is declared twice",
                    entry.name
                )));
            }
        }
        for (namespace, policy) in &document.namespaces {
            if !is_dns_label(namespace) {
                return Err(PolicyConfigError(format!(
                    "namespaces.{namespace:?} is not a namespace name"
                )));
            }
            if !policies.contains_key(policy) {
                return Err(PolicyConfigError(format!(
                    "namespaces.{namespace} names policy {policy:?}, which `policies` does not \
                     declare"
                )));
            }
        }
        Ok(Self {
            allow_ordinary_confirmation: document.allow_ordinary_confirmation,
            policies,
            bindings: document.namespaces,
            default_mode,
            installation: InstallationMarker::Unmarked,
        })
    }

    /// This set as a reader that observed `marker` sees it (PROD-16.1).
    ///
    /// The document is the same; only the unbound default can move, and only
    /// when the document sets no `defaultMode`.
    #[must_use]
    pub fn with_installation(mut self, marker: InstallationMarker) -> Self {
        self.installation = marker;
        self
    }

    /// The marker this set was given.
    #[must_use]
    pub fn installation(&self) -> InstallationMarker {
        self.installation
    }

    /// The explicit `defaultMode`, when the document sets one.
    #[must_use]
    pub fn default_mode(&self) -> Option<UnboundDefault> {
        self.default_mode
    }

    /// Why an UNBOUND namespace resolves the way it does — the order is the
    /// module header's: an explicit `defaultMode`, then the fresh-install
    /// marker, then `legacy-governed-v1`.
    #[must_use]
    pub fn unbound_basis(&self) -> UnboundBasis {
        match (self.default_mode, self.installation) {
            (Some(explicit), _) => UnboundBasis::Configured(explicit),
            (None, InstallationMarker::FreshInstallConfirm) => UnboundBasis::FreshInstall,
            (None, InstallationMarker::Unmarked) => UnboundBasis::Legacy,
        }
    }

    /// What an unbound namespace resolves to.
    #[must_use]
    pub fn unbound(&self) -> EffectivePolicy {
        match self.unbound_basis() {
            UnboundBasis::Configured(UnboundDefault::Confirm) | UnboundBasis::FreshInstall => {
                EffectivePolicy::Bound(default_confirm_policy())
            }
            UnboundBasis::Configured(UnboundDefault::Strict) | UnboundBasis::Legacy => {
                EffectivePolicy::Legacy
            }
        }
    }

    /// Whether `namespace` has an explicit binding.
    #[must_use]
    pub fn is_bound(&self, namespace: &str) -> bool {
        self.bindings.contains_key(namespace)
    }

    /// What `namespace` resolves to: its explicit binding — which always wins
    /// — or the unbound default ([`Self::unbound`]).
    #[must_use]
    pub fn resolve(&self, namespace: &str) -> EffectivePolicy {
        self.bindings
            .get(namespace)
            .and_then(|name| self.policies.get(name))
            .map_or_else(
                || self.unbound(),
                |policy| EffectivePolicy::Bound(policy.clone()),
            )
    }

    /// Whether the DOCUMENT configures anything at all (a policy, a binding or
    /// a `defaultMode`); the marker is not configuration.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.policies.is_empty() && self.bindings.is_empty() && self.default_mode.is_none()
    }

    /// The installation floor.
    #[must_use]
    pub fn allows_ordinary_confirmation(&self) -> bool {
        self.allow_ordinary_confirmation
    }

    /// The bound namespaces, sorted.
    #[must_use]
    pub fn bound_namespaces(&self) -> Vec<String> {
        self.bindings.keys().cloned().collect()
    }

    /// `sha256:<hex>` over the canonical form of the whole document — the
    /// readiness value D0 asks both processes to expose, so an operator can
    /// see that the console and the controller run the same configuration.
    ///
    /// PROD-16.1: it also covers what an UNBOUND namespace resolves to, when
    /// that is not `legacy-governed-v1` — so a console that has read the
    /// fresh-install marker and a controller that has not yet publish
    /// different digests, and the disagreement is visible. An installation
    /// whose unbound namespaces are legacy keeps the digest it always had.
    #[must_use]
    pub fn digest(&self) -> String {
        #[derive(Serialize)]
        #[serde(rename_all = "camelCase")]
        struct Canonical<'a> {
            allow_ordinary_confirmation: bool,
            policies: Vec<String>,
            namespaces: &'a BTreeMap<String, String>,
            #[serde(skip_serializing_if = "Option::is_none")]
            unbound_default: Option<String>,
        }
        let canonical = Canonical {
            allow_ordinary_confirmation: self.allow_ordinary_confirmation,
            policies: self.policies.values().map(ApprovalPolicy::digest).collect(),
            namespaces: &self.bindings,
            unbound_default: self.unbound().digest(),
        };
        sha256_prefixed(&serde_json::to_vec(&canonical).unwrap_or_default())
    }
}

// ---------------------------------------------------------------------------
// Authorization document v2 (D0)
// ---------------------------------------------------------------------------

/// The exact Kubernetes object a v2 document authorises.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct AuthorizedSubject {
    /// `logweir.dev/v1alpha1`.
    pub api_version: String,
    /// `Restore`.
    pub kind: String,
    /// The subject's namespace.
    pub namespace: String,
    /// The subject's name.
    pub name: String,
    /// The subject's immutable UID — what stops a document from following a
    /// delete/recreate of a same-named object.
    pub uid: String,
}

/// The authenticated requester the console attests.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct Requester {
    /// The identity issuer (an OIDC issuer, or `urn:logweir:local-admin`).
    pub issuer: String,
    /// The subject within that issuer.
    pub subject: String,
}

impl Requester {
    /// The stable principal id, `<issuer>#<subject>` — the same string
    /// `logweir-api`'s `Actor::id` produces and the form a `TrustPolicy`
    /// governed-approver key's `principal.id` must use for separation of duties
    /// to compare like with like.
    #[must_use]
    pub fn principal_id(&self) -> String {
        format!("{}#{}", self.issuer, self.subject)
    }
}

/// **PROD-16.2.** The second person of a console approval, as the console
/// authenticated them when they clicked Approve: the session's verified
/// issuer and subject, never a display name, an email, a group or a key id.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct Approver {
    /// The identity issuer (an OIDC issuer).
    pub issuer: String,
    /// The subject within that issuer.
    pub subject: String,
}

impl Approver {
    /// The stable principal id, `<issuer>#<subject>` — the form
    /// [`Requester::principal_id`] has.
    #[must_use]
    pub fn principal_id(&self) -> String {
        format!("{}#{}", self.issuer, self.subject)
    }
}

/// Which policy a document was issued under.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct PolicyRef {
    /// The policy name.
    pub name: String,
    /// [`ApprovalPolicy::digest`].
    pub digest: String,
}

/// **Authorization document v2** — what both modes sign.
///
/// `deny_unknown_fields`: a field this build does not know is a field whose
/// meaning it cannot enforce, and an authorization is the last place to ignore
/// one. A new field is a new `formatVersion`: PROD-15.1's `approvalSubject`
/// and `originalNameConfirmation` are format **2.1.0**
/// ([`RESTORE_AUTHORIZATION_FORMAT_VERSION_SUBJECT`]) — OPTIONAL, absent from
/// every document that does not need them (which stays 2.0.0, byte for byte),
/// refused by this build under a version that predates them
/// ([`RestoreAuthorization::check_format_version`]), and refused by every
/// reader built before them, which this attribute makes it do (an unknown
/// field). So the documents that carry them are exactly the ones an older
/// reader refuses, and no other document changes.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct RestoreAuthorization {
    /// [`RESTORE_AUTHORIZATION_FORMAT_VERSION`].
    pub format_version: String,
    /// [`RESTORE_AUTHORIZATION_KIND`].
    pub kind: String,
    /// The mode it was issued under. Must equal the bound policy's.
    pub authorization_mode: ApprovalMode,
    /// The exact subject.
    pub subject: AuthorizedSubject,
    /// `sha256:<hex>` of the subject's `spec.planBytes`.
    pub plan_hash: String,
    /// Who asked, as the console authenticated them.
    pub requester: Requester,
    /// The policy it was issued under.
    pub policy: PolicyRef,
    /// When the console signed it.
    pub issued_at: DateTime<Utc>,
    /// After this instant it authorises nothing new.
    pub expires_at: DateTime<Utc>,
    /// A change ticket: REQUIRED under `Governed`, optional under `Ordinary`
    /// (D0), and in both at most [`MAX_TICKET_LEN`] printable characters —
    /// [`check_ticket`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ticket: Option<String>,
    /// **PROD-15.1: the separate approval subject** — `originalName` for a
    /// restore under the ORIGINAL topic names, ABSENT for every other one
    /// (`crate::original_name::ApprovalSubject`). The console writes it only
    /// for a `Restore` that declares `spec.target.topicNaming.originalName`,
    /// and every boundary that reads the document holds it to the plan
    /// (`crate::original_name::check_approval_subject`).
    ///
    /// DEFINED FROM FORMAT 2.1.0. A document that carries it is written as
    /// 2.1.0 and refused under 2.0.0; every document without it stays 2.0.0,
    /// byte for byte what it was. This struct is `deny_unknown_fields`, so a
    /// reader that predates the key REFUSES the only documents that carry it
    /// (`DocumentInvalid`, unknown field) — it can never read one as an
    /// ordinary authorization.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub approval_subject: Option<String>,
    /// **OD-10 (2026-10-09): the typed confirmation.** On a one-person
    /// confirmation (`authorizationMode: Ordinary`) of a restore under the
    /// ORIGINAL topic names, the topic names the requester re-typed, as the
    /// console received them; ABSENT on every other document. Every boundary
    /// that reads the document holds them to the plan's `source.topics`
    /// (`crate::original_name::check_typed_confirmation`). Defined from format
    /// 2.1.0, as `approval_subject` is, and refused under 2.0.0.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub original_name_confirmation: Option<crate::original_name::OriginalNameConfirmation>,
    /// **PROD-16.2: the second person.** On an APPROVAL under a policy whose
    /// `approverSignature` is `Console`, the principal who signed in to the
    /// shared console and clicked Approve; ABSENT on every other document —
    /// the request such a policy's approver is shown, a one-person
    /// confirmation, a personal-key approval. The console writes it from the
    /// session it authenticated, after verifying its own signature on the
    /// stored request, and signs the whole document again with the same
    /// `ConsoleConfirmation` key.
    ///
    /// DEFINED FROM FORMAT 2.2.0, with [`Self::approved_at`]: written
    /// together, refused one without the other, refused under an older
    /// version, refused under a policy that is not `Console`
    /// ([`Self::check_approver_fields`], [`check_binding`]); refused as an
    /// unknown field by every reader built before it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub approver: Option<Approver>,
    /// **PROD-16.2: when the approver clicked**, on the console's clock — the
    /// clock that stamped `issuedAt`. Every reader requires
    /// `issuedAt <= approvedAt < expiresAt` ([`check_console_approval`]).
    /// Format 2.2.0, as `approver` is.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub approved_at: Option<DateTime<Utc>>,
}

impl RestoreAuthorization {
    /// The exact bytes the console signs and an `Approval` carries.
    #[must_use]
    pub fn to_bytes(&self) -> Vec<u8> {
        serde_json::to_vec(self).unwrap_or_default()
    }

    /// Parse signed bytes.
    ///
    /// # Errors
    ///
    /// [`AuthorizationRefusal::DocumentInvalid`] naming the parse error.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, AuthorizationRefusal> {
        let doc: Self = serde_json::from_slice(bytes).map_err(|e| {
            AuthorizationRefusal::DocumentInvalid(format!(
                "the bytes are not an authorization document v2: {e}"
            ))
        })?;
        // PROD-15.1: the 2.1.0 fields under a version that predates them are
        // refused at EVERY reader, here, before anything reads the fields.
        doc.check_subject_fields_version()?;
        // PROD-16.2: and the 2.2.0 fields, the same way.
        doc.check_approver_fields()?;
        Ok(doc)
    }

    /// Whether the document carries a field defined from format 2.2.0.
    #[must_use]
    pub fn carries_approver_fields(&self) -> bool {
        self.approver.is_some() || self.approved_at.is_some()
    }

    /// **PROD-16.2: `approver` and `approvedAt` are format 2.2.0, and come
    /// together.** A document that carries either under an older minor (or
    /// under a version this rule cannot read: three numeric parts,
    /// [`numeric_version`]) is refused: a 2.0.0 or 2.1.0 document says who
    /// ASKED and nothing about who approved, so one that names an approver
    /// under that version was not written by a console that knows the field.
    /// One of the two without the other is refused whatever the version: an
    /// approver with no instant, or an instant with nobody.
    ///
    /// Can only refuse; a document without the fields is decided exactly as
    /// before.
    ///
    /// # Errors
    ///
    /// [`AuthorizationRefusal::DocumentInvalid`], naming both versions.
    pub fn check_approver_fields(&self) -> Result<(), AuthorizationRefusal> {
        if !self.carries_approver_fields() {
            return Ok(());
        }
        if !numeric_version(&self.format_version).is_some_and(|(major, minor, _)| {
            major == 2 && minor >= RESTORE_AUTHORIZATION_APPROVER_SINCE_MINOR
        }) {
            return Err(AuthorizationRefusal::DocumentInvalid(format!(
                "the document carries `approver` or `approvedAt`, which are defined from \
                 formatVersion {RESTORE_AUTHORIZATION_FORMAT_VERSION_APPROVER}, and it declares \
                 {:?}; a document that predates the fields cannot carry them",
                self.format_version
            )));
        }
        if self.approver.is_none() || self.approved_at.is_none() {
            return Err(AuthorizationRefusal::DocumentInvalid(
                "the document carries one of `approver` and `approvedAt` without the other; a \
                 console approval names who approved AND when"
                    .to_string(),
            ));
        }
        Ok(())
    }

    /// Whether the document carries a field defined from format 2.1.0.
    #[must_use]
    pub fn carries_subject_fields(&self) -> bool {
        self.approval_subject.is_some() || self.original_name_confirmation.is_some()
    }

    /// **PROD-15.1: `approvalSubject` and `originalNameConfirmation` are
    /// format 2.1.0.** A document that carries either and declares an older
    /// minor (or none this build can read) is refused: a 2.0.0 document
    /// authorises an ordinary restore and nothing else, so one that names the
    /// original-name subject under that version was not written by a console
    /// that knows the subject. Can only refuse; a document without the fields
    /// is decided exactly as before.
    ///
    /// **The version is THREE NUMERIC PARTS** (review 2, L8): `2.<minor>.<patch>`,
    /// each part decimal digits with no sign, no padding and nothing after
    /// it ([`numeric_version`]). `2.1`, `2.01.0`, `2.+1.0`, `2.1.x`,
    /// `2.1.0-rc1` and `2.1.0.0` are not versions this rule reads, and a
    /// document carrying the fields under one is refused.
    ///
    /// # Errors
    ///
    /// [`AuthorizationRefusal::DocumentInvalid`], naming both versions.
    pub fn check_subject_fields_version(&self) -> Result<(), AuthorizationRefusal> {
        if !self.carries_subject_fields() {
            return Ok(());
        }
        if numeric_version(&self.format_version).is_some_and(|(major, minor, _)| {
            major == 2 && minor >= RESTORE_AUTHORIZATION_SUBJECT_SINCE_MINOR
        }) {
            return Ok(());
        }
        Err(AuthorizationRefusal::DocumentInvalid(format!(
            "the document carries `approvalSubject` or `originalNameConfirmation`, which are \
             defined from formatVersion {RESTORE_AUTHORIZATION_FORMAT_VERSION_SUBJECT}, and it \
             declares {:?}; a document that predates the fields cannot carry them",
            self.format_version
        )))
    }
}

/// `major.minor.patch` as three numbers, or `None`: exactly three parts, each
/// one or more ASCII digits with no leading zero (`0` itself aside), no sign
/// and no suffix. `u64::from_str` alone accepts `+1`, and a `split` that
/// reads two parts accepts anything after them.
#[must_use]
pub fn numeric_version(version: &str) -> Option<(u64, u64, u64)> {
    let mut parts = version.split('.');
    let mut next = || -> Option<u64> {
        let part = parts.next()?;
        let canonical = !part.is_empty()
            && part.bytes().all(|b| b.is_ascii_digit())
            && (part == "0" || !part.starts_with('0'));
        canonical.then(|| part.parse::<u64>().ok()).flatten()
    };
    let version = (next()?, next()?, next()?);
    parts.next().is_none().then_some(version)
}

/// What a boundary knows about the subject independently of the document.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ExpectedSubject {
    /// The subject's namespace.
    pub namespace: String,
    /// The subject's name.
    pub name: String,
    /// The subject's UID.
    pub uid: String,
    /// `sha256:<hex>` recomputed from the subject's own plan bytes.
    pub plan_hash: String,
}

/// Why a v2 document does not authorise this subject under this policy.
///
/// The variants are refusal CLASSES; each boundary maps them onto its own
/// closed reason set, and [`Self::reason`] is the shared spelling.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AuthorizationRefusal {
    /// Not a v2 document, the wrong kind, or a blank requester.
    DocumentInvalid(String),
    /// The document names another object.
    SubjectMismatch(String),
    /// The document names another plan.
    PlanHashMismatch {
        /// The hash inside the signed document.
        got: String,
        /// The hash recomputed from the subject.
        want: String,
    },
    /// The document names another policy, another digest, or another mode
    /// than the namespace is bound to — including a v2 document in a namespace
    /// with no binding.
    PolicyMismatch(String),
    /// The validity window is not well formed or exceeds the policy.
    WindowInvalid(String),
    /// The window has closed.
    Expired(String),
    /// **PROD-16.2.** The policy takes its approval from the console and the
    /// document names no approver: it is the REQUEST, and authorises
    /// nothing. The pending state of a two-person request — and the state a
    /// request written straight to the API server as an Approval stays in
    /// for ever.
    ApprovalRequired(String),
    /// **PROD-16.2.** The document's approver is not a second person: the
    /// requester, a principal that cannot be compared with the requester, the
    /// local administrator, a service account, or a principal of another
    /// issuer.
    SelfApproval(String),
}

impl AuthorizationRefusal {
    /// The condition reason every boundary writes for this class.
    #[must_use]
    pub const fn reason(&self) -> &'static str {
        match self {
            Self::DocumentInvalid(_) => "AuthorizationDocumentInvalid",
            Self::SubjectMismatch(_) => "AuthorizationSubjectMismatch",
            Self::PlanHashMismatch { .. } => "PlanHashMismatch",
            Self::PolicyMismatch(_) => "ApprovalPolicyMismatch",
            Self::WindowInvalid(_) => "AuthorizationWindowInvalid",
            Self::Expired(_) => "AuthorizationExpired",
            // The two reasons the Approval controller already writes for a
            // personal-key policy's pending request and for self-approval.
            Self::ApprovalRequired(_) => "GovernedApprovalRequired",
            Self::SelfApproval(_) => "SelfApprovalRefused",
        }
    }
}

impl std::fmt::Display for AuthorizationRefusal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::DocumentInvalid(d)
            | Self::SubjectMismatch(d)
            | Self::PolicyMismatch(d)
            | Self::WindowInvalid(d)
            | Self::Expired(d)
            | Self::ApprovalRequired(d)
            | Self::SelfApproval(d) => f.write_str(d),
            Self::PlanHashMismatch { got, want } => write!(
                f,
                "the authorization document names plan hash {got} but the subject's \
                 spec.planBytes hash to {want}; a changed plan needs a new confirmation"
            ),
        }
    }
}

/// The refusal a v2 document earns in a namespace with no binding.
#[must_use]
pub fn unbound_namespace_refusal(namespace: &str) -> AuthorizationRefusal {
    AuthorizationRefusal::PolicyMismatch(format!(
        "namespace {namespace} is bound to no approval policy, so it resolves to \
         {LEGACY_GOVERNED_POLICY_NAME}, which accepts a v1 approval document signed by a \
         GovernedApproval key and no authorization document v2"
    ))
}

/// The refusal a v1 approval document earns under an explicit binding.
#[must_use]
pub fn v1_under_bound_policy_refusal(policy: &ApprovalPolicy) -> AuthorizationRefusal {
    AuthorizationRefusal::PolicyMismatch(format!(
        "this namespace is bound to approval policy {} ({}), which accepts authorization \
         document v2 only; a v1 approval document carries no console-attested requester, so \
         neither the policy nor separation of duties can be checked against it",
        policy.name, policy.mode
    ))
}

/// Everything about a v2 document that does NOT depend on the clock: format,
/// subject, plan and policy — and, under a policy whose approval the console
/// signs (PROD-16.2), the second person ([`check_console_approval`]). The
/// runner, which admits a run the controller already admitted, calls this and
/// [`check_window_shape`]; the controller calls
/// [`check_restore_authorization`], which adds the clock.
///
/// **This is the question "does this document AUTHORISE this subject under
/// this policy".** Under a `Console` policy the request the approver is shown
/// does not, and is refused here ([`AuthorizationRefusal::ApprovalRequired`]);
/// the console, which must read that request before it signs the approval,
/// asks [`check_request_binding`] instead.
///
/// # Errors
///
/// The first [`AuthorizationRefusal`] in the order: document, subject, plan,
/// policy, requester, then the console approval.
pub fn check_binding(
    doc: &RestoreAuthorization,
    expected: &ExpectedSubject,
    policy: &ApprovalPolicy,
) -> Result<(), AuthorizationRefusal> {
    check_document_binding(doc, expected, policy)?;
    check_console_approval(doc, policy, None)
}

/// [`check_binding`] without the console approval: the document's own format
/// rules and its binding to the subject, the plan and the policy. What a
/// REQUEST and an approval have in common.
///
/// PROD-16.2 adds two rules here, both of which can only refuse: `approver`
/// and `approvedAt` obey their version rule
/// ([`RestoreAuthorization::check_approver_fields`]), and they belong to a
/// policy whose `approverSignature` is `Console` and to no other — so a
/// console-approved document is never read as anything under a personal-key
/// or a one-person policy, even one whose digest it names.
fn check_document_binding(
    doc: &RestoreAuthorization,
    expected: &ExpectedSubject,
    policy: &ApprovalPolicy,
) -> Result<(), AuthorizationRefusal> {
    if doc.format_version.split('.').next() != Some("2") || doc.kind != RESTORE_AUTHORIZATION_KIND {
        return Err(AuthorizationRefusal::DocumentInvalid(format!(
            "the document declares formatVersion {:?} and kind {:?}; this build reads major 2 and \
             kind {RESTORE_AUTHORIZATION_KIND:?}",
            doc.format_version, doc.kind
        )));
    }
    // PROD-15.1: the 2.1.0 fields under 2.0.0 are refused (`from_bytes` did
    // this for a parsed document; this is the same rule for one built in
    // memory, so no reader can skip it).
    doc.check_subject_fields_version()?;
    // PROD-16.2: and the 2.2.0 fields under 2.0.0 or 2.1.0, the same way.
    doc.check_approver_fields()?;
    let subject = &doc.subject;
    let mismatch = if subject.api_version != SUBJECT_API_VERSION {
        Some(format!("apiVersion {}", subject.api_version))
    } else if subject.kind != SUBJECT_KIND_RESTORE {
        Some(format!("kind {}", subject.kind))
    } else if subject.namespace != expected.namespace {
        Some(format!("namespace {}", subject.namespace))
    } else if subject.name != expected.name {
        Some(format!("name {}", subject.name))
    } else if subject.uid != expected.uid {
        Some(format!("uid {}", subject.uid))
    } else {
        None
    };
    if let Some(what) = mismatch {
        return Err(AuthorizationRefusal::SubjectMismatch(format!(
            "the authorization document names {what}, but the subject is Restore {}/{} UID {}; a \
             document authorises exactly one object",
            expected.namespace, expected.name, expected.uid
        )));
    }
    if doc.plan_hash != expected.plan_hash {
        return Err(AuthorizationRefusal::PlanHashMismatch {
            got: doc.plan_hash.clone(),
            want: expected.plan_hash.clone(),
        });
    }
    let digest = policy.digest();
    if doc.policy.name != policy.name
        || doc.policy.digest != digest
        || doc.authorization_mode != policy.mode
    {
        return Err(AuthorizationRefusal::PolicyMismatch(format!(
            "the authorization document was issued under policy {} ({}, {}), but this namespace \
             is bound to policy {} ({}, {}); a policy change requires a new confirmation",
            doc.policy.name,
            doc.authorization_mode,
            doc.policy.digest,
            policy.name,
            policy.mode,
            digest
        )));
    }
    if doc.requester.issuer.trim().is_empty() || doc.requester.subject.trim().is_empty() {
        return Err(AuthorizationRefusal::DocumentInvalid(
            "the authorization document names no requester issuer and subject; the console \
             attests who asked, and a document that names nobody attests nothing"
                .to_string(),
        ));
    }
    // PROD-15.1: a subject this build does not know is refused here, at every
    // boundary; whether it is the PLAN's subject is the caller's comparison
    // (`crate::original_name::check_approval_subject`), because the plan is
    // not in this function's hands.
    let subject = crate::original_name::ApprovalSubject::from_wire(doc.approval_subject.as_deref())
        .map_err(AuthorizationRefusal::DocumentInvalid)?;
    // OD-10: typed names belong ONLY to a one-person confirmation of an
    // original-name restore. Whether they are the plan's topics is, again,
    // the caller's comparison; their presence anywhere else is refused here.
    if doc.original_name_confirmation.is_some()
        && !(subject == crate::original_name::ApprovalSubject::OriginalName
            && doc.authorization_mode == ApprovalMode::Ordinary)
    {
        return Err(AuthorizationRefusal::DocumentInvalid(format!(
            "{}: the document carries typed topic names, which only a one-person confirmation \
             (Ordinary) of a restore under the original topic names carries",
            crate::original_name::ORIGINAL_NAME_CONFIRMATION_NOT_ACCEPTED
        )));
    }
    check_ticket(doc.authorization_mode, doc.ticket.as_deref())
        .map_err(AuthorizationRefusal::DocumentInvalid)?;
    // PROD-16.2: a console approver belongs to a `Console` policy and to no
    // other. AFTER the policy comparison above, so a document for another
    // policy is named as that; this arm is what is left when the digest
    // matches and the fields are there all the same.
    if doc.carries_approver_fields() && policy.approver_signature != ApproverSignature::Console {
        return Err(AuthorizationRefusal::PolicyMismatch(format!(
            "the authorization document names a console approver, and policy {} ({}) takes its \
             approval from {}; a console approval is accepted only under a policy whose \
             approverSignature is Console",
            policy.name,
            policy.mode,
            policy.approver_signature.as_str()
        )));
    }
    Ok(())
}

/// **PROD-16.2: what the console reads BEFORE it signs an approval** — a
/// stored REQUEST under a policy whose approval the console signs, checked at
/// `now`.
///
/// The caller has already verified its own `ConsoleConfirmation` signature
/// over the exact bytes `doc` was parsed from; everything the approval will
/// carry is then copied from `doc`, so this is where a signature lifted from
/// ANOTHER request is refused: another namespace, name or UID, another plan,
/// another policy or digest, an expired or malformed window. And a request is
/// not an approval: one that already names an approver is refused, so a
/// stored approval can never be presented for a second one.
///
/// # Errors
///
/// The first [`AuthorizationRefusal`].
pub fn check_request_binding(
    doc: &RestoreAuthorization,
    expected: &ExpectedSubject,
    policy: &ApprovalPolicy,
    now: DateTime<Utc>,
) -> Result<(), AuthorizationRefusal> {
    if policy.approver_signature != ApproverSignature::Console {
        return Err(AuthorizationRefusal::PolicyMismatch(format!(
            "policy {} ({}) takes its approval from {}; there is no request to approve in the \
             console under it",
            policy.name,
            policy.mode,
            policy.approver_signature.as_str()
        )));
    }
    if doc.carries_approver_fields() {
        return Err(AuthorizationRefusal::DocumentInvalid(
            "the stored request already names an approver; a request is what the console signed \
             for the requester, and an approval is never approved again"
                .to_string(),
        ));
    }
    check_document_binding(doc, expected, policy)?;
    check_window_shape(doc, policy)?;
    check_clock(doc, now)
}

/// The identity a console approval compares: the issuer and the subject, as
/// the console attested them.
fn principal_words(issuer: &str, subject: &str) -> String {
    format!("{issuer:?}#{subject:?}")
}

/// Whether every character is visible ASCII (0x21 to 0x7E): no space, no
/// control character, no code point outside ASCII.
fn is_visible_ascii(text: &str) -> bool {
    !text.is_empty() && text.bytes().all(|b| (0x21..=0x7e).contains(&b))
}

/// **PROD-16.2: whether a principal can take part in a console approval at
/// all** — on either side.
///
/// 1. **Comparable form.** Issuer and subject are each non-empty and visible
///    ASCII only; the issuer carries no `#` (the principal id is
///    `<issuer>#<subject>`, and OpenID Connect allows an issuer no fragment);
///    the subject is at most [`MAX_COMPARABLE_SUBJECT_LEN`] characters
///    (OpenID Connect Core: `sub` "MUST NOT exceed 255 ASCII characters"). A
///    conforming provider always passes. Anything else is NOT COMPARABLE: two
///    subjects that differ only by whitespace, or only by Unicode
///    normalisation, cannot both be inside this rule, so neither can be
///    passed off as a second person.
/// 2. **A person.** Not the local administrator ([`LOCAL_ADMIN_ISSUER`],
///    compared without case): its one identity cannot be two people. Not a
///    Kubernetes machine identity ([`KUBERNETES_SYSTEM_SUBJECT_PREFIX`],
///    without case): a service account is not a requester a human can be
///    told apart from.
///
/// # Errors
///
/// A sentence naming what is wrong, for the refusal that carries it.
pub fn console_principal(role: &str, issuer: &str, subject: &str) -> Result<(), String> {
    let shown = principal_words(issuer, subject);
    if !is_visible_ascii(issuer)
        || !is_visible_ascii(subject)
        || issuer.contains('#')
        || issuer.len() > MAX_COMPARABLE_ISSUER_LEN
        || subject.len() > MAX_COMPARABLE_SUBJECT_LEN
    {
        return Err(format!(
            "the {role} {shown} is not in a form that can be compared: a console approval \
             compares an issuer and a subject made of visible ASCII characters only (no space, \
             no control character, nothing outside ASCII; the issuer without `#`, the subject at \
             most {MAX_COMPARABLE_SUBJECT_LEN} characters, as OpenID Connect defines `sub`), and \
             an identity it cannot compare cannot be shown to be a second person"
        ));
    }
    if fold_issuer(issuer) == LOCAL_ADMIN_ISSUER {
        return Err(format!(
            "the {role} {shown} is the in-cluster administrator console's one identity, which \
             cannot be one of two people; a console approval needs two people the same identity \
             provider vouches for"
        ));
    }
    if subject
        .to_ascii_lowercase()
        .starts_with(KUBERNETES_SYSTEM_SUBJECT_PREFIX)
    {
        return Err(format!(
            "the {role} {shown} is a Kubernetes system identity (a service account, a node or a \
             system user), not a person; a console approval is between two people"
        ));
    }
    Ok(())
}

/// An issuer as a console approval compares it: ASCII lower case, with
/// trailing `/` removed — `https://idp.example/` and `https://IDP.example`
/// are one identity provider here. FOLDING CAN ONLY MAKE TWO PRINCIPALS THE
/// SAME, never different, and "the same" is the side that refuses.
#[must_use]
pub fn fold_issuer(issuer: &str) -> String {
    issuer.trim_end_matches('/').to_ascii_lowercase()
}

/// A subject as a console approval compares it: ASCII lower case. OpenID
/// Connect says `sub` is case sensitive; two subjects that differ only by
/// case are refused as one person all the same, which is the safe direction.
#[must_use]
pub fn fold_subject(subject: &str) -> String {
    subject.to_ascii_lowercase()
}

/// **PROD-16.2 — SEPARATION OF DUTIES FOR A CONSOLE APPROVAL: THE ONE RULE
/// the console (before it signs), the Approval controller, Restore admission
/// and the runner all apply**, over the requester the console attested and
/// the approver it attested.
///
/// 1. Each is a principal a console approval can compare, and a person
///    ([`console_principal`]).
/// 2. **The same issuer** ([`fold_issuer`]). Two issuers are two namespaces
///    of subjects; whether `alice` at one is `alice` at the other cannot be
///    known from here, so a principal of another issuer never establishes a
///    second person.
/// 3. **Another subject** ([`fold_subject`]).
///
/// It compares the issuer and the subject and nothing else: never a display
/// name, an email claim, a group, a role or a key id. Two sessions of one
/// user are one issuer and one subject, so they are one person. Every arm can
/// only refuse.
///
/// # Errors
///
/// [`AuthorizationRefusal::SelfApproval`], naming both principals.
pub fn console_separation(
    requester: &Requester,
    approver: &Approver,
) -> Result<(), AuthorizationRefusal> {
    console_principal("requester", &requester.issuer, &requester.subject)
        .map_err(AuthorizationRefusal::SelfApproval)?;
    console_principal("approver", &approver.issuer, &approver.subject)
        .map_err(AuthorizationRefusal::SelfApproval)?;
    let requester_words = principal_words(&requester.issuer, &requester.subject);
    let approver_words = principal_words(&approver.issuer, &approver.subject);
    if fold_issuer(&requester.issuer) != fold_issuer(&approver.issuer) {
        return Err(AuthorizationRefusal::SelfApproval(format!(
            "the approver {approver_words} and the requester {requester_words} come from two \
             issuers; whether a subject of one is a subject of the other cannot be known, so a \
             principal of another issuer is never a second person"
        )));
    }
    if fold_subject(&requester.subject) == fold_subject(&approver.subject) {
        return Err(AuthorizationRefusal::SelfApproval(format!(
            "the approver {approver_words} is the requester {requester_words} (the issuer and \
             the subject are compared without case); a two-person approval needs a second \
             person, and no role changes that"
        )));
    }
    Ok(())
}

/// **PROD-16.2 — the console approval, as every reader judges it** on top of
/// the binding: who approved, and when.
///
/// * A policy that takes its approval from a PERSONAL key: nothing to judge
///   here (`Ok`). Its documents carry no approver —
///   [`check_document_binding`] refused one that does — and its second
///   signature is the caller's to verify, exactly as before.
/// * A `Console` policy and a document with NO approver: the request, which
///   authorises nothing ([`AuthorizationRefusal::ApprovalRequired`]).
/// * A `Console` policy and an approver: [`console_separation`] between the
///   requester and the approver, and **the clock rule**: `issuedAt <=
///   approvedAt < expiresAt`, exactly, with no tolerance — the three instants
///   are inside the same signed bytes and were stamped by the same clock (the
///   console's), so they are compared with each other and not with the
///   reader's. A reader that has a clock passes it (`now`): an `approvedAt`
///   more than [`MAX_ISSUED_AT_SKEW_SECONDS`] ahead of it is refused, the
///   bound `issuedAt` has. The runner passes `None`: it reads no clock for a
///   run the controller admitted.
///
/// # Errors
///
/// `ApprovalRequired`, `SelfApproval`, `WindowInvalid` or `DocumentInvalid`.
pub fn check_console_approval(
    doc: &RestoreAuthorization,
    policy: &ApprovalPolicy,
    now: Option<DateTime<Utc>>,
) -> Result<(), AuthorizationRefusal> {
    if policy.approver_signature != ApproverSignature::Console {
        return Ok(());
    }
    let (approver, approved_at) = match (doc.approver.as_ref(), doc.approved_at) {
        (Some(approver), Some(approved_at)) => (approver, approved_at),
        (None, None) => {
            return Err(AuthorizationRefusal::ApprovalRequired(format!(
                "the console confirmed requester {} under policy {}, which takes its approval \
                 from the console, and nobody has approved: a second person signs in to the \
                 console and clicks Approve. A personal-key countersignature is not accepted \
                 under this policy. This document authorises nothing",
                doc.requester.principal_id(),
                policy.name
            )))
        }
        _ => {
            return Err(AuthorizationRefusal::DocumentInvalid(
                "the document carries one of `approver` and `approvedAt` without the other; a \
                 console approval names who approved AND when"
                    .to_string(),
            ))
        }
    };
    console_separation(&doc.requester, approver)?;
    if approved_at < doc.issued_at || approved_at >= doc.expires_at {
        return Err(AuthorizationRefusal::WindowInvalid(format!(
            "the approval was given at {}, outside the request's own window {}..{}; an approval \
             is given after the request was made and before it expires",
            approved_at.to_rfc3339(),
            doc.issued_at.to_rfc3339(),
            doc.expires_at.to_rfc3339()
        )));
    }
    if now.is_some_and(|now| {
        approved_at > now + chrono::Duration::seconds(MAX_ISSUED_AT_SKEW_SECONDS)
    }) {
        return Err(AuthorizationRefusal::WindowInvalid(format!(
            "the approval was given at {}, more than {MAX_ISSUED_AT_SKEW_SECONDS}s ahead of this \
             verifier's clock",
            approved_at.to_rfc3339(),
        )));
    }
    Ok(())
}

/// The change ticket's rule (D0: "ticket (required in Governed, optional in
/// Ordinary)"): under `Governed` a non-blank ticket of at most
/// [`MAX_TICKET_LEN`] characters; under `Ordinary` absent or the same shape.
///
/// # Errors
///
/// A sentence naming what is wrong.
pub fn check_ticket(mode: ApprovalMode, ticket: Option<&str>) -> Result<(), String> {
    match ticket {
        None if mode == ApprovalMode::Governed => Err(
            "a Governed authorization carries a change ticket (D0: required in Governed); \
             this document names none"
                .to_string(),
        ),
        None => Ok(()),
        Some(t) if t.trim().is_empty() || t.trim() != t => Err(format!(
            "the change ticket {t:?} is blank or carries surrounding whitespace"
        )),
        Some(t) if t.chars().count() > MAX_TICKET_LEN || t.chars().any(char::is_control) => Err(
            format!("the change ticket is at most {MAX_TICKET_LEN} printable characters"),
        ),
        Some(_) => Ok(()),
    }
}

/// The window's SHAPE, with no clock: positive and no longer than the policy
/// allows.
///
/// # Errors
///
/// [`AuthorizationRefusal::WindowInvalid`].
pub fn check_window_shape(
    doc: &RestoreAuthorization,
    policy: &ApprovalPolicy,
) -> Result<(), AuthorizationRefusal> {
    let lifetime = (doc.expires_at - doc.issued_at).num_seconds();
    if doc.expires_at <= doc.issued_at || lifetime > policy.max_age_seconds {
        return Err(AuthorizationRefusal::WindowInvalid(format!(
            "the authorization window {}..{} is {lifetime}s; policy {} allows a positive window of \
             at most {}s",
            doc.issued_at.to_rfc3339(),
            doc.expires_at.to_rfc3339(),
            policy.name,
            policy.max_age_seconds
        )));
    }
    Ok(())
}

/// The whole non-cryptographic verdict at `now`: the binding
/// ([`check_binding`]'s document, subject, plan and policy), then
/// [`check_window_shape`], then the clock, then — under a policy whose
/// approval the console signs — the second person and the instant they
/// approved ([`check_console_approval`], PROD-16.2).
///
/// # Errors
///
/// The first [`AuthorizationRefusal`].
pub fn check_restore_authorization(
    doc: &RestoreAuthorization,
    expected: &ExpectedSubject,
    policy: &ApprovalPolicy,
    now: DateTime<Utc>,
) -> Result<(), AuthorizationRefusal> {
    check_document_binding(doc, expected, policy)?;
    check_window_shape(doc, policy)?;
    check_clock(doc, now)?;
    // PROD-16.2: the second person, last — so an expired request reads as
    // expired, as a personal-key policy's does, and not as merely pending.
    check_console_approval(doc, policy, Some(now))
}

/// The clock half of [`check_restore_authorization`]: `issuedAt` not ahead of
/// `now` beyond the skew bound, `expiresAt` not passed.
fn check_clock(doc: &RestoreAuthorization, now: DateTime<Utc>) -> Result<(), AuthorizationRefusal> {
    // NO CLOCK IN EITHER MESSAGE (defect P9, poc-install 2026-09-24). The
    // `Approval` controller writes this text into a condition and skips the
    // write only when the status is byte-for-byte unchanged, so a message that
    // named `now` was a different status on every pass: each pass wrote, the
    // write woke the controller's own watch, and one expired `Approval` logged
    // `approval refused` every ~2 s for ever. The boundary that failed is the
    // stable fact; the instant it was judged is the condition's
    // `lastTransitionTime`.
    if doc.issued_at > now + chrono::Duration::seconds(MAX_ISSUED_AT_SKEW_SECONDS) {
        return Err(AuthorizationRefusal::WindowInvalid(format!(
            "the authorization document was issued at {}, more than {MAX_ISSUED_AT_SKEW_SECONDS}s \
             ahead of this verifier's clock",
            doc.issued_at.to_rfc3339(),
        )));
    }
    if doc.expires_at <= now {
        return Err(AuthorizationRefusal::Expired(format!(
            "the authorization expired at {}; an expired confirmation or approval authorises \
             nothing new — create a new Restore",
            doc.expires_at.to_rfc3339(),
        )));
    }
    Ok(())
}

/// Whether a key's `principal.id` is in the `<issuer>#<subject>` form the
/// console attests a requester in — the only form separation of duties can
/// compare (review M4). Anything else — an email, a display name, an
/// `install:` digest, surrounding whitespace — is a principal that could be
/// the requester under another spelling, so it cannot establish separation.
#[must_use]
pub fn is_issuer_subject_principal(principal_id: &str) -> bool {
    principal_id.trim() == principal_id
        && principal_id
            .split_once('#')
            .is_some_and(|(issuer, subject)| !issuer.is_empty() && !subject.is_empty())
}

/// Separation of duties (D0): the governed approver's principal must not be
/// the requester's. Compares the stable `principal_id` strings exactly —
/// never display names, and never key ids — and FAILS CLOSED on an approver
/// principal that is not `<issuer>#<subject>` ([`is_issuer_subject_principal`]):
/// `alice@example.com` differs from `https://idp#alice` as a string and may
/// still be Alice.
#[must_use]
pub fn separation_holds(requester: &Requester, approver_principal_id: &str) -> bool {
    is_issuer_subject_principal(approver_principal_id)
        && approver_principal_id != requester.principal_id()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn at(text: &str) -> DateTime<Utc> {
        DateTime::parse_from_rfc3339(text)
            .map(|t| t.with_timezone(&Utc))
            .unwrap_or_default()
    }

    const DOC: &str = "allowOrdinaryConfirmation: true
policies:
  - name: team-ordinary
    mode: Ordinary
  - name: prod-governed
    mode: Governed
    maxAgeSeconds: 3600
namespaces:
  team-a: team-ordinary
  prod: prod-governed
";

    fn set() -> ApprovalPolicySet {
        ApprovalPolicySet::parse(DOC).unwrap_or_default()
    }

    fn policy(mode: ApprovalMode) -> ApprovalPolicy {
        let name = match mode {
            ApprovalMode::Ordinary => "team-ordinary",
            ApprovalMode::Governed => "prod-governed",
        };
        set()
            .resolve(match mode {
                ApprovalMode::Ordinary => "team-a",
                ApprovalMode::Governed => "prod",
            })
            .bound()
            .cloned()
            .unwrap_or(ApprovalPolicy {
                name: name.into(),
                mode,
                max_age_seconds: 0,
                require_distinct_principal: false,
                approver_signature: ApproverSignature::PersonalKey,
            })
    }

    fn expected() -> ExpectedSubject {
        ExpectedSubject {
            namespace: "team-a".into(),
            name: "rst-1".into(),
            uid: "uid-1".into(),
            plan_hash: format!("sha256:{}", "a".repeat(64)),
        }
    }

    fn doc(policy: &ApprovalPolicy) -> RestoreAuthorization {
        RestoreAuthorization {
            format_version: RESTORE_AUTHORIZATION_FORMAT_VERSION.into(),
            kind: RESTORE_AUTHORIZATION_KIND.into(),
            authorization_mode: policy.mode,
            subject: AuthorizedSubject {
                api_version: SUBJECT_API_VERSION.into(),
                kind: SUBJECT_KIND_RESTORE.into(),
                namespace: "team-a".into(),
                name: "rst-1".into(),
                uid: "uid-1".into(),
            },
            plan_hash: format!("sha256:{}", "a".repeat(64)),
            requester: Requester {
                issuer: "https://idp.example".into(),
                subject: "alice".into(),
            },
            policy: PolicyRef {
                name: policy.name.clone(),
                digest: policy.digest(),
            },
            issued_at: at("2026-09-22T10:00:00Z"),
            expires_at: at("2026-09-22T10:10:00Z"),
            ticket: (policy.mode == ApprovalMode::Governed).then(|| "CHG-1".to_string()),
            approval_subject: None,
            original_name_confirmation: None,
            approver: None,
            approved_at: None,
        }
    }

    #[test]
    fn an_empty_document_is_the_legacy_installation() {
        let empty = ApprovalPolicySet::parse("").unwrap_or_else(|e| panic!("{e}"));
        assert!(empty.is_empty());
        assert_eq!(empty.resolve("anything"), EffectivePolicy::Legacy);
        assert_eq!(empty, ApprovalPolicySet::default());
    }

    #[test]
    fn a_namespace_resolves_to_its_binding_and_an_unbound_one_to_legacy() {
        let s = set();
        assert_eq!(s.resolve("team-a").mode(), ApprovalMode::Ordinary);
        assert_eq!(s.resolve("prod").mode(), ApprovalMode::Governed);
        assert_eq!(s.resolve("elsewhere"), EffectivePolicy::Legacy);
        assert_eq!(s.resolve("elsewhere").mode(), ApprovalMode::Governed);
        assert_eq!(s.resolve("elsewhere").name(), LEGACY_GOVERNED_POLICY_NAME);
        assert_eq!(s.resolve("elsewhere").digest(), None);
    }

    #[test]
    fn defaults_are_per_mode_and_governed_requires_distinct_principals() {
        let s = set();
        let ordinary = s
            .resolve("team-a")
            .bound()
            .cloned()
            .unwrap_or_else(|| panic!());
        assert_eq!(ordinary.max_age_seconds, DEFAULT_ORDINARY_MAX_AGE_SECONDS);
        assert!(!ordinary.require_distinct_principal);
        let governed = s
            .resolve("prod")
            .bound()
            .cloned()
            .unwrap_or_else(|| panic!());
        assert_eq!(governed.max_age_seconds, 3600);
        assert!(governed.require_distinct_principal);
    }

    #[test]
    fn ordinary_without_the_installation_floor_is_refused_not_demoted() {
        let err = ApprovalPolicySet::parse(&DOC.replace(
            "allowOrdinaryConfirmation: true",
            "allowOrdinaryConfirmation: false",
        ))
        .err()
        .unwrap_or_else(|| panic!("an Ordinary policy without the floor must be refused"));
        assert!(err.0.contains("allowOrdinaryConfirmation"), "{err}");
        let absent =
            ApprovalPolicySet::parse(&DOC.replace("allowOrdinaryConfirmation: true\n", ""));
        assert!(absent.is_err(), "absent floor is false");
    }

    #[test]
    fn every_malformed_document_is_refused_by_field() {
        for (text, needle) in [
            ("bogus: 1\n", "unknown field"),
            (
                "policies:\n  - name: legacy-governed-v1\n    mode: Governed\n",
                "reserved",
            ),
            ("policies:\n  - name: Bad_Name\n    mode: Governed\n", "DNS label"),
            (
                "policies:\n  - name: g\n    mode: Governed\n  - name: g\n    mode: Governed\n",
                "declared twice",
            ),
            (
                "policies:\n  - name: g\n    mode: Governed\n    requireDistinctPrincipal: false\n",
                "requireDistinctPrincipal",
            ),
            (
                "allowOrdinaryConfirmation: true\npolicies:\n  - name: o\n    mode: Ordinary\n    requireDistinctPrincipal: true\n",
                "requireDistinctPrincipal",
            ),
            (
                "policies:\n  - name: g\n    mode: Governed\n    maxAgeSeconds: 59\n",
                "maxAgeSeconds",
            ),
            (
                "policies:\n  - name: g\n    mode: Governed\n    maxAgeSeconds: 604801\n",
                "maxAgeSeconds",
            ),
            ("namespaces:\n  team-a: nothing\n", "does not declare"),
            (
                "policies:\n  - name: g\n    mode: Governed\nnamespaces:\n  Team_A: g\n",
                "not a namespace",
            ),
            (
                "policies:\n  - name: g\n    mode: Governed\n    extra: 1\n",
                "unknown field",
            ),
        ] {
            let err = ApprovalPolicySet::parse(text)
                .err()
                .unwrap_or_else(|| panic!("{text:?} must be refused"));
            assert!(err.0.contains(needle), "{text:?}: {err}");
        }
    }

    #[test]
    fn the_snapshot_is_canonical_and_round_trips() {
        let p = policy(ApprovalMode::Governed);
        let bytes = p.snapshot_bytes();
        assert_eq!(
            String::from_utf8(bytes.clone()).unwrap_or_default(),
            "{\"formatVersion\":\"1\",\"kind\":\"ApprovalPolicySnapshot\",\"name\":\"prod-governed\",\"mode\":\"Governed\",\"maxAgeSeconds\":3600,\"requireDistinctPrincipal\":true}"
        );
        assert_eq!(ApprovalPolicy::from_snapshot_bytes(&bytes), Ok(p.clone()));
        let spaced = String::from_utf8(bytes)
            .unwrap_or_default()
            .replace(',', ", ");
        assert!(ApprovalPolicy::from_snapshot_bytes(spaced.as_bytes()).is_err());
        assert_ne!(
            p.digest(),
            policy(ApprovalMode::Ordinary).digest(),
            "different policies have different digests"
        );
    }

    #[test]
    fn any_policy_edit_changes_the_digest() {
        let p = policy(ApprovalMode::Governed);
        let mut longer = p.clone();
        longer.max_age_seconds += 1;
        let mut renamed = p.clone();
        renamed.name.push('x');
        let mut flipped = p.clone();
        flipped.mode = ApprovalMode::Ordinary;
        for other in [longer, renamed, flipped] {
            assert_ne!(p.digest(), other.digest());
        }
    }

    #[test]
    fn a_matching_document_passes_every_check() {
        let p = policy(ApprovalMode::Ordinary);
        let d = doc(&p);
        assert_eq!(
            check_restore_authorization(&d, &expected(), &p, at("2026-09-22T10:05:00Z")),
            Ok(())
        );
        let bytes = d.to_bytes();
        assert_eq!(RestoreAuthorization::from_bytes(&bytes), Ok(d));
    }

    /// **PROD-15.1: `approvalSubject` and `originalNameConfirmation` are
    /// document format 2.1.0** — the standing authorization's 1.1.0 pattern.
    /// A document that carries either is WRITTEN as 2.1.0 and REFUSED under
    /// 2.0.0, by the parser and by the binding check; every document without
    /// them is written as 2.0.0, byte for byte what the writer before the
    /// fields produced, and read exactly as before.
    ///
    /// KILLS: writing 2.0.0 for a document that carries the subject; writing
    /// 2.1.0 for every document (an ordinary document's bytes would change);
    /// a reader that accepts the fields under 2.0.0; a reader that refuses
    /// 2.1.0.
    #[test]
    fn the_subject_fields_are_format_2_1_0_and_refused_under_2_0_0() {
        use crate::original_name::OriginalNameConfirmation;

        let typed = OriginalNameConfirmation {
            typed_topics: vec!["orders".into()],
        };
        // THE WRITER.
        assert_eq!(
            restore_authorization_format_version_for(None, None),
            "2.0.0"
        );
        assert_eq!(
            restore_authorization_format_version_for(Some("originalName"), None),
            "2.1.0"
        );
        assert_eq!(
            restore_authorization_format_version_for(Some("originalName"), Some(&typed)),
            "2.1.0"
        );
        assert_eq!(
            restore_authorization_format_version_for(None, Some(&typed)),
            "2.1.0"
        );
        assert_eq!(RESTORE_AUTHORIZATION_FORMAT_VERSION, "2.0.0");
        assert_eq!(RESTORE_AUTHORIZATION_FORMAT_VERSION_SUBJECT, "2.1.0");

        // AN ORDINARY DOCUMENT IS BYTE FOR BYTE WHAT IT WAS: these are the
        // bytes the writer produced before the fields existed, key for key.
        let p = policy(ApprovalMode::Ordinary);
        let ordinary = doc(&p);
        let was = format!(
            "{{\"formatVersion\":\"2.0.0\",\"kind\":\"RestoreAuthorization\",\
             \"authorizationMode\":\"Ordinary\",\"subject\":{{\"apiVersion\":\
             \"logweir.dev/v1alpha1\",\"kind\":\"Restore\",\"namespace\":\"team-a\",\
             \"name\":\"rst-1\",\"uid\":\"uid-1\"}},\"planHash\":\"sha256:{}\",\
             \"requester\":{{\"issuer\":\"https://idp.example\",\"subject\":\"alice\"}},\
             \"policy\":{{\"name\":\"{}\",\"digest\":\"{}\"}},\
             \"issuedAt\":\"2026-09-22T10:00:00Z\",\"expiresAt\":\"2026-09-22T10:10:00Z\"}}",
            "a".repeat(64),
            p.name,
            p.digest()
        );
        assert_eq!(String::from_utf8(ordinary.to_bytes()).unwrap(), was);

        // THE READER. A document carrying the subject, at its version:
        let mut subject = doc(&p);
        subject.approval_subject = Some("originalName".into());
        subject.original_name_confirmation = Some(typed.clone());
        subject.format_version = RESTORE_AUTHORIZATION_FORMAT_VERSION_SUBJECT.into();
        assert_eq!(check_binding(&subject, &expected(), &p), Ok(()));
        assert_eq!(
            RestoreAuthorization::from_bytes(&subject.to_bytes()),
            Ok(subject.clone())
        );
        assert!(String::from_utf8(subject.to_bytes())
            .unwrap()
            .starts_with("{\"formatVersion\":\"2.1.0\","));
        // ... and under a Governed policy, the subject alone.
        let g = policy(ApprovalMode::Governed);
        let mut governed = doc(&g);
        governed.approval_subject = Some("originalName".into());
        governed.format_version = "2.1.0".into();
        assert_eq!(check_binding(&governed, &expected(), &g), Ok(()));

        // The same fields under a version that predates them: refused by the
        // parser and by the binding check, naming both versions.
        for (label, strip_subject, strip_typed) in [
            ("both fields", false, false),
            ("the subject alone", false, true),
            ("the typed names alone", true, false),
        ] {
            // 2.0.x, and (review 2, L8) every string that is not three
            // numeric parts, each of which the looser rule read as 2.1.
            for version in [
                "2.0.0",
                "2.0.9",
                "2",
                "2.x.0",
                "2.",
                "2.1",
                "2.01.0",
                "2.+1.0",
                "2.-1.0",
                "2.1.x",
                "2.1.0-rc1",
                "2.1.0.0",
                "2.1.",
                "2..0",
                "2.1.0 ",
                "2.18446744073709551616.0",
            ] {
                let mut old = subject.clone();
                old.format_version = version.into();
                if strip_subject {
                    old.approval_subject = None;
                }
                if strip_typed {
                    old.original_name_confirmation = None;
                }
                for refused in [
                    check_binding(&old, &expected(), &p),
                    RestoreAuthorization::from_bytes(&old.to_bytes()).map(|_| ()),
                ] {
                    let refused = refused.expect_err(label);
                    assert_eq!(
                        refused.reason(),
                        "AuthorizationDocumentInvalid",
                        "{label}, {version}: {refused}"
                    );
                    let text = refused.to_string();
                    assert!(
                        text.contains("defined from formatVersion 2.1.0")
                            && text.contains(&format!("{version:?}")),
                        "{label}, {version}: {text}"
                    );
                }
            }
        }
        // Another major stays refused, whatever it carries — a major that
        // is not written exactly `2` included (L8).
        for version in ["3.1.0", "1.1.0", " 2.1.0", "02.1.0", "+2.1.0"] {
            let mut major = subject.clone();
            major.format_version = version.into();
            for refused in [
                check_binding(&major, &expected(), &p),
                RestoreAuthorization::from_bytes(&major.to_bytes()).map(|_| ()),
            ] {
                assert_eq!(
                    refused.map_err(|r| r.reason()),
                    Err("AuthorizationDocumentInvalid"),
                    "{version:?}"
                );
            }
        }

        // CONTROLS. A 2.0.0 document without the fields is read as before, and
        // a minor adds optional fields only: 2.1.0 without them is accepted.
        assert_eq!(check_binding(&ordinary, &expected(), &p), Ok(()));
        assert_eq!(
            RestoreAuthorization::from_bytes(&ordinary.to_bytes()),
            Ok(ordinary.clone())
        );
        let mut newer = ordinary.clone();
        newer.format_version = "2.1.0".into();
        assert_eq!(check_binding(&newer, &expected(), &p), Ok(()));
        assert_eq!(
            RestoreAuthorization::from_bytes(&newer.to_bytes()),
            Ok(newer)
        );
        // A later minor or patch of major 2, written as three numbers, reads
        // the fields: the rule is "from 2.1.0", not "exactly 2.1.0".
        for version in ["2.1.1", "2.2.0", "2.10.0"] {
            let mut later = subject.clone();
            later.format_version = version.into();
            assert_eq!(check_binding(&later, &expected(), &p), Ok(()), "{version}");
        }
        assert_eq!(numeric_version("2.1.0"), Some((2, 1, 0)));
        assert_eq!(numeric_version("0.0.0"), Some((0, 0, 0)));
        assert_eq!(numeric_version("10.20.30"), Some((10, 20, 30)));
        for not_a_version in [
            "",
            "2",
            "2.1",
            "2.1.0.0",
            "2.01.0",
            "2.+1.0",
            "a.b.c",
            "2.1.0-rc1",
        ] {
            assert_eq!(numeric_version(not_a_version), None, "{not_a_version:?}");
        }
    }

    #[test]
    fn each_binding_field_is_checked() {
        let p = policy(ApprovalMode::Ordinary);
        let now = at("2026-09-22T10:05:00Z");
        let mut cases: Vec<(RestoreAuthorization, &str)> = Vec::new();
        let mut d = doc(&p);
        d.subject.uid = "uid-2".into();
        cases.push((d, "AuthorizationSubjectMismatch"));
        let mut d = doc(&p);
        d.subject.name = "rst-2".into();
        cases.push((d, "AuthorizationSubjectMismatch"));
        let mut d = doc(&p);
        d.subject.namespace = "team-b".into();
        cases.push((d, "AuthorizationSubjectMismatch"));
        let mut d = doc(&p);
        d.subject.kind = "Backup".into();
        cases.push((d, "AuthorizationSubjectMismatch"));
        let mut d = doc(&p);
        d.plan_hash = format!("sha256:{}", "b".repeat(64));
        cases.push((d, "PlanHashMismatch"));
        let mut d = doc(&p);
        d.policy.digest = format!("sha256:{}", "c".repeat(64));
        cases.push((d, "ApprovalPolicyMismatch"));
        let mut d = doc(&p);
        d.policy.name = "prod-governed".into();
        cases.push((d, "ApprovalPolicyMismatch"));
        let mut d = doc(&p);
        d.authorization_mode = ApprovalMode::Governed;
        cases.push((d, "ApprovalPolicyMismatch"));
        let mut d = doc(&p);
        d.requester.subject = " ".into();
        cases.push((d, "AuthorizationDocumentInvalid"));
        let mut d = doc(&p);
        d.kind = "StandingRehearsalAuthorization".into();
        cases.push((d, "AuthorizationDocumentInvalid"));
        let mut d = doc(&p);
        d.format_version = "1.0.0".into();
        cases.push((d, "AuthorizationDocumentInvalid"));
        for (d, reason) in cases {
            let got = check_restore_authorization(&d, &expected(), &p, now)
                .err()
                .unwrap_or_else(|| panic!("{d:?} must be refused"));
            assert_eq!(got.reason(), reason, "{d:?}: {got}");
        }
    }

    #[test]
    fn an_ordinary_document_is_refused_under_a_governed_binding_and_the_reverse() {
        let ordinary = policy(ApprovalMode::Ordinary);
        let governed = policy(ApprovalMode::Governed);
        let now = at("2026-09-22T10:05:00Z");
        let refused = check_restore_authorization(&doc(&ordinary), &expected(), &governed, now);
        assert_eq!(
            refused.map_err(|r| r.reason()),
            Err("ApprovalPolicyMismatch"),
            "the downgrade is refused"
        );
        let refused = check_restore_authorization(&doc(&governed), &expected(), &ordinary, now);
        assert_eq!(
            refused.map_err(|r| r.reason()),
            Err("ApprovalPolicyMismatch")
        );
    }

    #[test]
    fn the_window_is_bounded_by_the_policy_and_the_clock() {
        let p = policy(ApprovalMode::Ordinary);
        let mut d = doc(&p);
        assert_eq!(
            check_restore_authorization(&d, &expected(), &p, at("2026-09-22T10:10:00Z"))
                .map_err(|r| r.reason()),
            Err("AuthorizationExpired"),
            "expiresAt is exclusive"
        );
        assert_eq!(
            check_restore_authorization(&d, &expected(), &p, at("2026-09-22T09:59:30Z")),
            Ok(()),
            "a small skew is tolerated"
        );
        assert_eq!(
            check_restore_authorization(&d, &expected(), &p, at("2026-09-22T09:58:59Z"))
                .map_err(|r| r.reason()),
            Err("AuthorizationWindowInvalid"),
            "an issuedAt beyond the skew bound is refused"
        );
        d.expires_at = d.issued_at + chrono::Duration::seconds(p.max_age_seconds + 1);
        assert_eq!(
            check_window_shape(&d, &p).map_err(|r| r.reason()),
            Err("AuthorizationWindowInvalid")
        );
        d.expires_at = d.issued_at + chrono::Duration::seconds(p.max_age_seconds);
        assert_eq!(check_window_shape(&d, &p), Ok(()));
        d.expires_at = d.issued_at;
        assert_eq!(
            check_window_shape(&d, &p).map_err(|r| r.reason()),
            Err("AuthorizationWindowInvalid")
        );
    }

    #[test]
    fn unknown_document_fields_are_refused() {
        let p = policy(ApprovalMode::Ordinary);
        let mut value = serde_json::to_value(doc(&p)).unwrap_or_default();
        value["approvedBy"] = serde_json::json!("mallory");
        let bytes = serde_json::to_vec(&value).unwrap_or_default();
        assert_eq!(
            RestoreAuthorization::from_bytes(&bytes).map_err(|r| r.reason()),
            Err("AuthorizationDocumentInvalid")
        );
    }

    #[test]
    fn separation_compares_principal_ids_exactly() {
        let requester = Requester {
            issuer: "https://idp.example".into(),
            subject: "alice".into(),
        };
        assert!(!separation_holds(&requester, "https://idp.example#alice"));
        assert!(!separation_holds(&requester, " https://idp.example#alice "));
        assert!(!separation_holds(&requester, ""));
        assert!(separation_holds(&requester, "https://idp.example#bob"));
        // FAILS CLOSED (review M4): a principal not in `<issuer>#<subject>`
        // form cannot be compared with a requester, so it never establishes
        // separation -- `alice@example.com` may be Alice.
        for other_form in [
            "alice",
            "alice@example.com",
            "install:sha256:abc",
            "#alice",
            "https://idp.example#",
            "https://idp.example#bob ",
        ] {
            assert!(
                !separation_holds(&requester, other_form),
                "{other_form:?} establishes nothing"
            );
            assert!(!is_issuer_subject_principal(other_form));
        }
        assert!(is_issuer_subject_principal("https://idp.example#bob"));
    }

    #[test]
    fn a_governed_document_carries_a_ticket_and_an_ordinary_one_may() {
        let governed = policy(ApprovalMode::Governed);
        let mut d = doc(&governed);
        d.ticket = None;
        assert!(matches!(
            check_binding(&d, &expected(), &governed),
            Err(AuthorizationRefusal::DocumentInvalid(_))
        ));
        for bad in ["", " CHG-1", "CHG\n1"] {
            d.ticket = Some(bad.to_string());
            assert!(
                check_binding(&d, &expected(), &governed).is_err(),
                "{bad:?}"
            );
        }
        d.ticket = Some("x".repeat(MAX_TICKET_LEN + 1));
        assert!(check_binding(&d, &expected(), &governed).is_err());
        d.ticket = Some("CHG-4711".to_string());
        assert_eq!(check_binding(&d, &expected(), &governed), Ok(()));

        let ordinary = policy(ApprovalMode::Ordinary);
        let mut d = doc(&ordinary);
        d.ticket = None;
        assert_eq!(check_binding(&d, &expected(), &ordinary), Ok(()));
        d.ticket = Some("CHG-1".to_string());
        assert_eq!(check_binding(&d, &expected(), &ordinary), Ok(()));
    }

    #[test]
    fn the_required_signatures_are_per_mode() {
        assert_eq!(
            ApprovalMode::Ordinary.required_usages(),
            &[KeyUsage::ConsoleConfirmation]
        );
        assert_eq!(
            ApprovalMode::Governed.required_usages(),
            &[KeyUsage::ConsoleConfirmation, KeyUsage::GovernedApproval]
        );
    }

    // ------------------------------------------------------------------
    // PROD-16.1: the unbound default
    // ------------------------------------------------------------------

    #[test]
    fn a_fresh_install_resolves_every_unbound_namespace_to_confirm() {
        let fresh =
            ApprovalPolicySet::default().with_installation(InstallationMarker::FreshInstallConfirm);
        let effective = fresh.resolve("anything");
        assert_eq!(effective, EffectivePolicy::Bound(default_confirm_policy()));
        assert_eq!(effective.mode(), ApprovalMode::Ordinary);
        assert_eq!(effective.name(), DEFAULT_CONFIRM_POLICY_NAME);
        assert_eq!(OperatorMode::of(&effective), OperatorMode::Confirm);
        assert_eq!(fresh.unbound_basis(), UnboundBasis::FreshInstall);
        // NEGATIVE CONTROL: the same empty document, unmarked.
        let unmarked = ApprovalPolicySet::default();
        assert_eq!(unmarked.resolve("anything"), EffectivePolicy::Legacy);
        assert_eq!(
            OperatorMode::of(&unmarked.resolve("anything")),
            OperatorMode::Strict
        );
    }

    #[test]
    fn an_upgraded_install_keeps_legacy_for_its_unbound_namespaces() {
        // An upgraded install's identity already existed, so it carries no
        // marker; its document (here the PLAT-19.2 example) keeps every
        // unbound namespace on legacy-governed-v1, and every binding as it was.
        let upgraded = set();
        assert_eq!(upgraded.installation(), InstallationMarker::Unmarked);
        assert_eq!(upgraded.resolve("elsewhere"), EffectivePolicy::Legacy);
        assert_eq!(upgraded.unbound_basis(), UnboundBasis::Legacy);
        assert_eq!(upgraded.resolve("team-a").name(), "team-ordinary");
        assert_eq!(upgraded.resolve("prod").name(), "prod-governed");
        // A parse never marks: the marker is only ever applied by a reader.
        assert_eq!(
            ApprovalPolicySet::parse(DOC).map(|s| s.installation()),
            Ok(InstallationMarker::Unmarked)
        );
    }

    #[test]
    fn explicit_bindings_win_over_the_unbound_default() {
        let fresh = set().with_installation(InstallationMarker::FreshInstallConfirm);
        assert_eq!(
            fresh.resolve("prod").mode(),
            ApprovalMode::Governed,
            "bound Governed stays"
        );
        assert_eq!(fresh.resolve("prod").name(), "prod-governed");
        assert_eq!(fresh.resolve("team-a").name(), "team-ordinary");
        assert!(fresh.is_bound("prod") && !fresh.is_bound("elsewhere"));
        assert_eq!(
            fresh.resolve("elsewhere").name(),
            DEFAULT_CONFIRM_POLICY_NAME
        );
    }

    #[test]
    fn an_explicit_default_mode_beats_the_marker_both_ways() {
        let strict = ApprovalPolicySet::parse("defaultMode: strict\n")
            .unwrap_or_else(|e| panic!("{e}"))
            .with_installation(InstallationMarker::FreshInstallConfirm);
        assert_eq!(strict.resolve("x"), EffectivePolicy::Legacy);
        assert_eq!(
            strict.unbound_basis(),
            UnboundBasis::Configured(UnboundDefault::Strict)
        );
        // The opt-in of an OLDER install: no marker, an explicit confirm —
        // which, like any Ordinary policy, needs D0's floor.
        let opted_in =
            ApprovalPolicySet::parse("allowOrdinaryConfirmation: true\ndefaultMode: confirm\n")
                .unwrap_or_else(|e| panic!("{e}"));
        assert_eq!(opted_in.installation(), InstallationMarker::Unmarked);
        assert_eq!(
            opted_in.resolve("x"),
            EffectivePolicy::Bound(default_confirm_policy())
        );
        assert!(!opted_in.is_empty(), "defaultMode is configuration");
        assert!(opted_in.allows_ordinary_confirmation());
        let floorless = ApprovalPolicySet::parse("defaultMode: confirm\n");
        assert!(
            floorless
                .as_ref()
                .is_err_and(|e| e.0.contains("allowOrdinaryConfirmation")),
            "{floorless:?}"
        );
    }

    const NS: &str = "logweir-system";

    fn claim() -> MarkerClaim {
        MarkerClaim {
            policy_name: "logweir-installation".into(),
            policy_uid: "uid-7".into(),
            signing_key_id: "a".repeat(64),
            console_key_id: "b".repeat(64),
        }
    }

    fn hook_policy() -> TrustPolicyFacts {
        TrustPolicyFacts {
            uid: "uid-7".into(),
            default: true,
            created_by: Some(CREATED_BY_IDENTITY_BOOTSTRAP.into()),
            approval_default: Some(APPROVAL_DEFAULT_CONFIRM.into()),
            keys: vec![
                TrustKeyFacts {
                    key_id: "a".repeat(64),
                    usages: vec!["EvidenceSigning".into()],
                    principal_id: format!("install:{NS}/logweir-signing-key"),
                },
                TrustKeyFacts {
                    key_id: "b".repeat(64),
                    usages: vec!["ConsoleConfirmation".into()],
                    principal_id: format!("console:{NS}/logweir-console-confirmation"),
                },
            ],
        }
    }

    #[test]
    fn the_claim_round_trips_and_nothing_else_parses() {
        let c = claim();
        assert_eq!(MarkerClaim::parse(&c.to_annotation()), Some(c.clone()));
        for other in [
            "confirm",
            "Confirm;policy=p;uid=u;signing=s;console=c",
            "confirm;policy=p;uid=u;signing=s",
            "confirm;policy=p;uid=u;signing=s;console=c;extra=1",
            "confirm;policy=;uid=u;signing=s;console=c",
            "confirm;uid=u;policy=p;signing=s;console=c",
            "confirm;policy=p ;uid=u;signing=s;console=c",
        ] {
            assert_eq!(MarkerClaim::parse(other), None, "{other:?}");
        }
    }

    /// THE BOUND MARKER (PROD-16.1 security review): honoured only beside the
    /// trust entry the identity hook made in the same fresh-install run. Each
    /// row below breaks ONE binding and must read unmarked.
    #[test]
    fn the_marker_is_honoured_only_beside_the_hook_made_trust_entry() {
        let annotation = claim().to_annotation();
        let id = "a".repeat(64);
        let ok = verify_marker(Some(&annotation), Some(&id), NS, Some(&hook_policy()));
        assert_eq!(ok, Ok(InstallationMarker::FreshInstallConfirm));
        assert_eq!(
            verify_marker(None, Some(&id), NS, None),
            Ok(InstallationMarker::Unmarked),
            "no annotation is simply unmarked"
        );
        let refused = |annotation: &str,
                       identity: &str,
                       namespace: &str,
                       policy: Option<TrustPolicyFacts>| {
            verify_marker(Some(annotation), Some(identity), namespace, policy.as_ref()).err()
        };
        // A marker patched in by hand (the bare word the first draft used).
        assert_eq!(
            refused("confirm", &id, NS, Some(hook_policy())),
            Some(MarkerRefusal::NotAClaim)
        );
        // A marker naming another identity.
        assert_eq!(
            refused(&annotation, &"c".repeat(64), NS, Some(hook_policy())),
            Some(MarkerRefusal::OtherIdentity)
        );
        // No hook-made trust entry at all (an UPGRADED install).
        assert_eq!(
            refused(&annotation, &id, NS, None),
            Some(MarkerRefusal::PolicyMissing)
        );
        // A policy of that name an administrator wrote later: another UID.
        let mut replaced = hook_policy();
        replaced.uid = "uid-8".into();
        assert_eq!(
            refused(&annotation, &id, NS, Some(replaced)),
            Some(MarkerRefusal::PolicyReplaced)
        );
        for strip in [0, 1] {
            let mut p = hook_policy();
            if strip == 0 {
                p.created_by = None;
            } else {
                p.approval_default = None;
            }
            assert_eq!(
                refused(&annotation, &id, NS, Some(p)),
                Some(MarkerRefusal::NotHookMade)
            );
        }
        let mut not_default = hook_policy();
        not_default.default = false;
        assert_eq!(
            refused(&annotation, &id, NS, Some(not_default)),
            Some(MarkerRefusal::NotDefault)
        );
        // The console key missing, given another usage, or two.
        let mut no_console = hook_policy();
        no_console.keys.truncate(1);
        let mut wrong_usage = hook_policy();
        wrong_usage.keys[1].usages = vec!["GovernedApproval".into()];
        let mut two_usages = hook_policy();
        two_usages.keys[1].usages.push("EvidenceSigning".into());
        for p in [no_console, wrong_usage, two_usages] {
            assert_eq!(
                refused(&annotation, &id, NS, Some(p)),
                Some(MarkerRefusal::KeysDiffer)
            );
        }
        assert_eq!(
            refused(&annotation, &id, "elsewhere", Some(hook_policy())),
            Some(MarkerRefusal::KeysDiffer),
            "principals of another installation namespace"
        );
    }

    #[test]
    fn local_admin_may_serve_confirm_and_strict_but_never_two_person() {
        assert!(OperatorMode::Confirm.allowed_in_local_admin());
        assert!(OperatorMode::Strict.allowed_in_local_admin());
        assert!(!OperatorMode::TwoPerson.allowed_in_local_admin());
    }

    #[test]
    fn the_operator_names_are_one_mapping() {
        for mode in [
            OperatorMode::Confirm,
            OperatorMode::TwoPerson,
            OperatorMode::Strict,
        ] {
            assert_eq!(OperatorMode::from_operator_name(mode.as_str()), Some(mode));
        }
        assert_eq!(OperatorMode::from_operator_name("Ordinary"), None);
        assert_eq!(
            OperatorMode::of(&set().resolve("team-a")),
            OperatorMode::Confirm
        );
        assert_eq!(
            OperatorMode::of(&set().resolve("prod")),
            OperatorMode::Strict
        );
        assert_eq!(
            OperatorMode::of(&EffectivePolicy::Legacy),
            OperatorMode::Strict
        );
        // A document may spell a policy's mode either way; the parsed policy,
        // its snapshot and its digest are the internal ones.
        let aliased = ApprovalPolicySet::parse(
            &DOC.replace("mode: Ordinary", "mode: confirm")
                .replace("mode: Governed", "mode: strict"),
        )
        .unwrap_or_else(|e| panic!("{e}"));
        assert_eq!(aliased.digest(), set().digest());
        assert_eq!(aliased.resolve("team-a"), set().resolve("team-a"));
    }

    #[test]
    fn prod_16_1_refusals_name_the_field() {
        for (text, needle) in [
            (
                "policies:\n  - name: default-confirm-v1\n    mode: Governed\n",
                "reserved",
            ),
            ("defaultMode: two-person\n", "set per namespace"),
            ("defaultMode: Ordinary\n", "not a mode"),
            ("defaultMode: \"\"\n", "not a mode"),
            (
                "policies:\n  - name: p\n    mode: confirm\n",
                "allowOrdinaryConfirmation",
            ),
        ] {
            let err = ApprovalPolicySet::parse(text)
                .err()
                .unwrap_or_else(|| panic!("{text:?} must be refused"));
            assert!(err.0.contains(needle), "{text:?}: {err}");
        }
    }

    /// THE OLDER READER, frozen: `PolicySetDocument` exactly as PLAT-19.2
    /// shipped it. A document carrying `defaultMode` must be REFUSED by it —
    /// an older controller or console reached by rollback refuses to start
    /// rather than run every unbound namespace on a default it cannot know
    /// (fail closed, never open).
    #[test]
    fn an_older_reader_refuses_the_new_field_and_never_reads_the_marker() {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields, rename_all = "camelCase")]
        #[allow(dead_code)]
        struct OldPolicySetDocument {
            #[serde(default)]
            allow_ordinary_confirmation: bool,
            #[serde(default)]
            policies: Vec<serde_yaml::Value>,
            #[serde(default)]
            namespaces: BTreeMap<String, String>,
        }
        let old = |text: &str| serde_yaml::from_str::<OldPolicySetDocument>(text).is_ok();
        assert!(
            !old("defaultMode: confirm\n"),
            "an older reader must refuse defaultMode"
        );
        assert!(!old("defaultMode: strict\n"));
        // NEGATIVE CONTROL: the document an install without `approvalPolicy.default`
        // renders is still one the older reader accepts.
        assert!(old(DOC));
        assert!(ApprovalPolicySet::parse(
            "allowOrdinaryConfirmation: true\ndefaultMode: confirm\n"
        )
        .is_ok());
    }

    #[test]
    fn the_default_confirm_policy_is_canonical_and_checks_like_a_binding() {
        let p = default_confirm_policy();
        assert_eq!(
            String::from_utf8(p.snapshot_bytes()).unwrap_or_default(),
            "{\"formatVersion\":\"1\",\"kind\":\"ApprovalPolicySnapshot\",\"name\":\"default-confirm-v1\",\"mode\":\"Ordinary\",\"maxAgeSeconds\":900,\"requireDistinctPrincipal\":false}"
        );
        assert_eq!(
            ApprovalPolicy::from_snapshot_bytes(&p.snapshot_bytes()),
            Ok(p.clone())
        );
        let mut d = doc(&p);
        d.requester = Requester {
            issuer: "urn:logweir:local-admin".into(),
            subject: "admin".into(),
        };
        let fresh =
            ApprovalPolicySet::default().with_installation(InstallationMarker::FreshInstallConfirm);
        let bound = fresh.resolve("team-a");
        let bound = bound.bound().cloned().unwrap_or_else(|| panic!("confirm"));
        assert_eq!(
            check_restore_authorization(&d, &expected(), &bound, at("2026-09-22T10:05:00Z")),
            Ok(())
        );
        // NEGATIVE CONTROL: an unmarked installation resolves the same
        // namespace to legacy, where no v2 document is accepted at all.
        assert!(ApprovalPolicySet::default()
            .resolve("team-a")
            .bound()
            .is_none());
    }

    #[test]
    fn the_set_digest_names_the_unbound_default_only_when_it_is_not_legacy() {
        // An install whose unbound namespaces are legacy keeps the PLAT-19.2
        // digest, computed here by the old formula.
        #[derive(Serialize)]
        #[serde(rename_all = "camelCase")]
        struct Old<'a> {
            allow_ordinary_confirmation: bool,
            policies: Vec<String>,
            namespaces: &'a BTreeMap<String, String>,
        }
        let s = set();
        let old = sha256_prefixed(
            &serde_json::to_vec(&Old {
                allow_ordinary_confirmation: true,
                policies: s.policies.values().map(ApprovalPolicy::digest).collect(),
                namespaces: &s.bindings,
            })
            .unwrap_or_default(),
        );
        assert_eq!(s.digest(), old);
        let fresh = set().with_installation(InstallationMarker::FreshInstallConfirm);
        assert_ne!(
            fresh.digest(),
            old,
            "a reader that saw the marker publishes another digest"
        );
        let explicit =
            ApprovalPolicySet::parse(&format!("defaultMode: confirm\n{DOC}")).unwrap_or_default();
        assert_eq!(
            explicit.digest(),
            fresh.digest(),
            "confirm is confirm, whatever decided it"
        );
    }

    // ------------------------------------------------------------------
    // PROD-16.2: two-person approval in the console
    // ------------------------------------------------------------------

    const TWO_PERSON_DOC: &str = "policies:
  - name: prod-pair
    mode: two-person
    maxAgeSeconds: 3600
  - name: prod-governed
    mode: Governed
    maxAgeSeconds: 3600
namespaces:
  team-a: prod-pair
  prod: prod-governed
";

    /// The two-person policy `team-a` is bound to.
    fn pair() -> ApprovalPolicy {
        ApprovalPolicySet::parse(TWO_PERSON_DOC)
            .unwrap_or_else(|e| panic!("{e}"))
            .resolve("team-a")
            .bound()
            .cloned()
            .unwrap_or_else(|| panic!("team-a is bound"))
    }

    /// A REQUEST under `policy`, as the console signs it for `alice`.
    fn request(policy: &ApprovalPolicy) -> RestoreAuthorization {
        doc(policy)
    }

    /// The APPROVAL of [`request`]: the same document, `bob` clicking four
    /// minutes after the request was made.
    fn approved(policy: &ApprovalPolicy) -> RestoreAuthorization {
        let mut d = request(policy);
        d.format_version = RESTORE_AUTHORIZATION_FORMAT_VERSION_APPROVER.into();
        d.approver = Some(Approver {
            issuer: "https://idp.example".into(),
            subject: "bob".into(),
        });
        d.approved_at = Some(at("2026-09-22T10:04:00Z"));
        d
    }

    fn approver(issuer: &str, subject: &str) -> Approver {
        Approver {
            issuer: issuer.into(),
            subject: subject.into(),
        }
    }

    fn requester(issuer: &str, subject: &str) -> Requester {
        Requester {
            issuer: issuer.into(),
            subject: subject.into(),
        }
    }

    const NOW: &str = "2026-09-22T10:05:00Z";

    /// **`approverSignature: Console` is INSIDE the snapshot and its digest,
    /// and a policy without it keeps the bytes it always had.**
    ///
    /// KILLS: the setting left out of the snapshot (a request made under one
    /// setting would be approved under the other); the setting written for a
    /// personal-key policy (every existing digest would move).
    #[test]
    fn the_approver_setting_is_inside_the_snapshot_digest_and_absent_keeps_the_bytes() {
        let p = pair();
        assert_eq!(p.mode, ApprovalMode::Governed);
        assert_eq!(p.approver_signature, ApproverSignature::Console);
        assert!(p.require_distinct_principal);
        let bytes = p.snapshot_bytes();
        assert_eq!(
            String::from_utf8(bytes.clone()).unwrap_or_default(),
            "{\"formatVersion\":\"1\",\"kind\":\"ApprovalPolicySnapshot\",\"name\":\"prod-pair\",\"mode\":\"Governed\",\"maxAgeSeconds\":3600,\"requireDistinctPrincipal\":true,\"approverSignature\":\"Console\"}"
        );
        assert_eq!(ApprovalPolicy::from_snapshot_bytes(&bytes), Ok(p.clone()));

        // The SAME policy with a personal key: the bytes a writer before the
        // setting produced, key for key, and another digest.
        let mut personal = p.clone();
        personal.approver_signature = ApproverSignature::PersonalKey;
        assert_eq!(
            String::from_utf8(personal.snapshot_bytes()).unwrap_or_default(),
            "{\"formatVersion\":\"1\",\"kind\":\"ApprovalPolicySnapshot\",\"name\":\"prod-pair\",\"mode\":\"Governed\",\"maxAgeSeconds\":3600,\"requireDistinctPrincipal\":true}"
        );
        assert_ne!(p.digest(), personal.digest());
        assert_eq!(
            ApprovalPolicy::from_snapshot_bytes(&personal.snapshot_bytes()),
            Ok(personal.clone())
        );

        // No second spelling: a snapshot that WRITES the default, one that
        // names another value, and Console on an Ordinary policy are refused.
        let text = String::from_utf8(personal.snapshot_bytes()).unwrap_or_default();
        for (label, bad) in [
            (
                "the default written out",
                text.replace('}', ",\"approverSignature\":\"PersonalKey\"}"),
            ),
            (
                "another value",
                text.replace('}', ",\"approverSignature\":\"console\"}"),
            ),
            ("null", text.replace('}', ",\"approverSignature\":null}")),
            (
                "Console on an Ordinary policy",
                String::from_utf8(bytes.clone())
                    .unwrap_or_default()
                    .replace("\"mode\":\"Governed\"", "\"mode\":\"Ordinary\"")
                    .replace(
                        "\"requireDistinctPrincipal\":true",
                        "\"requireDistinctPrincipal\":false",
                    ),
            ),
        ] {
            assert!(
                ApprovalPolicy::from_snapshot_bytes(bad.as_bytes()).is_err(),
                "{label}: {bad}"
            );
        }
    }

    /// THE OLDER RUNNER, frozen: `Snapshot` exactly as PROD-16.1 shipped it.
    /// It refuses a two-person snapshot (an unknown field) — it never runs
    /// one as a personal-key policy — and still reads every other snapshot.
    #[test]
    fn an_older_snapshot_reader_refuses_the_console_setting() {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields, rename_all = "camelCase")]
        #[allow(dead_code)]
        struct OldSnapshot {
            format_version: String,
            kind: String,
            name: String,
            mode: ApprovalMode,
            max_age_seconds: i64,
            require_distinct_principal: bool,
        }
        let old = |bytes: &[u8]| serde_json::from_slice::<OldSnapshot>(bytes);
        let refused = old(&pair().snapshot_bytes())
            .err()
            .map(|e| e.to_string())
            .unwrap_or_default();
        assert!(
            refused.contains("unknown field `approverSignature`"),
            "{refused}"
        );
        // NEGATIVE CONTROL: the snapshots every existing policy has.
        assert!(old(&policy(ApprovalMode::Governed).snapshot_bytes()).is_ok());
        assert!(old(&policy(ApprovalMode::Ordinary).snapshot_bytes()).is_ok());
        assert!(old(&default_confirm_policy().snapshot_bytes()).is_ok());
    }

    /// **`mode: two-person` is `Governed` with `approverSignature: Console`**:
    /// one policy, two spellings, one digest; and every other pairing is
    /// refused by field.
    #[test]
    fn two_person_is_governed_with_the_console_as_the_approvers_signature() {
        let operator = ApprovalPolicySet::parse(TWO_PERSON_DOC).unwrap_or_else(|e| panic!("{e}"));
        let internal = ApprovalPolicySet::parse(&TWO_PERSON_DOC.replace(
            "mode: two-person",
            "mode: Governed\n    approverSignature: Console",
        ))
        .unwrap_or_else(|e| panic!("{e}"));
        let both = ApprovalPolicySet::parse(&TWO_PERSON_DOC.replace(
            "mode: two-person",
            "mode: two-person\n    approverSignature: Console",
        ))
        .unwrap_or_else(|e| panic!("{e}"));
        assert_eq!(operator.digest(), internal.digest());
        assert_eq!(operator.digest(), both.digest());
        assert_eq!(operator.resolve("team-a"), internal.resolve("team-a"));
        let effective = operator.resolve("team-a");
        assert_eq!(OperatorMode::of(&effective), OperatorMode::TwoPerson);
        assert_eq!(effective.mode(), ApprovalMode::Governed);
        assert!(!OperatorMode::of(&effective).allowed_in_local_admin());
        // It needs no `allowOrdinaryConfirmation`: it is not Ordinary.
        assert!(!operator.allows_ordinary_confirmation());
        // NEGATIVE CONTROL: the Governed policy beside it is still strict,
        // with the snapshot it always had.
        let strict = operator.resolve("prod");
        assert_eq!(OperatorMode::of(&strict), OperatorMode::Strict);
        assert_eq!(
            strict.bound().map(|p| p.approver_signature),
            Some(ApproverSignature::PersonalKey)
        );
        assert!(OperatorMode::of(&strict).allowed_in_local_admin());

        // The installation digest moves when a policy gains the setting.
        let as_strict = ApprovalPolicySet::parse(&TWO_PERSON_DOC.replace("two-person", "strict"))
            .unwrap_or_else(|e| panic!("{e}"));
        assert_ne!(operator.digest(), as_strict.digest());

        for (text, needle) in [
            (
                "allowOrdinaryConfirmation: true\npolicies:\n  - name: p\n    mode: Ordinary\n    approverSignature: Console\n",
                "has no approver",
            ),
            (
                "allowOrdinaryConfirmation: true\npolicies:\n  - name: p\n    mode: confirm\n    approverSignature: Console\n",
                "has no approver",
            ),
            (
                "policies:\n  - name: p\n    mode: strict\n    approverSignature: Console\n",
                "personal-key approval",
            ),
            (
                "policies:\n  - name: p\n    mode: Governed\n    approverSignature: PersonalKey\n",
                "it is Console",
            ),
            (
                "policies:\n  - name: p\n    mode: Governed\n    approverSignature: console\n",
                "it is Console",
            ),
            (
                "policies:\n  - name: p\n    mode: two-person\n    approverSignature: \"\"\n",
                "it is Console",
            ),
            (
                "policies:\n  - name: p\n    mode: two-person\n    requireDistinctPrincipal: false\n",
                "requireDistinctPrincipal",
            ),
            ("defaultMode: two-person\n", "set per namespace"),
        ] {
            let err = ApprovalPolicySet::parse(text)
                .err()
                .unwrap_or_else(|| panic!("{text:?} must be refused"));
            assert!(err.0.contains(needle), "{text:?}: {err}");
        }
    }

    /// THE OLDER CONTROLLER AND CONSOLE, frozen: the policy entry exactly as
    /// PROD-16.1 shipped it. A document that says `approverSignature` is
    /// refused at start (fail closed), and so is `mode: two-person`, which
    /// that build refused by name.
    #[test]
    fn an_older_policy_reader_refuses_the_console_setting() {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields, rename_all = "camelCase")]
        #[allow(dead_code)]
        struct OldPolicyEntry {
            name: String,
            mode: String,
            #[serde(default)]
            max_age_seconds: Option<i64>,
            #[serde(default)]
            require_distinct_principal: Option<bool>,
        }
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields, rename_all = "camelCase")]
        #[allow(dead_code)]
        struct OldPolicySetDocument {
            #[serde(default)]
            allow_ordinary_confirmation: bool,
            #[serde(default)]
            default_mode: Option<String>,
            #[serde(default)]
            policies: Vec<OldPolicyEntry>,
            #[serde(default)]
            namespaces: BTreeMap<String, String>,
        }
        let old = |text: &str| serde_yaml::from_str::<OldPolicySetDocument>(text);
        // What the chart renders for a two-person policy: the internal words.
        let rendered = TWO_PERSON_DOC.replace(
            "mode: two-person",
            "mode: Governed\n    approverSignature: Console",
        );
        let refused = old(&rendered)
            .err()
            .map(|e| e.to_string())
            .unwrap_or_default();
        assert!(
            refused.contains("unknown field `approverSignature`"),
            "{refused}"
        );
        // NEGATIVE CONTROL: without the setting the older reader accepts it.
        assert!(old(&TWO_PERSON_DOC.replace("two-person", "Governed")).is_ok());
        assert!(old(DOC).is_ok());
    }

    /// **A request made under one setting cannot be approved under the
    /// other**: the digest the signed bytes name is the other policy's.
    #[test]
    fn a_request_made_under_one_setting_is_not_approved_under_the_other() {
        let console = pair();
        let mut personal = console.clone();
        personal.approver_signature = ApproverSignature::PersonalKey;
        let now = at(NOW);
        // Requested under the personal key, the namespace now says Console.
        let refused = check_restore_authorization(&request(&personal), &expected(), &console, now);
        assert_eq!(
            refused.map_err(|r| r.reason()),
            Err("ApprovalPolicyMismatch")
        );
        // Requested (and approved) under Console, the namespace now says a
        // personal key.
        for d in [request(&console), approved(&console)] {
            let refused = check_restore_authorization(&d, &expected(), &personal, now);
            assert_eq!(
                refused.map_err(|r| r.reason()),
                Err("ApprovalPolicyMismatch")
            );
            assert_eq!(
                check_binding(&d, &expected(), &personal).map_err(|r| r.reason()),
                Err("ApprovalPolicyMismatch")
            );
        }
        // NEGATIVE CONTROL: each under its own setting.
        assert_eq!(
            check_restore_authorization(&approved(&console), &expected(), &console, now),
            Ok(())
        );
        assert_eq!(
            check_restore_authorization(&request(&personal), &expected(), &personal, now),
            Ok(())
        );
    }

    /// **`approver` and `approvedAt` are document format 2.2.0**: written at
    /// that version exactly when present, refused under 2.0.0 and 2.1.0 by
    /// the parser and by the binding check, refused one without the other —
    /// and every document without them is byte for byte what it was.
    ///
    /// KILLS: a writer that gives an approval 2.0.0 or 2.1.0; a writer that
    /// gives every document 2.2.0; a reader that accepts the fields under an
    /// older version; a reader that refuses 2.2.0.
    #[test]
    fn the_approver_fields_are_format_2_2_0_and_refused_under_older_versions() {
        use crate::original_name::OriginalNameConfirmation;
        let typed = OriginalNameConfirmation {
            typed_topics: vec!["orders".into()],
        };
        // THE WRITER: 2.2.0 exactly when the approver is present.
        assert_eq!(
            restore_authorization_format_version(None, None, false),
            "2.0.0"
        );
        assert_eq!(
            restore_authorization_format_version(Some("originalName"), None, false),
            "2.1.0"
        );
        assert_eq!(
            restore_authorization_format_version(None, Some(&typed), false),
            "2.1.0"
        );
        assert_eq!(
            restore_authorization_format_version(None, None, true),
            "2.2.0"
        );
        assert_eq!(
            restore_authorization_format_version(Some("originalName"), None, true),
            "2.2.0"
        );
        assert_eq!(RESTORE_AUTHORIZATION_FORMAT_VERSION_APPROVER, "2.2.0");
        // The two-argument writer is the one without an approver.
        assert_eq!(
            restore_authorization_format_version_for(None, None),
            "2.0.0"
        );

        let p = pair();
        // A REQUEST UNDER A TWO-PERSON POLICY IS BYTE FOR BYTE A 2.0.0
        // DOCUMENT: these are the keys the writer before the fields produced.
        let was = format!(
            "{{\"formatVersion\":\"2.0.0\",\"kind\":\"RestoreAuthorization\",\
             \"authorizationMode\":\"Governed\",\"subject\":{{\"apiVersion\":\
             \"logweir.dev/v1alpha1\",\"kind\":\"Restore\",\"namespace\":\"team-a\",\
             \"name\":\"rst-1\",\"uid\":\"uid-1\"}},\"planHash\":\"sha256:{}\",\
             \"requester\":{{\"issuer\":\"https://idp.example\",\"subject\":\"alice\"}},\
             \"policy\":{{\"name\":\"{}\",\"digest\":\"{}\"}},\
             \"issuedAt\":\"2026-09-22T10:00:00Z\",\"expiresAt\":\"2026-09-22T10:10:00Z\",\
             \"ticket\":\"CHG-1\"}}",
            "a".repeat(64),
            p.name,
            p.digest()
        );
        assert_eq!(String::from_utf8(request(&p).to_bytes()).unwrap(), was);
        // ... and a 2.1.0 one, key for key, when it names the subject.
        let mut subject_request = request(&p);
        subject_request.approval_subject = Some("originalName".into());
        subject_request.format_version = "2.1.0".into();
        assert_eq!(
            String::from_utf8(subject_request.to_bytes()).unwrap(),
            was.replace("\"formatVersion\":\"2.0.0\"", "\"formatVersion\":\"2.1.0\"")
                .replace(
                    "\"ticket\":\"CHG-1\"}",
                    "\"ticket\":\"CHG-1\",\"approvalSubject\":\"originalName\"}"
                )
        );
        // THE APPROVAL: the same keys, then the two new ones, last.
        let a = approved(&p);
        assert_eq!(
            String::from_utf8(a.to_bytes()).unwrap(),
            was.replace("\"formatVersion\":\"2.0.0\"", "\"formatVersion\":\"2.2.0\"")
                .replace(
                    "\"ticket\":\"CHG-1\"}",
                    "\"ticket\":\"CHG-1\",\"approver\":{\"issuer\":\"https://idp.example\",\
                     \"subject\":\"bob\"},\"approvedAt\":\"2026-09-22T10:04:00Z\"}"
                )
        );
        assert_eq!(check_binding(&a, &expected(), &p), Ok(()));
        assert_eq!(
            RestoreAuthorization::from_bytes(&a.to_bytes()),
            Ok(a.clone())
        );

        // THE READER: either field under a version that predates it.
        for (label, strip_approver, strip_at) in [
            ("both fields", false, false),
            ("the approver alone", false, true),
            ("the instant alone", true, false),
        ] {
            for version in [
                "2.0.0",
                "2.0.9",
                "2.1.0",
                "2.1.9",
                "2",
                "2.2",
                "2.02.0",
                "2.+2.0",
                "2.2.x",
                "2.2.0-rc1",
                "2.2.0.0",
                "2.x.0",
                "2.2.0 ",
            ] {
                let mut old = a.clone();
                old.format_version = version.into();
                if strip_approver {
                    old.approver = None;
                }
                if strip_at {
                    old.approved_at = None;
                }
                for refused in [
                    check_binding(&old, &expected(), &p),
                    RestoreAuthorization::from_bytes(&old.to_bytes()).map(|_| ()),
                ] {
                    let refused = refused.expect_err(label);
                    assert_eq!(
                        refused.reason(),
                        "AuthorizationDocumentInvalid",
                        "{label}, {version}: {refused}"
                    );
                    let text = refused.to_string();
                    assert!(
                        text.contains("defined from formatVersion 2.2.0")
                            && text.contains(&format!("{version:?}")),
                        "{label}, {version}: {text}"
                    );
                }
            }
        }
        // One without the other, at the right version: refused too.
        for strip_approver in [true, false] {
            let mut half = a.clone();
            if strip_approver {
                half.approver = None;
            } else {
                half.approved_at = None;
            }
            for refused in [
                check_binding(&half, &expected(), &p),
                RestoreAuthorization::from_bytes(&half.to_bytes()).map(|_| ()),
            ] {
                let refused = refused.expect_err("one without the other");
                assert_eq!(refused.reason(), "AuthorizationDocumentInvalid");
                assert!(
                    refused.to_string().contains("without the other"),
                    "{refused}"
                );
            }
        }
        // Another major stays refused.
        for version in ["3.2.0", "1.2.0", " 2.2.0", "02.2.0"] {
            let mut major = a.clone();
            major.format_version = version.into();
            assert_eq!(
                check_binding(&major, &expected(), &p).map_err(|r| r.reason()),
                Err("AuthorizationDocumentInvalid"),
                "{version:?}"
            );
        }
        // CONTROLS. A later minor or patch reads the fields; and a minor adds
        // optional fields only, so 2.2.0 WITHOUT them parses (it is then a
        // request, which a Console policy does not accept as an approval).
        for version in ["2.2.0", "2.2.1", "2.3.0", "2.10.0"] {
            let mut later = a.clone();
            later.format_version = version.into();
            assert_eq!(check_binding(&later, &expected(), &p), Ok(()), "{version}");
        }
        let mut newer_request = request(&p);
        newer_request.format_version = "2.2.0".into();
        assert!(RestoreAuthorization::from_bytes(&newer_request.to_bytes()).is_ok());
    }

    /// THE OLDER READER, frozen: the document exactly as PROD-15.1 shipped it
    /// (2.1.0's fields, `deny_unknown_fields`). It refuses every 2.2.0
    /// document this build writes — each carries `approver` — and still reads
    /// a 2.0.0 and a 2.1.0 one.
    #[test]
    fn an_older_document_reader_refuses_the_approver_fields() {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields, rename_all = "camelCase")]
        #[allow(dead_code)]
        struct OldRestoreAuthorization {
            format_version: String,
            kind: String,
            authorization_mode: ApprovalMode,
            subject: AuthorizedSubject,
            plan_hash: String,
            requester: Requester,
            policy: PolicyRef,
            issued_at: DateTime<Utc>,
            expires_at: DateTime<Utc>,
            #[serde(default)]
            ticket: Option<String>,
            #[serde(default)]
            approval_subject: Option<String>,
            #[serde(default)]
            original_name_confirmation: Option<crate::original_name::OriginalNameConfirmation>,
        }
        let old = |bytes: &[u8]| serde_json::from_slice::<OldRestoreAuthorization>(bytes);
        let p = pair();
        let refused = old(&approved(&p).to_bytes())
            .err()
            .map(|e| e.to_string())
            .unwrap_or_default();
        assert!(refused.contains("unknown field `approver`"), "{refused}");
        let mut only_instant = approved(&p);
        only_instant.approver = None;
        let refused = old(&only_instant.to_bytes())
            .err()
            .map(|e| e.to_string())
            .unwrap_or_default();
        assert!(refused.contains("unknown field `approvedAt`"), "{refused}");
        // NEGATIVE CONTROL: the request, and a 2.1.0 request.
        assert!(old(&request(&p).to_bytes()).is_ok());
        let mut subject_request = request(&p);
        subject_request.approval_subject = Some("originalName".into());
        subject_request.format_version = "2.1.0".into();
        assert!(old(&subject_request.to_bytes()).is_ok());
    }

    /// **THE IDENTITY RULE.** Every row is a way the approver might not be a
    /// second person; each is refused `SelfApprovalRefused`, by the rule
    /// alone and by the whole verdict, and the control beside it is accepted.
    #[test]
    fn a_console_approval_needs_a_second_person_the_same_issuer_vouches_for() {
        let idp = "https://idp.example";
        let alice = requester(idp, "alice");
        // THE CONTROL: another subject of the same issuer.
        assert_eq!(console_separation(&alice, &approver(idp, "bob")), Ok(()));
        // ... and an issuer that differs only by a trailing slash or by case
        // is the SAME issuer, so a second subject of it is still accepted.
        assert_eq!(
            console_separation(&alice, &approver("https://idp.example/", "bob")),
            Ok(())
        );
        assert_eq!(
            console_separation(&alice, &approver("HTTPS://IDP.example", "bob")),
            Ok(())
        );

        let refused: Vec<(&str, Requester, Approver, &str)> = vec![
            (
                "the requester",
                alice.clone(),
                approver(idp, "alice"),
                "is the requester",
            ),
            (
                "a subject that differs only by case",
                alice.clone(),
                approver(idp, "Alice"),
                "is the requester",
            ),
            (
                "the requester under an issuer with a trailing slash",
                alice.clone(),
                approver("https://idp.example/", "alice"),
                "is the requester",
            ),
            (
                "the requester under an issuer in another case",
                requester("https://IDP.example", "alice"),
                approver(idp, "ALICE"),
                "is the requester",
            ),
            (
                "the same subject from another issuer",
                alice.clone(),
                approver("https://other-idp.example", "alice"),
                "two issuers",
            ),
            (
                "another subject from another issuer",
                alice.clone(),
                approver("https://other-idp.example", "bob"),
                "two issuers",
            ),
            (
                "a subject with a trailing space",
                alice.clone(),
                approver(idp, "alice "),
                "not in a form that can be compared",
            ),
            (
                "a subject with a leading tab",
                alice.clone(),
                approver(idp, "\talice"),
                "not in a form that can be compared",
            ),
            (
                "a subject with an inner space",
                alice.clone(),
                approver(idp, "ali ce"),
                "not in a form that can be compared",
            ),
            (
                "a subject with a zero-width joiner",
                alice.clone(),
                approver(idp, "ali\u{200d}ce"),
                "not in a form that can be compared",
            ),
            (
                "a precomposed letter (NFC)",
                requester(idp, "jos\u{e9}"),
                approver(idp, "bob"),
                "not in a form that can be compared",
            ),
            (
                "the same letter decomposed (NFD)",
                requester(idp, "bob"),
                approver(idp, "jose\u{301}"),
                "not in a form that can be compared",
            ),
            (
                "a fullwidth look-alike (NFKC)",
                alice.clone(),
                approver(idp, "\u{ff41}lice"),
                "not in a form that can be compared",
            ),
            (
                "an empty subject",
                alice.clone(),
                approver(idp, ""),
                "not in a form that can be compared",
            ),
            (
                "an empty issuer",
                alice.clone(),
                approver("", "bob"),
                "not in a form that can be compared",
            ),
            (
                "an issuer with a fragment",
                requester("https://idp.example#x", "alice"),
                approver("https://idp.example#x", "bob"),
                "not in a form that can be compared",
            ),
            (
                "a subject longer than OIDC allows",
                alice.clone(),
                approver(idp, &"b".repeat(MAX_COMPARABLE_SUBJECT_LEN + 1)),
                "not in a form that can be compared",
            ),
            (
                "a control character",
                alice.clone(),
                approver(idp, "bob\u{7}"),
                "not in a form that can be compared",
            ),
            (
                "the local administrator as the requester",
                requester(LOCAL_ADMIN_ISSUER, "admin"),
                approver(LOCAL_ADMIN_ISSUER, "bob"),
                "administrator console",
            ),
            (
                "the local administrator as the approver",
                alice.clone(),
                approver("URN:logweir:local-admin", "admin"),
                "administrator console",
            ),
            (
                "a service account as the requester",
                requester(idp, "system:serviceaccount:team-a:deployer"),
                approver(idp, "bob"),
                "Kubernetes system identity",
            ),
            (
                "a service account as the approver",
                alice.clone(),
                approver(idp, "System:ServiceAccount:team-a:approver"),
                "Kubernetes system identity",
            ),
        ];
        let p = pair();
        for (label, who_asked, who_approved, needle) in refused {
            let alone = console_separation(&who_asked, &who_approved);
            let alone = alone.expect_err(label);
            assert_eq!(alone.reason(), "SelfApprovalRefused", "{label}: {alone}");
            assert!(alone.to_string().contains(needle), "{label}: {alone}");
            // The whole verdict refuses it too, with and without a clock.
            let mut d = approved(&p);
            d.requester = who_asked;
            d.approver = Some(who_approved);
            for verdict in [
                check_binding(&d, &expected(), &p),
                check_restore_authorization(&d, &expected(), &p, at(NOW)),
                check_console_approval(&d, &p, None),
            ] {
                assert_eq!(
                    verdict.map_err(|r| r.reason()),
                    Err("SelfApprovalRefused"),
                    "{label}"
                );
            }
        }
        // A subject at exactly OIDC's bound is comparable.
        assert_eq!(
            console_separation(
                &alice,
                &approver(idp, &"b".repeat(MAX_COMPARABLE_SUBJECT_LEN))
            ),
            Ok(())
        );
        assert_eq!(fold_issuer("https://IdP.example//"), "https://idp.example");
        assert_eq!(fold_subject("Alice"), "alice");
    }

    /// **THE CLOCK RULE.** `issuedAt <= approvedAt < expiresAt`, exactly; and
    /// a reader with a clock refuses an approval from its own future.
    #[test]
    fn an_approval_is_given_inside_the_requests_own_window() {
        let p = pair();
        // issuedAt 10:00:00, expiresAt 10:10:00.
        let verdict = |approved_at: &str, now: Option<&str>| {
            let mut d = approved(&p);
            d.approved_at = Some(at(approved_at));
            check_console_approval(&d, &p, now.map(at)).map_err(|r| r.reason())
        };
        assert_eq!(
            verdict("2026-09-22T10:00:00Z", Some(NOW)),
            Ok(()),
            "at issuedAt"
        );
        assert_eq!(verdict("2026-09-22T10:04:00Z", Some(NOW)), Ok(()));
        assert_eq!(
            verdict("2026-09-22T10:09:59Z", Some("2026-09-22T10:09:59Z")),
            Ok(())
        );
        for (label, approved_at) in [
            ("a second before the request", "2026-09-22T09:59:59Z"),
            ("at the expiry (exclusive)", "2026-09-22T10:10:00Z"),
            ("after the expiry", "2026-09-22T10:10:01Z"),
            ("a day later", "2026-09-23T10:04:00Z"),
        ] {
            for now in [Some(NOW), None] {
                assert_eq!(
                    verdict(approved_at, now),
                    Err("AuthorizationWindowInvalid"),
                    "{label}"
                );
            }
        }
        // A reader's own clock: 60 s of skew is tolerated, 61 s is not; the
        // runner (no clock) reads only the signed instants.
        assert_eq!(
            verdict("2026-09-22T10:04:00Z", Some("2026-09-22T10:03:00Z")),
            Ok(())
        );
        assert_eq!(
            verdict("2026-09-22T10:04:00Z", Some("2026-09-22T10:02:59Z")),
            Err("AuthorizationWindowInvalid")
        );
        assert_eq!(verdict("2026-09-22T10:04:00Z", None), Ok(()));
        // The whole verdict carries the same rule.
        let mut late = approved(&p);
        late.approved_at = Some(at("2026-09-22T10:10:00Z"));
        assert_eq!(
            check_binding(&late, &expected(), &p).map_err(|r| r.reason()),
            Err("AuthorizationWindowInvalid")
        );
        assert_eq!(
            check_restore_authorization(&late, &expected(), &p, at(NOW)).map_err(|r| r.reason()),
            Err("AuthorizationWindowInvalid")
        );
    }

    /// **A two-person REQUEST authorises nothing, and an approver authorises
    /// nothing outside a two-person policy.**
    #[test]
    fn a_request_is_pending_and_an_approver_belongs_to_a_console_policy_alone() {
        let p = pair();
        let now = at(NOW);
        // The request, offered as the approval: pending, at every reader.
        for verdict in [
            check_binding(&request(&p), &expected(), &p),
            check_restore_authorization(&request(&p), &expected(), &p, now),
            check_console_approval(&request(&p), &p, None),
        ] {
            let refused = verdict.expect_err("a request is not an approval");
            assert_eq!(refused.reason(), "GovernedApprovalRequired", "{refused}");
            assert!(
                refused
                    .to_string()
                    .contains("personal-key countersignature is not accepted"),
                "{refused}"
            );
        }
        // ... and an EXPIRED request reads as expired, not as pending.
        assert_eq!(
            check_restore_authorization(&request(&p), &expected(), &p, at("2026-09-22T10:10:00Z"))
                .map_err(|r| r.reason()),
            Err("AuthorizationExpired")
        );
        // NEGATIVE CONTROL: approved, it authorises.
        assert_eq!(check_binding(&approved(&p), &expected(), &p), Ok(()));
        assert_eq!(
            check_restore_authorization(&approved(&p), &expected(), &p, now),
            Ok(())
        );

        // A console-approved document under a policy that is NOT Console,
        // even when it names that policy's own digest: a personal-key policy
        // (strict), and a one-person one (confirm).
        for strict in [
            policy(ApprovalMode::Governed),
            policy(ApprovalMode::Ordinary),
        ] {
            let mut forged = doc(&strict);
            forged.format_version = "2.2.0".into();
            forged.approver = Some(approver("https://idp.example", "bob"));
            forged.approved_at = Some(at("2026-09-22T10:04:00Z"));
            for verdict in [
                check_binding(&forged, &expected(), &strict),
                check_restore_authorization(&forged, &expected(), &strict, now),
            ] {
                let refused = verdict.expect_err("an approver outside a Console policy");
                assert_eq!(refused.reason(), "ApprovalPolicyMismatch", "{refused}");
                assert!(
                    refused.to_string().contains("approverSignature is Console"),
                    "{refused}"
                );
            }
            // NEGATIVE CONTROL: the same policy's own document, unchanged.
            assert_eq!(check_binding(&doc(&strict), &expected(), &strict), Ok(()));
            assert_eq!(
                check_console_approval(&doc(&strict), &strict, Some(now)),
                Ok(())
            );
        }
    }

    /// **THE COMBINATION WITH PROD-15.1.** An original-name restore approved
    /// by a second person carries the subject and the approver, at 2.2.0; the
    /// typed names of a one-person confirmation are refused on it, as on
    /// every `Governed` document.
    #[test]
    fn an_original_name_restore_approved_by_a_second_person_carries_the_subject_and_no_typed_names()
    {
        let p = pair();
        let mut d = approved(&p);
        d.approval_subject = Some("originalName".into());
        assert_eq!(
            restore_authorization_format_version(d.approval_subject.as_deref(), None, true),
            "2.2.0"
        );
        assert_eq!(check_binding(&d, &expected(), &p), Ok(()));
        assert_eq!(
            RestoreAuthorization::from_bytes(&d.to_bytes()),
            Ok(d.clone())
        );
        assert!(String::from_utf8(d.to_bytes()).unwrap().ends_with(
            "\"approvalSubject\":\"originalName\",\"approver\":{\"issuer\":\
             \"https://idp.example\",\"subject\":\"bob\"},\"approvedAt\":\"2026-09-22T10:04:00Z\"}"
        ));
        // Under 2.1.0 (the subject's own version) the approver is refused.
        let mut older = d.clone();
        older.format_version = "2.1.0".into();
        assert_eq!(
            check_binding(&older, &expected(), &p).map_err(|r| r.reason()),
            Err("AuthorizationDocumentInvalid")
        );
        // Typed names beside it: refused, the rule every Governed document has.
        let mut typed = d.clone();
        typed.original_name_confirmation = Some(crate::original_name::OriginalNameConfirmation {
            typed_topics: vec!["orders".into()],
        });
        let refused = check_binding(&typed, &expected(), &p).expect_err("typed names");
        assert_eq!(refused.reason(), "AuthorizationDocumentInvalid");
        assert!(
            refused
                .to_string()
                .contains(crate::original_name::ORIGINAL_NAME_CONFIRMATION_NOT_ACCEPTED),
            "{refused}"
        );
    }

    /// **WHAT THE CONSOLE READS BEFORE IT SIGNS AN APPROVAL.** A request its
    /// own signature verified is still held to the Restore, the plan and the
    /// policy the click is about — which is what refuses a signature copied
    /// from another request.
    #[test]
    fn a_stored_request_is_held_to_this_restore_plan_and_policy_before_it_is_approved() {
        let p = pair();
        let now = at(NOW);
        assert_eq!(
            check_request_binding(&request(&p), &expected(), &p, now),
            Ok(())
        );
        let mut cases: Vec<(&str, RestoreAuthorization, &str)> = Vec::new();
        let mut d = request(&p);
        d.subject.namespace = "team-b".into();
        cases.push(("another namespace", d, "AuthorizationSubjectMismatch"));
        let mut d = request(&p);
        d.subject.name = "rst-2".into();
        cases.push(("another Restore", d, "AuthorizationSubjectMismatch"));
        let mut d = request(&p);
        d.subject.uid = "uid-2".into();
        cases.push(("a recreated Restore", d, "AuthorizationSubjectMismatch"));
        let mut d = request(&p);
        d.plan_hash = format!("sha256:{}", "b".repeat(64));
        cases.push(("another plan", d, "PlanHashMismatch"));
        let mut d = request(&p);
        d.policy.digest = format!("sha256:{}", "c".repeat(64));
        cases.push(("another policy digest", d, "ApprovalPolicyMismatch"));
        let mut d = request(&p);
        d.policy.name = "prod-governed".into();
        cases.push(("another policy", d, "ApprovalPolicyMismatch"));
        let mut d = request(&p);
        d.authorization_mode = ApprovalMode::Ordinary;
        d.ticket = None;
        cases.push(("another mode", d, "ApprovalPolicyMismatch"));
        let mut d = request(&p);
        d.ticket = None;
        cases.push(("no ticket", d, "AuthorizationDocumentInvalid"));
        let mut d = request(&p);
        d.expires_at = d.issued_at + chrono::Duration::seconds(p.max_age_seconds + 1);
        cases.push((
            "a window longer than the policy",
            d,
            "AuthorizationWindowInvalid",
        ));
        cases.push((
            "an approval offered as a request",
            approved(&p),
            "AuthorizationDocumentInvalid",
        ));
        for (label, d, reason) in cases {
            assert_eq!(
                check_request_binding(&d, &expected(), &p, now).map_err(|r| r.reason()),
                Err(reason),
                "{label}"
            );
        }
        // The clock.
        assert_eq!(
            check_request_binding(&request(&p), &expected(), &p, at("2026-09-22T10:10:00Z"))
                .map_err(|r| r.reason()),
            Err("AuthorizationExpired")
        );
        assert_eq!(
            check_request_binding(&request(&p), &expected(), &p, at("2026-09-22T09:58:59Z"))
                .map_err(|r| r.reason()),
            Err("AuthorizationWindowInvalid")
        );
        // And never under a policy whose approval is a personal key.
        let strict = policy(ApprovalMode::Governed);
        assert_eq!(
            check_request_binding(&doc(&strict), &expected(), &strict, now).map_err(|r| r.reason()),
            Err("ApprovalPolicyMismatch")
        );
    }

    #[test]
    fn the_set_digest_moves_with_any_binding_or_policy() {
        let base = set().digest();
        let rebound =
            ApprovalPolicySet::parse(&DOC.replace("prod: prod-governed", "prod: team-ordinary"))
                .unwrap_or_default();
        assert_ne!(base, rebound.digest());
        let longer =
            ApprovalPolicySet::parse(&DOC.replace("maxAgeSeconds: 3600", "maxAgeSeconds: 3601"))
                .unwrap_or_default();
        assert_ne!(base, longer.digest());
        assert_eq!(base, set().digest());
    }
}
