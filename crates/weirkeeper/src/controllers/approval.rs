//! The `Approval` reconciler: five checks, in an order that is testable.
//!
//! An `Approval` that exists is not an approval; one whose status the
//! controller set to `Verified=True` is. Creating an `Approval` object is a
//! `POST` any namespace tenant can make. What makes it an authorisation is a
//! DSSE signature, by a key the namespace's trust lists, over bytes that bind
//! this plan and this kind of subject.
//!
//! # The order is load-bearing
//!
//! [`logweir_verify::verify_detached`] refuses a `payloadType` mismatch first
//! and returns `"no signature by key <want> in the sidecar"` for a key not in
//! the sidecar at all, both as `logweir_verify::Error::Verify`. A loop over the
//! roster calling `verify_detached` would report the same refusal for a
//! substituted document, an attacker's key and a broken signature, three
//! findings an operator must tell apart. [`evaluate`] makes its own
//! distinctions before it reaches the crypto, and the checks are numbered in
//! the code.
//!
//! # `approvalBytes` is document text, never base64
//!
//! Interface **I18**. The reconciler passes
//! `approval.spec.approvalBytes.as_bytes()` straight into [`evaluate`] with no
//! decode step, and the Restore admission hashes exactly those bytes. A base64
//! layer is the class of transformation `planBytes` exists to forbid;
//! `approval_bytes_are_the_document_text_not_base64` asserts the raw text
//! verifies and its base64 does not, so a decode step cannot be added
//! silently.
//!
//! # What this module does not claim
//!
//! It verifies. It holds no key material, and this crate links
//! [`logweir_verify`] and never the signing crate
//! (`scripts/check-one-signer.sh`). That is a linkage property only: the
//! capability to sign is unbroken while this controller holds Job CRUD over the
//! signing key's namespace.
//!
//! [`Approval`]: crate::crds::approval::Approval
//! [`TrustRoster`]: crate::crds::trust_roster::TrustRoster

use std::fmt;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use futures::StreamExt as _;
use kube::runtime::controller::Action;
use kube::runtime::reflector::ObjectRef;
use kube::runtime::{reflector, watcher, Controller};
use kube::{Api, Resource, ResourceExt};
use logweir_core::approval_policy::{
    self as policy, ApprovalPolicy, ApprovalPolicySet, AuthorizationRefusal, EffectivePolicy,
    ExpectedSubject, RestoreAuthorization, PAYLOAD_TYPE_RESTORE_AUTHORIZATION,
};
use logweir_core::ids::sha256_prefixed;
use logweir_core::trust::{KeyUsage, SigningRefusal};
use logweir_verify::{verify_detached, Sidecar, VerifyingKey};
use serde::Deserialize;
use serde_json::json;
use tracing::{debug, info, warn};

use super::Context;
use crate::conditions::{current_condition, merge_condition, StatusVersion};
use crate::crds::approval::{
    Approval, ApprovalStatus, ApproverKeyWindow, AuthorizationProvenance, SubjectKind,
    VerifiedSubjectRef,
};
use crate::crds::backup::Backup;
use crate::crds::restore::Restore;
use crate::crds::trust_roster::TrustRoster;
use crate::crds::Condition;
use crate::trust::{Resolution, ResolvedKey, ResolvedTrust, REASON_TRUST_POLICY_CONFLICT};

/// The DSSE `payloadType` an approval document is signed under.
///
/// Byte-equal to the CLI's constant in `crates/logweir/src/drill/phase1_approval.rs`:
/// the same document is verified by the CLI at phase 1 and by this controller.
/// `the_payload_type_is_byte_equal_to_the_cli` reads that file and compares.
pub const PAYLOAD_TYPE_APPROVAL: &str = "application/vnd.logweir.drill-approval+json;version=1.0.0";

/// The name of the one cluster-scoped [`TrustRoster`] (interface **I16**).
///
/// A fact, not a convention: it is never read from an `Approval`, a flag or an
/// environment variable, because a roster whose name the subject supplies is a
/// roster the subject can choose. Re-exported as [`crate::ROSTER_NAME`]. A
/// roster named otherwise gets [`ROSTER_NOT_FOUND_MESSAGE`].
/// [`TrustRoster`]: crate::crds::trust_roster::TrustRoster
pub const ROSTER_NAME: &str = "default";

/// The condition type this reconciler owns.
pub const CONDITION_VERIFIED: &str = "Verified";

/// The `reason` written when every check passed.
pub const REASON_VERIFIED: &str = "Verified";

/// The condition type recording that the Restore this `Approval` authorises
/// was admitted under its verdict.
///
/// Expiry bounds the time to admission and nothing after it: once a Job is
/// created, the run continues under its recorded policy snapshot. A document's
/// `expiresAt`, the policy binding and the keys' windows are questions about a
/// new use. Without this condition a consumed `Approval` was re-judged every
/// pass: a succeeded Restore's `Approval` was rewritten
/// `Verified=False/AuthorizationExpired`, its record nulled, and (the message
/// carrying the clock) a new status written every ~2 s for ever.
///
/// Once `Consumed=True` is on the object the verdict beside it is the audit
/// record of that admission: never re-evaluated or rewritten, and binding only
/// the Restore UID `status.verifiedSubjectRef` names, the replay fence that
/// refuses every other object.
pub const CONDITION_CONSUMED: &str = "Consumed";

/// The `reason` on [`CONDITION_CONSUMED`].
pub const REASON_RESTORE_ADMITTED: &str = "RestoreAdmitted";

/// The `Verified` reason on a consumed `Approval` one of whose keys was
/// revoked for `KeyCompromise` after this controller recorded the admission.
///
/// Never green: `status.verified` is `false` beside it. It preserves the order
/// of two recorded facts (the admission was observed before the revocation
/// took effect), which an investigation needs and a badge must not turn into
/// a pass. The record (authorization, key id, approver, `Consumed`) is kept.
pub const REASON_RECORDED_BEFORE_REVOCATION: &str = "RecordedBeforeRevocation";

/// The `Verified` reason on a consumed `Approval` whose key was revoked for
/// `KeyCompromise` effective at or before the recorded admission. The spelling
/// [`ApprovalRefusal::KeyRevoked`] writes for a fresh approval.
pub const REASON_KEY_REVOKED: &str = "KeyRevoked";

/// The message a missing roster produces (interface **I16**), verbatim.
///
/// Never a silent refusal: the likeliest cause is an install that skipped a
/// step, and a bare `RosterNotFound` would send the operator to their key.
/// `the_missing_roster_message_names_the_roster` asserts it contains
/// [`ROSTER_NAME`].
pub const ROSTER_NOT_FOUND_MESSAGE: &str =
    "no cluster-scoped TrustRoster named 'default'; see docs/kubernetes.md install step 1";

/// Why an `Approval` was refused.
///
/// The set is closed: the Restore admission, verification and the console
/// route on [`Self::reason`], so a variant is an interface.
///
/// `key_id` on the two roster variants is an identification string, not
/// always one id: check 3 names the declared id and the hash of the key
/// material it claims, and check 4 the sidecar's and the trust's lists. Each
/// must name every id it compared (`approval_refuses_a_key_outside_the_roster`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ApprovalRefusal {
    /// The signature did not verify, the sidecar did not parse, or the roster
    /// could not be loaded in full. Carries the reason.
    SignatureInvalid(String),
    /// No key on the roster's `approverKeys` matches, or an entry disagrees
    /// with its own key material.
    KeyIdNotInRoster {
        /// Every key id the comparison involved — see the type-level note.
        key_id: String,
    },
    /// The matched key is past its `notAfter`.
    KeyIdExpired {
        /// The matched roster entry's `keyId`.
        key_id: String,
        /// That entry's `notAfter`, RFC 3339.
        not_after: String,
    },
    /// The matched key is declared `Retired`.
    ///
    /// Not [`Self::KeyIdExpired`]: it refuses the same way but not for the
    /// same reason, and a retired key has not reached its `notAfter`.
    /// Reporting a deliberate retirement as an expiry would send an operator to
    /// extend a window that is not the problem.
    KeyRetired {
        /// The matched key's `keyId`.
        key_id: String,
        /// Its `retiredAt`, RFC 3339, when the policy records one.
        retired_at: Option<String>,
    },
    /// The matched key is declared `Revoked`.
    ///
    /// Its own reason, because a revocation must never read as an expiry:
    /// `KeyCompromise` means the private half may be in someone else's hands,
    /// and the remedy is an investigation, not a longer `notAfter`.
    KeyRevoked {
        /// The matched key's `keyId`.
        key_id: String,
        /// `KeyCompromise`, `Superseded` or `Unspecified`.
        reason: String,
        /// `revocationEffectiveFrom`, RFC 3339, when the policy records one.
        effective_from: Option<String>,
    },
    /// The matched key's validity window has not opened yet.
    ///
    /// Reachable only under a real `TrustPolicy`, where a successor key is
    /// staged with a future `notBefore`; the synthesised `legacy-roster-v1`
    /// gives every key a `notBefore` at the Unix epoch.
    KeyNotYetValid {
        /// The matched key's `keyId`.
        key_id: String,
        /// Its `notBefore`, RFC 3339.
        not_before: String,
    },
    /// Two or more `TrustPolicy` objects claim this namespace, so it resolves
    /// to nothing.
    ///
    /// Not a verdict about the approver: picking one of two contesting policies
    /// would be a trust decision made by a sort order. The same word appears on
    /// both policies' `status.conflicts`.
    TrustPolicyConflict {
        /// The contested namespace.
        namespace: String,
        /// Every policy claiming it, sorted.
        policies: Vec<String>,
    },
    /// The sidecar is a genuinely-signed sidecar for a DIFFERENT kind of
    /// document, handed over in place of an approval.
    PayloadTypeMismatch {
        /// What the sidecar's `payloadType` says.
        got: String,
        /// [`PAYLOAD_TYPE_APPROVAL`].
        want: String,
    },
    /// The approval names a different plan than the referent carries.
    PlanHashMismatch {
        /// The hash the approval document names.
        got: String,
        /// The hash of the referent's own `spec.planBytes`, recomputed here.
        want: String,
    },
    /// The approval binds one kind of subject and the referent is another.
    SubjectKindMismatch {
        /// The `subject_kind` inside the signed bytes.
        approval_says: String,
        /// The referent's actual kind.
        referent_is: String,
    },
    /// `spec.subjectRef.kind` is `RehearsalSchedule`: a standing rehearsal
    /// authorization, withdrawn in v1, so such an `Approval` is refused by name
    /// before its trust, signature or referent is read, and authorises nothing.
    StandingAuthorizationRefused {
        /// The name `spec.subjectRef.name` gives.
        name: String,
    },
    /// The authentic bytes are not a well-formed authorization document v2, or
    /// name no requester.
    AuthorizationDocumentInvalid {
        /// The parse, format or requester failure.
        detail: String,
    },
    /// A v2 document names another object than this referent: another
    /// namespace, name or UID.
    AuthorizationSubjectMismatch {
        /// The signed and observed identities.
        detail: String,
    },
    /// The document's policy, digest or mode is not the namespace's current
    /// binding, including a v1 document under an explicit binding and a v2
    /// document in an unbound namespace. The downgrade refusal: an ordinary
    /// confirmation presented in a governed namespace lands here.
    ApprovalPolicyMismatch {
        /// Both policies, named.
        detail: String,
    },
    /// The document's validity window is malformed, longer than the policy
    /// allows, or issued in the future.
    AuthorizationWindowInvalid {
        /// The exact boundary.
        detail: String,
    },
    /// The document's `expiresAt` has passed.
    AuthorizationExpired {
        /// The instant and the clock.
        detail: String,
    },
    /// A `Governed` document carries the console's confirmation and no
    /// approver countersignature yet. Not a bad signature: the pending state of
    /// a governed request, in which a console-only document written directly
    /// to the API server stays for ever.
    GovernedApprovalRequired {
        /// The requester and policy the confirmation names.
        detail: String,
    },
    /// The governed approver's principal is the requester's: self-approval,
    /// refused where the policy requires independence.
    SelfApprovalRefused {
        /// Both principals.
        detail: String,
    },
    /// There is no cluster-scoped [`TrustRoster`] named [`ROSTER_NAME`].
    ///
    /// [`evaluate`] never returns it (it takes a [`TrustRosterSpec`], so a
    /// roster exists); the reconciler produces it, so it lives in the same
    /// closed set the condition reasons come from.
    ///
    /// [`TrustRoster`]: crate::crds::trust_roster::TrustRoster
    RosterNotFound,
}

impl ApprovalRefusal {
    /// The condition `reason` for this refusal: the variant's own name.
    ///
    /// No wildcard arm: a new variant must fail to compile here rather than
    /// reach the cluster as a `reason` nobody chose.
    #[must_use]
    pub const fn reason(&self) -> &'static str {
        match self {
            Self::SignatureInvalid(_) => "SignatureInvalid",
            Self::KeyIdNotInRoster { .. } => "KeyIdNotInRoster",
            Self::KeyIdExpired { .. } => "KeyIdExpired",
            Self::KeyRetired { .. } => "KeyRetired",
            Self::KeyRevoked { .. } => "KeyRevoked",
            Self::KeyNotYetValid { .. } => "KeyNotYetValid",
            Self::TrustPolicyConflict { .. } => REASON_TRUST_POLICY_CONFLICT,
            Self::PayloadTypeMismatch { .. } => "PayloadTypeMismatch",
            Self::PlanHashMismatch { .. } => "PlanHashMismatch",
            Self::SubjectKindMismatch { .. } => "SubjectKindMismatch",
            Self::StandingAuthorizationRefused { .. } => "StandingAuthorizationRefused",
            Self::AuthorizationDocumentInvalid { .. } => "AuthorizationDocumentInvalid",
            Self::AuthorizationSubjectMismatch { .. } => "AuthorizationSubjectMismatch",
            Self::ApprovalPolicyMismatch { .. } => "ApprovalPolicyMismatch",
            Self::AuthorizationWindowInvalid { .. } => "AuthorizationWindowInvalid",
            Self::AuthorizationExpired { .. } => "AuthorizationExpired",
            Self::GovernedApprovalRequired { .. } => "GovernedApprovalRequired",
            Self::SelfApprovalRefused { .. } => "SelfApprovalRefused",
            Self::RosterNotFound => "RosterNotFound",
        }
    }
}

impl fmt::Display for ApprovalRefusal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::SignatureInvalid(why) => write!(f, "{why}"),
            Self::KeyIdNotInRoster { key_id } => write!(
                f,
                "no key on the TrustRoster '{ROSTER_NAME}' approverKeys authorises this \
                 approval: {key_id}"
            ),
            Self::KeyIdExpired { key_id, not_after } => write!(
                f,
                "the approval verified under key id {key_id}, whose notAfter {not_after} has \
                 passed; a key past notAfter does not authorise anything"
            ),
            Self::KeyRetired { key_id, retired_at } => write!(
                f,
                "the approval verified under key id {key_id}, which the resolved trust policy \
                 records as Retired{}; a retired key authorises nothing new, however green the \
                 archives it signed remain",
                match retired_at {
                    Some(at) => format!(" at {at}"),
                    None => String::new(),
                }
            ),
            Self::KeyRevoked {
                key_id,
                reason,
                effective_from,
            } => write!(
                f,
                "the approval verified under key id {key_id}, which the resolved trust policy \
                 records as Revoked ({reason}){}; a revoked key is not an expired key and \
                 authorises nothing",
                match effective_from {
                    Some(at) => format!(", effective {at}"),
                    None => String::new(),
                }
            ),
            Self::KeyNotYetValid { key_id, not_before } => write!(
                f,
                "the approval verified under key id {key_id}, whose notBefore {not_before} has \
                 not arrived; a key staged for a future rotation authorises nothing yet"
            ),
            Self::TrustPolicyConflict {
                namespace,
                policies,
            } => write!(
                f,
                "the namespace {namespace} is claimed by more than one TrustPolicy ({}), so it \
                 resolves to no trust at all and no approval in it is accepted; remove the \
                 namespace from all but one policy",
                policies.join(", ")
            ),
            Self::PayloadTypeMismatch { got, want } => write!(
                f,
                "the sidecar's payloadType is {got}, not {want} — this is a signed sidecar for \
                 a different kind of document, not a bad signature"
            ),
            Self::PlanHashMismatch { got, want } => write!(
                f,
                "the approval names plan hash {got} but the referent's spec.planBytes hash to \
                 {want}; the hash inside the signed document AND the spec.planHash beside it must \
                 both be the plan this approval authorises, so re-approve the exact plan you \
                 intend to run and record it with that hash"
            ),
            Self::SubjectKindMismatch {
                approval_says,
                referent_is,
            } => write!(
                f,
                "the approval binds subject kind {approval_says:?} and the referent is a \
                 {referent_is}; an approval for one kind never authorises another"
            ),
            Self::StandingAuthorizationRefused { name } => write!(
                f,
                "spec.subjectRef names RehearsalSchedule '{name}': a standing rehearsal \
                 authorization, and standing authorizations are withdrawn in v1 (scheduled \
                 rehearsals with them), so this Approval authorises nothing; approve each \
                 Restore with its own per-run Approval"
            ),
            Self::AuthorizationDocumentInvalid { detail }
            | Self::AuthorizationSubjectMismatch { detail }
            | Self::ApprovalPolicyMismatch { detail }
            | Self::AuthorizationWindowInvalid { detail }
            | Self::AuthorizationExpired { detail }
            | Self::GovernedApprovalRequired { detail }
            | Self::SelfApprovalRefused { detail } => write!(f, "{detail}"),
            Self::RosterNotFound => write!(f, "{ROSTER_NOT_FOUND_MESSAGE}"),
        }
    }
}

impl From<AuthorizationRefusal> for ApprovalRefusal {
    /// The shared v2 refusal classes onto this controller's closed set. The
    /// reason strings are the same on both sides
    /// (`AuthorizationRefusal::reason`), so this condition and a Restore
    /// admission refusal from the same check read alike.
    fn from(refusal: AuthorizationRefusal) -> Self {
        let detail = refusal.to_string();
        match refusal {
            AuthorizationRefusal::DocumentInvalid(_) => {
                Self::AuthorizationDocumentInvalid { detail }
            }
            AuthorizationRefusal::SubjectMismatch(_) => {
                Self::AuthorizationSubjectMismatch { detail }
            }
            AuthorizationRefusal::PlanHashMismatch { got, want } => {
                Self::PlanHashMismatch { got, want }
            }
            AuthorizationRefusal::PolicyMismatch(_) => Self::ApprovalPolicyMismatch { detail },
            AuthorizationRefusal::WindowInvalid(_) => Self::AuthorizationWindowInvalid { detail },
            AuthorizationRefusal::Expired(_) => Self::AuthorizationExpired { detail },
            // A two-person request nobody has approved is the same pending
            // state as a strict request without its countersignature, and an
            // approver who is not a second person earns the self-approval
            // refusal.
            AuthorizationRefusal::ApprovalRequired(_) => Self::GovernedApprovalRequired { detail },
            AuthorizationRefusal::SelfApproval(_) => Self::SelfApprovalRefused { detail },
        }
    }
}

/// The whole verdict, when every check passed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Verified {
    /// The `keyId` of the trust entry whose key actually verified the
    /// signature: the value [`verify_detached`] returned, never
    /// `sidecar.signatures[0].keyid`.
    pub matched_key_id: String,
    /// The policy usage under which this build admitted the signature. A v1
    /// approval document requires `GovernedApproval`, and
    /// `ConsoleConfirmation` fails closed.
    pub authorization_usage: KeyUsage,
    /// The matched key's declared validity window; `None` only when the
    /// resolved trust does not carry the matched id, which check 6 makes
    /// unreachable.
    ///
    /// One fact, two consumers: [`Self::valid_until`] requeues at the key's
    /// boundary rather than the next heartbeat (or an expired key's approval
    /// keeps reading `Verified=True`), and [`status_for`] publishes it so the
    /// restore preflight can compare a deadline against it. One field, so the
    /// published window and the requeue deadline cannot disagree.
    pub key_window: Option<ApproverKeyWindow>,
    /// The authenticated document's own expiry, when this document kind has
    /// one (an authorization document v2). The verdict is valid only until the
    /// earlier of this instant and the matched key's `notAfter`.
    pub document_expires_at: Option<DateTime<Utc>>,
    /// The approver named inside the signed bytes.
    pub approver: String,
    /// The change ticket named inside the signed bytes.
    pub ticket: String,
    /// Whether the matched approver key id also appears in
    /// `roster.spec.signingKeys[].keyId`.
    ///
    /// `false` means only "two different key ids" — one operator holding both
    /// keys satisfies it (`design-operator.md:169-181`). It is LABELLED, never
    /// refused.
    pub self_attested_risk: bool,
    /// Which trust answered: a `TrustPolicy`'s `metadata.name`, or
    /// `legacy-roster-v1` for the synthesised roster. It reaches the condition
    /// message as `trustSource=<name>`, which tells an operator looking at one
    /// `Approval` whether their migration is live.
    pub trust_source: String,
    /// The policy, mode and console-attested requester a v2 authorization was
    /// verified under; `None` for a v1 approval document. Boxed so
    /// `ApprovalOutcome`'s variants stay comparable in size.
    pub authorization: Option<Box<AuthorizationProvenance>>,
    /// Filled by [`decide`] with the API object whose bytes were checked.
    /// Pure [`evaluate`] callers have no Kubernetes referent and leave it
    /// absent; only a reconcile outcome is written to status.
    pub verified_subject_ref: Option<VerifiedSubjectRef>,
}

impl Verified {
    /// The instant this verdict stops being true on its own: the earlier of
    /// the authenticated document's expiry and the matched key's `notAfter`.
    ///
    /// A `None` window cannot happen (check 6 resolved the id), but an
    /// admission path may not abort on an invariant it merely believes: the
    /// timer falls back to the heartbeat and the status publishes no window,
    /// which is unknown, never valid.
    #[must_use]
    pub fn valid_until(&self) -> DateTime<Utc> {
        let key = self
            .key_window
            .as_ref()
            .map_or(DateTime::<Utc>::MAX_UTC, |window| window.not_after);
        match self.document_expires_at {
            Some(document) if document < key => document,
            Some(_) | None => key,
        }
    }
}

/// The approval document, as this controller reads it.
///
/// Every field defaults, which is fail-closed: `plan_hash` and `subject_kind`
/// are compared with values the controller computes, so an absent field is
/// `""`, which matches nothing, and check 7 or 8 refuses naming what the
/// document carried. `required` fields would turn that into a parse error that
/// reads as "the file is corrupt".
///
/// `subject_kind` is written by `logweir drill approve` inside the signed
/// bytes, which is the only reason check 8 is worth anything.
#[derive(Debug, Deserialize)]
struct ApprovalDocument {
    #[serde(default)]
    approver: String,
    #[serde(default)]
    ticket: String,
    #[serde(default)]
    plan_hash: String,
    #[serde(default)]
    subject_kind: String,
}

/// The whole verdict, as a pure function of bytes. No cluster access.
///
/// In order:
///
/// 0. Parse `sidecar_bytes` as a [`Sidecar`]; a parse failure is
///    [`ApprovalRefusal::SignatureInvalid`] naming the parse error.
/// 1. `sidecar.payload_type != PAYLOAD_TYPE_APPROVAL` →
///    [`ApprovalRefusal::PayloadTypeMismatch`] with both strings in full,
///    before any key is tried, so it is distinguishable from a bad signature.
/// 2. Every approver key parses, or [`ApprovalRefusal::SignatureInvalid`]
///    naming the failing `keyId`: a partially loaded roster is not a roster.
/// 3. Each entry's `keyId == key.key_id()`, or
///    [`ApprovalRefusal::KeyIdNotInRoster`] naming both, before any signature
///    is checked: an entry whose id is not the hash of its own key material is
///    not one a signature can be looked up in.
/// 4. If no `sidecar.signatures[].keyid` equals any entry's key id,
///    [`ApprovalRefusal::KeyIdNotInRoster`] naming the sidecar's key ids: this,
///    not a signature failure, is the verdict for a key outside the trust.
/// 5. [`verify_detached`] for each entry whose key id matched; the first `Ok`
///    wins, otherwise [`ApprovalRefusal::SignatureInvalid`] with the last
///    error's `Display`.
/// 6. The matched key may sign something new at `now`.
/// 7. The document's `plan_hash` equals `sha256_prefixed(referent_plan_bytes)`,
///    recomputed here and never read from the referent's status.
/// 8. The document's `subject_kind` equals `referent_kind`.
///
/// Steps 1 to 6 are answerable from the sidecar and the trust alone, so each
/// has a test that cannot pass by accident
/// (`approval_refuses_a_wrong_payload_type` passes a deliberately unparseable
/// key, so reaching step 2 would change the verdict). By step 7 the bytes are
/// authentic, so a parse failure means "this authentic document is not an
/// approval document", which its message says.
///
/// Check 8 keeps an `Approval` whose plan hash matches one kind of subject from
/// authorising another kind.
/// # Errors
///
/// Every refusal is an [`ApprovalRefusal`]; see the order above for which
/// check produces which.
pub fn evaluate(
    approval_bytes: &[u8],
    sidecar_bytes: &[u8],
    trust: &ResolvedTrust,
    now: DateTime<Utc>,
    referent_kind: &str,
    referent_plan_bytes: &[u8],
) -> Result<Verified, ApprovalRefusal> {
    evaluate_inner(
        approval_bytes,
        sidecar_bytes,
        trust,
        now,
        referent_kind,
        referent_plan_bytes,
    )
}

fn evaluate_inner(
    approval_bytes: &[u8],
    sidecar_bytes: &[u8],
    trust: &ResolvedTrust,
    now: DateTime<Utc>,
    referent_kind: &str,
    referent_plan_bytes: &[u8],
) -> Result<Verified, ApprovalRefusal> {
    // 0. The sidecar is a document before it is a signature.
    let sidecar: Sidecar = serde_json::from_slice(sidecar_bytes).map_err(|e| {
        ApprovalRefusal::SignatureInvalid(format!(
            "spec.sidecarBytes is not a DSSE sidecar document: {e}"
        ))
    })?;

    // 1. The payload type, before any key is tried. `verify_detached` covers
    // it too, so a genuinely signed document of another kind is refused by the
    // signature layer even if this comparison were forgotten.
    let want_payload = PAYLOAD_TYPE_APPROVAL;
    if sidecar.payload_type != want_payload {
        return Err(ApprovalRefusal::PayloadTypeMismatch {
            got: sidecar.payload_type,
            want: want_payload.to_string(),
        });
    }

    // 2. The whole approver key set parses, or nothing is accepted. Carried as
    // data by the resolution layer: the rule is the roster's ("a partially
    // loaded roster is not a roster"); a real `TrustPolicy` reports one
    // unparseable key and keeps the others working. `blocked_for` carries the
    // legacy message verbatim.
    //
    // A v1 approval document carries no ordinary/governed mode, and admitting
    // a ConsoleConfirmation key would let an ordinary confirmation authorize
    // governed work, so a v1 document requires GovernedApproval.
    let allowed_usages: &[KeyUsage] = &[KeyUsage::GovernedApproval];
    if let Some(message) = allowed_usages
        .iter()
        .find_map(|usage| trust.blocked_for(*usage))
    {
        return Err(ApprovalRefusal::SignatureInvalid(message.to_string()));
    }
    let declared: Vec<&ResolvedKey> = trust
        .keys
        .iter()
        .filter(|k| allowed_usages.iter().any(|usage| k.trust.has_usage(*usage)))
        .collect();
    let usage_names = join(allowed_usages.iter().map(|usage| usage.as_str()));

    // 3. An entry must agree with its own key material.
    for entry in &declared {
        if let Err(computed) = &entry.declared_id_matches {
            return Err(ApprovalRefusal::KeyIdNotInRoster {
                key_id: format!(
                    "{} entry declares keyId {} but its own spkiPem hashes to {computed}",
                    trust_object(trust),
                    entry.trust.key_id
                ),
            });
        }
    }

    // 4. A key outside the resolved trust is not a signature failure.
    let matching: Vec<&&ResolvedKey> = declared
        .iter()
        .filter(|k| k.is_usable() && sidecar.signatures.iter().any(|s| s.keyid == k.trust.key_id))
        .collect();
    if matching.is_empty() {
        return Err(ApprovalRefusal::KeyIdNotInRoster {
            key_id: format!(
                "the sidecar names [{}] and {}'s approval keys for [{}] are [{}]",
                join(sidecar.signatures.iter().map(|s| s.keyid.as_str())),
                trust_object(trust),
                usage_names,
                join(declared.iter().map(|k| k.trust.key_id.as_str())),
            ),
        });
    }

    // 5. The crypto, and only now. `verify_detached` returns the matched keyid,
    // never `signatures[0]`: a sidecar carrying one trusted and one attacker's
    // signature verifies under exactly one, and only that one may be reported.
    let mut last_error: Option<String> = None;
    let mut hit: Option<String> = None;
    for entry in matching {
        let Ok(key) = VerifyingKey::from_pem_str(&entry.spki_pem) else {
            // Unreachable: `is_usable()` already required a parsed PEM. Not
            // unwrapped, because a panic in an admission path is a denial of
            // service.
            continue;
        };
        match verify_detached(&key, want_payload, approval_bytes, &sidecar) {
            Ok(matched_key_id) => {
                hit = Some(matched_key_id);
                break;
            }
            Err(e) => last_error = Some(e.to_string()),
        }
    }
    let matched_key_id = match hit {
        Some(v) => v,
        None => {
            return Err(ApprovalRefusal::SignatureInvalid(
                last_error.unwrap_or_else(|| {
                    "no signature in the sidecar verified over spec.approvalBytes".to_string()
                }),
            ))
        }
    };

    // 6. The matched key must be able to sign something new: admission is a
    // new use. A signature by a key retired last week asks whether that key may
    // authorise something now, not whether an archive it signed still
    // verifies. The matched key's lifecycle, not the first key's.
    let matched_usage = allowed_usages
        .iter()
        .copied()
        .find(|usage| {
            trust
                .key(&matched_key_id)
                .is_some_and(|key| key.trust.has_usage(*usage))
        })
        .ok_or_else(|| ApprovalRefusal::KeyIdNotInRoster {
            key_id: format!(
                "{} offers no approval key for [{}] with id {matched_key_id}",
                trust_object(trust),
                usage_names
            ),
        })?;
    if let Err(refusal) = trust.may_sign_new_for(&matched_key_id, matched_usage, now) {
        return Err(signing_refusal(
            trust,
            &matched_key_id,
            matched_usage,
            refusal,
        ));
    }

    // The bytes are authentic; now read the document.
    let (approver, ticket, document_expires_at) = {
        let doc: ApprovalDocument = serde_json::from_slice(approval_bytes).map_err(|e| {
            ApprovalRefusal::SignatureInvalid(format!(
                "the signature over spec.approvalBytes verified under key id {matched_key_id}, but \
                 those bytes are not an approval document: {e}"
            ))
        })?;

        // 7. The plan hash, recomputed from the referent's own bytes.
        let want = sha256_prefixed(referent_plan_bytes);
        if doc.plan_hash != want {
            return Err(ApprovalRefusal::PlanHashMismatch {
                got: doc.plan_hash,
                want,
            });
        }

        // 8. The subject kind, from inside the signed bytes.
        if doc.subject_kind != referent_kind {
            return Err(ApprovalRefusal::SubjectKindMismatch {
                approval_says: doc.subject_kind,
                referent_is: referent_kind.to_string(),
            });
        }
        (doc.approver, doc.ticket, None)
    };

    // Labelled, never refused. Under a real `TrustPolicy` it is impossible: a
    // CEL rule makes a policy key declare exactly one usage, so only the
    // synthesised `legacy-roster-v1`, whose two lists may name the same key,
    // can set it.
    let self_attested_risk = trust
        .key(&matched_key_id)
        .is_some_and(|k| k.trust.has_usage(KeyUsage::EvidenceSigning));

    // The matched key's own window, present because check 6 resolved this id.
    // Never another key's: that would be a deadline about a key that
    // authorised nothing.
    let key_window = trust.key(&matched_key_id).map(|k| ApproverKeyWindow {
        key_id: matched_key_id.clone(),
        not_before: k.trust.not_before,
        not_after: k.trust.not_after,
    });

    Ok(Verified {
        key_window,
        matched_key_id,
        authorization_usage: matched_usage,
        document_expires_at,
        approver,
        ticket,
        self_attested_risk,
        trust_source: trust.source.name().to_string(),
        authorization: None,
        verified_subject_ref: None,
    })
}

/// One required signature, found and verified under a key of `usage` from the
/// resolved trust: the v2 twin of [`evaluate_inner`]'s checks 2-6. Only keys
/// declaring `usage` are offered, so a key of another usage can never satisfy
/// it.
fn verify_signature_under(
    trust: &ResolvedTrust,
    usage: KeyUsage,
    sidecar: &Sidecar,
    approval_bytes: &[u8],
    now: DateTime<Utc>,
) -> Result<String, ApprovalRefusal> {
    if let Some(message) = trust.blocked_for(usage) {
        return Err(ApprovalRefusal::SignatureInvalid(message.to_string()));
    }
    let declared: Vec<&ResolvedKey> = trust
        .keys
        .iter()
        .filter(|k| k.trust.has_usage(usage))
        .collect();
    for entry in &declared {
        if let Err(computed) = &entry.declared_id_matches {
            return Err(ApprovalRefusal::KeyIdNotInRoster {
                key_id: format!(
                    "{} entry declares keyId {} but its own spkiPem hashes to {computed}",
                    trust_object(trust),
                    entry.trust.key_id
                ),
            });
        }
    }
    let matching: Vec<&&ResolvedKey> = declared
        .iter()
        .filter(|k| k.is_usable() && sidecar.signatures.iter().any(|s| s.keyid == k.trust.key_id))
        .collect();
    if matching.is_empty() {
        return Err(ApprovalRefusal::KeyIdNotInRoster {
            key_id: format!(
                "the sidecar names [{}] and {}'s {} keys are [{}]",
                join(sidecar.signatures.iter().map(|s| s.keyid.as_str())),
                trust_object(trust),
                usage.as_str(),
                join(declared.iter().map(|k| k.trust.key_id.as_str())),
            ),
        });
    }
    let mut last_error: Option<String> = None;
    let mut hit: Option<String> = None;
    for entry in matching {
        let Ok(key) = VerifyingKey::from_pem_str(&entry.spki_pem) else {
            continue;
        };
        match verify_detached(
            &key,
            PAYLOAD_TYPE_RESTORE_AUTHORIZATION,
            approval_bytes,
            sidecar,
        ) {
            Ok(matched) => {
                hit = Some(matched);
                break;
            }
            Err(e) => last_error = Some(e.to_string()),
        }
    }
    let Some(key_id) = hit else {
        return Err(ApprovalRefusal::SignatureInvalid(
            last_error.unwrap_or_else(|| {
                format!("no {} signature in the sidecar verified", usage.as_str())
            }),
        ));
    };
    if let Err(refusal) = trust.may_sign_new_for(&key_id, usage, now) {
        return Err(signing_refusal(trust, &key_id, usage, refusal));
    }
    Ok(key_id)
}

/// Authorization document v2, as a pure function of bytes, the namespace's
/// resolved trust, its bound approval policy and the referent: the ordinary
/// and the governed mode in one order.
///
/// 0. The sidecar parses.
/// 1. Its `payloadType` is [`PAYLOAD_TYPE_RESTORE_AUTHORIZATION`], before any
///    key is tried.
/// 2. A `ConsoleConfirmation` signature by a usable key of this namespace's
///    trust verifies, and that key may sign something new. Both modes: without
///    it nothing attests who the requester was.
/// 3. The authentic bytes parse as the v2 document.
/// 4. [`policy::check_restore_authorization`]: format, the exact subject
///    including UID, the plan hash recomputed from the referent's own bytes,
///    the policy name, snapshot digest and mode against the namespace's
///    current binding, a non-blank requester, and the window at `now`.
/// 5. `Governed` only: a second signature by a usable `GovernedApproval` key
///    (absent is [`ApprovalRefusal::GovernedApprovalRequired`], the pending
///    state), and separation of duties: that key's `principal.id` must not be
///    the requester's `<issuer>#<subject>`.
///
///    A `Governed` policy whose `approverSignature` is `Console` has no second
///    key: the second person (`approver {issuer, subject}`, `approvedAt`) is
///    inside the bytes the console signed, and step 4 held them to
///    [`policy::check_console_approval`] (present, a second person of the
///    requester's issuer, never the local administrator or a service account,
///    inside the request's window). The console's signature is then the whole
///    authorisation; a `GovernedApproval` countersignature is neither asked for
///    nor accepted in its place.
///
/// It never accepts a `GovernedApproval` signature in place of the console's,
/// or the console's in place of the approver's under `Governed`: each usage is
/// checked by name.
///
/// # Errors
///
/// Every refusal is an [`ApprovalRefusal`].
pub fn evaluate_authorization_v2(
    approval_bytes: &[u8],
    sidecar_bytes: &[u8],
    trust: &ResolvedTrust,
    now: DateTime<Utc>,
    expected: &ExpectedSubject,
    bound: &ApprovalPolicy,
) -> Result<Verified, ApprovalRefusal> {
    // 0 / 1.
    let sidecar: Sidecar = serde_json::from_slice(sidecar_bytes).map_err(|e| {
        ApprovalRefusal::SignatureInvalid(format!(
            "spec.sidecarBytes is not a DSSE sidecar document: {e}"
        ))
    })?;
    if sidecar.payload_type != PAYLOAD_TYPE_RESTORE_AUTHORIZATION {
        return Err(ApprovalRefusal::PayloadTypeMismatch {
            got: sidecar.payload_type,
            want: PAYLOAD_TYPE_RESTORE_AUTHORIZATION.to_string(),
        });
    }

    // 2. The console's confirmation, in both modes.
    let confirmation_key_id = verify_signature_under(
        trust,
        KeyUsage::ConsoleConfirmation,
        &sidecar,
        approval_bytes,
        now,
    )?;

    // 3 / 4. The authentic bytes, against the referent and the policy.
    let doc = RestoreAuthorization::from_bytes(approval_bytes)?;
    policy::check_restore_authorization(&doc, expected, bound, now)?;
    let requester = doc.requester.principal_id();

    // 5. The governed approver, and separation of duties, by the table:
    // `ApprovalPolicy::route` reads `(mode, approverSignature)` once; each row
    // says whose signature authorises and whose name the verdict records, and
    // a pair that is not a row is refused by name.
    let route = bound.route()?;
    let (matched_key_id, usage, approver) = match route {
        policy::ApprovalRoute::RequesterConfirms => (
            confirmation_key_id.clone(),
            route.authorising_usage(),
            requester.clone(),
        ),
        // The console signs the approval. Step 4 judged the approver; the
        // verdict records whose approval it is, taken from the document by
        // `console_approval_of`, which refuses a document with no approver or
        // no `approvedAt` by name, so nothing is made up.
        policy::ApprovalRoute::SecondPersonInConsole => (
            confirmation_key_id.clone(),
            route.authorising_usage(),
            policy::console_approval_of(&doc, bound)?
                .approver
                .principal_id(),
        ),
        policy::ApprovalRoute::PersonalKey => {
            let countersigned = sidecar
                .signatures
                .iter()
                .any(|s| s.keyid != confirmation_key_id);
            if !countersigned {
                return Err(ApprovalRefusal::GovernedApprovalRequired {
                    detail: format!(
                        "the console confirmed requester {requester} under Governed policy {}, \
                         and no approver has countersigned: a governed request needs a second \
                         signature over the same bytes by a GovernedApproval key whose \
                         principal is not the requester's. This Approval authorises nothing",
                        bound.name
                    ),
                });
            }
            let approver_key_id = verify_signature_under(
                trust,
                KeyUsage::GovernedApproval,
                &sidecar,
                approval_bytes,
                now,
            )?;
            let principal = trust
                .key(&approver_key_id)
                .map(|k| k.trust.principal_id.clone())
                .unwrap_or_default();
            // Fail closed on a principal that cannot be compared:
            // `alice@example.com` and `https://idp#alice` differ as strings and
            // may be the same person. Only `<issuer>#<subject>` is comparable.
            if bound.require_distinct_principal && !policy::is_issuer_subject_principal(&principal)
            {
                return Err(ApprovalRefusal::SelfApprovalRefused {
                    detail: format!(
                        "the approver key {approver_key_id} belongs to principal {principal:?}, \
                         which is not in the `<issuer>#<subject>` form the console attests the \
                         requester {requester:?} in; policy {} requires a DIFFERENT principal, \
                         and a principal that cannot be compared cannot be shown to differ. \
                         Record the key's principal.id as `<issuer>#<subject>`",
                        bound.name
                    ),
                });
            }
            if bound.require_distinct_principal
                && !policy::separation_holds(&doc.requester, &principal)
            {
                return Err(ApprovalRefusal::SelfApprovalRefused {
                    detail: format!(
                        "the approver key {approver_key_id} belongs to principal {principal:?} and \
                         the requester is {requester:?}; policy {} requires the approver to be a \
                         different principal from the requester, and holding an administrator \
                         role does not change that",
                        bound.name
                    ),
                });
            }
            (approver_key_id, KeyUsage::GovernedApproval, principal)
        }
    };

    let key_window = trust.key(&matched_key_id).map(|k| ApproverKeyWindow {
        key_id: matched_key_id.clone(),
        not_before: k.trust.not_before,
        not_after: k.trust.not_after,
    });
    // The verdict stops being true at the earliest of the document's expiry
    // and the console key's `notAfter`; `Verified::valid_until` also takes the
    // authorising key's.
    let console_not_after = trust
        .key(&confirmation_key_id)
        .map_or(DateTime::<Utc>::MAX_UTC, |k| k.trust.not_after);
    let self_attested_risk = trust
        .key(&matched_key_id)
        .is_some_and(|k| k.trust.has_usage(KeyUsage::EvidenceSigning));
    Ok(Verified {
        key_window,
        authorization_usage: usage,
        document_expires_at: Some(doc.expires_at.min(console_not_after)),
        approver,
        ticket: doc.ticket.clone().unwrap_or_default(),
        self_attested_risk,
        trust_source: trust.source.name().to_string(),
        authorization: Some(Box::new(AuthorizationProvenance {
            mode: bound.mode.as_str().to_string(),
            policy_name: bound.name.clone(),
            policy_digest: bound.digest(),
            requester,
            confirmation_key_id,
        })),
        verified_subject_ref: None,
        matched_key_id,
    })
}

/// The object an operator would edit to change this refusal — the roster when
/// trust came from the synthesised `legacy-roster-v1`, the policy otherwise.
///
/// The refusal names the thing to edit: "no key on the TrustRoster 'default'"
/// is the wrong sentence for a namespace governed by `org-default`.
fn trust_object(trust: &ResolvedTrust) -> String {
    if trust.source.is_legacy() {
        format!("TrustRoster '{ROSTER_NAME}'")
    } else {
        format!("TrustPolicy '{}'", trust.source.name())
    }
}

/// One [`SigningRefusal`] as the refusal an operator reads.
///
/// No wildcard arm: a new [`SigningRefusal`] must fail to compile here.
///
/// `UntrustedSigner` and `KeyUsageMismatch` are unreachable from check 6 (the
/// key came from the trust's own allowed-usage set for this document kind);
/// they map to [`ApprovalRefusal::KeyIdNotInRoster`], which is what they would
/// mean if a refactor made them reachable.
fn signing_refusal(
    trust: &ResolvedTrust,
    key_id: &str,
    usage: KeyUsage,
    refusal: SigningRefusal,
) -> ApprovalRefusal {
    let key = trust.key(key_id);
    match refusal {
        SigningRefusal::UntrustedSigner | SigningRefusal::KeyUsageMismatch => {
            ApprovalRefusal::KeyIdNotInRoster {
                key_id: format!(
                    "{} offers no {} key with id {key_id}",
                    trust_object(trust),
                    usage.as_str()
                ),
            }
        }
        SigningRefusal::KeyIdExpired => ApprovalRefusal::KeyIdExpired {
            key_id: key_id.to_string(),
            not_after: key.map_or_else(String::new, |k| k.trust.not_after.to_rfc3339()),
        },
        SigningRefusal::KeyNotYetValid => ApprovalRefusal::KeyNotYetValid {
            key_id: key_id.to_string(),
            not_before: key.map_or_else(String::new, |k| k.trust.not_before.to_rfc3339()),
        },
        SigningRefusal::KeyRetired => ApprovalRefusal::KeyRetired {
            key_id: key_id.to_string(),
            retired_at: key.and_then(|k| k.trust.retired_at).map(|t| t.to_rfc3339()),
        },
        SigningRefusal::KeyRevoked => ApprovalRefusal::KeyRevoked {
            key_id: key_id.to_string(),
            // `{:?}` on the pure enum, whose variant names are the CRD's
            // `revocationReason` values, and the object that records it when
            // that is not the resolved trust itself: a `KeyCompromise`
            // revocation on any `TrustPolicy` applies to every namespace, and
            // `legacy-roster-v1` cannot express a revocation.
            reason: match key.filter(|k| !k.compromise_inherited_from.is_empty()) {
                Some(k) => format!(
                    "{:?}, recorded by TrustPolicy/{}",
                    k.trust.reason(),
                    k.compromise_inherited_from.join(", TrustPolicy/")
                ),
                None => format!(
                    "{:?}",
                    key.map_or(logweir_core::trust::RevocationReason::Unspecified, |k| k
                        .trust
                        .reason(),)
                ),
            },
            effective_from: key
                .and_then(|k| k.trust.revocation_effective_from.or(k.trust.revoked_at))
                .map(|t| t.to_rfc3339()),
        },
    }
}

/// `a, b, c`, used only inside refusal messages.
fn join<'a>(ids: impl Iterator<Item = &'a str>) -> String {
    ids.collect::<Vec<_>>().join(", ")
}

/// What the API server said when the roster was asked for.
///
/// Three states, not a `Result`, because a 404 and a connection reset must not
/// become the same thing: a 404 is a verdict (written as
/// [`ApprovalRefusal::RosterNotFound`]); a transport error is not a verdict
/// about anybody's approval and requeues.
#[derive(Debug)]
pub enum RosterLoad {
    /// The roster exists. Boxed (`clippy::large_enum_variant`): a
    /// `TrustRoster` is ~464 bytes and `NotFound` is empty.
    Found(Box<TrustRoster>),
    /// The API server answered 404 for `trustrosters/default`.
    NotFound,
}

/// Resolve the one cluster-scoped [`TrustRoster`] named [`ROSTER_NAME`].
///
/// The name comes from [`ROSTER_NAME`] and nowhere else: not the `Approval`, a
/// flag or the environment.
///
/// # Errors
///
/// Any API error that is not a 404, verbatim, so the caller can requeue.
///
/// [`TrustRoster`]: crate::crds::trust_roster::TrustRoster
pub async fn load_roster(client: &kube::Client) -> Result<RosterLoad, kube::Error> {
    let api: Api<TrustRoster> = Api::all(client.clone());
    match api.get(ROSTER_NAME).await {
        Ok(roster) => Ok(RosterLoad::Found(Box::new(roster))),
        Err(kube::Error::Api(e)) if e.code == 404 => Ok(RosterLoad::NotFound),
        Err(e) => Err(e),
    }
}

/// A problem with the object an `Approval` points at.
///
/// Not an [`ApprovalRefusal`] variant: that set is an interface others route
/// on, and these are properties of the referent, not verdicts about the
/// signature. "Your cluster is missing an object" is not "your approval does
/// not authorise this plan".
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReferentProblem {
    /// `spec.subjectRef` names an object that does not exist in this
    /// namespace.
    ReferentNotFound {
        /// `Restore` or `Backup`.
        kind: String,
        /// The name `spec.subjectRef.name` gave.
        name: String,
    },
    /// The referent exists and carries no `spec.planBytes` to hash.
    ///
    /// `Backup.spec` has no `planBytes` field, and check 7 recomputes the plan
    /// hash from the referent's own bytes, so an `Approval` whose
    /// `subjectRef.kind` is `Backup` is refused here naming the missing field,
    /// rather than hashing something nobody signed. `Backup` is in
    /// [`SubjectKind`] because the CRD admits it and check 8 must tell the
    /// kinds apart.
    ReferentHasNoPlanBytes {
        /// `Backup`.
        kind: String,
        /// The name `spec.subjectRef.name` gave.
        name: String,
    },
    /// This immutable Approval was already verified for an older incarnation
    /// of the same-named object. Re-verification must not rebind it.
    ReferentUidChanged {
        /// `Restore` or `Backup`.
        kind: String,
        /// The immutable subject name in `spec.subjectRef`.
        name: String,
        /// The UID recorded by the first successful verification.
        verified_uid: String,
        /// The UID now returned by the API server.
        current_uid: String,
    },
}

impl ReferentProblem {
    /// The condition `reason` for this problem: the variant's own name.
    #[must_use]
    pub const fn reason(&self) -> &'static str {
        match self {
            Self::ReferentNotFound { .. } => "ReferentNotFound",
            Self::ReferentHasNoPlanBytes { .. } => "ReferentHasNoPlanBytes",
            Self::ReferentUidChanged { .. } => "ReferentUidChanged",
        }
    }
}

impl fmt::Display for ReferentProblem {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ReferentNotFound { kind, name } => write!(
                f,
                "spec.subjectRef names {kind} '{name}', which does not exist in this namespace"
            ),
            Self::ReferentHasNoPlanBytes { kind, name } => write!(
                f,
                "spec.subjectRef names {kind} '{name}', and the {kind} kind carries no \
                 spec.planBytes for the plan hash to be recomputed from; in tag 1 an approval \
                 binds a Restore's planBytes"
            ),
            Self::ReferentUidChanged {
                kind,
                name,
                verified_uid,
                current_uid,
            } => write!(
                f,
                "spec.subjectRef names {kind} '{name}', but this Approval was verified for UID \
                 {verified_uid} and that name now has UID {current_uid}; create a new Approval \
                 for the recreated object"
            ),
        }
    }
}

/// What one reconcile decided, before it was patched onto `/status`.
///
/// Returned so a test can assert over the verdict as well as the route table.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ApprovalOutcome {
    /// Every check passed.
    Verified(Verified),
    /// One of the five checks, or the missing roster, refused it.
    Refused(ApprovalRefusal),
    /// The referent could not supply the bytes check 7 hashes.
    Referent(ReferentProblem),
    /// The referent Restore was admitted under this verdict: the verdict is a
    /// record now, kept rather than judged again. See [`CONDITION_CONSUMED`].
    Consumed(Consumption),
}

/// The admission that consumed a verified `Approval`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Consumption {
    /// The exact Restore admitted: the `status.verifiedSubjectRef` the verdict
    /// was recorded for, matched against the object the API server returned
    /// (name, namespace and UID).
    pub subject: VerifiedSubjectRef,
    /// When the Restore controller recorded `Admitted=True`, when the Restore
    /// says (the condition's `lastTransitionTime`).
    pub admitted_at: Option<DateTime<Utc>>,
    /// `None` when the stored `Verified=True` status is kept verbatim, the
    /// ordinary case. `Some` only when an earlier build had already withdrawn
    /// the verdict and this pass re-established it by verifying at the
    /// admission instant, the one clock an admitted run's authorization was
    /// ever about.
    pub reverified: Option<Box<Verified>>,
    /// What the record's verdict is now. The one key event that reaches a
    /// consumed record is a `KeyCompromise` revocation: expiry, `notAfter`,
    /// retirement and a `Superseded`/`Unspecified` revocation bound new uses
    /// and leave the record; a compromise says the signature itself may not be
    /// the key holder's, which is never green for any document the key signed.
    pub standing: Standing,
}

/// The verdict a consumed record carries.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Standing {
    /// No key the record names is revoked for compromise in the resolved
    /// trust: the recorded `Verified=True` stands.
    Clean,
    /// A key the record names is revoked for `KeyCompromise`: `Verified`
    /// False, never green; the record itself is kept.
    Compromised(Compromise),
    /// An earlier pass withdrew the verdict for a compromise, and today's
    /// resolution no longer shows the revocation (a `TrustPolicy` deleted, or
    /// the namespace re-bound). Sticky: a revocation is monotonic in the CRD,
    /// so it only "disappears" when the object recording it did, which
    /// exonerates nothing.
    StillWithdrawn {
        /// The stored `Verified` reason.
        reason: &'static str,
    },
}

/// A `KeyCompromise` revocation of a key a consumed verdict names.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Compromise {
    /// The revoked key's id.
    pub key_id: String,
    /// Which recorded field names it: `status.matchedKeyId` or
    /// `status.authorization.confirmationKeyId`.
    pub field: &'static str,
    /// `revocationEffectiveFrom`, else `revokedAt`: the instant an observation
    /// is compared against.
    pub effective_from: Option<DateTime<Utc>>,
    /// The independent, controller-written observation: the admission instant
    /// the Restore controller recorded (`Consumed`'s `lastTransitionTime`).
    pub observed_at: Option<DateTime<Utc>>,
    /// Decided by `logweir_core::trust::decide` itself: the observation
    /// precedes the effective instant.
    pub recorded_before: bool,
}

impl Compromise {
    /// The `Verified` condition's reason for this compromise.
    #[must_use]
    pub const fn reason(&self) -> &'static str {
        if self.recorded_before {
            REASON_RECORDED_BEFORE_REVOCATION
        } else {
            REASON_KEY_REVOKED
        }
    }

    /// The `Verified` condition's message. Recorded instants only, never the
    /// clock, so an unchanged compromise is never rewritten.
    #[must_use]
    pub fn message(&self, subject: &VerifiedSubjectRef) -> String {
        let at = |t: Option<DateTime<Utc>>| {
            t.map_or_else(|| "an unrecorded instant".to_string(), |t| t.to_rfc3339())
        };
        format!(
            "the key {} ({}) is revoked for KeyCompromise, effective {}. Restore {}/{} (UID {}) \
             was admitted under this Approval at {}, {}. The authorization, key ids, approver \
             and Consumed record are kept for the investigation; the verdict is never green \
             while the revocation stands",
            self.key_id,
            self.field,
            at(self.effective_from),
            subject.namespace,
            subject.name,
            subject.uid,
            at(self.observed_at),
            if self.recorded_before {
                "BEFORE the revocation took effect (RecordedBeforeRevocation: the order of the \
                 two recorded facts, not a pass)"
            } else {
                "not before the revocation took effect, so no recorded observation separates \
                 this signature from the compromise"
            }
        )
    }
}

impl Consumption {
    /// The [`CONDITION_CONSUMED`] message: a function of the recorded facts
    /// and no clock, so a second pass computes the same bytes and writes
    /// nothing.
    #[must_use]
    pub fn message(&self) -> String {
        format!(
            "Restore {}/{} (UID {}) was admitted under this verdict{}; an authorization's expiry \
             bounds the time to admission, so the verified record beside this condition is the \
             audit trail of that admission and is not judged again. It binds only that Restore \
             UID and authorises nothing else",
            self.subject.namespace,
            self.subject.name,
            self.subject.uid,
            self.admitted_at
                .map_or_else(String::new, |at| format!(" at {}", at.to_rfc3339())),
        )
    }
}

impl ApprovalOutcome {
    /// The `reason` this outcome writes into the `Verified` condition.
    #[must_use]
    pub const fn reason(&self) -> &'static str {
        match self {
            Self::Consumed(Consumption {
                standing: Standing::Compromised(compromise),
                ..
            }) => compromise.reason(),
            Self::Consumed(Consumption {
                standing: Standing::StillWithdrawn { reason },
                ..
            }) => reason,
            Self::Verified(_) | Self::Consumed(_) => REASON_VERIFIED,
            Self::Refused(r) => r.reason(),
            Self::Referent(p) => p.reason(),
        }
    }

    /// Whether `status.verified` is `true`: never for a consumed record whose
    /// key was revoked for compromise.
    #[must_use]
    pub const fn is_verified(&self) -> bool {
        matches!(
            self,
            Self::Verified(_)
                | Self::Consumed(Consumption {
                    standing: Standing::Clean,
                    ..
                })
        )
    }

    /// Whether this outcome is a consumed verdict, kept as a record.
    #[must_use]
    pub const fn is_consumed(&self) -> bool {
        matches!(self, Self::Consumed(_))
    }

    /// The instant this outcome stops being true on its own; `None` for an
    /// outcome no clock can withdraw.
    ///
    /// Only a `Verified` has one. A refusal becomes a pass only through an edit,
    /// which wakes this controller through its watch or the heartbeat. A pass
    /// becomes a refusal with nobody touching anything, and that direction
    /// must never be late: an expired key would authorise a restore. A consumed
    /// verdict has none either: the one use it authorised has happened.
    #[must_use]
    pub fn valid_until(&self) -> Option<DateTime<Utc>> {
        match self {
            Self::Verified(v) => Some(v.valid_until()),
            Self::Refused(_) | Self::Referent(_) | Self::Consumed(_) => None,
        }
    }

    /// The `message` this outcome writes into the `Verified` condition.
    ///
    /// A consumed verdict kept verbatim writes no `Verified` condition of its
    /// own ([`status_for`] carries the stored one); one re-established at the
    /// admission instant says so, and names the instant.
    #[must_use]
    pub fn message(&self) -> String {
        match self {
            Self::Consumed(Consumption {
                standing: Standing::Compromised(compromise),
                subject,
                ..
            }) => compromise.message(subject),
            Self::Consumed(Consumption {
                standing: Standing::StillWithdrawn { reason },
                ..
            }) => format!(
                "the verdict stays withdrawn ({reason}): a KeyCompromise revocation is not undone \
                 by the object that recorded it disappearing"
            ),
            Self::Consumed(c) => match c.reverified.as_deref() {
                Some(v) => format!(
                    "{} — verified as of the admission at {}, the instant the Restore controller \
                     recorded Admitted=True (an earlier build had withdrawn this verdict after \
                     the admission)",
                    Self::Verified(v.clone()).message(),
                    c.admitted_at
                        .map_or_else(|| "an unrecorded instant".to_string(), |t| t.to_rfc3339())
                ),
                None => c.message(),
            },
            Self::Verified(v) if v.authorization.is_some() => {
                let provenance = v.authorization.as_ref();
                format!(
                    "authorization document v2 verified: the console's ConsoleConfirmation \
                     signature attests requester {} and the {} key {} authorised it \
                     (trustSource={}, approvalPolicy={} mode {}); subject, UID, plan hash, policy \
                     digest and validity window all matched",
                    provenance.map_or("", |p| p.requester.as_str()),
                    v.authorization_usage.as_str(),
                    v.matched_key_id,
                    v.trust_source,
                    provenance.map_or("", |p| p.policy_name.as_str()),
                    provenance.map_or("", |p| p.mode.as_str()),
                )
            }
            Self::Verified(v) => format!(
                "the DSSE signature over spec.approvalBytes verified under {} key {} \
                 (trustSource={}), and the recomputed plan hash matched",
                v.authorization_usage.as_str(),
                v.matched_key_id,
                v.trust_source
            ),
            Self::Refused(r) => r.to_string(),
            Self::Referent(p) => p.to_string(),
        }
    }
}

/// Anything that made a reconcile impossible rather than negative.
///
/// Neither variant is a verdict: every refusal an operator can act on is an
/// [`ApprovalRefusal`] or a [`ReferentProblem`] and reaches the cluster as a
/// condition. This is only "the reconcile could not be completed", which
/// requeues and writes nothing.
#[derive(Debug)]
pub enum ReconcileError {
    /// The `Approval` carries no namespace. Unreachable for an object that
    /// came from the API server; named rather than unwrapped.
    NoNamespace(String),
    /// The referent carries no UID. Unreachable for a persisted API object.
    NoUid(String),
    /// A referent's sealed spec could not be rendered as the canonical bytes
    /// its digest is taken over, or those bytes were not UTF-8.
    ///
    /// Unreachable (`to_deterministic_json` always emits UTF-8), which is why
    /// it must not be a silent default: an empty string would make check 7
    /// compare `sha256("")`, fail-closed by luck rather than construction.
    Canonicalization(String),
    /// The API server could not be talked to. Requeue.
    Api(kube::Error),
}

impl fmt::Display for ReconcileError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NoNamespace(name) => {
                write!(f, "the object {name} carries no metadata.namespace")
            }
            Self::NoUid(name) => write!(f, "the object {name} carries no metadata.uid"),
            Self::Canonicalization(detail) => write!(f, "{detail}"),
            Self::Api(e) => write!(f, "kubernetes API error: {e}"),
        }
    }
}

impl std::error::Error for ReconcileError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::NoNamespace(_) | Self::NoUid(_) | Self::Canonicalization(_) => None,
            Self::Api(e) => Some(e),
        }
    }
}

impl From<kube::Error> for ReconcileError {
    fn from(e: kube::Error) -> Self {
        Self::Api(e)
    }
}

/// Decide one `Approval`, without writing anything.
///
/// Split from the patch: the decision needs the cluster and the patch needs
/// the decision, so `a_missing_roster_names_itself` can assert the decision
/// and that the referent was never fetched, off one route table.
///
/// # Errors
///
/// [`ReconcileError`] for anything that is not a verdict.
pub async fn decide(
    approval: &Approval,
    client: &kube::Client,
) -> Result<ApprovalOutcome, ReconcileError> {
    decide_with_policy(approval, client, &ApprovalPolicySet::default()).await
}

/// The v1 / v2 choice for a `Restore` referent, made on the namespace's
/// binding, never on what the document claims to be.
///
/// An unbound namespace accepts the v1 approval; a bound one accepts exactly
/// authorization document v2 naming its policy. The sidecar's payload type
/// only chooses which refusal names the mismatch; every path to `Verified`
/// verifies a signature over those bytes.
#[allow(clippy::too_many_arguments)]
fn evaluate_restore_referent(
    approval: &Approval,
    trust: &ResolvedTrust,
    now: DateTime<Utc>,
    namespace: &str,
    plan_bytes: &str,
    referent: Option<&VerifiedSubjectRef>,
    effective: &EffectivePolicy,
) -> Result<Verified, ApprovalRefusal> {
    let payload_type = serde_json::from_str::<Sidecar>(&approval.spec.sidecar_bytes)
        .ok()
        .map(|s| s.payload_type);
    match effective {
        EffectivePolicy::Legacy => {
            if payload_type.as_deref() == Some(PAYLOAD_TYPE_RESTORE_AUTHORIZATION) {
                return Err(policy::unbound_namespace_refusal(namespace).into());
            }
            evaluate(
                approval.spec.approval_bytes.as_bytes(),
                approval.spec.sidecar_bytes.as_bytes(),
                trust,
                now,
                SubjectKind::Restore.as_str(),
                plan_bytes.as_bytes(),
            )
        }
        EffectivePolicy::Bound(bound) => {
            if payload_type.as_deref() == Some(PAYLOAD_TYPE_APPROVAL) {
                return Err(policy::v1_under_bound_policy_refusal(bound).into());
            }
            let expected = ExpectedSubject {
                namespace: namespace.to_string(),
                name: approval.spec.subject_ref.name.clone(),
                uid: referent.map(|r| r.uid.clone()).unwrap_or_default(),
                plan_hash: sha256_prefixed(plan_bytes.as_bytes()),
            };
            evaluate_authorization_v2(
                approval.spec.approval_bytes.as_bytes(),
                approval.spec.sidecar_bytes.as_bytes(),
                trust,
                now,
                &expected,
                bound,
            )
        }
    }
}

/// The refusal an `Approval` naming a `RehearsalSchedule` gets: standing
/// authorizations are withdrawn in v1.
fn standing_withdrawn(approval: &Approval) -> ApprovalOutcome {
    ApprovalOutcome::Refused(ApprovalRefusal::StandingAuthorizationRefused {
        name: approval.spec.subject_ref.name.clone(),
    })
}

/// [`decide`], under the installation's approval policies.
///
/// # Errors
///
/// [`ReconcileError`] for anything that is not a verdict.
pub async fn decide_with_policy(
    approval: &Approval,
    client: &kube::Client,
    policies: &ApprovalPolicySet,
) -> Result<ApprovalOutcome, ReconcileError> {
    decide_with_policy_at(approval, client, policies, Utc::now()).await
}

/// [`decide_with_policy`] with the clock passed in, so a test can decide a
/// fixed document inside its own validity window.
///
/// # Errors
///
/// [`ReconcileError`] for anything that is not a verdict.
pub async fn decide_with_policy_at(
    approval: &Approval,
    client: &kube::Client,
    policies: &ApprovalPolicySet,
    now: DateTime<Utc>,
) -> Result<ApprovalOutcome, ReconcileError> {
    let name = approval.name_any();
    let namespace = approval
        .namespace()
        .ok_or_else(|| ReconcileError::NoNamespace(name.clone()))?;

    // A standing authorization is withdrawn: refused by name first, before a
    // stored record, the trust or the referent is read, and never recorded
    // `Verified` or `Consumed`. The kind stays in the CRD's enum so older
    // objects still decode.
    if approval.spec.subject_ref.kind == SubjectKind::RehearsalSchedule {
        return Ok(standing_withdrawn(approval));
    }

    // A consumed verdict is a record, not a gate. An `Approval` whose Restore
    // was admitted under it has authorised the one thing it ever will; a
    // document expiry, a key's `notAfter`, a retirement or a policy edit after
    // that is about new uses. Two ways in:
    //
    // 1. This build already recorded `Consumed=True`: the stored status alone.
    // 2. The stored verdict is still `Verified=True` and the Restore says it
    //    was admitted under this Approval object (`admitted_under`, bound by
    //    UID through the Restore's own approval bundle). Checked ahead of the
    //    full evaluation, so the pass that first sees the admission cannot be
    //    the one that withdraws the verdict.
    //
    // One key event still reaches it: a `KeyCompromise` revocation, which
    // makes every document the key signed never green. So a consumed record
    // reads the namespace's trust on every pass, and a compromise of a key it
    // names turns `Verified` false (`RecordedBeforeRevocation`, or
    // `KeyRevoked`) while the record is kept.
    let subject = &approval.spec.subject_ref;
    let stored = approval.status.as_ref();
    let mut prefetched: Option<Option<Restore>> = None;
    let mut candidate = recorded_consumption(approval);
    if candidate.is_none()
        && subject.kind == SubjectKind::Restore
        && stored.and_then(|s| s.verified) == Some(true)
    {
        if let Some(previous) = stored.and_then(|s| s.verified_subject_ref.as_ref()) {
            let restore = get_restore(client, &namespace, &subject.name).await?;
            if let Some(found) = restore.as_ref() {
                let current = restore_subject(found, &subject.name, &namespace)?;
                if &current == previous {
                    let bundle = admission_bundle(client, &namespace, found).await?;
                    if let Some(admitted_at) = admitted_under(found, approval, bundle.as_ref()) {
                        candidate = Some(Consumption {
                            subject: current,
                            admitted_at,
                            reverified: None,
                            standing: Standing::Clean,
                        });
                    }
                }
            }
            prefetched = Some(restore);
        }
    }
    if let Some(mut consumption) = candidate {
        let withdrawn = stored_withdrawal(approval);
        let unchanged = |withdrawn: Option<&'static str>| match withdrawn {
            Some(reason) => Standing::StillWithdrawn { reason },
            None => Standing::Clean,
        };
        match crate::trust::resolve(client, &namespace).await? {
            Resolution::Trust(trust) => {
                // A record is kept only while the signature it records still
                // verifies under the key it names (when the trust carries
                // that key): a `Consumed` planted on bytes that key never
                // signed is judged from scratch below.
                if record_signature_holds(approval, &trust) {
                    consumption.standing =
                        match compromise_of(approval, &trust, consumption.admitted_at, now) {
                            Some(compromise) => Standing::Compromised(compromise),
                            None => unchanged(withdrawn),
                        };
                    return Ok(ApprovalOutcome::Consumed(consumption));
                }
            }
            // A contested or absent trust is not a revocation: it says nothing
            // about the keys this record names, so the record stands.
            Resolution::Conflict { .. } | Resolution::Unconfigured => {
                consumption.standing = unchanged(withdrawn);
                return Ok(ApprovalOutcome::Consumed(consumption));
            }
        }
    }

    // The namespace's trust first; an absent or contested one short-circuits,
    // because without a key set fetching the referent would only reach a
    // known conclusion. `trust::resolve` falls back to the synthesised
    // `legacy-roster-v1`, so an unmigrated cluster gets exactly
    // `TrustRoster/default`'s key set, and `Unconfigured` is `RosterNotFound`
    // with the same message.
    let trust = match crate::trust::resolve(client, &namespace).await? {
        Resolution::Trust(trust) => trust,
        Resolution::Conflict {
            namespace,
            policies,
        } => {
            return Ok(ApprovalOutcome::Refused(
                ApprovalRefusal::TrustPolicyConflict {
                    namespace,
                    policies,
                },
            ))
        }
        Resolution::Unconfigured => {
            return Ok(ApprovalOutcome::Refused(ApprovalRefusal::RosterNotFound))
        }
    };

    let referent_kind = subject.kind.as_str();
    // When the referent Restore was admitted under this Approval but the
    // stored verdict no longer says Verified (rewritten by an earlier build),
    // the verdict is asked at the admission instant, the only one an admitted
    // run's authorization was ever about.
    let mut consumed_at: Option<DateTime<Utc>> = None;
    let (plan_bytes, verified_subject_ref) = match subject.kind {
        SubjectKind::Restore => {
            let restore = match prefetched {
                Some(restore) => restore,
                None => get_restore(client, &namespace, &subject.name).await?,
            };
            match restore {
                Some(restore) => {
                    let current = restore_subject(&restore, &subject.name, &namespace)?;
                    if let Some(previous) = approval
                        .status
                        .as_ref()
                        .and_then(|status| status.verified_subject_ref.as_ref())
                    {
                        if previous != &current {
                            return Ok(ApprovalOutcome::Referent(
                                ReferentProblem::ReferentUidChanged {
                                    kind: referent_kind.to_string(),
                                    name: subject.name.clone(),
                                    verified_uid: previous.uid.clone(),
                                    current_uid: current.uid,
                                },
                            ));
                        }
                        // Only a verdict recorded for this UID can have been
                        // consumed by its admission.
                        let bundle = admission_bundle(client, &namespace, &restore).await?;
                        consumed_at = admitted_under(&restore, approval, bundle.as_ref()).flatten();
                    }
                    (restore.spec.plan_bytes, Some(current))
                }
                None => {
                    return Ok(ApprovalOutcome::Referent(
                        ReferentProblem::ReferentNotFound {
                            kind: referent_kind.to_string(),
                            name: subject.name.clone(),
                        },
                    ))
                }
            }
        }
        SubjectKind::Backup => {
            let api: Api<Backup> = Api::namespaced(client.clone(), &namespace);
            match api.get(&subject.name).await {
                // The object exists and the kind has no planBytes; both facts
                // are reported, so the operator is not told the Backup is
                // missing.
                Ok(_) => {
                    return Ok(ApprovalOutcome::Referent(
                        ReferentProblem::ReferentHasNoPlanBytes {
                            kind: referent_kind.to_string(),
                            name: subject.name.clone(),
                        },
                    ))
                }
                Err(kube::Error::Api(e)) if e.code == 404 => {
                    return Ok(ApprovalOutcome::Referent(
                        ReferentProblem::ReferentNotFound {
                            kind: referent_kind.to_string(),
                            name: subject.name.clone(),
                        },
                    ))
                }
                Err(e) => return Err(e.into()),
            }
        }
        // Refused at the top of this function; matched here because the enum
        // is closed.
        SubjectKind::RehearsalSchedule => return Ok(standing_withdrawn(approval)),
    };

    // `.as_bytes()`, with no decode step (interface **I18**): `approvalBytes`
    // and `sidecarBytes` are the UTF-8 document text, never base64.
    let want = sha256_prefixed(plan_bytes.as_bytes());
    let evaluated = if subject.kind == SubjectKind::Restore {
        // The one kind an approval policy governs; a `Backup` referent was
        // refused before this point.
        evaluate_restore_referent(
            approval,
            &trust,
            consumed_at.unwrap_or(now),
            &namespace,
            &plan_bytes,
            verified_subject_ref.as_ref(),
            &policies.resolve(&namespace),
        )
    } else {
        evaluate(
            approval.spec.approval_bytes.as_bytes(),
            approval.spec.sidecar_bytes.as_bytes(),
            &trust,
            now,
            referent_kind,
            plan_bytes.as_bytes(),
        )
    };
    Ok(match evaluated {
        Ok(mut verified) => {
            // The unsigned claim must agree with the signed one.
            // `spec.planHash` is a plain CRD field that `kubectl` and the UI
            // show so a reader can compare; checks 1-8 never read it (check 7
            // compares the hash inside the signed bytes). Without this an
            // `Approval` could be `Verified=True` while displaying a hash that
            // is not the plan it authorises. The verdict is check 7's, because
            // the fact is: this approval names a plan the referent does not
            // carry. The message names both hashes.
            if approval.spec.plan_hash != want {
                return Ok(ApprovalOutcome::Refused(
                    ApprovalRefusal::PlanHashMismatch {
                        got: approval.spec.plan_hash.clone(),
                        want,
                    },
                ));
            }
            verified.verified_subject_ref = verified_subject_ref.clone();
            match (consumed_at, verified_subject_ref) {
                (Some(at), Some(subject)) => ApprovalOutcome::Consumed(Consumption {
                    subject,
                    admitted_at: Some(at),
                    reverified: Some(Box::new(verified)),
                    // Unreachable with a compromised key: `may_sign_new`
                    // refuses a revoked key at every instant, so the
                    // re-evaluation above has already refused.
                    standing: Standing::Clean,
                }),
                _ => ApprovalOutcome::Verified(verified),
            }
        }
        Err(refusal) => ApprovalOutcome::Refused(refusal),
    })
}

/// The `/status` body one outcome produces.
#[must_use]
pub fn status_for(
    approval: &Approval,
    outcome: &ApprovalOutcome,
    now: DateTime<Utc>,
) -> ApprovalStatus {
    if let ApprovalOutcome::Consumed(consumption) = outcome {
        return consumed_status(approval, consumption, now);
    }
    let verified = outcome.is_verified();
    let (
        matched_key_id,
        approver,
        ticket,
        self_attested_risk,
        key_window,
        authorization,
        verified_subject_ref,
    ) = match outcome {
        ApprovalOutcome::Verified(v) => (
            Some(v.matched_key_id.clone()),
            Some(v.approver.clone()),
            Some(v.ticket.clone()),
            Some(v.self_attested_risk),
            v.key_window.clone(),
            v.authorization.as_deref().cloned(),
            v.verified_subject_ref.clone(),
        ),
        // A refused approval reports no approver, key id or key window: an
        // approver lifted from bytes whose signature did not verify is an
        // attacker-controlled string, and a window left from a withdrawn
        // verdict would let a reader compare a deadline against it and find it
        // open. No window reads as `unknown`, never valid.
        //
        // Subject provenance is different: once established it is a replay
        // fence and survives every later failure, including referent deletion.
        _ => (
            None,
            None,
            None,
            None,
            None,
            None,
            approval
                .status
                .as_ref()
                .and_then(|status| status.verified_subject_ref.clone()),
        ),
    };
    ApprovalStatus {
        verified: Some(verified),
        matched_key_id,
        approver,
        ticket,
        self_attested_risk,
        approver_key_window: key_window,
        authorization,
        verified_subject_ref,
        conditions: Some(vec![merge_condition(
            current_condition(
                approval.status.as_ref().and_then(|s| s.conditions.as_ref()),
                CONDITION_VERIFIED,
            ),
            Condition {
                r#type: CONDITION_VERIFIED.to_string(),
                status: if verified { "True" } else { "False" }.to_string(),
                observed_generation: approval.metadata.generation,
                last_transition_time: Some(now),
                reason: Some(outcome.reason().to_string()),
                message: Some(outcome.message()),
            },
        )]),
    }
}

/// [`status_for`] for a verdict the referent's admission consumed.
///
/// `reverified: None`, the ordinary case: the stored `Verified=True` status
/// (`authorization`, `matchedKeyId`, `approver`, the key window, the
/// `Verified` condition with its original `lastTransitionTime`) is copied as
/// it stands, and [`CONDITION_CONSUMED`] is added. Nothing is recomputed, so
/// no later clock, key event or policy edit reaches the record.
///
/// `reverified: Some` repairs an object an earlier build rewrote: the fields
/// come from the verdict re-established at the admission instant, and its
/// `Verified` message names that instant.
///
/// An existing `Consumed=True` condition is carried verbatim, so the next pass
/// computes the same status and the no-write guard sends nothing.
fn consumed_status(
    approval: &Approval,
    consumption: &Consumption,
    now: DateTime<Utc>,
) -> ApprovalStatus {
    let stored = approval.status.as_ref();
    let stored_conditions = stored.and_then(|s| s.conditions.as_ref());
    // `lastTransitionTime` is the admission instant the Restore controller
    // recorded, not when this controller noticed: the independent observation
    // a later compromise is compared against. Carried verbatim once set.
    let consumed = match current_condition(stored_conditions, CONDITION_CONSUMED) {
        Some(existing) if existing.status == "True" => existing.clone(),
        existing => merge_condition(
            existing,
            Condition {
                r#type: CONDITION_CONSUMED.to_string(),
                status: "True".to_string(),
                observed_generation: approval.metadata.generation,
                last_transition_time: Some(consumption.admitted_at.unwrap_or(now)),
                reason: Some(REASON_RESTORE_ADMITTED.to_string()),
                message: Some(consumption.message()),
            },
        ),
    };
    let upsert = |conditions: &mut Vec<Condition>, next: Condition| match conditions
        .iter_mut()
        .find(|c| c.r#type == next.r#type)
    {
        Some(slot) => *slot = next,
        None => conditions.push(next),
    };
    let mut status = match consumption.reverified.as_deref() {
        None => stored.cloned().unwrap_or_default(),
        Some(verified) => {
            let mut status =
                status_for(approval, &ApprovalOutcome::Verified(verified.clone()), now);
            let message = ApprovalOutcome::Consumed(Consumption {
                standing: Standing::Clean,
                ..consumption.clone()
            })
            .message();
            for condition in status.conditions.iter_mut().flatten() {
                if condition.r#type == CONDITION_VERIFIED {
                    condition.message = Some(message.clone());
                }
            }
            status
        }
    };
    let mut conditions = status.conditions.take().unwrap_or_default();
    // A compromise withdraws the verdict and nothing else: the authorization,
    // key ids, approver, ticket, window and subject binding are the record an
    // investigation starts from.
    if let Standing::Compromised(compromise) = &consumption.standing {
        let next = merge_condition(
            current_condition(Some(&conditions), CONDITION_VERIFIED),
            Condition {
                r#type: CONDITION_VERIFIED.to_string(),
                status: "False".to_string(),
                observed_generation: approval.metadata.generation,
                last_transition_time: Some(now),
                reason: Some(compromise.reason().to_string()),
                message: Some(compromise.message(&consumption.subject)),
            },
        );
        upsert(&mut conditions, next);
        status.verified = Some(false);
    }
    upsert(&mut conditions, consumed);
    status.conditions = Some(conditions);
    status
}

/// The stored `Verified` reason when an earlier pass withdrew a consumed
/// verdict for a compromise: [`Standing::StillWithdrawn`]'s input.
fn stored_withdrawal(approval: &Approval) -> Option<&'static str> {
    let status = approval.status.as_ref()?;
    if status.verified != Some(false) {
        return None;
    }
    let verified = current_condition(status.conditions.as_ref(), CONDITION_VERIFIED)?;
    match verified.reason.as_deref() {
        Some(REASON_RECORDED_BEFORE_REVOCATION) => Some(REASON_RECORDED_BEFORE_REVOCATION),
        Some(REASON_KEY_REVOKED) => Some(REASON_KEY_REVOKED),
        _ => None,
    }
}

/// The consumption the stored status already records: `Consumed=True`, a
/// subject binding and a matched key, written together by an earlier pass.
/// Read before any other API call.
///
/// Not conditioned on `verified == true`: a consumed record whose key was later
/// revoked for compromise carries `verified: false` and is still a consumed
/// record whose `Consumed` condition and authorization must survive.
///
/// `admitted_at` is `Consumed`'s `lastTransitionTime`, the admission instant
/// (see [`consumed_status`]).
fn recorded_consumption(approval: &Approval) -> Option<Consumption> {
    let status = approval.status.as_ref()?;
    let subject = status.verified_subject_ref.clone()?;
    status.matched_key_id.as_ref()?;
    let consumed = current_condition(status.conditions.as_ref(), CONDITION_CONSUMED)?;
    (consumed.status == "True").then_some(Consumption {
        subject,
        admitted_at: consumed.last_transition_time,
        reverified: None,
        standing: Standing::Clean,
    })
}

/// The per-Restore approval bundle the Restore controller materialised before
/// the runner Job: the one object that records which `Approval` (by UID) the
/// Restore was admitted under. `None` when the Restore is not admitted (no
/// read is made), or when the bundle is absent.
async fn admission_bundle(
    client: &kube::Client,
    namespace: &str,
    restore: &Restore,
) -> Result<Option<k8s_openapi::api::core::v1::ConfigMap>, ReconcileError> {
    let admitted = current_condition(
        restore.status.as_ref().and_then(|s| s.conditions.as_ref()),
        crate::conditions::CONDITION_ADMITTED,
    )
    .is_some_and(|c| c.status == "True");
    if !admitted {
        return Ok(None);
    }
    let api: Api<k8s_openapi::api::core::v1::ConfigMap> =
        Api::namespaced(client.clone(), namespace);
    Ok(api
        .get_opt(&super::restore::approval_bundle_config_map_name(
            &restore.name_any(),
        ))
        .await?)
}

/// The `Approval` UID `bundle` records, when the bundle is this Restore's own:
/// controlled by exactly this Restore UID, and annotated with it.
///
/// A ConfigMap is a namespace tenant's object as much as the controller's, so
/// one that is not demonstrably the Restore controller's bundle for this
/// Restore answers nothing.
#[must_use]
pub fn admitted_approval_uid(
    bundle: &k8s_openapi::api::core::v1::ConfigMap,
    restore_uid: &str,
) -> Option<String> {
    let meta = &bundle.metadata;
    let owned = meta.owner_references.as_ref().is_some_and(|refs| {
        refs.iter().any(|r| {
            r.controller == Some(true) && r.kind == Restore::kind(&()) && r.uid == restore_uid
        })
    });
    let annotations = meta.annotations.as_ref()?;
    let annotated = annotations
        .get(super::restore::BUNDLE_RESTORE_UID_ANNOTATION)
        .is_some_and(|uid| uid == restore_uid);
    if !owned || !annotated {
        return None;
    }
    annotations
        .get(super::restore::BUNDLE_APPROVAL_UID_ANNOTATION)
        .filter(|uid| !uid.trim().is_empty())
        .cloned()
}

/// When `restore` records that it was admitted under `approval`: this object,
/// not merely this name.
///
/// * The Restore names this `Approval` in `spec.approvalRef` and carries
///   `Admitted=True`, written on the pass that creates the runner Job. An
///   `Admitted=False` hold is exactly the not-yet-admitted request an expiry
///   must still refuse.
/// * The Restore's own approval bundle ([`admitted_approval_uid`]) records this
///   object's UID. A Restore admitted before per-Restore bundles has none, and
///   then the object must have existed at the admission (`creationTimestamp`
///   at or before it): an `Approval` re-created under the same name later is
///   another object.
///
/// `Some(None)` (no instant on the condition) cannot bind the fallback, so it
/// answers `None`. `Restore.status` is written only by this controller's
/// service account, the same trust the Restore controller places in
/// `Approval.status.verified`.
#[must_use]
pub fn admitted_under(
    restore: &Restore,
    approval: &Approval,
    bundle: Option<&k8s_openapi::api::core::v1::ConfigMap>,
) -> Option<Option<DateTime<Utc>>> {
    if restore.spec.authorization.is_some()
        || restore.spec.approval_ref_name().trim() != approval.name_any()
    {
        return None;
    }
    let admitted = current_condition(
        restore.status.as_ref().and_then(|s| s.conditions.as_ref()),
        crate::conditions::CONDITION_ADMITTED,
    )?;
    if admitted.status != "True" {
        return None;
    }
    let restore_uid = restore.uid().unwrap_or_default();
    let bound = match bundle.and_then(|b| admitted_approval_uid(b, &restore_uid)) {
        Some(uid) => approval.uid().is_some_and(|mine| mine == uid),
        None => match (
            approval.metadata.creation_timestamp.as_ref(),
            admitted.last_transition_time,
        ) {
            (Some(created), Some(at)) => created.0 <= at,
            _ => false,
        },
    };
    bound.then_some(admitted.last_transition_time)
}

/// Whether the signature the stored verdict records still verifies over
/// `spec.approvalBytes` under the key `status.matchedKeyId` names.
/// Cryptography only, no clock or lifecycle: an expired, retired or revoked key
/// still answers `true` ([`compromise_of`] asks the compromise question).
///
/// A key today's resolution no longer carries cannot be checked, and the record
/// stands: an absence (a policy deleted, a namespace re-bound) says nothing
/// about the signature, and a compromise is an affirmative revocation.
fn record_signature_holds(approval: &Approval, trust: &ResolvedTrust) -> bool {
    let Some(key_id) = approval
        .status
        .as_ref()
        .and_then(|s| s.matched_key_id.as_deref())
    else {
        return false;
    };
    let Some(entry) = trust.key(key_id) else {
        return true;
    };
    let Ok(sidecar) = serde_json::from_str::<Sidecar>(&approval.spec.sidecar_bytes) else {
        return false;
    };
    if sidecar.payload_type != PAYLOAD_TYPE_APPROVAL
        && sidecar.payload_type != PAYLOAD_TYPE_RESTORE_AUTHORIZATION
    {
        return false;
    }
    let Ok(key) = VerifyingKey::from_pem_str(&entry.spki_pem) else {
        return false;
    };
    let payload_type = sidecar.payload_type.clone();
    verify_detached(
        &key,
        &payload_type,
        approval.spec.approval_bytes.as_bytes(),
        &sidecar,
    )
    .is_ok_and(|matched| matched == key_id)
}

/// A `KeyCompromise` revocation of a key the stored verdict names.
///
/// The rule is `logweir_core::trust::decide`'s, called, not rewritten: its
/// compromise rows compare the independent observation with
/// `revocationEffectiveFrom` (else `revokedAt`) and never read the document's
/// claim. Only a `Revoked`/`KeyCompromise` key is asked.
fn compromise_of(
    approval: &Approval,
    trust: &ResolvedTrust,
    observed_at: Option<DateTime<Utc>>,
    now: DateTime<Utc>,
) -> Option<Compromise> {
    use logweir_core::trust as core;
    let status = approval.status.as_ref()?;
    let named = [
        (status.matched_key_id.as_deref(), "status.matchedKeyId"),
        (
            status
                .authorization
                .as_ref()
                .map(|a| a.confirmation_key_id.as_str()),
            "status.authorization.confirmationKeyId",
        ),
    ];
    for (key_id, field) in named {
        let Some(key_id) = key_id.filter(|id| !id.is_empty()) else {
            continue;
        };
        let Some(entry) = trust.key(key_id) else {
            continue;
        };
        let key = &entry.trust;
        if key.state != core::KeyState::Revoked
            || key.reason() != core::RevocationReason::KeyCompromise
        {
            continue;
        }
        // A usage the key declares, so `decide` reaches its compromise rows
        // rather than the usage row. A key with no usage cannot have produced
        // the verdict; it fails closed below.
        let recorded_before = key.usages.first().is_some_and(|usage| {
            core::decide(
                Some(key),
                *usage,
                &core::EvidenceClaim::absent(core::ClaimAbsence::FieldAbsent),
                &core::IndependentObservation { observed_at },
                now,
            )
            .reason
                == Some(core::UntrustReason::RecordedBeforeRevocation)
        });
        return Some(Compromise {
            key_id: key_id.to_string(),
            field,
            effective_from: key.revocation_effective_from.or(key.revoked_at),
            observed_at,
            recorded_before,
        });
    }
    None
}

/// The exact subject reference for a `Restore` the API server returned.
fn restore_subject(
    restore: &Restore,
    name: &str,
    namespace: &str,
) -> Result<VerifiedSubjectRef, ReconcileError> {
    let uid = restore
        .uid()
        .ok_or_else(|| ReconcileError::NoUid(name.to_string()))?;
    Ok(VerifiedSubjectRef {
        api_version: Restore::api_version(&()).to_string(),
        kind: SubjectKind::Restore,
        name: name.to_string(),
        namespace: namespace.to_string(),
        uid,
    })
}

/// `GET` the Restore an `Approval` names; a 404 is `None`.
async fn get_restore(
    client: &kube::Client,
    namespace: &str,
    name: &str,
) -> Result<Option<Restore>, ReconcileError> {
    let api: Api<Restore> = Api::namespaced(client.clone(), namespace);
    match api.get(name).await {
        Ok(restore) => Ok(Some(restore)),
        Err(kube::Error::Api(e)) if e.code == 404 => Ok(None),
        Err(e) => Err(e.into()),
    }
}

/// The `status` keys this reconciler computes as `Option` and must be able to
/// clear, which is why [`status_patch_body`] exists.
///
/// [`status_for`] sets them to `None` for a refused `Approval`, and each is
/// `skip_serializing_if = "Option::is_none"`, so the patch body would have no
/// such key and RFC 7386 would leave the old value: a verdict withdrawn after
/// a pass would keep its approver, key id and key window beside
/// `Verified=False`. A stranded window is the urgent one: the restore
/// preflight would find an open window for a key that authorises nothing.
///
/// `verifiedSubjectRef` is not here: it is a replay fence, not a verdict, and
/// must survive every later failure.
pub const CLEARABLE_STATUS_FIELDS: [&str; 6] = [
    "matchedKeyId",
    "approver",
    "ticket",
    "selfAttestedRisk",
    "approverKeyWindow",
    // A policy and requester left behind by a withdrawn verdict is the same
    // "absent read as valid" shape the window is.
    "authorization",
];

/// The `{"status": …}` merge-patch body, with an explicit `null` for every
/// [`CLEARABLE_STATUS_FIELDS`] key this pass computed as `None`.
///
/// [`crate::conditions::status_unchanged`] applies the body exactly as the API
/// server would, so the no-write-when-nothing-changed guard keeps working: a
/// `null` for a key the object does not have merges to nothing.
#[must_use]
pub fn status_patch_body(status: &ApprovalStatus) -> serde_json::Value {
    let mut body = serde_json::to_value(status).unwrap_or(serde_json::Value::Null);
    if let Some(map) = body.as_object_mut() {
        for field in CLEARABLE_STATUS_FIELDS {
            map.entry(field.to_string())
                .or_insert(serde_json::Value::Null);
        }
    }
    json!({ "status": body })
}

/// Decide one `Approval` and patch **only** its `/status`.
///
/// Nothing else is touched: not the `spec` (sealed by CEL; an approval whose
/// bytes a controller could edit is not an approval), and never a `DELETE`: a
/// refused `Approval` stays as the audit trail of a rejected attempt.
///
/// # Errors
///
/// [`ReconcileError`] for anything that is not a verdict.
pub async fn reconcile_approval(
    approval: &Approval,
    client: &kube::Client,
) -> Result<ApprovalOutcome, ReconcileError> {
    reconcile_approval_with_policy(approval, client, &ApprovalPolicySet::default()).await
}

/// [`reconcile_approval`], under the installation's approval policies.
///
/// # Errors
///
/// [`ReconcileError`] for anything that is not a verdict.
pub async fn reconcile_approval_with_policy(
    approval: &Approval,
    client: &kube::Client,
    policies: &ApprovalPolicySet,
) -> Result<ApprovalOutcome, ReconcileError> {
    reconcile_approval_with_policy_at(approval, client, policies, Utc::now()).await
}

/// [`reconcile_approval_with_policy`] with the clock passed in, so a test can
/// reconcile one fixed document at two instants and count the writes.
///
/// # Errors
///
/// [`ReconcileError`] for anything that is not a verdict.
pub async fn reconcile_approval_with_policy_at(
    approval: &Approval,
    client: &kube::Client,
    policies: &ApprovalPolicySet,
    now: DateTime<Utc>,
) -> Result<ApprovalOutcome, ReconcileError> {
    let name = approval.name_any();
    let namespace = approval
        .namespace()
        .ok_or_else(|| ReconcileError::NoNamespace(name.clone()))?;
    let outcome = decide_with_policy_at(approval, client, policies, now).await?;
    let status = status_for(approval, &outcome, now);

    let api: Api<Approval> = Api::namespaced(client.clone(), &namespace);
    let patch = status_patch_body(&status);
    let current = approval
        .status
        .as_ref()
        .and_then(|s| serde_json::to_value(s).ok());
    // The log line follows the write: an unchanged verdict is logged at
    // debug, not as a warning on every pass.
    let changed = !crate::conditions::status_unchanged(current.as_ref(), &patch);
    // No write when nothing changed: an `Approval`'s verdict is a function of
    // its sealed spec, the trust and the referent, and a write on every pass
    // would wake its own watch (7,114 reconciles in 90 s for one steady
    // object). Under seam S7's precondition, because two passes over one
    // `Approval` can overlap and a verdict from a stale watch-cache copy must
    // not overwrite a newer one.
    crate::conditions::patch_status_preconditioned(
        &api,
        "Approval",
        &name,
        &StatusVersion::observed(approval.meta()),
        current.as_ref(),
        patch,
    )
    .await?;

    if !changed {
        debug!(
            approval = %name,
            namespace = %namespace,
            reason = outcome.reason(),
            consumed = outcome.is_consumed(),
            "approval verdict unchanged; nothing written"
        );
    } else if outcome.is_consumed() {
        info!(
            approval = %name,
            namespace = %namespace,
            "approval consumed by its Restore's admission; the verified record is kept and not \
             judged again"
        );
    } else if outcome.is_verified() {
        info!(
            approval = %name,
            namespace = %namespace,
            reason = outcome.reason(),
            "approval verified"
        );
    } else {
        // Warn, not error: a refused approval is an expected outcome of this
        // controller doing its job.
        warn!(
            approval = %name,
            namespace = %namespace,
            reason = outcome.reason(),
            "approval refused"
        );
    }
    Ok(outcome)
}

/// The `kube::runtime` reconcile entry point.
async fn reconcile(
    approval: Arc<Approval>,
    ctx: Arc<Context>,
    policies: crate::approval_policy::PolicySource,
) -> Result<Action, ReconcileError> {
    // The document with the fresh-install marker as last read.
    let policies = policies.effective();
    let outcome = reconcile_approval_with_policy(&approval, &ctx.client, &policies).await?;
    // Not `Action::await_change()`: this controller does not watch
    // `TrustRoster`, so a roster installed after the approval would never be
    // noticed; the five-minute heartbeat makes that self-healing. But an
    // expired key must not wait five minutes, so a verified outcome requeues
    // at its own `notAfter` (see `ApprovalOutcome::valid_until` for why the
    // asymmetry is the safe one).
    Ok(requeue_for(&outcome, Utc::now()).map_or_else(Action::await_change, Action::requeue))
}

/// When this outcome is looked at again; `None` is "only when something
/// changes" (`Action::await_change`).
///
/// A consumed verdict waits for a change: nothing can alter a record of an
/// admission that already happened. Otherwise a verified outcome requeues at
/// its own `notAfter`/expiry and everything else at the heartbeat; never a hot
/// loop, because a refusal's message carries no clock and an unchanged status
/// is never written.
#[must_use]
pub fn requeue_for(outcome: &ApprovalOutcome, now: DateTime<Utc>) -> Option<std::time::Duration> {
    if outcome.is_consumed() {
        return None;
    }
    Some(super::trust_policy::requeue_before(
        outcome.valid_until(),
        now,
    ))
}

/// Requeue on an error, naming it. Never a panic and never a drop.
fn error_policy(approval: Arc<Approval>, err: &ReconcileError, _ctx: Arc<Context>) -> Action {
    warn!(
        approval = %approval.name_any(),
        error = %err,
        "approval reconcile failed; requeueing"
    );
    Action::requeue(std::time::Duration::from_secs(30))
}

/// The `Approval`s a `TrustPolicy` event should enqueue: the re-trust trigger
/// `controllers::backup` and `controllers::restore` carry too.
///
/// The verdict is a function of the namespace's resolved trust, and without
/// this a policy edit (a revoked key, a narrowed `notAfter`) would reach an
/// `Approval` only at the five-minute heartbeat. `objects` is this controller's
/// own store, so the mapping costs no API calls and over-approximates on the
/// safe side ([`crate::verification::targets_in_scope`]).
fn policy_targets(
    objects: &reflector::Store<Approval>,
    scopes: &crate::trust::PolicyScopeMemory,
    policy: &crate::crds::trust_policy::TrustPolicy,
) -> Vec<ObjectRef<Approval>> {
    // The union of before and after: a `watches` mapper sees only the new
    // object, so an edit that narrows the scope would otherwise enqueue
    // nothing in the namespace it stopped governing. See
    // `trust::PolicyScopeMemory`.
    let scope = scopes.observe(policy);
    crate::verification::targets_in_scope(objects.state(), &scope)
}

/// Run the `Approval` controller until the process ends.
///
/// `Api::all`: this controller reconciles approvals in every namespace, one
/// controller per cluster.
///
/// `policies` is the installation's approval-policy document, read once by
/// `main` with the fresh-install marker as last read; an empty, unmarked set is
/// every namespace on `legacy-governed-v1`.
pub fn controller(
    client: kube::Client,
    policies: crate::approval_policy::PolicySource,
) -> impl std::future::Future<Output = ()> + Send {
    // One watch per watched namespace: `crate::scope` is the whole cluster
    // unless `LOGWEIR_WATCH_NAMESPACES` names the execution namespaces, and
    // then this reconciler runs once per namespace with an `Api::namespaced`
    // watch, the only shape the scoped chart's RoleBindings permit. Every copy
    // shares the one policy set.
    crate::scope::run_everywhere(move |namespace| {
        controller_in(client.clone(), namespace, policies.clone())
    })
}

/// One watch of [`controller`], over `namespace` (`None` is the whole
/// cluster).
fn controller_in(
    client: kube::Client,
    namespace: Option<String>,
    policies: crate::approval_policy::PolicySource,
) -> impl std::future::Future<Output = ()> + Send {
    let api: Api<Approval> = crate::scope::api(&client, namespace.as_deref());
    let policy_api: Api<crate::crds::trust_policy::TrustPolicy> = Api::all(client.clone());
    // One memory of what each policy bound last, owned by the mapper.
    let scopes = Arc::new(crate::trust::PolicyScopeMemory::default());
    // This reconciler holds no archive handle, spelled out so the one context
    // field that is a capability is visible at every construction site.
    let ctx = Arc::new(Context {
        client,
        archive: None,
        // This reconciler creates no runner Job, so the image is "unused",
        // never "no override is configured" (see `super::Context::runner_image`).
        runner_image: crate::job::RunnerImage::default(),
    });
    async move {
        let controller = Controller::new(api, watcher::Config::default());
        let objects = controller.store();
        controller
            // The re-trust trigger: a `TrustPolicy` event maps to the
            // `Approval`s in the namespaces that policy could govern (see
            // [`policy_targets`]). `decide` resolves trust live on every pass,
            // so the trigger only has to say when.
            .watches(policy_api, watcher::Config::default(), move |policy| {
                policy_targets(&objects, &scopes, &policy)
            })
            .run(
                move |approval, ctx| reconcile(approval, ctx, policies.clone()),
                error_policy,
                ctx,
            )
            // Every item is already logged by `reconcile_approval` or
            // `error_policy`; the stream exists to be driven.
            .for_each(|_| std::future::ready(()))
            .await;
    }
}
