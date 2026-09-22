//! The `Approval` reconciler: five checks, in an order that is testable.
//!
//! # Why this file exists
//!
//! *"A `DrillApproval` that exists is not an approval; a `DrillApproval` whose
//! status the controller set to `Verified=True` is"*
//! (`docs/mvp/design-operator.md:93-95`; `DrillApproval` is the corpus's name
//! for what Global Constraint 34 renames [`Approval`]). Creating an
//! `Approval` object is a `POST` any namespace tenant can make. What makes it
//! an authorisation is a DSSE signature, by a key on the one cluster-scoped
//! [`TrustRoster`], over bytes that bind **this** plan and **this** kind of
//! subject. This module is that sentence.
//!
//! # The order is load-bearing, and getting it wrong collapses every refusal
//!
//! [`logweir_verify::verify_detached`] refuses a `payloadType` mismatch
//! FIRST (`crates/logweir-verify/src/verify.rs:18-29`) and returns
//! `"no signature by key <want> in the sidecar"` for a key that is not in the
//! sidecar at all (`:66-68`). Both come back as
//! `logweir_verify::Error::Verify`. So an algorithm that simply loops the
//! roster's keys calling `verify_detached` reports *the same refusal* for a
//! substituted document, for an attacker's key, and for a genuinely broken
//! signature — three findings an operator must be able to tell apart, flattened
//! into one. [`evaluate`] therefore makes its own distinctions BEFORE it
//! reaches the crypto, and the checks are numbered in the code so a reviewer
//! can read the order off the source.
//!
//! # `approvalBytes` is document text, never base64
//!
//! Interface **I18**, spec §3.2. The reconciler passes
//! `approval.spec.approvalBytes.as_bytes()` straight into [`evaluate`] with no
//! decode step, and Task 20 hashes exactly those bytes. A base64 layer between
//! the approver's file and the verified bytes is the class of transformation
//! `planBytes` exists to forbid, and
//! `approval_bytes_are_the_document_text_not_base64` asserts the two paths are
//! DISTINGUISHABLE — the raw-text fixture verifies and a base64 of the same
//! document does not — so a future decode step cannot be added silently.
//!
//! # What this module does not claim
//!
//! It verifies. It holds no key material of its own, and this crate links
//! [`logweir_verify`] and never the crate that holds the signing half (Global
//! Constraint 27; `scripts/check-one-signer.sh`). That is a LINKAGE property
//! and the narrowed position Global Constraint 27 records is the whole of it:
//! the *capability* to sign is unbroken while this controller holds Job CRUD
//! over the signing key's namespace, and nothing here changes that.
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
    self as policy, ApprovalMode, ApprovalPolicy, ApprovalPolicySet, AuthorizationRefusal,
    EffectivePolicy, ExpectedSubject, RestoreAuthorization, PAYLOAD_TYPE_RESTORE_AUTHORIZATION,
};
use logweir_core::ids::sha256_prefixed;
use logweir_core::trust::{KeyUsage, SigningRefusal};
use logweir_verify::{verify_detached, Sidecar, VerifyingKey};
use serde::Deserialize;
use serde_json::json;
use tracing::{info, warn};

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
/// BYTE-EQUAL to `crates/logweir/src/drill/phase1_approval.rs:12`, and it has
/// to be: the same document is verified by the CLI at phase 1 and by this
/// controller before a Job exists. `the_payload_type_is_byte_equal_to_the_cli`
/// reads the other constant out of that file and compares, so the two cannot
/// drift without a red test.
pub const PAYLOAD_TYPE_APPROVAL: &str = "application/vnd.logweir.drill-approval+json;version=1.0.0";

/// The name of the one cluster-scoped [`TrustRoster`] — interface **I16**.
///
/// **A FACT, NOT A CONVENTION.** Both reconcilers in this directory and Task
/// 24's evidence verification resolve `trustrosters/default` by this constant;
/// the name is not read from an `Approval`, from a flag or from an
/// environment variable, because a roster whose name the subject supplies is a
/// roster the subject can choose. It is re-exported from the crate root as
/// [`crate::ROSTER_NAME`], which is the ONE path Tasks 20, 21, 24, 27 and 28
/// name.
///
/// An adopter who names their roster something else gets
/// [`ROSTER_NOT_FOUND_MESSAGE`] — an explanation, not silence.
///
/// [`TrustRoster`]: crate::crds::trust_roster::TrustRoster
pub const ROSTER_NAME: &str = "default";

/// The condition type this reconciler owns.
pub const CONDITION_VERIFIED: &str = "Verified";

/// The `reason` written when every check passed.
pub const REASON_VERIFIED: &str = "Verified";

/// The message a missing roster produces — interface **I16**, verbatim.
///
/// NEVER A SILENT REFUSAL. The single most likely way to reach this is an
/// install that skipped step 1, and "Verified=False, reason RosterNotFound"
/// with no message would send the operator looking at their key instead of at
/// their install. `the_missing_roster_message_names_the_roster` asserts this
/// string contains [`ROSTER_NAME`], so the two cannot drift apart.
pub const ROSTER_NOT_FOUND_MESSAGE: &str =
    "no cluster-scoped TrustRoster named 'default'; see docs/kubernetes.md install step 1";

/// Why an `Approval` was refused.
///
/// THE SET IS CLOSED. Tasks 20, 24 and 27 read
/// [`Self::reason`] off `status.conditions[type=Verified].reason` and route on
/// it, so a variant is an interface and not an implementation detail.
///
/// `key_id` ON THE TWO ROSTER VARIANTS IS AN IDENTIFICATION STRING, NOT
/// ALWAYS ONE ID. Check 3 has two ids to name (the declared one and the sha256
/// of the key material it claims to describe) and check 4 has two LISTS to
/// name (the sidecar's and the roster's). Both are refusals about *which key
/// ids are involved*, and the brief requires each to name every id it
/// compared; a single `String` that says so is what
/// `approval_refuses_a_key_outside_the_roster` reads.
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
    /// The matched key is declared `Retired` — PLAT-19.1, D3 §7.4.
    ///
    /// **NOT [`Self::KeyIdExpired`], although §7.4 says a retired key "refuses
    /// a fresh approval exactly as `KeyIdExpired` does today".** It refuses the
    /// same way; it does not refuse for the same reason, and `KeyIdExpired`
    /// carries a `notAfter` a retired key has not reached. Reporting a
    /// deliberate retirement as an expiry would send an operator to extend a
    /// window that is not the problem.
    KeyRetired {
        /// The matched key's `keyId`.
        key_id: String,
        /// Its `retiredAt`, RFC 3339, when the policy records one.
        retired_at: Option<String>,
    },
    /// The matched key is declared `Revoked` — PLAT-19.1, D3 §7.4.
    ///
    /// ITS OWN REASON, AND THAT IS THE POINT. A revocation is the one key
    /// event an operator must never read as an expiry: `KeyCompromise` means
    /// the private half is believed to be in someone else's hands, and the
    /// remedy is an investigation, not a longer `notAfter`.
    KeyRevoked {
        /// The matched key's `keyId`.
        key_id: String,
        /// `KeyCompromise`, `Superseded` or `Unspecified`.
        reason: String,
        /// `revocationEffectiveFrom`, RFC 3339, when the policy records one.
        effective_from: Option<String>,
    },
    /// The matched key's validity window has not opened yet — PLAT-19.1.
    ///
    /// REACHABLE ONLY UNDER A REAL `TrustPolicy`: §7.6 step 1 stages a
    /// successor key before the cutover, and a future `notBefore` is the
    /// natural way to do that. The synthesised `legacy-roster-v1` gives every
    /// key a `notBefore` at the Unix epoch, so no roster-only cluster can reach
    /// this.
    KeyNotYetValid {
        /// The matched key's `keyId`.
        key_id: String,
        /// Its `notBefore`, RFC 3339.
        not_before: String,
    },
    /// Two or more `TrustPolicy` objects claim this namespace, so it resolves
    /// to **nothing** — PLAT-19.1, D3 §7.1.
    ///
    /// NOT A VERDICT ABOUT THE APPROVER. Picking one of two contesting
    /// policies would be a trust decision made by a sort order, and the safe
    /// reading of a disagreement about authority is that there is none. The
    /// same word appears on both policies' `status.conflicts`.
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
    /// A standing document's `scope.templateDigest` names another sealed
    /// schedule template.
    TemplateDigestMismatch {
        /// The digest inside the signed scope.
        got: String,
        /// The digest recomputed from the referent's sealed spec.
        want: String,
    },
    /// A standing document names another schedule identity.
    SubjectMismatch {
        /// A precise description of the signed and observed identities.
        detail: String,
    },
    /// A standing document's issue/expiry window is not usable now.
    WindowInvalid {
        /// The exact invalid boundary or duration.
        detail: String,
    },
    /// A standing document is malformed or carries an empty scope.
    ScopeInvalid {
        /// The exact missing or unsupported scope member.
        detail: String,
    },
    /// Authentic bytes are not a supported standing-authorization document.
    StandingDocumentInvalid {
        /// The parse, format-version or document-kind mismatch.
        detail: String,
    },
    /// PLAT-19.2: the authentic bytes are not a well-formed authorization
    /// document v2, or name no requester.
    AuthorizationDocumentInvalid {
        /// The parse, format or requester failure.
        detail: String,
    },
    /// PLAT-19.2: a v2 document names another object than this referent —
    /// another namespace, name or UID.
    AuthorizationSubjectMismatch {
        /// The signed and observed identities.
        detail: String,
    },
    /// PLAT-19.2: the document's policy, digest or mode is not the namespace's
    /// current binding — including a v1 document under an explicit binding
    /// and a v2 document in an unbound namespace. THE DOWNGRADE REFUSAL: an
    /// ordinary confirmation presented in a governed namespace lands here.
    ApprovalPolicyMismatch {
        /// Both policies, named.
        detail: String,
    },
    /// PLAT-19.2: the document's validity window is malformed, longer than the
    /// policy allows, or issued in the future.
    AuthorizationWindowInvalid {
        /// The exact boundary.
        detail: String,
    },
    /// PLAT-19.2: the document's `expiresAt` has passed.
    AuthorizationExpired {
        /// The instant and the clock.
        detail: String,
    },
    /// PLAT-19.2: a `Governed` document carries the console's confirmation
    /// and no approver countersignature yet. NOT A BAD SIGNATURE: it is the
    /// pending state of a governed request, and the one a console-only
    /// document written directly to the API server stays in for ever.
    GovernedApprovalRequired {
        /// The requester and policy the confirmation names.
        detail: String,
    },
    /// PLAT-19.2: the governed approver's principal is the requester's —
    /// self-approval, refused where the policy requires independence.
    SelfApprovalRefused {
        /// Both principals.
        detail: String,
    },
    /// There is no cluster-scoped [`TrustRoster`] named [`ROSTER_NAME`].
    ///
    /// The one variant [`evaluate`] can never return — it takes a
    /// [`TrustRosterSpec`] by reference, so by the time it runs a roster
    /// exists. The reconciler produces it, which is why it lives in the same
    /// closed set the condition reasons come from.
    ///
    /// [`TrustRoster`]: crate::crds::trust_roster::TrustRoster
    RosterNotFound,
}

impl ApprovalRefusal {
    /// The condition `reason` for this refusal: the variant's own name.
    ///
    /// NO WILDCARD ARM, DELIBERATELY. An eighth variant must fail to compile
    /// here rather than reach the cluster as a `reason` nobody chose.
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
            Self::TemplateDigestMismatch { .. } => "TemplateDigestMismatch",
            Self::SubjectMismatch { .. } => "SubjectMismatch",
            Self::WindowInvalid { .. } => "WindowInvalid",
            Self::ScopeInvalid { .. } => "ScopeInvalid",
            Self::StandingDocumentInvalid { .. } => "StandingDocumentInvalid",
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
            Self::TemplateDigestMismatch { got, want } => write!(
                f,
                "the standing authorization scope names template digest {got}, but the referred \
                 RehearsalSchedule's sealed spec hashes to {want}"
            ),
            Self::SubjectMismatch { detail } => write!(f, "{detail}"),
            Self::WindowInvalid { detail } => write!(f, "{detail}"),
            Self::ScopeInvalid { detail } => write!(f, "{detail}"),
            Self::StandingDocumentInvalid { detail }
            | Self::AuthorizationDocumentInvalid { detail }
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
    /// REASON strings are the same on both sides
    /// (`AuthorizationRefusal::reason`), so a condition written here and a
    /// Restore admission refusal written from the same check read alike.
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
        }
    }
}

/// The whole verdict, when every check passed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Verified {
    /// The `keyId` of the roster entry whose key actually verified the
    /// signature — the value [`verify_detached`] RETURNED, never
    /// `sidecar.signatures[0].keyid`.
    pub matched_key_id: String,
    /// The policy usage under which the matched signature was admitted.
    /// The policy usage under which this build admitted the signature.
    /// Until PLAT-19.2 carries an immutable policy mode through the standing
    /// document and execution bundle, every approval requires
    /// `GovernedApproval` and `ConsoleConfirmation` fails closed.
    pub authorization_usage: KeyUsage,
    /// **The matched key's declared validity window** — `None` only when the
    /// resolved trust does not carry the matched id at all, which check 6 has
    /// already made unreachable.
    ///
    /// TWO CONSUMERS, ONE FACT. [`Self::valid_until`] reads `notAfter` off it
    /// to requeue AT the key's boundary instead of at the next heartbeat
    /// (defect `TRUST-EXPIRY-LAG`: on lab-refresh-4 an approval read
    /// `Verified=True` across 22 samples over 2 m 35 s after its approver key
    /// expired, at one unchanged `resourceVersion`), and [`status_for`]
    /// publishes the whole of it so the restore preflight can compare a
    /// deadline against it (defect `APPROVAL-KEY-WINDOW-UNPUBLISHED`).
    ///
    /// **IT WAS A BARE `valid_until` UNTIL THE SECOND CONSUMER ARRIVED**, and
    /// keeping one field rather than adding a second is the point: a published
    /// window and a requeue deadline that could disagree is a status saying the
    /// key is good until 12:00 while the controller sleeps until 13:00. They
    /// are the same value read twice.
    pub key_window: Option<ApproverKeyWindow>,
    /// The authenticated document's own expiry, when this document kind has
    /// one. A standing verdict is valid only until the earlier of this instant
    /// and the matched key's `notAfter`.
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
    /// `legacy-roster-v1` for the synthesised roster (D3 §7.5).
    ///
    /// IT REACHES THE CONDITION MESSAGE AS `trustSource=<name>`, which is what
    /// D3 §15's L10 asserts on an unmigrated cluster and what tells an operator
    /// looking at one `Approval` whether their migration is live.
    pub trust_source: String,
    /// PLAT-19.2: the policy, mode and console-attested requester a v2
    /// authorization was verified under; `None` for a v1 approval document.
    /// Boxed so `ApprovalOutcome`'s variants stay comparable in size.
    pub authorization: Option<Box<AuthorizationProvenance>>,
    /// Filled by [`decide`] with the API object whose bytes were checked.
    /// Pure [`evaluate`] callers have no Kubernetes referent and leave it
    /// absent; only a reconcile outcome is written to status.
    pub verified_subject_ref: Option<VerifiedSubjectRef>,
}

impl Verified {
    /// **The instant this verdict stops being true on its own** — the earlier
    /// of the authenticated document's expiry and the matched key's
    /// `notAfter`.
    ///
    /// THE FALLBACK IS A DEADLINE THAT NEVER FIRES RATHER THAN A PANIC. A
    /// `None` window means the resolved trust does not carry the id check 6
    /// resolved through `may_sign_new_for`, which cannot happen; an admission
    /// path may not abort on an invariant it merely believes, so the timer
    /// falls back to the heartbeat and the STATUS publishes nothing at all —
    /// unknown, never valid.
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
/// EVERY FIELD DEFAULTS, AND THAT IS FAIL-CLOSED, NOT LENIENT. `plan_hash` and
/// `subject_kind` are compared against values the controller computes itself,
/// so an absent field yields `""`, which matches no real hash and no real
/// kind: the approval is refused by check 7 or check 8 with a message naming
/// what it did and did not carry. Making them `required` instead would route
/// the same document to a *parse* error, which reads as "the file is corrupt"
/// rather than "this approval does not bind a plan".
///
/// `subject_kind` is written by `logweir drill approve` (Task 22 extends it)
/// and is therefore INSIDE the signed bytes — which is the only reason check 8
/// is worth anything.
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
/// # The order, and why each step is where it is
///
/// 0. Parse `sidecar_bytes` as a [`Sidecar`]; a parse failure is
///    [`ApprovalRefusal::SignatureInvalid`] naming the parse error.
/// 1. `sidecar.payload_type != PAYLOAD_TYPE_APPROVAL` →
///    [`ApprovalRefusal::PayloadTypeMismatch`] with both strings **in full**,
///    **before any key is tried**. [`verify_detached`] also refuses a
///    mismatch, so checking it here is what makes the refusal
///    DISTINGUISHABLE from a bad signature.
/// 2. Parse **every** `approverKeys[]` entry. If any fails,
///    [`ApprovalRefusal::SignatureInvalid`] naming that `keyId` — a partially
///    loaded roster is not a roster, and no approval is accepted against one.
/// 3. For each parsed entry, `entry.keyId == key.key_id()`; a disagreement is
///    [`ApprovalRefusal::KeyIdNotInRoster`] naming both, before any signature
///    is checked. An entry whose declared id is not the hash of its own key
///    material is not a roster entry a signature can be looked up in.
/// 4. If no `sidecar.signatures[].keyid` equals any roster entry's key id,
///    [`ApprovalRefusal::KeyIdNotInRoster`] naming the sidecar's key ids —
///    **this, and not a signature failure, is the verdict for a key outside
///    the roster.**
/// 5. [`verify_detached`] for each entry whose key id matched; the first `Ok`
///    wins, otherwise [`ApprovalRefusal::SignatureInvalid`] with the last
///    error's `Display`.
/// 6. The matched entry's `notAfter` must be in the future relative to `now`.
/// 7. The document's `plan_hash` must equal
///    `sha256_prefixed(referent_plan_bytes)`, **recomputed here** and never
///    read from the referent's status.
/// 8. The document's `subject_kind` must equal `referent_kind`.
///
/// # Why the document is parsed between 6 and 7
///
/// Steps 1 through 6 are answerable from the SIDECAR and the ROSTER alone, and
/// keeping them that way is what lets each of them have a test that cannot be
/// satisfied by accident: `approval_refuses_a_wrong_payload_type` passes a
/// roster whose only entry is deliberately unparseable, so reaching step 2
/// would change the verdict. Parsing the payload earlier would add a *ninth*
/// way to reach `SignatureInvalid` ahead of checks whose whole point is not to
/// be `SignatureInvalid`. By step 7 the bytes are known-authentic, so a parse
/// failure there means "this authentic document is not an approval document",
/// which is what its message says.
///
/// # Why the fifth check exists at all
///
/// Without it the second approval degenerates. An `Approval` whose `planHash`
/// matches a `Restore` would be accepted for a `Switchover`, and "a valid
/// signature by a rostered key exists in this namespace" is a property any
/// approved restore in that namespace already produced. Tag 1 has no
/// `Switchover`, so the check is cheap now and expensive to retrofit — and its
/// absence would be invisible until tag 2.
///
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
        None,
    )
}

/// Verify the standing document carried by an `Approval` for one exact
/// `RehearsalSchedule` referent.
///
/// This is deliberately a second typed entry point rather than a permissive
/// parse in [`evaluate`]: the per-run path continues to parse only
/// [`ApprovalDocument`], while this path requires every field of
/// [`logweir_core::execution_contract::StandingAuthorization`].
pub fn evaluate_standing(
    approval_bytes: &[u8],
    sidecar_bytes: &[u8],
    trust: &ResolvedTrust,
    now: DateTime<Utc>,
    referent: &VerifiedSubjectRef,
    template_digest: &str,
) -> Result<Verified, ApprovalRefusal> {
    evaluate_inner(
        approval_bytes,
        sidecar_bytes,
        trust,
        now,
        SubjectKind::RehearsalSchedule.as_str(),
        &[],
        Some((referent, template_digest)),
    )
}

/// Validate the authenticated claims of one standing document against the
/// schedule object the API server returned.
pub fn validate_standing_document(
    doc: &logweir_core::execution_contract::StandingAuthorization,
    referent: &VerifiedSubjectRef,
    template_digest: &str,
    now: DateTime<Utc>,
) -> Result<(), ApprovalRefusal> {
    use logweir_core::execution_contract as wire;

    if doc.format_version.split('.').next() != Some("1")
        || doc.kind != wire::STANDING_AUTHORIZATION_KIND
    {
        return Err(ApprovalRefusal::StandingDocumentInvalid {
            detail: format!(
                "the standing authorization declares formatVersion {:?} and kind {:?}; this \
                 build reads major 1 and kind {:?}",
                doc.format_version,
                doc.kind,
                wire::STANDING_AUTHORIZATION_KIND
            ),
        });
    }
    let subject = &doc.subject_ref;
    if subject.api_version != referent.api_version
        || subject.kind != SubjectKind::RehearsalSchedule.as_str()
        || subject.namespace != referent.namespace
        || subject.name != referent.name
        || subject.uid != referent.uid
    {
        return Err(ApprovalRefusal::SubjectMismatch {
            detail: format!(
                "the signed standing subject is {}/{}/{} UID {}, but the referent is \
                 {}/{}/{} UID {}",
                subject.api_version,
                subject.namespace,
                subject.name,
                subject.uid,
                referent.api_version,
                referent.namespace,
                referent.name,
                referent.uid
            ),
        });
    }
    let lifetime = doc.expires_at - doc.issued_at;
    if lifetime <= chrono::Duration::zero()
        || lifetime > chrono::Duration::days(wire::MAX_STANDING_AUTHORIZATION_DAYS)
        || doc.issued_at > now
        || doc.expires_at <= now
    {
        return Err(ApprovalRefusal::WindowInvalid {
            detail: format!(
                "the standing authorization window {}..{} is not valid at {} or exceeds {} days",
                doc.issued_at.to_rfc3339(),
                doc.expires_at.to_rfc3339(),
                now.to_rfc3339(),
                wire::MAX_STANDING_AUTHORIZATION_DAYS
            ),
        });
    }
    let scope = &doc.scope;
    let scope_invalid = scope.template_digest.trim().is_empty()
        || scope.target_cluster_id.trim().is_empty()
        || scope.topic_prefix.trim().is_empty()
        || scope.topics.is_empty()
        || scope.topics.iter().any(|topic| topic.trim().is_empty())
        || scope.max_partitions == 0
        || scope.records_per_partition == 0
        || scope.deadline_seconds == 0
        || !scope.is_scratch_only();
    if scope_invalid {
        return Err(ApprovalRefusal::ScopeInvalid {
            detail: "the standing authorization scope must name a template digest, target \
                     cluster, topic prefix, at least one non-blank topic, positive bounds, and \
                     exactly scratch mode"
                .to_string(),
        });
    }
    if scope.template_digest != template_digest {
        return Err(ApprovalRefusal::TemplateDigestMismatch {
            got: scope.template_digest.clone(),
            want: template_digest.to_string(),
        });
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn evaluate_inner(
    approval_bytes: &[u8],
    sidecar_bytes: &[u8],
    trust: &ResolvedTrust,
    now: DateTime<Utc>,
    referent_kind: &str,
    referent_plan_bytes: &[u8],
    standing: Option<(&VerifiedSubjectRef, &str)>,
) -> Result<Verified, ApprovalRefusal> {
    // ---- 0. the sidecar is a document before it is a signature -----------
    let sidecar: Sidecar = serde_json::from_slice(sidecar_bytes).map_err(|e| {
        ApprovalRefusal::SignatureInvalid(format!(
            "spec.sidecarBytes is not a DSSE sidecar document: {e}"
        ))
    })?;

    // ---- 1. the payload type, BEFORE ANY KEY IS TRIED --------------------
    //
    // WHICH payload type depends on the REFERENT KIND, and that is the whole of
    // D3 W7's change to this function (PLAT-14.3). A `RehearsalSchedule`
    // subject carries a STANDING rehearsal authorization, whose DSSE payload
    // type is its own
    // (`execution_contract::PAYLOAD_TYPE_STANDING_AUTHORIZATION`) precisely so
    // that a genuinely signed drill approval replayed as a standing
    // authorization — or the reverse — is refused by the SIGNATURE layer rather
    // than by a field comparison somebody could forget to write. The payload
    // type is part of what `verify_detached` covers, so choosing it here is
    // choosing what the signature is over.
    let want_payload =
        if referent_kind == crate::crds::approval::SubjectKind::RehearsalSchedule.as_str() {
            logweir_core::execution_contract::PAYLOAD_TYPE_STANDING_AUTHORIZATION
        } else {
            PAYLOAD_TYPE_APPROVAL
        };
    if sidecar.payload_type != want_payload {
        return Err(ApprovalRefusal::PayloadTypeMismatch {
            got: sidecar.payload_type,
            want: want_payload.to_string(),
        });
    }

    // ---- 2. the WHOLE approver key set parses, or nothing is accepted ----
    //
    // CARRIED AS DATA BY THE RESOLUTION LAYER, not re-derived here. The rule
    // is the ROSTER's ("a partially loaded roster is not a roster") and it
    // stops at the roster: a real `TrustPolicy` reports one unparseable key as
    // one `Unparseable` key and keeps the other 63 working, because it has
    // somewhere to record which entry failed. `blocked_for` carries the
    // legacy message byte-for-byte, so the sentence an operator reads did not
    // change on the day this path replaced the roster walk.
    // This build does not yet carry PLAT-19.2's immutable ordinary/governed
    // policy mode through the signed standing document. Admitting a
    // ConsoleConfirmation key merely because one exists would let an ordinary
    // confirmation authorize governed work. Fail closed on the current
    // format: both per-run and standing documents require GovernedApproval.
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

    // ---- 3. an entry must agree with its own key material ----------------
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

    // ---- 4. a key outside the resolved trust is NOT a signature failure ---
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

    // ---- 5. the crypto, and only now --------------------------------------
    //
    // `verify_detached` returns the MATCHED keyid, never `signatures[0]`
    // (`crates/logweir-verify/src/verify.rs:60`). A sidecar carrying two
    // signatures — one by a rostered key, one by an attacker's — verifies
    // under exactly one of them, and only that one may be reported. This
    // binding is the whole reason the return value is used instead of being
    // discarded in favour of an id already in hand.
    let mut last_error: Option<String> = None;
    let mut hit: Option<String> = None;
    for entry in matching {
        let Ok(key) = VerifyingKey::from_pem_str(&entry.spki_pem) else {
            // Unreachable: `is_usable()` already required a parsed PEM. Named
            // rather than unwrapped, because a panic in an admission path is a
            // denial of service.
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

    // ---- 6. THE MATCHED KEY MUST BE ABLE TO SIGN SOMETHING NEW ------------
    //
    // D3 §7.4's first question, and this is the call site that makes it a
    // question at all: *admission is a new use*. An `Approval` object that
    // arrives today carrying a signature by a key retired last week is asking
    // whether that key may authorise something now — not whether an archive it
    // signed last year still verifies, which is `decide`'s question and has a
    // different answer.
    //
    // The MATCHED key's lifecycle, not the first key's: a policy may hold
    // several approver keys and only one of them signed this.
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

    // ---- the bytes are authentic; now read the RIGHT document shape -------
    let (approver, ticket, document_expires_at) =
        if let Some((referent, template_digest)) = standing {
            use logweir_core::execution_contract as wire;

            let doc: wire::StandingAuthorization =
                serde_json::from_slice(approval_bytes).map_err(|e| {
                    ApprovalRefusal::StandingDocumentInvalid {
                        detail: format!(
                            "the signature over spec.approvalBytes verified under key id \
                     {matched_key_id}, but those bytes are not a standing authorization: {e}"
                        ),
                    }
                })?;
            validate_standing_document(&doc, referent, template_digest, now)?;
            // v1 standing documents intentionally carry no person/ticket fields.
            (String::new(), String::new(), Some(doc.expires_at))
        } else {
            let doc: ApprovalDocument = serde_json::from_slice(approval_bytes).map_err(|e| {
                ApprovalRefusal::SignatureInvalid(format!(
                "the signature over spec.approvalBytes verified under key id {matched_key_id}, but \
                 those bytes are not an approval document: {e}"
            ))
            })?;

            // ---- 7. the plan hash, RECOMPUTED from the referent's own bytes ---
            let want = sha256_prefixed(referent_plan_bytes);
            if doc.plan_hash != want {
                return Err(ApprovalRefusal::PlanHashMismatch {
                    got: doc.plan_hash,
                    want,
                });
            }

            // ---- 8. the subject kind, from inside the signed bytes ------------
            if doc.subject_kind != referent_kind {
                return Err(ApprovalRefusal::SubjectKindMismatch {
                    approval_says: doc.subject_kind,
                    referent_is: referent_kind.to_string(),
                });
            }
            (doc.approver, doc.ticket, None)
        };

    // LABELLED, NEVER REFUSED — and under a real `TrustPolicy` it is
    // STRUCTURALLY IMPOSSIBLE rather than merely absent. D3 §7.3's CEL rule G8
    // makes a policy key declare exactly one usage, so `has_usage` here can
    // only be true for the synthesised `legacy-roster-v1`, whose two lists may
    // name the same key id. That is precisely §7.3's "the label remains for
    // legacy-roster namespaces": the label goes on where the separation cannot
    // be enforced, and the enforcement replaces it where it can.
    let self_attested_risk = trust
        .key(&matched_key_id)
        .is_some_and(|k| k.trust.has_usage(KeyUsage::EvidenceSigning));

    // THE MATCHED KEY'S OWN WINDOW, and it is present by construction: check 6
    // above resolved this id through `may_sign_new_for`, which refuses
    // `UntrustedSigner` when the resolved trust does not carry it. THE MATCHED
    // key's, never the first key's — a policy may hold several approver keys
    // and only one of them signed this, and publishing another one's window
    // would be a deadline about a key that authorised nothing.
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

/// One required signature, found and verified under a key of `usage` from
/// the resolved trust — the v2 twin of [`evaluate_inner`]'s checks 2-6. Only
/// keys DECLARING `usage` are offered, so a key of another usage can never
/// satisfy it (D3 §7.3; G8 makes every policy key carry exactly one).
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

/// **Authorization document v2** (PLAT-19.2, decision D0), as a pure function
/// of bytes, the namespace's resolved trust, its BOUND approval policy and the
/// referent — the ordinary and the governed mode in one order.
///
/// # The order
///
/// 0. The sidecar parses.
/// 1. Its `payloadType` is [`PAYLOAD_TYPE_RESTORE_AUTHORIZATION`], before any
///    key is tried.
/// 2. A `ConsoleConfirmation` signature by a usable key of this namespace's
///    trust verifies, and that key may sign something new. **Both modes.**
///    Without it nothing attests who the requester was.
/// 3. The authentic bytes parse as the v2 document.
/// 4. [`policy::check_restore_authorization`]: format, the exact subject
///    INCLUDING UID, the plan hash recomputed from the referent's own bytes,
///    the policy name AND snapshot digest AND mode against the namespace's
///    current binding, a non-blank requester, and the window at `now`.
/// 5. `Governed` only: a second signature, by a usable `GovernedApproval` key
///    — absent is [`ApprovalRefusal::GovernedApprovalRequired`], the pending
///    state — and then separation of duties: that key's `principal.id` must
///    not be the requester's `<issuer>#<subject>`.
///
/// # What it never does
///
/// Accept a `GovernedApproval` signature in place of the console's, or the
/// console's in place of the approver's under `Governed`: the two usages are
/// checked by name, one each.
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
    // ---- 0 / 1 ------------------------------------------------------------
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

    // ---- 2. the console's confirmation, in BOTH modes ----------------------
    let confirmation_key_id = verify_signature_under(
        trust,
        KeyUsage::ConsoleConfirmation,
        &sidecar,
        approval_bytes,
        now,
    )?;

    // ---- 3 / 4. the authentic bytes, against the referent and the policy ---
    let doc = RestoreAuthorization::from_bytes(approval_bytes)?;
    policy::check_restore_authorization(&doc, expected, bound, now)?;
    let requester = doc.requester.principal_id();

    // ---- 5. the governed approver, and separation of duties ----------------
    let (matched_key_id, usage, approver) = match bound.mode {
        ApprovalMode::Ordinary => (
            confirmation_key_id.clone(),
            KeyUsage::ConsoleConfirmation,
            requester.clone(),
        ),
        ApprovalMode::Governed => {
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
    // THE VERDICT STOPS BEING TRUE at the earliest of the document's expiry
    // and the console key's notAfter; the authorising key's notAfter is the
    // window's own and `Verified::valid_until` takes the minimum with it.
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
/// THE REFUSAL NAMES THE THING TO EDIT. "no key on the TrustRoster 'default'
/// authorises this" is the wrong sentence for a namespace governed by
/// `org-default`, and sending an operator to an object that no longer decides
/// anything for their namespace is worse than saying nothing.
fn trust_object(trust: &ResolvedTrust) -> String {
    if trust.source.is_legacy() {
        format!("TrustRoster '{ROSTER_NAME}'")
    } else {
        format!("TrustPolicy '{}'", trust.source.name())
    }
}

/// One [`SigningRefusal`] as the refusal an operator reads.
///
/// # No wildcard arm
///
/// A seventh [`SigningRefusal`] must fail to compile here rather than reach the
/// cluster as a reason nobody chose — the same rule
/// [`ApprovalRefusal::reason`] follows.
///
/// `UntrustedSigner` and `KeyUsageMismatch` are UNREACHABLE from check 6: the
/// key came out of the resolved trust's own allowed-usage set for this
/// document kind, so it exists and it carries the selected usage. They map to
/// [`ApprovalRefusal::KeyIdNotInRoster`] because that is what they would mean
/// if a future refactor made them reachable — the key the signature named is
/// not one this namespace's trust offers for approvals.
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
            // `{:?}` on the pure enum, whose variant names ARE the CRD's
            // `revocationReason` values (`KeyCompromise`, `Superseded`,
            // `Unspecified`) — the same string an operator reads off the
            // policy they would go and edit.
            reason: format!(
                "{:?}",
                key.map_or(logweir_core::trust::RevocationReason::Unspecified, |k| k
                    .trust
                    .reason(),)
            ),
            effective_from: key
                .and_then(|k| k.trust.revocation_effective_from.or(k.trust.revoked_at))
                .map(|t| t.to_rfc3339()),
        },
    }
}

/// `a, b, c` — used only inside refusal messages.
fn join<'a>(ids: impl Iterator<Item = &'a str>) -> String {
    ids.collect::<Vec<_>>().join(", ")
}

/// What the API server said when the roster was asked for.
///
/// A THREE-STATE ANSWER AND NOT A `Result<TrustRoster, ApprovalRefusal>`,
/// because a 404 and a connection reset must not become the same thing. A 404
/// is a VERDICT — the install skipped step 1 — and is written to
/// `status.conditions` as [`ApprovalRefusal::RosterNotFound`]. A transport
/// error is not a verdict about anybody's approval and must requeue instead of
/// stamping a refusal onto an object that may well be fine.
#[derive(Debug)]
pub enum RosterLoad {
    /// The roster exists.
    ///
    /// BOXED. A `TrustRoster` is ~464 bytes and `NotFound` is empty, so an
    /// unboxed variant makes every `Ok(NotFound)` carry the larger one's
    /// footprint (`clippy::large_enum_variant`). `Box` keeps the two states
    /// named — which is the whole point of this type — at one allocation per
    /// reconcile.
    Found(Box<TrustRoster>),
    /// The API server answered 404 for `trustrosters/default`.
    NotFound,
}

/// Resolve the one cluster-scoped [`TrustRoster`] named [`ROSTER_NAME`].
///
/// THE ROSTER-LOADING PATH every consumer in chain O shares — Task 24's
/// evidence verification calls exactly this. The name comes from
/// [`ROSTER_NAME`] and from nowhere else: not from the `Approval`, not from a
/// flag, not from the environment.
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
/// NOT AN EIGHTH [`ApprovalRefusal`] VARIANT. That set is an interface Tasks
/// 20, 24 and 27 route on; these two are properties of the REFERENT, not
/// verdicts about the signature, and conflating them would put "your cluster
/// is missing an object" in the same vocabulary as "your approval does not
/// authorise this plan".
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
    /// REACHABLE IN TAG 1, FOR TWO KINDS AND FOR DIFFERENT REASONS.
    /// `Backup.spec` has no
    /// `planBytes` field (`crates/weirkeeper/src/crds/backup.rs`), and check 7
    /// recomputes the plan hash from the referent's own bytes. So an
    /// `Approval` whose `subjectRef.kind` is `Backup` is refused here, with a
    /// message that says which field is missing — rather than silently hashing
    /// something nobody signed, or degrading check 7 to a no-op for one kind.
    /// Tag 1's approval flow is the restore wizard (interface **I19**);
    /// `Backup` is in [`SubjectKind`] because the CRD's CEL seal admits it and
    /// check 8 must be able to tell the kinds apart.
    ///
    /// `RehearsalSchedule` reaches it for a different reason: D3 §4.3 binds a
    /// digest recomputed from that referent's own sealed spec, and the
    /// recomputation is the rehearsal worker's. Until it lands, an approval
    /// naming one is refused HERE rather than verified against bytes nobody
    /// hashed.
    ReferentHasNoPlanBytes {
        /// `Backup` or `RehearsalSchedule`.
        kind: String,
        /// The name `spec.subjectRef.name` gave.
        name: String,
    },
    /// This immutable Approval was already verified for an older incarnation
    /// of the same-named object.  Re-verification must not rebind it.
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
/// Returned so a test can assert over the VERDICT as well as over the route
/// table — the same reason [`evaluate`] is a pure function.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ApprovalOutcome {
    /// Every check passed.
    Verified(Verified),
    /// One of the five checks, or the missing roster, refused it.
    Refused(ApprovalRefusal),
    /// The referent could not supply the bytes check 7 hashes.
    Referent(ReferentProblem),
}

impl ApprovalOutcome {
    /// The `reason` this outcome writes into the `Verified` condition.
    #[must_use]
    pub const fn reason(&self) -> &'static str {
        match self {
            Self::Verified(_) => REASON_VERIFIED,
            Self::Refused(r) => r.reason(),
            Self::Referent(p) => p.reason(),
        }
    }

    /// Whether `status.verified` is `true`.
    #[must_use]
    pub const fn is_verified(&self) -> bool {
        matches!(self, Self::Verified(_))
    }

    /// **The instant this outcome stops being true on its own** — `None` for an
    /// outcome no clock can withdraw.
    ///
    /// ONLY A `Verified` HAS ONE, and that asymmetry is the point. A refusal
    /// can only become a pass through an EDIT — a roster installed, a key
    /// added, a referent created — and an edit wakes this controller through
    /// its watch or through the heartbeat, which is what the five-minute
    /// interval is for. A pass, on the other hand, becomes a refusal with
    /// nobody touching anything, and that is the direction that must never be
    /// late: `KeyNotYetValid` reading refused for a few minutes too long is a
    /// closed door left closed; `KeyIdExpired` reading verified for a few
    /// minutes too long is an expired key authorising a restore.
    #[must_use]
    pub fn valid_until(&self) -> Option<DateTime<Utc>> {
        match self {
            Self::Verified(v) => Some(v.valid_until()),
            Self::Refused(_) | Self::Referent(_) => None,
        }
    }

    /// The `message` this outcome writes into the `Verified` condition.
    #[must_use]
    pub fn message(&self) -> String {
        match self {
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
            Self::Verified(v) => {
                let matched = if v.document_expires_at.is_some() {
                    "subject digest"
                } else {
                    "plan hash"
                };
                format!(
                    "the DSSE signature over spec.approvalBytes verified under {} key {} \
                     (trustSource={}), and the recomputed {matched} matched",
                    v.authorization_usage.as_str(),
                    v.matched_key_id,
                    v.trust_source
                )
            }
            Self::Refused(r) => r.to_string(),
            Self::Referent(p) => p.to_string(),
        }
    }
}

/// Anything that made a reconcile impossible rather than negative.
///
/// TWO VARIANTS, AND NEITHER IS A VERDICT. Every refusal an operator could act
/// on is an [`ApprovalRefusal`] or a [`ReferentProblem`] and reaches the
/// cluster as a condition; this type is only for "the reconcile could not be
/// completed", which requeues and writes nothing.
///
/// HAND-WRITTEN `Display`/`Error` RATHER THAN A `thiserror` DERIVE, so this
/// task adds no manifest entry at all beyond the two it must
/// (`logweir-core`, `futures`). Global Constraint 38 closes the workspace
/// graph; `thiserror` would add no *package*, but twenty lines that already
/// exist in the standard library are not worth a dependency edge whose only
/// job is to write them.
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
    /// ITS OWN VARIANT, AND NOT `NoUid` CARRYING A MESSAGE ABOUT SOMETHING
    /// ELSE (review finding F7). `to_deterministic_json` always emits UTF-8, so
    /// this is unreachable — which is precisely why it must not be a silent
    /// default: an empty string here would make check 7 compare `sha256("")`
    /// against the signed `plan_hash`, and that comparison would be fail-closed
    /// by arithmetic luck rather than by construction, in an admission path.
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
/// SPLIT FROM THE PATCH ON PURPOSE. The decision needs the cluster (a roster
/// and a referent); the patch needs the decision. Keeping them apart is what
/// lets `a_missing_roster_names_itself` assert the decision AND assert that
/// the referent was never fetched, off one route table.
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

/// The v1 / v2 choice for a `Restore` referent, made on the NAMESPACE'S
/// BINDING and never on what the document claims to be (PLAT-19.2).
///
/// An unbound namespace accepts exactly today's v1 approval; a bound one
/// accepts exactly authorization document v2 naming its policy. The sidecar's
/// payload type is read only to choose which refusal names the mismatch —
/// every path to `Verified` still verifies a signature over those bytes.
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

/// [`decide`], under the installation's approval policies — PLAT-19.2.
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

    // THE NAMESPACE'S TRUST FIRST, and an absent or contested one
    // short-circuits everything: without a resolved key set there is no set of
    // keys any signature could be checked against, so fetching the referent
    // would be work performed to reach a conclusion already known.
    //
    // PLAT-19.1 REPLACES `load_roster` HERE. `trust::resolve` reads every
    // `TrustPolicy` and falls back to the synthesised `legacy-roster-v1`, so a
    // cluster that has never seen a policy resolves to exactly the key set
    // `TrustRoster/default` gave it — and `Unconfigured` is today's
    // `RosterNotFound`, the same refusal with the same message.
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

    let subject = &approval.spec.subject_ref;
    let referent_kind = subject.kind.as_str();
    let (plan_bytes, verified_subject_ref) = match subject.kind {
        SubjectKind::Restore => {
            let api: Api<Restore> = Api::namespaced(client.clone(), &namespace);
            match api.get(&subject.name).await {
                Ok(restore) => {
                    let uid = restore
                        .uid()
                        .ok_or_else(|| ReconcileError::NoUid(subject.name.clone()))?;
                    let current = VerifiedSubjectRef {
                        api_version: Restore::api_version(&()).to_string(),
                        kind: SubjectKind::Restore,
                        name: subject.name.clone(),
                        namespace: namespace.clone(),
                        uid,
                    };
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
                    }
                    (restore.spec.plan_bytes, Some(current))
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
        SubjectKind::Backup => {
            let api: Api<Backup> = Api::namespaced(client.clone(), &namespace);
            match api.get(&subject.name).await {
                // The object exists; the KIND has no planBytes. Both facts are
                // reported, in that order, so an operator is not told their
                // Backup is missing when it is not.
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
        // D3 W7 (PLAT-14.3) LANDS THE RECOMPUTATION THIS ARM WAS WAITING FOR.
        //
        // `ReferentHasNoPlanBytes` no longer applies to this kind (D3 §4.3's
        // own sentence). A `RehearsalSchedule` has no `planBytes` and does not
        // need any: what the standing authorization binds is a digest over the
        // referent's OWN SEALED SPEC MINUS `suspend`
        // (`crate::rehearsal::template_bytes`), recomputed here, every pass,
        // from the object the API server just returned. Checks 7 and 8 are
        // unchanged code — check 7 compares `sha256_prefixed(those bytes)` with
        // the `plan_hash` inside the signed document, and check 8 compares the
        // signed `subject_kind` with `RehearsalSchedule` — which is what D3
        // §4.3 means by "the same rule applied to a different referent".
        //
        // The spec is sealed except `suspend`, so the digest cannot drift under
        // a running authorization; `suspend` is excluded so that PAUSING an
        // unattended rehearsal does not invalidate the document authorising it,
        // which would make the one control an operator reaches for in an
        // incident the control that breaks the schedule.
        SubjectKind::RehearsalSchedule => {
            use crate::crds::rehearsal_schedule::RehearsalSchedule;
            let api: Api<RehearsalSchedule> = Api::namespaced(client.clone(), &namespace);
            match api.get(&subject.name).await {
                Ok(schedule) => {
                    let uid = schedule
                        .uid()
                        .ok_or_else(|| ReconcileError::NoUid(subject.name.clone()))?;
                    let current = VerifiedSubjectRef {
                        api_version: RehearsalSchedule::api_version(&()).to_string(),
                        kind: SubjectKind::RehearsalSchedule,
                        name: subject.name.clone(),
                        namespace: namespace.clone(),
                        uid,
                    };
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
                    }
                    let bytes = crate::rehearsal::template_bytes(&schedule.spec).map_err(|e| {
                        // Unreachable for this type, which carries no float.
                        // Reported rather than panicking: an admission path is
                        // not a place to abort.
                        ReconcileError::Canonicalization(format!(
                            "the RehearsalSchedule {} spec could not be canonicalised: {e}",
                            subject.name
                        ))
                    })?;
                    // PROPAGATED, NEVER DEFAULTED. `to_deterministic_json`
                    // always emits UTF-8, so this cannot fire — and a silent
                    // `""` would make check 7 compare `sha256("")` against the
                    // signed `plan_hash`, which is fail-closed by arithmetic
                    // luck rather than by construction.
                    let text = String::from_utf8(bytes).map_err(|e| {
                        ReconcileError::Canonicalization(format!(
                            "the RehearsalSchedule {}'s canonical spec bytes are not UTF-8: {e}",
                            subject.name
                        ))
                    })?;
                    (text, Some(current))
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
    };

    // `.as_bytes()`, WITH NO DECODE STEP. Interface **I18**: `approvalBytes`
    // and `sidecarBytes` are the UTF-8 document text, verbatim, never base64.
    let want = sha256_prefixed(plan_bytes.as_bytes());
    let evaluated = if subject.kind == SubjectKind::RehearsalSchedule {
        // Constructed in the schedule arm above. Keeping the option in the
        // shared tuple makes the Restore path byte-for-byte the same shape.
        let referent = verified_subject_ref
            .as_ref()
            .ok_or_else(|| ReconcileError::NoUid(subject.name.clone()))?;
        evaluate_standing(
            approval.spec.approval_bytes.as_bytes(),
            approval.spec.sidecar_bytes.as_bytes(),
            &trust,
            now,
            referent,
            &want,
        )
    } else if subject.kind == SubjectKind::Restore {
        // PLAT-19.2: THE ONE KIND AN APPROVAL POLICY GOVERNS. The standing
        // document above keeps its own GovernedApproval-only format (its
        // schedule-side admission is `rehearsal_schedule`'s), and a `Backup`
        // referent was refused before this point.
        evaluate_restore_referent(
            approval,
            &trust,
            now,
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
            // ---- THE UNSIGNED CLAIM MUST AGREE WITH THE SIGNED ONE -----
            //
            // `spec.planHash` is a plain CRD field beside the documents: a
            // create form fills it in so an operator can compare it, and
            // `kubectl get approval -o yaml`, `kubectl get approval` and
            // the UI all SHOW it. Checks 1-8 never read it -- check 7
            // recomputes the hash and compares it with the one INSIDE the
            // signed bytes, which is what authorisation must rest on. So
            // without this an `Approval` could be `Verified=True` while
            // displaying a plan hash that is not the plan it authorises,
            // and the one thing this field exists for -- letting a reader
            // compare -- would be the one thing it could not be trusted
            // for. The CRD has always said a wrong `planHash` is a refusal
            // (`crds/approval.rs`); this is where that becomes true.
            //
            // THE VERDICT IS CHECK 7's, because the FACT is check 7's: this
            // approval names a plan the referent does not carry. The
            // message names both hashes and says both places must agree.
            if approval.spec.plan_hash != want {
                return Ok(ApprovalOutcome::Refused(
                    if subject.kind == SubjectKind::RehearsalSchedule {
                        ApprovalRefusal::TemplateDigestMismatch {
                            got: approval.spec.plan_hash.clone(),
                            want,
                        }
                    } else {
                        ApprovalRefusal::PlanHashMismatch {
                            got: approval.spec.plan_hash.clone(),
                            want,
                        }
                    },
                ));
            }
            verified.verified_subject_ref = verified_subject_ref;
            ApprovalOutcome::Verified(verified)
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
        // A refused approval reports NO approver, NO key id AND NO KEY
        // WINDOW. An approver name lifted out of bytes whose signature did
        // not verify is an attacker-controlled string on a status field a
        // UI renders; a window left over from a verdict that has since been
        // withdrawn is worse, because a reader compares a deadline against
        // it and finds it open. No key matched, so there is no window —
        // which the preflight reads as `unknown`, never as valid.
        //
        // Subject provenance is different: once established it is a replay
        // fence and must survive every later failure, including referent
        // deletion.
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

/// The `status` keys this reconciler computes as `Option` and must be able to
/// CLEAR — defect `STATUS-PATCH-NO-RV`'s neighbour, and the reason
/// [`status_patch_body`] exists.
///
/// # A merge patch that omits a key leaves it
///
/// [`status_for`] already says a refused `Approval` reports no approver, no key
/// id, no self-attestation label and no key window. It said so by setting each
/// to `None`, and every one of them is `skip_serializing_if = "Option::is_none"`
/// — so the patch body simply had no such key and RFC 7386 left the old value
/// on the object. An `Approval` verified at 09:00 and refused at 09:05 kept
/// `status.approver`, `status.matchedKeyId` and (from this task on) the
/// approver key's window beside a `Verified=False` condition.
///
/// The window is what makes it urgent rather than untidy. The restore
/// preflight compares a restore's deadline against it and caps its re-check at
/// its `notAfter`; a window stranded by a withdrawn verdict is a reader finding
/// an OPEN window for a key that authorises nothing, which is precisely the
/// "treat absent as valid" failure the whole field exists to avoid. The other
/// three are swept with it because they are the same bug in the same body.
///
/// `verifiedSubjectRef` is DELIBERATELY NOT HERE. It is a replay fence, not a
/// verdict: once established it must survive every later failure, and
/// [`status_for`] carries it forward for exactly that reason.
pub const CLEARABLE_STATUS_FIELDS: [&str; 6] = [
    "matchedKeyId",
    "approver",
    "ticket",
    "selfAttestedRisk",
    "approverKeyWindow",
    // PLAT-19.2: a policy and requester left behind by a withdrawn verdict is
    // the same "absent read as valid" shape the window is.
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
/// NOTHING ELSE IS TOUCHED. Not the `spec` — sealed by CEL, and an approval
/// whose bytes a controller could edit is not an approval — and never a
/// `DELETE`: a refused `Approval` STAYS in the cluster as the audit trail of a
/// rejected attempt.
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
    let name = approval.name_any();
    let namespace = approval
        .namespace()
        .ok_or_else(|| ReconcileError::NoNamespace(name.clone()))?;
    let outcome = decide_with_policy(approval, client, policies).await?;
    let status = status_for(approval, &outcome, Utc::now());

    let api: Api<Approval> = Api::namespaced(client.clone(), &namespace);
    let patch = status_patch_body(&status);
    // NO WRITE WHEN NOTHING CHANGED — plan erratum E11(d), review finding H-2.
    // An `Approval` is the most steady object this controller holds: its spec
    // is sealed by CEL and its verdict is a function of that spec, the roster
    // and the referent. Before this guard the unconditional
    // `lastTransitionTime: Some(now)` in [`status_for`] made every pass a
    // change, its own status patch woke the watch, and one steady `Approval`
    // ran 7,114 reconciles in 90.4 s. The outcome is still logged below.
    //
    // AND UNDER SEAM S7's PRECONDITION — defect STATUS-PATCH-NO-RV's sweep.
    // This write was unconditional while the chart's README said every status
    // write in this crate is preconditioned; a verdict computed from a stale
    // watch-cache copy is exactly what `min(10 m, notAfter)` re-checks make
    // likely here, because two passes over one `Approval` can overlap.
    crate::conditions::patch_status_preconditioned(
        &api,
        "Approval",
        &name,
        &StatusVersion::observed(approval.meta()),
        approval
            .status
            .as_ref()
            .and_then(|s| serde_json::to_value(s).ok())
            .as_ref(),
        patch,
    )
    .await?;

    if outcome.is_verified() {
        info!(
            approval = %name,
            namespace = %namespace,
            reason = outcome.reason(),
            "approval verified"
        );
    } else {
        // WARN AND NOT ERROR: a refused approval is a correct, expected
        // outcome of this controller doing its job, and an operator whose log
        // filter is `error` should not be paged for one.
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
    policies: Arc<ApprovalPolicySet>,
) -> Result<Action, ReconcileError> {
    let outcome = reconcile_approval_with_policy(&approval, &ctx.client, &policies).await?;
    // NOT `Action::await_change()`. `TrustRoster` is a different kind and this
    // controller does not watch it, so a roster that arrives after the
    // approval would otherwise never be noticed: an `Approval` refused with
    // `RosterNotFound` at 09:00 would still say so at 17:00 with the roster
    // installed at 09:05. Five minutes is the interval at which "install the
    // roster, then look again" is self-healing.
    //
    // …AND FIVE MINUTES IS THE WRONG ANSWER IN THE OTHER DIRECTION (defect
    // `TRUST-EXPIRY-LAG`). "Install the roster, then look again" may wait; "the
    // approver key expired thirty seconds ago" may not, and the heartbeat made
    // an expired key read `Verified=True` for 2 m 35 s on lab-refresh-4. A
    // verified outcome therefore requeues at its own `notAfter`, and everything
    // else keeps the heartbeat — see `ApprovalOutcome::valid_until` for why the
    // asymmetry is the safe one.
    Ok(Action::requeue(super::trust_policy::requeue_before(
        outcome.valid_until(),
        Utc::now(),
    )))
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

/// The `Approval`s a `TrustPolicy` event should enqueue — the same re-trust
/// trigger `controllers::backup` and `controllers::restore` carry (D3 W10).
///
/// # Why this controller needs one at all
///
/// Its verdict is a function of the namespace's RESOLVED TRUST, and this
/// reconciler does not watch `TrustPolicy`: before this, a policy edit reached
/// an `Approval` only at the five-minute heartbeat (or sooner, at the matched
/// key's `notAfter`, which defect `TRUST-EXPIRY-LAG` armed). Five minutes is
/// the right pace for "install the roster, then look again"; it is the wrong
/// pace for "this key was revoked thirty seconds ago", and it is the wrong pace
/// for the WINDOW this task publishes — a narrowed `notAfter` that an operator
/// can see on the `TrustPolicy` but not yet on the `Approval` is exactly the
/// lag the published window exists to remove.
///
/// It costs nothing to fix: `objects` is this controller's OWN store, so the
/// mapping is zero API calls, and it over-approximates on the safe side — see
/// [`crate::verification::targets_in_scope`].
fn policy_targets(
    objects: &reflector::Store<Approval>,
    scopes: &crate::trust::PolicyScopeMemory,
    policy: &crate::crds::trust_policy::TrustPolicy,
) -> Vec<ObjectRef<Approval>> {
    // THE UNION OF BEFORE AND AFTER. A `watches` mapper is handed only the NEW
    // object, so an edit that NARROWS — a namespace removed, `default` cleared
    // — would otherwise enqueue nothing in the namespace it just stopped
    // governing, which is the one edit that certainly changed that namespace's
    // resolution. See `trust::PolicyScopeMemory`.
    let scope = scopes.observe(policy);
    crate::verification::targets_in_scope(objects.state(), &scope)
}

/// Run the `Approval` controller until the process ends.
///
/// `Api::all`: this controller reconciles approvals in every namespace, which
/// is what a cluster-scoped install means (Global Constraint 30 — one
/// controller per cluster, no fleet).
///
/// `policies` is the installation's approval-policy document, read once by
/// `main` (PLAT-19.2); an empty set is every namespace on `legacy-governed-v1`.
pub fn controller(
    client: kube::Client,
    policies: Arc<ApprovalPolicySet>,
) -> impl std::future::Future<Output = ()> + Send {
    // D0 STAGE 5: ONE WATCH PER WATCHED NAMESPACE. `crate::scope` is the whole
    // cluster unless `LOGWEIR_WATCH_NAMESPACES` names the execution
    // namespaces, and then this reconciler runs once per namespace with an
    // `Api::namespaced` watch — the only shape the scoped chart's RoleBindings
    // permit. Every copy shares the one policy set (PLAT-19.2).
    crate::scope::run_everywhere(move |namespace| {
        controller_in(client.clone(), namespace, Arc::clone(&policies))
    })
}

/// One watch of [`controller`], over `namespace` (`None` is the whole
/// cluster, the behaviour before D0 stage 5).
fn controller_in(
    client: kube::Client,
    namespace: Option<String>,
    policies: Arc<ApprovalPolicySet>,
) -> impl std::future::Future<Output = ()> + Send {
    let api: Api<Approval> = crate::scope::api(&client, namespace.as_deref());
    let policy_api: Api<crate::crds::trust_policy::TrustPolicy> = Api::all(client.clone());
    // ONE memory of what each policy bound last, owned by the mapper — the
    // same object `controllers::backup` and `controllers::restore` give theirs.
    let scopes = Arc::new(crate::trust::PolicyScopeMemory::default());
    // Task 19: this reconciler holds NO archive handle. Spelled out
    // rather than defaulted, so the one context field that is a
    // capability is visible at every construction site.
    let ctx = Arc::new(Context {
        client,
        archive: None,
        // Task 33 (and Task 37's pull policy beside it): this reconciler
        // creates no runner Job, so the image and policy this process was
        // handed would be carried and never read. The default here is
        // "unused", never "no override is configured" — see
        // `super::Context::runner_image`.
        runner_image: crate::job::RunnerImage::default(),
    });
    async move {
        let controller = Controller::new(api, watcher::Config::default());
        let objects = controller.store();
        controller
            // THE RE-TRUST TRIGGER. A `TrustPolicy` event maps to the
            // `Approval`s this controller already holds in the namespaces that
            // policy could govern — see [`policy_targets`]. No reflector of its
            // own: `decide` resolves trust live on every pass, so the trigger
            // only has to say WHEN.
            .watches(policy_api, watcher::Config::default(), move |policy| {
                policy_targets(&objects, &scopes, &policy)
            })
            .run(
                move |approval, ctx| reconcile(approval, ctx, Arc::clone(&policies)),
                error_policy,
                ctx,
            )
            // Every item is already logged by `reconcile_approval` or by
            // `error_policy`; the stream exists to be DRIVEN, and a second log
            // line per event would double every one of them.
            .for_each(|_| std::future::ready(()))
            .await;
    }
}
