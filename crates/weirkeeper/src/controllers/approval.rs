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
use kube::api::{Patch, PatchParams};
use kube::runtime::controller::Action;
use kube::runtime::{watcher, Controller};
use kube::{Api, Resource, ResourceExt};
use logweir_core::ids::sha256_prefixed;
use logweir_core::trust::{KeyUsage, SigningRefusal};
use logweir_verify::{verify_detached, Sidecar, VerifyingKey};
use serde::Deserialize;
use serde_json::json;
use tracing::{debug, info, warn};

use super::Context;
use crate::conditions::{current_condition, merge_condition, status_unchanged};
use crate::crds::approval::{Approval, ApprovalStatus, SubjectKind, VerifiedSubjectRef};
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
/// SEVEN VARIANTS, AND THE SET IS CLOSED. Tasks 20, 24 and 27 read
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
            Self::RosterNotFound => write!(f, "{ROSTER_NOT_FOUND_MESSAGE}"),
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
    /// Filled by [`decide`] with the API object whose bytes were checked.
    /// Pure [`evaluate`] callers have no Kubernetes referent and leave it
    /// absent; only a reconcile outcome is written to status.
    pub verified_subject_ref: Option<VerifiedSubjectRef>,
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
    if let Some(message) = trust.blocked_for(KeyUsage::GovernedApproval) {
        return Err(ApprovalRefusal::SignatureInvalid(message.to_string()));
    }
    let declared: Vec<&ResolvedKey> = trust
        .keys
        .iter()
        .filter(|k| k.trust.has_usage(KeyUsage::GovernedApproval))
        .collect();

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
                "the sidecar names [{}] and {}'s GovernedApproval keys are [{}]",
                join(sidecar.signatures.iter().map(|s| s.keyid.as_str())),
                trust_object(trust),
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
    if let Err(refusal) = trust.may_sign_new_for(&matched_key_id, KeyUsage::GovernedApproval, now) {
        return Err(signing_refusal(trust, &matched_key_id, refusal));
    }

    // ---- the bytes are authentic; now read what they say ------------------
    let doc: ApprovalDocument = serde_json::from_slice(approval_bytes).map_err(|e| {
        ApprovalRefusal::SignatureInvalid(format!(
            "the signature over spec.approvalBytes verified under key id {matched_key_id}, but \
             those bytes are not an approval document: {e}"
        ))
    })?;

    // ---- 7. the plan hash, RECOMPUTED from the referent's own bytes -------
    //
    // Never read from the referent's `status.planHash` — a status is written
    // by a controller and is not part of anything anyone signed, so a status
    // field could rescue an approval that binds a different plan.
    // `approval_recomputes_the_plan_hash_from_the_referent_bytes`'s second arm
    // sets that status to the CORRECT value and asserts it does not.
    let want = sha256_prefixed(referent_plan_bytes);
    if doc.plan_hash != want {
        return Err(ApprovalRefusal::PlanHashMismatch {
            got: doc.plan_hash,
            want,
        });
    }

    // ---- 8. the subject kind, from inside the signed bytes ----------------
    if doc.subject_kind != referent_kind {
        return Err(ApprovalRefusal::SubjectKindMismatch {
            approval_says: doc.subject_kind,
            referent_is: referent_kind.to_string(),
        });
    }

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

    Ok(Verified {
        matched_key_id,
        approver: doc.approver,
        ticket: doc.ticket,
        self_attested_risk,
        trust_source: trust.source.name().to_string(),
        verified_subject_ref: None,
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
/// key came out of the resolved trust's own `GovernedApproval` list, so it
/// exists and it carries the usage. They map to
/// [`ApprovalRefusal::KeyIdNotInRoster`] because that is what they would mean
/// if a future refactor made them reachable — the key the signature named is
/// not one this namespace's trust offers for approvals.
fn signing_refusal(
    trust: &ResolvedTrust,
    key_id: &str,
    refusal: SigningRefusal,
) -> ApprovalRefusal {
    let key = trust.key(key_id);
    match refusal {
        SigningRefusal::UntrustedSigner | SigningRefusal::KeyUsageMismatch => {
            ApprovalRefusal::KeyIdNotInRoster {
                key_id: format!(
                    "{} offers no GovernedApproval key with id {key_id}",
                    trust_object(trust)
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

    /// The `message` this outcome writes into the `Verified` condition.
    #[must_use]
    pub fn message(&self) -> String {
        match self {
            Self::Verified(v) => format!(
                "the DSSE signature over spec.approvalBytes verified under GovernedApproval key \
                 {} (trustSource={}), and the recomputed plan hash matched",
                v.matched_key_id, v.trust_source
            ),
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
    Ok(
        match evaluate(
            approval.spec.approval_bytes.as_bytes(),
            approval.spec.sidecar_bytes.as_bytes(),
            &trust,
            Utc::now(),
            referent_kind,
            plan_bytes.as_bytes(),
        ) {
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
                let want = sha256_prefixed(plan_bytes.as_bytes());
                if approval.spec.plan_hash != want {
                    return Ok(ApprovalOutcome::Refused(
                        ApprovalRefusal::PlanHashMismatch {
                            got: approval.spec.plan_hash.clone(),
                            want,
                        },
                    ));
                }
                verified.verified_subject_ref = verified_subject_ref;
                ApprovalOutcome::Verified(verified)
            }
            Err(refusal) => ApprovalOutcome::Refused(refusal),
        },
    )
}

/// The `/status` body one outcome produces.
#[must_use]
pub fn status_for(
    approval: &Approval,
    outcome: &ApprovalOutcome,
    now: DateTime<Utc>,
) -> ApprovalStatus {
    let verified = outcome.is_verified();
    let (matched_key_id, approver, ticket, self_attested_risk, verified_subject_ref) = match outcome
    {
        ApprovalOutcome::Verified(v) => (
            Some(v.matched_key_id.clone()),
            Some(v.approver.clone()),
            Some(v.ticket.clone()),
            Some(v.self_attested_risk),
            v.verified_subject_ref.clone(),
        ),
        // A refused approval reports NO approver and NO key id. An approver
        // name lifted out of bytes whose signature did not verify is an
        // attacker-controlled string on a status field a UI renders. Subject
        // provenance is different: once established it is a replay fence and
        // must survive every later failure, including referent deletion.
        _ => (
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
    let name = approval.name_any();
    let namespace = approval
        .namespace()
        .ok_or_else(|| ReconcileError::NoNamespace(name.clone()))?;
    let outcome = decide(approval, client).await?;
    let status = status_for(approval, &outcome, Utc::now());

    let api: Api<Approval> = Api::namespaced(client.clone(), &namespace);
    let patch = json!({ "status": status });
    // NO WRITE WHEN NOTHING CHANGED — plan erratum E11(d), review finding H-2.
    // An `Approval` is the most steady object this controller holds: its spec
    // is sealed by CEL and its verdict is a function of that spec, the roster
    // and the referent. Before this guard the unconditional
    // `lastTransitionTime: Some(now)` in [`status_for`] made every pass a
    // change, its own status patch woke the watch, and one steady `Approval`
    // ran 7,114 reconciles in 90.4 s. The outcome is still logged below.
    if status_unchanged(
        approval
            .status
            .as_ref()
            .and_then(|s| serde_json::to_value(s).ok())
            .as_ref(),
        &patch,
    ) {
        debug!(
            approval = %name,
            namespace = %namespace,
            "the computed status equals the one on the object; no patch is sent"
        );
    } else {
        api.patch_status(&name, &PatchParams::default(), &Patch::Merge(patch))
            .await?;
    }

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
async fn reconcile(approval: Arc<Approval>, ctx: Arc<Context>) -> Result<Action, ReconcileError> {
    reconcile_approval(&approval, &ctx.client).await?;
    // NOT `Action::await_change()`. `TrustRoster` is a different kind and this
    // controller does not watch it, so a roster that arrives after the
    // approval would otherwise never be noticed: an `Approval` refused with
    // `RosterNotFound` at 09:00 would still say so at 17:00 with the roster
    // installed at 09:05. Five minutes is the interval at which "install the
    // roster, then look again" is self-healing.
    Ok(Action::requeue(std::time::Duration::from_secs(300)))
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

/// Run the `Approval` controller until the process ends.
///
/// `Api::all`: this controller reconciles approvals in every namespace, which
/// is what a cluster-scoped install means (Global Constraint 30 — one
/// controller per cluster, no fleet).
pub fn controller(client: kube::Client) -> impl std::future::Future<Output = ()> + Send {
    let api: Api<Approval> = Api::all(client.clone());
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
        Controller::new(api, watcher::Config::default())
            .run(reconcile, error_policy, ctx)
            // Every item is already logged by `reconcile_approval` or by
            // `error_policy`; the stream exists to be DRIVEN, and a second log
            // line per event would double every one of them.
            .for_each(|_| std::future::ready(()))
            .await;
    }
}
