//! The controller's own verification of the evidence the UI renders, and the
//! two green-badge rules, one per kind (interface **I21**).
//!
//! # A verdict only when a verdict happened
//!
//! [`VerificationVerdict::NotAttempted`] is distinct from
//! [`VerificationVerdict::Invalid`]: `Invalid` is a claim about the document,
//! and an adopter who gives the controller no read-only bucket credential has
//! produced no answer at all. The honest badge for that is the literal word
//! `unverified` beside the printed `logweir drill verify` command.
//!
//! # The credential is a separate Secret and a different principal
//!
//! `logweir-evidence-ro` is projected into the controller's own pod as
//! environment (`charts/logweir/templates/deployment.yaml`, `optional: true`),
//! so a cluster without it starts and every verification reads
//! `NotAttempted`. It cannot write or delete in any bucket: the controller's
//! one archive handle is built in `main` through the read-only constructor
//! (guard **G-RET**), and no control-plane component holds a delete
//! capability against object storage.
//!
//! The controller reads that credential from its own environment, never
//! through the Kubernetes API: the `weirkeeper` ClusterRole grants no verb on
//! `secrets`, and `tests/linkage.rs::the_controller_never_reads_a_secret`
//! keeps it that way.
//!
//! # Two rules, one per kind
//!
//! "Passed" is a different field on each kind:
//!
//! * **`Backup` is green ⟺ `verification.result == Valid` AND
//!   `status.exitCode == 0`**: [`backup_badge`].
//! * **`Restore` is green ⟺ `verification.result == Valid` AND
//!   `status.outcome == pass`**: [`restore_badge`]. A recorded `exitCode`
//!   other than `0` is never green: an exit-2 run publishes its signed
//!   failure, and the exit code stays authoritative for success.
//!
//! There is no `outcome` on the `Backup` path at all (`BackupStatus`,
//! [`VerificationResult`] and `BackupReceipt` carry none), so a single rule
//! reading `status.outcome` would render every `Backup` ungreen, and
//! `a_backup_is_green_only_on_valid_and_exit_code_zero`'s first fixture fails.
//!
//! Either badge is labelled **"verified by weirkeeper at `<verifiedAt>`
//! against key `<matchedKeyId>`"**, never "verified in your browser"; anything
//! else is the literal word [`UNVERIFIED`], never `pass`.
//!
//! # The roster must carry key material
//!
//! [`verify_evidence`]'s step 4 resolves a signing key from the namespace's
//! resolved trust: a `TrustPolicy`'s `spec.keys[].spkiPem`, or the synthesised
//! `legacy-roster-v1`'s translation of `TrustRoster.spec.signingKeys[].spkiPem`.
//! A list of key ids alone would leave nothing to verify against, and
//! `status.evidence.verification.result` could never be `Valid`.
//!
//! # A signature is not a verdict
//!
//! `logweir-verify` answers "did this key make these bytes". The badge's
//! second question, *may this installation still trust the key that made
//! them*, is [`logweir_core::trust::decide`]'s, asked of the key the signature
//! selected against the namespace's resolved trust ([`crate::trust::resolve`]).
//!
//! So [`VerificationVerdict::Untrusted`] means the bytes are authentic and the
//! signer is one this installation will not accept. It is deliberately not
//! `Invalid`: calling an archive corrupt when its key was revoked sends an
//! operator to re-run a backup instead of to their trust policy. Every reader
//! that predates it treats anything that is not `Valid` as unverified
//! (`ui/pages/backups.js`), so it fails closed on old surfaces.
//!
//! Three inputs decide it, each a different kind of fact:
//!
//! * the **key**, as the resolved policy carries it (state, window, usages);
//! * the document's own **claim** about when it was signed
//!   ([`logweir_core::trust::claimed_signing_time`]), attacker-controlled for
//!   a compromised key, which is why the compromise rows never read it;
//! * the **independent observation**: this controller's own
//!   `status.evidence.verification.verifiedAt` from an earlier reconcile, and
//!   nothing else.
//!
//! The observation lives on the status, so
//! [`VerificationResult::to_status_value`] performs the trust decision rather
//! than [`verify_evidence`], and a trust-only change does not move
//! `verifiedAt` (see that method).
//!
//! # `Store` is blocking, so nothing here is `async` (interface I13)
//!
//! Every [`Store`] method drives its own current-thread runtime, and `kube`
//! drives every reconciler on a runtime, so a direct call compiles and panics
//! with *Cannot start a runtime from within a runtime*. [`verify_evidence`] is
//! a plain synchronous function, the body of a `tokio::task::spawn_blocking`
//! closure, over the shared `Arc<Store>` built once at controller start.
//! `tests/retention.rs::no_store_call_is_made_outside_spawn_blocking` names
//! this file and fails if a `Store` call appears outside such a closure.
//!
//! # The signing-oracle residual
//!
//! Job CRUD in a namespace holding `logweir-signing-key` is equivalent to
//! holding that key, so `weirkeeper` is a signing oracle. The hardened layout
//! (the signing Secret in a namespace where `weirkeeper` has no Job CRUD) is
//! documented, not mandated, because mandating it would split the
//! single-namespace install.
//! this file in its `I13_FILES` and fails if a `Store` call appears in a
//! function body outside such a closure.
//!
//! # The signing-oracle residual, stated rather than implied
//!
//! Global Constraint 27, O1/O0 default **(a)**, accepted: Job CRUD in a
//! namespace holding `logweir-signing-key` is equivalent to holding that key,
//! so `weirkeeper` is a signing oracle. The hardened layout — the signing
//! Secret in a namespace where `weirkeeper` has no Job CRUD — is **documented,
//! not mandated** (`docs/kubernetes.md` §15), because mandating it would split
//! the single-namespace install the "a stranger applies one file" decision
//! rests on.

use std::fmt;

use chrono::{DateTime, SecondsFormat, Utc};
use logweir_core::ids::sha256_prefixed;
use logweir_core::trust::{
    decide, ClaimAbsence, EvidenceClaim, IndependentObservation, KeyUsage, TrustBasis, TrustResult,
    TrustedKey, UntrustReason, Verdict,
};
use logweir_store::{caps, Store, StoreError};
use logweir_verify::{verify_detached, Sidecar, VerifyingKey};
use serde_json::{json, Value};

use crate::conditions::{
    current_condition, merge_condition, CONDITION_VERIFIED, REASON_COMPLETE_NOT_COVERED,
    REASON_EXIT_CODE_NOT_ZERO, REASON_OUTCOME_NOT_PASS, REASON_VERIFICATION_INVALID,
    REASON_VERIFICATION_NOT_ATTEMPTED, REASON_VERIFICATION_UNTRUSTED, REASON_VERIFIED,
};
use crate::crds::Condition;
use crate::trust::{Resolution, ResolvedTrust, TrustSource};

/// The `detail` on a `NotAttempted` produced because the controller holds no
/// evidence credential at all.
///
/// A sentence that names the way out: an adopter who gives the control plane
/// no bucket access is running with verification display off, and the CLI
/// command that answers the same question is printed here.
pub const NO_CREDENTIAL_DETAIL: &str =
    "no evidence credential is configured; run the printed logweir drill verify command instead";

/// The `detail` on a `NotAttempted` produced by an empty
/// `TrustRoster.spec.signingKeys`.
///
/// Verbatim: `an_empty_signing_key_list_names_itself` asserts it. A roster
/// with no signing key material is the one configuration under which no
/// evidence can ever verify, and a silent `Invalid` would blame the documents.
pub const NO_SIGNING_KEYS_DETAIL: &str = "the TrustRoster lists no signing key material; add the \
                                          runner's public key to spec.signingKeys";

/// The `detail` on a `NotAttempted` produced by a bound `TrustPolicy` that
/// carries no `EvidenceSigning` key at all.
///
/// Distinct from [`NO_SIGNING_KEYS_DETAIL`]: a namespace governed by a
/// `TrustPolicy` has no `spec.signingKeys` to add anything to, and the roster
/// no longer decides anything for it.
pub const NO_POLICY_KEYS_DETAIL: &str =
    "the TrustPolicy bound to this namespace lists no key with usage EvidenceSigning; add the \
     runner's public key to spec.keys";

/// The `detail` on a `NotAttempted` produced because two `TrustPolicy` objects
/// claim this namespace. It carries the machine token
/// [`crate::trust::REASON_TRUST_POLICY_CONFLICT`], the word an operator greps
/// on the policies' `status.conflicts` and on a refused `Approval`.
#[must_use]
pub fn trust_policy_conflict_detail(namespace: &str, policies: &[String]) -> String {
    format!(
        "{}: the namespace {namespace} is claimed by more than one TrustPolicy ({}), so it \
         resolves to no trust at all and nothing was verified; remove the namespace from all but \
         one policy",
        crate::trust::REASON_TRUST_POLICY_CONFLICT,
        policies.join(", ")
    )
}

/// The word every badge that is not green renders.
///
/// Never `pass`, and never "verified in your browser": `pass` is a
/// `Restore`'s own `outcome` value, and reusing it for "we did not check"
/// shows the happy word over an unchecked document.
pub const UNVERIFIED: &str = "unverified";

/// What the controller made of one piece of signed evidence.
///
/// `Valid` and `Invalid` are claims about the document; `NotAttempted` is a
/// claim about the controller. See this module's header.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum VerificationVerdict {
    /// The bytes matched the recorded digest and a roster signing key verified
    /// the detached DSSE sidecar over them.
    Valid,
    /// A claim about the document: the digest did not match, or no roster
    /// signing key verified the sidecar. **Never** produced by a storage
    /// failure.
    Invalid,
    /// No verdict was reached, and the `detail` says why: no evidence
    /// credential, an unreadable or absent object, a resolved trust with no
    /// signing key material, or a namespace two `TrustPolicy` objects contest.
    NotAttempted,
    /// A claim about the signer: the bytes are authentic (a key made this
    /// signature over exactly these bytes) and the key is one this
    /// installation will not accept: unknown to the resolved policy, revoked,
    /// carrying the wrong usage, or used outside its trusted window.
    ///
    /// Never `Invalid`: `Invalid` says the document is not what it claims to
    /// be; `Untrusted` says it is exactly that and that is the problem. An
    /// operator acts on their trust policy for one and their archive for the
    /// other.
    Untrusted,
    /// Not a verdict yet: an evidence-fetch check Job is reading this run's
    /// evidence with the destination's `evidenceRead` grant (or waiting for a
    /// slot), and the controller will verify what it relays. Written with no
    /// `matchedKeyId`, never green, and replaced by one of the four values
    /// above. Like `NotAttempted`, a claim about the controller.
    Pending,
}

impl VerificationVerdict {
    /// The wire spelling, as it lands on `status.evidence.verification.result`.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Valid => "Valid",
            Self::Invalid => "Invalid",
            Self::NotAttempted => "NotAttempted",
            Self::Untrusted => "Untrusted",
            Self::Pending => "Pending",
        }
    }
}

impl fmt::Display for VerificationVerdict {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Everything a trust verdict needs about one verified signature, carried from
/// the moment the signature matched to the moment the status is written.
///
/// The key travels as [`TrustedKey`], not a key id, so the answer does not
/// depend on a second lookup: re-resolving at projection time could describe
/// a key the signature never matched if the policy changed between the reads.
/// The lifecycle facts are small, owned and carry no PEM.
///
/// `key` is `None` for a signature that verified under a key the resolved
/// policy does not carry: impossible on the fresh path, but ordinary on the
/// re-trust path ([`retrust`]), where the stored `matchedKeyId` is looked up
/// in a policy that has since changed.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TrustProjection {
    /// The matched key's lifecycle, as the resolved policy carries it.
    pub key: Option<TrustedKey>,
    /// What the DOCUMENT says about when it was signed.
    pub claim: EvidenceClaim,
    /// Which policy answered, with the revision that answered.
    pub policy: crate::crds::PolicyRef,
    /// The `TrustPolicy` objects whose `KeyCompromise` revocation reached the
    /// matched key although the answering policy does not record one
    /// ([`crate::trust::ResolvedKey::compromise_inherited_from`]), so the
    /// `detail` names the object that records the compromise. Empty
    /// otherwise.
    pub compromise_recorded_by: Vec<String>,
}

impl TrustProjection {
    /// The verdict this projection reaches, given one observation and a clock.
    ///
    /// `usage` is always [`KeyUsage::EvidenceSigning`] here; it is spelled at
    /// the call site so the usage-separation refusal is visible in the source
    /// that performs it.
    #[must_use]
    pub fn verdict(&self, observation: &IndependentObservation, now: DateTime<Utc>) -> Verdict {
        decide(
            self.key.as_ref(),
            KeyUsage::EvidenceSigning,
            &self.claim,
            observation,
            now,
        )
    }

    /// The `status.evidence.verification.trust` block for one verdict.
    #[must_use]
    pub fn to_status_value(&self, verdict: &Verdict) -> Value {
        json!({
            "basis": verdict.basis.as_str(),
            "keyState": verdict.key_state.as_str(),
            "policy": self.policy,
        })
    }
}

/// The `PolicyRef` one resolved source reports on a verdict.
#[must_use]
pub fn policy_ref(source: &TrustSource) -> crate::crds::PolicyRef {
    match source {
        TrustSource::Policy {
            name,
            uid,
            generation,
        } => crate::crds::PolicyRef {
            name: Some(name.clone()),
            uid: uid.clone(),
            generation: *generation,
        },
        // The synthesised policy has no UID and no generation; inventing either
        // would name an object an operator could not find. The name
        // `legacy-roster-v1` is the one true thing, and the condition message
        // carries it as `trustSource=legacy-roster-v1`.
        TrustSource::LegacyRoster => crate::crds::PolicyRef {
            name: Some(crate::trust::LEGACY_POLICY_NAME.to_string()),
            uid: None,
            generation: None,
        },
    }
}

/// One verification, as the controller performed it. `detail` carries the
/// `Error`'s `Display` on `Invalid`, the reason on `NotAttempted`, and `None`
/// on `Valid`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct VerificationResult {
    /// The verdict.
    pub result: VerificationVerdict,
    /// The `keyId` of the signing key that verified the signature: the
    /// trust's declared id, not the sidecar's, because that is what an
    /// operator edits and greps `status.matchedKeyId` against.
    pub matched_key_id: Option<String>,
    /// The DSSE `payloadType` that was verified, echoed from the caller.
    pub payload_type: String,
    /// When this verification happened.
    pub verified_at: DateTime<Utc>,
    /// The `Error`'s `Display` on `Invalid`; the reason on `NotAttempted`; the
    /// refused trust row on `Untrusted`.
    pub detail: Option<String>,
    /// What the signature's own key looked like in the resolved policy;
    /// `None` when no signature matched.
    pub trust: Option<TrustProjection>,
}

impl VerificationResult {
    /// A `NotAttempted` carrying `detail`.
    ///
    /// `pub` because [`verify_oracle`] also decides two `NotAttempted`s
    /// (unreadable trust, a panicked blocking task).
    #[must_use]
    pub fn not_attempted(payload_type: &str, detail: impl Into<String>) -> Self {
        Self {
            result: VerificationVerdict::NotAttempted,
            matched_key_id: None,
            payload_type: payload_type.to_string(),
            verified_at: Utc::now(),
            detail: Some(detail.into()),
            trust: None,
        }
    }

    /// A `Pending` carrying `detail`: an evidence-fetch Job is reading the
    /// document, and nothing has been decided about it.
    #[must_use]
    pub fn pending(payload_type: &str, detail: impl Into<String>) -> Self {
        Self {
            result: VerificationVerdict::Pending,
            matched_key_id: None,
            payload_type: payload_type.to_string(),
            verified_at: Utc::now(),
            detail: Some(detail.into()),
            trust: None,
        }
    }

    /// Whether this verification is a pass: `Valid` on a basis
    /// [`ValidBasis::is_pass`] admits, the rule a stored block is read by.
    ///
    /// Gates the receipt-derived facts (`records`, `capture`) the `Backup`
    /// reconciler copies only from a document an accepted key attested to.
    /// No projection (a `result` set without the trust layer) is an absent
    /// block.
    #[must_use]
    pub fn is_pass(&self) -> bool {
        self.result == VerificationVerdict::Valid
            && self
                .trust
                .as_ref()
                .map_or(ValidBasis::Absent, |p| {
                    ValidBasis::of_core(
                        p.verdict(&IndependentObservation::none(), self.verified_at)
                            .basis,
                    )
                })
                .is_pass()
    }

    /// An `Invalid` carrying `detail`.
    #[must_use]
    pub fn invalid(payload_type: &str, detail: impl Into<String>) -> Self {
        Self {
            result: VerificationVerdict::Invalid,
            matched_key_id: None,
            payload_type: payload_type.to_string(),
            verified_at: Utc::now(),
            detail: Some(detail.into()),
            trust: None,
        }
    }

    /// A signature that verified under `matched_key_id`, with the trust
    /// question still to be asked of `projection`.
    ///
    /// `result` is decided here with no observation, so a caller that never
    /// reaches [`Self::to_status_value`] (the reconcilers' log line, every
    /// consumer of the oracle) still reads the right word. The independent
    /// observation is only read on the two compromise rows, and both are
    /// `Untrusted`, so it can refine `basis` and `reason` but never flip
    /// `Valid` and `Untrusted`. `the_observation_only_moves_the_basis` asserts
    /// that over the whole table.
    fn signed(payload_type: &str, matched_key_id: String, projection: TrustProjection) -> Self {
        let now = Utc::now();
        let verdict = projection.verdict(&IndependentObservation::none(), now);
        Self {
            result: match verdict.result {
                TrustResult::Valid => VerificationVerdict::Valid,
                TrustResult::Untrusted => VerificationVerdict::Untrusted,
            },
            detail: untrusted_detail(&verdict, &projection, &matched_key_id),
            matched_key_id: Some(matched_key_id),
            payload_type: payload_type.to_string(),
            verified_at: now,
            trust: Some(projection),
        }
    }

    /// The independent observation a stored verification block carries: the
    /// `verifiedAt` a controller of this installation wrote on an earlier
    /// reconcile, and nothing else.
    ///
    /// Never the document's own claim: the compromise rule turns on the two
    /// being different things, which is why
    /// [`logweir_core::trust::IndependentObservation`] and
    /// [`logweir_core::trust::EvidenceClaim`] are distinct types.
    #[must_use]
    pub fn observation(existing: Option<&Value>) -> IndependentObservation {
        let Some(block) = existing else {
            return IndependentObservation::none();
        };
        // It must be an observation, not just an instant. Every verdict
        // carries a `verifiedAt`, including those that never read the
        // document (`NotAttempted` for no credential, an unfetchable object or
        // contested trust; `Invalid` for a digest mismatch). None of those is
        // evidence this installation saw the document, and accepting one would
        // put a false "recorded having seen this document before the
        // revocation" in front of an operator. Only `Valid` and `Untrusted`
        // read a signature, and both name its key.
        let verdict_about_a_signature = matches!(
            block.get("result").and_then(Value::as_str),
            Some("Valid" | "Untrusted")
        ) && block
            .get("matchedKeyId")
            .and_then(Value::as_str)
            .is_some_and(|id| !id.is_empty());
        if !verdict_about_a_signature {
            return IndependentObservation::none();
        }
        match block
            .get("verifiedAt")
            .and_then(Value::as_str)
            .and_then(|s| DateTime::parse_from_rfc3339(s).ok())
        {
            Some(t) => IndependentObservation::at(t.with_timezone(&Utc)),
            None => IndependentObservation::none(),
        }
    }

    /// This result as the `status.evidence.verification` object, reusing
    /// `existing`'s `verifiedAt` when nothing but the clock has moved.
    ///
    /// A reconciler's own status patch wakes it, so a fresh clock read on
    /// every pass would make every pass a write and a wake-up; the patch is
    /// skipped only if an unchanged verdict renders the same bytes. So
    /// `verifiedAt` is when this verdict was reached, not when it was last
    /// re-confirmed: a change in `result`, `matchedKeyId`, `payloadType` or
    /// `detail` takes the new clock; anything else keeps the stored one.
    ///
    /// A trust-only change does not move it either. `verifiedAt` is the one
    /// independent observation of when this installation saw the document; if
    /// a revocation refreshed it, the next pass would read "observed after the
    /// revocation" and flip `RecordedBeforeRevocation` to `Revoked` for good,
    /// the controller destroying its own evidence. So the identity that
    /// decides whether the instant moves is the signature (the key that made
    /// it and the media type it was made over), not the verdict. A
    /// verification that matched no key has no trust layer to move.
    #[must_use]
    pub fn to_status_value(&self, existing: Option<&Value>) -> Value {
        let mut block = serde_json::Map::new();
        let observation = Self::observation(existing);
        let verdict = self
            .trust
            .as_ref()
            .map(|p| (p, p.verdict(&observation, self.verified_at)));

        let result = match &verdict {
            Some((_, v)) => match v.result {
                TrustResult::Valid => VerificationVerdict::Valid,
                TrustResult::Untrusted => VerificationVerdict::Untrusted,
            },
            None => self.result,
        };
        block.insert("result".into(), json!(result.as_str()));
        if let Some(id) = &self.matched_key_id {
            block.insert("matchedKeyId".into(), json!(id));
        }
        block.insert("payloadType".into(), json!(self.payload_type));
        let detail = match (&verdict, &self.matched_key_id) {
            (Some((p, v)), Some(id)) => untrusted_detail(v, p, id),
            _ => self.detail.clone(),
        };
        if let Some(d) = &detail {
            block.insert("detail".into(), json!(d));
        }
        if let Some((p, v)) = &verdict {
            if let Some(at) = p.claim.signed_at {
                block.insert(
                    "signedAt".into(),
                    json!(at.to_rfc3339_opts(SecondsFormat::Secs, true)),
                );
            }
            block.insert("trust".into(), p.to_status_value(v));
        }

        // The signature's identity, not the verdict's (see above).
        let same_substance = if self.matched_key_id.is_some() {
            existing.is_some_and(|e| {
                e.get("matchedKeyId").is_some()
                    && ["matchedKeyId", "payloadType"]
                        .iter()
                        .all(|k| e.get(*k) == block.get(*k))
            })
        } else {
            existing.is_some_and(|e| {
                ["result", "matchedKeyId", "payloadType", "detail"]
                    .iter()
                    .all(|k| e.get(*k) == block.get(*k))
            })
        };
        let at = match (same_substance, existing.and_then(|e| e.get("verifiedAt"))) {
            (true, Some(stored)) => stored.clone(),
            _ => json!(self.verified_at.to_rfc3339_opts(SecondsFormat::Secs, true)),
        };
        block.insert("verifiedAt".into(), at);
        Value::Object(block)
    }
}

/// The `detail` for one trust verdict: `None` on a `Valid`, and a sentence
/// naming the refused row otherwise.
///
/// Every sentence names the key and the policy, so an operator knows which
/// key was refused and which object to edit. It never carries key material.
///
/// [`UntrustReason::SignedOutsideValidity`] carries a second clause when the
/// document claimed no signing time at all: "signed after the key was retired"
/// and "this scorecard records no phase to compare" are different faults.
#[must_use]
fn untrusted_detail(
    verdict: &Verdict,
    projection: &TrustProjection,
    key_id: &str,
) -> Option<String> {
    let reason = verdict.reason?;
    let policy = projection
        .policy
        .name
        .as_deref()
        .unwrap_or(crate::trust::LEGACY_POLICY_NAME);
    let head = format!(
        "the signature over this document verified under key {key_id}, and the trust policy \
         {policy} does not accept it"
    );
    let tail = match reason {
        UntrustReason::UntrustedSigner => format!(
            " ({}): {policy} carries no key with that id, so nothing here says this installation \
             ever trusted the signer",
            reason.as_str()
        ),
        UntrustReason::KeyUsageMismatch => format!(
            " ({}): that key is declared for a different usage and an evidence signature is \
             never accepted from it",
            reason.as_str()
        ),
        UntrustReason::SignedOutsideValidity => {
            let why = match projection.claim.absence {
                Some(absence) => absence.detail().to_string(),
                None => "the document's claimed signing time is outside the window that key was \
                         trusted in"
                    .to_string(),
            };
            format!(" ({}): {why}", reason.as_str())
        }
        UntrustReason::RecordedBeforeRevocation => format!(
            " ({}): that key was revoked for compromise, and this installation recorded having \
             seen this document before the revocation took effect — the instant is shown, and it \
             is never green",
            reason.as_str()
        ),
        UntrustReason::Revoked => format!(
            " ({}): that key was revoked for compromise and this installation has no record of \
             having seen this document before the revocation took effect, so it fails closed",
            reason.as_str()
        ),
    };
    // The object that records the compromise, when it is not the one that
    // answered: a namespace resolving through `legacy-roster-v1` or another
    // policy still reads the key as revoked, and is told where the revocation
    // lives.
    let compromise_row = matches!(
        reason,
        UntrustReason::Revoked | UntrustReason::RecordedBeforeRevocation
    );
    let recorded = if !compromise_row || projection.compromise_recorded_by.is_empty() {
        String::new()
    } else {
        format!(
            "; the revocation is recorded by TrustPolicy/{}, and a KeyCompromise revocation \
             applies to every namespace whichever trust it resolves through",
            projection.compromise_recorded_by.join(", TrustPolicy/")
        )
    };
    Some(head + &tail + &recorded)
}

/// Read the object with the read-only evidence handle, check the digest the
/// status recorded, verify the DSSE sidecar against the resolved trust's
/// signing key material, and then ask whether that key is still trusted.
///
/// 1. **No `store`** → `NotAttempted` with [`NO_CREDENTIAL_DETAIL`]: a fact
///    about the install, not the document.
/// 2. **`get` the payload and the sidecar** through the read-only handle.
///    Every `StoreError` maps to `NotAttempted`, carrying its `Display`: an
///    object that could not be fetched has made no claim, so it is never
///    `Invalid`.
/// 3. **Recompute `sha256_prefixed(payload)`** and compare it with the digest
///    the status recorded. A mismatch is `Invalid`, naming both digests: these
///    are not the bytes this run reported writing, and verifying the signature
///    alone would accept a genuinely signed older document in its place.
/// 4. **For each resolved key carrying [`KeyUsage::EvidenceSigning`]**, build
///    [`VerifyingKey::from_pem_str`] over its `spkiPem` and call
///    [`verify_detached`]. The first `Ok` selects that entry's own `keyId`;
///    otherwise `Invalid` with the last error's `Display`. An empty list is
///    `NotAttempted` with [`NO_SIGNING_KEYS_DETAIL`] (legacy) or
///    [`NO_POLICY_KEYS_DETAIL`]: `Invalid` would blame every document for a
///    missing line in one trust object.
/// 5. **The trust question**: the selected key's lifecycle against the
///    document's claimed signing time
///    ([`logweir_core::trust::claimed_signing_time`]). A retired, expired,
///    revoked, not-yet-open or wrong-usage key is
///    [`VerificationVerdict::Untrusted`], never `Invalid`.
///
/// The usage filter is part of step 4: a `GovernedApproval` or
/// `ConsoleConfirmation` key is never offered to [`verify_detached`], so it
/// cannot become a `matchedKeyId` by luck, and [`logweir_core::trust::decide`]
/// refuses the usage again ([`UntrustReason::KeyUsageMismatch`]).
///
/// An unparseable signing entry is skipped and the rest are tried, and an
/// entry's declared `keyId` is never compared with its material's hash: the
/// declared id is what an operator greps for, and a `TrustPolicy` whose id
/// disagrees with its material is already `Loaded=False/KeyIdMismatch`.
///
/// Synchronous: the body of a `spawn_blocking` closure (interface I13).
#[must_use]
pub fn verify_evidence(
    store: Option<&Store>,
    trust: &ResolvedTrust,
    payload_key: &str,
    payload_sha256: &str,
    sidecar_key: &str,
    payload_type: &str,
) -> VerificationResult {
    // STEP 1. No credential is not a bad document.
    let Some(store) = store else {
        return VerificationResult::not_attempted(payload_type, NO_CREDENTIAL_DETAIL);
    };

    // STEP 2. Both objects, through the read-only handle; every StoreError is
    // NotAttempted.
    //
    // Each under its cap, never whole: this process serves every namespace,
    // so a tenant's huge object must not take memory other reconciles need.
    // The document is read under [`controller_cap_for`] its payload type (the
    // evidence relay's cap for the same document, so both read paths agree),
    // the sidecar under `caps::SIDECAR`. An object over its cap is refused on
    // the size the store reports, before a body byte is read, as a final
    // `NotAttempted` naming the cap: it will not shrink.
    //
    // The document's worst case is reserved from the controller's one read
    // budget before it is read and held until the verdict is built.
    let _reservation = crate::read_budget::ReadBudget::controller()
        .reserve(crate::read_budget::DOCUMENT_READ_COST_BYTES);
    let payload = match store.get_capped(payload_key, controller_cap_for(payload_type)) {
        Ok((bytes, _version)) => bytes,
        Err(e) => return VerificationResult::not_attempted(payload_type, store_detail(&e)),
    };
    let sidecar_bytes = match store.get_capped(sidecar_key, caps::SIDECAR) {
        Ok((bytes, _version)) => bytes,
        Err(e) => return VerificationResult::not_attempted(payload_type, store_detail(&e)),
    };
    verify_fetched(
        &payload,
        &sidecar_bytes,
        trust,
        payload_key,
        payload_sha256,
        sidecar_key,
        payload_type,
    )
}

/// Steps 3–5 of [`verify_evidence`] over bytes that were already fetched.
///
/// The controller's own read-only handle and an evidence-fetch Job's relay
/// (`crate::evidence_fetch`) both end here, so the digest fence, the DSSE
/// check, the usage filter and the trust projection are one body of code
/// whichever principal read the bucket. A relayed document is not trusted more
/// than a fetched one: its digest is recomputed here against
/// `payload_sha256`, which the caller takes from the runner's own report,
/// never from the relay.
///
/// Pure and synchronous: no store, no clock beyond the verdict's instant.
#[must_use]
pub fn verify_fetched(
    payload: &[u8],
    sidecar_bytes: &[u8],
    trust: &ResolvedTrust,
    payload_key: &str,
    payload_sha256: &str,
    sidecar_key: &str,
    payload_type: &str,
) -> VerificationResult {
    // The verifier enforces its own document cap: both callers stop at it, but
    // bytes over the cap of their payload type are not hashed, copied or read
    // for a claim, whoever passed them. Over it the verdict is a final
    // `NotAttempted` naming the key and the cap, never `Invalid`: nothing was
    // read that could make a claim.
    let cap = controller_cap_for(payload_type);
    if payload.len() as u64 > cap {
        return VerificationResult::not_attempted(
            payload_type,
            over_read_cap_detail(
                payload_key,
                cap,
                &logweir_store::OverCap::Reported(payload.len() as u64),
            ),
        );
    }
    if sidecar_bytes.len() as u64 > caps::SIDECAR {
        return VerificationResult::not_attempted(
            payload_type,
            over_read_cap_detail(
                sidecar_key,
                caps::SIDECAR,
                &logweir_store::OverCap::Reported(sidecar_bytes.len() as u64),
            ),
        );
    }

    // STEP 3. The digest the status recorded, against the bytes fetched now.
    // A mismatch is `Invalid`.
    let computed = sha256_prefixed(payload);
    if computed != payload_sha256 {
        return VerificationResult::invalid(
            payload_type,
            format!(
                "digest mismatch at {payload_key}: the status records {payload_sha256} and the \
                 bytes in the archive are {computed}"
            ),
        );
    }

    // The sidecar has to parse before any key can be tried. A sidecar that is
    // not JSON is a fact about the object, so this is `Invalid`.
    let sidecar: Sidecar = match serde_json::from_slice(sidecar_bytes) {
        Ok(s) => s,
        Err(e) => {
            return VerificationResult::invalid(
                payload_type,
                format!("{sidecar_key} is not a DSSE sidecar: {e}"),
            )
        }
    };

    // STEP 4. The resolved trust's EvidenceSigning key material. An empty list
    // names itself and is never `Invalid`. A whole-usage block (the legacy
    // policy's "a partially loaded roster is not a roster") is also
    // `NotAttempted`: a fact about the trust object, not this document.
    if let Some(blocked) = trust.blocked_for(KeyUsage::EvidenceSigning) {
        return VerificationResult::not_attempted(payload_type, blocked);
    }
    let declared: Vec<&crate::trust::ResolvedKey> = trust
        .keys
        .iter()
        .filter(|k| k.trust.has_usage(KeyUsage::EvidenceSigning))
        .collect();
    if declared.is_empty() {
        return VerificationResult::not_attempted(
            payload_type,
            if trust.source.is_legacy() {
                NO_SIGNING_KEYS_DETAIL
            } else {
                NO_POLICY_KEYS_DETAIL
            },
        );
    }
    let mut last_error: Option<String> = None;
    for entry in declared {
        let key = match VerifyingKey::from_pem_str(&entry.spki_pem) {
            Ok(k) => k,
            Err(e) => {
                last_error = Some(format!("{}: {e}", entry.trust.key_id));
                continue;
            }
        };
        match verify_detached(&key, payload_type, payload, &sidecar) {
            // The policy's own `keyId`, not the one out of the sidecar: when
            // they differ, the id an operator can act on is the one in the
            // object they edit.
            Ok(_sidecar_key_id) => {
                // The claim comes out of the document that just verified:
                // unverified bytes have no claim worth reading, and a claim
                // read before the digest check would be a substituted
                // document's. Verified bytes that are not JSON fail closed as
                // `Unparseable`.
                let claim = claim_of(payload_type, payload);
                return VerificationResult::signed(
                    payload_type,
                    entry.trust.key_id.clone(),
                    TrustProjection {
                        key: Some(entry.trust.clone()),
                        claim,
                        policy: policy_ref(&trust.source),
                        compromise_recorded_by: entry.compromise_inherited_from.clone(),
                    },
                );
            }
            Err(e) => last_error = Some(format!("{}: {e}", entry.trust.key_id)),
        }
    }
    VerificationResult::invalid(
        payload_type,
        last_error.unwrap_or_else(|| {
            "no signing key on the TrustRoster verified this sidecar".to_string()
        }),
    )
}

/// Whether `payload_type` is a backup receipt's
/// ([`logweir_core::trust::is_backup_receipt`]): the one document the
/// controller holds under [`caps::CONTROLLER_RECEIPT`] and never parses into
/// a tree.
#[must_use]
pub fn is_backup_receipt(payload_type: &str) -> bool {
    logweir_core::trust::is_backup_receipt(payload_type)
}

/// The cap the shared controller reads one signed document of `payload_type`
/// under: [`caps::CONTROLLER_RECEIPT`] for a backup receipt,
/// [`caps::CONTROLLER_DOCUMENT`] for every other type, and nothing else
/// (`the_controllers_document_cap_is_one_of_its_two_rows`).
///
/// A receipt grows with a backup's topics, and the controller never parses
/// one whole ([`claim_of`], `observe_archive`). A scorecard is still parsed
/// into a `serde_json::Value`, at up to 37 times its size, so its cap is the
/// smaller.
#[must_use]
pub fn controller_cap_for(payload_type: &str) -> u64 {
    if is_backup_receipt(payload_type) {
        caps::CONTROLLER_RECEIPT
    } else {
        caps::CONTROLLER_DOCUMENT
    }
}

/// The signing time a VERIFIED document claims, with its absence named.
///
/// A backup receipt is not parsed into a tree: its `finished_at` is folded
/// from the bytes (`logweir_core::receipt_facts`) by the rule
/// [`EvidenceClaim::from_document`] reads it by: no field is `FieldAbsent`, a
/// value that is not an RFC 3339 string is `Unparseable`, and bytes that are
/// not JSON are `Unparseable`. `logweir-core`'s `receipt_facts` rows hold the
/// two readings together.
///
/// Every other document is parsed whole, only inside the cap its type is read
/// under. No caller passes bytes over it; this states the bound where the
/// parse is.
fn claim_of(payload_type: &str, payload: &[u8]) -> EvidenceClaim {
    if is_backup_receipt(payload_type) {
        let facts = logweir_core::receipt_facts::ReceiptFacts::fold(payload);
        return EvidenceClaim::from_receipt_facts(facts.as_ref());
    }
    if payload.len() as u64 > caps::CONTROLLER_DOCUMENT {
        return EvidenceClaim::absent(logweir_core::trust::ClaimAbsence::Unparseable);
    }
    match serde_json::from_slice::<Value>(payload) {
        Ok(json) => EvidenceClaim::from_document(payload_type, &json),
        Err(_) => EvidenceClaim::absent(logweir_core::trust::ClaimAbsence::Unparseable),
    }
}

/// [`verify_evidence`], with the namespace's whole [`Resolution`] in front of
/// it.
///
/// The two non-trust resolutions are verdicts about the cluster, not the
/// document, so both are `NotAttempted`:
///
/// * [`Resolution::Conflict`]: two policies claim this namespace, so it
///   resolves to nothing, and the detail carries
///   [`crate::trust::REASON_TRUST_POLICY_CONFLICT`]. An `Untrusted` would
///   blame a signer for an administrators' disagreement.
/// * [`Resolution::Unconfigured`]: no policy and no roster, carrying
///   [`NO_SIGNING_KEYS_DETAIL`] verbatim, as an unmigrated cluster always has.
#[must_use]
pub fn verify_resolved(
    store: Option<&Store>,
    resolution: &Resolution,
    payload_key: &str,
    payload_sha256: &str,
    sidecar_key: &str,
    payload_type: &str,
) -> VerificationResult {
    match resolution {
        Resolution::Trust(trust) => verify_evidence(
            store,
            trust,
            payload_key,
            payload_sha256,
            sidecar_key,
            payload_type,
        ),
        Resolution::Conflict {
            namespace,
            policies,
        } => VerificationResult::not_attempted(
            payload_type,
            trust_policy_conflict_detail(namespace, policies),
        ),
        Resolution::Unconfigured => {
            VerificationResult::not_attempted(payload_type, NO_SIGNING_KEYS_DETAIL)
        }
    }
}

/// [`verify_resolved`] over bytes that were already fetched: the same three
/// resolutions, the same [`verify_fetched`].
#[must_use]
pub fn verify_fetched_resolved(
    payload: &[u8],
    sidecar: &[u8],
    resolution: &Resolution,
    payload_key: &str,
    payload_sha256: &str,
    sidecar_key: &str,
    payload_type: &str,
) -> VerificationResult {
    match resolution {
        Resolution::Trust(trust) => verify_fetched(
            payload,
            sidecar,
            trust,
            payload_key,
            payload_sha256,
            sidecar_key,
            payload_type,
        ),
        Resolution::Conflict {
            namespace,
            policies,
        } => VerificationResult::not_attempted(
            payload_type,
            trust_policy_conflict_detail(namespace, policies),
        ),
        Resolution::Unconfigured => {
            VerificationResult::not_attempted(payload_type, NO_SIGNING_KEYS_DETAIL)
        }
    }
}

/// [`verify_oracle`]'s trust resolution in front of [`verify_fetched_resolved`],
/// so the evidence-fetch Job's half has no second verification path.
///
/// The namespace's trust is resolved exactly as the controller's own handle
/// resolves it (`crate::trust::resolve`), and an unreadable resolution is the
/// same `NotAttempted` with [`ROSTER_UNREADABLE_DETAIL`]. There is no store
/// here, so nothing needs `spawn_blocking`: the bytes are already in memory.
pub async fn verify_relayed(
    client: &kube::Client,
    r: &EvidenceRef,
    payload: &[u8],
    sidecar: &[u8],
) -> VerificationResult {
    let resolution = match crate::trust::resolve(client, &r.namespace).await {
        Ok(resolution) => resolution,
        Err(e) => {
            tracing::warn!(
                error = %e,
                namespace = %r.namespace,
                "this namespace's trust could not be resolved; this verification is \
                 NotAttempted rather than Invalid"
            );
            return VerificationResult::not_attempted(r.payload_type, ROSTER_UNREADABLE_DETAIL);
        }
    };
    verify_fetched_resolved(
        payload,
        sidecar,
        &resolution,
        &r.payload_key,
        &r.payload_sha256,
        &r.sidecar_key,
        r.payload_type,
    )
}

/// A `StoreError` as the `detail` of a `NotAttempted`.
///
/// `NotFound` gets its own sentence: "the object is not there" and "the bucket
/// would not answer" are different things for an operator, and for the retry
/// ([`not_attempted_class`]): a definite absence is final, and a store that
/// would not answer (a missing credential, a denial, a timeout) is read again.
///
/// An object over the controller's read cap is a third, final sentence,
/// [`over_read_cap_detail`]: the read was answered and the object will not
/// shrink. It names the key, the cap and the size the store reported.
#[must_use]
pub fn store_detail(e: &StoreError) -> String {
    match e {
        StoreError::NotFound(key) => {
            format!("the evidence object {key}{EVIDENCE_OBJECT_ABSENT_SUFFIX}")
        }
        StoreError::TooLarge { key, cap, observed } => over_read_cap_detail(key, *cap, observed),
        other => format!("{EVIDENCE_OBJECT_UNREADABLE_PREFIX}{other}"),
    }
}

/// [`store_detail`]'s sentence for an object over the controller's read cap:
/// the twin of the evidence relay's "is larger than the N-byte cap an evidence
/// fetch relays", and final like it (it begins with the key, so no transient
/// prefix of [`not_attempted_class`] matches).
#[must_use]
pub fn over_read_cap_detail(key: &str, cap: u64, observed: &logweir_store::OverCap) -> String {
    format!("{key} is larger than the {cap}{CONTROLLER_READ_CAP_PHRASE} ({observed}); nothing was verified")
}

/// What every [`over_read_cap_detail`] says after the cap's byte count.
pub const CONTROLLER_READ_CAP_PHRASE: &str = "-byte cap weirkeeper reads";

/// How [`store_detail`] begins for every store failure that is not a definite
/// absence: the transient class of [`not_attempted_class`].
pub const EVIDENCE_OBJECT_UNREADABLE_PREFIX: &str = "the evidence object could not be read: ";

/// How [`store_detail`] ends for the store's own `NotFound`: the one final
/// read failure of [`not_attempted_class`].
pub const EVIDENCE_OBJECT_ABSENT_SUFFIX: &str = " is not in the archive; nothing was verified";

/// How [`verify_oracle`]'s `NotAttempted` begins when its blocking task did
/// not come back. Transient.
pub const VERIFICATION_TASK_INCOMPLETE_PREFIX: &str = "the verification task did not complete: ";

/// How a `Restore`'s `NotAttempted` begins when the controller's own read of
/// the reported scorecard produced no document
/// (`controllers::restore::unread_scorecard_detail` and its destination twin).
/// Transient: the reader cannot tell a missing object from one it may not
/// read, so it is read again on the retry schedule and then stops.
pub const UNREAD_SCORECARD_PREFIX: &str = "the runner reported scorecard `";

/// How a controller read's `NotAttempted` begins when the receipt verified but
/// the archive observation carrying its window did not answer. Transient: a
/// `Valid` without its window would be final and never make the run a
/// recovery point.
pub const RECEIPT_WINDOW_UNREAD_PREFIX: &str = "the receipt verified, but ";

/// How an evidence-fetch Job's relay begins when the store answered its grant
/// with an error that is not `NotFound` (a denial, a timeout): transient, and
/// the Job is retried on the schedule.
pub const EVIDENCE_FETCH_UNREADABLE_PREFIX: &str = "the evidence-fetch Job could not read ";

/// How an evidence-fetch relay's own integrity failures begin (no result
/// document, one that did not verify, an object present with no bytes, a
/// length or digest that does not describe the bytes): "relay unreadable",
/// retried with a new Job. An object over the relay's cap is a fact about the
/// object, and final.
pub const EVIDENCE_FETCH_RELAY_PREFIX: &str = "the evidence-fetch relay";

/// What a controller-side `NotAttempted` says about trying again.
///
/// A read that failed for want of a credential must be retried, or the run
/// stays unverified after the credential exists and is never a recovery
/// point. The class is decided from the sentence the producing function wrote,
/// so the retry and the sentence cannot disagree: every producer spells its
/// sentence with one of the constants above, and `tests/verification.rs`
/// holds each producer to its class.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NotAttemptedClass {
    /// The read did not happen or did not answer, for a reason a later read
    /// can outlive without anything on the object changing: a store failure
    /// that is not `NotFound`, an unreadable trust resolution, a blocking task
    /// that did not come back, a scorecard read that produced nothing. Read
    /// again on the schedule.
    Transient,
    /// The store said `NotFound` for the document: a fact about the archive.
    /// Never read again, not on the schedule and not after a restart.
    Absent,
    /// Anything else: a fact about this installation's configuration (no
    /// archive handle, no signing key, two policies claiming the namespace, a
    /// location outside the handle's bucket) that no second read can change.
    /// Not on the schedule; an inline-archive run is read once more by the
    /// next controller process, where a configuration change arrives
    /// (`evidence_fetch::ReadMemory`).
    Final,
}

/// The [`NotAttemptedClass`] of one stored or fresh `NotAttempted` detail.
#[must_use]
pub fn not_attempted_class(detail: &str) -> NotAttemptedClass {
    if detail.contains(EVIDENCE_OBJECT_ABSENT_SUFFIX) {
        return NotAttemptedClass::Absent;
    }
    let transient = [
        EVIDENCE_OBJECT_UNREADABLE_PREFIX,
        ROSTER_UNREADABLE_DETAIL,
        VERIFICATION_TASK_INCOMPLETE_PREFIX,
        UNREAD_SCORECARD_PREFIX,
        EVIDENCE_FETCH_UNREADABLE_PREFIX,
        EVIDENCE_FETCH_RELAY_PREFIX,
        RECEIPT_WINDOW_UNREAD_PREFIX,
    ];
    if transient.iter().any(|prefix| detail.starts_with(prefix)) {
        NotAttemptedClass::Transient
    } else {
        NotAttemptedClass::Final
    }
}

/// What a UI renders for one object.
///
/// `green` is the badge; `reason` is the CamelCase `reason` of the `Verified`
/// condition that says the same thing on the object itself; `label` is the
/// sentence beside it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Badge {
    /// Whether the badge is green.
    pub green: bool,
    /// The `Verified` condition's `reason` — [`REASON_VERIFIED`] when green,
    /// and one of [`REASON_VERIFICATION_INVALID`],
    /// [`REASON_VERIFICATION_NOT_ATTEMPTED`], [`REASON_COMPLETE_NOT_COVERED`],
    /// [`REASON_EXIT_CODE_NOT_ZERO`] or [`REASON_OUTCOME_NOT_PASS`] otherwise.
    pub reason: &'static str,
    /// "verified by weirkeeper at `<verifiedAt>` against key `<matchedKeyId>`"
    /// when green, and the literal [`UNVERIFIED`] in every other case.
    pub label: String,
}

impl Badge {
    fn green(verified_at: &str, matched_key_id: &str, historical: bool) -> Self {
        let mut label =
            format!("verified by weirkeeper at {verified_at} against key {matched_key_id}");
        // A pass, not a warning: a key that signed while valid and has since
        // been retired is what rotation looks like, so the qualifier explains
        // the badge rather than hedging it.
        if historical {
            label.push_str(" (signed before that key was retired)");
        }
        Self {
            green: true,
            reason: REASON_VERIFIED,
            label,
        }
    }

    fn not_green(reason: &'static str) -> Self {
        Self {
            green: false,
            reason,
            label: UNVERIFIED.to_string(),
        }
    }
}

/// The verification half of both rules: `Ok(())` when the block is a `Valid`
/// this controller could have written, `Err(reason)` otherwise.
///
/// A `Valid` with no `matchedKeyId` or no `verifiedAt` is never produced by
/// [`verify_evidence`], so it is treated as no verdict rather than as a green
/// badge whose label cannot be rendered.
///
/// The basis is checked too, as defence in depth: the green rule is
/// `Valid ∧ (basis Current|Historical) ∧ run success`, and although
/// [`logweir_core::trust::decide`] only produces `Valid` on those bases, a
/// badge rule that reads one field is one edit away from rendering green over
/// a basis nobody intended.
fn valid_verification(status: &Value) -> Result<(&str, &str, bool), &'static str> {
    let v = status.pointer("/evidence/verification");
    let result = v
        .and_then(|v| v.get("result"))
        .and_then(Value::as_str)
        .unwrap_or("");
    match result {
        "Valid" => {}
        "Invalid" => return Err(REASON_VERIFICATION_INVALID),
        "Untrusted" => return Err(REASON_VERIFICATION_UNTRUSTED),
        _ => return Err(REASON_VERIFICATION_NOT_ATTEMPTED),
    }
    let at = v
        .and_then(|v| v.get("verifiedAt"))
        .and_then(Value::as_str)
        .ok_or(REASON_VERIFICATION_NOT_ATTEMPTED)?;
    let key = v
        .and_then(|v| v.get("matchedKeyId"))
        .and_then(Value::as_str)
        .ok_or(REASON_VERIFICATION_NOT_ATTEMPTED)?;
    // Two different states. No `trust` key (or `trust: null`) is an object an
    // older controller wrote, correctly green. A `trust` block this build
    // cannot read (`{}`, no or `null` basis, a number, an unknown word) is
    // malformed, and the arm that keeps the old rule must not swallow it. The
    // rule is [`ValidBasis`], shared with every other reader of a stored
    // `Valid`.
    match ValidBasis::of_json(v.and_then(|v| v.get("trust"))) {
        ValidBasis::Absent | ValidBasis::Current => Ok((at, key, false)),
        ValidBasis::Historical => Ok((at, key, true)),
        // The policy has not been applied to this old verdict yet. Not green,
        // and the honest reason is that nothing was attempted;
        // `VerificationUntrusted` would claim a refusal nobody made.
        ValidBasis::Unverified => Err(REASON_VERIFICATION_NOT_ATTEMPTED),
        ValidBasis::Refused => Err(REASON_VERIFICATION_UNTRUSTED),
    }
}

/// How a stored `verification.result: Valid` reads once its `trust` block is
/// applied: the one rule every reader of a stored `Valid` uses.
///
/// The green rule is `Valid ∧ (basis Current|Historical) ∧ run success`, and
/// an absent `trust` keeps the pre-existing rule. So a `Valid` is a pass on
/// exactly three shapes (no trust block, `basis: Current`,
/// `basis: Historical`) and on nothing else. In this allow-list form, a basis
/// a later build invents, a `null`, a block with no basis, the string `None`
/// and `RecordedBeforeRevocation` all land on a non-pass arm without anyone
/// naming them. Readers that compared `result == "Valid"` alone would pass
/// objects carrying `Valid` + `Unverified`; one function means one place to
/// be wrong.
///
/// The two non-pass arms are different facts. [`Self::Unverified`] is
/// "nothing has been compared yet" ([`TrustBasis::Unverified`]), read as
/// `NotAttempted`. [`Self::Refused`] is a verdict this installation does not
/// accept, or a block this build cannot read, read as `Untrusted`. Neither is
/// ever a pass.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ValidBasis {
    /// No `trust` block at all: an object an older controller wrote. A pass,
    /// under the pre-existing rule.
    Absent,
    /// `basis: Current`. A pass.
    Current,
    /// `basis: Historical`: the key has since been retired, expired or
    /// superseded and signed while it was valid. A pass, and not a warning.
    Historical,
    /// `basis: Unverified`: the policy has not been applied to this verdict
    /// yet. Not a pass; reads as `NotAttempted`.
    Unverified,
    /// Every other shape of a present block: `RecordedBeforeRevocation`,
    /// `None`, a missing, `null` or non-string basis, a word this build does
    /// not know. Not a pass; reads as `Untrusted`.
    Refused,
}

impl ValidBasis {
    /// From the basis word of a block that is present (`Some`) or absent
    /// (`None`). `Some(None)` is a block with no readable basis.
    #[must_use]
    pub fn of_word(block: Option<Option<&str>>) -> Self {
        match block {
            None => Self::Absent,
            Some(Some(TRUST_BASIS_CURRENT)) => Self::Current,
            Some(Some(TRUST_BASIS_HISTORICAL)) => Self::Historical,
            Some(Some(TRUST_BASIS_UNVERIFIED)) => Self::Unverified,
            Some(_) => Self::Refused,
        }
    }

    /// From a raw `verification.trust` value.
    ///
    /// `trust: null` is an absent block. The CRD declares the field
    /// `nullable: true`, so an update, server-side apply or JSON patch can
    /// store a `null` (a merge patch's `null` deletes the key instead). It
    /// reads one way everywhere, as absent: a `null` field carries nothing a
    /// trust decision could be read from; Kubernetes and RFC 7386 treat `null`
    /// as "no value"; every typed reader (`Option<TrustBasis>`) already sees
    /// it that way; and it grants nothing, since whoever can write it can
    /// delete the key.
    ///
    /// Everything that is a block but unreadable (`{}`, a `null` or missing
    /// `basis`, a number, an unknown word) is [`Self::Refused`].
    #[must_use]
    pub fn of_json(trust: Option<&Value>) -> Self {
        match trust {
            None | Some(Value::Null) => Self::Absent,
            Some(t) => Self::of_word(Some(t.get("basis").and_then(Value::as_str))),
        }
    }

    /// From the typed CRD block. A `trust: null` deserialises to `None` and is
    /// [`Self::Absent`], exactly as [`Self::of_json`] reads it.
    #[must_use]
    pub fn of_block(trust: Option<&crate::crds::TrustBasis>) -> Self {
        Self::of_word(trust.map(|t| t.basis.as_deref()))
    }

    /// From [`logweir_core::trust::decide`]'s own basis, a block that is
    /// always present.
    #[must_use]
    pub fn of_core(basis: TrustBasis) -> Self {
        Self::of_word(Some(Some(basis.as_str())))
    }

    /// Whether a `Valid` on this basis is a pass.
    #[must_use]
    pub fn is_pass(self) -> bool {
        matches!(self, Self::Absent | Self::Current | Self::Historical)
    }
}

/// Whether a stored `verification.result` is a pass: `Valid` on a basis
/// [`ValidBasis::is_pass`] admits, and nothing else.
#[must_use]
pub fn stored_result_is_pass(result: Option<&str>, basis: ValidBasis) -> bool {
    result == Some(VerificationVerdict::Valid.as_str()) && basis.is_pass()
}

/// `trust.basis` for a verdict reached against a key that was current.
const TRUST_BASIS_CURRENT: &str = "Current";
/// `trust.basis` for a verdict reached against a key that has since been
/// retired, expired or superseded.
const TRUST_BASIS_HISTORICAL: &str = "Historical";

/// `trust.basis` for a stored verdict no policy has been applied to yet,
/// because the status predates `signedAt` and the document has not been
/// re-read ([`logweir_core::trust::TrustBasis::Unverified`]).
const TRUST_BASIS_UNVERIFIED: &str = "Unverified";

/// **Interface I21, the `Backup` half.** Green ⟺ `verification.result ==
/// Valid` AND `status.exitCode == 0`.
///
/// It never reads `outcome`: `BackupStatus` has none, so a rule that read it
/// would render every `Backup` ungreen
/// (`a_backup_is_green_only_on_valid_and_exit_code_zero`).
#[must_use]
pub fn backup_badge(status: &Value) -> Badge {
    let (at, key, historical) = match valid_verification(status) {
        Ok(triple) => triple,
        Err(reason) => return Badge::not_green(reason),
    };
    if status.get("exitCode").and_then(Value::as_i64) != Some(0) {
        return Badge::not_green(REASON_EXIT_CODE_NOT_ZERO);
    }
    Badge::green(at, key, historical)
}

/// **Interface I21, the `Restore` half.** Green ⟺ `verification.result ==
/// Valid` AND no signed complete verification that says `covered: false`
/// ([`REASON_COMPLETE_NOT_COVERED`], judged before the outcome so a real one,
/// `fail-integrity` at exit 2, is named by that reason and not by
/// `OutcomeNotPass`) AND `status.outcome == pass` AND no recorded `exitCode`
/// other than `0`.
///
/// `outcome` is the scorecard's own string, copied and never re-derived. Only
/// `pass` is green: a `fail-integrity` run produced a valid signed document
/// saying the restore did not reconcile, and a green badge over it would
/// invert the most valuable thing the tool reports.
///
/// The exit code stays authoritative: an exit-2 run's signed scorecard is
/// fetched and verified like a passing one's, and a rule that read only the
/// document would be one runner defect away from green over a failed run. A
/// recorded `exitCode` that is not `0` is never green. An absent `exitCode`
/// (a pre-terminal object, or a projection of the verdict alone) is judged on
/// `outcome`.
#[must_use]
pub fn restore_badge(status: &Value) -> Badge {
    let (at, key, historical) = match valid_verification(status) {
        Ok(triple) => triple,
        Err(reason) => return Badge::not_green(reason),
    };
    // A complete verification that did not cover the restore is never a pass,
    // whatever `outcome` says, and is judged before the outcome so it names
    // itself: `OutcomeNotPass` would hide why it failed from an operator
    // alerting on the reason. The verification is judged first, so this is
    // only ever read over a document whose signature verified.
    if status.pointer("/integrity/complete/covered") == Some(&Value::Bool(false)) {
        return Badge::not_green(REASON_COMPLETE_NOT_COVERED);
    }
    if status.get("outcome").and_then(Value::as_str) != Some("pass") {
        return Badge::not_green(REASON_OUTCOME_NOT_PASS);
    }
    if status
        .get("exitCode")
        .is_some_and(|code| code.as_i64() != Some(0))
    {
        return Badge::not_green(REASON_EXIT_CODE_NOT_ZERO);
    }
    Badge::green(at, key, historical)
}

/// Whether `status.evidence.verification` is a `Valid` verdict on a basis the
/// badge accepts: the verification half of [`restore_badge`] and
/// [`backup_badge`], without their run-outcome half.
///
/// `Restore.status.completion` is gated on exactly this: the completion panel
/// carries no trust caption of its own, so its facts are published only from
/// a scorecard whose signature verified. Invalid, Untrusted, NotAttempted,
/// Pending and a stored `Unverified` basis all answer `false`.
#[must_use]
pub fn verification_is_valid(status: &Value) -> bool {
    valid_verification(status).is_ok()
}

/// The `Verified` condition for `badge`, with the `metav1.Condition`
/// `lastTransitionTime` contract applied.
///
/// `existing` is the object's current `Verified` condition, so a badge that
/// has not changed keeps the instant it changed at
/// (`conditions::merge_condition`).
#[must_use]
pub fn verified_condition(
    badge: &Badge,
    existing: Option<&Condition>,
    observed_generation: Option<i64>,
    now: DateTime<Utc>,
) -> Condition {
    merge_condition(
        existing,
        Condition {
            r#type: CONDITION_VERIFIED.to_string(),
            status: if badge.green { "True" } else { "False" }.to_string(),
            observed_generation,
            last_transition_time: Some(now),
            reason: Some(badge.reason.to_string()),
            message: Some(badge.label.clone()),
        },
    )
}

/// The keys [`VerificationResult::to_status_value`] omits
/// when the verdict does not hold them — the ones a merge PATCH has to null.
///
/// `signedAt` and `trust` are written only for a verdict that carried a trust
/// projection; `matchedKeyId` and `detail` only for one that has them.
const VERIFICATION_CLEARED_KEYS: [&str; 4] = ["matchedKeyId", "detail", "signedAt", "trust"];

/// One rendered `status.evidence.verification` block as a merge PATCH: every
/// field this verdict does not hold written as an explicit `null`.
///
/// A JSON merge patch leaves an omitted key in place and deletes a key sent as
/// `null`. `to_status_value` omits `matchedKeyId`, `detail`, `signedAt` and
/// `trust` when the verdict has none, so without this a stale `matchedKeyId`
/// would stay under a new `NotAttempted`, and `retrust` would then make a
/// trust decision about a document this controller never fetched. Seam **S7**:
/// every write is a resourceVersion-preconditioned merge PATCH with an
/// explicit `null` for a field that no longer holds.
///
/// The nulls belong to the patch, not the rendered block: the badge is
/// computed over `to_status_value`'s block, and this wrapper is applied only on
/// the way into a patch. It cannot cause a write storm:
/// `conditions::apply_merge_patch` removes a key sent as `null` and does
/// nothing when it was absent, so `status_unchanged` still answers
/// "unchanged" for a verdict that has not moved.
///
/// Both writers take it: the second patch ([`crate::controllers::backup`],
/// [`crate::controllers::restore`]), where an evidence-fetch Job's later
/// verdict replaces a `Pending`, and [`Retrust::patch`], which re-derives
/// stored verdicts on terminal objects at every trust edit.
#[must_use]
pub fn verification_patch_value(block: Value) -> Value {
    let Value::Object(mut map) = block else {
        return block;
    };
    for key in VERIFICATION_CLEARED_KEYS {
        map.entry(key.to_string()).or_insert(Value::Null);
    }
    Value::Object(map)
}

/// The SECOND `/status` merge patch: `status.evidence.verification` plus the
/// full condition list with `Verified` merged into it.
///
/// A separate patch, so a verification failure can never prevent the exit
/// code from being recorded: a 500 here leaves the exit code, the evidence keys
/// and the scorecard-derived fields where the first patch put them
/// (`verification_is_a_second_patch_after_the_status_patch`).
///
/// It carries the whole condition list because a merge patch replaces arrays:
/// `[Verified]` alone would delete the terminal `Complete`/`Failed` condition.
/// `terminal` is the list the first patch carried, taken from the patch value,
/// because the in-memory object was read before that patch.
#[must_use]
pub fn second_patch(terminal: &[Condition], verified: Condition, verification: Value) -> Value {
    let mut conditions: Vec<Condition> = terminal
        .iter()
        .filter(|c| c.r#type != verified.r#type)
        .cloned()
        .collect();
    conditions.push(verified);
    json!({
        "status": {
            "evidence": { "verification": verification },
            "conditions": conditions,
        }
    })
}

/// The condition list a terminal `/status` merge patch carried, as
/// [`Condition`]s.
///
/// Reads the patch value rather than the object: after the first PATCH the
/// in-memory object is stale. An empty list is the truthful answer for a patch
/// that carried no `conditions` key.
#[must_use]
pub fn conditions_in(patch: &Value) -> Vec<Condition> {
    patch
        .pointer("/status/conditions")
        .and_then(|c| serde_json::from_value::<Vec<Condition>>(c.clone()).ok())
        .unwrap_or_default()
}

/// The `status.evidence.verification` block currently on the object, for
/// [`VerificationResult::to_status_value`]'s timestamp rule.
#[must_use]
pub fn stored_verification(status: Option<&Value>) -> Option<&Value> {
    status?.pointer("/evidence/verification")
}

/// What one verification names: the two objects, the digest the status
/// recorded, and the media type that binds them.
///
/// By value, because the future outlives the call.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EvidenceRef {
    /// The namespace of the object this evidence belongs to: the one input
    /// trust resolution cannot derive. A namespace never names its own trust;
    /// the policy names the namespaces it governs. So this is
    /// `metadata.namespace`, read by the reconciler off the object it
    /// reconciles, never a name the subject supplied and could choose.
    pub namespace: String,
    /// The object key of the signed payload — a backup receipt or a scorecard.
    pub payload_key: String,
    /// `sha256:<hex>` as the status recorded it, from the bytes the run wrote.
    pub payload_sha256: String,
    /// The object key of the detached DSSE sidecar.
    pub sidecar_key: String,
    /// `logweir_verify::PAYLOAD_TYPE_BACKUP_RECEIPT` (interface **I3**) or
    /// `PAYLOAD_TYPE_SCORECARD`, never a string a reconcile arm invented.
    pub payload_type: &'static str,
}

/// The verification half of a reconciler, injected.
///
/// An oracle, not a `Store`, for the reason
/// `controllers::backup::ArchiveOracle` gives: `Store` is blocking (**I13**),
/// so the real implementation is a `spawn_blocking` closure over the shared
/// `Arc<Store>`, and an injected oracle lets
/// `verification_is_a_second_patch_after_the_status_patch` assert the route
/// order with no bucket anywhere.
///
/// `BoxFuture<'static, …>`, not `'a`: an `Fn`'s `Output` is matched exactly,
/// so a borrowed future would force a `'static` local. The future owns what it
/// reads.
pub type VerifyOracle<'a> = &'a (dyn Fn(EvidenceRef) -> futures::future::BoxFuture<'static, VerificationResult>
         + Send
         + Sync
         + 'a);

/// The oracle for a controller that holds no evidence credential: it attempts
/// nothing and says so.
///
/// Used when `Context::archive` is `None` (no `logweir-evidence-ro`), and by
/// unit tests that are not about verification. The verdict is `NotAttempted`
/// with [`NO_CREDENTIAL_DETAIL`], never `Invalid`.
#[must_use]
pub fn unverified_evidence(
    r: EvidenceRef,
) -> futures::future::BoxFuture<'static, VerificationResult> {
    Box::pin(async move { VerificationResult::not_attempted(r.payload_type, NO_CREDENTIAL_DETAIL) })
}

/// The `detail` on a `NotAttempted` produced because the cluster-scoped
/// `TrustRoster` could not be read at all.
///
/// Distinct from [`NO_SIGNING_KEYS_DETAIL`], which is about a readable roster
/// that lists nothing: an operator told to "add the runner's public key" when
/// the problem is a 503 will edit an object that was already correct.
pub const ROSTER_UNREADABLE_DETAIL: &str =
    "this namespace's trust could not be resolved (the TrustPolicy list or the cluster-scoped \
     TrustRoster `default` could not be read); nothing was verified";

/// The real verification oracle: the controller's read-only evidence handle,
/// the namespace's resolved trust, and [`verify_evidence`] on a blocking
/// thread.
///
/// `Store::get_capped` drives its own runtime, so calling [`verify_evidence`]
/// straight from the `async move` block below compiles and panics at the
/// first verification (interface I13).
/// `tests/retention.rs::no_store_call_is_made_outside_spawn_blocking` names
/// `verify_evidence(` in its `STORE_CALL_TOKENS` because this call site holds
/// a `Store::get_capped` while naming no `Store` at all.
///
/// Three ways reach `NotAttempted` before the document is fetched: no archive
/// handle, unreadable trust, and a blocking task that did not come back. None
/// is `Invalid`: each is a fact about this controller.
pub fn verify_oracle(
    archive: Option<std::sync::Arc<Store>>,
    client: kube::Client,
) -> impl Fn(EvidenceRef) -> futures::future::BoxFuture<'static, VerificationResult>
       + Send
       + Sync
       + 'static {
    move |r: EvidenceRef| {
        let handle = archive.clone();
        let client = client.clone();
        Box::pin(async move {
            let Some(handle) = handle else {
                return unverified_evidence(r).await;
            };
            // The namespace's trust, not one cluster-scoped roster: `resolve`
            // lists every `TrustPolicy` and falls back to the synthesised
            // `legacy-roster-v1`, so an unmigrated cluster reaches the same key
            // set.
            let resolution = match crate::trust::resolve(&client, &r.namespace).await {
                Ok(resolution) => resolution,
                Err(e) => {
                    tracing::warn!(
                        error = %e,
                        namespace = %r.namespace,
                        roster = crate::ROSTER_NAME,
                        "this namespace's trust could not be resolved; this verification is \
                         NotAttempted rather than Invalid"
                    );
                    return VerificationResult::not_attempted(
                        r.payload_type,
                        ROSTER_UNREADABLE_DETAIL,
                    );
                }
            };
            let payload_type = r.payload_type;
            // One `spawn_blocking`, two `get`s (interface I13).
            match tokio::task::spawn_blocking(move || {
                verify_resolved(
                    Some(&handle),
                    &resolution,
                    &r.payload_key,
                    &r.payload_sha256,
                    &r.sidecar_key,
                    r.payload_type,
                )
            })
            .await
            {
                Ok(result) => result,
                Err(e) => VerificationResult::not_attempted(
                    payload_type,
                    format!("{VERIFICATION_TASK_INCOMPLETE_PREFIX}{e}"),
                ),
            }
        })
    }
}

/// `conditions` with the object's existing `Verified` condition carried
/// forward.
///
/// A merge patch replaces arrays, so a terminal patch writing
/// `conditions: [Complete, EvidenceRecorded]` would delete the `Verified`
/// condition [`second_patch`] added on the previous pass; the second patch
/// re-adds it, the write wakes the reconciler, and the two fight for ever (20
/// reconciles a second, each a real write, on a live cluster). `Verified` is
/// the controller's fact about the run, computed after the terminal write, so
/// every terminal patch builder calls this.
/// `a_verified_object_reconciles_without_a_patch` asserts the patch count is
/// zero.
#[must_use]
pub fn carry_verified(existing: Option<&Vec<Condition>>, conditions: Vec<Value>) -> Vec<Value> {
    carry_conditions(existing, conditions, &[CONDITION_VERIFIED])
}

/// [`carry_verified`], for any set of condition types a patch builder does not
/// own (`Verified`, `RunnerReady`).
///
/// The hot loop is a property of merge patch replacing arrays, not of one
/// condition: every condition one pass writes and the next builder does not
/// own is a candidate for the same fight.
///
/// Order is part of the contract: the builder's own conditions first, then
/// each carried type in the order given, so a steady object computes the same
/// array every pass and `conditions::status_unchanged` can skip the patch. A
/// type the builder already wrote is never carried: its own answer wins, which
/// makes a changed verdict land.
#[must_use]
pub fn carry_conditions(
    existing: Option<&Vec<Condition>>,
    mut conditions: Vec<Value>,
    types: &[&str],
) -> Vec<Value> {
    // `Vec<Value>`, not `Vec<Condition>`: the builders build their arrays as
    // `json!(merge_condition(…))`, and a round trip through the struct here
    // would make a byte-for-byte comparison depend on two serialisations
    // agreeing (see `conditions::status_unchanged`).
    for r#type in types {
        if conditions
            .iter()
            .any(|c| c.get("type") == Some(&json!(r#type)))
        {
            continue;
        }
        if let Some(v) = current_condition(existing, r#type) {
            conditions.push(json!(v));
        }
    }
    conditions
}

/// One object's verdict, re-derived from what is already on its status.
///
/// Terminal objects are not re-read, yet a revocation must still change what
/// the console says. A revocation changes nothing about the document, so
/// nothing is fetched: the stored `matchedKeyId`, `signedAt` and `verifiedAt`
/// are the three facts [`logweir_core::trust::decide`] takes. No storage read,
/// no signature check, no Job.
///
/// Changes: `status.evidence.verification.{result,trust,detail}`, and the
/// `Verified` condition. Does not change: `phase`, `exitCode`, `outcome`,
/// `evidence.receiptKey`, `evidence.sidecarKey`, `matchedKeyId`,
/// `payloadType`, `signedAt`, and above all `verifiedAt`, the only independent
/// observation, which a downgrade must not refresh (see
/// [`VerificationResult::to_status_value`]).
///
/// The `Verified` condition is rewritten although it is not a verification
/// field: leaving `Verified=True` beside `result: Untrusted` would be a green
/// condition over a verdict this installation just refused. It is the
/// controller's own fact about the run, already rewritten by
/// [`second_patch`], and it carries a `reason` and `message` naming the key
/// and the policy that decided. No run fact moves.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Retrust {
    /// The new `status.evidence.verification` block.
    pub verification: Value,
    /// The re-derived `Verified` condition.
    pub verified: Condition,
    /// The `result` that was on the object before this pass.
    pub from: String,
    /// The `result` this pass reached.
    pub to: String,
}

impl Retrust {
    /// The `/status` merge patch, carrying the whole condition list (a merge
    /// patch replaces arrays, so `[Verified]` alone would delete the terminal
    /// `Complete`/`Failed` condition). `existing` is the object's current list.
    ///
    /// The block is written through [`verification_patch_value`], with
    /// explicit nulls: [`retrust`] omits `detail`, `signedAt` and `trust` when
    /// the re-derived verdict has none, and a merge patch would otherwise leave
    /// a stale "the trust policy X does not accept it" under `result: Valid`,
    /// or a stale `trust` block under `NotAttempted`. This writer runs on
    /// terminal objects at every trust edit, so this is where it is reachable.
    /// The badge is computed over the rendered block before this method.
    #[must_use]
    pub fn patch(&self, existing: &[Condition]) -> Value {
        second_patch(
            existing,
            self.verified.clone(),
            verification_patch_value(self.verification.clone()),
        )
    }
}

/// Re-derive one stored verdict against `resolution` — `None` when nothing
/// changes.
///
/// `status` is the object's whole `status` as JSON; `badge` is the kind's own
/// green rule ([`backup_badge`] or [`restore_badge`]), so the condition this
/// produces obeys the same two-field rule the fresh path does.
///
/// `Invalid` and `NotAttempted` with no `matchedKeyId` are not trust verdicts:
/// no key was selected, so a policy change has nothing to re-decide, and
/// re-deriving would invent a verdict out of a digest mismatch. They return
/// `None`.
///
/// `None` means "send nothing". The comparison is over the rendered block and
/// condition, so a policy edit touching a key this object never used writes
/// nothing; otherwise one `kubectl apply` would wake every terminal object.
#[must_use]
pub fn retrust(
    status: &Value,
    resolution: &Resolution,
    badge: fn(&Value) -> Badge,
    existing: Option<&Vec<Condition>>,
    observed_generation: Option<i64>,
    now: DateTime<Utc>,
) -> Option<Retrust> {
    retrust_with(
        status,
        resolution,
        badge,
        existing,
        observed_generation,
        now,
        &SigningTime::NotNeeded,
    )
}

/// [`retrust`], with the outcome of one bounded re-read of the document in
/// hand.
///
/// `signing_time` supplies exactly one missing input: the document's own
/// claimed signing time, for a status written before this controller recorded
/// it. Everything else is still re-derived from the status, and no signature is
/// re-checked. `signing_time_in` compares the bytes with the digest the run
/// recorded, so the recovered claim is as trustworthy as the verdict being
/// repaired.
///
/// * [`SigningTime::Recovered`]: the block gains a `signedAt`, and the verdict
///   is the one a fresh run would reach.
/// * [`SigningTime::Absent`]: the document genuinely claims no signing time,
///   and refuses as it always has, now for a reason true of the document.
/// * [`SigningTime::Unreadable`] / [`SigningTime::NotAttempted`] /
///   [`SigningTime::NotNeeded`]: nothing was learned, so the verdict waits on
///   a [`TrustBasis::Unverified`] basis with the reason said. Not `Untrusted`,
///   because nothing was read; not green, because [`Badge`] refuses that basis.
#[must_use]
pub fn retrust_with(
    status: &Value,
    resolution: &Resolution,
    badge: fn(&Value) -> Badge,
    existing: Option<&Vec<Condition>>,
    observed_generation: Option<i64>,
    now: DateTime<Utc>,
    signing_time: &SigningTime,
) -> Option<Retrust> {
    let stored = status.pointer("/evidence/verification")?;
    let matched_key_id = stored.get("matchedKeyId").and_then(Value::as_str)?;
    let payload_type = stored.get("payloadType").and_then(Value::as_str)?;
    let from = stored.get("result").and_then(Value::as_str)?.to_string();

    let mut block = serde_json::Map::new();
    let (result, detail, trust) = match resolution {
        Resolution::Conflict {
            namespace,
            policies,
        } => (
            VerificationVerdict::NotAttempted.as_str().to_string(),
            Some(trust_policy_conflict_detail(namespace, policies)),
            None,
        ),
        Resolution::Unconfigured => (
            VerificationVerdict::NotAttempted.as_str().to_string(),
            Some(NO_SIGNING_KEYS_DETAIL.to_string()),
            None,
        ),
        Resolution::Trust(resolved) => {
            let projection = TrustProjection {
                key: resolved.key(matched_key_id).map(|k| k.trust.clone()),
                // The stored claim, or the caller's one bounded re-read; the
                // document is not fetched here. An absence the document itself
                // carries fails closed under `decide`'s rule, named.
                claim: match signing_time {
                    SigningTime::Recovered(at) => EvidenceClaim::at(*at),
                    SigningTime::Absent(absence) => EvidenceClaim::absent(*absence),
                    SigningTime::NotNeeded
                    | SigningTime::Deferred
                    | SigningTime::Unreadable(_)
                    | SigningTime::NotAttempted(_)
                    | SigningTime::OverCap(_) => stored_claim(stored),
                },
                policy: policy_ref(&resolved.source),
                compromise_recorded_by: resolved
                    .key(matched_key_id)
                    .map(|k| k.compromise_inherited_from.clone())
                    .unwrap_or_default(),
            };
            let verdict = projection.verdict(&VerificationResult::observation(Some(stored)), now);
            let trust = Some(projection.to_status_value(&verdict));
            // Nothing was read, so nothing is withdrawn: only the basis and the
            // sentence change, and the badge stops being green on the basis.
            if verdict.awaits_signing_time() {
                // `NotAttempted`, not the stored `Valid`: every reader that
                // predates the basis treats anything that is not `Valid` as
                // unverified, and three read `result` alone
                // (`ui/pages/backups.js`, `logweir_api::status`, the `SIGNED`
                // printer column), so a carried `Valid` rendered a green badge
                // over a document no policy had been applied to. `NotAttempted`
                // is literally true and fails closed on all of them;
                // `matchedKeyId` and `verifiedAt` still record the original
                // observation, and `detail` says what happened.
                //
                // A deferred pass reproduces the previous sentence, because it
                // learned nothing new; recomputing it would make a silent pass
                // a write.
                let detail = match signing_time {
                    SigningTime::Deferred => stored
                        .get("detail")
                        .and_then(Value::as_str)
                        .map(str::to_string),
                    _ => None,
                }
                .unwrap_or_else(|| unverified_detail(&projection, matched_key_id, signing_time));
                (
                    VerificationVerdict::NotAttempted.as_str().to_string(),
                    Some(detail),
                    trust,
                )
            } else {
                let result = match verdict.result {
                    TrustResult::Valid => VerificationVerdict::Valid,
                    TrustResult::Untrusted => VerificationVerdict::Untrusted,
                };
                let detail = untrusted_detail(&verdict, &projection, matched_key_id);
                (result.as_str().to_string(), detail, trust)
            }
        }
    };

    // The same key order `VerificationResult::to_status_value` writes, so both
    // paths render the same bytes for the same verdict (`serde_json` preserves
    // insertion order here, and `conditions::status_unchanged` decides whether
    // a patch is sent).
    block.insert("result".into(), json!(result));
    block.insert("matchedKeyId".into(), json!(matched_key_id));
    block.insert("payloadType".into(), json!(payload_type));
    if let Some(d) = &detail {
        block.insert("detail".into(), json!(d));
    }
    // Carried verbatim, never recomputed. The one exception: a block with no
    // signing time takes the one the bounded re-read found, in the
    // `SecondsFormat::Secs` spelling the fresh path uses.
    if let Some(v) = stored.get("signedAt").filter(|v| !v.is_null()) {
        block.insert("signedAt".into(), v.clone());
    } else if let SigningTime::Recovered(at) = signing_time {
        block.insert(
            "signedAt".into(),
            json!(at.to_rfc3339_opts(SecondsFormat::Secs, true)),
        );
    }
    if let Some(mut t) = trust {
        // The two fields that make "one bounded re-read" bounded: `retryAfter`
        // bars the next attempt after one that learned nothing;
        // `signingTimeRead` settles the object once a completed read shows the
        // absence is the document's. Both live on `trust`, so nulling the
        // block clears them with the verdict, and both are dropped once a read
        // succeeds.
        let object = t.as_object_mut().expect("a trust block is an object");
        match signing_time {
            SigningTime::Unreadable(_) | SigningTime::NotAttempted(_) => {
                object.insert(
                    "retryAfter".into(),
                    json!((now + chrono::Duration::seconds(RETRY_AFTER_SECS))
                        .to_rfc3339_opts(SecondsFormat::Secs, true)),
                );
            }
            // Carried verbatim, or a deferred pass would render a different
            // block on every reconcile.
            SigningTime::Deferred => {
                // Clamped on the way out too, so a hand-written far-future
                // instant is replaced by the ceiling; a legitimate value is
                // already below it and stays byte-identical.
                if let Some(until) = deferred_until(stored, now) {
                    object.insert(
                        "retryAfter".into(),
                        json!(until.to_rfc3339_opts(SecondsFormat::Secs, true)),
                    );
                }
                if let Some(v) = stored.pointer("/trust/signingTimeRead") {
                    object.insert("signingTimeRead".into(), v.clone());
                }
            }
            // The document answered, so the question is closed: no backoff.
            SigningTime::Absent(_) => {
                object.insert("signingTimeRead".into(), json!(SIGNING_TIME_ABSENT));
            }
            // Over the cap: the next read would answer the same, so it
            // settles too, with its own word.
            SigningTime::OverCap(_) => {
                object.insert("signingTimeRead".into(), json!(SIGNING_TIME_OVER_CAP));
            }
            // A successful read carries neither: `signedAt` is on the block,
            // and `compared_a_claim` answers from the basis.
            SigningTime::Recovered(_) => {}
            // Nothing was attempted and nothing is forgotten: a caller not
            // performing the recovery must not clear a bound it did not set.
            SigningTime::NotNeeded => {
                for key in ["retryAfter", "signingTimeRead"] {
                    if let Some(v) = stored.pointer(&format!("/trust/{key}")) {
                        object.insert((*key).to_string(), v.clone());
                    }
                }
            }
        }
        block.insert("trust".into(), t);
    }
    if let Some(v) = stored.get("verifiedAt") {
        block.insert("verifiedAt".into(), v.clone());
    }
    let verification = Value::Object(block);

    // The badge is computed over the status that will exist: this object's
    // own `exitCode`/`outcome` beside the new verification block.
    let mut projected = status.clone();
    projected["evidence"] = json!({ "verification": verification.clone() });
    let stored_condition = current_condition(existing, CONDITION_VERIFIED);
    let verified = verified_condition(
        &badge(&projected),
        stored_condition,
        observed_generation,
        now,
    );

    // Both halves: a `Verified` condition inconsistent with an already-correct
    // block (clobbered by another builder, or a partial patch) must still be
    // repaired, or a green condition sits beside an `Untrusted` result.
    // `merge_condition` keeps the stored `lastTransitionTime` when status and
    // reason are unchanged, so a steady object still sends nothing.
    if &verification == stored && stored_condition == Some(&verified) {
        return None;
    }
    Some(Retrust {
        verification,
        verified,
        from,
        to: result,
    })
}

/// The `detail` for a stored verdict no policy has been applied to yet. It
/// names the key, the policy and what happens next, and never says the
/// document is untrusted: nothing about the document has been examined, and
/// an operator should see their own upgrade, not their archive.
fn unverified_detail(
    projection: &TrustProjection,
    matched_key_id: &str,
    signing_time: &SigningTime,
) -> String {
    let policy = projection
        .policy
        .name
        .as_deref()
        .unwrap_or("this namespace's trust");
    let why = match signing_time {
        SigningTime::NotNeeded => "the document has not been re-read yet".to_string(),
        // A fallback: `retrust_with` reuses the stored sentence on a deferred
        // pass, so this is reached only for a block that carried none.
        SigningTime::Deferred => {
            "the last attempt learned nothing and the next one is not due yet".to_string()
        }
        SigningTime::Unreadable(detail) => {
            format!("the archive was read for it and did not answer: {detail}")
        }
        SigningTime::NotAttempted(detail) => format!("no re-read was attempted: {detail}"),
        SigningTime::OverCap(detail) => format!("the document was not re-read: {detail}"),
        // Unreachable through `retrust_with`, which comes here only for a
        // claim still `NotRecorded`, and both arms replace the claim. A
        // sentence rather than a panic: a `detail` is no place to abort.
        SigningTime::Recovered(_) | SigningTime::Absent(_) => {
            "the re-read produced no usable claim".to_string()
        }
    };
    format!(
        "the signature over this document verified under key {matched_key_id}, and the trust \
         policy {policy} has not been applied to it yet (Unverified): this status was written \
         before the controller recorded the document's own signing time, so the key's validity \
         window has not been checked and nothing about the document is in doubt. {why}. The \
         previous verdict is kept, it is not rendered as verified, and the document is re-read \
         on the next policy event"
    )
}

/// The claim a stored verification block carries, with the absence named when
/// it carries none.
///
/// A block with no `signedAt` is one of two things:
///
/// * this installation never recorded the instant (an older build wrote the
///   status). Nobody has read the document: [`ClaimAbsence::NotRecorded`],
///   which [`decide`] answers with an undecided verdict whose contract is one
///   bounded re-read.
/// * the document carries no readable signing time (a build that knows the
///   field read it and found none): [`ClaimAbsence::FieldAbsent`], which fails
///   closed.
///
/// The discriminator is the basis: is this block evidence that a signing time
/// was ever compared to the key's window? Only [`TRUST_BASIS_CURRENT`] and
/// [`TRUST_BASIS_HISTORICAL`] require `claim.signed_at` in [`decide`]'s window
/// rows. Everything else (no `trust` block, no `basis`, `None`, `Unverified`,
/// a later build's spelling) compared nothing and is "not yet asked". These
/// shapes are all `NotRecorded`:
///
/// | shape | written by |
/// |---|---|
/// | no `trust` block at all | a controller predating trust policies |
/// | `trust.basis: Unverified` | this build, waiting on its own re-read |
/// | `trust.basis: None`, no `signedAt` | an intermediate build's re-derivation |
///
/// A document that genuinely carries no signing time is written by a current
/// build as `basis: None` with no `signedAt`, the same bytes as the third
/// shape, so it is re-read once too: [`decide`] refuses it as before, and the
/// completed read records `trust.signingTimeRead: absent` so it is never asked
/// again. One `Store::get_capped` is the only way to tell the two apart.
///
/// The bound is on the object, because a terminal object reconciles every
/// `REQUEUE_SECS`: an attempt that learns nothing records [`RETRY_AFTER_SECS`]
/// in `trust.retryAfter` and bars the next one; see [`signing_time_need`].
fn stored_claim(stored: &Value) -> EvidenceClaim {
    match stored.get("signedAt").and_then(Value::as_str) {
        None if !compared_a_claim(stored) => EvidenceClaim::absent(ClaimAbsence::NotRecorded),
        None => EvidenceClaim::absent(ClaimAbsence::FieldAbsent),
        Some(text) => match DateTime::parse_from_rfc3339(text) {
            Ok(t) => EvidenceClaim::at(t.with_timezone(&Utc)),
            Err(_) => EvidenceClaim::absent(ClaimAbsence::Unparseable),
        },
    }
}

/// Whether a stored block is evidence that a signing time was read out of the
/// document and compared to the key's validity window.
///
/// An allow-list, not a deny-list: [`decide`] reaches [`TRUST_BASIS_CURRENT`]
/// and [`TRUST_BASIS_HISTORICAL`] only through window rows that need
/// `claim.signed_at`, so only those had a real claim. Listing the bases that
/// must re-read would miss the next spelling a later build invents.
///
/// `RecordedBeforeRevocation` is not on the list: it comes from the
/// independent observation, never the claim. Re-reading such an object cannot
/// change its verdict (the compromise rows run first), but fills in the
/// `signedAt` the record should have had, once.
fn compared_a_claim(stored: &Value) -> bool {
    // The read already happened and the document answered: `signingTimeRead`
    // is written only when a completed re-read found no signing time, so the
    // absence is the document's. Without it, such a document would be re-read
    // on every reconcile for ever. `overCap` settles the same way: the store
    // would answer the same size again.
    if matches!(
        stored
            .pointer("/trust/signingTimeRead")
            .and_then(Value::as_str),
        Some(SIGNING_TIME_ABSENT | SIGNING_TIME_OVER_CAP)
    ) {
        return true;
    }
    matches!(
        stored.pointer("/trust/basis").and_then(Value::as_str),
        Some(TRUST_BASIS_CURRENT | TRUST_BASIS_HISTORICAL)
    )
}

/// `trust.signingTimeRead` for a completed re-read whose document carried no
/// signing time.
const SIGNING_TIME_ABSENT: &str = "absent";

/// `trust.signingTimeRead` for a re-read the store answered with a document
/// over the controller's read cap.
pub const SIGNING_TIME_OVER_CAP: &str = "overCap";

/// How long a fruitless attempt bars the next one.
///
/// A terminal object reconciles every `REQUEUE_SECS` (15 s), not only on a
/// policy event, so without this a thousand objects whose archive cannot
/// answer would issue ~67 reads a second for verdicts that cannot change.
/// Fifteen minutes costs one `get` and one small patch per object per quarter
/// hour, and an archive that comes back is noticed within one window.
const RETRY_AFTER_SECS: i64 = 900;

/// What a reconcile should do about this object's missing signing time.
///
/// Three answers, not a `bool`: "nothing to read" re-derives normally, while
/// "inside the backoff" must reproduce the previous block byte for byte or the
/// pass patches on every reconcile.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ReadPlan {
    /// Nothing to read: the block already carries a signing time, a basis that
    /// compared one, a completed read, or no document to read.
    None,
    /// An attempt is owed and due. The caller performs it.
    Read(SigningTimeNeed),
    /// An attempt was made recently and learned nothing; `trust.retryAfter`
    /// has not passed. No read, no `evidence_source` resolution, and the stored
    /// sentence is carried forward unchanged.
    Deferred,
}

/// The `status.evidence` key pairs that name a signed document and the digest
/// the run recorded for it: a `Backup`'s receipt, and a `Restore`'s scorecard.
///
/// The digest is not optional: the bytes that digest to what the status
/// recorded are the bytes whose signature was verified. Without it this would
/// be "believe whatever is in the bucket now", the substitution
/// [`verify_evidence`]'s step 3 catches.
const EVIDENCE_DOCUMENT_FIELDS: [(&str, &str); 2] = [
    ("receiptKey", "receiptSha256"),
    ("scorecardKey", "scorecardSha256"),
];

/// The one document a re-trust pass may re-read, and the digest that makes the
/// read safe.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SigningTimeNeed {
    /// The object key of the signed payload, off `status.evidence`.
    pub payload_key: String,
    /// `sha256:<hex>` as the run recorded it.
    pub payload_sha256: String,
    /// The media type the stored block recorded, so the signing time is read
    /// from the field that type keeps it in.
    pub payload_type: String,
}

/// What one bounded re-read of the document produced.
///
/// "The archive did not answer" and "the document answered and carries no
/// signing time" are different facts: the first keeps the previous verdict on
/// an [`logweir_core::trust::TrustBasis::Unverified`] basis and is retried;
/// the second is the document's own absence and refuses as it always has.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SigningTime {
    /// Nothing was read, because nothing needed to be: the stored block
    /// already carries a `signedAt`, or does not need one.
    NotNeeded,
    /// Nothing was read because the last attempt learned nothing and its
    /// `trust.retryAfter` has not passed. The previous sentence and backoff
    /// are carried verbatim, so the pass writes nothing.
    Deferred,
    /// The document was read, its digest matched, and this is its own claimed
    /// signing time, by [`logweir_core::trust::read_claimed_signing_time`], the
    /// function the fresh path uses.
    Recovered(DateTime<Utc>),
    /// The document was read and its digest matched, and it genuinely carries
    /// no readable signing time. The absence is the document's now.
    Absent(ClaimAbsence),
    /// The archive was reached for and did not answer: no object, a storage
    /// error, or bytes that are not the bytes the run recorded. Retried after
    /// the backoff.
    Unreadable(String),
    /// No read was attempted at all, and why: no evidence credential, or a
    /// destination whose evidence grant only a pod may hold.
    NotAttempted(String),
    /// The store answered with a document larger than the controller's read
    /// cap, so it was not read. A fact about the document, like
    /// [`Self::Absent`]: it will not shrink, so the question is settled
    /// (`trust.signingTimeRead: overCap`, no `retryAfter`). Nothing was
    /// learned, so the stored result is kept on an unverified basis.
    OverCap(String),
}

/// Whether a stored status needs one bounded re-read before its verdict can be
/// re-derived, and what to read.
///
/// All three must hold: a `matchedKeyId` (a signature to judge), a claim that
/// is [`ClaimAbsence::NotRecorded`] (the status predates `signedAt`), and a
/// document key with its recorded digest on `status.evidence`. Without the
/// third nothing is safe to read, and the object stays on its previous
/// verdict with the reason said.
#[must_use]
pub fn signing_time_need(status: Option<&Value>, now: DateTime<Utc>) -> ReadPlan {
    let Some(need) = signing_time_owed(status) else {
        return ReadPlan::None;
    };
    // The backoff is read off the object, not process memory: a controller
    // restart re-lists every object, which is exactly when the bound matters.
    let deferred = stored_verification(status)
        .and_then(|block| deferred_until(block, now))
        .is_some_and(|until| now < until);
    if deferred {
        ReadPlan::Deferred
    } else {
        ReadPlan::Read(need)
    }
}

/// The instant before which no re-read is attempted, clamped to
/// `now + RETRY_AFTER_SECS`.
///
/// `trust.retryAfter` can be hand-written by anyone with `patch` on the status
/// subresource. A far-future value is still fail-closed (a deferred pass
/// re-derives the verdict, cannot hold a `Valid` or render green, and cannot
/// delay a `KeyCompromise` revocation), but "never repaired" is a denial this
/// field must not grant.
///
/// `min(stored, now + RETRY_AFTER_SECS)` would not be a clamp: the ceiling
/// moves with `now`, so a `2099` value would defer for ever in fifteen-minute
/// steps. A value above the ceiling is not one this controller wrote and is
/// honoured as no backoff: the read happens on this pass and rewrites the
/// field. A legitimate value is at or below the ceiling and is honoured
/// unchanged.
///
/// `None` when the block records no backoff, or one this controller would
/// never have written.
fn deferred_until(stored: &Value, now: DateTime<Utc>) -> Option<DateTime<Utc>> {
    let stored_until = stored
        .pointer("/trust/retryAfter")
        .and_then(Value::as_str)
        .and_then(|s| DateTime::parse_from_rfc3339(s).ok())
        .map(|t| t.with_timezone(&Utc))?;
    (stored_until <= now + chrono::Duration::seconds(RETRY_AFTER_SECS)).then_some(stored_until)
}

/// The document a re-read would fetch, ignoring the backoff: the pure half of
/// [`signing_time_need`].
#[must_use]
pub fn signing_time_owed(status: Option<&Value>) -> Option<SigningTimeNeed> {
    let block = stored_verification(status)?;
    block
        .get("matchedKeyId")
        .and_then(Value::as_str)
        .filter(|id| !id.is_empty())?;
    if stored_claim(block).absence != Some(ClaimAbsence::NotRecorded) {
        return None;
    }
    let payload_type = block.get("payloadType").and_then(Value::as_str)?;
    let evidence = status?.get("evidence")?;
    let (key, digest) = EVIDENCE_DOCUMENT_FIELDS.iter().find_map(|(k, d)| {
        let key = evidence
            .get(k)
            .and_then(Value::as_str)
            .filter(|s| !s.is_empty())?;
        let digest = evidence
            .get(d)
            .and_then(Value::as_str)
            .filter(|s| !s.is_empty())?;
        Some((key, digest))
    })?;
    Some(SigningTimeNeed {
        payload_key: key.to_string(),
        payload_sha256: digest.to_string(),
        payload_type: payload_type.to_string(),
    })
}

/// The signing time in `bytes`, after checking them against the digest the
/// status recorded. Pure.
#[must_use]
pub fn signing_time_in(bytes: &[u8], need: &SigningTimeNeed) -> SigningTime {
    let computed = sha256_prefixed(bytes);
    if computed != need.payload_sha256 {
        return SigningTime::Unreadable(format!(
            "digest mismatch at {}: the status records {} and the bytes in the archive are \
             {computed}; no signing time is taken from a document that is not the one this run \
             reported writing",
            need.payload_key, need.payload_sha256
        ));
    }
    // By [`claim_of`], so a receipt is folded and never parsed whole.
    let claim = claim_of(&need.payload_type, bytes);
    match (claim.signed_at, claim.absence) {
        (Some(at), _) => SigningTime::Recovered(at),
        (None, Some(absence)) => SigningTime::Absent(absence),
        (None, None) => SigningTime::Absent(ClaimAbsence::Unparseable),
    }
}

/// One `get` through the read-only handle, then [`signing_time_in`].
///
/// Synchronous: the body of a `spawn_blocking` closure (interface I13). Named
/// in `tests/retention.rs::STORE_CALL_TOKENS` because it holds a
/// `Store::get_capped` while its callers name no `Store` at all.
#[must_use]
pub fn read_signing_time(store: Option<&Store>, need: &SigningTimeNeed) -> SigningTime {
    let Some(store) = store else {
        return SigningTime::NotAttempted(NO_CREDENTIAL_DETAIL.to_string());
    };
    // Under the same cap and budget as `verify_evidence`'s read of the
    // document.
    let _reservation = crate::read_budget::ReadBudget::controller()
        .reserve(crate::read_budget::DOCUMENT_READ_COST_BYTES);
    match store.get_capped(&need.payload_key, controller_cap_for(&need.payload_type)) {
        Ok((bytes, _version)) => signing_time_in(&bytes, need),
        // Over the cap: settled, not retried.
        Err(e @ StoreError::TooLarge { .. }) => SigningTime::OverCap(store_detail(&e)),
        Err(e) => SigningTime::Unreadable(store_detail(&e)),
    }
}

/// The `detail` for a re-read not attempted because the evidence path could
/// not be resolved: a fact about this controller's API connection, so
/// `NotAttempted`, and the verdict is still re-derived on that pass.
#[must_use]
pub fn evidence_path_unreadable(e: &kube::Error) -> String {
    format!(
        "this run's evidence path could not be resolved, so no re-read was attempted: {e}; the \
         verdict is re-derived from the status alone and the read is tried again on the next \
         policy event"
    )
}

/// [`read_signing_time`] on a blocking thread: the seam both reconcilers call.
///
/// `unread` is the evidence path's own refusal when there is no handle to read
/// through (`EvidenceSource::NotAttempted`'s detail), passed through verbatim
/// so the operator reads the sentence the fresh path would have written.
///
/// Exactly one attempt, with no loop: if the archive does not answer, the
/// object keeps its previous verdict on an `Unverified` basis until the
/// backoff passes. A loop would turn one unreachable bucket into a controller
/// that never finishes a reconcile.
pub async fn recover_signing_time(
    handle: Option<std::sync::Arc<Store>>,
    unread: Option<String>,
    need: SigningTimeNeed,
) -> SigningTime {
    if let Some(detail) = unread {
        return SigningTime::NotAttempted(detail);
    }
    let Some(handle) = handle else {
        return SigningTime::NotAttempted(NO_CREDENTIAL_DETAIL.to_string());
    };
    match tokio::task::spawn_blocking(move || read_signing_time(Some(&handle), &need)).await {
        Ok(recovered) => recovered,
        Err(e) => {
            SigningTime::Unreadable(format!("the signing-time re-read did not complete: {e}"))
        }
    }
}

/// One verdict in the recovery catalog's vocabulary, a different set of words
/// for the same two questions.
///
/// `Backup`/`Restore` status carries `result` plus `trust.basis`; a catalog
/// point carries one string per point across a paged view, so the pair is
/// flattened:
///
/// | here | catalog |
/// |---|---|
/// | `Valid`, basis `Current` | `Verified` |
/// | `Valid`, basis `Historical` | `VerifiedHistorical` |
/// | `Valid`, no verdict | `Verified` |
/// | `Valid`, basis `Unverified` | `NotAttempted` |
/// | `Valid`, any other basis ([`ValidBasis::Refused`]) | as `Untrusted` below |
/// | `Untrusted`, `UntrustedSigner` | `UntrustedSigner` |
/// | `Untrusted`, `Revoked`/`RecordedBeforeRevocation` | `Revoked` |
/// | `Untrusted`, any other row | `UntrustedSigner` |
/// | `Invalid` | `Invalid` |
/// | `NotAttempted` | `NotAttempted` |
///
/// `NoEvidence` (a manifest with no receipt at all) is a fact about the
/// archive listing, decided by the catalog worker before it calls this. One
/// function, so a catalog point's verdict and a `Backup`'s badge cannot
/// disagree about the same key.
#[must_use]
pub fn catalog_verification(
    result: VerificationVerdict,
    verdict: Option<&Verdict>,
) -> CatalogVerdict {
    let state = match result {
        VerificationVerdict::Invalid => "Invalid",
        // A pending fetch has read nothing, which is what `NotAttempted` says.
        VerificationVerdict::NotAttempted | VerificationVerdict::Pending => "NotAttempted",
        // The allow-list: no verdict is an absent block; a basis that is not a
        // pass is read by the rows its own word belongs to.
        VerificationVerdict::Valid => {
            match verdict.map_or(ValidBasis::Absent, |v| ValidBasis::of_core(v.basis)) {
                ValidBasis::Absent | ValidBasis::Current => "Verified",
                ValidBasis::Historical => "VerifiedHistorical",
                ValidBasis::Unverified => "NotAttempted",
                ValidBasis::Refused => untrusted_catalog_word(verdict),
            }
        }
        VerificationVerdict::Untrusted => untrusted_catalog_word(verdict),
    };
    CatalogVerdict {
        state,
        reason: verdict.and_then(|v| v.reason),
        basis: verdict.map(|v| v.basis),
    }
}

/// The catalog word for a verdict this installation does not accept:
/// `Revoked` for a revocation row (or a `RecordedBeforeRevocation` basis),
/// `UntrustedSigner` for every other.
fn untrusted_catalog_word(verdict: Option<&Verdict>) -> &'static str {
    match verdict.map(|v| (v.basis, v.reason)) {
        Some((_, Some(UntrustReason::Revoked | UntrustReason::RecordedBeforeRevocation)))
        | Some((TrustBasis::RecordedBeforeRevocation, _)) => "Revoked",
        _ => "UntrustedSigner",
    }
}

/// One catalog point's verdict: the catalog word, and the row that produced
/// it.
///
/// The six words are lossy in a way that changes the remedy. Three rows
/// flatten into `UntrustedSigner` ("signature verifies under a key the policy
/// does not list"), which is false for [`UntrustReason::KeyUsageMismatch`] and
/// [`UntrustReason::SignedOutsideValidity`], where the key is listed; the
/// remedy "add the key as `Retired/EvidenceSigning`" would change nothing.
/// `RecordedBeforeRevocation` flattens to `Revoked`, whose definition says the
/// opposite of what happened. The reason travels with the word so the catalog
/// can write a remedy sentence that is true, without widening its shipped
/// enum.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CatalogVerdict {
    /// The catalog word: `Verified`, `VerifiedHistorical`, `UntrustedSigner`,
    /// `Revoked`, `Invalid` or `NotAttempted`.
    pub state: &'static str,
    /// The row that produced it, when a signer was judged at all.
    pub reason: Option<UntrustReason>,
    /// The basis, so `Verified` and `VerifiedHistorical` stay distinguishable
    /// without re-deriving them from [`Self::state`].
    pub basis: Option<TrustBasis>,
}

/// Whether a stored status carries a verdict a policy change could re-decide.
///
/// The cheap guard in front of the expensive one: [`apply_retrust`] must
/// resolve trust (an API read) before it can call [`retrust`], and an object
/// that never had a signature verified must not pay for it.
#[must_use]
pub fn has_trust_verdict(status: Option<&Value>) -> bool {
    stored_verification(status)
        .and_then(|v| v.get("matchedKeyId"))
        .and_then(Value::as_str)
        .is_some_and(|id| !id.is_empty())
}

/// Re-derive one object's stored verdict against `resolution` and, when it
/// changed, PATCH it: re-evaluation without re-fetching.
///
/// A `/status` merge PATCH carrying exactly `evidence.verification` and the
/// whole `conditions` array, with `metadata.resourceVersion` as a
/// precondition (seam **S7**). An object that changed since the read answers
/// 409, and the next reconcile starts from what it now says: this pass reasons
/// from stored fields, so writing over a concurrent update would reason from
/// fields it never saw.
///
/// `Ok(None)` means nothing changed and nothing was sent: one `kubectl apply`
/// on a policy must not wake every terminal object into a write.
///
/// # Errors
///
/// Any `kube::Error` from the PATCH. A 409 is returned as-is so the caller
/// requeues rather than retrying inside one reconcile.
pub async fn apply_retrust<K>(
    api: &kube::Api<K>,
    object: &K,
    resolution: &Resolution,
    badge: fn(&Value) -> Badge,
    now: DateTime<Utc>,
    signing_time: &SigningTime,
) -> Result<Option<Retrust>, kube::Error>
where
    K: kube::Resource + Clone + serde::de::DeserializeOwned + std::fmt::Debug + serde::Serialize,
    <K as kube::Resource>::DynamicType: Default,
{
    let name = kube::ResourceExt::name_any(object);
    let Ok(value) = serde_json::to_value(object) else {
        return Ok(None);
    };
    let Some(status) = value.get("status") else {
        return Ok(None);
    };
    let conditions: Vec<Condition> = status
        .get("conditions")
        .and_then(|c| serde_json::from_value(c.clone()).ok())
        .unwrap_or_default();
    let Some(result) = retrust_with(
        status,
        resolution,
        badge,
        Some(&conditions),
        object.meta().generation,
        now,
        signing_time,
    ) else {
        return Ok(None);
    };
    // No precondition, no patch: an object without a `resourceVersion` may
    // not be written over blind.
    let Some(resource_version) = object.meta().resource_version.clone() else {
        tracing::warn!(
            object = %name,
            "the object carries no metadata.resourceVersion, so its verdict is not re-derived; \
             seam S7 makes the precondition mandatory and there is nothing to precondition on"
        );
        return Ok(None);
    };
    let mut patch = result.patch(&conditions);
    patch
        .as_object_mut()
        .expect("a status patch is always a JSON object")
        .insert(
            "metadata".to_string(),
            json!({ "name": name, "resourceVersion": resource_version }),
        );
    // The log line says whether a read happened: a log that denies a read
    // that did happen is as wrong as a status that does.
    let how = match signing_time {
        SigningTime::Recovered(_) | SigningTime::Absent(_) => {
            "re-deriving after ONE bounded re-read of the document, which supplied the signing \
             time this status was written without"
        }
        SigningTime::NotNeeded
        | SigningTime::Deferred
        | SigningTime::Unreadable(_)
        | SigningTime::NotAttempted(_)
        | SigningTime::OverCap(_) => {
            "re-deriving from the stored matchedKeyId, signedAt and verifiedAt — no storage \
             read and no signature check"
        }
    };
    tracing::info!(
        object = %name,
        from = %result.from,
        to = %result.to,
        how,
        "the resolved trust policy changed this object's verdict"
    );
    api.patch_status(
        &name,
        &kube::api::PatchParams::default(),
        &kube::api::Patch::Merge(patch),
    )
    .await?;
    Ok(Some(result))
}

/// The objects a `TrustPolicy` event should enqueue: those in the namespaces
/// the event could have changed the resolution of.
///
/// No LIST: `objects` is the controller's own reflector snapshot
/// (`Controller::store()`), the same index as its watch, already warm and no
/// staler than the event, so the trigger costs no API calls.
///
/// It over-approximates on purpose: `scope` is the union of what the policy
/// bound before and after the event ([`crate::trust::PolicyScopeMemory`]), and
/// a `default: true` policy on either side covers every namespace. An extra
/// enqueue costs one re-derivation that writes nothing; a missed one leaves a
/// revoked key green, and a terminal `Restore` parked on
/// `Action::await_change()` is never reconciled otherwise.
///
/// Generic over a `Vec`, so the mapping is testable without a running watch.
#[must_use]
pub fn targets_in_scope<K>(
    objects: Vec<std::sync::Arc<K>>,
    scope: &crate::trust::PolicyScope,
) -> Vec<kube::runtime::reflector::ObjectRef<K>>
where
    K: kube::Resource,
    <K as kube::Resource>::DynamicType: Default + Clone + std::hash::Hash + Eq,
{
    objects
        .into_iter()
        .filter(|object| {
            object
                .meta()
                .namespace
                .as_deref()
                .is_some_and(|ns| scope.covers(ns))
        })
        .map(|object| kube::runtime::reflector::ObjectRef::from_obj(&*object))
        .collect()
}
