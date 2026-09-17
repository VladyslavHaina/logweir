//! The controller's own verification of the evidence the UI renders — and the
//! two green-badge rules, one per kind (interface **I21**).
//!
//! # The whole point in one sentence
//!
//! `weirkeeper` renders a verdict only when a verdict actually happened, which
//! is why [`VerificationVerdict::NotAttempted`] exists as a value distinct
//! from [`VerificationVerdict::Invalid`]: `Invalid` is a claim about the
//! DOCUMENT, and an adopter who declines to give the controller a read-only
//! bucket credential has not produced a bad document — they have produced no
//! answer at all, and the honest badge for that is the literal word
//! `unverified` beside the printed `logweir drill verify` command.
//!
//! # The credential is a FIFTH Secret and a DIFFERENT principal
//!
//! Spec §9, Global Constraint 6 ("a separate bucket and a separate
//! principal"). `logweir-evidence-ro` is projected into the controller's own
//! pod as environment (`config/manager/deployment.yaml`, `optional: true`), so
//! a cluster without it STARTS and every verification here reads
//! `NotAttempted`. It cannot write or delete in any bucket — the handle is
//! built once, in `main`, through the read-only constructor — and since
//! retention only reports (Task 19, guard **G-RET**), **no Logweir component
//! holds any delete capability against object storage in tag 1.**
//!
//! The controller reads that credential from **its own environment and never
//! through the Kubernetes API**: the `weirkeeper` ClusterRole grants no verb
//! on `secrets` (Task 21), and `tests/linkage.rs::the_controller_never_reads_a_secret`
//! keeps it that way.
//!
//! # The rule is TWO rules, one per kind
//!
//! Spec §8 amendment 4, critique C **H1**/**H2**. "Passed" is a different
//! field on each kind, so there is no one badge rule:
//!
//! * **`Backup` is green ⟺ `verification.result == Valid` AND
//!   `status.exitCode == 0`** — [`backup_badge`].
//! * **`Restore` is green ⟺ `verification.result == Valid` AND
//!   `status.outcome == pass`** — [`restore_badge`].
//!
//! **There is no `outcome` on the `Backup` path at all** (spec §3.2, C95):
//! `BackupStatus` carries `exitCode` and no `outcome`, [`VerificationResult`]
//! carries none, and `BackupReceipt` carries `exit_code` and none. Adding one
//! is explicitly not taken, which is what makes the mutant "use one badge rule
//! for both kinds by reading `status.outcome` on a `Backup`" die at assertion
//! time: the field is ALWAYS absent there, so a single rule renders every
//! `Backup` ungreen and
//! `a_backup_is_green_only_on_valid_and_exit_code_zero`'s first fixture fails.
//!
//! Either badge is labelled **"verified by weirkeeper at `<verifiedAt>`
//! against key `<matchedKeyId>`"** and never "verified in your browser";
//! anything else is the literal word [`UNVERIFIED`], never `pass`
//! (`design-operator.md:135-139`). The in-browser WASM verifier is cut from
//! tag 1, and rendering no browser-computed verdict is strictly more honest
//! than a green badge over a verification that did not happen there.
//!
//! # The roster must carry KEY MATERIAL, and that is why `signingKeys[]` exists
//!
//! Critique B **H6**, spec amendment 3. [`verify_evidence`]'s step 4 resolves a
//! signing key from the namespace's resolved trust — a `TrustPolicy`'s
//! `spec.keys[].spkiPem`, or the synthesised `legacy-roster-v1`'s translation
//! of `TrustRoster.spec.signingKeys[].spkiPem`. With a
//! `signingKeyIds: Vec<String>` shape there would be nothing to verify
//! against: every call would return `NotAttempted`,
//! `status.evidence.verification.result` could never be `Valid`, and **this
//! task's own exit criterion would be unsatisfiable.** The shape was fixed at
//! slot 5 (`crds::trust_roster`) rather than retrofitted here.
//!
//! # A SIGNATURE IS NOT A VERDICT — PLAT-19.1, decision D3 §7.4
//!
//! `logweir-verify` answers "did this key make these bytes". That is not the
//! question a badge renders. The second question — *may this installation
//! still trust the key that made them* — is
//! [`logweir_core::trust::decide`]'s, and it is asked here, of the key the
//! signature selected, against the namespace's resolved trust
//! ([`crate::trust::resolve`]).
//!
//! So a fourth result joins the three above: [`VerificationVerdict::Untrusted`]
//! — the bytes are authentic and the signer is one this installation will not
//! accept. It is deliberately NOT `Invalid`: telling an operator their archive
//! is corrupt when their key was revoked sends them to re-run a backup instead
//! of to their trust policy. Every reader that predates it treats anything
//! that is not `Valid` as unverified (`ui/pages/backups.js`), so the new value
//! fails closed on every old surface.
//!
//! Three inputs decide it and each is a different kind of fact:
//!
//! * the **key**, as the resolved policy carries it (state, window, usages);
//! * the document's own **claim** about when it was signed
//!   ([`logweir_core::trust::claimed_signing_time`]) — attacker-controlled for
//!   a compromised key, which is why the compromise rows never read it;
//! * the **independent observation**, which is this controller's own
//!   `status.evidence.verification.verifiedAt` from an earlier reconcile and
//!   nothing else.
//!
//! The observation is why [`VerificationResult::to_status_value`] performs the
//! trust decision rather than [`verify_evidence`]: the stored instant lives on
//! the status and only the projection sees it. It is also why a trust-only
//! change **does not move `verifiedAt`** — see that method.
//!
//! # Interface I13 — `Store` is blocking, so nothing here is `async`
//!
//! Every [`Store`] method drives its own current-thread runtime, and `kube`
//! drives every reconciler ON a runtime, so a direct call COMPILES CLEANLY and
//! panics with *Cannot start a runtime from within a runtime* at the first
//! reconcile. [`verify_evidence`] is therefore a plain synchronous function —
//! the body of a `tokio::task::spawn_blocking` closure, never an `async fn` —
//! over the shared `Arc<Store>` built once at controller start (Task 19).
//! `tests/retention.rs::no_store_call_is_made_outside_spawn_blocking` names
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
    decide, EvidenceClaim, IndependentObservation, KeyUsage, TrustBasis, TrustResult, TrustedKey,
    UntrustReason, Verdict,
};
use logweir_store::{Store, StoreError};
use logweir_verify::{verify_detached, Sidecar, VerifyingKey};
use serde_json::{json, Value};

use crate::conditions::{
    current_condition, merge_condition, CONDITION_VERIFIED, REASON_EXIT_CODE_NOT_ZERO,
    REASON_OUTCOME_NOT_PASS, REASON_VERIFICATION_INVALID, REASON_VERIFICATION_NOT_ATTEMPTED,
    REASON_VERIFICATION_UNTRUSTED, REASON_VERIFIED,
};
use crate::crds::Condition;
use crate::trust::{Resolution, ResolvedTrust, TrustSource};

/// The `detail` on a `NotAttempted` produced because the controller holds no
/// evidence credential at all.
///
/// A SENTENCE THAT NAMES THE WAY OUT. Spec §9's "documented switch": an
/// adopter who will not give the control plane bucket access is not broken,
/// they are running with verification display off, and the CLI command that
/// answers the same question is the one printed here.
pub const NO_CREDENTIAL_DETAIL: &str =
    "no evidence credential is configured; run the printed logweir drill verify command instead";

/// The `detail` on a `NotAttempted` produced by an empty
/// `TrustRoster.spec.signingKeys`.
///
/// **VERBATIM, AND IT NAMES ITSELF.** This is the sentence
/// `an_empty_signing_key_list_names_itself` asserts. A roster with no signing
/// key material is the one configuration under which no evidence in the
/// cluster can ever verify, and a silent `Invalid` would blame the documents
/// for it.
pub const NO_SIGNING_KEYS_DETAIL: &str = "the TrustRoster lists no signing key material; add the \
                                          runner's public key to spec.signingKeys";

/// The `detail` on a `NotAttempted` produced by a bound `TrustPolicy` that
/// carries no `EvidenceSigning` key at all.
///
/// DISTINCT FROM [`NO_SIGNING_KEYS_DETAIL`], and it has to be: an operator
/// whose namespace is governed by a `TrustPolicy` does not have a
/// `spec.signingKeys` to add anything to, and sending them to edit the roster
/// would send them to an object that no longer decides anything for their
/// namespace (D3 §7.2).
pub const NO_POLICY_KEYS_DETAIL: &str =
    "the TrustPolicy bound to this namespace lists no key with usage EvidenceSigning; add the \
     runner's public key to spec.keys";

/// The `detail` on a `NotAttempted` produced because two `TrustPolicy` objects
/// claim this namespace (D3 §7.1).
///
/// IT CARRIES THE MACHINE TOKEN [`crate::trust::REASON_TRUST_POLICY_CONFLICT`]
/// so the same word an operator greps on the policies' `status.conflicts` and
/// on a refused `Approval` is greppable here too.
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
/// **NEVER `pass`**, and never "verified in your browser"
/// (`design-operator.md:135-139`). `pass` is a `Restore`'s own `outcome`
/// value; reusing it for "we did not check" is how a UI comes to show the
/// happy word over an unchecked document.
pub const UNVERIFIED: &str = "unverified";

/// What the controller made of one piece of signed evidence.
///
/// `Valid` and `Invalid` are claims about the DOCUMENT; `NotAttempted` is a
/// claim about the CONTROLLER. Keeping them apart is the whole reason this
/// enum has three variants rather than a `bool` — see this module's header.
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
    /// A claim about the SIGNER, and the fourth value D3 §7.4 adds. The bytes
    /// are authentic — a key made this signature over exactly these bytes —
    /// and the key is one this installation will not accept: unknown to the
    /// resolved policy, revoked, carrying the wrong usage, or used outside the
    /// window it was trusted in.
    ///
    /// **NEVER `Invalid`.** `Invalid` says the document is not what it claims
    /// to be; `Untrusted` says it is exactly what it claims to be and that is
    /// the problem. An operator acts on their trust policy for one and on
    /// their archive for the other.
    Untrusted,
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
        }
    }
}

impl fmt::Display for VerificationVerdict {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Everything a trust verdict needs about ONE verified signature, carried from
/// the moment the signature matched to the moment the status is written.
///
/// # Why the key travels as [`TrustedKey`] and not as a key id
///
/// Because the answer must not depend on a second lookup. The signature
/// selected one entry of one resolved policy; re-resolving by id at projection
/// time would open a window in which the policy changed between the two reads
/// and the verdict described a key the signature never matched. The lifecycle
/// facts are small, owned and pure — see [`logweir_core::trust::TrustedKey`],
/// which deliberately carries no PEM.
///
/// `key` is `None` for a signature that verified under a key the resolved
/// policy does not carry. That cannot happen on the fresh path — the key came
/// FROM the policy — but it is the ordinary case on the re-trust path
/// ([`retrust`]), where the stored `matchedKeyId` is looked up in a policy that
/// has since changed, and in a namespace whose trust now resolves elsewhere.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TrustProjection {
    /// The matched key's lifecycle, as the resolved policy carries it.
    pub key: Option<TrustedKey>,
    /// What the DOCUMENT says about when it was signed.
    pub claim: EvidenceClaim,
    /// Which policy answered, with the revision that answered.
    pub policy: crate::crds::PolicyRef,
}

impl TrustProjection {
    /// The verdict this projection reaches, given one observation and a clock.
    ///
    /// `usage` is always [`KeyUsage::EvidenceSigning`] here; it is spelled at
    /// the call site rather than baked in so the usage-separation refusal is
    /// visible in the source that performs it (D3 §7.3).
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
        // THE SYNTHESISED POLICY HAS NO UID AND NO GENERATION, and inventing
        // either would make a badge name an object an operator could go and
        // look at. The name is the one thing that is true: D3 §7.5's
        // `legacy-roster-v1`, which L10 asserts appears in the condition
        // message as `trustSource=legacy-roster-v1`.
        TrustSource::LegacyRoster => crate::crds::PolicyRef {
            name: Some(crate::trust::LEGACY_POLICY_NAME.to_string()),
            uid: None,
            generation: None,
        },
    }
}

/// One verification, as the controller performed it.
///
/// THE SHAPE IS THE BRIEF'S, VERBATIM. `detail` carries the `Error`'s
/// `Display` on `Invalid` and the reason on `NotAttempted`, and is `None` on
/// `Valid` — there is nothing to explain about an answer that came out yes.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct VerificationResult {
    /// `Valid`, `Invalid` or `NotAttempted`.
    pub result: VerificationVerdict,
    /// The `keyId` of the `TrustRoster` signing key that verified the
    /// signature. **The ROSTER's declared id**, not the sidecar's: the roster
    /// is what an operator edits and what `status.matchedKeyId` has to be
    /// greppable against.
    pub matched_key_id: Option<String>,
    /// The DSSE `payloadType` that was verified, echoed from the caller.
    pub payload_type: String,
    /// When this verification happened.
    pub verified_at: DateTime<Utc>,
    /// The `Error`'s `Display` on `Invalid`; the reason on `NotAttempted`; the
    /// refused trust row on `Untrusted`.
    pub detail: Option<String>,
    /// What the signature's own key looked like in the resolved policy —
    /// `None` when no signature matched, so there is no key to have an opinion
    /// about (PLAT-19.1, D3 §7.4).
    pub trust: Option<TrustProjection>,
}

impl VerificationResult {
    /// A `NotAttempted` carrying `detail`.
    ///
    /// `pub` because the reasons a verification is not attempted are not all
    /// inside [`verify_evidence`]: unreadable trust and a blocking task that
    /// panicked are both decided by [`verify_oracle`], and both are
    /// `NotAttempted` for exactly the reason this module's header gives.
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

    /// An `Invalid` carrying `detail`.
    fn invalid(payload_type: &str, detail: impl Into<String>) -> Self {
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
    /// # Why `result` is decided here with NO observation
    ///
    /// So a caller that never reaches [`Self::to_status_value`] — the log line
    /// in both reconcilers, and every consumer of the oracle — still reads the
    /// right word. The independent observation is only read on D3 §7.4's two
    /// compromise rows and **both of them are `Untrusted`**, so adding it later
    /// can refine `basis` and `reason` and can never change `Valid` into
    /// `Untrusted` or back. `the_observation_only_moves_the_basis` asserts that
    /// over the whole table rather than trusting this paragraph.
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
    /// `verifiedAt` a controller of THIS installation wrote on an earlier
    /// reconcile, and nothing else.
    ///
    /// **NEVER THE DOCUMENT'S OWN CLAIM.** D3 §7.4's compromise rule turns on
    /// the two being different things, which is why
    /// [`logweir_core::trust::IndependentObservation`] and
    /// [`logweir_core::trust::EvidenceClaim`] are distinct types although both
    /// wrap one optional instant.
    #[must_use]
    pub fn observation(existing: Option<&Value>) -> IndependentObservation {
        match existing
            .and_then(|e| e.get("verifiedAt"))
            .and_then(Value::as_str)
            .and_then(|s| DateTime::parse_from_rfc3339(s).ok())
        {
            Some(t) => IndependentObservation::at(t.with_timezone(&Utc)),
            None => IndependentObservation::none(),
        }
    }

    /// This result as the `status.evidence.verification` object, **reusing
    /// `existing`'s `verifiedAt` when nothing but the clock has moved.**
    ///
    /// # Why the timestamp is not simply `self.verified_at`
    ///
    /// Plan erratum **E11(d)**, and it is the difference between a quiet
    /// controller and 12,000 reconciles in 90 s. A reconciler's own status
    /// patch is what wakes it, so a field that carries a fresh clock read on
    /// every pass makes every pass a write and every write a wake-up. Task
    /// 16b's contract (`conditions::status_unchanged`,
    /// `patch_status_if_changed`) skips a patch that would change nothing —
    /// and it can only do that if a verdict that has not changed renders the
    /// same bytes.
    ///
    /// So `verifiedAt` is **when this verdict was reached**, not when it was
    /// last re-confirmed. A change in `result`, `matchedKeyId`, `payloadType`
    /// or `detail` is a new verdict and takes the new clock; anything else
    /// keeps the stored one.
    ///
    /// # …AND A TRUST-ONLY CHANGE DOES NOT MOVE IT EITHER
    ///
    /// PLAT-19.1, and it is not a refinement — it is the difference between a
    /// verdict and a coin flip. `verifiedAt` is the ONE independent
    /// observation D3 §7.4 accepts about when this installation saw the
    /// document. If a revocation downgraded the verdict AND refreshed
    /// `verifiedAt`, the new instant would be after `revocationEffectiveFrom`,
    /// the next pass would read it as "observed after the revocation", and the
    /// verdict would flip from `RecordedBeforeRevocation` to `Revoked` and
    /// stay there — the controller would have destroyed its own evidence by
    /// recording its own conclusion. §7.4 says the re-trust patch writes
    /// `result`, `trust` and `detail` and nothing else; this is that rule,
    /// enforced on the fresh path too.
    ///
    /// So the identity that decides whether the instant moves is the
    /// **signature**, not the verdict: the key that made it and the media type
    /// it was made over. A verification that matched no key keeps today's
    /// comparison exactly, because there is no trust layer over it to move.
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

        // THE SIGNATURE'S IDENTITY, NOT THE VERDICT'S — see the note above.
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

/// The `detail` for one trust verdict — `None` on a `Valid`, and a sentence
/// naming the refused row otherwise.
///
/// EVERY SENTENCE NAMES THE KEY AND THE POLICY, because "untrusted" with
/// neither is a dead end: an operator has to know WHICH key this installation
/// refused and WHICH object to edit. It never carries key material — the id,
/// the policy name and a fixed sentence, exactly like the rest of `detail`.
///
/// [`UntrustReason::SignedOutsideValidity`] carries a second clause when the
/// document claimed no signing time at all, because those are two very
/// different faults reported by one row: "signed after the key was retired" and
/// "this scorecard records no phase, so there was nothing to compare".
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
    Some(head + &tail)
}

/// Read the object with the read-only evidence handle, check the digest the
/// status recorded, then verify the DSSE sidecar against the resolved trust's
/// signing key material — and then ask whether that key is still trusted.
///
/// Returns `NotAttempted` (never `Invalid`) when no evidence credential is
/// configured.
///
/// # The four steps, in order, and why each verdict is the one it is
///
/// 1. **No `store`** → `NotAttempted` with [`NO_CREDENTIAL_DETAIL`]. The
///    controller was not given a credential; that is a fact about the install,
///    not about the document.
/// 2. **`get` the payload and the sidecar** through the read-only handle.
///    `StoreError::NotFound` and **every other** `StoreError` map to
///    `NotAttempted`, the latter carrying the error's `Display`. A storage
///    failure is never `Invalid`, because `Invalid` is a claim about the
///    document and an object that could not be fetched has made no claim.
/// 3. **Recompute `sha256_prefixed(payload)`** and compare it against the
///    digest the status recorded. A mismatch **is** `Invalid`, and its
///    `detail` names both digests: the bytes in the bucket are not the bytes
///    this run reported writing, which is precisely the substitution a digest
///    on the status exists to catch. Skipping this and verifying the signature
///    alone would accept a genuinely-signed OLDER document in place of this
///    one.
/// 4. **For each resolved key carrying [`KeyUsage::EvidenceSigning`]**, build
///    [`VerifyingKey::from_pem_str`] over its `spkiPem` and call
///    [`verify_detached`]. The FIRST `Ok` selects that entry's own `keyId`;
///    otherwise `Invalid` with the LAST error's `Display`. An **empty** list is
///    `NotAttempted` with [`NO_SIGNING_KEYS_DETAIL`] (legacy) or
///    [`NO_POLICY_KEYS_DETAIL`] — it names itself rather than failing silently,
///    and returning `Invalid` there would blame every document in the cluster
///    for a missing line in one cluster-scoped object.
/// 5. **The trust question**, PLAT-19.1: the selected key's lifecycle against
///    the document's own claimed signing time
///    ([`logweir_core::trust::claimed_signing_time`]). A key that is retired,
///    expired, revoked, not yet open or carrying the wrong usage produces
///    [`VerificationVerdict::Untrusted`] — never `Invalid`, because the bytes
///    are exactly what they claim to be.
///
/// # THE USAGE FILTER IS STEP 4's, NOT AN AFTERTHOUGHT (D3 §7.3)
///
/// A key declared for `GovernedApproval` or `ConsoleConfirmation` is not
/// offered to [`verify_detached`] at all, so it cannot become a
/// `matchedKeyId` by luck. [`logweir_core::trust::decide`] refuses the usage a
/// second time from the other side, which is why a key that reached this far
/// with the wrong usage is [`UntrustReason::KeyUsageMismatch`] rather than a
/// verification that quietly succeeded.
///
/// # WHY THE PARSE FAILURE IS SKIPPED AND THE ID MISMATCH IS NOT CHECKED
///
/// Both are today's behaviour and D3 §7.5 requires it byte-for-byte: this
/// function has always SKIPPED an unparseable signing entry and kept trying
/// the rest (unlike `controllers::approval`, which refuses everything), and it
/// has never compared an entry's declared `keyId` with the hash of its own
/// material. The declared id is reported as `matchedKeyId` because that is the
/// string an operator greps for in the object they edit. A `TrustPolicy` whose
/// key id disagrees with its material is already `Loaded=False/KeyIdMismatch`
/// on its own status, which is where that fault belongs.
///
/// # Interface I13
///
/// Synchronous on purpose: this is the body of a `spawn_blocking` closure. See
/// this module's header.
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

    // STEP 2. Both objects, through the read-only handle. EVERY StoreError is
    // NotAttempted — `NotFound` and the rest alike.
    let payload = match store.get(payload_key) {
        Ok((bytes, _version)) => bytes,
        Err(e) => return VerificationResult::not_attempted(payload_type, store_detail(&e)),
    };
    let sidecar_bytes = match store.get(sidecar_key) {
        Ok((bytes, _version)) => bytes,
        Err(e) => return VerificationResult::not_attempted(payload_type, store_detail(&e)),
    };

    // STEP 3. The digest the status recorded, against the bytes in the bucket
    // right now. A MISMATCH IS `Invalid`.
    let computed = sha256_prefixed(&payload);
    if computed != payload_sha256 {
        return VerificationResult::invalid(
            payload_type,
            format!(
                "digest mismatch at {payload_key}: the status records {payload_sha256} and the \
                 bytes in the archive are {computed}"
            ),
        );
    }

    // The sidecar has to parse before any key can be tried against it. A
    // sidecar that is not JSON is a fact about the OBJECT, so this is
    // `Invalid`: the bytes were fetched, they are the evidence this run named,
    // and they are not a DSSE sidecar.
    let sidecar: Sidecar = match serde_json::from_slice(&sidecar_bytes) {
        Ok(s) => s,
        Err(e) => {
            return VerificationResult::invalid(
                payload_type,
                format!("{sidecar_key} is not a DSSE sidecar: {e}"),
            )
        }
    };

    // STEP 4. The resolved trust's EvidenceSigning key material — interface
    // I17, now resolved per namespace (D3 §7.1). An EMPTY list names itself
    // and is never `Invalid`.
    //
    // A WHOLE-USAGE BLOCK IS ALSO `NotAttempted`. The synthesised legacy
    // policy carries one ("a partially loaded roster is not a roster"), and it
    // is a fact about the trust object rather than about this document.
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
        match verify_detached(&key, payload_type, &payload, &sidecar) {
            // THE POLICY'S OWN `keyId`, not the one `verify_detached` returned
            // out of the sidecar. They are the same string whenever the policy
            // declares its ids correctly, and when they are not, the id an
            // operator can act on is the one written in the object they edit.
            Ok(_sidecar_key_id) => {
                // THE CLAIM COMES OUT OF THE DOCUMENT THAT JUST VERIFIED, and
                // that ordering is the point: bytes whose signature has not
                // been checked have no claim worth reading, and a claim read
                // before the digest comparison would be the SUBSTITUTED
                // document's claim.
                let claim = match serde_json::from_slice::<Value>(&payload) {
                    Ok(json) => EvidenceClaim::from_document(payload_type, &json),
                    // The signature verified over bytes that are not JSON at
                    // all — possible only for a payload type this build does
                    // not model. Fails closed with the reason named.
                    Err(_) => EvidenceClaim::absent(logweir_core::trust::ClaimAbsence::Unparseable),
                };
                return VerificationResult::signed(
                    payload_type,
                    entry.trust.key_id.clone(),
                    TrustProjection {
                        key: Some(entry.trust.clone()),
                        claim,
                        policy: policy_ref(&trust.source),
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

/// [`verify_evidence`], with the namespace's whole [`Resolution`] in front of
/// it.
///
/// THE TWO NON-TRUST RESOLUTIONS ARE VERDICTS ABOUT THE CLUSTER, not about the
/// document, so both are `NotAttempted`:
///
/// * [`Resolution::Conflict`] — two policies claim this namespace, so it
///   resolves to NOTHING (D3 §7.1) and the detail carries
///   [`crate::trust::REASON_TRUST_POLICY_CONFLICT`]. Refusing here rather than
///   picking one is the whole content of that rule; an `Untrusted` would blame
///   a signer for an administrators' disagreement.
/// * [`Resolution::Unconfigured`] — no policy and no roster, which is today's
///   answer through `controllers::roster_spec`'s empty-roster flattening and
///   carries [`NO_SIGNING_KEYS_DETAIL`] **byte-for-byte** so an unmigrated
///   cluster reads exactly what it reads now.
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

/// A `StoreError` as the `detail` of a `NotAttempted`.
///
/// `NotFound` gets its own sentence because "the object is not there" and "the
/// bucket would not answer" are different things for an operator to act on,
/// and both are `NotAttempted`.
fn store_detail(e: &StoreError) -> String {
    match e {
        StoreError::NotFound(key) => {
            format!("the evidence object {key} is not in the archive; nothing was verified")
        }
        other => format!("the evidence object could not be read: {other}"),
    }
}

// ---------------------------------------------------------------------------
// Interface I21 — the two green-badge rules, one per kind
// ---------------------------------------------------------------------------

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
    /// [`REASON_VERIFICATION_NOT_ATTEMPTED`], [`REASON_EXIT_CODE_NOT_ZERO`] or
    /// [`REASON_OUTCOME_NOT_PASS`] otherwise.
    pub reason: &'static str,
    /// "verified by weirkeeper at `<verifiedAt>` against key `<matchedKeyId>`"
    /// when green, and the literal [`UNVERIFIED`] in every other case.
    pub label: String,
}

impl Badge {
    fn green(verified_at: &str, matched_key_id: &str, historical: bool) -> Self {
        let mut label =
            format!("verified by weirkeeper at {verified_at} against key {matched_key_id}");
        // D3 §7.4: "a `Historical` badge carries 'verified against retired key
        // `<id>` (signed before retirement)'". IT IS A PASS AND NOT A WARNING —
        // a key that signed while valid and has since been retired is what
        // rotation is supposed to look like (§7.6), so the qualifier explains
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
/// A `Valid` with no `matchedKeyId` or no `verifiedAt` is not a verdict
/// [`verify_evidence`] ever produces — both are set on every `Valid` — so a
/// block shaped like that is treated as no verdict at all rather than as a
/// green badge whose label cannot be rendered.
///
/// # The basis is checked as well, and that is DEFENCE IN DEPTH
///
/// D3 §7.4 states the green rule as `Valid ∧ (basis Current|Historical) ∧ run
/// success`. [`logweir_core::trust::decide`] only ever produces `Valid` on
/// those two bases, so the second clause is redundant **today** — and it is
/// written out anyway, for the same reason `Verdict::may_render_green`
/// excludes `RecordedBeforeRevocation` twice over: a badge rule that reads one
/// field is one edit away from rendering green over a basis nobody intended,
/// and this one is read by the surface an operator actually looks at.
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
    let basis = v
        .and_then(|v| v.pointer("/trust/basis"))
        .and_then(Value::as_str);
    let historical = match basis {
        Some(TRUST_BASIS_CURRENT) | None => false,
        Some(TRUST_BASIS_HISTORICAL) => true,
        // A `Valid` on any other basis is a shape this controller does not
        // write. It is not green.
        Some(_) => return Err(REASON_VERIFICATION_UNTRUSTED),
    };
    Ok((at, key, historical))
}

/// `trust.basis` for a verdict reached against a key that was current.
const TRUST_BASIS_CURRENT: &str = "Current";
/// `trust.basis` for a verdict reached against a key that has since been
/// retired, expired or superseded.
const TRUST_BASIS_HISTORICAL: &str = "Historical";

/// **Interface I21, the `Backup` half.** Green ⟺ `verification.result ==
/// Valid` AND `status.exitCode == 0`.
///
/// IT NEVER READS `outcome`, AND THERE IS NONE TO READ. `BackupStatus` carries
/// `exitCode` and no `outcome` (spec §3.2, C95). A rule that read `outcome`
/// here would find the field absent on every `Backup` ever written and render
/// every one of them ungreen — which is what
/// `a_backup_is_green_only_on_valid_and_exit_code_zero` fails on.
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
/// Valid` AND `status.outcome == pass`.
///
/// `outcome` is the scorecard's own string, copied onto the status by Task
/// 20's reconciler and never re-derived — `pass`, `fail-objective`,
/// `fail-integrity` or `preflight-failed` in the frozen 1.0.0 enum. Only
/// `pass` is green: a `fail-integrity` run produced a perfectly valid signed
/// document SAYING THE RESTORE DID NOT RECONCILE, and a green badge over it
/// would invert the most valuable thing the tool reports.
#[must_use]
pub fn restore_badge(status: &Value) -> Badge {
    let (at, key, historical) = match valid_verification(status) {
        Ok(triple) => triple,
        Err(reason) => return Badge::not_green(reason),
    };
    if status.get("outcome").and_then(Value::as_str) != Some("pass") {
        return Badge::not_green(REASON_OUTCOME_NOT_PASS);
    }
    Badge::green(at, key, historical)
}

// ---------------------------------------------------------------------------
// The SECOND patch
// ---------------------------------------------------------------------------

/// The `Verified` condition for `badge`, with the `metav1.Condition`
/// `lastTransitionTime` contract applied.
///
/// `existing` is the object's current `Verified` condition, so a badge that
/// has not changed keeps the instant it changed at (Task 16b,
/// `conditions::merge_condition`).
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

/// The SECOND `/status` merge patch: `status.evidence.verification` plus the
/// full condition list with `Verified` merged into it.
///
/// # Why it is a second, separate patch and not one more key on the first
///
/// So a verification failure can never prevent the exit code from being
/// recorded. The terminal patch lands first and on its own; this one follows.
/// A 500 on this patch leaves the exit code, the evidence keys and the
/// scorecard-derived fields exactly where the first patch put them — which is
/// what `verification_is_a_second_patch_after_the_status_patch` asserts by
/// answering the second route with a 500 and reading the object back.
/// Folding the two together makes the mutant's own failure mode the proof: the
/// 500 then loses the exit code too.
///
/// # Why it carries the WHOLE condition list
///
/// A JSON merge patch REPLACES arrays (RFC 7386;
/// `conditions::apply_merge_patch` is the API server's half written out). A
/// second patch carrying only `[Verified]` would therefore DELETE the terminal
/// `Complete`/`Failed` condition the first patch just wrote. `terminal` is the
/// condition list that first patch carried — taken from the patch value
/// itself, because the in-memory object this reconcile holds was read BEFORE
/// that patch and no longer says what the object says.
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
/// Reads the patch VALUE rather than the object, for the reason
/// [`second_patch`] documents: after the first PATCH the in-memory object is
/// stale, and the only exact record of what was written is the thing that was
/// written. An empty list is the truthful answer for a patch that carried no
/// `conditions` key at all.
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

// ---------------------------------------------------------------------------
// The oracle — interface I13's shape, as the two reconcilers take it
// ---------------------------------------------------------------------------

/// What one verification names: the two objects, the digest the status
/// recorded, and the media type that binds them.
///
/// BY VALUE, because the future outlives the call: everything the
/// `spawn_blocking` closure reads has to be owned by it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EvidenceRef {
    /// The namespace of the object this evidence belongs to — **the one input
    /// trust resolution cannot derive** (PLAT-19.1, D3 §7.1).
    ///
    /// A NAMESPACE NEVER NAMES ITS OWN TRUST; the policy names the namespaces
    /// it governs. So this is not a policy name an object supplied — it is
    /// `metadata.namespace`, read by the reconciler off the object it is
    /// reconciling, and `crate::trust::resolve` decides from it. A roster whose
    /// name the subject supplies is a roster the subject can choose; a
    /// namespace is not a choice the subject makes at verification time.
    pub namespace: String,
    /// The object key of the signed payload — a backup receipt or a scorecard.
    pub payload_key: String,
    /// `sha256:<hex>` as the status recorded it, from the bytes the run wrote.
    pub payload_sha256: String,
    /// The object key of the detached DSSE sidecar.
    pub sidecar_key: String,
    /// `logweir_verify::PAYLOAD_TYPE_BACKUP_RECEIPT` (interface **I3**) or
    /// `PAYLOAD_TYPE_SCORECARD` — never a string a reconcile arm invented.
    pub payload_type: &'static str,
}

/// The verification half of a reconciler, injected.
///
/// # Why an oracle and not a `Store` on the reconcile
///
/// The same argument `controllers::backup::ArchiveOracle` makes, for the same
/// interface. `Store` is blocking (**I13**) and `kube` drives reconcilers on a
/// runtime, so the real implementation is a `spawn_blocking` closure over the
/// shared `Arc<Store>`; writing the reconcile against an injected oracle is
/// what lets `verification_is_a_second_patch_after_the_status_patch` assert the
/// route ORDER over `mock_client` with no bucket anywhere.
///
/// `BoxFuture<'static, …>` and not `BoxFuture<'a, …>`: an `Fn`'s `Output` is an
/// associated type matched exactly, so a future borrowed for `'a` makes `'a`
/// the lifetime of both the future and the `&'a dyn`, and the borrow checker
/// then asks for a `'static` local. The future owns everything it reads.
pub type VerifyOracle<'a> = &'a (dyn Fn(EvidenceRef) -> futures::future::BoxFuture<'static, VerificationResult>
         + Send
         + Sync
         + 'a);

/// The oracle for a controller that holds no evidence credential: it attempts
/// nothing and says so.
///
/// This is what both reconcilers use when `Context::archive` is `None` — a
/// controller started without `logweir-evidence-ro` — and what every unit test
/// that is not about verification passes. The verdict is `NotAttempted` with
/// [`NO_CREDENTIAL_DETAIL`], **never** `Invalid`: see this module's header.
#[must_use]
pub fn unverified_evidence(
    r: EvidenceRef,
) -> futures::future::BoxFuture<'static, VerificationResult> {
    Box::pin(async move { VerificationResult::not_attempted(r.payload_type, NO_CREDENTIAL_DETAIL) })
}

/// The `detail` on a `NotAttempted` produced because the cluster-scoped
/// `TrustRoster` could not be read at all.
///
/// DISTINCT FROM [`NO_SIGNING_KEYS_DETAIL`], which is about a roster that IS
/// readable and lists nothing. An API failure is not an empty list, and an
/// operator who is told to "add the runner's public key" when the real problem
/// is a 503 will edit an object that was already correct.
pub const ROSTER_UNREADABLE_DETAIL: &str =
    "this namespace's trust could not be resolved (the TrustPolicy list or the cluster-scoped \
     TrustRoster `default` could not be read); nothing was verified";

/// The real verification oracle: the controller's read-only evidence handle,
/// the cluster-scoped `TrustRoster`, and [`verify_evidence`] on a blocking
/// thread.
///
/// # Interface I13, and it is not decoration
///
/// `Store::get` drives its own current-thread runtime and `kube` drives every
/// reconciler ON a runtime, so calling [`verify_evidence`] straight from the
/// `async move` block below COMPILES CLEANLY and panics with *Cannot start a
/// runtime from within a runtime* at the first verification.
/// `tests/retention.rs::no_store_call_is_made_outside_spawn_blocking` names
/// `verify_evidence(` in its `STORE_CALL_TOKENS` for exactly that reason: this
/// call site holds a `Store::get` while naming no `Store` at all, which is the
/// same blindness the Task 20 review found in `observe_archive(` and
/// `observe_scorecard(`.
///
/// # THREE WAYS TO REACH `NotAttempted` BEFORE THE DOCUMENT IS EVEN FETCHED
///
/// No archive handle, an unreadable roster, and a blocking task that did not
/// come back. **None of them is `Invalid`**: each is a fact about this
/// controller, and `Invalid` is a claim about the document.
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
            // PLAT-19.1: the NAMESPACE's trust, not one cluster-scoped
            // roster. `resolve` lists every `TrustPolicy` and falls back to
            // the synthesised `legacy-roster-v1`, so an unmigrated cluster
            // reaches exactly the key set it reaches today.
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
            // ONE `spawn_blocking`, TWO `get`s — interface I13.
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
                    format!("the verification task did not complete: {e}"),
                ),
            }
        })
    }
}

/// `conditions` with the object's existing `Verified` condition carried
/// forward — **the fix for a hot loop, measured on a live cluster.**
///
/// # What went wrong, in one sentence
///
/// A JSON merge patch REPLACES arrays (RFC 7386), so a terminal `/status`
/// patch writing `conditions: [Complete, EvidenceRecorded]` DELETES the
/// `Verified` condition [`second_patch`] added on the previous pass — the
/// second patch then re-adds it, the write wakes the reconciler, and the two
/// patches fight forever.
///
/// **MEASURED** on the Phase B run that otherwise passed: 20 `Backup`
/// reconciles and 20 `Restore` reconciles **per second**, each one a real
/// write, in a controller whose whole condition contract (Task 16b, plan
/// erratum **E11(d)**) exists to make a steady object issue zero patches. The
/// verdict was correct on every pass; the object never stopped being rewritten.
///
/// So every terminal patch builder calls this. `Verified` is not the run's
/// fact — it is the CONTROLLER's fact about the run, computed after the
/// terminal write — and a builder that owns the array has to carry the parts
/// of it that are not its own. `a_verified_object_reconciles_without_a_patch`
/// is the regression, and it asserts the count is ZERO rather than small.
#[must_use]
pub fn carry_verified(existing: Option<&Vec<Condition>>, conditions: Vec<Value>) -> Vec<Value> {
    carry_conditions(existing, conditions, &[CONDITION_VERIFIED])
}

/// [`carry_verified`], for any set of condition types a patch builder does not
/// own — D3 §2.2's `carry_conditions(&[Verified, RunnerReady])`.
///
/// # Why the general form exists
///
/// Because the hot loop [`carry_verified`] documents is not a property of the
/// `Verified` condition; it is a property of **JSON merge patch replacing
/// arrays**. Every condition written by one pass and not owned by the builder
/// of the next is a candidate for the same fight, and D3 §2.2 adds a second one
/// (`RunnerReady`, written by the progress path and not by the terminal
/// builder). Writing the rule once, over a list, is what keeps the third from
/// having to rediscover it.
///
/// ORDER IS PART OF THE CONTRACT: the builder's own conditions first, then each
/// carried type in the order given. A steady object must compute the same array
/// on every pass or `conditions::status_unchanged` cannot skip the patch, and a
/// set-based implementation would not guarantee that.
///
/// A type the builder already wrote is never carried — the builder's own answer
/// about a condition it owns wins over the stored one, which is what makes a
/// verdict that CHANGED land.
#[must_use]
pub fn carry_conditions(
    existing: Option<&Vec<Condition>>,
    mut conditions: Vec<Value>,
    types: &[&str],
) -> Vec<Value> {
    // `Vec<Value>` AND NOT `Vec<Condition>`, because the patch builders build
    // their arrays as `json!(merge_condition(…))` and a round trip through the
    // struct here would re-serialise every element — turning a comparison that
    // is currently byte-for-byte into one that depends on two serialisations
    // agreeing. See `conditions::status_unchanged`'s note on why every element
    // this crate writes comes from `Condition` and `serde` in one step.
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

// ---------------------------------------------------------------------------
// The re-trust pass — D3 §7.4, "re-evaluation without re-fetching"
// ---------------------------------------------------------------------------

/// One object's verdict, re-derived from what is already on its status.
///
/// # Why a re-trust pass exists at all
///
/// Terminal objects are not re-read (D3 §1) — and a revocation must still
/// change what the console says. The resolution is that a revocation changes
/// **nothing about the document**, so nothing needs to be fetched: the stored
/// `matchedKeyId`, `signedAt` and `verifiedAt` are the three facts
/// [`logweir_core::trust::decide`] takes, and all three are already on the
/// status. No storage read, no signature check, no Job.
///
/// # WHAT CHANGES, AND WHAT DOES NOT
///
/// Changes: `status.evidence.verification.{result,trust,detail}`, and the
/// `Verified` condition that says the same thing on the object itself.
///
/// Does **not** change: `phase`, `exitCode`, `outcome`, `evidence.receiptKey`,
/// `evidence.sidecarKey`, `matchedKeyId`, `payloadType`, `signedAt`, and — the
/// one that is load-bearing rather than tidy — **`verifiedAt`**. That instant
/// is the only independent observation D3 §7.4 accepts, and a pass that
/// refreshed it while downgrading a verdict would destroy the evidence it just
/// reasoned from; see [`VerificationResult::to_status_value`].
///
/// # Why the `Verified` condition IS rewritten, although §7.4 lists three fields
///
/// **Recorded deviation.** §7.4 says the patch touches
/// `evidence.verification.{result,trust,detail}` and "never `phase`,
/// `exitCode`, `outcome` or any other field". Read literally that would leave
/// `Verified=True/Verified` beside `result: Untrusted` — a green condition over
/// a verdict this installation just refused, which is the silent-revocation
/// failure PLAT-19.1 exists to prevent. The `Verified` condition is not one of
/// the run's facts; it is **the controller's own fact about the run**, computed
/// after the terminal write and already rewritten by the second patch
/// ([`second_patch`]) for exactly this reason. So the list is read as "no run
/// fact", and the condition moves with the verdict — carrying a `reason` and a
/// `message` that say which key and which policy decided, which is the
/// "recorded reason" a granted verdict may not be withdrawn without.
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
    /// The `/status` merge patch, carrying the WHOLE condition list.
    ///
    /// A merge patch replaces arrays (RFC 7386), so a patch carrying only
    /// `[Verified]` would delete the terminal `Complete`/`Failed` condition.
    /// `existing` is the object's current condition list.
    #[must_use]
    pub fn patch(&self, existing: &[Condition]) -> Value {
        second_patch(existing, self.verified.clone(), self.verification.clone())
    }
}

/// Re-derive one stored verdict against `resolution` — `None` when nothing
/// changes.
///
/// `status` is the object's whole `status` as JSON; `badge` is the kind's own
/// green rule ([`backup_badge`] or [`restore_badge`]), so the condition this
/// produces obeys the same two-field rule the fresh path does.
///
/// # The rows this pass declines to touch
///
/// `Invalid` and `NotAttempted` with **no `matchedKeyId`** are not trust
/// verdicts: no key was ever selected, so there is nothing for a policy change
/// to re-decide, and re-deriving one would invent a verdict out of a digest
/// mismatch. They are returned as `None`, unchanged. A block with a
/// `matchedKeyId` is a signature that verified, and that is the whole input
/// set this pass needs.
///
/// # `None` MEANS "SEND NOTHING", and that is erratum E11(d)
///
/// The comparison is over the rendered block and the rendered condition, not
/// over a "did the generation change" flag: a policy edit that touches a key
/// this object never used must produce no write at all, or one `kubectl apply`
/// wakes every terminal object in the cluster.
#[must_use]
pub fn retrust(
    status: &Value,
    resolution: &Resolution,
    badge: fn(&Value) -> Badge,
    existing: Option<&Vec<Condition>>,
    observed_generation: Option<i64>,
    now: DateTime<Utc>,
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
            VerificationVerdict::NotAttempted,
            Some(trust_policy_conflict_detail(namespace, policies)),
            None,
        ),
        Resolution::Unconfigured => (
            VerificationVerdict::NotAttempted,
            Some(NO_SIGNING_KEYS_DETAIL.to_string()),
            None,
        ),
        Resolution::Trust(resolved) => {
            let projection = TrustProjection {
                key: resolved.key(matched_key_id).map(|k| k.trust.clone()),
                // THE STORED CLAIM, NOT A FRESH READ. The document is not
                // fetched, so `signedAt` is whatever the verification that DID
                // fetch it recorded — and an object written by a controller
                // that predates `signedAt` carries none, which fails closed
                // under `decide`'s rule with the absence named.
                claim: stored_claim(stored),
                policy: policy_ref(&resolved.source),
            };
            let verdict = projection.verdict(&VerificationResult::observation(Some(stored)), now);
            let result = match verdict.result {
                TrustResult::Valid => VerificationVerdict::Valid,
                TrustResult::Untrusted => VerificationVerdict::Untrusted,
            };
            let detail = untrusted_detail(&verdict, &projection, matched_key_id);
            let trust = Some(projection.to_status_value(&verdict));
            (result, detail, trust)
        }
    };

    // THE SAME KEY ORDER `VerificationResult::to_status_value` writes, so a
    // block this pass produces and a block the fresh path produces are the
    // same bytes for the same verdict. `serde_json` preserves insertion order
    // in this workspace, and `conditions::status_unchanged` is what decides
    // whether a patch is sent at all.
    block.insert("result".into(), json!(result.as_str()));
    block.insert("matchedKeyId".into(), json!(matched_key_id));
    block.insert("payloadType".into(), json!(payload_type));
    if let Some(d) = &detail {
        block.insert("detail".into(), json!(d));
    }
    // CARRIED VERBATIM, NEVER RECOMPUTED — see this type's header.
    if let Some(v) = stored.get("signedAt") {
        block.insert("signedAt".into(), v.clone());
    }
    if let Some(t) = trust {
        block.insert("trust".into(), t);
    }
    if let Some(v) = stored.get("verifiedAt") {
        block.insert("verifiedAt".into(), v.clone());
    }
    let verification = Value::Object(block);
    if &verification == stored {
        return None;
    }

    // THE BADGE IS COMPUTED OVER THE STATUS THAT WILL EXIST: this object's own
    // `exitCode`/`outcome`, which this pass does not touch, beside the new
    // verification block.
    let mut projected = status.clone();
    projected["evidence"] = json!({ "verification": verification.clone() });
    let verified = verified_condition(
        &badge(&projected),
        current_condition(existing, CONDITION_VERIFIED),
        observed_generation,
        now,
    );
    Some(Retrust {
        verification,
        verified,
        from,
        to: result.as_str().to_string(),
    })
}

/// The claim a stored verification block carries, with the absence NAMED when
/// it carries none.
fn stored_claim(stored: &Value) -> EvidenceClaim {
    match stored.get("signedAt").and_then(Value::as_str) {
        None => EvidenceClaim::absent(logweir_core::trust::ClaimAbsence::FieldAbsent),
        Some(text) => match DateTime::parse_from_rfc3339(text) {
            Ok(t) => EvidenceClaim::at(t.with_timezone(&Utc)),
            Err(_) => EvidenceClaim::absent(logweir_core::trust::ClaimAbsence::Unparseable),
        },
    }
}

// ---------------------------------------------------------------------------
// The catalog's own six-word vocabulary — D3 §5.4
// ---------------------------------------------------------------------------

/// One verdict in the **recovery catalog's** vocabulary (D3 §5.4), which is a
/// different set of words for the same two questions.
///
/// `Backup`/`Restore` status carries `result` plus `trust.basis` because those
/// objects have room for both; a catalog point carries ONE string per point
/// across a paged view, so §5.4 flattens the pair:
///
/// | here | §5.4 |
/// |---|---|
/// | `Valid`, basis `Current` | `Verified` |
/// | `Valid`, basis `Historical` | `VerifiedHistorical` |
/// | `Untrusted`, `UntrustedSigner` | `UntrustedSigner` |
/// | `Untrusted`, `Revoked`/`RecordedBeforeRevocation` | `Revoked` |
/// | `Untrusted`, any other row | `UntrustedSigner` |
/// | `Invalid` | `Invalid` |
/// | `NotAttempted` | `NotAttempted` |
///
/// **`NoEvidence` is not produced here** — §5.4's seventh value means "a
/// manifest with no receipt at all", which is a fact about the archive listing
/// and not about a verification, so the catalog worker decides it before ever
/// calling this.
///
/// This function is the SEAM D3 §14 leaves between W8 and W10: it exists so the
/// catalog's per-point verdict and a `Backup`'s badge cannot disagree about the
/// same key, and nothing in `controllers/recovery_catalog.rs` is touched to
/// provide it.
#[must_use]
pub fn catalog_verification(
    result: VerificationVerdict,
    verdict: Option<&Verdict>,
) -> &'static str {
    match result {
        VerificationVerdict::Invalid => "Invalid",
        VerificationVerdict::NotAttempted => "NotAttempted",
        VerificationVerdict::Valid => match verdict.map(|v| v.basis) {
            Some(TrustBasis::Historical) => "VerifiedHistorical",
            _ => "Verified",
        },
        VerificationVerdict::Untrusted => match verdict.and_then(|v| v.reason) {
            Some(UntrustReason::Revoked | UntrustReason::RecordedBeforeRevocation) => "Revoked",
            _ => "UntrustedSigner",
        },
    }
}
