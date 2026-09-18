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
    decide, ClaimAbsence, EvidenceClaim, IndependentObservation, KeyUsage, TrustBasis, TrustResult,
    TrustedKey, UntrustReason, Verdict,
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
        let Some(block) = existing else {
            return IndependentObservation::none();
        };
        // ---- IT MUST BE AN OBSERVATION, NOT JUST AN INSTANT (finding F5) ---
        //
        // Every verdict carries a `verifiedAt`, including the ones that never
        // read the document: a `NotAttempted` written because there was no
        // credential, because the object could not be fetched, or because two
        // policies contest the namespace, and an `Invalid` written because the
        // digest did not match. None of those is evidence that this
        // installation SAW the document, and D3 §7.4's compromise rule rests on
        // exactly that claim — so accepting one would put a false provenance
        // sentence ("recorded having seen this document before the revocation
        // took effect") in front of an operator, about the one key nobody
        // should trust.
        //
        // The two results a signature produced are the two that read it, and
        // both of them name the key that made it.
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
    // TWO DIFFERENT STATES, AND THEY WERE FOLDED TOGETHER (finding F7).
    //
    // **No `trust` key at all** is the additive-compatibility case: an object
    // written by a controller that predates PLAT-19.1, correctly green.
    // **A `trust` key this build cannot read** — no `basis`, a `null`, a
    // number, a basis nobody defined — is malformed, and a badge rule that
    // reads one field is one edit away from rendering green over a state
    // nobody intended. That is the argument this module makes for reading the
    // basis clause at all, so the arm that keeps the old rule must not be the
    // one that swallows a shape the old rule never had.
    let historical = match v.and_then(|v| v.get("trust")) {
        None => false,
        Some(trust) => match trust.get("basis").and_then(Value::as_str) {
            Some(TRUST_BASIS_CURRENT) => false,
            Some(TRUST_BASIS_HISTORICAL) => true,
            // THE VERDICT IS THE OLD ONE AND THE POLICY HAS NOT BEEN APPLIED
            // TO IT YET — `TRUST-UPGRADE-SIGNEDAT`. Not green, and the reason
            // an operator reads is the honest one: nothing was attempted. The
            // arm below would render `VerificationUntrusted` over a `Valid`
            // result nobody has refused, which is the same dishonesty in the
            // other direction from a green badge.
            Some(TRUST_BASIS_UNVERIFIED) => return Err(REASON_VERIFICATION_NOT_ATTEMPTED),
            _ => return Err(REASON_VERIFICATION_UNTRUSTED),
        },
    };
    Ok((at, key, historical))
}

/// `trust.basis` for a verdict reached against a key that was current.
const TRUST_BASIS_CURRENT: &str = "Current";
/// `trust.basis` for a verdict reached against a key that has since been
/// retired, expired or superseded.
const TRUST_BASIS_HISTORICAL: &str = "Historical";

/// `trust.basis` for a stored verdict NO policy has been applied to yet,
/// because the status predates `signedAt` and the document has not been
/// re-read — [`logweir_core::trust::TrustBasis::Unverified`].
const TRUST_BASIS_UNVERIFIED: &str = "Unverified";

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

/// The keys [`VerificationResult::to_status_value`] omits
/// when the verdict does not hold them — the ones a merge PATCH has to null.
///
/// `signedAt` and `trust` are written only for a verdict that carried a trust
/// projection; `matchedKeyId` and `detail` only for one that has them.
const VERIFICATION_CLEARED_KEYS: [&str; 4] = ["matchedKeyId", "detail", "signedAt", "trust"];

/// One rendered `status.evidence.verification` block, **as a merge PATCH**:
/// every field this verdict does not hold written as an explicit `null`.
///
/// # Why this exists, and why it is not inside [`VerificationResult::to_status_value`]
///
/// A JSON merge patch (RFC 7386) LEAVES AN OMITTED KEY IN PLACE and DELETES a
/// key sent as `null`. `to_status_value` builds a fresh block and simply omits
/// `matchedKeyId`, `detail`, `signedAt` and `trust` when the verdict has none
/// — so a block that replaced a stored one carrying a `matchedKeyId` would
/// leave that key id sitting under the new verdict. `retrust`
/// (`verification.rs`, guarded by `has_trust_verdict`) re-derives `Valid` or
/// `Untrusted` from a stored `matchedKeyId`, so a stale one under a
/// `NotAttempted` is a trust decision about a document this controller never
/// fetched. The D3 W2 record's clause — *"every write a
/// resourceVersion-preconditioned merge PATCH with explicit `null` for a field
/// that no longer holds"* — and seam **S7** are the rule; this is its second
/// half.
///
/// **THE NULLS BELONG TO THE PATCH AND NOT TO THE RENDERED BLOCK.**
/// [`valid_verification`] reads `trust` with
/// `None => compatible, Some(malformed) => Untrusted`, so a `trust: null`
/// inside the value the badge is computed over would turn a legacy `Valid`
/// with no trust projection into `Untrusted`. The badge is computed over
/// `to_status_value`'s block, unchanged; this wrapper is applied on the way
/// into [`second_patch`] and nowhere else.
///
/// **IT CANNOT CAUSE A WRITE STORM.** `conditions::apply_merge_patch` removes
/// a key sent as `null` and does nothing when it was already absent, so
/// `status_unchanged` still answers "unchanged" for a verdict that has not
/// moved — which is erratum **E11(d)**'s whole argument, kept.
///
/// # Both writers take it
///
/// The second patch ([`crate::controllers::backup`] and
/// [`crate::controllers::restore`]) and [`Retrust::patch`]. Review finding
/// **R4**: the re-trust patch was the one that had NOT been wrapped, and it is
/// the one where the staleness is reachable — see that method.
///
/// # Reachability, stated rather than assumed
///
/// **On the second patch there is no producer today.** `status_is_terminal`
/// short-circuits every pass after the terminal patch, so that patch runs at
/// most once per object and no `NotAttempted` block is ever merged over a
/// `Valid` one. The nulls are written there anyway, because the argument that
/// makes them unnecessary is an argument about a DIFFERENT function
/// (`status_is_terminal`) that the next change to it would silently retire.
///
/// **On [`Retrust::patch`] a producer exists**, which is why finding R4 is not
/// hypothetical: `apply_retrust` runs on TERMINAL objects, repeatedly, every
/// time a `TrustRoster` or policy edit re-derives a stored verdict.
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
    ///
    /// # The block is written as a PATCH, with explicit nulls
    ///
    /// Review finding **R4**. [`retrust`] builds a fresh block and omits
    /// `detail`, `signedAt` and `trust` when the re-derived verdict has none,
    /// and a merge patch leaves an omitted key in place — so an
    /// `Untrusted -> Valid` re-derivation after a policy edit used to leave the
    /// stale *"the trust policy X does not accept it"* sentence sitting under
    /// `result: Valid`, and a `Trust -> Conflict/Unconfigured` one left a stale
    /// `trust` block under `NotAttempted`.
    ///
    /// **THIS IS THE WRITER WHERE THAT IS REACHABLE.** The second patch runs at
    /// most once per object (`status_is_terminal` short-circuits every later
    /// pass); `apply_retrust` runs on TERMINAL objects, repeatedly, every time a
    /// `TrustRoster` or an installation policy changes. So the same
    /// [`verification_patch_value`] both reconcilers apply is applied here, and
    /// for a stronger reason.
    ///
    /// The badge is NOT recomputed from the nulled value: [`retrust`] computes
    /// it over the rendered block, before this method is reached, for the
    /// reason [`verification_patch_value`] gives.
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
/// hand — `TRUST-UPGRADE-SIGNEDAT`.
///
/// # What `signing_time` changes, and what it deliberately does not
///
/// It supplies exactly ONE missing input: the document's own claimed signing
/// time, for a status written before this controller recorded it. Everything
/// else is still re-derived from the status — the `matchedKeyId`, the
/// `verifiedAt` observation, the payload type — and no signature is re-checked
/// here. `signing_time_in` compares the bytes against the digest the run
/// recorded, which is what makes a claim read out of the archive exactly as
/// trustworthy as the verdict being repaired.
///
/// * [`SigningTime::Recovered`] — the claim is the document's, the block gains
///   a `signedAt`, and the verdict is the one a fresh run would reach.
/// * [`SigningTime::Absent`] — the document was read and genuinely claims no
///   signing time. It refuses exactly as it always has, now for a reason that
///   is true of the document.
/// * [`SigningTime::Unreadable`] / [`SigningTime::NotAttempted`] /
///   [`SigningTime::NotNeeded`] — nothing was learned, so **the stored result
///   is kept verbatim** on a [`TrustBasis::Unverified`] basis with the reason
///   said out loud. Not `Untrusted`, because nothing was read; not `Valid`
///   under the new policy either, because [`Badge`] refuses that basis.
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
                // THE STORED CLAIM, OR THE ONE BOUNDED RE-READ THE CALLER
                // PERFORMED. The document is not fetched HERE, and on every
                // path but the pre-`signedAt` repair it is not fetched at all:
                // `signedAt` is whatever the verification that DID fetch it
                // recorded, and an absence the document itself carries fails
                // closed under `decide`'s rule with the absence named.
                claim: match signing_time {
                    SigningTime::Recovered(at) => EvidenceClaim::at(*at),
                    SigningTime::Absent(absence) => EvidenceClaim::absent(*absence),
                    SigningTime::NotNeeded
                    | SigningTime::Unreadable(_)
                    | SigningTime::NotAttempted(_) => stored_claim(stored),
                },
                policy: policy_ref(&resolved.source),
            };
            let verdict = projection.verdict(&VerificationResult::observation(Some(stored)), now);
            let trust = Some(projection.to_status_value(&verdict));
            // NOTHING WAS READ, SO NOTHING IS WITHDRAWN. The stored result is
            // carried across verbatim; only the basis and the sentence change,
            // and the badge stops being green on the basis alone.
            if verdict.awaits_signing_time() {
                // `NotAttempted`, AND NOT THE STORED `Valid` — review finding
                // **F1**, critical.
                //
                // Carrying the previous result across kept the string "Valid"
                // on a block no policy had been applied to, and `crds::TrustBasis`
                // states the invariant every surface leans on: *"every old
                // reader treats anything that is not `Valid` as unverified"*.
                // Three of them read `result` and nothing else —
                // `ui/pages/backups.js`'s `validVerification`,
                // `logweir_api::status`'s projection, and the `SIGNED` printer
                // column — so a console rendered a green *"verified by
                // weirkeeper at … against key …"* badge over a document it had
                // not re-verified. Measured by the reviewer against this exact
                // block.
                //
                // `NotAttempted` is not a refusal and it is literally true: no
                // verification was attempted under this policy. It is the value
                // every consumer that predates the basis already fails closed
                // on, so the one write below fixes all of them at once. Nothing
                // is lost — `matchedKeyId` and `verifiedAt` still record the
                // original observation, and `detail` says what happened.
                (
                    VerificationVerdict::NotAttempted.as_str().to_string(),
                    Some(unverified_detail(&projection, matched_key_id, signing_time)),
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

    // THE SAME KEY ORDER `VerificationResult::to_status_value` writes, so a
    // block this pass produces and a block the fresh path produces are the
    // same bytes for the same verdict. `serde_json` preserves insertion order
    // in this workspace, and `conditions::status_unchanged` is what decides
    // whether a patch is sent at all.
    block.insert("result".into(), json!(result));
    block.insert("matchedKeyId".into(), json!(matched_key_id));
    block.insert("payloadType".into(), json!(payload_type));
    if let Some(d) = &detail {
        block.insert("detail".into(), json!(d));
    }
    // CARRIED VERBATIM, NEVER RECOMPUTED — see this type's header. The one
    // exception is the pre-PLAT-19.1 repair: a block that carries no signing
    // time at all takes the one the bounded re-read read out of the document,
    // rendered in the same `SecondsFormat::Secs` spelling the fresh path uses
    // so the two produce identical bytes for the same receipt.
    if let Some(v) = stored.get("signedAt").filter(|v| !v.is_null()) {
        block.insert("signedAt".into(), v.clone());
    } else if let SigningTime::Recovered(at) = signing_time {
        block.insert(
            "signedAt".into(),
            json!(at.to_rfc3339_opts(SecondsFormat::Secs, true)),
        );
    }
    if let Some(t) = trust {
        block.insert("trust".into(), t);
    }
    if let Some(v) = stored.get("verifiedAt") {
        block.insert("verifiedAt".into(), v.clone());
    }
    let verification = Value::Object(block);

    // THE BADGE IS COMPUTED OVER THE STATUS THAT WILL EXIST: this object's own
    // `exitCode`/`outcome`, which this pass does not touch, beside the new
    // verification block.
    let mut projected = status.clone();
    projected["evidence"] = json!({ "verification": verification.clone() });
    let stored_condition = current_condition(existing, CONDITION_VERIFIED);
    let verified = verified_condition(
        &badge(&projected),
        stored_condition,
        observed_generation,
        now,
    );

    // BOTH HALVES, AND THE SECOND IS NOT DECORATION (finding F6).
    //
    // This header has always said the comparison is over the rendered block AND
    // the rendered condition; the code compared the block alone and computed
    // the condition after the early return. So a `Verified` condition left
    // inconsistent with an already-correct block — one clobbered by another
    // builder writing the array, or left by a partially applied patch — was
    // never repaired: the pass saw an unchanged block and sent nothing, and a
    // green condition beside an `Untrusted` result is the silent revocation
    // this task exists to prevent. `merge_condition` keeps the stored
    // `lastTransitionTime` when status and reason are unchanged, so a genuinely
    // steady object still renders the identical condition and still sends
    // nothing (erratum E11(d)).
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

/// The `detail` for a stored verdict no policy has been applied to yet.
///
/// IT NAMES THE KEY, THE POLICY AND WHAT HAPPENS NEXT, like every other
/// `detail` in this module — and it never says the document is untrusted,
/// because nothing about the document has been examined. An operator reading
/// it should be able to tell that this is their own upgrade and not their
/// archive.
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
        SigningTime::Unreadable(detail) => {
            format!("the archive was read for it and did not answer: {detail}")
        }
        SigningTime::NotAttempted(detail) => format!("no re-read was attempted: {detail}"),
        // UNREACHABLE THROUGH `retrust_with`, which reaches this function only
        // for a claim that is still `NotRecorded` — and both of these arms
        // replace the claim. A sentence rather than a panic, because a
        // `detail` is not the place to abort a reconcile.
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

/// The claim a stored verification block carries, with the absence NAMED when
/// it carries none.
///
/// # Two absences, and telling them apart is `TRUST-UPGRADE-SIGNEDAT`
///
/// A block with no `signedAt` is one of two very different things:
///
/// * **this installation never recorded the instant** — the status was written,
///   or last re-derived, by a build that did not have the field. Nothing about
///   the document is in doubt and nobody has read it. That is
///   [`ClaimAbsence::NotRecorded`], and [`decide`] answers it with an UNDECIDED
///   verdict whose contract is one bounded re-read.
/// * **the DOCUMENT carries no readable signing time** — a build that knows the
///   field read the document and found none. That is
///   [`ClaimAbsence::FieldAbsent`] and it fails closed, as it always has.
///
/// # THE DISCRIMINATOR IS THE BASIS, AND IT TOOK THREE LAB RUNS TO GET RIGHT
///
/// The question is never "which build wrote this block" — nothing on the status
/// says so — but **"is this block evidence that a signing time was ever
/// compared to the key's validity window?"** Exactly two bases can only have
/// been reached from a real claim: [`TRUST_BASIS_CURRENT`] and
/// [`TRUST_BASIS_HISTORICAL`] both require `claim.signed_at` inside
/// [`decide`]'s window rows. Everything else — no `trust` block, no `basis`
/// inside one, `None`, `Unverified`, or a spelling a later build invents — is a
/// block that compared nothing, and the honest answer for it is "not yet
/// asked".
///
/// Three shapes have been seen in the field and all three are `NotRecorded`:
///
/// | shape | written by |
/// |---|---|
/// | no `trust` block at all | a controller predating PLAT-19.1 |
/// | `trust.basis: Unverified` | this build, waiting on its own re-read |
/// | `trust.basis: None`, no `signedAt` | the intermediate `e7d0e79` build's re-derivation |
///
/// **The third is the one that shipped broken.** Two earlier attempts keyed on
/// the PRESENCE of a `trust` block and then on the literal string
/// `Unverified`, and the lab's five 2026-09-14 objects matched neither: the
/// `e7d0e79` controller had already re-stamped them with
/// `{basis: "None", keyState: "Active", policy: {name: legacy-roster-v1}}` and
/// no `signedAt`, so they classified `FieldAbsent`, no re-read was ever
/// scheduled, and three consecutive lab refreshes reported the same five
/// objects `Untrusted` (`lab-refresh-3.result.md` §9). Enumerating the bases
/// that MEAN something, rather than the shapes that do not, is what makes the
/// rule closed under builds nobody has written yet.
///
/// # What this costs, and why it is the right side to err on
///
/// A document that genuinely carries no signing time is written by a current
/// build as `basis: None` with no `signedAt` — **the same bytes** as the lab's
/// legacy re-stamp. They are indistinguishable on the status, so this rule
/// re-reads that document too. The read answers with the document's own
/// absence, [`decide`] refuses it exactly as before, the re-rendered block is
/// identical and **no patch is sent** ([`retrust_with`] returns `None`). The
/// cost is therefore one `Store::get` per policy event and zero writes, and it
/// buys the only thing that can tell the two apart: reading the document.
/// Guessing the other way is what left five sound archives marked `Untrusted`.
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
/// **AN ALLOW-LIST, NOT A DENY-LIST**, and that is the whole lesson of
/// `TRUST-UPGRADE-SIGNEDAT`. [`decide`] reaches [`TRUST_BASIS_CURRENT`] and
/// [`TRUST_BASIS_HISTORICAL`] only through its window rows, and both of those
/// rows are unreachable without `claim.signed_at` — so a block carrying either
/// one had a real claim, and a block carrying anything else did not. Listing
/// the bases that must NOT re-read is the form that stays correct when a build
/// nobody has written yet invents a sixth spelling; listing the ones that must
/// is the form that shipped twice and missed the field twice.
///
/// `RecordedBeforeRevocation` is deliberately NOT on this list. It is reached
/// from the independent observation and never from the claim, so a block
/// carrying it has compared nothing either; re-reading such an object cannot
/// change its verdict — the compromise rows run first — but it does fill in the
/// `signedAt` the record should have had, once, after which it is settled.
fn compared_a_claim(stored: &Value) -> bool {
    matches!(
        stored.pointer("/trust/basis").and_then(Value::as_str),
        Some(TRUST_BASIS_CURRENT | TRUST_BASIS_HISTORICAL)
    )
}

// ---------------------------------------------------------------------------
// The signing-time recovery — TRUST-UPGRADE-SIGNEDAT
// ---------------------------------------------------------------------------

/// The `status.evidence` key pairs that name a signed document and the digest
/// the run recorded for it: a `Backup`'s receipt, and a `Restore`'s scorecard.
///
/// THE DIGEST IS NOT OPTIONAL. It is what makes reading a signing time out of
/// the archive as trustworthy as the verdict that is being repaired: the bytes
/// that digest to what the status recorded are the bytes whose signature was
/// verified. Without it this would be "believe whatever is in the bucket now",
/// which is precisely the substitution [`verify_evidence`]'s step 3 exists to
/// catch.
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
    /// out of the field THAT type keeps it in.
    pub payload_type: String,
}

/// What one bounded re-read of the document produced.
///
/// # Four answers, and only one of them moves a verdict to `Valid`
///
/// The point of the enum is that "the archive did not answer" and "the
/// document answered, and it carries no signing time" are not the same fact
/// and must not produce the same status. The first keeps the previous verdict
/// on an [`logweir_core::trust::TrustBasis::Unverified`] basis and is retried;
/// the second is the document's own absence and refuses exactly as it always
/// has.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SigningTime {
    /// Nothing was read, because nothing needed to be: the stored block
    /// already carries a `signedAt`, or it is not a pre-PLAT-19.1 block.
    NotNeeded,
    /// The document was read, its digest matched, and this is its own claimed
    /// signing time — derived by [`logweir_core::trust::read_claimed_signing_time`],
    /// the same function the fresh path uses.
    Recovered(DateTime<Utc>),
    /// The document was read and its digest matched, and it genuinely carries
    /// no readable signing time. The absence is the DOCUMENT's now.
    Absent(ClaimAbsence),
    /// The archive was reached for and did not answer — no object, a storage
    /// error, bytes that are not the bytes the run recorded. Bounded: one
    /// attempt, and the next policy event tries again.
    Unreadable(String),
    /// No read was attempted at all, and why: this controller holds no
    /// evidence credential, or the destination reads evidence with a grant
    /// only a pod may hold (D2 §3.9).
    NotAttempted(String),
}

/// Whether a stored status needs one bounded re-read before its verdict can be
/// re-derived, and what to read — `None` when it does not.
///
/// # The three things that all have to be true
///
/// A `matchedKeyId` (there is a signature to have an opinion about), a claim
/// that is [`ClaimAbsence::NotRecorded`] (the status predates `signedAt`), and
/// a document key WITH its recorded digest on `status.evidence`. An object
/// missing the third cannot be repaired — there is nothing safe to read — and
/// it stays on the previous verdict with the reason said out loud rather than
/// being flipped on a field that did not exist when it was written.
#[must_use]
pub fn signing_time_need(status: Option<&Value>) -> Option<SigningTimeNeed> {
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

/// The signing time in `bytes`, **after** checking them against the digest the
/// status recorded.
///
/// PURE, so every row of the table above is a unit test with no bucket
/// anywhere.
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
    let Ok(json) = serde_json::from_slice::<Value>(bytes) else {
        return SigningTime::Absent(ClaimAbsence::Unparseable);
    };
    match logweir_core::trust::read_claimed_signing_time(&need.payload_type, &json) {
        Ok(at) => SigningTime::Recovered(at),
        Err(absence) => SigningTime::Absent(absence),
    }
}

/// ONE `get` through the read-only handle, then [`signing_time_in`].
///
/// # Interface I13
///
/// Synchronous on purpose: this is the body of a `spawn_blocking` closure, for
/// the reason this module's header gives. It is named in
/// `tests/retention.rs::STORE_CALL_TOKENS` because it holds a `Store::get`
/// while its callers name no `Store` at all — the same blindness that let a
/// planted call survive that guard once.
#[must_use]
pub fn read_signing_time(store: Option<&Store>, need: &SigningTimeNeed) -> SigningTime {
    let Some(store) = store else {
        return SigningTime::NotAttempted(NO_CREDENTIAL_DETAIL.to_string());
    };
    match store.get(&need.payload_key) {
        Ok((bytes, _version)) => signing_time_in(&bytes, need),
        Err(e) => SigningTime::Unreadable(store_detail(&e)),
    }
}

/// The `detail` for a re-read that was not attempted because the evidence path
/// itself could not be resolved — review finding **F6**.
///
/// A kube API failure is a fact about this controller's connection, not about
/// the document, so it reads as `NotAttempted` like every other reason there is
/// no handle, and the verdict is still re-derived on the pass that saw it.
#[must_use]
pub fn evidence_path_unreadable(e: &kube::Error) -> String {
    format!(
        "this run's evidence path could not be resolved, so no re-read was attempted: {e}; the \
         verdict is re-derived from the status alone and the read is tried again on the next \
         policy event"
    )
}

/// [`read_signing_time`] on a blocking thread — the seam both reconcilers call.
///
/// `unread` is the evidence path's own refusal when there is no handle to read
/// through (`EvidenceSource::NotAttempted`'s detail): a destination that
/// declares no `evidenceRead`, one whose grant only a pod may hold, or a
/// location no installation policy allowlists. It is passed through verbatim
/// rather than re-derived, so the sentence an operator reads here is the one
/// the fresh path would have written.
///
/// **Exactly one attempt.** There is no retry loop, no backoff and no queue:
/// if the archive does not answer, the object keeps its previous verdict on an
/// `Unverified` basis and the NEXT policy event tries once more. A loop here
/// would turn one unreachable bucket into a controller that never finishes a
/// reconcile.
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
) -> CatalogVerdict {
    let state = match result {
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
    };
    CatalogVerdict {
        state,
        reason: verdict.and_then(|v| v.reason),
        basis: verdict.map(|v| v.basis),
    }
}

/// One catalog point's verdict: §5.4's word, **and the row that produced it**.
///
/// # Why the reason travels with the word (review finding F4)
///
/// Because §5.4's six values are lossy in a way that changes the REMEDY. Three
/// different rows flatten into `UntrustedSigner`, whose §5.4 definition is
/// "signature verifies under a key the policy does not list" — true for
/// [`UntrustReason::UntrustedSigner`] and false for
/// [`UntrustReason::KeyUsageMismatch`] and
/// [`UntrustReason::SignedOutsideValidity`], where the key IS listed. D3 §15's
/// L7 remedy for `UntrustedSigner` is "add the key as
/// `Retired/EvidenceSigning`"; an operator handed that for a
/// `SignedOutsideValidity` point adds a key that is already there and nothing
/// changes. `RecordedBeforeRevocation` flattens to `Revoked`, whose definition
/// says explicitly "and **no** independent pre-revocation observation" — the
/// opposite of what happened.
///
/// Adding the two missing words to §5.4 was the alternative and was not taken:
/// that vocabulary is a shipped enum on a status a console renders, and
/// widening it is a CRD decision belonging to whoever owns
/// `recovery_catalog.rs`. This keeps the six values exactly as §5.4 defines
/// them and hands W8 what it needs to write a remedy sentence that is true.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CatalogVerdict {
    /// D3 §5.4's word: `Verified`, `VerifiedHistorical`, `UntrustedSigner`,
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
/// THE CHEAP GUARD IN FRONT OF THE EXPENSIVE ONE. [`retrust`] declines a block
/// with no `matchedKeyId` anyway, but [`apply_retrust`] has to RESOLVE trust
/// before it can call it, and resolving costs an API read. An object that has
/// never had a signature verified against it — every non-terminal run, every
/// run that wrote no evidence — must not pay for a question it cannot have an
/// answer to.
#[must_use]
pub fn has_trust_verdict(status: Option<&Value>) -> bool {
    stored_verification(status)
        .and_then(|v| v.get("matchedKeyId"))
        .and_then(Value::as_str)
        .is_some_and(|id| !id.is_empty())
}

/// Re-derive one object's stored verdict against `resolution` and, when it
/// changed, PATCH it — D3 §7.4's "re-evaluation without re-fetching".
///
/// # What this sends, and the precondition it carries
///
/// A `/status` merge PATCH carrying exactly `evidence.verification` and the
/// whole `conditions` array (a merge patch replaces arrays, RFC 7386), with
/// `metadata.resourceVersion` as a **precondition** — seam **S7**. An object
/// that changed between the read and this write answers 409 and the next
/// reconcile starts again from what the object now says, which matters more
/// here than anywhere else: this pass reasons from stored fields, so writing
/// over a concurrent update would reason from fields it never saw.
///
/// `Ok(None)` means nothing changed and **nothing was sent** (erratum
/// **E11(d)**): one `kubectl apply` on a policy must not wake every terminal
/// object in the cluster into a write.
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
    // NO PRECONDITION, NO PATCH. An object the API server handed us without a
    // `resourceVersion` is not one this pass may write over blind.
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
    // AN HONEST LOG LINE ON BOTH PATHS. The sentence below used to promise
    // "no storage read", which is true of every pass but the pre-PLAT-19.1
    // repair — and a log that says a read did not happen while one did is the
    // same defect as a status that says it.
    let how = match signing_time {
        SigningTime::Recovered(_) | SigningTime::Absent(_) => {
            "re-deriving after ONE bounded re-read of the document, which supplied the signing \
             time this status was written without"
        }
        SigningTime::NotNeeded | SigningTime::Unreadable(_) | SigningTime::NotAttempted(_) => {
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
/// # No LIST, and that is the whole design
///
/// `objects` is the controller's OWN reflector snapshot
/// (`Controller::store()`). This controller already runs a watch over every
/// object of its kind — that is what `Controller::new` is — so the store is the
/// same index, already paid for, and cannot be staler than the event being
/// mapped. The trigger therefore costs **zero** API calls and is bounded by the
/// objects this controller holds rather than by the cluster.
///
/// # It over-approximates on purpose
///
/// `scope` is the UNION of what the policy bound before the event and what it
/// binds after ([`crate::trust::PolicyScopeMemory`]), and a `default: true`
/// policy on either side covers every namespace. Enqueuing an object a policy
/// does not govern costs one re-derivation that writes nothing ([`retrust`]
/// returns `None` for an unchanged block, erratum **E11(d)**); failing to
/// enqueue one leaves a revoked key green until something else happens to
/// reconcile it — and for a terminal `Restore`, which parks on
/// `Action::await_change()`, nothing else does. The two errors are not
/// symmetric and this rounds the safe way.
///
/// Generic, and taking a `Vec` rather than a `Store`, so the mapping is
/// testable without a running watch: the property under test is which objects
/// come out, not how the snapshot was obtained.
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
