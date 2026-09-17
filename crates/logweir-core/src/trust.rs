//! The trust lifecycle, as pure arithmetic over a key's declared history
//! (PLAT-19.1, decision D3 §7.4).
//!
//! # Two questions, and they are not the same question
//!
//! *May this key sign something new?* and *does this existing evidence still
//! verify?* were one question for as long as trust was a flat roster: a key
//! was on it or it was not, and a `notAfter` in the past meant "refuse". That
//! answer is wrong for half of what an operator does. Rotation without a
//! blanket trust gap needs a key that may no longer sign and whose old
//! signatures still verify — [`may_sign_new`] answers the first question and
//! [`decide`] answers the second, and the whole point of PLAT-19.1 is that
//! they are allowed to disagree.
//!
//! # Retirement is not revocation, and compromise is not supersession
//!
//! Three lifecycle facts, three different consequences for stored evidence:
//!
//! * **Retired / expired** — the key was trustworthy when it signed. Evidence
//!   claiming a signing time at or before the boundary is [`TrustBasis::Historical`]:
//!   VALID, on a stated basis, never green-washed into [`TrustBasis::Current`].
//! * **Revoked, `Superseded` or `Unspecified`** — a replacement, with no
//!   suspicion. Treated as a retirement at `revocationEffectiveFrom`.
//! * **Revoked, `KeyCompromise`** — the private half may be in someone else's
//!   hands, so the document's OWN claimed signing time is attacker-controlled
//!   and is deliberately not consulted. The only accepted evidence that
//!   something existed before the compromise is an INDEPENDENT OBSERVATION —
//!   a `verifiedAt` a controller wrote on an earlier reconcile — and even then
//!   the verdict is `Untrusted` with [`UntrustReason::RecordedBeforeRevocation`],
//!   rendered with the recorded instant and never as a pass. An imported
//!   archive this installation never observed has no such history and FAILS
//!   CLOSED.
//!
//! # No clock, no I/O, no crypto — and `now` is an argument
//!
//! Global Constraint 1, machine-checked by `scripts/check-pure-core.sh`. Every
//! instant this module reasons about is passed in: `now`, the claimed signing
//! time, the independent observation. A clock read here would make the verdict
//! for a stored document depend on when it was asked about rather than on what
//! the policy says, and would make every row of §7.4's table untestable
//! without sleeping.
//!
//! It holds no key material either. [`TrustedKey`] carries a key's IDENTITY and
//! its HISTORY, never its PEM: whether a signature verifies is
//! `logweir_verify::verify_detached`'s answer, and whether the key that
//! produced it is still trusted is this module's. Keeping them apart is what
//! lets the controller re-evaluate a revocation against evidence it does not
//! re-fetch (D3 §7.4, "re-evaluation without re-fetching").
//!
//! # Who calls this
//!
//! `weirkeeper::trust` resolves a namespace to one of these key sets and wraps
//! both entry points; `weirkeeper::verification` and
//! `weirkeeper::controllers::approval` consume the verdicts. This crate names
//! none of them.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// What a key is allowed to be used for (D3 §7.3).
///
/// SEPARATED BECAUSE P17 REQUIRES IT FOR PLAT-19.2. One key that both signs
/// evidence and authorises restores is a key whose holder can approve their
/// own work, and the usage set is what turns "the same operator holds both"
/// from a labelled risk into a refusal.
///
/// A usage MISMATCH is its own verdict ([`UntrustReason::KeyUsageMismatch`])
/// and never a signature failure: a genuinely signed approval presented as an
/// evidence signature is a different fault from a forgery, and an operator
/// told "bad signature" will go looking at the wrong thing.
#[derive(Deserialize, Serialize, Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum KeyUsage {
    /// Receipts, scorecards, teardown attestations, catalog records and
    /// retention records — the installation's own signing identity.
    EvidenceSigning,
    /// Governed approval and standing-authorization documents, signed by
    /// humans on their own machines.
    GovernedApproval,
    /// The console confirmation signature of PLAT-19.2's ordinary mode.
    ///
    /// NEVER SYNTHESISED FROM THE LEGACY ROSTER (D3 §7.3): an old controller
    /// reached by rollback must not be able to mistake an ordinary
    /// confirmation for a governed approval, so the legacy roster produces
    /// only [`Self::EvidenceSigning`] and [`Self::GovernedApproval`] keys.
    ConsoleConfirmation,
}

impl KeyUsage {
    /// The wire spelling — the same string the `TrustPolicy` CRD serialises.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::EvidenceSigning => "EvidenceSigning",
            Self::GovernedApproval => "GovernedApproval",
            Self::ConsoleConfirmation => "ConsoleConfirmation",
        }
    }
}

/// Where a key is in its declared lifecycle. **Monotonic** — the CRD's G3 rule
/// lets it move `Active → Retired` and `Active|Retired → Revoked`, and nowhere
/// else.
#[derive(Deserialize, Serialize, Clone, Copy, Debug, PartialEq, Eq)]
pub enum KeyState {
    /// May sign something new, inside its validity window.
    Active,
    /// May no longer sign. What it signed before `retiredAt` still verifies.
    Retired,
    /// Withdrawn. What that means for stored evidence depends entirely on
    /// [`RevocationReason`].
    Revoked,
}

/// Why a key was revoked. **The distinction is the load-bearing one.**
#[derive(Deserialize, Serialize, Clone, Copy, Debug, PartialEq, Eq)]
pub enum RevocationReason {
    /// The private half may be in someone else's hands. The document's own
    /// claimed signing time is not accepted.
    KeyCompromise,
    /// Replaced by a newer key, with no suspicion.
    Superseded,
    /// No reason recorded. Read as [`Self::Superseded`] — the CONSERVATIVE
    /// reading is the other one, but a policy that treated every unexplained
    /// revocation as a compromise would make routine supersession invalidate
    /// every archive an operator forgot to annotate. D3 §7.4's table names
    /// `Superseded`/`Unspecified` together for exactly that reason.
    Unspecified,
}

/// One trusted key's identity and history — **no key material** (see the
/// module header).
#[derive(Deserialize, Serialize, Clone, Debug, PartialEq, Eq)]
pub struct TrustedKey {
    /// The sha256 of the DER SPKI, lowercase hex.
    pub key_id: String,
    /// A stable principal identifier: `install:<digest>`, an email, an OIDC
    /// subject, or `legacy:<keyId>` for a key synthesised from a `TrustRoster`.
    pub principal_id: String,
    /// What this key may be used for. An empty set may do nothing.
    pub usages: Vec<KeyUsage>,
    /// The start of the validity window.
    pub not_before: DateTime<Utc>,
    /// The end of the validity window.
    pub not_after: DateTime<Utc>,
    /// The declared lifecycle state.
    pub state: KeyState,
    /// When it stopped being allowed to sign.
    pub retired_at: Option<DateTime<Utc>>,
    /// When it was revoked. Recorded for display; the instant that DECIDES is
    /// [`Self::revocation_effective_from`].
    pub revoked_at: Option<DateTime<Utc>>,
    /// Why it was revoked. Absent is read as [`RevocationReason::Unspecified`].
    pub revocation_reason: Option<RevocationReason>,
    /// The instant from which the revocation applies to stored evidence.
    pub revocation_effective_from: Option<DateTime<Utc>>,
}

impl TrustedKey {
    /// Whether this key carries `usage`.
    #[must_use]
    pub fn has_usage(&self, usage: KeyUsage) -> bool {
        self.usages.contains(&usage)
    }

    /// The revocation reason, with absent read as
    /// [`RevocationReason::Unspecified`].
    #[must_use]
    pub fn reason(&self) -> RevocationReason {
        self.revocation_reason
            .unwrap_or(RevocationReason::Unspecified)
    }

    /// The last instant at which a signature by this key is still accepted.
    ///
    /// THE EARLIEST OF EVERY BOUND THAT APPLIES, never the latest. A key that
    /// was retired in March and whose `notAfter` is in December stopped being
    /// able to sign in March, and a boundary that took the later of the two
    /// would accept nine months of signatures the operator had already
    /// withdrawn permission for.
    ///
    /// `None` for a compromise revocation: there is no instant at which that
    /// key's own claim about when it signed means anything (see [`decide`]).
    #[must_use]
    pub fn accepted_through(&self) -> Option<DateTime<Utc>> {
        let mut bound = self.not_after;
        if let Some(retired) = self.retired_at {
            bound = bound.min(retired);
        }
        match self.state {
            KeyState::Active => Some(bound),
            KeyState::Retired => Some(bound),
            KeyState::Revoked => match self.reason() {
                RevocationReason::KeyCompromise => None,
                RevocationReason::Superseded | RevocationReason::Unspecified => {
                    // "treated as retirement at `revocationEffectiveFrom`"
                    // (D3 §7.4). A revocation with no effective instant — which
                    // the CRD's G5 rule makes impossible on a live object and
                    // which a hand-built spec can still express — falls back to
                    // `revokedAt` and then to the window, never to "no bound".
                    let effective = self.revocation_effective_from.or(self.revoked_at);
                    Some(effective.map_or(bound, |e| bound.min(e)))
                }
            },
        }
    }
}

/// A key as resolved against a clock — the `status.keys[].effectiveState`
/// vocabulary of D3 §7.1.
///
/// SIX VALUES, AND `Unparseable` IS NOT PRODUCED HERE. Whether a `spkiPem` is
/// a key this build can read is a question about key material, which this
/// crate deliberately never sees; the controller overrides the state with
/// [`Self::Unparseable`] when the PEM does not parse.
#[derive(Deserialize, Serialize, Clone, Copy, Debug, PartialEq, Eq)]
pub enum EffectiveState {
    /// Declared `Active` and inside its validity window.
    Active,
    /// Declared `Active` and `now` is before `notBefore`.
    NotYetValid,
    /// Declared `Active` and `now` is at or after `notAfter`.
    Expired,
    /// Declared `Retired`.
    Retired,
    /// Declared `Revoked`.
    Revoked,
    /// The `spkiPem` is not a P-256 or Ed25519 public key. Never returned by
    /// [`effective_state`]; written by the controller.
    Unparseable,
}

impl EffectiveState {
    /// The wire spelling, as it lands on `status.keys[].effectiveState`.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Active => "Active",
            Self::NotYetValid => "NotYetValid",
            Self::Expired => "Expired",
            Self::Retired => "Retired",
            Self::Revoked => "Revoked",
            Self::Unparseable => "Unparseable",
        }
    }
}

/// `status.keys[].usableForVerification` — whether evidence this key signed
/// still verifies, and on what basis.
///
/// **[`Self::Historical`] IS NOT A DOWNGRADE OF [`Self::Full`].** It is the
/// honest answer for a key that was valid when it signed and has since been
/// retired, and a console that rendered it as a failure would make routine
/// rotation look like an incident.
#[derive(Deserialize, Serialize, Clone, Copy, Debug, PartialEq, Eq)]
pub enum VerificationUse {
    /// Evidence signed inside the window verifies as `Current`.
    Full,
    /// Evidence signed at or before the boundary verifies as `Historical`.
    Historical,
    /// Nothing this key signed verifies on the strength of its own claim.
    None,
}

impl VerificationUse {
    /// The wire spelling.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Full => "Full",
            Self::Historical => "Historical",
            Self::None => "None",
        }
    }
}

/// `status.evidence.verification.trust.keyState` (D3 §7.4).
///
/// FIVE VALUES, AND THEY ARE NOT [`EffectiveState`]'s SIX. A verdict about a
/// stored document reports the key's lifecycle as the verifier saw it —
/// `Unknown` when the policy does not carry the key at all — and has no use
/// for `NotYetValid` or `Unparseable`, neither of which a matched signature
/// can have come from. Two vocabularies because they answer two questions;
/// merging them would put `Unparseable` on a document that verified.
#[derive(Deserialize, Serialize, Clone, Copy, Debug, PartialEq, Eq)]
pub enum TrustKeyState {
    /// Inside its validity window and not retired or revoked.
    Active,
    /// Declared `Retired`.
    Retired,
    /// Declared `Active`, past `notAfter`.
    Expired,
    /// Declared `Revoked`.
    Revoked,
    /// The policy does not carry this key id.
    Unknown,
}

impl TrustKeyState {
    /// The wire spelling.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Active => "Active",
            Self::Retired => "Retired",
            Self::Expired => "Expired",
            Self::Revoked => "Revoked",
            Self::Unknown => "Unknown",
        }
    }
}

/// `status.evidence.verification.trust.basis` (D3 §7.4).
#[derive(Deserialize, Serialize, Clone, Copy, Debug, PartialEq, Eq)]
pub enum TrustBasis {
    /// The key was active and the evidence was signed inside its window.
    Current,
    /// The key has since been retired, expired or superseded, and the evidence
    /// was signed at or before the boundary.
    Historical,
    /// A compromise-revoked key, with a controller-written observation from
    /// before the revocation took effect. **Never green.**
    RecordedBeforeRevocation,
    /// No basis at all.
    None,
}

impl TrustBasis {
    /// The wire spelling.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Current => "Current",
            Self::Historical => "Historical",
            Self::RecordedBeforeRevocation => "RecordedBeforeRevocation",
            Self::None => "None",
        }
    }
}

/// Why a stored document is not trusted. **The set is closed**; every variant
/// is a row of D3 §7.4's table.
#[derive(Deserialize, Serialize, Clone, Copy, Debug, PartialEq, Eq)]
pub enum UntrustReason {
    /// The policy carries no key with this id.
    UntrustedSigner,
    /// The key exists and does not carry the usage this document needs.
    KeyUsageMismatch,
    /// The claimed signing time is outside the window the key was trusted in —
    /// or there is no claimed signing time at all, which is the same refusal
    /// because the claim is what the window is checked against.
    SignedOutsideValidity,
    /// A compromise-revoked key, with an independent observation from before
    /// the revocation took effect.
    RecordedBeforeRevocation,
    /// A compromise-revoked key with no independent observation. Fails closed.
    Revoked,
}

impl UntrustReason {
    /// The wire spelling, as it lands on
    /// `status.evidence.verification.detail`'s machine half and on the UI's
    /// reason string.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::UntrustedSigner => "UntrustedSigner",
            Self::KeyUsageMismatch => "KeyUsageMismatch",
            Self::SignedOutsideValidity => "SignedOutsideValidity",
            Self::RecordedBeforeRevocation => "RecordedBeforeRevocation",
            Self::Revoked => "Revoked",
        }
    }
}

/// `Valid` or `Untrusted` — the two results [`decide`] produces.
///
/// IT NEVER PRODUCES `Invalid` OR `NotAttempted`. Those are
/// `weirkeeper::verification`'s vocabulary and they are claims about the
/// DOCUMENT and about the CONTROLLER respectively; this function is asked only
/// about the KEY, and is asked it about a signature that already verified.
#[derive(Deserialize, Serialize, Clone, Copy, Debug, PartialEq, Eq)]
pub enum TrustResult {
    /// The key's lifecycle permits this document to be trusted.
    Valid,
    /// It does not, and [`Verdict::reason`] says which row of the table
    /// decided it.
    Untrusted,
}

impl TrustResult {
    /// The wire spelling, as it lands on
    /// `status.evidence.verification.result`.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Valid => "Valid",
            Self::Untrusted => "Untrusted",
        }
    }
}

/// What the key's lifecycle says about one stored document.
#[derive(Deserialize, Serialize, Clone, Copy, Debug, PartialEq, Eq)]
pub struct Verdict {
    /// `Valid` or `Untrusted`.
    pub result: TrustResult,
    /// The basis, which is what a badge renders beside the result.
    pub basis: TrustBasis,
    /// The key's lifecycle as this verdict saw it.
    pub key_state: TrustKeyState,
    /// `None` on a `Valid`; the row that refused, otherwise.
    pub reason: Option<UntrustReason>,
}

impl Verdict {
    /// Whether this verdict may render green — `Valid` on a
    /// `Current`/`Historical` basis and nothing else.
    ///
    /// [`TrustBasis::RecordedBeforeRevocation`] is deliberately excluded here
    /// AND is already an `Untrusted`: two independent reasons it cannot be
    /// green, because D3 §7.4 says it is "rendered with the recorded instant,
    /// never green" and one of those reasons could be edited away by accident.
    #[must_use]
    pub fn may_render_green(&self) -> bool {
        matches!(self.result, TrustResult::Valid)
            && matches!(self.basis, TrustBasis::Current | TrustBasis::Historical)
    }

    fn valid(basis: TrustBasis, key_state: TrustKeyState) -> Self {
        Self {
            result: TrustResult::Valid,
            basis,
            key_state,
            reason: None,
        }
    }

    fn untrusted(reason: UntrustReason, basis: TrustBasis, key_state: TrustKeyState) -> Self {
        Self {
            result: TrustResult::Untrusted,
            basis,
            key_state,
            reason: Some(reason),
        }
    }
}

/// What a stored document claims about itself: the signing time read out of
/// its own bytes by [`claimed_signing_time`].
///
/// A STRUCT AND NOT A BARE `Option<DateTime<Utc>>`, so a call site cannot pass
/// the independent observation in the claim's place — they are both
/// `Option<DateTime<Utc>>`, they mean opposite things, and swapping them is
/// the mutation that would make a compromised key's own word count as
/// corroboration.
#[derive(Deserialize, Serialize, Clone, Copy, Debug, PartialEq, Eq, Default)]
pub struct EvidenceClaim {
    /// The document's own latest pre-signature timestamp, absent when the
    /// document carries none or could not be read.
    pub signed_at: Option<DateTime<Utc>>,
    /// **Why** it is absent, when it is. `None` when `signed_at` is present.
    ///
    /// A NAMED REASON, NOT JUST A MISSING VALUE. Every absence refuses under
    /// [`decide`]'s fail-closed rule, and every one of them renders the same
    /// [`UntrustReason::SignedOutsideValidity`] — which tells an operator
    /// nothing about which of four very different things went wrong. This is
    /// what the caller puts in `status.evidence.verification.detail`, so
    /// "this scorecard recorded no phase" does not arrive as "signed outside
    /// validity".
    pub absence: Option<ClaimAbsence>,
}

/// Why a document carries no readable signing time.
///
/// The set is closed and each variant is a DIFFERENT operator action: an
/// unknown payload type is a build that has not learned a document; an absent
/// field is a document that is not what it says it is; an empty phase list is
/// a run that recorded nothing; an unparseable value is a malformed instant.
#[derive(Deserialize, Serialize, Clone, Copy, Debug, PartialEq, Eq)]
pub enum ClaimAbsence {
    /// This build does not know where this media type keeps its signing time.
    UnknownPayloadType,
    /// The document does not carry the field this type's signing time lives in.
    FieldAbsent,
    /// A scorecard whose `phases` list is empty. **Schema-valid**: the
    /// scorecard schema puts no `minItems` on `phases`, so a run that recorded
    /// no phase produces a document with nothing to read.
    EmptyPhases,
    /// The field is present and is not an RFC 3339 instant.
    Unparseable,
}

impl ClaimAbsence {
    /// The wire spelling, for the verification `detail`.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::UnknownPayloadType => "UnknownPayloadType",
            Self::FieldAbsent => "FieldAbsent",
            Self::EmptyPhases => "EmptyPhases",
            Self::Unparseable => "Unparseable",
        }
    }

    /// A sentence an operator can act on.
    #[must_use]
    pub const fn detail(self) -> &'static str {
        match self {
            Self::UnknownPayloadType => {
                "this build does not know where this payload type records its signing time, so \
                 the key's validity window could not be checked"
            }
            Self::FieldAbsent => {
                "the document carries no signing-time field for its payload type, so the key's \
                 validity window could not be checked"
            }
            Self::EmptyPhases => {
                "this scorecard records no phase, so it carries no signing time and the key's \
                 validity window could not be checked"
            }
            Self::Unparseable => {
                "the document's signing-time field is not an RFC 3339 instant, so the key's \
                 validity window could not be checked"
            }
        }
    }
}

impl EvidenceClaim {
    /// A claim naming an instant.
    #[must_use]
    pub const fn at(signed_at: DateTime<Utc>) -> Self {
        Self {
            signed_at: Some(signed_at),
            absence: None,
        }
    }

    /// A document that claims no signing time, for a named reason. Every
    /// window check refuses it: the claim is what the window is compared
    /// against, so its absence is not "no constraint", it is "nothing to
    /// check".
    #[must_use]
    pub const fn absent(absence: ClaimAbsence) -> Self {
        Self {
            signed_at: None,
            absence: Some(absence),
        }
    }

    /// A document that claims no signing time and offers no reason. Test
    /// shorthand; production callers use [`Self::from_document`], which always
    /// names one.
    #[must_use]
    pub const fn unknown() -> Self {
        Self {
            signed_at: None,
            absence: None,
        }
    }

    /// **The production constructor.** Reads the claim out of one document,
    /// naming the reason when there is none.
    #[must_use]
    pub fn from_document(payload_type: &str, json: &Value) -> Self {
        match read_claimed_signing_time(payload_type, json) {
            Ok(at) => Self::at(at),
            Err(absence) => Self::absent(absence),
        }
    }
}

/// The controller-written observation that a document existed at a given
/// instant.
///
/// A NAMED TYPE FOR THE SAME REASON [`EvidenceClaim`] IS ONE. The only
/// accepted observation is one this installation wrote itself — a
/// `status.evidence.verification.verifiedAt` from an earlier reconcile — and
/// the whole compromise rule turns on it not being the document's own claim.
#[derive(Deserialize, Serialize, Clone, Copy, Debug, PartialEq, Eq, Default)]
pub struct IndependentObservation {
    /// When a controller of this installation recorded having seen the
    /// document. `None` for an imported archive with no local history.
    pub observed_at: Option<DateTime<Utc>>,
}

impl IndependentObservation {
    /// An observation at `observed_at`.
    #[must_use]
    pub const fn at(observed_at: DateTime<Utc>) -> Self {
        Self {
            observed_at: Some(observed_at),
        }
    }

    /// No local history. A compromise-revoked signer then fails closed
    /// (D3 §16: "there is no independent timestamp for evidence this
    /// installation never observed").
    #[must_use]
    pub const fn none() -> Self {
        Self { observed_at: None }
    }
}

/// The key resolved against a clock, for `status.keys[].effectiveState`.
///
/// Never returns [`EffectiveState::Unparseable`] — see that variant.
#[must_use]
pub fn effective_state(key: &TrustedKey, now: DateTime<Utc>) -> EffectiveState {
    match key.state {
        KeyState::Revoked => EffectiveState::Revoked,
        KeyState::Retired => EffectiveState::Retired,
        KeyState::Active => {
            if now < key.not_before {
                EffectiveState::NotYetValid
            } else if now >= key.not_after {
                EffectiveState::Expired
            } else {
                EffectiveState::Active
            }
        }
    }
}

/// `status.keys[].usableForNewSignatures` — the window half of "may this key
/// sign something new?", with no usage in hand.
#[must_use]
pub fn usable_for_new_signatures(key: &TrustedKey, now: DateTime<Utc>) -> bool {
    matches!(effective_state(key, now), EffectiveState::Active)
}

/// `status.keys[].usableForVerification`.
#[must_use]
pub fn usable_for_verification(key: &TrustedKey, now: DateTime<Utc>) -> VerificationUse {
    match effective_state(key, now) {
        EffectiveState::Active => VerificationUse::Full,
        // A key that is not yet valid has signed nothing inside its window,
        // and a window that has not opened cannot have closed behind a
        // signature: `Historical` would be a promise about evidence that
        // cannot exist.
        EffectiveState::NotYetValid | EffectiveState::Unparseable => VerificationUse::None,
        EffectiveState::Expired | EffectiveState::Retired => VerificationUse::Historical,
        EffectiveState::Revoked => match key.reason() {
            RevocationReason::KeyCompromise => VerificationUse::None,
            RevocationReason::Superseded | RevocationReason::Unspecified => {
                VerificationUse::Historical
            }
        },
    }
}

/// Why a key may not sign something new.
///
/// FIVE VARIANTS, THE SET IS CLOSED, AND `KeyIdExpired` KEEPS ITS OLD NAME.
/// `weirkeeper::controllers::approval` writes the variant name onto
/// `status.conditions[type=Verified].reason`, and `KeyIdExpired` is the string
/// today's roster path already writes (`controllers/approval.rs`) and that
/// `docs/kubernetes.md` §8 lists among the nine. Renaming it here would change
/// a published refusal vocabulary in a commit that is about key lifecycle.
#[derive(Deserialize, Serialize, Clone, Copy, Debug, PartialEq, Eq)]
pub enum SigningRefusal {
    /// The policy carries no key with this id.
    UntrustedSigner,
    /// The key does not carry the usage this signature needs.
    KeyUsageMismatch,
    /// `now` is before `notBefore`.
    KeyNotYetValid,
    /// `now` is at or after `notAfter`.
    KeyIdExpired,
    /// The key is `Retired`.
    KeyRetired,
    /// The key is `Revoked`, for any reason.
    KeyRevoked,
}

impl SigningRefusal {
    /// The wire spelling.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::UntrustedSigner => "UntrustedSigner",
            Self::KeyUsageMismatch => "KeyUsageMismatch",
            Self::KeyNotYetValid => "KeyNotYetValid",
            Self::KeyIdExpired => "KeyIdExpired",
            Self::KeyRetired => "KeyRetired",
            Self::KeyRevoked => "KeyRevoked",
        }
    }
}

/// **May this key sign something new?** `Active` ∧ `notBefore ≤ now < notAfter`
/// ∧ the usage matches (D3 §7.4).
///
/// APPROVAL VERIFICATION AT ADMISSION IS A NEW USE. An `Approval` object that
/// arrives today, carrying a signature by a key retired last week, is asking
/// this question and not [`decide`]'s: the refusal is
/// [`SigningRefusal::KeyIdExpired`] exactly as today's roster path produces it
/// (`controllers/approval.rs` check 6). A retired approver key authorises
/// nothing new, however green the archives it signed still are.
///
/// # Errors
///
/// A [`SigningRefusal`] naming which of the four conditions failed.
pub fn may_sign_new(
    key: Option<&TrustedKey>,
    usage: KeyUsage,
    now: DateTime<Utc>,
) -> Result<(), SigningRefusal> {
    let Some(key) = key else {
        return Err(SigningRefusal::UntrustedSigner);
    };
    if !key.has_usage(usage) {
        return Err(SigningRefusal::KeyUsageMismatch);
    }
    match key.state {
        KeyState::Retired => return Err(SigningRefusal::KeyRetired),
        KeyState::Revoked => return Err(SigningRefusal::KeyRevoked),
        KeyState::Active => {}
    }
    if now < key.not_before {
        return Err(SigningRefusal::KeyNotYetValid);
    }
    if now >= key.not_after {
        return Err(SigningRefusal::KeyIdExpired);
    }
    Ok(())
}

/// **Does this existing evidence still verify?** — D3 §7.4's table, every row.
///
/// `key` is the policy entry the signature matched, `None` when the policy
/// carries no such key id. `claim` is what the DOCUMENT says about when it was
/// signed ([`claimed_signing_time`]); `observation` is what a CONTROLLER of
/// this installation wrote about having seen it. `now` is the clock, passed in.
///
/// # The rows, in the order they are decided
///
/// 1. **No key** → `Untrusted`, [`UntrustReason::UntrustedSigner`]. Checked
///    first because everything below reads the key.
/// 2. **Usage mismatch** → `Untrusted`, [`UntrustReason::KeyUsageMismatch`].
///    Before the lifecycle, deliberately: an approval key presented as an
///    evidence signer is refused for what it IS, not for when it expired.
/// 3. **Compromise revocation** → its own two rows, and the ONLY two that read
///    `observation`. With an observation strictly before
///    `revocationEffectiveFrom`: `Untrusted`,
///    [`UntrustReason::RecordedBeforeRevocation`], basis
///    [`TrustBasis::RecordedBeforeRevocation`]. Without one: `Untrusted`,
///    [`UntrustReason::Revoked`]. **The document's own claim is not read on
///    either row** — that is the whole content of the compromise rule.
/// 4. **Everything else** is a window question against
///    [`TrustedKey::accepted_through`]: a claim inside `[notBefore, notAfter)`
///    on an `Active` key is [`TrustBasis::Current`]; a claim in
///    `[notBefore, boundary]` on an expired, retired or superseded key is
///    [`TrustBasis::Historical`]; anything else — including a document with NO
///    claimed signing time — is [`UntrustReason::SignedOutsideValidity`].
///
/// # Why an absent claim refuses
///
/// Fail closed. The claim is the thing the window is compared against, so its
/// absence is not "no constraint applies", it is "the constraint cannot be
/// checked". Treating it as "inside the window" would make every document
/// whose timestamp field a future format renames verify green against a
/// retired key.
#[must_use]
pub fn decide(
    key: Option<&TrustedKey>,
    usage: KeyUsage,
    claim: &EvidenceClaim,
    observation: &IndependentObservation,
    now: DateTime<Utc>,
) -> Verdict {
    // ---- row: the policy does not carry this key -------------------------
    let Some(key) = key else {
        return Verdict::untrusted(
            UntrustReason::UntrustedSigner,
            TrustBasis::None,
            TrustKeyState::Unknown,
        );
    };

    let key_state = match effective_state(key, now) {
        EffectiveState::Active | EffectiveState::NotYetValid => TrustKeyState::Active,
        EffectiveState::Expired => TrustKeyState::Expired,
        EffectiveState::Retired => TrustKeyState::Retired,
        EffectiveState::Revoked => TrustKeyState::Revoked,
        // UNREACHABLE THROUGH `effective_state`, WHICH NEVER RETURNS IT, and
        // unreachable through a caller: a key whose PEM does not parse cannot
        // have produced the signature that selected it. `Unknown` rather than
        // a panic, because a verdict is not the place to abort a reconcile.
        EffectiveState::Unparseable => TrustKeyState::Unknown,
    };

    // ---- row: the usage does not match -----------------------------------
    if !key.has_usage(usage) {
        return Verdict::untrusted(UntrustReason::KeyUsageMismatch, TrustBasis::None, key_state);
    }

    // ---- the two compromise rows, and the ONLY reads of `observation` ----
    if key.state == KeyState::Revoked && key.reason() == RevocationReason::KeyCompromise {
        // `revocationEffectiveFrom` is what an observation is compared
        // against. Absent (impossible on a live object, G5) means there is no
        // instant before which anything was safe, so this fails closed.
        let effective = key.revocation_effective_from.or(key.revoked_at);
        let recorded_before = match (observation.observed_at, effective) {
            (Some(observed), Some(effective)) => observed < effective,
            _ => false,
        };
        return if recorded_before {
            Verdict::untrusted(
                UntrustReason::RecordedBeforeRevocation,
                TrustBasis::RecordedBeforeRevocation,
                key_state,
            )
        } else {
            Verdict::untrusted(UntrustReason::Revoked, TrustBasis::None, key_state)
        };
    }

    // ---- every remaining row is a window question ------------------------
    let refuse = || {
        Verdict::untrusted(
            UntrustReason::SignedOutsideValidity,
            TrustBasis::None,
            key_state,
        )
    };
    let Some(signed_at) = claim.signed_at else {
        return refuse();
    };
    if signed_at < key.not_before {
        return refuse();
    }
    // A CLAIM IN THE FUTURE IS NOT A CLAIM. A document cannot have been signed
    // after the instant it is being asked about, and `signed_at` is a field the
    // DOCUMENT controls: without this bound a signer may claim any instant up
    // to `notAfter` and be trusted as `Current` today. Review finding F4.
    if signed_at > now {
        return refuse();
    }
    // A KEY WHOSE WINDOW HAS NOT OPENED VERIFIES NOTHING.
    //
    // SUBSUMED TODAY BY THE BOUND ABOVE, AND KEPT ANYWAY — measured, not
    // assumed. `NotYetValid` means `now < notBefore`, and the window test
    // already required `signed_at >= notBefore`, so any claim that reaches
    // here against such a key is necessarily after `now` and the F4 bound
    // refuses it first. Planting "drop this arm" as a mutant therefore does
    // NOT kill: the recorded mutant is the realistic pair — relax the F4 bound
    // for clock skew AND drop this arm — which is exactly how the hole would
    // come back. Defence in depth on a fail-open that shipped once is worth
    // more than a tidy mutation table.
    //
    // Review finding F3:
    // §7.6 step 1 stages the successor key before the cutover, and staging it
    // with a future `notBefore` is the natural way to do that — without this
    // arm a document claiming a time inside the unopened window rendered
    // `Valid`/`Current` and GREEN, while `status.keys[]` reported
    // `usableForVerification: None` for the same key at the same instant. The
    // badge and the console disagreed and the badge was the permissive one.
    // `usable_for_verification` is the oracle the test compares against, so
    // the two surfaces cannot drift apart again.
    if matches!(effective_state(key, now), EffectiveState::NotYetValid) {
        return refuse();
    }
    // An `Active` key inside its own window, signed inside that window, is the
    // only way to `Current`. The half-open comparison matches the
    // new-signature rule's `notBefore ≤ t < notAfter`.
    if key.state == KeyState::Active && now < key.not_after && signed_at < key.not_after {
        return Verdict::valid(TrustBasis::Current, key_state);
    }
    match key.accepted_through() {
        // Inclusive at the boundary: D3 §7.4 says "signed AT OR BEFORE
        // `notAfter`/`retiredAt`".
        Some(boundary) if signed_at <= boundary => {
            Verdict::valid(TrustBasis::Historical, key_state)
        }
        _ => Verdict::untrusted(
            UntrustReason::SignedOutsideValidity,
            TrustBasis::None,
            key_state,
        ),
    }
}

// ---------------------------------------------------------------------------
// The claimed signing time
// ---------------------------------------------------------------------------

/// The `payloadType` media types whose claimed signing time this module can
/// read, WITHOUT their `;version=` parameter.
///
/// MATCHED ON THE BASE TYPE AND NOT ON THE FULL CONSTANT. The constants live
/// in `logweir-verify` (and `logweir`'s approval module), which this crate
/// does not and must not depend on — it is the pure layer, and
/// `logweir-verify` links both signature primitives. Matching the base media
/// type also means a future `;version=1.1.0` of the same document keeps
/// resolving to the same field instead of silently becoming "no claimed
/// signing time", which under [`decide`]'s fail-closed rule would refuse every
/// document of that version.
const BACKUP_RECEIPT: &str = "application/vnd.logweir.backup-receipt+json";
/// See [`BACKUP_RECEIPT`].
const SCORECARD: &str = "application/vnd.logweir.drill-scorecard+json";
/// See [`BACKUP_RECEIPT`].
const TEARDOWN: &str = "application/vnd.logweir.drill-teardown+json";
/// See [`BACKUP_RECEIPT`].
const APPROVAL: &str = "application/vnd.logweir.drill-approval+json";
/// See [`BACKUP_RECEIPT`].
const CATALOG_POINT: &str = "application/vnd.logweir.catalog-point+json";

/// The document's own latest pre-signature timestamp (D3 §7.4).
///
/// | `payloadType` | field |
/// |---|---|
/// | backup receipt | `finished_at` |
/// | drill scorecard | `phases[last].at` |
/// | drill teardown | `deleted_at` |
/// | drill approval | `approved_at` |
/// | catalog point record | `recorded_at` |
///
/// `None` for an unknown media type, a missing field or a value that is not an
/// RFC 3339 instant — and under [`decide`]'s fail-closed rule a `None` refuses
/// rather than passing.
///
/// # Why `phases[last]` and not `phases[0]` or a named phase
///
/// "Latest pre-signature timestamp" is the property: the scorecard is signed
/// after its last phase is recorded, so the last phase's `at` is the closest
/// instant to the signature the document itself carries. It is read by
/// POSITION rather than by phase name because the phase vocabulary is the
/// engine's and grows; a named phase would make a scorecard from a run that
/// skipped it claim no signing time and fail closed for a reason that has
/// nothing to do with trust.
///
/// # This is a CLAIM
///
/// It is read out of the document, so it is exactly as trustworthy as the key
/// that signed the document — which is why [`decide`] does not consult it on
/// the compromise rows.
#[must_use]
pub fn claimed_signing_time(payload_type: &str, json: &Value) -> Option<DateTime<Utc>> {
    read_claimed_signing_time(payload_type, json).ok()
}

/// [`claimed_signing_time`], with the absence NAMED — review finding F7.
///
/// # Errors
///
/// A [`ClaimAbsence`] saying which of the four things went wrong. An
/// **empty `phases`** is its own variant and not a missing field: the
/// scorecard schema puts no `minItems` on `phases`, so a run that recorded no
/// phase writes a schema-valid document with no signing time in it, and
/// "this scorecard records no phase" is a different thing for an operator to
/// read than "the field is absent".
pub fn read_claimed_signing_time(
    payload_type: &str,
    json: &Value,
) -> Result<DateTime<Utc>, ClaimAbsence> {
    let base = payload_type.split(';').next().unwrap_or("").trim();
    let value = match base {
        BACKUP_RECEIPT => json.get("finished_at"),
        SCORECARD => {
            let phases = json
                .get("phases")
                .and_then(Value::as_array)
                .ok_or(ClaimAbsence::FieldAbsent)?;
            let last = phases.last().ok_or(ClaimAbsence::EmptyPhases)?;
            last.get("at")
        }
        TEARDOWN => json.get("deleted_at"),
        APPROVAL => json.get("approved_at"),
        CATALOG_POINT => json.get("recorded_at"),
        _ => return Err(ClaimAbsence::UnknownPayloadType),
    };
    let text = value
        .ok_or(ClaimAbsence::FieldAbsent)?
        .as_str()
        .ok_or(ClaimAbsence::Unparseable)?;
    parse_rfc3339(text).ok_or(ClaimAbsence::Unparseable)
}

/// One RFC 3339 instant, in UTC.
fn parse_rfc3339(s: &str) -> Option<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(s)
        .ok()
        .map(|t| t.with_timezone(&Utc))
}
