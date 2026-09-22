//! Pure names for the controller-to-runner Restore execution contract.
//!
//! The Kubernetes Job template is immutable after creation.  New Restore Jobs
//! therefore carry the exact public input digests in environment variables;
//! the runner compares them with the bytes it read from projected volumes
//! before it constructs any data-plane client.  Keeping the names here makes
//! the controller and runner share one wire contract without introducing a
//! dependency between their crates.
//!
//! # Version 2 (decision D3 §8, Amendment I)
//!
//! [`VERSION`] is `"2"`. The bump is the whole point of the version existing:
//! v2 carries three things a v1 runner would silently not do — the recovery
//! **point binding** (D3 §5.5), the **standing rehearsal authorization**
//! (D3 §4.3) and the **progress / teardown stdout lines** (D3 §2.4) — so an
//! old binary must refuse the new argument *before dispatch* rather than run a
//! plan whose checks it does not implement. It does: the argv handshake is
//! `VERSION_ARG` and an old build's exact-match on `"1"` rejects `"2"`.
//!
//! **Additive, and v1 documents still load and verify byte for byte.** Every
//! v2 field is a new environment variable or a new optional block in an
//! existing document; nothing v1 wrote changes shape, and no v1 digest moves.
//! What v1 does NOT get is permission to carry v2 material — see
//! [`ContractVersion`] for the exact rule and the reason it is not a courtesy.

use crate::rehearsal_scope::{RehearsalScope, MODE_SCRATCH};
use serde::{Deserialize, Serialize};

/// The execution contract this build implements and the controller stamps
/// into every new Restore Job.
pub const VERSION: &str = "2";
/// The tag-1 contract. Still *accepted* by this build, under the one narrow
/// transition [`ContractVersion::V1`] documents.
pub const VERSION_V1: &str = "1";
/// The current contract, spelled out so a reader comparing against a literal
/// does not have to know whether [`VERSION`] has moved on.
pub const VERSION_V2: &str = "2";
pub const VERSION_ARG: &str = "--execution-contract-version";

pub const VERSION_ENV: &str = "LOGWEIR_EXECUTION_CONTRACT_VERSION";
pub const SUBJECT_API_VERSION_ENV: &str = "LOGWEIR_EXECUTION_SUBJECT_API_VERSION";
pub const SUBJECT_KIND_ENV: &str = "LOGWEIR_EXECUTION_SUBJECT_KIND";
pub const SUBJECT_NAME_ENV: &str = "LOGWEIR_EXECUTION_SUBJECT_NAME";
pub const SUBJECT_NAMESPACE_ENV: &str = "LOGWEIR_EXECUTION_SUBJECT_NAMESPACE";
pub const SUBJECT_UID_ENV: &str = "LOGWEIR_EXECUTION_SUBJECT_UID";
pub const APPROVAL_NAME_ENV: &str = "LOGWEIR_EXECUTION_APPROVAL_NAME";
pub const APPROVAL_UID_ENV: &str = "LOGWEIR_EXECUTION_APPROVAL_UID";
pub const PLAN_SHA256_ENV: &str = "LOGWEIR_EXECUTION_PLAN_SHA256";
pub const APPROVAL_SHA256_ENV: &str = "LOGWEIR_EXECUTION_APPROVAL_SHA256";
pub const APPROVAL_SIDECAR_SHA256_ENV: &str = "LOGWEIR_EXECUTION_APPROVAL_SIDECAR_SHA256";
pub const APPROVER_KEY_SHA256_ENV: &str = "LOGWEIR_EXECUTION_APPROVER_KEY_SHA256";
pub const ALLOWED_CLUSTERS_SHA256_ENV: &str = "LOGWEIR_EXECUTION_ALLOWED_CLUSTERS_SHA256";

/// The MANDATORY core of an APPROVAL-authorized run, unchanged from v1 and
/// all-or-nothing in both versions.
///
/// It stays thirteen on purpose. A v2 variable added here would make every v1
/// Job's environment "incomplete" the moment this binary shipped, which is the
/// opposite of additive — the v2 additions live in [`V2_ENV`] and are
/// conditional by block.
///
/// **PLAT-14.3b: two of the thirteen are the per-run approval's.**
/// [`APPROVAL_SHA256_ENV`] and [`APPROVAL_SIDECAR_SHA256_ENV`] pin a bundle
/// member a standing rehearsal has no slot for, so a `standing` contract omits
/// them and a `standing` contract that pins them is refused. The eleven a run
/// carries WHATEVER authorized it are [`STANDING_MANDATORY_ENV`]; this array
/// stays the approval path's complete list, which is what every v1 Job and
/// every ordinary Restore still emits.
pub const ALL_ENV: [&str; 13] = [
    VERSION_ENV,
    SUBJECT_API_VERSION_ENV,
    SUBJECT_KIND_ENV,
    SUBJECT_NAME_ENV,
    SUBJECT_NAMESPACE_ENV,
    SUBJECT_UID_ENV,
    APPROVAL_NAME_ENV,
    APPROVAL_UID_ENV,
    PLAN_SHA256_ENV,
    APPROVAL_SHA256_ENV,
    APPROVAL_SIDECAR_SHA256_ENV,
    APPROVER_KEY_SHA256_ENV,
    ALLOWED_CLUSTERS_SHA256_ENV,
];

/// The mandatory names a STANDING-authorized run carries — [`ALL_ENV`] minus
/// the two per-run approval digests (PLAT-14.3b).
///
/// `APPROVAL_NAME_ENV` and `APPROVAL_UID_ENV` STAY: they name the standing
/// `Approval`, which is a real object a human signed and the one an auditor
/// looks up. What goes is only the pair that pins bundle members a rehearsal
/// does not have.
pub const STANDING_MANDATORY_ENV: [&str; 11] = [
    VERSION_ENV,
    SUBJECT_API_VERSION_ENV,
    SUBJECT_KIND_ENV,
    SUBJECT_NAME_ENV,
    SUBJECT_NAMESPACE_ENV,
    SUBJECT_UID_ENV,
    APPROVAL_NAME_ENV,
    APPROVAL_UID_ENV,
    PLAN_SHA256_ENV,
    APPROVER_KEY_SHA256_ENV,
    ALLOWED_CLUSTERS_SHA256_ENV,
];

// ---------------------------------------------------------------------------
// v2 additions (D0 "bundle contract v2", D3 §4.3 and §5.5)
// ---------------------------------------------------------------------------

/// Which authorization this run was created under: [`AUTHORIZATION_KIND_APPROVAL`]
/// (the per-run `Approval`, tag 1's only shape) or [`AUTHORIZATION_KIND_STANDING`]
/// (D3 §4.3's standing rehearsal authorization).
///
/// **Absent means `approval`.** That is the compatible direction and the
/// fail-closed one at the same time: a standing-authorized run *needs* the
/// scope block to be admitted at all, so an environment that forgot to say
/// `standing` gets the ordinary path and the scope file it mounted is refused
/// as unexpected, rather than a scope check being silently skipped.
pub const AUTHORIZATION_KIND_ENV: &str = "LOGWEIR_EXECUTION_AUTHORIZATION_KIND";
/// `sha256:<hex>` over the exact mounted **standing rehearsal authorization
/// document** — the envelope the approver signed, from which the scope is
/// derived. Same shape and same reason as `PLAN_SHA256_ENV`.
///
/// It replaced a bare `LOGWEIR_EXECUTION_SCOPE_SHA256` over a raw
/// [`RehearsalScope`], and the replacement is the whole point: a scope whose
/// only provenance is a digest the CONTROLLER set is a scope the controller
/// could mint, and D3 §4.3's own first sentence names "a controller that could
/// mint its own authorization" as the bypass PLAT-19.2 exists to prevent.
pub const AUTHORIZATION_SHA256_ENV: &str = "LOGWEIR_EXECUTION_AUTHORIZATION_SHA256";
/// `sha256:<hex>` over the DSSE sidecar carrying the approver's signature over
/// those exact envelope bytes.
pub const AUTHORIZATION_SIDECAR_SHA256_ENV: &str = "LOGWEIR_EXECUTION_AUTHORIZATION_SIDECAR_SHA256";
/// `sha256:<hex>` over the mounted [`AuthorizationKeyring`] — D3 §4.3(e)'s
/// "the trusted public keys", the anchor the signature is checked against.
pub const AUTHORIZATION_KEYS_SHA256_ENV: &str = "LOGWEIR_EXECUTION_AUTHORIZATION_KEYS_SHA256";
/// The `RehearsalSchedule` UID the standing authorization names as its
/// subject. Carried so the runner's refusal can say WHICH schedule's
/// authorization it was executing under without parsing the document.
pub const REHEARSAL_SCHEDULE_UID_ENV: &str = "LOGWEIR_EXECUTION_REHEARSAL_SCHEDULE_UID";
/// `sha256:<hex>` over the mounted approval-policy snapshot (D0: "policy
/// snapshot/digest"). **PLAT-19.2 fills this half**; this contract only has to
/// be able to carry it, and to refuse a v1 invocation that tries to.
pub const POLICY_SNAPSHOT_SHA256_ENV: &str = "LOGWEIR_EXECUTION_POLICY_SNAPSHOT_SHA256";
/// `sha256:<hex>` over the mounted confirmation-issuer public key — the second
/// of D0's "both public keys". `APPROVER_KEY_SHA256_ENV` is the first.
pub const CONFIRMATION_KEY_SHA256_ENV: &str = "LOGWEIR_EXECUTION_CONFIRMATION_KEY_SHA256";

/// `sha256:<hex>` over the mounted [`EvidenceKeyring`] — the anchor the
/// runner verifies a bound recovery point's receipt signature against (D3
/// §5.5 step 6, RUNNER-POINT-BINDING-SKIPS-SIGNATURE). Pinned exactly when
/// the plan carries `source.point`.
pub const EVIDENCE_KEYS_SHA256_ENV: &str = "LOGWEIR_EXECUTION_EVIDENCE_KEYS_SHA256";

/// Every v2-only variable. **Each is optional**, in the blocks
/// [`ContractVersion`] documents; none of them may appear under v1.
pub const V2_ENV: [&str; 8] = [
    AUTHORIZATION_KIND_ENV,
    AUTHORIZATION_SHA256_ENV,
    AUTHORIZATION_SIDECAR_SHA256_ENV,
    AUTHORIZATION_KEYS_SHA256_ENV,
    REHEARSAL_SCHEDULE_UID_ENV,
    POLICY_SNAPSHOT_SHA256_ENV,
    CONFIRMATION_KEY_SHA256_ENV,
    EVIDENCE_KEYS_SHA256_ENV,
];

/// The union, and what a reader uses to answer "is there a contract in this
/// environment at all".
///
/// It has to be the union rather than [`ALL_ENV`]: a Job that set only v2
/// variables — a controller bug, or a partially applied template — must be
/// diagnosed as an incomplete contract, not treated as a credential-free
/// standalone invocation that runs with no contract checks whatsoever.
pub const ALL_ENV_ANY: [&str; 21] = [
    VERSION_ENV,
    SUBJECT_API_VERSION_ENV,
    SUBJECT_KIND_ENV,
    SUBJECT_NAME_ENV,
    SUBJECT_NAMESPACE_ENV,
    SUBJECT_UID_ENV,
    APPROVAL_NAME_ENV,
    APPROVAL_UID_ENV,
    PLAN_SHA256_ENV,
    APPROVAL_SHA256_ENV,
    APPROVAL_SIDECAR_SHA256_ENV,
    APPROVER_KEY_SHA256_ENV,
    ALLOWED_CLUSTERS_SHA256_ENV,
    AUTHORIZATION_KIND_ENV,
    AUTHORIZATION_SHA256_ENV,
    AUTHORIZATION_SIDECAR_SHA256_ENV,
    AUTHORIZATION_KEYS_SHA256_ENV,
    REHEARSAL_SCHEDULE_UID_ENV,
    POLICY_SNAPSHOT_SHA256_ENV,
    CONFIRMATION_KEY_SHA256_ENV,
    EVIDENCE_KEYS_SHA256_ENV,
];

/// The `--triggered-by` prefix a standing-authorized rehearsal run carries.
///
/// # Why `triggered_by` changes shape at all
///
/// `validate_execution_contract` binds `--triggered-by` to the authorization
/// the immutable Job template names, and for an ordinary `Restore` that is
/// `approval/<name>`. A rehearsal's reason is not an approval a human clicked
/// for this run — it is a SLOT of a schedule — and the value is copied verbatim
/// into the signed scorecard's `triggered_by`, which is what an auditor reads
/// to find out why the run happened. `approval/<standing approval>` would be
/// true and useless: every slot of every schedule under one standing document
/// would read identically, and neither the schedule nor the slot would appear
/// anywhere in the signed evidence.
///
/// # The shape, and who checks which half
///
/// `rehearsal/<schedule name>/<slot>`. The runner checks the SHAPE in
/// `validate_execution_contract` (before anything is parsed) and binds the
/// `<schedule name>` segment to the standing document's own
/// `subjectRef.name` only after that document's signature verifies — so the
/// schedule this run claims to be is pinned by SIGNED bytes, not by an
/// environment variable the controller sets.
pub const TRIGGERED_BY_REHEARSAL_PREFIX: &str = "rehearsal/";

/// `--triggered-by` for one rehearsal slot, built in exactly one place so the
/// controller that emits it and the runner that parses it cannot disagree.
#[must_use]
pub fn rehearsal_triggered_by(schedule: &str, slot: &str) -> String {
    format!("{TRIGGERED_BY_REHEARSAL_PREFIX}{schedule}/{slot}")
}

/// The `(schedule, slot)` inside a rehearsal trigger, or `None` when the value
/// is not one.
///
/// Both segments must be non-empty: `rehearsal//x` names no schedule and
/// `rehearsal/x/` names no slot, and either would make the binding below
/// vacuous. A slot never contains `/` ([`crate::ids`]-shaped
/// `YYYYmmddThhmmss`), so the split is unambiguous.
#[must_use]
pub fn parse_rehearsal_triggered_by(value: &str) -> Option<(&str, &str)> {
    let rest = value.strip_prefix(TRIGGERED_BY_REHEARSAL_PREFIX)?;
    let (schedule, slot) = rest.split_once('/')?;
    if schedule.is_empty() || slot.is_empty() || slot.contains('/') {
        return None;
    }
    Some((schedule, slot))
}

pub const AUTHORIZATION_KIND_APPROVAL: &str = "approval";
pub const AUTHORIZATION_KIND_STANDING: &str = "standing";

/// WHICH authorization a run executes under, as a closed set rather than a
/// string a reader has to interpret.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum AuthorizationKind {
    /// The per-run `Approval` and its DSSE sidecar. Tag 1's only shape, and
    /// what an absent [`AUTHORIZATION_KIND_ENV`] means.
    #[default]
    Approval,
    /// D3 §4.3's standing rehearsal authorization: one signed scope, proven to
    /// contain each slot's rendered plan, twice — once by the controller
    /// before the `Restore` exists, once by the runner against the mounted
    /// bundle before any data-plane client is constructed.
    Standing,
}

impl AuthorizationKind {
    #[must_use]
    pub fn parse(value: &str) -> Option<Self> {
        match value {
            AUTHORIZATION_KIND_APPROVAL => Some(Self::Approval),
            AUTHORIZATION_KIND_STANDING => Some(Self::Standing),
            _ => None,
        }
    }

    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Approval => AUTHORIZATION_KIND_APPROVAL,
            Self::Standing => AUTHORIZATION_KIND_STANDING,
        }
    }
}

/// The v1/v2 decision, **explicit in the type** so no call site can carry the
/// version as a `String` and compare it with a literal it spelled itself.
///
/// # What v1 still buys, and what it does not
///
/// D0: "existing bundle v1 remains accepted only for already-created legacy
/// governed Restores under the documented transition", and D3 §8 Amendment I
/// repeats it. A runner cannot read a `Restore`'s creation timestamp — it
/// holds no cluster credential at all — so "already-created legacy" is decided
/// from the only thing the runner can see: **a v1 invocation may carry no v2
/// material.** No point binding in the plan, no standing authorization, no
/// policy snapshot, no confirmation key. Those are exactly the things a
/// Restore created *after* the v2 rollout has, so a v1 contract carrying any
/// of them is a new Restore wearing an old version number, and it is refused.
///
/// This is the rule the mutant "v1 accepted for a new Restore" has to break,
/// and it is checkable with no clock and no API server.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ContractVersion {
    V1,
    V2,
}

impl ContractVersion {
    /// The wire value, or `None` for a version this build does not implement.
    ///
    /// `None` and not a defaulted `V1`: an unknown version means the
    /// controller is newer than the runner, and guessing downward would run a
    /// plan under checks its author did not ask for.
    #[must_use]
    pub fn parse(value: &str) -> Option<Self> {
        match value {
            VERSION_V1 => Some(Self::V1),
            VERSION_V2 => Some(Self::V2),
            _ => None,
        }
    }

    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::V1 => VERSION_V1,
            Self::V2 => VERSION_V2,
        }
    }

    /// Whether this version may carry any of the v2 material at all.
    #[must_use]
    pub fn carries_v2_material(self) -> bool {
        matches!(self, Self::V2)
    }

    /// The refusal a v1 invocation earns when it carries `what`, or `None`
    /// when the pairing is legal.
    ///
    /// Returns the message rather than a bare `bool` so every call site refuses
    /// in the same words, and so the words name the remedy: a legacy Restore is
    /// finished or deleted, it is not upgraded in place.
    #[must_use]
    pub fn refuse_v2_material(self, what: &str) -> Option<String> {
        match self {
            Self::V2 => None,
            Self::V1 => Some(format!(
                "execution contract v1 cannot carry {what}: v1 is accepted only for Restores \
                 created before the contract v2 rollout, and those carry none of it. Let the \
                 in-flight legacy Restore finish (or delete it) and create the new one, which \
                 the controller stamps as v{VERSION_V2}; no data operation was started"
            )),
        }
    }
}

impl std::fmt::Display for ContractVersion {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

// ---------------------------------------------------------------------------
// The recovery point bound into plan bytes (D3 §5.1 / §5.5)
// ---------------------------------------------------------------------------

/// `source.point` — the recovery point a v2 restore plan is bound to.
///
/// # Why it is in the PLAN and not in the environment
///
/// The environment is the controller's word for it; the plan is what the
/// approver signed. Binding the point into plan bytes means the approval
/// covers *which archive object this restore recovers from*, and PLAT-15.2's
/// disaster path — a fresh installation with no `Backup` CR anywhere — has
/// nothing else to bind to. The runner re-derives the identity from the bytes
/// it actually read, so a point that was swapped underneath an approved plan
/// is a digest mismatch rather than a quiet substitution.
///
/// # Absent means v1's behaviour, exactly
///
/// `#[serde(default)]` on the field that holds it: a plan with no `point`
/// block selects its archive set the way every plan did before this existed
/// (`source.backup`, `latestCompleted` or a pinned id) and the runner performs
/// no binding check. That is not a weaker mode for the same run — it is the
/// only mode a pre-catalog plan can express.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PointBinding {
    /// `lwp1-` + 32 lowercase hex characters, content-derived from the signed
    /// receipt (D3 §5.1). A DISPLAY and lookup key; `receipt_sha256` is the
    /// binding.
    pub point_id: String,
    /// The object key of the signed backup receipt, under `logweir/backups/`.
    pub receipt_key: String,
    /// `sha256:<hex>` over the exact stored receipt bytes.
    pub receipt_sha256: String,
    /// `sha256:<hex>` over the manifest bytes the receipt itself attests
    /// (`BackupReceipt.archive.manifest_sha256`). Checked so a receipt that is
    /// intact but describes a different archive cannot pass.
    pub manifest_sha256: String,
}

// ---------------------------------------------------------------------------
// The SIGNED standing rehearsal authorization (D3 §4.3(e), D0 authorization
// document v2)
// ---------------------------------------------------------------------------

/// The DSSE payload type the standing rehearsal authorization is signed under.
///
/// Its own type, not the drill approval's: a payload type is part of what
/// `verify_detached` checks, so a genuinely signed *approval* replayed as a
/// standing authorization is refused by the signature layer rather than by a
/// field comparison somebody could forget to write.
pub const PAYLOAD_TYPE_STANDING_AUTHORIZATION: &str =
    "application/vnd.logweir.standing-rehearsal-authorization+json;version=1.0.0";

/// The document format this build reads. A MAJOR bump is a refusal; a minor
/// adds optional fields only (GC12's rule, applied to this document).
pub const STANDING_AUTHORIZATION_FORMAT_VERSION: &str = "1.0.0";
/// `kind` — inside the signed bytes, so it cannot be relabelled.
pub const STANDING_AUTHORIZATION_KIND: &str = "StandingRehearsalAuthorization";
/// The only subject kind a standing rehearsal authorization may name.
pub const REHEARSAL_SCHEDULE_KIND: &str = "RehearsalSchedule";
/// The API group/version every subject of this document belongs to.
pub const SUBJECT_API_VERSION: &str = "logweir.dev/v1alpha1";
/// D3 §4.3: `expiresAt - issuedAt <= 90 days`. Re-checked at the runner
/// because it is a property of the SIGNED bytes: a document minted with a
/// ten-year life is one the decision does not permit, whoever minted it.
pub const MAX_STANDING_AUTHORIZATION_DAYS: i64 = 90;

/// What the standing authorization names as its subject (D3 §4.3, §4.5).
///
/// The UID is the field that matters. It is what turns
/// `LOGWEIR_EXECUTION_REHEARSAL_SCHEDULE_UID` — a label the controller sets —
/// into a binding the runner can actually check: the environment says which
/// schedule this run claims to be, the SIGNED document says which schedule the
/// human authorised, and the runner requires them to agree.
#[derive(Deserialize, Serialize, Clone, Debug, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct AuthorizationSubject {
    pub api_version: String,
    pub kind: String,
    pub namespace: String,
    pub name: String,
    pub uid: String,
}

/// **The document a human signs once so an unattended rehearsal can run many
/// times** (D3 §4.3(e)).
///
/// # Why the runner reads THIS and not a bare scope
///
/// The first shape of this contract mounted a raw [`RehearsalScope`] pinned by
/// a digest in the pod template. That digest is set by the controller, so
/// against the adversary §4.3 actually names — "a controller that could mint
/// its own authorization" — the runner's half of "checked twice" proved
/// nothing at all. §4.3(e) says the bundle carries "the authorization
/// document, its signatures, the trusted public keys, the scope and the
/// rendered plan"; this is that document, the scope is INSIDE it, and the
/// runner derives the scope only after the signature over these exact bytes
/// verifies under a pinned key that is allowed to authorise.
///
/// # The shape, and who owns it
///
/// D0's authorization document v2 fixes the family (`formatVersion`, a
/// `subject` block with a UID, `issuedAt`/`expiresAt`, a DSSE sidecar over the
/// exact bytes). PLAT-19.2 has not fixed the standing variant, so this is the
/// minimal document D3 §4.3/§4.5 requires, and it is the contract W7 and
/// PLAT-19.2 must PRODUCE. Fields PLAT-19.2 adds later (`policy`, `requester`,
/// `ticket`, a second signature) are additive: unknown keys are ignored on
/// read, so a document carrying them still verifies here.
#[derive(Deserialize, Serialize, Clone, Debug, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct StandingAuthorization {
    pub format_version: String,
    pub kind: String,
    pub subject_ref: AuthorizationSubject,
    /// What the signature actually authorises. Read only after it verified.
    pub scope: RehearsalScope,
    pub issued_at: chrono::DateTime<chrono::Utc>,
    pub expires_at: chrono::DateTime<chrono::Utc>,
}

/// One key the bundle presents as able to authorise a rehearsal.
///
/// It carries KEY MATERIAL, which is what makes it different from
/// [`crate::trust::TrustedKey`] — that type is the POLICY record and holds no
/// material by design. This is the controller's projection of the keys its
/// `TrustPolicy` resolution already accepted, rendered into the bundle so the
/// runner can check a signature without holding a cluster credential.
///
/// **Lifecycle stays the controller's.** `state`, `notBefore`/`notAfter` and
/// revocation are `trust::decide`'s to evaluate, and they are evaluated before
/// this keyring is written. What the runner re-checks is what it can: that the
/// signature verifies under a key the controller PINNED, and that the key
/// carries a usage allowed to authorise.
#[derive(Deserialize, Serialize, Clone, Debug, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct AuthorizationKey {
    /// The sha256 of the DER SPKI, lowercase hex — the same number `openssl`
    /// prints (`docs/keys.md`).
    pub key_id: String,
    /// PEM SPKI. Public material only; a private key here would be a defect
    /// this type cannot express a use for.
    pub public_key_pem: String,
    pub usages: Vec<crate::trust::KeyUsage>,
}

impl AuthorizationKey {
    /// Whether this key may authorise a rehearsal at all.
    ///
    /// The standing document/bundle format carries no approval-policy mode —
    /// PLAT-19.2 carries one end to end for per-run Restores only
    /// (`crate::approval_policy`) — so only
    /// [`crate::trust::KeyUsage::GovernedApproval`] may authorize a rehearsal,
    /// under every policy. `ConsoleConfirmation` fails closed here;
    /// `EvidenceSigning` never authorizes.
    #[must_use]
    pub fn may_authorize(&self) -> bool {
        self.usages
            .contains(&crate::trust::KeyUsage::GovernedApproval)
    }
}

/// D3 §4.3(e)'s "the trusted public keys", as a mounted bundle member.
#[derive(Deserialize, Serialize, Clone, Debug, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct AuthorizationKeyring {
    pub format_version: String,
    pub keys: Vec<AuthorizationKey>,
}

/// The evidence-signing keys a restore Job verifies a bound recovery point's
/// receipt against — D3 §5.5 step 6's "the mounted trust bundle for
/// `EvidenceSigning`", as a mounted bundle member.
///
/// # Public material and LIFECYCLE, because the runner decides
///
/// Unlike [`AuthorizationKeyring`], whose lifecycle the controller evaluates
/// before it writes the keyring, this one carries each key's whole
/// [`crate::trust::TrustedKey`] record: the runner judges the receipt with
/// [`crate::trust::decide`] — the one rule the controller applies to Backup
/// evidence — against the receipt's OWN claimed signing time, which only the
/// runner has read. So a retired key still verifies what it signed while it
/// was valid, a key revoked for compromise verifies nothing, and a key the
/// namespace's trust does not list is refused by being absent. The controller
/// renders every key of the namespace's resolved trust whose public half
/// parses and whose declared id is its own; `decide` checks the usage.
#[derive(Deserialize, Serialize, Clone, Debug, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct EvidenceKeyring {
    /// [`EVIDENCE_KEYRING_FORMAT_VERSION`].
    pub format_version: String,
    /// The keys, in the trust source's order.
    pub keys: Vec<EvidenceKey>,
}

/// One key of an [`EvidenceKeyring`].
#[derive(Deserialize, Serialize, Clone, Debug, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct EvidenceKey {
    /// PEM SPKI. Public material only.
    pub public_key_pem: String,
    /// The key's identity, usages and lifecycle, as the namespace's trust
    /// resolved them when the bundle was rendered.
    pub trust: crate::trust::TrustedKey,
}

/// The one format an [`EvidenceKeyring`] is written in.
pub const EVIDENCE_KEYRING_FORMAT_VERSION: &str = "1.0.0";

/// The token a standing-authorization refusal opens with.
///
/// The two spellings are D3 §4.3's own controller skip reasons, reused so the
/// controller's `status.lastSkipped.reason` and the runner's refusal say the
/// same word about the same fault.
pub const AUTHORIZATION_INVALID: &str = "AuthorizationInvalid";
/// The expiry half of the pair.
pub const AUTHORIZATION_EXPIRED: &str = "AuthorizationExpired";

/// Why a standing authorization was refused, with the token a reader greps for.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuthorizationRefusal {
    pub token: &'static str,
    pub detail: String,
}

impl AuthorizationRefusal {
    fn invalid(detail: String) -> Self {
        Self {
            token: AUTHORIZATION_INVALID,
            detail,
        }
    }
    fn expired(detail: String) -> Self {
        Self {
            token: AUTHORIZATION_EXPIRED,
            detail,
        }
    }
}

impl std::fmt::Display for AuthorizationRefusal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}. {}", self.token, self.detail)
    }
}

/// Everything about a VERIFIED standing authorization that is decidable
/// without a broker, a bucket or a cluster credential — **except the
/// signature**, which is `crates/logweir`'s to check because this crate links
/// no crypto (Global Constraint 1, `scripts/check-pure-core.sh`).
///
/// `now` is an ARGUMENT and never a clock read here, for the same reason every
/// other predicate in this crate takes one.
///
/// # What it does NOT check, and who does
///
/// `scope.template_digest` — whether the scope matches the schedule's sealed
/// spec — is the CONTROLLER's each-slot check (D3 §4.3(a)). It is recomputed
/// from the `RehearsalSchedule`'s own spec, which a runner holding no cluster
/// credential cannot read at all. The runner's half is the subject UID, which
/// it can compare because both sides are in hand.
pub fn admit_standing_authorization(
    doc: &StandingAuthorization,
    expected_schedule_uid: Option<&str>,
    now: chrono::DateTime<chrono::Utc>,
) -> Result<(), AuthorizationRefusal> {
    let major = doc
        .format_version
        .split('.')
        .next()
        .unwrap_or_default()
        .to_string();
    if major != "1" {
        return Err(AuthorizationRefusal::invalid(format!(
            "the standing authorization declares formatVersion {:?}; this build reads major 1 \
             ({STANDING_AUTHORIZATION_FORMAT_VERSION})",
            doc.format_version
        )));
    }
    if doc.kind != STANDING_AUTHORIZATION_KIND {
        return Err(AuthorizationRefusal::invalid(format!(
            "the signed document declares kind {:?}, not {STANDING_AUTHORIZATION_KIND:?}",
            doc.kind
        )));
    }
    if doc.subject_ref.api_version != SUBJECT_API_VERSION
        || doc.subject_ref.kind != REHEARSAL_SCHEDULE_KIND
    {
        return Err(AuthorizationRefusal::invalid(format!(
            "the signed document authorises {}/{}, not a {SUBJECT_API_VERSION} \
             {REHEARSAL_SCHEDULE_KIND}",
            doc.subject_ref.api_version, doc.subject_ref.kind
        )));
    }
    if doc.subject_ref.uid.trim().is_empty() {
        return Err(AuthorizationRefusal::invalid(
            "the signed document names no subject UID, so it cannot be bound to a schedule"
                .to_string(),
        ));
    }
    // THE BINDING. Without it a document signed for one schedule authorises
    // every schedule in the namespace, and the environment's UID is decoration.
    match expected_schedule_uid {
        None => {
            return Err(AuthorizationRefusal::invalid(format!(
                "the execution contract names no RehearsalSchedule UID, so the signed \
                 authorization for {} cannot be bound to this run",
                doc.subject_ref.uid
            )))
        }
        Some(uid) if uid != doc.subject_ref.uid => {
            return Err(AuthorizationRefusal::invalid(format!(
                "the signed authorization is for RehearsalSchedule UID {}, but this run is \
                 RehearsalSchedule UID {uid}",
                doc.subject_ref.uid
            )))
        }
        Some(_) => {}
    }
    if doc.expires_at <= doc.issued_at {
        return Err(AuthorizationRefusal::invalid(format!(
            "the signed authorization expires at {} , at or before it was issued at {}",
            doc.expires_at.to_rfc3339(),
            doc.issued_at.to_rfc3339()
        )));
    }
    if (doc.expires_at - doc.issued_at).num_days() > MAX_STANDING_AUTHORIZATION_DAYS {
        return Err(AuthorizationRefusal::invalid(format!(
            "the signed authorization runs {} days, and D3 §4.3 caps a standing rehearsal \
             authorization at {MAX_STANDING_AUTHORIZATION_DAYS}",
            (doc.expires_at - doc.issued_at).num_days()
        )));
    }
    if doc.issued_at > now {
        return Err(AuthorizationRefusal::invalid(format!(
            "the signed authorization is not valid until {}; it is now {}",
            doc.issued_at.to_rfc3339(),
            now.to_rfc3339()
        )));
    }
    if doc.expires_at <= now {
        return Err(AuthorizationRefusal::expired(format!(
            "the signed authorization expired at {}; it is now {}",
            doc.expires_at.to_rfc3339(),
            now.to_rfc3339()
        )));
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// The progress channel (D3 §2.4), contract-versioned and bounded
// ---------------------------------------------------------------------------

/// `progress-contract=<version>` — the ONE line that says which grammar the
/// `progress-phase=` lines that follow are written in.
///
/// D3 §2.4 calls the channel "contract-versioned" and does not put a version
/// inside the per-phase line, because the per-phase line is read by prefix out
/// of a bounded tail and every byte of it is budget. So the version is
/// announced once, at the top of the run, and the per-phase grammar stays
/// exactly `<n>:<name>`.
pub const PROGRESS_CONTRACT_PREFIX: &str = "progress-contract=";
/// `progress-phase=<n>:<name>` (D3 §2.4).
pub const PROGRESS_PHASE_PREFIX: &str = "progress-phase=";
/// The hard cap on a whole progress line, including the prefix.
///
/// It exists because the controller reads pod logs with
/// `LogParams{tail_lines: 50, limit_bytes: 65536}` and a line that could grow
/// without bound could push every evidence key out of that window — the
/// progress channel is explicitly optional, and it may never cost a reader the
/// lines that are not.
pub const PROGRESS_LINE_MAX_BYTES: usize = 96;
/// The longest phase name the channel will render.
pub const PROGRESS_NAME_MAX_BYTES: usize = 32;

/// **The restore runner's ten phases, by number and name — the CLOSED
/// vocabulary of the progress channel.**
///
/// D3 §2.4: "`logweir restore run`: existing phases `0..9` with their existing
/// names". They are listed here, in the pure layer, because the channel is a
/// filter (see [`progress_phase_line`]) and a filter needs something to filter
/// against. A charset rule alone would not do: `hunter2` is lowercase
/// alphanumeric, and a channel that could render it could render a projected
/// password.
///
/// `crates/logweir/tests/progress_channel.rs` pins this table against the
/// `drill::record` call sites, so a renamed phase is a failing test rather
/// than a phase that silently stops being announced.
pub const RESTORE_PHASE_NAMES: [(i8, &str); 10] = [
    (0, "admit"),
    (1, "approval"),
    (2, "target-ready"),
    (3, "target-diff"),
    (4, "sample-select"),
    (5, "preflight"),
    (6, "restore"),
    (7, "verify"),
    (8, "score-and-sign"),
    (9, "teardown"),
];

/// **The backup runner's five named steps, all at phase `-1`.**
///
/// D3 §2.4: the backup path "has no numbered phases after admission", so it
/// announces `-1:admit` and then `engine`, `readback`, `sign`, `upload`.
/// Inventing `0..4` here would put two unrelated numbering schemes on one
/// channel and leave a controller unable to tell which runner a
/// `progress-phase=2:` line came from.
pub const BACKUP_STEP_NAMES: [&str; 5] = ["admit", "engine", "readback", "sign", "upload"];

/// The `progress-contract=` line for `version`.
#[must_use]
pub fn progress_contract_line(version: ContractVersion) -> String {
    format!("{PROGRESS_CONTRACT_PREFIX}{}", version.as_str())
}

/// One `progress-phase=` line, or **`None` for anything this channel refuses
/// to say**.
///
/// # This is a filter, not a formatter, and that is the security property
///
/// A progress line is printed to a pod log that a controller reads and a UI
/// renders, on a path that has plan-controlled strings, broker errors and
/// projected credentials in scope. So the `(phase, name)` pair must be one
/// this build already knows — a member of [`RESTORE_PHASE_NAMES`] or of
/// [`BACKUP_STEP_NAMES`] at phase `-1` — and nothing else is renderable at
/// all. A closed vocabulary and not a charset rule, because `hunter2` passes
/// every charset rule anyone would write.
///
/// A caller that hands this function a credential, a URL with userinfo, a
/// broker error, a record's bytes or a newline that would forge a second line
/// gets `None`, and its caller prints nothing. Silence is the right failure:
/// D3 §2.4 says in as many words that "absence is not an error", so a reader
/// already has to treat a missing line as normal.
#[must_use]
pub fn progress_phase_line(phase: i8, name: &str) -> Option<String> {
    let known = match phase {
        -1 => BACKUP_STEP_NAMES.contains(&name),
        _ => RESTORE_PHASE_NAMES
            .iter()
            .any(|(p, n)| *p == phase && *n == name),
    };
    if !known {
        return None;
    }
    // Belt and braces over the table itself: the bound is what keeps the
    // channel from ever costing a reader the evidence keys in a 50-line,
    // 64 KiB tail, and a table entry is as capable of being edited as a
    // call site is.
    if name.len() > PROGRESS_NAME_MAX_BYTES {
        return None;
    }
    let line = format!("{PROGRESS_PHASE_PREFIX}{phase}:{name}");
    if line.len() > PROGRESS_LINE_MAX_BYTES {
        return None;
    }
    Some(line)
}

/// `teardown-key=<key>` (D3 §2.4), printed only when phase 9 attested.
pub const TEARDOWN_KEY_PREFIX: &str = "teardown-key=";

// ---------------------------------------------------------------------------
// plan ∈ scope (D3 §4.3(d)), the runner's half and the controller's
// ---------------------------------------------------------------------------

/// Everything a rendered plan says that a signed rehearsal scope constrains.
///
/// A struct rather than `&DrillSpec` directly so the SAME predicate serves
/// both halves of D3 §4.3's "checked twice": the controller builds these facts
/// from the plan it is about to render and freeze, the runner builds them from
/// the plan bytes it actually mounted ([`plan_scope_facts`]). One predicate,
/// two producers — which is the only arrangement in which "the controller
/// proved it" and "the runner proved it" mean the same thing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlanScopeFacts {
    /// The allowed-cluster set this run was given, from the separately mounted
    /// allowlist document — never from the plan, which cannot widen its own.
    pub allowed_cluster_ids: Vec<String>,
    /// The prefix every source topic is actually mapped through.
    pub topic_prefix: String,
    /// The plan's `source.topics`, in plan order.
    pub source_topics: Vec<String>,
    /// The names this plan will create on the target, in `source_topics` order.
    pub mapped_topics: Vec<String>,
    /// The wire spelling of `target.mode`.
    pub mode: String,
    /// `sample.records_per_partition`.
    pub records_per_partition: u64,
    /// `sample.max_partitions` — **`None` means the plan states no bound**,
    /// which is how a plan asks to sample every candidate partition.
    ///
    /// It is an `Option` and not a defaulted number because absent and "a very
    /// large number" are different claims, and only one of them can be
    /// compared against a signed ceiling. See [`plan_within_scope`] for why
    /// absent is a MISMATCH here rather than a pass.
    pub max_partitions: Option<u32>,
}

/// The facts a mounted restore plan and its allowlist state.
///
/// Pure, and it derives the mapped names the same way phase 0 does — through
/// [`crate::spec::target_topic_prefix`], which is the one place that rule
/// lives. Deriving them a second way here would let the scope check bless
/// names phase 0 then maps differently.
#[must_use]
pub fn plan_scope_facts(
    plan: &crate::spec::DrillSpec,
    allowed: &crate::spec::AllowedClusters,
) -> PlanScopeFacts {
    let topic_prefix = crate::spec::target_topic_prefix(plan);
    PlanScopeFacts {
        allowed_cluster_ids: allowed.allowed_cluster_ids.clone(),
        mapped_topics: plan
            .source
            .topics
            .iter()
            .map(|t| format!("{topic_prefix}{t}"))
            .collect(),
        topic_prefix,
        source_topics: plan.source.topics.clone(),
        mode: plan.target.mode.to_string(),
        records_per_partition: plan.sample.records_per_partition as u64,
        max_partitions: plan.sample.max_partitions,
    }
}

/// Every way a plan fell outside the signed scope, named.
///
/// **All of them, never the first.** An operator fixing a rehearsal template
/// one refusal at a time, at one slot per week, is the reason: a check that
/// stops at the first mismatch turns a five-minute edit into five weeks.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScopeRefusal {
    pub mismatches: Vec<String>,
}

impl std::fmt::Display for ScopeRefusal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "the rendered plan is outside the signed standing rehearsal authorization: {}",
            self.mismatches.join("; ")
        )
    }
}

/// D3 §4.3(d): prove `plan ∈ scope`.
///
/// # What it can prove here, and what it delegates
///
/// * mode, prefix, source topics, mapped names and records-per-partition are
///   all functions of the plan's own bytes, so they are proven outright;
/// * the **target cluster id** is not a plan field — a cluster id is read from
///   the broker, and this check runs before any broker exists. So it is proven
///   through the rail that already enforces it: the mounted allowlist must be
///   exactly the signed `target_cluster_id`, and phase 0 then refuses any
///   observed id outside that set. Narrowing the allowlist is what turns
///   "the signed scope names cluster X" into "this run cannot reach anything
///   but X" without a second broker round trip;
/// * **`max_partitions` IS a plan field** (`sample.max_partitions`,
///   `spec.rs`), and it is compared — with absent read as UNBOUNDED and
///   therefore as a mismatch, which is the fail-closed direction. An earlier
///   revision of this comment claimed it was "not a plan field at all"; that
///   was simply wrong, and the review that caught it was right that the
///   asymmetry against `records_per_partition` was invisible to W7.
///
///   The honest caveat is about MEANING, not about existence: the scope's
///   `maxPartitions` is D3 §4.1's bound on the point ("partition total `<=`
///   maxPartitions when the catalog supplies counts"), i.e. a ceiling on how
///   big a point the rehearsal may select, while `sample.max_partitions`
///   TRUNCATES the candidate list phase 4 built (`phase4_sample.rs`). A plan
///   that satisfies the second has not thereby satisfied the first — the
///   controller still owes the point-selection check — so this comparison is
///   necessary and not sufficient, and W7 keeps §4.2's filter.
/// * `deadline_seconds` genuinely is NOT a plan field — it is the Job's
///   `activeDeadlineSeconds`. It stays the controller's half of §4.3(d) and
///   this predicate does not pretend to check it.
pub fn plan_within_scope(
    facts: &PlanScopeFacts,
    scope: &RehearsalScope,
) -> Result<(), Box<ScopeRefusal>> {
    let mut mismatches = Vec::new();

    // FIRST, per `RehearsalScope::is_scratch_only`'s own contract: a scope
    // naming a mode this build does not implement is one this build must not
    // act on, whatever the rest of it says.
    if !scope.is_scratch_only() {
        mismatches.push(format!(
            "the signed scope permits modes [{}], and this build implements only `{MODE_SCRATCH}`",
            scope.modes.join(", ")
        ));
    }
    if facts.mode != MODE_SCRATCH {
        mismatches.push(format!(
            "the plan runs in mode `{}`; a standing rehearsal authorization permits \
             `{MODE_SCRATCH}` only",
            facts.mode
        ));
    }
    if facts.topic_prefix != scope.topic_prefix {
        mismatches.push(format!(
            "the plan maps through prefix `{}`; the signed scope is `{}`",
            facts.topic_prefix, scope.topic_prefix
        ));
    }
    let outside: Vec<&String> = facts
        .source_topics
        .iter()
        .filter(|t| !scope.topics.contains(t))
        .collect();
    if let Some(first) = outside.first() {
        mismatches.push(format!(
            "source topic `{first}` is not in the signed scope; every topic outside it, in plan \
             order: {}",
            outside
                .iter()
                .map(|t| format!("`{t}`"))
                .collect::<Vec<_>>()
                .join(", ")
        ));
    }
    // The mapped names, independently of the prefix check above: a plan whose
    // prefix matched but whose mapping did not derive from it would restore
    // into names the signed prefix does not cover, and the prefix comparison
    // alone would have passed it.
    for (source, mapped) in facts.source_topics.iter().zip(facts.mapped_topics.iter()) {
        let expected = format!("{}{source}", scope.topic_prefix);
        if mapped != &expected {
            mismatches.push(format!(
                "the plan maps `{source}` to `{mapped}`; the signed scope's prefix yields \
                 `{expected}`"
            ));
        }
    }
    if facts.mapped_topics.len() != facts.source_topics.len() {
        mismatches.push(format!(
            "the plan maps {} of its {} source topics",
            facts.mapped_topics.len(),
            facts.source_topics.len()
        ));
    }
    if facts.records_per_partition > u64::from(scope.records_per_partition) {
        mismatches.push(format!(
            "the plan samples {} records per partition; the signed scope permits {}",
            facts.records_per_partition, scope.records_per_partition
        ));
    }
    // ABSENT IS A MISMATCH, not a pass. A plan stating no partition bound is a
    // plan asking for every candidate partition, and "unbounded" is not inside
    // any finite ceiling a human signed. Reading absent as "fine" would make
    // the one way to evade this check the easiest thing to write.
    match facts.max_partitions {
        None => mismatches.push(format!(
            "the plan states no `sample.max_partitions`, so it is unbounded; the signed scope \
             permits at most {}",
            scope.max_partitions
        )),
        Some(max) if max > scope.max_partitions => mismatches.push(format!(
            "the plan samples up to {max} partitions; the signed scope permits {}",
            scope.max_partitions
        )),
        Some(_) => {}
    }
    // The target cluster id, through the allowlist — see this function's doc
    // comment for why that is the strongest form available before a broker
    // exists, and why it is EQUALITY and not membership.
    if facts.allowed_cluster_ids.len() != 1
        || facts.allowed_cluster_ids[0] != scope.target_cluster_id
    {
        mismatches.push(format!(
            "the mounted allowed-cluster set is [{}]; a standing rehearsal authorization admits \
             exactly the signed target cluster id `{}` and nothing else",
            facts.allowed_cluster_ids.join(", "),
            scope.target_cluster_id
        ));
    }

    if mismatches.is_empty() {
        Ok(())
    } else {
        Err(Box::new(ScopeRefusal { mismatches }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_contract_version_is_two_and_v1_is_still_nameable() {
        assert_eq!(VERSION, "2");
        assert_eq!(ContractVersion::parse(VERSION), Some(ContractVersion::V2));
        assert_eq!(ContractVersion::parse("1"), Some(ContractVersion::V1));
        assert_eq!(ContractVersion::parse("3"), None);
        assert_eq!(ContractVersion::parse(""), None);
    }

    #[test]
    fn v1_may_not_carry_v2_material_and_v2_may() {
        assert!(ContractVersion::V2
            .refuse_v2_material("a point binding")
            .is_none());
        let refusal = ContractVersion::V1
            .refuse_v2_material("a point binding")
            .expect("v1 must refuse v2 material");
        assert!(refusal.contains("a point binding"), "{refusal}");
        assert!(
            refusal.contains("no data operation was started"),
            "{refusal}"
        );
    }

    #[test]
    fn the_environment_sets_are_disjoint_and_their_union_is_every_name() {
        for name in V2_ENV {
            assert!(!ALL_ENV.contains(&name), "{name} is in both sets");
        }
        assert_eq!(ALL_ENV.len() + V2_ENV.len(), ALL_ENV_ANY.len());
        for name in ALL_ENV.iter().chain(V2_ENV.iter()) {
            assert!(ALL_ENV_ANY.contains(name), "{name} missing from the union");
        }
        let mut sorted = ALL_ENV_ANY.to_vec();
        sorted.sort_unstable();
        sorted.dedup();
        assert_eq!(sorted.len(), ALL_ENV_ANY.len(), "a name is listed twice");
    }

    #[test]
    fn every_contract_variable_is_namespaced_to_logweir_execution() {
        for name in ALL_ENV_ANY {
            assert!(name.starts_with("LOGWEIR_EXECUTION_"), "{name}");
        }
    }

    #[test]
    fn a_progress_line_carries_a_phase_and_a_name_and_nothing_else() {
        assert_eq!(
            progress_phase_line(0, "admit").as_deref(),
            Some("progress-phase=0:admit")
        );
        assert_eq!(
            progress_phase_line(-1, "readback").as_deref(),
            Some("progress-phase=-1:readback")
        );
        assert_eq!(
            progress_contract_line(ContractVersion::V2),
            "progress-contract=2"
        );
    }

    /// The mutant this channel exists to survive: a caller that hands the
    /// formatter a projected password, a URL with userinfo, or anything that
    /// could forge a second line gets NOTHING on stdout.
    /// THE MUTANT this channel exists to survive. Note `hunter2` and
    /// `s3cr3t`: both pass every charset rule anyone would write, which is
    /// exactly why the vocabulary is a closed TABLE and not a character class.
    #[test]
    fn a_progress_line_refuses_to_carry_a_credential_or_forge_a_line() {
        for hostile in [
            "hunter2",
            "s3cr3t",
            "s3cr3t-P@ssw0rd",
            "https://user:pass@example.invalid/bucket",
            "admit\nscorecard-key=logweir/drills/forged.json",
            "admit sasl.password=hunter2",
            "ADMIT",
            "admit_step",
            "",
            &"a".repeat(PROGRESS_NAME_MAX_BYTES + 1),
        ] {
            assert_eq!(
                progress_phase_line(0, hostile),
                None,
                "the channel rendered {hostile:?}"
            );
        }
        // A legal name at the wrong phase is also not a thing this build says:
        // `admit` is phase 0 and step -1, never phase 5.
        assert_eq!(progress_phase_line(5, "admit"), None);
        assert_eq!(progress_phase_line(42, "admit"), None);
        assert_eq!(progress_phase_line(-2, "admit"), None);
        // And a restore phase name is not a backup step.
        assert_eq!(progress_phase_line(-1, "verify"), None);
    }

    #[test]
    fn every_line_the_channel_can_say_is_within_the_bound() {
        for (phase, name) in RESTORE_PHASE_NAMES {
            let line = progress_phase_line(phase, name).expect("a known phase renders");
            assert!(line.len() <= PROGRESS_LINE_MAX_BYTES, "{line}");
        }
        for name in BACKUP_STEP_NAMES {
            let line = progress_phase_line(-1, name).expect("a known step renders");
            assert!(line.len() <= PROGRESS_LINE_MAX_BYTES, "{line}");
        }
    }

    #[test]
    fn the_restore_vocabulary_is_the_ten_phases_and_nothing_else() {
        assert_eq!(RESTORE_PHASE_NAMES.len(), 10);
        for (i, (phase, _)) in RESTORE_PHASE_NAMES.iter().enumerate() {
            assert_eq!(*phase as usize, i, "the table is dense and in phase order");
        }
    }

    fn scope() -> RehearsalScope {
        RehearsalScope {
            template_digest: "sha256:aa".into(),
            target_cluster_id: "TARGET00000000000000000".into(),
            topic_prefix: "rehearsal-3f2a91c7-".into(),
            topics: vec!["orders".into(), "payments".into()],
            max_partitions: 200,
            records_per_partition: 25,
            deadline_seconds: 3600,
            modes: vec![MODE_SCRATCH.to_string()],
        }
    }

    fn facts() -> PlanScopeFacts {
        PlanScopeFacts {
            allowed_cluster_ids: vec!["TARGET00000000000000000".into()],
            topic_prefix: "rehearsal-3f2a91c7-".into(),
            source_topics: vec!["orders".into()],
            mapped_topics: vec!["rehearsal-3f2a91c7-orders".into()],
            mode: MODE_SCRATCH.to_string(),
            records_per_partition: 25,
            max_partitions: Some(200),
        }
    }

    #[test]
    fn a_rendered_plan_inside_the_scope_is_accepted() {
        plan_within_scope(&facts(), &scope()).expect("this plan is inside the signed scope");
    }

    #[test]
    fn every_mismatch_is_named_and_not_only_the_first() {
        let mut f = facts();
        f.topic_prefix = "rehearsal-deadbeef-".into();
        f.mapped_topics = vec!["rehearsal-deadbeef-orders".into()];
        f.source_topics = vec!["ledger".into()];
        f.mode = "newTopic".into();
        f.records_per_partition = 1000;
        f.max_partitions = Some(4000);
        f.allowed_cluster_ids = vec!["OTHER000000000000000000".into()];
        let refusal = plan_within_scope(&f, &scope()).expect_err("outside the scope");
        let rendered = refusal.to_string();
        for expected in [
            "mode `newTopic`",
            "rehearsal-deadbeef-",
            "`ledger`",
            "1000 records per partition",
            "up to 4000 partitions",
            "OTHER000000000000000000",
        ] {
            assert!(
                rendered.contains(expected),
                "{expected} missing:\n{rendered}"
            );
        }
        assert!(
            refusal.mismatches.len() >= 6,
            "only {} mismatches named:\n{rendered}",
            refusal.mismatches.len()
        );
    }

    #[test]
    fn a_scope_naming_a_mode_this_build_does_not_implement_is_refused() {
        let mut s = scope();
        s.modes = vec![MODE_SCRATCH.into(), "newTopic".into()];
        let refusal = plan_within_scope(&facts(), &s).expect_err("an unknown mode is refused");
        assert!(refusal.to_string().contains("implements only"), "{refusal}");
    }

    /// The mapped names are checked independently of the prefix: a plan whose
    /// prefix matched the scope but whose mapping did not derive from it would
    /// restore into names nobody signed.
    #[test]
    fn a_mapped_name_that_does_not_derive_from_the_signed_prefix_is_refused() {
        let mut f = facts();
        f.mapped_topics = vec!["orders".into()];
        let refusal = plan_within_scope(&f, &scope()).expect_err("an unmapped name is refused");
        assert!(refusal.to_string().contains("`orders`"), "{refusal}");
    }

    /// Membership is not enough: an allowlist of two clusters would let phase 0
    /// admit a target the scope never named.
    #[test]
    fn a_widened_allowed_cluster_set_is_refused_even_though_it_contains_the_signed_id() {
        let mut f = facts();
        f.allowed_cluster_ids = vec![
            "TARGET00000000000000000".into(),
            "OTHER000000000000000000".into(),
        ];
        let refusal = plan_within_scope(&f, &scope()).expect_err("a widened allowlist is refused");
        assert!(
            refusal
                .to_string()
                .contains("exactly the signed target cluster id"),
            "{refusal}"
        );
    }

    // ---- the signed standing authorization (D3 §4.3(e)) -------------------

    fn authorization(uid: &str, issued_days_ago: i64, life_days: i64) -> StandingAuthorization {
        let issued = chrono::DateTime::parse_from_rfc3339("2026-06-01T00:00:00Z")
            .expect("a fixed instant")
            .with_timezone(&chrono::Utc)
            - chrono::Duration::days(issued_days_ago);
        StandingAuthorization {
            format_version: STANDING_AUTHORIZATION_FORMAT_VERSION.to_string(),
            kind: STANDING_AUTHORIZATION_KIND.to_string(),
            subject_ref: AuthorizationSubject {
                api_version: SUBJECT_API_VERSION.to_string(),
                kind: REHEARSAL_SCHEDULE_KIND.to_string(),
                namespace: "team-a".to_string(),
                name: "weekly-orders".to_string(),
                uid: uid.to_string(),
            },
            scope: scope(),
            issued_at: issued,
            expires_at: issued + chrono::Duration::days(life_days),
        }
    }

    fn at(rfc3339: &str) -> chrono::DateTime<chrono::Utc> {
        chrono::DateTime::parse_from_rfc3339(rfc3339)
            .expect("a fixed instant")
            .with_timezone(&chrono::Utc)
    }

    #[test]
    fn a_valid_standing_authorization_for_this_schedule_is_admitted() {
        admit_standing_authorization(
            &authorization("uid-1", 1, 30),
            Some("uid-1"),
            at("2026-06-02T00:00:00Z"),
        )
        .expect("a current, correctly-subjected authorization is admitted");
    }

    /// THE BINDING the environment's UID could not provide on its own: a
    /// document signed for another schedule must not authorise this run.
    #[test]
    fn an_authorization_for_another_schedule_is_refused() {
        let refusal = admit_standing_authorization(
            &authorization("uid-other", 1, 30),
            Some("uid-1"),
            at("2026-06-02T00:00:00Z"),
        )
        .expect_err("a foreign subject is refused");
        assert_eq!(refusal.token, AUTHORIZATION_INVALID);
        assert!(refusal.to_string().contains("uid-other"), "{refusal}");
        assert!(refusal.to_string().contains("uid-1"), "{refusal}");
    }

    #[test]
    fn an_authorization_with_no_uid_in_the_contract_cannot_be_bound() {
        let refusal = admit_standing_authorization(
            &authorization("uid-1", 1, 30),
            None,
            at("2026-06-02T00:00:00Z"),
        )
        .expect_err("an unbindable authorization is refused");
        assert_eq!(refusal.token, AUTHORIZATION_INVALID);
        assert!(
            refusal
                .to_string()
                .contains("names no RehearsalSchedule UID"),
            "{refusal}"
        );
    }

    #[test]
    fn an_expired_authorization_is_refused_under_its_own_token() {
        let refusal = admit_standing_authorization(
            &authorization("uid-1", 40, 30),
            Some("uid-1"),
            at("2026-06-02T00:00:00Z"),
        )
        .expect_err("an expired authorization is refused");
        assert_eq!(
            refusal.token, AUTHORIZATION_EXPIRED,
            "expiry is its own reason, so a controller's skip reason and the runner's refusal \
             say the same word (D3 §4.3)"
        );
    }

    #[test]
    fn an_authorization_that_is_not_yet_valid_is_refused() {
        let refusal = admit_standing_authorization(
            &authorization("uid-1", -5, 30),
            Some("uid-1"),
            at("2026-06-02T00:00:00Z"),
        )
        .expect_err("a future authorization is refused");
        assert_eq!(refusal.token, AUTHORIZATION_INVALID);
        assert!(refusal.to_string().contains("not valid until"), "{refusal}");
    }

    /// D3 §4.3 caps a standing authorization at 90 days, and the cap is a
    /// property of the SIGNED bytes — so the runner re-checks it rather than
    /// trusting whoever minted the document to have applied it.
    #[test]
    fn an_authorization_longer_than_ninety_days_is_refused() {
        let refusal = admit_standing_authorization(
            &authorization("uid-1", 1, MAX_STANDING_AUTHORIZATION_DAYS + 1),
            Some("uid-1"),
            at("2026-06-02T00:00:00Z"),
        )
        .expect_err("a document beyond the cap is refused");
        assert_eq!(refusal.token, AUTHORIZATION_INVALID);
        assert!(
            refusal.to_string().contains("caps a standing rehearsal"),
            "{refusal}"
        );
    }

    #[test]
    fn a_document_of_another_kind_or_major_is_refused() {
        let mut wrong_kind = authorization("uid-1", 1, 30);
        wrong_kind.kind = "Approval".to_string();
        assert_eq!(
            admit_standing_authorization(&wrong_kind, Some("uid-1"), at("2026-06-02T00:00:00Z"))
                .expect_err("another kind is refused")
                .token,
            AUTHORIZATION_INVALID
        );

        let mut wrong_major = authorization("uid-1", 1, 30);
        wrong_major.format_version = "2.0.0".to_string();
        let refusal =
            admit_standing_authorization(&wrong_major, Some("uid-1"), at("2026-06-02T00:00:00Z"))
                .expect_err("a higher major is refused");
        assert!(refusal.to_string().contains("major 1"), "{refusal}");

        let mut wrong_subject = authorization("uid-1", 1, 30);
        wrong_subject.subject_ref.kind = "Restore".to_string();
        assert!(admit_standing_authorization(
            &wrong_subject,
            Some("uid-1"),
            at("2026-06-02T00:00:00Z")
        )
        .is_err());
    }

    /// Key-usage separation (D3 §7.3): the installation's own evidence key may
    /// never authorise its own rehearsals.
    #[test]
    fn only_an_authorization_usage_key_may_authorize() {
        use crate::trust::KeyUsage;
        let key = |usages: Vec<KeyUsage>| AuthorizationKey {
            key_id: "sha256:aa".to_string(),
            public_key_pem: "-----BEGIN PUBLIC KEY-----".to_string(),
            usages,
        };
        assert!(key(vec![KeyUsage::GovernedApproval]).may_authorize());
        assert!(
            !key(vec![KeyUsage::ConsoleConfirmation]).may_authorize(),
            "a console key authorises no rehearsal: the standing format carries no policy mode"
        );
        assert!(
            !key(vec![KeyUsage::EvidenceSigning]).may_authorize(),
            "the evidence signing key must never be able to authorise a rehearsal"
        );
        assert!(
            !key(vec![]).may_authorize(),
            "an empty usage set may do nothing"
        );
        assert!(key(vec![KeyUsage::EvidenceSigning, KeyUsage::GovernedApproval]).may_authorize());
    }

    #[test]
    fn the_signed_document_round_trips_through_its_camel_case_grammar() {
        let doc = authorization("uid-1", 1, 30);
        let json = serde_json::to_string(&doc).expect("serialises");
        for key in [
            "formatVersion",
            "subjectRef",
            "apiVersion",
            "issuedAt",
            "expiresAt",
        ] {
            assert!(json.contains(key), "{key} missing from {json}");
        }
        let back: StandingAuthorization = serde_json::from_str(&json).expect("parses");
        assert_eq!(back, doc);
        // Unknown fields are IGNORED, so a document PLAT-19.2 later enriches
        // with `policy`/`requester`/`ticket` still verifies against this build.
        let enriched = json.replace(
            "{\"formatVersion\"",
            "{\"requester\":{\"issuer\":\"https://idp\"},\"formatVersion\"",
        );
        let with_extras: StandingAuthorization =
            serde_json::from_str(&enriched).expect("an enriched document still parses");
        assert_eq!(with_extras.scope, doc.scope);
    }

    /// F4: `sample.max_partitions` IS a plan field, and absent is UNBOUNDED,
    /// which is not inside any finite ceiling a human signed.
    #[test]
    fn an_unbounded_partition_count_is_outside_every_signed_ceiling() {
        let mut f = facts();
        f.max_partitions = None;
        let refusal = plan_within_scope(&f, &scope()).expect_err("unbounded is refused");
        assert!(
            refusal
                .to_string()
                .contains("states no `sample.max_partitions`"),
            "{refusal}"
        );

        f.max_partitions = Some(scope().max_partitions + 1);
        let refusal = plan_within_scope(&f, &scope()).expect_err("over the ceiling is refused");
        assert!(
            refusal.to_string().contains("up to 201 partitions"),
            "{refusal}"
        );

        f.max_partitions = Some(scope().max_partitions);
        plan_within_scope(&f, &scope()).expect("exactly the ceiling is inside it");
    }

    #[test]
    fn a_point_binding_round_trips_through_the_plan_documents_grammar() {
        let binding = PointBinding {
            point_id: format!("lwp1-{}", "a".repeat(32)),
            receipt_key: "logweir/backups/nightly-7/run-9.receipt.json".into(),
            receipt_sha256: format!("sha256:{}", "b".repeat(64)),
            manifest_sha256: format!("sha256:{}", "c".repeat(64)),
        };
        let yaml = serde_yaml::to_string(&binding).expect("serialises");
        assert!(yaml.contains("point_id:"), "{yaml}");
        assert!(yaml.contains("receipt_key:"), "{yaml}");
        let back: PointBinding = serde_yaml::from_str(&yaml).expect("parses");
        assert_eq!(back, binding);
    }

    /// **LOW-2: `STANDING_MANDATORY_ENV` is a hand-written list, and this is
    /// what binds it to [`ALL_ENV`].**
    ///
    /// Without this row a fourteenth mandatory name added to `ALL_ENV` would
    /// silently escape the standing contract's completeness assertion in
    /// `weirkeeper`, because that row loops the standing list. The rule is the
    /// one sentence the constant's doc comment states: `ALL_ENV` minus exactly
    /// the two per-run approval digests, in `ALL_ENV`'s order.
    #[test]
    fn the_standing_mandatory_set_is_all_env_minus_the_two_approval_digests() {
        let expected: Vec<&str> = ALL_ENV
            .iter()
            .copied()
            .filter(|n| *n != APPROVAL_SHA256_ENV && *n != APPROVAL_SIDECAR_SHA256_ENV)
            .collect();
        assert_eq!(
            STANDING_MANDATORY_ENV.to_vec(),
            expected,
            "STANDING_MANDATORY_ENV is ALL_ENV minus the per-run approval slot, in order"
        );
        assert_eq!(STANDING_MANDATORY_ENV.len(), ALL_ENV.len() - 2);
        for name in [APPROVAL_SHA256_ENV, APPROVAL_SIDECAR_SHA256_ENV] {
            assert!(
                !STANDING_MANDATORY_ENV.contains(&name),
                "{name} pins a bundle member a rehearsal does not have"
            );
        }
        // And the two that STAY: they name the standing Approval object, which
        // is a real object a human signed and the one an auditor looks up.
        for name in [APPROVAL_NAME_ENV, APPROVAL_UID_ENV] {
            assert!(STANDING_MANDATORY_ENV.contains(&name), "{name} must stay");
        }
    }

    #[test]
    fn an_authorization_kind_is_a_closed_set() {
        assert_eq!(
            AuthorizationKind::parse("approval"),
            Some(AuthorizationKind::Approval)
        );
        assert_eq!(
            AuthorizationKind::parse("standing"),
            Some(AuthorizationKind::Standing)
        );
        assert_eq!(AuthorizationKind::parse("Standing"), None);
        assert_eq!(AuthorizationKind::parse("none"), None);
        assert_eq!(AuthorizationKind::default(), AuthorizationKind::Approval);
    }
}
