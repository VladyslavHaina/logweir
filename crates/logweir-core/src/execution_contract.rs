//! Pure names for the controller-to-runner Restore execution contract.
//!
//! The Kubernetes Job template is immutable after creation.  New Restore Jobs
//! therefore carry the exact public input digests in environment variables;
//! the runner compares them with the bytes it read from projected volumes
//! before it constructs any data-plane client.  Keeping the names here makes
//! the controller and runner share one wire contract without introducing a
//! dependency between their crates.
//!
//! # Version 2
//!
//! [`VERSION`] is `"2"`. The bump is the whole point of the version existing:
//! v2 carries three things a v1 runner would silently not do — the recovery
//! **point binding**, the **standing rehearsal authorization**
//! and the **progress / teardown stdout lines** — so an
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
/// **Two of the thirteen are the per-run approval's.**
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
/// the two per-run approval digests.
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

/// Which authorization this run was created under: [`AUTHORIZATION_KIND_APPROVAL`]
/// (the per-run `Approval`, tag 1's only shape) or [`AUTHORIZATION_KIND_STANDING`].
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
/// A digest over the SIGNED document and not over a raw [`RehearsalScope`]: a
/// scope whose only provenance is a digest the CONTROLLER set is a scope the
/// controller could mint.
pub const AUTHORIZATION_SHA256_ENV: &str = "LOGWEIR_EXECUTION_AUTHORIZATION_SHA256";
/// `sha256:<hex>` over the DSSE sidecar carrying the approver's signature over
/// those exact envelope bytes.
pub const AUTHORIZATION_SIDECAR_SHA256_ENV: &str = "LOGWEIR_EXECUTION_AUTHORIZATION_SIDECAR_SHA256";
/// `sha256:<hex>` over the mounted [`AuthorizationKeyring`]: the trusted
/// public keys, the anchor the signature is checked against.
pub const AUTHORIZATION_KEYS_SHA256_ENV: &str = "LOGWEIR_EXECUTION_AUTHORIZATION_KEYS_SHA256";
/// The `RehearsalSchedule` UID the standing authorization names as its
/// subject. Carried so the runner's refusal can say WHICH schedule's
/// authorization it was executing under without parsing the document.
pub const REHEARSAL_SCHEDULE_UID_ENV: &str = "LOGWEIR_EXECUTION_REHEARSAL_SCHEDULE_UID";
/// `sha256:<hex>` over the mounted approval-policy snapshot. This contract
/// carries it, and refuses a v1 invocation that tries to.
pub const POLICY_SNAPSHOT_SHA256_ENV: &str = "LOGWEIR_EXECUTION_POLICY_SNAPSHOT_SHA256";
/// `sha256:<hex>` over the mounted confirmation-issuer public key — the second
/// of D0's "both public keys". `APPROVER_KEY_SHA256_ENV` is the first.
pub const CONFIRMATION_KEY_SHA256_ENV: &str = "LOGWEIR_EXECUTION_CONFIRMATION_KEY_SHA256";

/// `sha256:<hex>` over the mounted [`EvidenceKeyring`]: the anchor the runner
/// verifies a bound recovery point's receipt signature against. Pinned exactly
/// when the plan carries `source.point`.
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
/// # The shape
///
/// `rehearsal/<schedule name>/<slot>`, as a rehearsal's signed scorecard
/// records it. Standing authorizations are withdrawn; this reader stays for
/// the documents already signed.
pub const TRIGGERED_BY_REHEARSAL_PREFIX: &str = "rehearsal/";

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
    /// The standing rehearsal authorization: one signed scope, proven to
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
/// A v1 bundle is accepted only for an already-created legacy governed
/// Restore. A runner cannot read a `Restore`'s creation timestamp — it
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

/// `source.point` — the recovery point a v2 restore plan is bound to.
///
/// # Why it is in the PLAN and not in the environment
///
/// The environment is the controller's word for it; the plan is what the
/// approver signed. Binding the point into plan bytes means the approval
/// covers *which archive object this restore recovers from*, and the
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
///
/// # Present, `source.backup` names the point's own set
///
/// The receipt describes ONE backup set, so a plan carrying the block names
/// that set in `source.backup` — never `latestCompleted`, never another id —
/// and the runner refuses any other plan (`PointBindingSetMismatch`, exit 3),
/// and any run whose restored set is not the one the binding verified.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PointBinding {
    /// `lwp1-` + 32 lowercase hex characters, content-derived from the signed
    /// receipt. A DISPLAY and lookup key; `receipt_sha256` is the
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
///
/// It is also the version a document whose scope carries no field newer than
/// 1.0.0 was minted at; one that does was minted at
/// [`STANDING_AUTHORIZATION_FORMAT_VERSION_COVERAGE`].
pub const STANDING_AUTHORIZATION_FORMAT_VERSION: &str = "1.0.0";

/// The minor that defines `scope.coverage` and `scope.completeMaxRecords`. A
/// document declaring an older minor that carries either is refused: the fields
/// are a signer's choice of a STRONGER check, and a document that predates them
/// cannot have made it.
pub const STANDING_AUTHORIZATION_COVERAGE_SINCE_MINOR: u64 = 1;

/// The version a document whose scope carries `coverage` or
/// `completeMaxRecords` is minted at.
pub const STANDING_AUTHORIZATION_FORMAT_VERSION_COVERAGE: &str = "1.1.0";

/// `kind` — inside the signed bytes, so it cannot be relabelled.
pub const STANDING_AUTHORIZATION_KIND: &str = "StandingRehearsalAuthorization";
/// The only subject kind a standing rehearsal authorization may name.
pub const REHEARSAL_SCHEDULE_KIND: &str = "RehearsalSchedule";
/// The API group/version every subject of this document belongs to.
pub const SUBJECT_API_VERSION: &str = "logweir.dev/v1alpha1";
/// `expiresAt - issuedAt <= 90 days`. Re-checked at the runner
/// because it is a property of the SIGNED bytes: a document minted with a
/// ten-year life is one the decision does not permit, whoever minted it.
pub const MAX_STANDING_AUTHORIZATION_DAYS: i64 = 90;

/// What the standing authorization names as its subject.
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
/// times.**
///
/// # Why the runner reads THIS and not a bare scope
///
/// A raw [`RehearsalScope`] pinned by a digest in the pod template is pinned by
/// the controller, so against "a controller that could mint its own
/// authorization" the runner's half of "checked twice" would prove nothing. The
/// bundle carries the authorization document, its signatures, the trusted
/// public keys, the scope and the rendered plan; the scope is INSIDE this
/// document, and the runner derives it only after the signature over these
/// exact bytes verifies under a pinned key that is allowed to authorise.
///
/// # The shape
///
/// The authorization document v2 family (`formatVersion`, a `subject` block
/// with a UID, `issuedAt`/`expiresAt`, a DSSE sidecar over the exact bytes).
/// Unknown keys are ignored on read, so fields added later (`policy`,
/// `requester`, `ticket`, a second signature) still verify here.
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
    /// The standing document/bundle format carries no approval-policy mode (the
    /// policy is carried end to end for per-run Restores only,
    /// `crate::approval_policy`), so only
    /// [`crate::trust::KeyUsage::GovernedApproval`] may authorize a rehearsal,
    /// under every policy. `ConsoleConfirmation` fails closed here;
    /// `EvidenceSigning` never authorizes.
    #[must_use]
    pub fn may_authorize(&self) -> bool {
        self.usages
            .contains(&crate::trust::KeyUsage::GovernedApproval)
    }
}

/// The trusted public keys, as a mounted bundle member.
#[derive(Deserialize, Serialize, Clone, Debug, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct AuthorizationKeyring {
    pub format_version: String,
    pub keys: Vec<AuthorizationKey>,
}

/// The evidence-signing keys a restore Job verifies a bound recovery point's
/// receipt against, as a mounted bundle member.
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
/// The two spellings are the controller's skip reasons, reused so the
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
/// spec — is the CONTROLLER's each-slot check. It is recomputed
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
    // The scope's coverage fields, in a document new enough to
    // carry them, and coherent. Each can only REFUSE: a scope without them
    // admits what every 1.0.0 scope admitted (`plan_within_scope`).
    if doc.scope.needs_coverage_format() {
        let minor = doc
            .format_version
            .split('.')
            .nth(1)
            .and_then(|m| m.parse::<u64>().ok());
        if minor.is_none_or(|m| m < STANDING_AUTHORIZATION_COVERAGE_SINCE_MINOR) {
            return Err(AuthorizationRefusal::invalid(format!(
                "the signed scope carries `coverage` or `completeMaxRecords`, which are defined \
                 from formatVersion {STANDING_AUTHORIZATION_FORMAT_VERSION_COVERAGE}, and the \
                 document declares {:?}",
                doc.format_version
            )));
        }
    }
    match (doc.scope.signed_coverage(), doc.scope.complete_max_records) {
        (crate::spec::Coverage::Sampled, Some(_)) => {
            return Err(AuthorizationRefusal::invalid(
                "the signed scope states `completeMaxRecords` and does not authorise \
                 `coverage: complete`; a bound on a verification it does not permit authorises \
                 nothing"
                    .to_string(),
            ))
        }
        (crate::spec::Coverage::Complete, Some(0)) => {
            return Err(AuthorizationRefusal::invalid(
                "the signed scope's `completeMaxRecords` is 0; a complete verification that may \
                 decode no record compares nothing"
                    .to_string(),
            ))
        }
        _ => {}
    }
    // A SCOPE THAT AUTHORISES COMPLETE COVERAGE SIGNS
    // `maxPartitions: 0`, and only that. A complete plan samples no
    // partitions, so this build never reads the bound for one; a reader built
    // before format 1.1.0 ignores `coverage`, reads a sampled scope, and under
    // a bound of 0 refuses every plan. Any other value would let that older
    // reader admit a sampled plan the signer did not authorise, so it is
    // refused here — by every reader and by the minter, which all call this.
    if doc.scope.signed_coverage() == crate::spec::Coverage::Complete
        && doc.scope.max_partitions != crate::rehearsal_scope::COMPLETE_SCOPE_MAX_PARTITIONS
    {
        return Err(AuthorizationRefusal::invalid(format!(
            "the signed scope authorises `coverage: complete` and states `maxPartitions: {}`; a \
             scope that authorises complete coverage signs `maxPartitions: {}` (a complete plan \
             samples no partitions, and {} is what makes every reader older than formatVersion \
             {STANDING_AUTHORIZATION_FORMAT_VERSION_COVERAGE} refuse every plan under it)",
            doc.scope.max_partitions,
            crate::rehearsal_scope::COMPLETE_SCOPE_MAX_PARTITIONS,
            crate::rehearsal_scope::COMPLETE_SCOPE_MAX_PARTITIONS
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

/// `progress-contract=<version>` — the ONE line that says which grammar the
/// `progress-phase=` lines that follow are written in.
///
/// The channel is contract-versioned, but the per-phase line carries no
/// version: it is read by prefix out of a bounded tail and every byte of it is
/// budget. So the version is announced once, at the top of the run, and the
/// per-phase grammar stays exactly `<n>:<name>`.
pub const PROGRESS_CONTRACT_PREFIX: &str = "progress-contract=";
/// `progress-phase=<n>:<name>`.
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
/// They are listed here, in the pure layer, because the channel is a
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
/// The backup path has no numbered phases after admission, so it
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
/// absence is not an error, and a reader already treats a missing line as
/// normal.
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

/// `teardown-key=<key>`, printed only when phase 9 attested.
pub const TEARDOWN_KEY_PREFIX: &str = "teardown-key=";

/// `teardown-failed=[{"topic":…,"error":…},…]`: the topics a
/// restore's teardown left on the target cluster, each with why. The runner
/// prints it whenever teardown left one: phase 9 of a run that signed, and the
/// teardown of a run that failed after it created its target topics, which
/// signs nothing. The controller copies it to `Restore.status.teardown.failed`.
pub const TEARDOWN_FAILED_PREFIX: &str = "teardown-failed=";

/// `teardown-failed-count=<n>`: how many topics the teardown left, printed
/// with the `teardown-failed=` line, whose list carries at most
/// [`TEARDOWN_FAILED_MAX_TOPICS`] of them (FX-38). The controller writes it to
/// `Restore.status.teardown.failedCount`, so a cut list never reads as whole.
pub const TEARDOWN_FAILED_COUNT_PREFIX: &str = "teardown-failed-count=";

/// The most topics, and the most characters of one error, the line carries:
/// the bounds the Restore CRD puts on `status.teardown.failed`.
pub const TEARDOWN_FAILED_MAX_TOPICS: usize = 256;
pub const TEARDOWN_FAILED_MAX_ERROR_CHARS: usize = 256;

/// The longest value a genuine line carries; a reader refuses a longer one
/// before parsing it.
pub const TEARDOWN_FAILED_MAX_VALUE_BYTES: usize = TEARDOWN_FAILED_MAX_TOPICS
    * (crate::guard::MAX_TOPIC_NAME_CHARS + 4 * TEARDOWN_FAILED_MAX_ERROR_CHARS + 32);

/// One entry of the `teardown-failed=` line.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TeardownFailed {
    pub topic: String,
    pub error: String,
}

impl TeardownFailed {
    /// Kafka-legal names only, the error on one line and cut to its bound.
    fn bounded(topic: &str, error: &str) -> Option<Self> {
        crate::guard::topic_name_is_kafka_legal(topic).then(|| TeardownFailed {
            topic: topic.to_string(),
            error: error
                .chars()
                .map(|c| if c.is_control() { ' ' } else { c })
                .take(TEARDOWN_FAILED_MAX_ERROR_CHARS)
                .collect(),
        })
    }
}

/// The `teardown-failed=` line for `failed` (topic, why), or `None` when
/// nothing was left.
#[must_use]
pub fn teardown_failed_line(failed: &[(String, String)]) -> Option<String> {
    let entries: Vec<TeardownFailed> = failed
        .iter()
        .filter_map(|(topic, error)| TeardownFailed::bounded(topic, error))
        .take(TEARDOWN_FAILED_MAX_TOPICS)
        .collect();
    if entries.is_empty() {
        return None;
    }
    Some(format!(
        "{TEARDOWN_FAILED_PREFIX}{}",
        serde_json::to_string(&entries).ok()?
    ))
}

/// The `teardown-failed=` line and the `teardown-failed-count=` line for
/// `failed`, or nothing when nothing was left.
#[must_use]
pub fn teardown_failed_lines(failed: &[(String, String)]) -> Vec<String> {
    let Some(list) = teardown_failed_line(failed) else {
        return Vec::new();
    };
    let count = failed
        .iter()
        .filter_map(|(topic, error)| TeardownFailed::bounded(topic, error))
        .count();
    vec![list, format!("{TEARDOWN_FAILED_COUNT_PREFIX}{count}")]
}

/// What a reader keeps of a `teardown-failed=` value: `None` unless it is a
/// JSON array no longer than a genuine one; entries bounded as the writer
/// bounds them.
#[must_use]
pub fn parse_teardown_failed(value: &str) -> Option<Vec<TeardownFailed>> {
    if value.len() > TEARDOWN_FAILED_MAX_VALUE_BYTES {
        return None;
    }
    let entries: Vec<TeardownFailed> = serde_json::from_str(value).ok()?;
    let kept: Vec<TeardownFailed> = entries
        .iter()
        .filter_map(|e| TeardownFailed::bounded(&e.topic, &e.error))
        .take(TEARDOWN_FAILED_MAX_TOPICS)
        .collect();
    (!kept.is_empty()).then_some(kept)
}

/// Everything a rendered plan says that a signed rehearsal scope constrains.
///
/// A struct rather than `&DrillSpec` directly so the SAME predicate serves
/// both halves of the "checked twice" rule: the controller builds these facts
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
    /// The replay-selection keys the plan states,
    /// in words (`restore.point_in_time` with a start, `restore.partitions`);
    /// empty for a plan that restores everything from the archive's floor.
    /// A standing rehearsal restores everything, and no approver saw a
    /// narrowing in its plan bytes, so [`plan_within_scope`] refuses any.
    pub replay_selection: Vec<String>,
    /// `sample.coverage`, which must be exactly the signed
    /// scope's ([`RehearsalScope::signed_coverage`]). A `complete` plan
    /// verifies every partition of every restored topic, so it states NO
    /// `sample.max_partitions` — phase 0 refuses the pair.
    pub coverage: crate::spec::Coverage,
    /// `sample.complete_max_records`, held to the signed
    /// scope's `completeMaxRecords` when it states one.
    pub complete_max_records: Option<u64>,
    /// `restore.allow_transaction_markers`. The plan bytes are not hash-bound
    /// to a standing authorization, so [`plan_within_scope`] refuses it: only a
    /// per-run approval binds it.
    pub allows_transaction_markers: bool,
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
        replay_selection: {
            let mut keys = Vec::new();
            if plan.restore.window_start.is_some() {
                keys.push("a window start (restore.point_in_time \"<start>/<end>\")".to_string());
            }
            if !plan.restore.partitions.is_empty() {
                keys.push("partition subsets (restore.partitions)".to_string());
            }
            keys
        },
        coverage: plan.sample.coverage,
        complete_max_records: plan.sample.complete_max_records,
        allows_transaction_markers: plan.restore.allow_transaction_markers,
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

/// Prove `plan ∈ scope`.
///
/// # What it can prove here, and what it delegates
///
/// * mode, prefix, source topics, mapped names and records-per-partition are
///   all functions of the plan's own bytes, so they are proven outright;
/// * the **target cluster id** is not a plan field (it is read from the broker,
///   and this check runs before any broker exists), so it is proven through the
///   rail that already enforces it: the mounted allowlist must be exactly the
///   signed `target_cluster_id`, and phase 0 then refuses any observed id
///   outside that set;
/// * **`max_partitions` IS a plan field** (`sample.max_partitions`), compared
///   with absent read as UNBOUNDED and therefore as a mismatch, the fail-closed
///   direction. It is necessary and not sufficient: the scope's `maxPartitions`
///   bounds the point a rehearsal may select, while `sample.max_partitions`
///   TRUNCATES phase 4's candidate list, so the controller still owes the
///   point-selection check;
/// * **`coverage` is SIGNED, and the plan's must be exactly it.** A scope that
///   states no `coverage` (every scope signed before document format 1.1.0)
///   authorises `sampled` and nothing else, and a scope that says `complete`
///   refuses a sampled plan: a weaker check than the one signed is not the one
///   authorised;
/// * **a complete plan states no `max_partitions`**, and under a scope that
///   authorises complete coverage that is admitted: it samples nothing, and
///   phase 0 refuses it beside one. Its work is bounded by the Job's
///   `deadlineSeconds` and, when the scope states `completeMaxRecords`, by the
///   plan's `sample.complete_max_records`, which must then be present and no
///   larger;
/// * `deadline_seconds` is NOT a plan field (it is the Job's
///   `activeDeadlineSeconds`) and stays the controller's half.
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
    //
    // THE COVERAGE IS SIGNED. A scope that names none (every
    // 1.0.0 scope) authorises `sampled` only; the plan's must be exactly the
    // signed one, in both directions.
    let signed = scope.signed_coverage();
    if facts.coverage != signed {
        mismatches.push(format!(
            "the plan asks for `sample.coverage: {}` and the signed scope authorises `{}` \
             coverage{}",
            facts.coverage.as_str(),
            signed.as_str(),
            if scope.coverage.is_none() {
                " (it states no `coverage`, which authorises sampled rehearsals only)"
            } else {
                ""
            }
        ));
    }
    // A COMPLETE PLAN SAMPLES NOTHING, so its absent partition bound is not
    // an unbounded sample — under a scope that authorised complete coverage
    // (the arm above refuses it under any other). The sampled arms are
    // exactly what they were.
    match (facts.coverage, facts.max_partitions) {
        (crate::spec::Coverage::Complete, None) => {}
        (crate::spec::Coverage::Complete, Some(max)) => mismatches.push(format!(
            "the plan asks for `sample.coverage: complete` and states `sample.max_partitions: \
             {max}`; a complete verification checks every partition, and phase 0 refuses the \
             pair"
        )),
        (crate::spec::Coverage::Sampled, None) => mismatches.push(format!(
            "the plan states no `sample.max_partitions`, so it is unbounded; the signed scope \
             permits at most {}",
            scope.max_partitions
        )),
        (crate::spec::Coverage::Sampled, Some(max)) if max > scope.max_partitions => mismatches
            .push(format!(
                "the plan samples up to {max} partitions; the signed scope permits {}",
                scope.max_partitions
            )),
        (crate::spec::Coverage::Sampled, Some(_)) => {}
    }
    // The signer's record bound on a complete verification, when they stated
    // one: ABSENT IN THE PLAN IS UNBOUNDED, and so a mismatch, exactly as an
    // absent partition bound is for a sampled plan.
    if let (crate::spec::Coverage::Complete, Some(cap)) =
        (facts.coverage, scope.complete_max_records)
    {
        match facts.complete_max_records {
            None => mismatches.push(format!(
                "the plan states no `sample.complete_max_records`, so its complete \
                 verification is unbounded; the signed scope permits at most {cap} records"
            )),
            Some(n) if n > cap => mismatches.push(format!(
                "the plan's complete verification may decode {n} records; the signed scope \
                 permits {cap}"
            )),
            Some(_) => {}
        }
    }
    // A standing rehearsal restores every partition
    // from the archive's floor. A plan stating a replay selection would sign a
    // narrowed verdict no approver saw — the plan bytes are not hash-bound to
    // the standing authorization, only checked against its scope — so any
    // selection is outside every signed scope.
    if !facts.replay_selection.is_empty() {
        mismatches.push(format!(
            "the plan states a replay selection ({}); a standing rehearsal authorization \
             restores every partition from the archive's floor and admits no narrowing \
             nobody approved",
            facts.replay_selection.join(", ")
        ));
    }
    // The override that restores transaction markers as data needs an approver
    // to have seen it, for the same reason.
    if facts.allows_transaction_markers {
        mismatches.push(
            "the plan states restore.allow_transaction_markers: true; a standing rehearsal \
             authorization never authorises restoring transaction markers as data"
                .to_string(),
        );
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
    /// could forge a second line gets NOTHING on stdout. `hunter2` and `s3cr3t`
    /// pass every charset rule anyone would write, which is exactly why the
    /// vocabulary is a closed TABLE and not a character class.
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
            coverage: None,
            complete_max_records: None,
        }
    }

    /// A 1.1.0 scope that authorises complete coverage, as it is signed: no
    /// record bound, and `maxPartitions: 0`
    /// ([`crate::rehearsal_scope::COMPLETE_SCOPE_MAX_PARTITIONS`]).
    fn complete_scope() -> RehearsalScope {
        RehearsalScope {
            coverage: Some(crate::spec::Coverage::Complete),
            max_partitions: crate::rehearsal_scope::COMPLETE_SCOPE_MAX_PARTITIONS,
            ..scope()
        }
    }

    /// The facts of a complete plan: no partition bound, no record bound.
    fn complete_facts() -> PlanScopeFacts {
        PlanScopeFacts {
            coverage: crate::spec::Coverage::Complete,
            max_partitions: None,
            ..facts()
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
            replay_selection: Vec::new(),
            coverage: crate::spec::Coverage::Sampled,
            complete_max_records: None,
            allows_transaction_markers: false,
        }
    }

    /// **A standing rehearsal authorization admits no
    /// replay selection: a plan stating a window start (the interval form of
    /// `restore.point_in_time`) or `restore.partitions` is outside every
    /// signed scope, named, whatever else it matches. The control is the same
    /// plan with a plain point in time, which is inside. KILLS: the mismatch
    /// deleted (a narrowed rehearsal would sign a verdict no approver saw),
    /// `plan_scope_facts` not reading the start.
    #[test]
    fn a_plan_stating_a_replay_selection_is_outside_every_standing_scope() {
        let plan = |restore: &str| -> crate::spec::DrillSpec {
            serde_yaml::from_str(&format!(
                "source:\n  storage: {{backend: filesystem, path: /a}}\n  topics: [orders]\n\
                 target:\n  bootstrap_servers: [k:9092]\n  topic_mapping_prefix: rehearsal-3f2a91c7-\n\
                 sample:\n  window_start: \"2026-01-01T00:00:00Z\"\n  window_end: \"2026-01-02T00:00:00Z\"\n  \
                 max_partitions: 200\n\
                 restore:\n{restore}\
                 objectives: {{}}\n\
                 evidence: {{backend: filesystem, path: /e}}\n"
            ))
            .expect("the plan parses")
        };
        let allowed: crate::spec::AllowedClusters =
            serde_json::from_str(r#"{"allowed_cluster_ids": ["TARGET00000000000000000"]}"#)
                .expect("the allowlist parses");
        let control = plan_scope_facts(
            &plan("  point_in_time: \"2026-01-01T12:00:00Z\"\n"),
            &allowed,
        );
        assert!(control.replay_selection.is_empty());
        plan_within_scope(&control, &scope()).expect("a plain point in time is inside the scope");

        let narrowed = plan_scope_facts(
            &plan("  point_in_time: \"2026-01-01T06:00:00Z/2026-01-01T12:00:00Z\"\n"),
            &allowed,
        );
        let refusal =
            plan_within_scope(&narrowed, &scope()).expect_err("a window start is refused");
        assert_eq!(refusal.mismatches.len(), 1, "{refusal}");
        assert!(
            refusal.mismatches[0].starts_with(
                "the plan states a replay selection (a window start (restore.point_in_time"
            ),
            "{refusal}"
        );
        let subset = plan_scope_facts(
            // A subset is written beside the open-start interval.
            &plan("  point_in_time: \"../2026-01-01T12:00:00Z\"\n  partitions: {orders: [1]}\n"),
            &allowed,
        );
        let refusal = plan_within_scope(&subset, &scope()).expect_err("a subset is refused");
        assert!(
            refusal.mismatches[0].contains("partition subsets (restore.partitions)"),
            "{refusal}"
        );
    }

    /// **A standing rehearsal authorization never
    /// admits `restore.allow_transaction_markers: true`: no approver signed the
    /// plan bytes. The control is the same plan stating `false`, which is
    /// inside. KILLS: the mismatch deleted, `plan_scope_facts` not reading the
    /// field.
    #[test]
    fn a_plan_allowing_transaction_markers_is_outside_every_standing_scope() {
        let plan = |allow: bool| -> crate::spec::DrillSpec {
            serde_yaml::from_str(&format!(
                "source:\n  storage: {{backend: filesystem, path: /a}}\n  topics: [orders]\n\
                 target:\n  bootstrap_servers: [k:9092]\n  topic_mapping_prefix: rehearsal-3f2a91c7-\n\
                 sample:\n  window_start: \"2026-01-01T00:00:00Z\"\n  window_end: \"2026-01-02T00:00:00Z\"\n  \
                 max_partitions: 200\n\
                 restore:\n  point_in_time: \"2026-01-01T12:00:00Z\"\n  allow_transaction_markers: {allow}\n\
                 objectives: {{}}\n\
                 evidence: {{backend: filesystem, path: /e}}\n"
            ))
            .expect("the plan parses")
        };
        let allowed: crate::spec::AllowedClusters =
            serde_json::from_str(r#"{"allowed_cluster_ids": ["TARGET00000000000000000"]}"#)
                .expect("the allowlist parses");
        let control = plan_scope_facts(&plan(false), &allowed);
        assert!(!control.allows_transaction_markers);
        plan_within_scope(&control, &scope()).expect("`false` is inside the scope");
        let allowing = plan_scope_facts(&plan(true), &allowed);
        let refusal = plan_within_scope(&allowing, &scope()).expect_err("the override is refused");
        assert_eq!(
            refusal.mismatches,
            vec![
                "the plan states restore.allow_transaction_markers: true; a standing rehearsal \
                 authorization never authorises restoring transaction markers as data"
                    .to_string()
            ]
        );
    }

    /// **The coverage is SIGNED**:
    ///
    /// 1. a complete plan under a scope that states no `coverage` (every 1.0.0
    ///    scope) or says `sampled` is REFUSED, by name;
    /// 2. the same plan under a scope that authorises `complete` is admitted —
    ///    its absent `max_partitions` is not an unbounded sample;
    /// 3. a sampled plan with no partition bound is still refused, under
    ///    either scope;
    /// 4. a complete plan that states `max_partitions` is still refused.
    ///
    /// KILLS: the coverage-equality arm removed (row 1 admits); the
    /// `(Complete, None)` arm folded into the unbounded refusal (row 2
    /// refuses); the `(Sampled, None)` arm admitted (row 3); the
    /// `(Complete, Some)` arm admitted (row 4).
    #[test]
    fn a_complete_plan_is_admitted_only_under_a_scope_that_signed_complete() {
        // 1. an old scope, and an explicitly sampled one.
        for (what, signed) in [
            ("no coverage", scope()),
            (
                "coverage: sampled",
                RehearsalScope {
                    coverage: Some(crate::spec::Coverage::Sampled),
                    ..scope()
                },
            ),
        ] {
            let refusal = plan_within_scope(&complete_facts(), &signed)
                .expect_err("a complete plan under a sampled-only scope");
            let rendered = refusal.to_string();
            assert!(
                rendered.contains("`sample.coverage: complete`")
                    && rendered.contains("authorises `sampled` coverage"),
                "{what}: {rendered}"
            );
            assert_eq!(
                refusal.mismatches.len(),
                1,
                "{what}: only the coverage: {rendered}"
            );
        }
        // 2. the same plan under a scope that signed complete.
        plan_within_scope(&complete_facts(), &complete_scope())
            .expect("a complete plan under a complete scope samples nothing to bound");
        // 3. a sampled plan with no bound, under either scope.
        for signed in [scope(), complete_scope()] {
            let unbounded = PlanScopeFacts {
                max_partitions: None,
                ..facts()
            };
            let refusal = plan_within_scope(&unbounded, &signed).expect_err("unbounded");
            assert!(
                refusal
                    .to_string()
                    .contains("states no `sample.max_partitions`"),
                "{refusal}"
            );
        }
        // 4. complete plus a partition bound.
        let both = PlanScopeFacts {
            max_partitions: Some(10),
            ..complete_facts()
        };
        let refusal = plan_within_scope(&both, &complete_scope()).expect_err("the pair");
        let rendered = refusal.to_string();
        assert!(
            rendered.contains("`sample.coverage: complete`")
                && rendered.contains("`sample.max_partitions: 10`"),
            "{rendered}"
        );
    }

    /// A scope that signed complete coverage refuses a SAMPLED plan: a weaker
    /// check than the signer chose is not the one they authorised.
    ///
    /// The complete scope signs `maxPartitions: 0`, so an ordinary
    /// sampled plan is ALSO past its partition bound (two mismatches). The
    /// plan whose `max_partitions` is 0 — the one sampled plan a reader older
    /// than format 1.1.0 admits under this scope (it samples nothing, and
    /// every build's phase 7 refuses an empty selection) — is inside the bound,
    /// so the coverage equality is what refuses it here, alone.
    ///
    /// KILLS: the equality reduced to "complete needs a complete scope" (the
    /// zero-bound sampled plan, inside every other bound, would be admitted).
    #[test]
    fn a_sampled_plan_under_a_complete_scope_is_refused() {
        let words = "`sample.coverage: sampled` and the signed scope authorises `complete`";
        let refusal = plan_within_scope(&facts(), &complete_scope()).expect_err("downgraded");
        assert!(refusal.to_string().contains(words), "{refusal}");
        assert_eq!(refusal.mismatches.len(), 2, "coverage and bound: {refusal}");
        let zero_bound = PlanScopeFacts {
            max_partitions: Some(0),
            ..facts()
        };
        let refusal = plan_within_scope(&zero_bound, &complete_scope()).expect_err("downgraded");
        assert!(refusal.to_string().contains(words), "{refusal}");
        assert_eq!(refusal.mismatches.len(), 1, "the coverage alone: {refusal}");
    }

    /// The signer's record bound: a complete plan under a scope stating
    /// `completeMaxRecords` must state its own bound, no larger. Absent in the
    /// plan is unbounded and refused; a larger bound is refused; an equal or
    /// smaller one is admitted; with no bound in the scope, any plan bound (or
    /// none) is inside it.
    ///
    /// KILLS: the `None` arm admitted; the comparison reversed or `>=`.
    #[test]
    fn a_signed_record_bound_holds_the_complete_plan() {
        let bounded = RehearsalScope {
            complete_max_records: Some(1_000),
            ..complete_scope()
        };
        let refusal = plan_within_scope(&complete_facts(), &bounded).expect_err("unbounded");
        assert!(
            refusal
                .to_string()
                .contains("states no `sample.complete_max_records`"),
            "{refusal}"
        );
        let over = PlanScopeFacts {
            complete_max_records: Some(1_001),
            ..complete_facts()
        };
        let refusal = plan_within_scope(&over, &bounded).expect_err("over the bound");
        assert!(
            refusal.to_string().contains("may decode 1001 records"),
            "{refusal}"
        );
        for n in [1_000, 1] {
            let inside = PlanScopeFacts {
                complete_max_records: Some(n),
                ..complete_facts()
            };
            plan_within_scope(&inside, &bounded).expect("inside the signed bound");
            plan_within_scope(&inside, &complete_scope()).expect("no signed bound");
        }
    }

    /// The document carrying the new fields must say so, and they must be
    /// coherent: `coverage`/`completeMaxRecords` under formatVersion 1.0.0 is
    /// refused; a record bound beside a sampled-only scope is refused; a bound
    /// of 0 is refused; the same scope at 1.1.0 is admitted, and a 1.0.0
    /// document without them is admitted as before.
    ///
    /// KILLS: the minor check removed (the first refusal); `<` turned `<=`
    /// (the 1.1.0 admission); each arm of the coherence match.
    #[test]
    fn the_coverage_fields_need_format_1_1_and_cohere() {
        let now = chrono::DateTime::parse_from_rfc3339("2026-06-02T00:00:00Z")
            .unwrap()
            .with_timezone(&chrono::Utc);
        let doc = |version: &str, scope: RehearsalScope| StandingAuthorization {
            format_version: version.to_string(),
            kind: STANDING_AUTHORIZATION_KIND.to_string(),
            subject_ref: AuthorizationSubject {
                api_version: SUBJECT_API_VERSION.to_string(),
                kind: REHEARSAL_SCHEDULE_KIND.to_string(),
                namespace: "team-a".into(),
                name: "weekly".into(),
                uid: "u-1".into(),
            },
            scope,
            issued_at: now - chrono::Duration::days(1),
            expires_at: now + chrono::Duration::days(1),
        };
        let refused =
            admit_standing_authorization(&doc("1.0.0", complete_scope()), Some("u-1"), now)
                .expect_err("a 1.0.0 document cannot carry coverage");
        assert!(
            refused.detail.contains("defined from formatVersion 1.1.0"),
            "{refused}"
        );
        admit_standing_authorization(&doc("1.1.0", complete_scope()), Some("u-1"), now)
            .expect("1.1.0 carries it");
        admit_standing_authorization(&doc("1.0.0", scope()), Some("u-1"), now)
            .expect("a 1.0.0 scope without it, as before");
        // A version with NO numeric minor is not "1.1.0 or
        // later": the fields are refused under it. The control: the same
        // versions are admitted for a scope that does not carry them (the
        // major is 1), so the refusal is the minor's, and a later minor
        // carries them.
        for version in ["1", "1.x.0", "1."] {
            let refused =
                admit_standing_authorization(&doc(version, complete_scope()), Some("u-1"), now)
                    .expect_err("no numeric minor cannot carry coverage");
            assert!(
                refused.detail.contains("defined from formatVersion 1.1.0"),
                "{version}: {refused}"
            );
            admit_standing_authorization(&doc(version, scope()), Some("u-1"), now)
                .unwrap_or_else(|e| panic!("the control, {version} without the fields: {e}"));
        }
        admit_standing_authorization(&doc("1.2.0", complete_scope()), Some("u-1"), now)
            .expect("a later minor carries them");
        let sampled_bound = RehearsalScope {
            complete_max_records: Some(5),
            ..scope()
        };
        let refused = admit_standing_authorization(&doc("1.1.0", sampled_bound), Some("u-1"), now)
            .expect_err("a bound beside sampled");
        assert!(refused.detail.contains("does not authorise"), "{refused}");
        let zero = RehearsalScope {
            complete_max_records: Some(0),
            ..complete_scope()
        };
        let refused = admit_standing_authorization(&doc("1.1.0", zero), Some("u-1"), now)
            .expect_err("a bound of 0");
        assert!(refused.detail.contains("is 0"), "{refused}");
        // A COMPLETE SCOPE SIGNS `maxPartitions: 0`: any other
        // value is a document an older reader would read as a sampled scope
        // with a usable partition bound, so it is refused. The control is
        // `complete_scope()` itself, admitted above, and a sampled scope with
        // a positive bound, admitted as before.
        for wide in [1, 200] {
            let complete_wide = RehearsalScope {
                max_partitions: wide,
                ..complete_scope()
            };
            let refused =
                admit_standing_authorization(&doc("1.1.0", complete_wide), Some("u-1"), now)
                    .expect_err("a complete scope with a partition bound");
            assert!(
                refused
                    .detail
                    .contains(&format!("states `maxPartitions: {wide}`"))
                    && refused.detail.contains("signs `maxPartitions: 0`"),
                "{refused}"
            );
        }
        admit_standing_authorization(&doc("1.0.0", scope()), Some("u-1"), now)
            .expect("a sampled scope keeps its positive bound");

        let json = serde_json::to_string(&scope()).expect("serialises");
        assert!(
            !json.contains("coverage") && !json.contains("completeMaxRecords"),
            "a scope without the fields serialises as a 1.0.0 scope did: {json}"
        );
        let json = serde_json::to_string(&complete_scope()).expect("serialises");
        assert!(json.contains("\"coverage\":\"complete\""), "{json}");
    }

    /// The facts carry the plan's own coverage, so the runner's half of
    /// "checked twice" reads what the mounted bytes say.
    #[test]
    fn the_scope_facts_carry_the_plans_coverage() {
        let yaml = |coverage: &str| {
            format!(
                "source:\n  storage: {{backend: filesystem, path: /a}}\n  backup: b\n  \
                 topics: [orders]\ntarget:\n  bootstrap_servers: [\"k:9092\"]\n  mode: scratch\n  \
                 topic_mapping_prefix: rehearsal-3f2a91c7-\n  marker_topic: m\n\
                 sample:\n  window_start: \"2026-01-01T00:00:00Z\"\n  \
                 window_end: \"2026-01-01T01:00:00Z\"\n{coverage}\
                 objectives: {{}}\nevidence: {{backend: filesystem, path: /e}}\n"
            )
        };
        let allowed = crate::spec::AllowedClusters {
            allowed_cluster_ids: vec!["TARGET00000000000000000".into()],
            source_cluster_id: None,
        };
        let sampled: crate::spec::DrillSpec = serde_yaml::from_str(&yaml("")).expect("a plan");
        assert_eq!(
            plan_scope_facts(&sampled, &allowed).coverage,
            crate::spec::Coverage::Sampled
        );
        let complete: crate::spec::DrillSpec =
            serde_yaml::from_str(&yaml("  coverage: complete\n")).expect("a plan");
        assert_eq!(
            plan_scope_facts(&complete, &allowed).coverage,
            crate::spec::Coverage::Complete
        );
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

    /// A standing authorization is capped at 90 days, and the cap is a
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

    /// Key-usage separation: the installation's own evidence key may
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
        // Unknown fields are IGNORED, so a document later enriched with
        // `policy`/`requester`/`ticket` still verifies against this build.
        let enriched = json.replace(
            "{\"formatVersion\"",
            "{\"requester\":{\"issuer\":\"https://idp\"},\"formatVersion\"",
        );
        let with_extras: StandingAuthorization =
            serde_json::from_str(&enriched).expect("an enriched document still parses");
        assert_eq!(with_extras.scope, doc.scope);
    }

    /// `sample.max_partitions` IS a plan field, and absent is UNBOUNDED,
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

    /// **`STANDING_MANDATORY_ENV` is a hand-written list, and this is
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

    /// The `teardown-failed=` line round-trips, is held to the
    /// CRD's bounds on both sides, and carries only names a broker accepts.
    #[test]
    fn the_teardown_failed_line_is_bounded_and_round_trips() {
        let failed = vec![
            (
                "drill-a".to_string(),
                "Broker: Topic authorization failed".to_string(),
            ),
            ("bad name\n".to_string(), "dropped".to_string()),
            ("drill-b".to_string(), format!("x\ny{}", "e".repeat(400))),
        ];
        let line = teardown_failed_line(&failed).expect("a line");
        assert_eq!(line.lines().count(), 1, "{line}");
        let value = line
            .strip_prefix(TEARDOWN_FAILED_PREFIX)
            .expect("the prefix");
        let back = parse_teardown_failed(value).expect("parsed");
        assert_eq!(back.len(), 2);
        assert_eq!(back[0].topic, "drill-a");
        assert_eq!(back[0].error, "Broker: Topic authorization failed");
        assert_eq!(back[1].topic, "drill-b");
        assert!(back[1].error.starts_with("x y"));
        assert_eq!(
            back[1].error.chars().count(),
            TEARDOWN_FAILED_MAX_ERROR_CHARS
        );
        // Nothing left, no line; a value that is not the array, nothing read.
        assert!(teardown_failed_line(&[]).is_none());
        assert!(parse_teardown_failed("{}").is_none());
        assert!(parse_teardown_failed("[]").is_none());
        assert!(parse_teardown_failed(&" ".repeat(TEARDOWN_FAILED_MAX_VALUE_BYTES + 1)).is_none());
        let many: Vec<(String, String)> = (0..300)
            .map(|i| (format!("drill-{i}"), "e".to_string()))
            .collect();
        let line = teardown_failed_line(&many).unwrap();
        let value = line.strip_prefix(TEARDOWN_FAILED_PREFIX).unwrap();
        assert_eq!(
            parse_teardown_failed(value).unwrap().len(),
            TEARDOWN_FAILED_MAX_TOPICS
        );
    }

    /// FX-38 (8): past the CRD's 256 names the `teardown-failed=` list is cut
    /// and its `teardown-failed-count=` line says how many were left; an
    /// illegal name is in neither. CONTROL: one topic is a list of one and a
    /// count of one; nothing left prints nothing.
    #[test]
    fn a_cut_teardown_list_carries_its_count() {
        let left: Vec<(String, String)> = (0..300)
            .map(|i| (format!("drill-t{i}"), "x".to_string()))
            .chain(std::iter::once(("bad name".to_string(), "x".to_string())))
            .collect();
        let lines = teardown_failed_lines(&left);
        assert_eq!(lines.len(), 2);
        let listed =
            parse_teardown_failed(lines[0].strip_prefix(TEARDOWN_FAILED_PREFIX).unwrap()).unwrap();
        assert_eq!(listed.len(), TEARDOWN_FAILED_MAX_TOPICS);
        assert_eq!(lines[1], format!("{TEARDOWN_FAILED_COUNT_PREFIX}300"));
        let one = teardown_failed_lines(&left[..1]);
        assert_eq!(one[1], format!("{TEARDOWN_FAILED_COUNT_PREFIX}1"));
        assert!(teardown_failed_lines(&[]).is_empty());
    }
}
