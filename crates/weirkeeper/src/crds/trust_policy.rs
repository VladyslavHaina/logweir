//! `TrustPolicy` — which keys may authorise, which may attest, and what
//! happened to each of them since.
//!
//! # Cluster-scoped, for the reason the roster is (ADR 0008 Amendment G)
//!
//! "A roster whose name the subject supplies is a roster the subject can
//! choose" (`docs/kubernetes.md` §8). A namespace therefore never names its own
//! trust: the POLICY names the namespaces it governs, and a namespace claimed
//! by two policies resolves to **nothing** rather than to whichever one a
//! reconciler happened to list first.
//!
//! # Mutable, and monotonic
//!
//! This is the half of PLAT-19.1 that `TrustRoster` cannot do: a key has a
//! lifecycle. So the spec is deliberately NOT sealed — an administrator
//! retires and revokes keys on a live policy — and an object-level CEL rule
//! makes every change one-way instead:
//!
//! - every `keyId` that existed still exists (one object-level rule), with
//!   identical public material (one per-key rule);
//! - `notAfter` may only move earlier;
//! - `state` moves `Active → Retired`, `Active|Retired → Revoked`, nowhere else;
//! - `revokedAt` and `revocationEffectiveFrom` are write-once;
//! - a `KeyCompromise` revocation stays a compromise (G9).
//!
//! **Public material cannot be edited away**, because old archives still need
//! it: a receipt signed in March must still verify in December, and a policy
//! that could drop the key would make every archive it signed unverifiable in
//! one `kubectl apply`. Retirement and revocation change the VERDICT for new
//! and stored evidence (§7.4); they never remove the key.
//!
//! # Why the rules are object-level, and why the list is an associative list
//!
//! Object-level for the reason [`super::backup_schedule::SUSPEND_ONLY_RULE`]
//! is: a per-field transition rule does not fire on an absent → present
//! transition, and `optionalOldSelf` is 1.30+, above the 1.29 floor. The
//! map/`filter` form of the rule does not compile at all — the measured API
//! server error is quoted in that module's header, and this file does not
//! repeat it.
//!
//! `spec.keys` is declared `x-kubernetes-list-type: map` keyed by `keyId`, and
//! that does two jobs. The API server itself refuses a duplicate key id — the
//! CEL alternative is a quadratic self-join to enforce something the server
//! already enforces. And it lets the PER-KEY rules be transition rules: the
//! server correlates `oldSelf` with the entry that has the same key, so
//! [`G7_KEY_MATERIAL_IS_IMMUTABLE_RULE`] and the three lifecycle rules are
//! evaluated once per item instead of inside a quadratic walk. That is not an
//! optimisation: the quadratic form was REFUSED by a live API server for
//! exceeding the CEL cost budget, and the measurement is quoted at G1.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use super::{Condition, SpecRule, Time};

/// A key id: the sha256 of the DER SPKI, lowercase hex.
pub const KEY_ID_PATTERN: &str = "^[0-9a-f]{64}$";

/// A namespace name — a DNS-1123 label.
pub const NAMESPACE_PATTERN: &str = "^[a-z0-9]([-a-z0-9]*[a-z0-9])?$";

/// G1 — every key that existed still exists.
///
/// # Why this is the ONLY object-level rule, and why it compares key ids alone
///
/// It is the only one that cannot be asked of an item: a key that has been
/// REMOVED has no `self` to attach a rule to. So it is a quadratic walk, and
/// the only thing it compares is the 64-character `keyId` — the per-key
/// material checks moved to [`G7_KEY_MATERIAL_IS_IMMUTABLE_RULE`], which the
/// API server evaluates once per item.
///
/// MEASURED, NOT GUESSED. The first shape of this rule did every comparison
/// inside the quadratic walk, including `spkiPem` at `maxLength: 4096`, and a
/// live `kubectl --context docker-desktop apply` on 2026-09-16 refused the
/// whole CRD:
///
/// ```text
/// spec.validation.openAPIV3Schema.properties[spec].x-kubernetes-validations[0].rule:
///   Forbidden: estimated rule cost exceeds budget by factor of more than 100x
/// spec.validation.openAPIV3Schema: Forbidden: x-kubernetes-validations estimated
///   rule cost total for entire OpenAPIv3 schema exceeds budget by factor of 51.6x
/// ```
///
/// A CRD the API server will not install is a CRD that seals nothing, which is
/// the same failure the map-form rule in
/// [`super::backup_schedule`]'s header records. The split below is what makes
/// it installable.
pub const G1_KEYS_ARE_APPEND_ONLY_RULE: &str =
    "oldSelf.keys.all(o, self.keys.exists(n, n.keyId == o.keyId))";
/// G1's message.
pub const G1_KEYS_ARE_APPEND_ONLY_MESSAGE: &str = "spec.keys is append-only: an existing keyId may not be removed — old archives still need the public material that signed them";

/// G7 — a key's public material and identity are immutable.
///
/// A TRANSITION RULE ON ONE ITEM OF AN ASSOCIATIVE LIST. `spec.keys` is
/// `x-kubernetes-list-type: map` keyed by `keyId`, so the API server
/// correlates `oldSelf` with the entry that has the same key and evaluates
/// this once per item — O(n) rather than the O(n²) it replaced. A NEWLY added
/// key has no `oldSelf` and the rule is simply not evaluated for it, which is
/// exactly the append-only semantics wanted.
///
/// `usages` is compared because a usage set that could be widened in place
/// would let an evidence-signing key become an approval key without anybody
/// issuing a new one.
pub const G7_KEY_MATERIAL_IS_IMMUTABLE_RULE: &str = "self.spkiPem == oldSelf.spkiPem && self.algorithm == oldSelf.algorithm && self.usages == oldSelf.usages && self.principal.id == oldSelf.principal.id && self.notBefore == oldSelf.notBefore";
/// G7's message.
pub const G7_KEY_MATERIAL_IS_IMMUTABLE_MESSAGE: &str = "a key's spkiPem, algorithm, usages, principal.id and notBefore are immutable; issue a new keyId instead";

/// G2 — `notAfter` may only move earlier. Per item, like G7.
pub const G2_NOT_AFTER_ONLY_SHORTENS_RULE: &str = "self.notAfter <= oldSelf.notAfter";
/// G2's message.
pub const G2_NOT_AFTER_ONLY_SHORTENS_MESSAGE: &str =
    "a key's notAfter may only be brought forward, never extended";

/// G3 — `state` is monotonic: `Active → Retired`, `Active|Retired → Revoked`.
/// Per item, like G7.
pub const G3_STATE_IS_MONOTONIC_RULE: &str = "oldSelf.state == 'Active' ? true : (oldSelf.state == 'Retired' ? self.state != 'Active' : self.state == 'Revoked')";
/// G3's message.
pub const G3_STATE_IS_MONOTONIC_MESSAGE: &str =
    "a key's state moves Active -> Retired, Active|Retired -> Revoked, and never backwards";

/// G4 — the revocation instants are write-once. Per item, like G7.
///
/// `revocationEffectiveFrom` is what decides whether stored evidence signed
/// before a compromise is still trusted (§7.4). A field that could be moved
/// later is a field that can be moved past an attacker's signature.
pub const G4_REVOCATION_IS_WRITE_ONCE_RULE: &str = "(!has(oldSelf.revokedAt) || (has(self.revokedAt) && self.revokedAt == oldSelf.revokedAt)) && (!has(oldSelf.revocationEffectiveFrom) || (has(self.revocationEffectiveFrom) && self.revocationEffectiveFrom == oldSelf.revocationEffectiveFrom))";
/// G4's message.
pub const G4_REVOCATION_IS_WRITE_ONCE_MESSAGE: &str =
    "revokedAt and revocationEffectiveFrom are immutable once written";

/// G9 — a `KeyCompromise` revocation stays a compromise. Per item, like G7.
///
/// # The edit G3 and G4 left open (`TRUSTPOLICY-DELETE-DROPS-REVOCATION`)
///
/// G3 keeps a revoked key revoked and G4 keeps its instants, but
/// `revocationReason` was free: editing `KeyCompromise` to `Superseded` in
/// place turns D3 §7.4's compromise row ("never green", the document's own
/// claim not accepted) into a retirement at `revocationEffectiveFrom`, and
/// every document the key signed before that instant re-verifies
/// `Valid`/`Historical` — the same re-trust as deleting the policy, in one
/// `kubectl apply`. So once a REVOKED key's reason is `KeyCompromise` it stays
/// so. The other direction stays open on purpose: a key revoked as
/// `Superseded` whose private half later turns out to be exposed must be
/// escalatable.
///
/// Guarded on `oldSelf.state == 'Revoked'` because the reason is read only
/// for a revoked key; a `KeyCompromise` written beside `Active` records
/// nothing and fixes nothing either way.
pub const G9_COMPROMISE_IS_STICKY_RULE: &str = "oldSelf.state != 'Revoked' || !has(oldSelf.revocationReason) || oldSelf.revocationReason != 'KeyCompromise' || (has(self.revocationReason) && self.revocationReason == 'KeyCompromise')";
/// G9's message.
pub const G9_COMPROMISE_IS_STICKY_MESSAGE: &str = "a key revoked for KeyCompromise stays revoked for KeyCompromise: changing the reason would re-trust every document it signed before revocationEffectiveFrom";

/// G5 — a `Revoked` key carries both revocation instants; a `Retired` one
/// carries `retiredAt`.
pub const G5_LIFECYCLE_FIELDS_RULE: &str = "(self.state != 'Revoked' || (has(self.revokedAt) && has(self.revocationEffectiveFrom))) && (self.state != 'Retired' || has(self.retiredAt))";
/// G5's message.
pub const G5_LIFECYCLE_FIELDS_MESSAGE: &str =
    "a Revoked key needs revokedAt and revocationEffectiveFrom; a Retired key needs retiredAt";

/// G6 — validity runs forwards.
pub const G6_VALIDITY_ORDER_RULE: &str = "self.notBefore < self.notAfter";
/// G6's message.
pub const G6_VALIDITY_ORDER_MESSAGE: &str = "notBefore must be before notAfter";

/// G8 — one key, one usage.
///
/// # Why `size() == 1` is the whole of §7.3's three exclusions
///
/// D3 §7.3 forbids `EvidenceSigning` with either approval usage and forbids
/// `GovernedApproval` with `ConfirmationIssuer` (landed here as
/// [`KeyUsage::ConsoleConfirmation`]). With exactly three enum values those
/// three pairs are every pair there is, so "at most one usage" is EXACTLY
/// equivalent to the pairwise form and costs the estimator one `size()` call
/// instead of three list scans. `usages` already has `length(min = 1)`, so the
/// rule reads as "exactly one".
///
/// # A NON-TRANSITION rule, per item, and why the distinction matters
///
/// Beside G5 and G6, not beside G7. A transition rule is not evaluated for a
/// newly added key, which would leave a dual-usage key creatable on the very
/// path — appending to `spec.keys` — that G1 makes the only path there is.
///
/// # Why it lands NOW rather than with its consumer
///
/// **The window closes permanently the first time a policy is applied.** G7
/// makes `usages` immutable and G1 makes `spec.keys` append-only, so a
/// dual-usage key created before this rule exists can never be narrowed and
/// never be removed — no Logweir role holds `delete` on `trustpolicies`. And
/// adding the rule afterwards BRICKS that object: a per-item non-transition
/// rule is evaluated on every write, so the policy becomes un-updatable and
/// its keys can never be retired or revoked. A cluster today has no
/// `TrustPolicy` at all, which is the only moment this costs nothing.
///
/// # One key on two roster lists
///
/// `logweir trust migrate-roster` REFUSES such a roster, naming the
/// overlapping `keyId`s, rather than emitting an entry the API server would
/// reject at `kubectl apply` time. `weirkeeper::trust::synthesize_legacy`
/// still merges them in memory: it is never written as an object, this rule
/// never sees it, and the merge is what keeps an unmigrated roster-only
/// cluster working (§7.3 keeps the `selfAttestedRisk` LABEL for exactly those
/// namespaces).
pub const G8_ONE_USAGE_PER_KEY_RULE: &str = "self.usages.size() == 1";
/// G8's message.
pub const G8_ONE_USAGE_PER_KEY_MESSAGE: &str = "a key declares exactly one usage: EvidenceSigning, GovernedApproval and ConsoleConfirmation are mutually exclusive, because a key that both attests and authorises is a key whose holder can approve their own work — issue a separate keyId per usage";

/// The rules on `.spec`: exactly the one that cannot be asked of an item.
pub const SPEC_RULES: [SpecRule; 1] = [SpecRule::new(
    G1_KEYS_ARE_APPEND_ONLY_RULE,
    G1_KEYS_ARE_APPEND_ONLY_MESSAGE,
)];

/// The NON-transition rules attached to one key entry.
pub const NESTED_RULES: [(&[&str], &str, &str); 3] = [
    (
        &["keys", "[]"],
        G5_LIFECYCLE_FIELDS_RULE,
        G5_LIFECYCLE_FIELDS_MESSAGE,
    ),
    (
        &["keys", "[]"],
        G6_VALIDITY_ORDER_RULE,
        G6_VALIDITY_ORDER_MESSAGE,
    ),
    (
        &["keys", "[]"],
        G8_ONE_USAGE_PER_KEY_RULE,
        G8_ONE_USAGE_PER_KEY_MESSAGE,
    ),
];

/// The TRANSITION rules attached to one key entry, which the API server
/// correlates by `keyId` because the list is an associative list.
pub const KEY_TRANSITION_RULES: [(&[&str], &str, &str); 5] = [
    (
        &["keys", "[]"],
        G7_KEY_MATERIAL_IS_IMMUTABLE_RULE,
        G7_KEY_MATERIAL_IS_IMMUTABLE_MESSAGE,
    ),
    (
        &["keys", "[]"],
        G2_NOT_AFTER_ONLY_SHORTENS_RULE,
        G2_NOT_AFTER_ONLY_SHORTENS_MESSAGE,
    ),
    (
        &["keys", "[]"],
        G3_STATE_IS_MONOTONIC_RULE,
        G3_STATE_IS_MONOTONIC_MESSAGE,
    ),
    (
        &["keys", "[]"],
        G4_REVOCATION_IS_WRITE_ONCE_RULE,
        G4_REVOCATION_IS_WRITE_ONCE_MESSAGE,
    ),
    (
        &["keys", "[]"],
        G9_COMPROMISE_IS_STICKY_RULE,
        G9_COMPROMISE_IS_STICKY_MESSAGE,
    ),
];

/// The path and merge key of the associative list.
pub const KEYS_LIST_MAP: (&[&str], &[&str]) = (&["keys"], &["keyId"]);

/// What a key is allowed to be used for.
///
/// SEPARATED BECAUSE P17 REQUIRES IT FOR PLAT-19.2. One key that both signs
/// evidence and authorises restores is a key whose holder can approve their own
/// work; the usage set is what makes "the runner's signing identity" and "an
/// approver" different grants.
#[derive(Deserialize, Serialize, Clone, Copy, Debug, JsonSchema, PartialEq, Eq)]
pub enum KeyUsage {
    /// Verifies receipts, scorecards, teardown attestations, catalog records
    /// and retention records — the installation's own signing identity.
    EvidenceSigning,
    /// Verifies a governed approval document.
    GovernedApproval,
    /// Verifies a console confirmation signature (PLAT-19.2).
    ConsoleConfirmation,
}

/// The signature algorithm the public key uses.
#[derive(Deserialize, Serialize, Clone, Copy, Debug, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
#[schemars(rename_all = "lowercase")]
pub enum KeyAlgorithm {
    /// ECDSA over NIST P-256.
    P256,
    /// Ed25519.
    Ed25519,
}

/// Where a key is in its lifecycle. **Monotonic** (G3).
#[derive(Deserialize, Serialize, Clone, Copy, Debug, JsonSchema, PartialEq, Eq)]
pub enum KeyState {
    /// May sign something new, inside its validity window.
    Active,
    /// May no longer sign, but evidence it signed before `retiredAt` still
    /// verifies — `trust.basis: Historical`.
    Retired,
    /// Withdrawn. What that means for STORED evidence depends on
    /// `revocationReason`: a supersession is a retirement at
    /// `revocationEffectiveFrom`, a compromise is not.
    Revoked,
}

/// Why a key was revoked. The distinction is load-bearing for stored evidence.
#[derive(Deserialize, Serialize, Clone, Copy, Debug, JsonSchema, PartialEq, Eq)]
pub enum RevocationReason {
    /// The private key may be in someone else's hands. A document's own
    /// claimed signing time is attacker-controlled, so it is NOT accepted:
    /// only a controller-written observation from an earlier reconcile counts,
    /// and an imported archive with no such history fails closed.
    KeyCompromise,
    /// Replaced by a newer key, with no suspicion. Treated as a retirement at
    /// `revocationEffectiveFrom`.
    Superseded,
    /// No reason recorded. Treated as `Superseded`.
    Unspecified,
}

/// Who holds a key, as an identity and a label.
#[derive(Deserialize, Serialize, Clone, Debug, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct KeyPrincipal {
    /// A stable identifier. For a `GovernedApproval` key it MUST be the
    /// approver's `<issuer>#<subject>` exactly as the console's identity
    /// provider asserts them (for example `https://idp.example.com#8f3c…`):
    /// separation of duties compares it with the requester's, and any other
    /// form is refused under a Governed approval policy. Other usages may use
    /// `install:<digest>` or another stable id. **Immutable** (G1): it is what
    /// an audit trail joins on.
    #[schemars(length(min = 1, max = 253))]
    pub id: String,
    /// A human-readable label. Mutable, because a display name is not an
    /// identity.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(length(max = 253))]
    pub display: Option<String>,
}

/// One trusted public key, and what has happened to it.
#[derive(Deserialize, Serialize, Clone, Debug, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct TrustedKey {
    /// The sha256 of the DER SPKI, lowercase hex. The merge key of this list
    /// and the id every verdict is recorded against.
    ///
    /// `length` IS LOAD-BEARING AND NOT REDUNDANT WITH THE PATTERN. The CEL
    /// cost estimator reads `maxLength` and nothing else: with only a regex
    /// here it assumes the largest string a request could carry, and G1's
    /// quadratic walk over this field was refused live for exceeding the cost
    /// budget by more than 100x. A 64-character bound makes the same rule cost
    /// about 29,000 units against a 10,000,000 budget.
    #[schemars(regex(path = "KEY_ID_PATTERN"), length(min = 64, max = 64))]
    pub key_id: String,
    /// The PUBLIC key, PEM-encoded. **Public material and nothing else** — a
    /// private key in this field would be a private key in `kubectl get -o
    /// yaml`, and `logweir trust export` writes this object verbatim.
    #[schemars(length(min = 1, max = 4096))]
    pub spki_pem: String,
    /// The algorithm, checked by the controller against the PEM.
    pub algorithm: KeyAlgorithm,
    /// What this key may be used for. Immutable (G7).
    ///
    /// **EXACTLY ONE** (G8): the three usages are mutually exclusive, so a key
    /// that both attests and authorises cannot exist on a policy. See
    /// [`G8_ONE_USAGE_PER_KEY_RULE`] for why the rule had to land before the
    /// first policy object did.
    ///
    /// COMPARED AS AN ORDERED LIST, which is stricter than the property needs.
    /// CEL's `==` on a list is order-sensitive and this is not declared
    /// `x-kubernetes-list-type: set`, so re-ordering `usages` without changing
    /// the SET is refused with G7's message — over-strict, and harmless because
    /// a usage set is written once and never edited. Declaring it a set would
    /// relax the comparison; it would also let the API server merge entries
    /// from two appliers, which is not wanted on a field that decides what a
    /// key may authorise.
    #[schemars(length(min = 1, max = 1))]
    pub usages: Vec<KeyUsage>,
    /// Who holds it.
    pub principal: KeyPrincipal,
    /// The start of the validity window.
    pub not_before: Time,
    /// The end of the validity window. May only be brought forward (G2).
    pub not_after: Time,
    /// Where the key is in its lifecycle.
    pub state: KeyState,
    /// When it stopped being allowed to sign. Required for `Retired`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retired_at: Option<Time>,
    /// When it was revoked. Write-once (G4).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub revoked_at: Option<Time>,
    /// Why. Absent is read as `Unspecified`. Once a revoked key's reason is
    /// `KeyCompromise` it stays so (G9); a supersession may still be escalated
    /// to a compromise.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub revocation_reason: Option<RevocationReason>,
    /// The instant from which the revocation applies to stored evidence.
    /// Write-once (G4).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub revocation_effective_from: Option<Time>,
}

/// `TrustPolicy.spec`.
#[derive(kube::CustomResource, Deserialize, Serialize, Clone, Debug, JsonSchema)]
#[kube(
    group = "logweir.dev",
    version = "v1alpha1",
    kind = "TrustPolicy",
    doc = "The keys that may authorise and the keys that may attest, with an explicit lifecycle (ADR 0008 Amendment G). Cluster-scoped, so a namespace never names its own trust: the policy names the namespaces it governs. The spec is MUTABLE and every change is one-way — keys are append-only, notAfter only shortens, state only moves Active -> Retired -> Revoked, and the revocation instants are write-once. Public key material is never removed, because old archives still need it.",
    plural = "trustpolicies",
    singular = "trustpolicy",
    status = "TrustPolicyStatus",
    printcolumn = r#"{"name":"DEFAULT","type":"boolean","jsonPath":".spec.default"}"#,
    printcolumn = r#"{"name":"KEYS","type":"integer","jsonPath":".status.keyCount"}"#,
    printcolumn = r#"{"name":"LOADED","type":"string","jsonPath":".status.loaded"}"#,
    printcolumn = r#"{"name":"BOUND","type":"string","jsonPath":".status.boundNamespaces[*]"}"#,
    printcolumn = r#"{"name":"AGE","type":"date","jsonPath":".metadata.creationTimestamp"}"#
)]
#[serde(rename_all = "camelCase")]
pub struct TrustPolicySpec {
    /// The fallback for namespaces no policy names explicitly. At most one
    /// policy cluster-wide may set it; the controller reports a conflict
    /// rather than picking one.
    #[serde(default)]
    pub default: bool,
    /// The namespaces this policy governs, by exact name. Never a pattern: a
    /// pattern is how a new namespace silently inherits a trust decision
    /// nobody made for it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(
        length(max = 256),
        inner(regex(path = "NAMESPACE_PATTERN"), length(max = 63))
    )]
    pub namespaces: Option<Vec<String>>,
    /// The cluster ids a restore may target. Replaces
    /// `TrustRoster.spec.allowedClusterIds`, at the same scope and with the
    /// same semantics: a namespace tenant must not be able to widen it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(length(max = 64), inner(length(max = 253)))]
    pub allowed_target_cluster_ids: Option<Vec<String>>,
    /// The keys. Append-only (G1), keyed by `keyId`.
    #[schemars(length(max = 64))]
    pub keys: Vec<TrustedKey>,
}

/// One key, as the controller evaluated it.
#[derive(Deserialize, Serialize, Clone, Debug, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct KeyVerdict {
    /// Which key.
    pub key_id: String,
    /// `Active`, `NotYetValid`, `Expired`, `Retired`, `Revoked` or
    /// `Unparseable` — the spec state resolved against the clock and the PEM.
    pub effective_state: String,
    /// Whether this key may sign something NEW right now.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub usable_for_new_signatures: Option<bool>,
    /// `Full`, `Historical` or `None` — whether evidence this key signed still
    /// verifies, and on what basis. **`Historical` is not a downgrade of
    /// `Full`**: it is the honest answer for a key that was valid when it
    /// signed and has since been retired.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub usable_for_verification: Option<String>,
}

/// A namespace two policies both claim.
#[derive(Deserialize, Serialize, Clone, Debug, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct NamespaceConflict {
    /// The contested namespace.
    pub namespace: String,
    /// Every policy claiming it.
    #[schemars(length(max = 16))]
    pub policies: Vec<String>,
}

/// `TrustPolicy.status`.
#[derive(Deserialize, Serialize, Clone, Debug, Default, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct TrustPolicyStatus {
    /// The `metadata.generation` these verdicts were computed from.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub observed_generation: Option<i64>,
    /// When the verdicts were last computed. Debounced to at least 300 s, so a
    /// heartbeat does not become a write loop (erratum E11(d)).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub evaluated_at: Option<Time>,
    /// Whether every key parsed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub loaded: Option<bool>,
    /// How many keys the spec carries, for the printer column.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub key_count: Option<i64>,
    /// The per-key verdicts. `evaluatedAt` beside them is what lets a consumer
    /// tell "not evaluated" from "evaluated and valid" — the thing
    /// `TrustRoster.status.expiredKeyIds` could not say.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(length(max = 64))]
    pub keys: Option<Vec<KeyVerdict>>,
    /// The namespaces this policy actually governs.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(length(max = 256))]
    pub bound_namespaces: Option<Vec<String>>,
    /// Namespaces claimed by more than one policy. Each of them resolves to
    /// NOTHING: every approval and verification there is refused with
    /// `TrustPolicyConflict` rather than silently taking one policy's answer.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(length(max = 64))]
    pub conflicts: Option<Vec<NamespaceConflict>>,
    /// The condition set: `Loaded`, `Bound`, `ExpiringSoon`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub conditions: Option<Vec<Condition>>,
}
