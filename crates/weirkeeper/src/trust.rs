//! Which trust a namespace resolves to, and the synthesised `legacy-roster-v1`
//! (PLAT-19.1, decision D3 §7.1 and §7.5).
//!
//! # The resolution order, and why a contested namespace resolves to NOTHING
//!
//! `spec.namespaces` match → the `default: true` policy → the roster,
//! synthesised. Three steps, in that order, and one rule that is not an
//! ordering at all: **a namespace claimed by two policies resolves to
//! nothing.** Every approval and every verification there is refused with
//! [`REASON_TRUST_POLICY_CONFLICT`].
//!
//! Picking one — the first listed, the newest, the alphabetically smallest —
//! would be a trust decision made by a sort order. Two administrators who each
//! believe they govern `team-a` disagree about which keys may authorise a
//! restore there, and the safe reading of a disagreement about authority is
//! that there is none. The conflict is reported on both policies'
//! `status.conflicts` so it is visible in `kubectl get trustpolicy` rather
//! than only in a refusal message.
//!
//! Two `default: true` policies are the same fault one level up: a namespace
//! no policy names explicitly falls to the default, and if there are two
//! defaults it falls to a conflict.
//!
//! # `legacy-roster-v1` is SYNTHESISED, never applied
//!
//! It exists only in this process's memory. Nothing writes a `TrustPolicy`
//! object for it, because a cluster that has one would then have a policy an
//! administrator never reviewed and whose CEL rules would immediately forbid
//! editing the roster's own behaviour back out. The upgrade path is explicit
//! and reviewable — `logweir trust migrate-roster`, `kubectl apply` — and
//! until it is taken, this synthesis is what keeps a roster-only cluster
//! working unchanged.
//!
//! # "A partially loaded roster is not a roster", and where that rule STOPS
//!
//! Today's two consumers do not treat a bad PEM alike, and reproducing that
//! exactly is the whole content of §7.5's "byte-for-byte":
//!
//! * `controllers::approval::evaluate` check 2 refuses **every** approval when
//!   any `approverKeys` entry fails to parse, naming the first bad `keyId`.
//!   [`ResolvedTrust::blocked_for`] carries that refusal, with the message
//!   byte-for-byte.
//! * `verification::verify_evidence` step 4 SKIPS an unparseable
//!   `signingKeys` entry and keeps trying the rest.
//!
//! So the block is per USAGE, not per policy. A real [`TrustPolicy`] blocks
//! neither: an unparseable key there is one key with
//! `effectiveState: Unparseable`, the others keep working, and the reason the
//! roster's stricter rule is not carried over is that the roster has nowhere
//! to RECORD a per-key verdict — it has one `loaded` boolean — while the
//! policy reports each key by id. An unparseable key can verify nothing, so
//! leaving it out widens no trust; refusing the whole policy for it would make
//! one bad paste in a 64-key policy stop every restore in every bound
//! namespace.
//!
//! # No clock here either
//!
//! Every function in this module takes the instants it needs. The parsing is
//! here because it needs `logweir_verify` (which `logweir-core` must not
//! link); the arithmetic stays in [`logweir_core::trust`].

use std::collections::{BTreeMap, BTreeSet};

use chrono::{DateTime, TimeZone as _, Utc};
use kube::{Api, ResourceExt as _};
use logweir_core::trust::{
    decide, may_sign_new, EvidenceClaim, IndependentObservation, KeyUsage, TrustedKey, Verdict,
};
use logweir_verify::VerifyingKey;

use crate::crds::trust_policy::{
    KeyAlgorithm, KeyState, RevocationReason, TrustPolicy, TrustedKey as SpecKey,
};
use crate::crds::trust_roster::{KeyEntry, TrustRosterSpec};

/// The name the synthesised policy reports itself under (D3 §7.5).
///
/// It is not a `metadata.name` — no object carries it — but it IS what
/// `status.evidence.verification.trust.policy.name` renders, so an operator
/// reading a badge can tell "this verified against the roster" from "this
/// verified against org-default" without guessing.
pub const LEGACY_POLICY_NAME: &str = "legacy-roster-v1";

/// The `notBefore` every synthesised legacy key carries.
///
/// The roster has no `notBefore` field, so there is no value to carry
/// verbatim; the honest translation of "this key has always been trusted" is a
/// window that opens at the Unix epoch. A synthesised `notBefore` of "now"
/// would retroactively invalidate every archive the roster signed, which is
/// the exact failure §7.5 exists to prevent.
pub const LEGACY_NOT_BEFORE_RFC3339: &str = "1970-01-01T00:00:00Z";

/// The `notAfter` a synthesised legacy key gets when the roster entry has
/// none.
///
/// `TrustRoster.spec.*.notAfter` is `Option` and an absent one means "does not
/// expire" in every consumer today. A far-future instant is how that is said
/// in a shape whose `notAfter` is required.
pub const LEGACY_NOT_AFTER_RFC3339: &str = "9999-12-31T23:59:59Z";

/// The refusal a contested namespace produces (D3 §7.1).
///
/// A CONDITION `reason` AND A REFUSAL STRING AT ONCE, deliberately: the same
/// word appears on the `TrustPolicy`'s own `Bound` condition and on the
/// `Approval` or run that was refused, so an operator grepping one finds the
/// other.
pub const REASON_TRUST_POLICY_CONFLICT: &str = "TrustPolicyConflict";

/// One key as this cluster resolved it: the pure lifecycle facts, plus the
/// material needed to check a signature against it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ResolvedKey {
    /// The lifecycle half, which [`logweir_core::trust`] reasons over.
    pub trust: TrustedKey,
    /// The **public** key, SubjectPublicKeyInfo in PEM form.
    pub spki_pem: String,
    /// The algorithm the policy declares.
    pub algorithm: KeyAlgorithm,
    /// `Err` when `spki_pem` is not a P-256 or Ed25519 public key, carrying
    /// the parse error's `Display`. `effectiveState` is then `Unparseable`.
    pub parsed: Result<(), String>,
    /// `false` when the declared `keyId` is not the sha256 of this key's own
    /// DER SPKI, carrying what it actually hashes to.
    ///
    /// SEPARATE FROM [`Self::parsed`] BECAUSE THE REFUSALS DIFFER. Today's
    /// approval path reports a bad PEM as `SignatureInvalid` and a
    /// disagreeing id as `KeyIdNotInRoster` (checks 2 and 3), and an entry
    /// whose declared id is not the hash of its own material is not an entry a
    /// signature can be looked up in.
    pub declared_id_matches: Result<(), String>,
}

impl ResolvedKey {
    /// Whether this key can be used to check a signature at all.
    #[must_use]
    pub fn is_usable(&self) -> bool {
        self.parsed.is_ok() && self.declared_id_matches.is_ok()
    }
}

/// Where a namespace's trust came from.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TrustSource {
    /// A real `TrustPolicy` object.
    Policy {
        /// `metadata.name`.
        name: String,
        /// `metadata.uid`, so a badge names the object and not just a name a
        /// deleted-and-recreated policy could reuse.
        uid: Option<String>,
        /// `metadata.generation` the keys were read from.
        generation: Option<i64>,
    },
    /// The synthesised [`LEGACY_POLICY_NAME`].
    LegacyRoster,
}

impl TrustSource {
    /// The name this source reports on `trust.policy.name`.
    #[must_use]
    pub fn name(&self) -> &str {
        match self {
            Self::Policy { name, .. } => name,
            Self::LegacyRoster => LEGACY_POLICY_NAME,
        }
    }

    /// Whether this is the synthesised legacy policy.
    #[must_use]
    pub fn is_legacy(&self) -> bool {
        matches!(self, Self::LegacyRoster)
    }
}

/// A usage whose WHOLE key set is refused, and why.
///
/// The legacy roster's "a partially loaded roster is not a roster", carried as
/// data rather than re-derived by each consumer — see the module header for
/// why it is per usage.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BlockedUsage {
    /// Which usage is refused.
    pub usage: KeyUsage,
    /// The message, byte-for-byte what `controllers::approval::evaluate`
    /// check 2 writes today.
    pub message: String,
}

/// The trust one namespace resolved to.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ResolvedTrust {
    /// Which policy, or the synthesised legacy one.
    pub source: TrustSource,
    /// Every key, in the order the policy (or the roster's two lists) declares
    /// them. ORDER IS PART OF THE CONTRACT: `verify_evidence` tries keys in
    /// order and reports the LAST error, and a reordering would change which
    /// error an operator reads.
    pub keys: Vec<ResolvedKey>,
    /// The cluster ids a restore may target, from `allowedTargetClusterIds`
    /// (or the roster's `allowedClusterIds`).
    pub allowed_target_cluster_ids: Vec<String>,
    /// Usages whose whole key set is refused.
    pub blocked: Vec<BlockedUsage>,
}

impl ResolvedTrust {
    /// The key with this id, whether or not it is usable.
    #[must_use]
    pub fn key(&self, key_id: &str) -> Option<&ResolvedKey> {
        self.keys.iter().find(|k| k.trust.key_id == key_id)
    }

    /// Every USABLE key carrying `usage`, in declaration order — the list a
    /// verifier tries a signature against.
    pub fn keys_for(&self, usage: KeyUsage) -> impl Iterator<Item = &ResolvedKey> {
        self.keys
            .iter()
            .filter(move |k| k.is_usable() && k.trust.has_usage(usage))
    }

    /// The whole-set refusal for `usage`, if any.
    #[must_use]
    pub fn blocked_for(&self, usage: KeyUsage) -> Option<&str> {
        self.blocked
            .iter()
            .find(|b| b.usage == usage)
            .map(|b| b.message.as_str())
    }

    /// **The seam W10 wires in.** [`logweir_core::trust::decide`], resolved
    /// against this namespace's policy by the stored `matchedKeyId`.
    ///
    /// A key id that is not in the policy is `UntrustedSigner`, which is
    /// exactly what a revocation-by-deletion would look like — and cannot
    /// happen, because the CRD's G1 rule makes `spec.keys` append-only.
    #[must_use]
    pub fn decide_for(
        &self,
        key_id: &str,
        usage: KeyUsage,
        claim: &EvidenceClaim,
        observation: &IndependentObservation,
        now: DateTime<Utc>,
    ) -> Verdict {
        decide(
            self.key(key_id).map(|k| &k.trust),
            usage,
            claim,
            observation,
            now,
        )
    }

    /// **The other seam W10 wires in.**
    /// [`logweir_core::trust::may_sign_new`], resolved by key id.
    ///
    /// # Errors
    ///
    /// A [`logweir_core::trust::SigningRefusal`].
    pub fn may_sign_new_for(
        &self,
        key_id: &str,
        usage: KeyUsage,
        now: DateTime<Utc>,
    ) -> Result<(), logweir_core::trust::SigningRefusal> {
        may_sign_new(self.key(key_id).map(|k| &k.trust), usage, now)
    }
}

/// What a namespace resolved to.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Resolution {
    /// One policy (or the synthesised legacy one) governs it.
    Trust(Box<ResolvedTrust>),
    /// Two or more policies claim it. **Resolves to nothing.**
    Conflict {
        /// The contested namespace.
        namespace: String,
        /// Every policy claiming it, sorted, so the message is stable.
        policies: Vec<String>,
    },
    /// No policy names it, there is no default, and there is no `TrustRoster`
    /// either — today's `RosterNotFound`.
    Unconfigured,
}

/// Resolve one namespace against every `TrustPolicy` in the cluster and the
/// roster — **pure**, so the whole table is testable without a client.
///
/// `policies` is every `TrustPolicy` object; `roster` is
/// `TrustRoster/default`'s spec, or `None` when there is none.
#[must_use]
pub fn resolve_in(
    namespace: &str,
    policies: &[TrustPolicy],
    roster: Option<&TrustRosterSpec>,
) -> Resolution {
    // ---- 1. an explicit `spec.namespaces` match --------------------------
    let explicit: Vec<&TrustPolicy> = policies
        .iter()
        .filter(|p| {
            p.spec
                .namespaces
                .as_ref()
                .is_some_and(|ns| ns.iter().any(|n| n == namespace))
        })
        .collect();
    if explicit.len() > 1 {
        return conflict(namespace, &explicit);
    }
    if let Some(policy) = explicit.first() {
        return Resolution::Trust(Box::new(from_policy(policy)));
    }

    // ---- 2. the default policy -------------------------------------------
    //
    // TWO DEFAULTS ARE A CONFLICT FOR EVERY NAMESPACE THAT FALLS TO THEM, and
    // not for the ones an explicit match already answered: an administrator
    // who named their namespaces has already decided, and a second default
    // elsewhere in the cluster must not take that away.
    let defaults: Vec<&TrustPolicy> = policies.iter().filter(|p| p.spec.default).collect();
    if defaults.len() > 1 {
        return conflict(namespace, &defaults);
    }
    if let Some(policy) = defaults.first() {
        return Resolution::Trust(Box::new(from_policy(policy)));
    }

    // ---- 3. the synthesised legacy roster --------------------------------
    match roster {
        Some(spec) => Resolution::Trust(Box::new(synthesize_legacy(spec))),
        None => Resolution::Unconfigured,
    }
}

/// A [`Resolution::Conflict`] naming every policy, sorted.
fn conflict(namespace: &str, policies: &[&TrustPolicy]) -> Resolution {
    let mut names: Vec<String> = policies.iter().map(|p| p.name_any()).collect();
    names.sort();
    Resolution::Conflict {
        namespace: namespace.to_string(),
        policies: names,
    }
}

/// Every namespace two or more policies claim, and who claims it.
///
/// THE STATUS HALF OF THE SAME RULE [`resolve_in`] enforces, computed once for
/// the whole cluster so `status.conflicts` on each policy names the same set.
/// A default policy contributes nothing here: it claims the namespaces nobody
/// else named, which is not a claim that can collide with an explicit one.
/// Two DEFAULTS are reported under the sentinel namespace [`DEFAULT_SENTINEL`],
/// because the set of namespaces they contest is every namespace that does not
/// exist yet as well as every one that does.
#[must_use]
pub fn conflicts(policies: &[TrustPolicy]) -> BTreeMap<String, Vec<String>> {
    let mut claims: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for p in policies {
        for ns in p.spec.namespaces.iter().flatten() {
            claims.entry(ns.clone()).or_default().push(p.name_any());
        }
    }
    let defaults: Vec<String> = policies
        .iter()
        .filter(|p| p.spec.default)
        .map(kube::ResourceExt::name_any)
        .collect();
    if defaults.len() > 1 {
        claims.insert(DEFAULT_SENTINEL.to_string(), defaults);
    }
    claims.retain(|_, v| {
        v.sort();
        v.dedup();
        v.len() > 1
    });
    claims
}

/// The `status.conflicts[].namespace` written when two policies both set
/// `default: true`.
///
/// NOT A REAL NAMESPACE, and it cannot be one: `*` is not a DNS-1123 label, so
/// no namespace can ever be named this and no reader can mistake it for one.
pub const DEFAULT_SENTINEL: &str = "*";

/// The namespaces one policy actually governs, given the whole set.
///
/// `spec.namespaces` minus the contested ones. A `default: true` policy
/// governs every namespace no other policy names, which is unbounded and is
/// therefore NOT enumerated here — the controller reports that in its `Bound`
/// condition instead. Listing it would require `list` on `namespaces`, a verb
/// this controller does not have and should not get to fill in a status field.
#[must_use]
pub fn bound_namespaces(policy: &TrustPolicy, policies: &[TrustPolicy]) -> Vec<String> {
    let contested = conflicts(policies);
    let mut out: Vec<String> = policy
        .spec
        .namespaces
        .iter()
        .flatten()
        .filter(|ns| !contested.contains_key(*ns))
        .cloned()
        .collect();
    out.sort();
    out.dedup();
    out
}

/// One `TrustPolicy` object as resolved trust.
#[must_use]
pub fn from_policy(policy: &TrustPolicy) -> ResolvedTrust {
    ResolvedTrust {
        source: TrustSource::Policy {
            name: policy.name_any(),
            uid: policy.metadata.uid.clone(),
            generation: policy.metadata.generation,
        },
        keys: policy.spec.keys.iter().map(resolve_spec_key).collect(),
        allowed_target_cluster_ids: policy
            .spec
            .allowed_target_cluster_ids
            .clone()
            .unwrap_or_default(),
        // A REAL POLICY BLOCKS NO USAGE. See the module header: an unparseable
        // key here is one key with `effectiveState: Unparseable`, not a reason
        // to refuse the other 63.
        blocked: Vec::new(),
    }
}

/// One `spec.keys[]` entry, parsed.
fn resolve_spec_key(key: &SpecKey) -> ResolvedKey {
    let (parsed, declared_id_matches) = check_material(&key.key_id, &key.spki_pem);
    ResolvedKey {
        trust: TrustedKey {
            key_id: key.key_id.clone(),
            principal_id: key.principal.id.clone(),
            usages: key.usages.iter().copied().map(usage_of).collect(),
            not_before: key.not_before,
            not_after: key.not_after,
            state: match key.state {
                KeyState::Active => logweir_core::trust::KeyState::Active,
                KeyState::Retired => logweir_core::trust::KeyState::Retired,
                KeyState::Revoked => logweir_core::trust::KeyState::Revoked,
            },
            retired_at: key.retired_at,
            revoked_at: key.revoked_at,
            revocation_reason: key.revocation_reason.map(reason_of),
            revocation_effective_from: key.revocation_effective_from,
        },
        spki_pem: key.spki_pem.clone(),
        algorithm: key.algorithm,
        parsed,
        declared_id_matches,
    }
}

/// The CRD usage enum as the pure one. **No wildcard arm**: a fourth usage on
/// the CRD must fail to compile here rather than reach a verdict as something
/// this crate guessed.
#[must_use]
pub fn usage_of(usage: crate::crds::trust_policy::KeyUsage) -> KeyUsage {
    match usage {
        crate::crds::trust_policy::KeyUsage::EvidenceSigning => KeyUsage::EvidenceSigning,
        crate::crds::trust_policy::KeyUsage::GovernedApproval => KeyUsage::GovernedApproval,
        crate::crds::trust_policy::KeyUsage::ConsoleConfirmation => KeyUsage::ConsoleConfirmation,
    }
}

/// The CRD revocation reason as the pure one. No wildcard arm, for the reason
/// [`usage_of`] records.
#[must_use]
pub fn reason_of(reason: RevocationReason) -> logweir_core::trust::RevocationReason {
    match reason {
        RevocationReason::KeyCompromise => logweir_core::trust::RevocationReason::KeyCompromise,
        RevocationReason::Superseded => logweir_core::trust::RevocationReason::Superseded,
        RevocationReason::Unspecified => logweir_core::trust::RevocationReason::Unspecified,
    }
}

/// Parse the PEM and check the declared id against the material it describes.
///
/// TWO ANSWERS, NOT ONE, because today's two refusals differ — see
/// [`ResolvedKey::declared_id_matches`].
fn check_material(declared: &str, pem: &str) -> (Result<(), String>, Result<(), String>) {
    match VerifyingKey::from_pem_str(pem) {
        Ok(key) => {
            let computed = key.key_id();
            if computed == declared {
                (Ok(()), Ok(()))
            } else {
                (Ok(()), Err(computed))
            }
        }
        // A PEM that does not parse has no key id to compare, so the second
        // answer is `Ok`: reporting BOTH faults for one bad paste would make
        // the message name a hash that was never computed.
        Err(e) => (Err(e.to_string()), Ok(())),
    }
}

/// `TrustRoster/default` as the synthesised `legacy-roster-v1` (D3 §7.5).
///
/// `approverKeys` → [`KeyUsage::GovernedApproval`], `signingKeys` →
/// [`KeyUsage::EvidenceSigning`], `allowedClusterIds` →
/// `allowedTargetClusterIds`, `notAfter` verbatim, `state: Active`,
/// `principal.id: legacy:<keyId>`.
///
/// # A key on BOTH lists becomes ONE key with BOTH usages
///
/// The roster's two lists may name the same `keyId` — that overlap is what
/// `controllers/approval.rs`'s `selfAttestedRisk` LABELS rather than refuses
/// (`design-operator.md:169-181`), and D3 §7.3 keeps the label for
/// legacy-roster namespaces precisely because the lists may overlap. A key id
/// is unique in a policy, so the synthesis merges them. The usage separation
/// the CRD enforces applies to policies an administrator writes; it cannot be
/// applied retroactively to a roster that predates it without refusing to
/// start against a configuration that has been working.
///
/// # No `ConsoleConfirmation` is ever synthesised
///
/// D3 §7.3, and it is a rollback rule rather than a taste: an old controller
/// reached by rollback reads only the roster, and a confirmation key
/// synthesised into the roster's world would let an ordinary confirmation be
/// mistaken for a governed approval. The roster has no field it could come
/// from anyway.
#[must_use]
pub fn synthesize_legacy(roster: &TrustRosterSpec) -> ResolvedTrust {
    let mut order: Vec<String> = Vec::new();
    let mut merged: BTreeMap<String, (KeyEntry, BTreeSet<KeyUsage>)> = BTreeMap::new();
    let lists = roster
        .approver_keys
        .iter()
        .map(|e| (KeyUsage::GovernedApproval, e))
        .chain(
            roster
                .signing_keys
                .iter()
                .map(|e| (KeyUsage::EvidenceSigning, e)),
        );
    for (usage, entry) in lists {
        match merged.get_mut(&entry.key_id) {
            Some((_, usages)) => {
                usages.insert(usage);
            }
            None => {
                order.push(entry.key_id.clone());
                let mut usages = BTreeSet::new();
                usages.insert(usage);
                merged.insert(entry.key_id.clone(), (entry.clone(), usages));
            }
        }
    }

    let keys: Vec<ResolvedKey> = order
        .iter()
        .filter_map(|id| merged.get(id))
        .map(|(entry, usages)| {
            let (parsed, declared_id_matches) = check_material(&entry.key_id, &entry.spki_pem);
            ResolvedKey {
                trust: TrustedKey {
                    key_id: entry.key_id.clone(),
                    principal_id: format!("legacy:{}", entry.key_id),
                    usages: usages.iter().copied().collect(),
                    not_before: legacy_not_before(),
                    not_after: entry.not_after.unwrap_or_else(legacy_not_after),
                    state: logweir_core::trust::KeyState::Active,
                    retired_at: None,
                    revoked_at: None,
                    revocation_reason: None,
                    revocation_effective_from: None,
                },
                spki_pem: entry.spki_pem.clone(),
                // THE ROSTER DECLARES NO ALGORITHM. `VerifyingKey::from_pem_str`
                // DISCOVERS it, so nothing is lost; the field is filled from
                // what the PEM turned out to be, and defaults to P-256 for a
                // PEM that did not parse at all (where it is never read).
                algorithm: algorithm_of(&entry.spki_pem),
                parsed,
                declared_id_matches,
            }
        })
        .collect();

    // THE ROSTER'S RULE, REPRODUCED EXACTLY. `approval::evaluate` check 2
    // walks `approverKeys` IN ORDER and returns on the FIRST unparseable
    // entry; the message below is that check's, byte-for-byte, so the refusal
    // an operator reads does not change on the day this path replaces it.
    let mut blocked = Vec::new();
    for entry in &roster.approver_keys {
        if let Err(e) = VerifyingKey::from_pem_str(&entry.spki_pem) {
            blocked.push(BlockedUsage {
                usage: KeyUsage::GovernedApproval,
                message: format!(
                    "TrustRoster '{}' entry keyId {} carries an spkiPem that is not a P-256 or \
                     Ed25519 public key ({e}); a partially loaded roster is not a roster, so no \
                     approval is accepted against it",
                    crate::ROSTER_NAME,
                    entry.key_id
                ),
            });
            break;
        }
    }

    ResolvedTrust {
        source: TrustSource::LegacyRoster,
        keys,
        allowed_target_cluster_ids: roster.allowed_cluster_ids.clone(),
        blocked,
    }
}

/// [`LEGACY_NOT_BEFORE_RFC3339`] as an instant.
///
/// A CONSTANT PARSED, not a `Utc::now()` and not a magic literal in three
/// places. `timestamp_opt(0, 0)` cannot fail for zero, and the `expect` names
/// the constant so a future edit that breaks it says which.
#[must_use]
pub fn legacy_not_before() -> DateTime<Utc> {
    DateTime::parse_from_rfc3339(LEGACY_NOT_BEFORE_RFC3339)
        .map(|t| t.with_timezone(&Utc))
        .unwrap_or_else(|_| Utc.timestamp_opt(0, 0).single().expect("the Unix epoch"))
}

/// [`LEGACY_NOT_AFTER_RFC3339`] as an instant.
#[must_use]
pub fn legacy_not_after() -> DateTime<Utc> {
    DateTime::parse_from_rfc3339(LEGACY_NOT_AFTER_RFC3339)
        .map(|t| t.with_timezone(&Utc))
        .unwrap_or_else(|_| DateTime::<Utc>::MAX_UTC)
}

/// What algorithm a PEM turned out to be.
///
/// The roster declares none and the policy does; this is the one-way bridge.
/// A PEM that does not parse is reported as P-256, which is never read: an
/// unparseable key is `Unparseable` and is excluded from every key list.
#[must_use]
pub fn algorithm_of(pem: &str) -> KeyAlgorithm {
    match VerifyingKey::from_pem_str(pem) {
        Ok(VerifyingKey::Ed25519(_)) => KeyAlgorithm::Ed25519,
        _ => KeyAlgorithm::P256,
    }
}

/// Whether the policy's declared `algorithm` agrees with what its `spkiPem`
/// actually is — the check D3 §7.1 gives the controller ("`algorithm`:
/// immutable; controller checks it against the PEM").
///
/// # Errors
///
/// The two algorithm names, when they disagree.
pub fn algorithm_agrees(key: &ResolvedKey) -> Result<(), String> {
    if key.parsed.is_err() {
        return Ok(());
    }
    let actual = algorithm_of(&key.spki_pem);
    if actual == key.algorithm {
        Ok(())
    } else {
        Err(format!(
            "declares algorithm {:?} and its spkiPem is {:?}",
            key.algorithm, actual
        ))
    }
}

// ---------------------------------------------------------------------------
// The cluster-reading half
// ---------------------------------------------------------------------------

/// Every `TrustPolicy` in the cluster, plus `TrustRoster/default`, resolved
/// for one namespace.
///
/// **This is the call that replaces `controllers::approval::load_roster` in
/// W10.** It is `async` and it is the ONLY thing in this module that touches
/// the API; everything it decides is [`resolve_in`]'s, which is pure.
///
/// # Why it lists policies rather than getting one by name
///
/// A namespace never names its own trust (D3 §7.1) — the POLICY names the
/// namespaces it governs — so there is no name to get. The conflict rule needs
/// the whole set for the same reason: "is this namespace claimed twice" is not
/// answerable from one object.
///
/// # Errors
///
/// A `kube::Error` from either read. A 404 on the roster is NOT an error: it
/// is [`Resolution::Unconfigured`] when no policy covered the namespace, which
/// is what today's `RosterNotFound` reports.
pub async fn resolve(client: &kube::Client, namespace: &str) -> Result<Resolution, kube::Error> {
    let policies: Api<TrustPolicy> = Api::all(client.clone());
    let list = policies.list(&kube::api::ListParams::default()).await?;
    let roster = match crate::controllers::approval::load_roster(client).await? {
        crate::controllers::approval::RosterLoad::Found(roster) => Some(roster.spec),
        crate::controllers::approval::RosterLoad::NotFound => None,
    };
    Ok(resolve_in(namespace, &list.items, roster.as_ref()))
}
