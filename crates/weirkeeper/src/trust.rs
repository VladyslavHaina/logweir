//! Which trust a namespace resolves to, and the synthesised `legacy-roster-v1`
//! (decision D3 §7.1 and §7.5).
//!
//! # Resolution, and why a contested namespace resolves to nothing
//!
//! `spec.namespaces` match → the `default: true` policy → the roster,
//! synthesised. A namespace claimed by two policies resolves to nothing: every
//! approval and verification there is refused with
//! [`REASON_TRUST_POLICY_CONFLICT`]. Picking one by any sort order would be a
//! trust decision made by a sort order; the safe reading of a disagreement
//! about authority is that there is none. The conflict is also reported on
//! both policies' `status.conflicts`. Two `default: true` policies are the same
//! fault for every namespace that falls to the default.
//!
//! # `legacy-roster-v1` is synthesised, never applied
//!
//! It exists only in memory. A written `TrustPolicy` for it would be a policy
//! no administrator reviewed, whose CEL rules would forbid editing the roster's
//! behaviour back out. The upgrade path is explicit (`logweir trust
//! migrate-roster`, `kubectl apply`); until it is taken, this synthesis keeps a
//! roster-only cluster working unchanged.
//!
//! # "A partially loaded roster is not a roster", and where that rule stops
//!
//! The roster's two consumers treat a bad PEM differently, and §7.5's
//! "byte-for-byte" reproduces that: `controllers::approval::evaluate` check 2
//! refuses every approval when any `approverKeys` entry fails to parse
//! ([`ResolvedTrust::blocked_for`] carries that message byte for byte), while
//! `verification::verify_evidence` skips an unparseable `signingKeys` entry. So
//! the block is per usage. A real [`TrustPolicy`] blocks neither: an
//! unparseable key there is one key with `effectiveState: Unparseable`. It can
//! verify nothing, so leaving it out widens no trust, while refusing the whole
//! policy would let one bad paste stop every restore in every bound namespace.
//!
//! # A compromise revocation is a fact about the key, not about a policy
//!
//! A compromise says the private half may be in someone else's hands, whichever
//! object a namespace resolves through. So [`resolve_in`] applies every
//! `KeyCompromise` revocation any `TrustPolicy` in the cluster records,
//! including one being deleted, to the resolved key with the same id, whatever
//! the source ([`compromise_records`],
//! [`ResolvedKey::compromise_inherited_from`]). Otherwise deleting the
//! recording policy, or re-binding the namespace, would drop it to a source
//! that lists the key as ordinary and re-verify everything it signed. The
//! overlay only ever moves a key to `Revoked`, which G3 forbids undoing, and a
//! key the source does not list stays `UntrustedSigner`.
//!
//! The overlay can only honour a record that still exists, which is what the
//! `TrustPolicy` reconciler's finalizer is for (`controllers::trust_policy`,
//! [`deletion_guard`]): a policy recording a compromise is not released until
//! another live policy records it, or nothing lists the key any more.
//!
//! # No clock here
//!
//! Every function takes the instants it needs. The parsing is here because it
//! needs `logweir_verify` (which `logweir-core` must not link); the arithmetic
//! stays in [`logweir_core::trust`].

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

/// The `notBefore` every synthesised legacy key carries: the Unix epoch, the
/// honest translation of "always trusted" for a roster with no `notBefore`. A
/// synthesised "now" would retroactively invalidate every archive the roster
/// signed.
pub const LEGACY_NOT_BEFORE_RFC3339: &str = "1970-01-01T00:00:00Z";

/// The `notAfter` a synthesised legacy key gets when the roster entry has
/// none.
///
/// `TrustRoster.spec.*.notAfter` is `Option` and an absent one means "does not
/// expire" in every consumer today. A far-future instant is how that is said
/// in a shape whose `notAfter` is required.
pub const LEGACY_NOT_AFTER_RFC3339: &str = "9999-12-31T23:59:59Z";

/// The refusal a contested namespace produces (D3 §7.1): a condition `reason`
/// and a refusal string at once, so the same word appears on the
/// `TrustPolicy`'s `Bound` condition and on the refused object.
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
    /// DER SPKI, carrying what it actually hashes to. Separate from
    /// [`Self::parsed`] because the refusals differ: the approval path reports
    /// a bad PEM as `SignatureInvalid` and a disagreeing id as
    /// `KeyIdNotInRoster`.
    pub declared_id_matches: Result<(), String>,
    /// The `TrustPolicy` objects whose `KeyCompromise` revocation of this key
    /// id was applied although the resolved source does not record one.
    /// Sorted; empty when the state is the source's own. A refusal names the
    /// object that actually records the compromise, not a roster that cannot
    /// express a revocation.
    pub compromise_inherited_from: Vec<String>,
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
    /// The message, byte for byte what `controllers::approval::evaluate` check
    /// 2 writes.
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

    /// [`logweir_core::trust::decide`], resolved against this namespace's
    /// policy by the stored `matchedKeyId`. A key id not in the policy is
    /// `UntrustedSigner`; revocation by deletion cannot happen, because the
    /// CRD's G1 rule makes `spec.keys` append-only.
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
    /// either (`RosterNotFound`).
    Unconfigured,
}

/// Resolve one namespace against every `TrustPolicy` in the cluster and the
/// roster (`TrustRoster/default`'s spec, or `None`): pure, so the whole table
/// is testable without a client.
///
/// The source is chosen by the three steps; its keys then pass through
/// [`apply_compromises`] with the records of every policy in `policies`,
/// including one carrying a `deletionTimestamp` (see the module header).
#[must_use]
pub fn resolve_in(
    namespace: &str,
    policies: &[TrustPolicy],
    roster: Option<&TrustRosterSpec>,
) -> Resolution {
    match resolve_source(namespace, policies, roster) {
        Resolution::Trust(mut resolved) => {
            apply_compromises(&mut resolved, &compromise_records(policies));
            Resolution::Trust(resolved)
        }
        other => other,
    }
}

/// [`resolve_in`]'s three steps, before any compromise record is applied.
fn resolve_source(
    namespace: &str,
    policies: &[TrustPolicy],
    roster: Option<&TrustRosterSpec>,
) -> Resolution {
    // 1. An explicit `spec.namespaces` match.
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

    // 2. The default policy. Two defaults conflict for every namespace that
    //    falls to them, but not for one an explicit match already answered.
    let defaults: Vec<&TrustPolicy> = policies.iter().filter(|p| p.spec.default).collect();
    if defaults.len() > 1 {
        return conflict(namespace, &defaults);
    }
    if let Some(policy) = defaults.first() {
        return Resolution::Trust(Box::new(from_policy(policy)));
    }

    // 3. The synthesised legacy roster.
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

/// Every namespace two or more policies claim, and who claims it: the status
/// half of [`resolve_in`]'s rule, computed once so every policy's
/// `status.conflicts` names the same set. A default policy claims only
/// namespaces nobody named, so it collides with no explicit claim; two defaults
/// are reported under [`DEFAULT_SENTINEL`], since they contest every namespace,
/// present and future.
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

/// One `TrustPolicy` object as resolved trust, with every `KeyCompromise`
/// revocation `policies` records applied to it — the view its own
/// `status.keys[]` reports (the keys page's `EVALUATION` column).
///
/// A key this policy declares `Active` that another policy revoked for
/// compromise is evaluated `Revoked` HERE TOO, because that is what every
/// verification and approval in this policy's namespaces now decides; a green
/// `Active` beside it would be the one surface still disagreeing.
#[must_use]
pub fn from_policy_in(policy: &TrustPolicy, policies: &[TrustPolicy]) -> ResolvedTrust {
    let mut resolved = from_policy(policy);
    apply_compromises(&mut resolved, &compromise_records(policies));
    resolved
}

/// Whether one `spec.keys[]` entry records a `KeyCompromise` revocation.
///
/// BOTH FIELDS. The reason is read only for a key whose state is `Revoked`
/// ([`logweir_core::trust::decide`]), so a `revocationReason: KeyCompromise`
/// beside `state: Active` records nothing; and an absent reason is
/// `Unspecified`, which D3 §7.4 treats as a supersession.
#[must_use]
pub fn records_compromise(key: &SpecKey) -> bool {
    key.state == KeyState::Revoked && key.revocation_reason == Some(RevocationReason::KeyCompromise)
}

/// One key id's `KeyCompromise` revocation, as every `TrustPolicy` in the
/// cluster records it together.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CompromiseRecord {
    /// The key id.
    pub key_id: String,
    /// The instant stored evidence is compared against: the earliest
    /// `revocationEffectiveFrom` (else `revokedAt`) any record declares, or
    /// `None` when some record declares neither, which `decide` fails closed
    /// on. The earliest, because it separates fewer observations from the
    /// compromise and so cannot be wrong in the dangerous direction; either way
    /// the verdict is `Untrusted`, only the basis can differ.
    pub effective_from: Option<DateTime<Utc>>,
    /// The earliest `revokedAt`, for display. `None` whenever `effective_from`
    /// is, so `decide`'s `revocationEffectiveFrom.or(revokedAt)` cannot find
    /// an instant the records did not agree on.
    pub revoked_at: Option<DateTime<Utc>>,
    /// Every policy recording it, sorted and de-duplicated.
    pub recorded_by: Vec<String>,
}

/// Every `KeyCompromise` revocation `policies` record, by key id.
///
/// EVERY OBJECT, including one carrying a `deletionTimestamp`: a policy the
/// finalizer is holding is still a record (it exists precisely because the
/// record has not been carried anywhere else yet), and ignoring it while it
/// waits would reopen the window the finalizer closes.
#[must_use]
pub fn compromise_records(policies: &[TrustPolicy]) -> BTreeMap<String, CompromiseRecord> {
    // (every record's bound, every record's revokedAt, every recorder)
    type Seen = (
        Vec<Option<DateTime<Utc>>>,
        Vec<DateTime<Utc>>,
        BTreeSet<String>,
    );
    let mut seen: BTreeMap<String, Seen> = BTreeMap::new();
    for policy in policies {
        let name = policy.name_any();
        for key in policy.spec.keys.iter().filter(|k| records_compromise(k)) {
            let entry = seen.entry(key.key_id.clone()).or_default();
            entry
                .0
                .push(key.revocation_effective_from.or(key.revoked_at));
            entry.1.extend(key.revoked_at);
            entry.2.insert(name.clone());
        }
    }
    seen.into_iter()
        .map(|(key_id, (bounds, revoked, by))| {
            let effective_from = earliest_bound(bounds);
            let revoked_at = effective_from.and_then(|_| revoked.into_iter().min());
            (
                key_id.clone(),
                CompromiseRecord {
                    key_id,
                    effective_from,
                    revoked_at,
                    recorded_by: by.into_iter().collect(),
                },
            )
        })
        .collect()
}

/// The earliest of `bounds`, where an absent bound is earlier than any
/// instant ("compromised from the start").
fn earliest_bound(
    bounds: impl IntoIterator<Item = Option<DateTime<Utc>>>,
) -> Option<DateTime<Utc>> {
    let mut out: Option<DateTime<Utc>> = None;
    for bound in bounds {
        let at = bound?;
        out = Some(out.map_or(at, |o| o.min(at)));
    }
    out
}

/// Apply `records` to every key of `resolved` with the same id: `Revoked`,
/// `KeyCompromise`, the records' instant — see [`resolve_in`].
///
/// # A key its own source already records as compromised is left BYTE-FOR-BYTE
/// alone unless another policy also records it
///
/// So a cluster with one policy and one revocation reads exactly as it did
/// before this overlay existed; only a SECOND record of the same compromise
/// can move the instant, and only earlier.
pub fn apply_compromises(
    resolved: &mut ResolvedTrust,
    records: &BTreeMap<String, CompromiseRecord>,
) {
    let own = match &resolved.source {
        TrustSource::Policy { name, .. } => Some(name.clone()),
        TrustSource::LegacyRoster => None,
    };
    for key in &mut resolved.keys {
        let Some(record) = records.get(&key.trust.key_id) else {
            continue;
        };
        let already = key.trust.state == logweir_core::trust::KeyState::Revoked
            && key.trust.reason() == logweir_core::trust::RevocationReason::KeyCompromise;
        let others: Vec<String> = record
            .recorded_by
            .iter()
            .filter(|n| Some(*n) != own.as_ref())
            .cloned()
            .collect();
        if already && others.is_empty() {
            continue;
        }
        let own_bound =
            already.then(|| key.trust.revocation_effective_from.or(key.trust.revoked_at));
        let effective_from = earliest_bound(own_bound.into_iter().chain([record.effective_from]));
        let revoked_at = effective_from.and_then(|_| {
            already
                .then_some(key.trust.revoked_at)
                .flatten()
                .into_iter()
                .chain(record.revoked_at)
                .min()
        });
        key.trust.state = logweir_core::trust::KeyState::Revoked;
        key.trust.revocation_reason = Some(logweir_core::trust::RevocationReason::KeyCompromise);
        key.trust.revocation_effective_from = effective_from;
        key.trust.revoked_at = revoked_at;
        if !already {
            key.compromise_inherited_from = if others.is_empty() {
                record.recorded_by.clone()
            } else {
                others
            };
        }
    }
}

/// The trust source name a still-listed key is reported under when the roster
/// lists it.
pub const ROSTER_SOURCE: &str = "TrustRoster/default";

/// One `KeyCompromise` revocation a policy records, and whether the cluster
/// would still know about it if the policy went away.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HeldRevocation {
    /// The revoked key's id.
    pub key_id: String,
    /// Every OTHER `TrustPolicy` without a `deletionTimestamp` that records
    /// the same key revoked for `KeyCompromise`, sorted. One is enough: the
    /// resolution overlay applies it to every namespace.
    pub carried_by: Vec<String>,
    /// Every other trust source that lists the key as anything but revoked
    /// for compromise — [`ROSTER_SOURCE`], or `TrustPolicy/<name>` (with or
    /// without a `deletionTimestamp`: a policy being held is still read) —
    /// sorted. These are what would trust the key again if the record went.
    pub still_listed_by: Vec<String>,
}

impl HeldRevocation {
    /// Whether the record may go: another live policy carries it, or nothing
    /// in the cluster lists the key as trusted any more (which an older
    /// controller, reading only the roster, also honours).
    #[must_use]
    pub fn may_be_dropped(&self) -> bool {
        !self.carried_by.is_empty() || self.still_listed_by.is_empty()
    }
}

/// What deleting one `TrustPolicy` would lose.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct DeletionGuard {
    /// One entry per `KeyCompromise` revocation the policy records, in spec
    /// order. EMPTY for a policy recording none, which the reconciler never
    /// holds.
    pub held: Vec<HeldRevocation>,
}

impl DeletionGuard {
    /// Whether the policy records any compromise at all — the condition under
    /// which the reconciler places its finalizer.
    #[must_use]
    pub fn guards_anything(&self) -> bool {
        !self.held.is_empty()
    }

    /// Whether every record may go — the condition under which the
    /// reconciler releases a policy that is being deleted.
    #[must_use]
    pub fn releasable(&self) -> bool {
        self.held.iter().all(HeldRevocation::may_be_dropped)
    }

    /// The records that keep the policy.
    pub fn blocking(&self) -> impl Iterator<Item = &HeldRevocation> {
        self.held.iter().filter(|h| !h.may_be_dropped())
    }
}

/// What deleting `policy` would lose, given every policy in the cluster and
/// `TrustRoster/default`'s spec — **pure**.
///
/// # Why a LIVE carrier, and why the still-listed side counts every object
///
/// `carried_by` skips any policy with a `deletionTimestamp`. Two policies
/// recording the same compromise, deleted together, would otherwise each
/// release on the strength of the other and the record would vanish with
/// both. `still_listed_by` counts a held policy too, because a policy that is
/// waiting is still resolved through and still trusts what it lists.
///
/// `policies` must be a CONSISTENT read (a `list` from the API server, not a
/// watch cache): two passes that each saw the other policy without its
/// `deletionTimestamp` could otherwise both release. `reconcile_policy`'s own
/// `list` is that read.
#[must_use]
pub fn deletion_guard(
    policy: &TrustPolicy,
    policies: &[TrustPolicy],
    roster: Option<&TrustRosterSpec>,
) -> DeletionGuard {
    let me = policy.name_any();
    let others: Vec<&TrustPolicy> = policies.iter().filter(|p| p.name_any() != me).collect();
    let held = policy
        .spec
        .keys
        .iter()
        .filter(|k| records_compromise(k))
        .map(|revoked| {
            let id = revoked.key_id.as_str();
            let mut carried_by: Vec<String> = others
                .iter()
                .filter(|p| p.metadata.deletion_timestamp.is_none())
                .filter(|p| {
                    p.spec
                        .keys
                        .iter()
                        .any(|k| k.key_id == id && records_compromise(k))
                })
                .map(|p| p.name_any())
                .collect();
            carried_by.sort();
            let mut still_listed_by: Vec<String> = others
                .iter()
                .filter(|p| {
                    p.spec
                        .keys
                        .iter()
                        .any(|k| k.key_id == id && !records_compromise(k))
                })
                .map(|p| format!("TrustPolicy/{}", p.name_any()))
                .collect();
            if roster.is_some_and(|r| {
                r.approver_keys
                    .iter()
                    .chain(r.signing_keys.iter())
                    .any(|e| e.key_id == id)
            }) {
                still_listed_by.push(ROSTER_SOURCE.to_string());
            }
            still_listed_by.sort();
            HeldRevocation {
                key_id: id.to_string(),
                carried_by,
                still_listed_by,
            }
        })
        .collect();
    DeletionGuard { held }
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
        compromise_inherited_from: Vec::new(),
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

/// Parse the PEM and check the declared id against the material it describes:
/// two answers, because the two refusals differ (see
/// [`ResolvedKey::declared_id_matches`]).
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

/// `TrustRoster/default` as the synthesised `legacy-roster-v1` (D3 §7.5):
/// `approverKeys` → [`KeyUsage::GovernedApproval`], `signingKeys` →
/// [`KeyUsage::EvidenceSigning`], `allowedClusterIds` →
/// `allowedTargetClusterIds`, `notAfter` verbatim, `state: Active`,
/// `principal.id: legacy:<keyId>`.
///
/// A key on both lists becomes one key with both usages: a key id is unique in a
/// policy, and the usage separation the CRD enforces cannot be applied
/// retroactively to a working roster (approval's `selfAttestedRisk` labels the
/// overlap instead, D3 §7.3).
///
/// No `ConsoleConfirmation` key is ever synthesised (D3 §7.3): a controller
/// reached by rollback reads only the roster, and a synthesised confirmation
/// key could let an ordinary confirmation pass for a governed approval.
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
                compromise_inherited_from: Vec::new(),
            }
        })
        .collect();

    // The roster's rule, reproduced exactly: `approval::evaluate` check 2 walks
    // `approverKeys` in order and returns on the first unparseable entry, with
    // this message byte for byte.
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

/// Every `TrustPolicy` in the cluster, plus `TrustRoster/default`, resolved for
/// one namespace: the only API-touching function here; every decision is
/// [`resolve_in`]'s. It lists rather than gets because the policy names its
/// namespaces, not the reverse, and "is this namespace claimed twice" needs the
/// whole set.
///
/// # Errors
///
/// A `kube::Error` from either read. A 404 on the roster is not an error: it is
/// [`Resolution::Unconfigured`] when no policy covered the namespace.
pub async fn resolve(client: &kube::Client, namespace: &str) -> Result<Resolution, kube::Error> {
    let policies: Api<TrustPolicy> = Api::all(client.clone());
    let list = policies.list(&kube::api::ListParams::default()).await?;
    resolve_with(&list.items, client, namespace).await
}

/// [`resolve`], with the policy set supplied by a caller that already holds one
/// in a reflector store: a controller that watches `TrustPolicy` (as the
/// re-trust trigger requires) has the set in memory, and the snapshot cannot be
/// staler than the event that woke the reconcile.
///
/// # Errors
///
/// A `kube::Error` from the roster read. A 404 there is not an error: it is
/// [`Resolution::Unconfigured`] when no policy covered the namespace.
pub async fn resolve_with(
    policies: &[TrustPolicy],
    client: &kube::Client,
    namespace: &str,
) -> Result<Resolution, kube::Error> {
    // The roster is the fallback, so it is read last and often not at all:
    // running `resolve_in` once with no roster answers "would a roster be
    // consulted?" with the same function, so a governed or contested namespace
    // is decided from the snapshot alone and costs no `GET trustrosters`.
    match resolve_in(namespace, policies, None) {
        Resolution::Unconfigured => {
            let roster = match crate::controllers::approval::load_roster(client).await? {
                crate::controllers::approval::RosterLoad::Found(roster) => Some(roster.spec),
                crate::controllers::approval::RosterLoad::NotFound => None,
            };
            Ok(resolve_in(namespace, policies, roster.as_ref()))
        }
        answered => Ok(answered),
    }
}

/// Whether a `TrustPolicy` event could change what `namespace` resolves to: the
/// trigger half of the re-trust pass (D3 §7.4).
///
/// Over-approximate, never under: enqueuing an object the policy does not
/// govern costs one re-derivation that writes nothing, while failing to enqueue
/// one leaves a revoked key green. So a `default: true` policy claims every
/// namespace here, though resolution would give a named namespace to its own
/// policy.
#[must_use]
pub fn may_govern(policy: &TrustPolicy, namespace: &str) -> bool {
    declared_scope(policy).covers(namespace)
}

/// The namespaces one `TrustPolicy` event could have changed the resolution of.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PolicyScope {
    /// Exactly these namespaces.
    Namespaces(BTreeSet<String>),
    /// Every namespace. A `default: true` policy is the fallback for every
    /// namespace no other policy names, which is unbounded and includes
    /// namespaces that do not exist yet — the same reason
    /// [`bound_namespaces`] does not enumerate it.
    Everything,
}

impl PolicyScope {
    /// Whether this scope covers `namespace`.
    #[must_use]
    pub fn covers(&self, namespace: &str) -> bool {
        match self {
            Self::Everything => true,
            Self::Namespaces(names) => names.contains(namespace),
        }
    }

    /// The union of two scopes. [`Self::Everything`] absorbs.
    #[must_use]
    pub fn union(self, other: Self) -> Self {
        match (self, other) {
            (Self::Everything, _) | (_, Self::Everything) => Self::Everything,
            (Self::Namespaces(mut a), Self::Namespaces(b)) => {
                a.extend(b);
                Self::Namespaces(a)
            }
        }
    }

    /// Whether this scope covers nothing at all.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        matches!(self, Self::Namespaces(names) if names.is_empty())
    }
}

/// The scope one policy object declares **right now**, with no history.
#[must_use]
pub fn declared_scope(policy: &TrustPolicy) -> PolicyScope {
    if policy.spec.default {
        return PolicyScope::Everything;
    }
    PolicyScope::Namespaces(
        policy
            .spec
            .namespaces
            .iter()
            .flatten()
            .cloned()
            .collect::<BTreeSet<String>>(),
    )
}

/// What each policy bound the last time it was seen, so an edit that narrows
/// still wakes what it stopped governing.
///
/// A `watches` mapper receives only the new object: removing `team-a` from
/// `spec.namespaces`, or clearing `spec.default`, would enqueue nothing in
/// `team-a`, and a terminal `Restore` (`Action::await_change()`) there would
/// keep a green badge although its new fallback may not carry the key. So the
/// mapper enqueues the union of the scope before and after the change. The
/// mapper is the only writer, so the remembered value cannot race the reflector
/// feeding resolution. A policy never seen before is pure widening (a starting
/// controller re-derives everything anyway). A `Delete` event carries the last
/// known object; the remembered entry stays, so a delete and re-create under
/// one name still unions correctly.
///
/// A compromise record reaches every namespace ([`resolve_in`]), so the
/// recording policy's declared scope is not the set its event can change. The
/// memory also keeps each policy's records ([`compromise_signature`]), and the
/// event widens to [`PolicyScope::Everything`] when:
///
/// * the records changed (a compromise added, escalated from a supersession, or
///   edited away by hand);
/// * it is the policy's first sighting and it carries any;
/// * it carries any and is being deleted: the release of a held record, a
///   finalizer removed by hand, and the final `Delete` event all change what
///   other namespaces resolve to.
///
/// Status-only events (the 300 s heartbeat) change none of these, so the
/// fan-out is paid once per record change.
#[derive(Debug, Default)]
pub struct PolicyScopeMemory {
    seen: std::sync::Mutex<BTreeMap<String, (PolicyScope, CompromiseSignature)>>,
}

/// A policy's `KeyCompromise` records as the trigger compares them: each key
/// id with the instant it records (`revocationEffectiveFrom`, else
/// `revokedAt`). EMPTY for a policy recording none.
pub type CompromiseSignature = BTreeMap<String, Option<DateTime<Utc>>>;

/// [`CompromiseSignature`] of one policy.
#[must_use]
pub fn compromise_signature(policy: &TrustPolicy) -> CompromiseSignature {
    policy
        .spec
        .keys
        .iter()
        .filter(|k| records_compromise(k))
        .map(|k| {
            (
                k.key_id.clone(),
                k.revocation_effective_from.or(k.revoked_at),
            )
        })
        .collect()
}

impl PolicyScopeMemory {
    /// Record `policy`'s current scope and return the union with the previous
    /// one — every namespace this event could have changed — or
    /// [`PolicyScope::Everything`] when the event touches a compromise record
    /// (see the type's header).
    #[must_use]
    pub fn observe(&self, policy: &TrustPolicy) -> PolicyScope {
        let now = declared_scope(policy);
        let records = compromise_signature(policy);
        let name = policy.name_any();
        let mut seen = match self.seen.lock() {
            Ok(guard) => guard,
            // A poisoned lock must not narrow the scope: the safe answer is
            // "everything could have changed".
            Err(_) => return PolicyScope::Everything,
        };
        let deleting = policy.metadata.deletion_timestamp.is_some();
        let before = seen.insert(name, (now.clone(), records.clone()));
        let widen = match &before {
            Some((_, was)) => was != &records || (deleting && !records.is_empty()),
            None => !records.is_empty(),
        };
        if widen {
            return PolicyScope::Everything;
        }
        match before {
            Some((before, _)) => now.union(before),
            None => now,
        }
    }
}

/// The one `TrustPolicy` reflector a reconciler's watches share, and the flag
/// that says it has synced. One per reconciler, not per watched namespace:
/// `TrustPolicy` is cluster-scoped, so per-namespace copies would be identical
/// cluster-wide watches.
#[derive(Clone)]
pub struct SharedPolicies {
    /// The reflector's store.
    pub store: kube::runtime::reflector::Store<crate::crds::trust_policy::TrustPolicy>,
    /// Set once the store has synced. Until then the re-trust pass is skipped
    /// and everything else runs exactly as it would without a policy.
    pub synced: std::sync::Arc<std::sync::atomic::AtomicBool>,
}

/// Build the store and spawn its reflector (backed off) and its sync probe.
/// Call it from inside the runtime, once per reconciler.
#[must_use]
pub fn spawn_policy_reflector(client: &kube::Client) -> SharedPolicies {
    use futures::StreamExt as _;
    use kube::runtime::{watcher, WatchStreamExt as _};
    let (store, writer) =
        kube::runtime::reflector::store::<crate::crds::trust_policy::TrustPolicy>();
    let policies: Api<TrustPolicy> = Api::all(client.clone());
    let synced = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    // Not `wait_until_ready().await` before starting: a cluster without the
    // `trustpolicies` CRD never syncs, and the reconciler would never run.
    let probe = store.clone();
    let flag = std::sync::Arc::clone(&synced);
    tokio::spawn(async move {
        if probe.wait_until_ready().await.is_ok() {
            flag.store(true, std::sync::atomic::Ordering::Relaxed);
        }
    });
    tokio::spawn(
        // Backed off, like every trigger watch `Controller` runs; otherwise a
        // refused LIST (a scoped ServiceAccount without the cluster-scoped trust
        // grant) is retried in a tight loop.
        kube::runtime::reflector::reflector(
            writer,
            watcher(policies, watcher::Config::default()).default_backoff(),
        )
        .for_each(|_| std::future::ready(())),
    );
    SharedPolicies { store, synced }
}
