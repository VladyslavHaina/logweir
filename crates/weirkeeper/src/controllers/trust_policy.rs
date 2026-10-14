//! The `TrustPolicy` reconciler: parse the keys, resolve each against the
//! clock, say which namespaces this policy governs and which it contests, and
//! retire the roster it replaces (D3 §7.1).
//!
//! # Status is the point
//!
//! There is no side effect to reconcile. The status carries three claims an
//! operator cannot check by reading the YAML: every `spkiPem` parses and hashes
//! to the `keyId` beside it, where each key sits against the clock now, and
//! which namespaces this policy actually governs once conflicts are removed.
//!
//! # `evaluatedAt` is a debounced heartbeat
//!
//! D3 §7.7's keys view renders `unknown` when `status` is absent, when
//! `observedGeneration` lags, or when `evaluatedAt` is older than fifteen
//! minutes, so a healthy controller must keep `evaluatedAt` moving. A field
//! refreshed on every pass would spin the reconciler on its own status patch,
//! so `evaluatedAt` moves when the substance changed or [`HEARTBEAT`] has
//! elapsed, and otherwise the stored value is written back and
//! [`status_unchanged`] skips the patch: one write per policy per five
//! minutes, and fifteen minutes of tolerance is three missed heartbeats.
//!
//! # `Superseded` on the roster
//!
//! Once a matching policy exists, `TrustRoster/default` gets `Superseded=True`
//! (D3 §7.5). The roster's own reconciler watches rosters, and a policy being
//! created is not a roster event, so the controller that observes the cause
//! writes it. The roster is not deleted and its `spec` is not touched: rollback
//! reads it unchanged.
//!
//! # The one write that is not status: a finalizer on a compromise record
//!
//! A `KeyCompromise` revocation lives on the policy that records it, and
//! [`crate::trust::resolve_in`] applies it everywhere only while some object
//! still records it; deleting the only such policy would hand its namespaces
//! back to a roster that lists the key as ordinary. So a policy that records a
//! compromise carries [`COMPROMISE_FINALIZER`], and a deletion is released only
//! when [`crate::trust::deletion_guard`] says the record may go: another live
//! policy records it, or nothing in the cluster lists the key as trusted.
//! Until then the object stays, with its `deletionTimestamp`, and is resolved
//! through as before; an older controller after a rollback honours it too,
//! because a finalizer is data on the object. `CompromiseGuard` says which key
//! holds it and what would release it.
//!
//! The cost: Kubernetes RBAC cannot grant a write narrower than the object, so
//! this controller holds `patch` on `trustpolicies` beside
//! `trustpolicies/status`. The one call site is [`reconcile_finalizer`], whose
//! body is `metadata.{name, resourceVersion, finalizers}` and nothing else
//! (`tests/trust_revocation_durable.rs` pins it), and every spec edit the verb
//! could make is monotonic under G1–G9 and attributed in `managedFields`.
//!
//! # No private key material
//!
//! `spkiPem` is a public key. This reconciler reads no Secret
//! (`tests/linkage.rs`'s `the_controller_never_reads_a_secret`), and the crate
//! links the verifying half of the DSSE machinery, never the signing half
//! (Global Constraint 27).

use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Utc};
use futures::StreamExt as _;
use kube::api::{ListParams, Patch, PatchParams};
use kube::runtime::controller::Action;
use kube::runtime::{watcher, Controller};
use kube::{Api, ResourceExt as _};
use serde_json::json;
use tracing::{debug, info, warn};

use super::approval::{ReconcileError, ROSTER_NAME};
use super::Context;
use crate::conditions::{current_condition, merge_condition, status_unchanged};
use crate::crds::trust_policy::{
    KeyVerdict, NamespaceConflict, TrustPolicy, TrustPolicyStatus, TrustedKey,
};
use crate::crds::trust_roster::{TrustRoster, TrustRosterSpec};
use crate::crds::Condition;
use crate::trust::{
    algorithm_agrees, bound_namespaces, conflicts, deletion_guard, DeletionGuard, ResolvedKey,
    DEFAULT_SENTINEL, ROSTER_SOURCE,
};
use logweir_core::trust::{
    effective_state, usable_for_new_signatures, usable_for_verification, EffectiveState,
};

/// How often the reconciler re-evaluates, and the floor under an `evaluatedAt`
/// rewrite. Both, deliberately: see the module header.
pub const HEARTBEAT: Duration = Duration::from_secs(300);

/// How long before `notAfter` an `Active` key raises `ExpiringSoon`.
///
/// Thirty days, a planning horizon rather than an alarm: rotation
/// (`docs/keys.md`) includes waiting for in-flight Jobs, and nothing is blocked
/// by the condition (a key inside the window still signs), so being generous
/// costs a line in `kubectl get` while being tight costs an unplanned expiry.
pub const EXPIRING_SOON_DAYS: i64 = 30;

/// The floor under a boundary requeue, so a deadline already upon us cannot
/// become `Action::requeue(0)`, the one way this could spin. It costs at most
/// one second of overshoot, in the sub-second window before a boundary.
pub const MIN_REQUEUE: Duration = Duration::from_secs(1);

/// The requeue for a status a CLOCK can change: at `deadline` if that is
/// sooner than [`HEARTBEAT`], and at `HEARTBEAT` otherwise.
///
/// A verdict about a key depends on the clock as well as the object, and
/// nothing writes to the object when the clock passes a boundary, so a key past
/// its `notAfter` would read valid until the next heartbeat (measured: an
/// expired approver key kept `Verified=True` for 2 m 35 s). Expiry information
/// that is unevaluated or stale must read unknown, not valid. A shorter
/// heartbeat would trade the lag for write-free reconciles on every object and
/// still leave a window; waking at the boundary costs one extra reconcile per
/// key lifetime and closes it.
///
/// The wakeup lands on the boundary, not after it: the windows are half-open
/// (`may_sign_new` refuses at `now >= notAfter`), so a pass waking exactly then
/// computes the post-boundary verdict. Jitter self-corrects: a pass that wakes
/// early asks for `deadline - now` again, now milliseconds, rather than falling
/// back to a whole [`HEARTBEAT`]; padding would guarantee a stale window.
///
/// A deadline already past returns [`HEARTBEAT`] (the pass that computed it
/// applied the transition), and a sub-second one is floored at
/// [`MIN_REQUEUE`], so the result is always in `[MIN_REQUEUE, HEARTBEAT]`.
#[must_use]
pub fn requeue_before(deadline: Option<DateTime<Utc>>, now: DateTime<Utc>) -> Duration {
    let Some(deadline) = deadline else {
        return HEARTBEAT;
    };
    let Ok(until) = (deadline - now).to_std() else {
        // Already past: the verdict this pass wrote is the post-boundary one.
        return HEARTBEAT;
    };
    until.max(MIN_REQUEUE).min(HEARTBEAT)
}

/// The earliest future instant at which one of these keys changes state **by
/// the clock alone** — `notBefore`, `notAfter`, or the `ExpiringSoon` horizon.
///
/// All three are transitions this controller writes and no edit announces:
/// `NotYetValid -> Active`, `Active -> Expired`, and `NotExpiring ->
/// ExpiringSoon`. A key already past all three contributes nothing, so a
/// policy whose keys are all expired settles on [`HEARTBEAT`].
#[must_use]
pub fn next_clock_change(keys: &[ResolvedKey], now: DateTime<Utc>) -> Option<DateTime<Utc>> {
    keys.iter()
        .flat_map(|k| {
            [
                k.trust.not_before,
                k.trust.not_after,
                k.trust.not_after - chrono::Duration::days(EXPIRING_SOON_DAYS),
            ]
        })
        .filter(|at| *at > now)
        .min()
}

/// The condition type reporting that every key parsed.
pub const CONDITION_LOADED: &str = "Loaded";
/// The condition type reporting which namespaces this policy governs.
pub const CONDITION_BOUND: &str = "Bound";
/// The condition type reporting a key approaching its `notAfter`.
pub const CONDITION_EXPIRING_SOON: &str = "ExpiringSoon";

/// `Loaded=True`: every key parsed and agreed with its own id and algorithm.
pub const REASON_LOADED: &str = "Loaded";
/// `Loaded=False`: a `spkiPem` is not a P-256 or Ed25519 public key.
pub const REASON_UNPARSEABLE_KEY: &str = "UnparseableKey";
/// `Loaded=False`: a declared `keyId` is not the sha256 of its own material.
pub const REASON_KEY_ID_MISMATCH: &str = "KeyIdMismatch";
/// `Loaded=False`: a declared `algorithm` is not what the PEM turned out to be.
pub const REASON_ALGORITHM_MISMATCH: &str = "AlgorithmMismatch";

/// `Bound=True`: this policy governs the namespaces it names.
pub const REASON_BOUND: &str = "Bound";
/// `Bound=True`: this policy is the cluster default.
pub const REASON_DEFAULT_POLICY: &str = "DefaultPolicy";
/// `Bound=False`: this policy names no namespace and is not the default.
pub const REASON_NOT_BOUND: &str = "NotBound";
/// `Bound=False`: every namespace it names is contested.
pub const REASON_CONFLICT: &str = crate::trust::REASON_TRUST_POLICY_CONFLICT;

/// `ExpiringSoon=True`: an `Active` key is inside [`EXPIRING_SOON_DAYS`] of
/// its `notAfter`.
pub const REASON_EXPIRING_SOON: &str = "ExpiringSoon";
/// `ExpiringSoon=False`: no `Active` key is inside the window.
pub const REASON_NOT_EXPIRING: &str = "NotExpiring";

/// `Superseded` on the roster a policy replaces.
pub const CONDITION_SUPERSEDED: &str = "Superseded";
/// `Superseded=True`: a MATCHING policy exists (D3 §7.5, verbatim).
pub const REASON_SUPERSEDED_BY_TRUST_POLICY: &str = "SupersededByTrustPolicy";
/// `Superseded=False`: no policy displaces the roster, so it is still what
/// unbound namespaces resolve to.
pub const REASON_ROSTER_STILL_CONSULTED: &str = "RosterStillConsulted";

/// The finalizer this reconciler holds on a `TrustPolicy` that records a
/// `KeyCompromise` revocation — see the module header.
pub const COMPROMISE_FINALIZER: &str = "logweir.dev/compromise-revocation";

/// How soon a policy HELD for deletion is looked at again.
///
/// Shorter than [`HEARTBEAT`], because what releases it (a successor policy, the
/// roster re-created without the key) is an event on another object, which does
/// not wake this one. It writes nothing while nothing changed.
pub const HOLD_RECHECK: Duration = Duration::from_secs(15);

/// The condition type reporting what a compromise record holds.
pub const CONDITION_COMPROMISE_GUARD: &str = "CompromiseGuard";
/// `CompromiseGuard=True`: this policy records a `KeyCompromise` revocation
/// and the finalizer guards it.
pub const REASON_COMPROMISE_RECORDED: &str = "CompromiseRecorded";
/// `CompromiseGuard=True`: this policy lists a key another policy revoked for
/// compromise, and does not record the revocation itself.
pub const REASON_COMPROMISE_INHERITED: &str = "CompromiseInherited";
/// `CompromiseGuard=True`: this policy is being deleted and is held, because
/// deleting it would lose a compromise record.
pub const REASON_DELETION_BLOCKED: &str = "DeletionBlocked";
/// `CompromiseGuard=False`: nothing here records or inherits a compromise.
pub const REASON_NO_COMPROMISE_RECORDED: &str = "NoCompromiseRecorded";

/// What one policy reconcile decided.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PolicyVerdict {
    /// Whether every key parsed, hashed to its own id and matched its declared
    /// algorithm.
    pub loaded: bool,
    /// The per-key verdicts, in spec order.
    pub keys: Vec<KeyVerdict>,
    /// The namespaces this policy actually governs.
    pub bound: Vec<String>,
    /// The namespaces two or more policies claim.
    pub conflicts: Vec<NamespaceConflict>,
    /// The four conditions, ready to merge.
    pub conditions: Vec<Condition>,
    /// What deleting this policy would lose — the finalizer's input.
    pub guard: DeletionGuard,
}

/// What [`reconcile_finalizer`] does with [`COMPROMISE_FINALIZER`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FinalizerAction {
    /// Nothing to do.
    None,
    /// Place it: the policy records a compromise and is not being deleted.
    Add,
    /// Keep it: the policy is being deleted and a record would be lost.
    Hold,
    /// Remove it: the policy is being deleted and every record may go.
    Release,
}

/// What to do with the finalizer, given the object and its guard — **pure**.
///
/// The API server refuses a new finalizer on an object carrying a
/// `deletionTimestamp`, so a record revoked and deleted before this controller
/// saw it cannot be held after the fact: the one residual (see `docs/keys.md`,
/// *Replacing a TrustPolicy*).
#[must_use]
pub fn finalizer_action(policy: &TrustPolicy, guard: &DeletionGuard) -> FinalizerAction {
    let held = policy
        .metadata
        .finalizers
        .iter()
        .flatten()
        .any(|f| f == COMPROMISE_FINALIZER);
    let deleting = policy.metadata.deletion_timestamp.is_some();
    match (deleting, held) {
        (false, false) if guard.guards_anything() => FinalizerAction::Add,
        (true, true) if guard.releasable() => FinalizerAction::Release,
        (true, true) => FinalizerAction::Hold,
        _ => FinalizerAction::None,
    }
}

/// Parse and resolve one policy against the whole set and a clock: pure, like
/// [`super::trust_roster::evaluate`]. `policies` is every `TrustPolicy` in the
/// cluster, including this one: the conflict rule needs the whole set.
#[must_use]
pub fn evaluate(
    policy: &TrustPolicy,
    policies: &[TrustPolicy],
    now: DateTime<Utc>,
) -> PolicyVerdict {
    evaluate_in(policy, policies, None, now)
}

/// [`evaluate`], with `TrustRoster/default`'s spec in hand — what the
/// reconciler calls, because whether a compromise record may be dropped
/// depends on whether the roster still lists the key
/// ([`crate::trust::deletion_guard`]).
///
/// The per-key verdicts are the policy's own keys WITH every compromise
/// `policies` records applied ([`crate::trust::from_policy_in`]): a key this
/// policy declares `Active` that another policy revoked for compromise is
/// reported `Revoked` here, because that is what this controller decides for
/// it in this policy's namespaces.
#[must_use]
pub fn evaluate_in(
    policy: &TrustPolicy,
    policies: &[TrustPolicy],
    roster: Option<&TrustRosterSpec>,
    now: DateTime<Utc>,
) -> PolicyVerdict {
    let resolved = crate::trust::from_policy_in(policy, policies);
    let guard = deletion_guard(policy, policies, roster);

    // The per-key verdicts.
    let mut keys = Vec::with_capacity(resolved.keys.len());
    let mut load_failure: Option<(&'static str, String)> = None;
    for (key, spec) in resolved.keys.iter().zip(policy.spec.keys.iter()) {
        let state = key_state(key, now);
        if load_failure.is_none() {
            load_failure = first_fault(key, spec);
        }
        keys.push(KeyVerdict {
            key_id: key.trust.key_id.clone(),
            effective_state: state.as_str().to_string(),
            // A key that did not parse is neither: `false`/`None` rather than
            // omitted, so a consumer cannot read the absence as "not evaluated".
            usable_for_new_signatures: Some(
                key.is_usable() && usable_for_new_signatures(&key.trust, now),
            ),
            usable_for_verification: Some(if key.is_usable() {
                usable_for_verification(&key.trust, now)
                    .as_str()
                    .to_string()
            } else {
                logweir_core::trust::VerificationUse::None
                    .as_str()
                    .to_string()
            }),
        });
    }

    // Binding and conflicts.
    let all_conflicts = conflicts(policies);
    let name = policy.name_any();
    let mine: Vec<NamespaceConflict> = all_conflicts
        .iter()
        .filter(|(_, claimants)| claimants.contains(&name))
        .map(|(namespace, policies)| NamespaceConflict {
            namespace: namespace.clone(),
            policies: policies.clone(),
        })
        .collect();
    let bound = bound_namespaces(policy, policies);

    // The conditions.
    let (loaded, loaded_condition) = loaded_condition(&keys, load_failure.as_ref());
    let conditions = vec![
        condition(policy, CONDITION_LOADED, loaded_condition, now),
        condition(
            policy,
            CONDITION_BOUND,
            bound_condition(policy, &bound, &mine),
            now,
        ),
        condition(
            policy,
            CONDITION_EXPIRING_SOON,
            expiring_condition(&resolved.keys, now),
            now,
        ),
        condition(
            policy,
            CONDITION_COMPROMISE_GUARD,
            compromise_condition(policy, &resolved.keys, &guard),
            now,
        ),
    ];

    PolicyVerdict {
        loaded,
        keys,
        bound,
        conflicts: mine,
        conditions,
        guard,
    }
}

/// `CompromiseGuard` — what this policy's compromise records hold, and what
/// would release them.
///
/// One condition, four readings, most actionable first: a deletion being held,
/// a record being guarded, a revocation inherited from elsewhere, nothing. Every
/// message is built from recorded facts only, never the clock, so an unchanged
/// guard is never rewritten.
fn compromise_condition(
    policy: &TrustPolicy,
    keys: &[ResolvedKey],
    guard: &DeletionGuard,
) -> (bool, &'static str, String) {
    let inherited: Vec<String> = keys
        .iter()
        .filter(|k| !k.compromise_inherited_from.is_empty())
        .map(|k| {
            format!(
                "{} (recorded by TrustPolicy/{})",
                k.trust.key_id,
                k.compromise_inherited_from.join(", TrustPolicy/")
            )
        })
        .collect();
    // The same sentence whether it stands alone or follows another reading.
    let inherited_sentence = format!(
        "this policy lists {}, which another TrustPolicy revoked for KeyCompromise. This \
         controller treats the key as revoked in this policy's namespaces as well, and an older \
         controller would not: record the revocation here too (state: Revoked, \
         revocationReason: KeyCompromise).",
        inherited.join("; ")
    );
    let inherited_clause = if inherited.is_empty() {
        String::new()
    } else {
        format!(" Also, {inherited_sentence}")
    };
    let roster_listed: Vec<&str> = guard
        .held
        .iter()
        .filter(|h| h.still_listed_by.iter().any(|s| s == ROSTER_SOURCE))
        .map(|h| h.key_id.as_str())
        .collect();
    let roster_clause = if roster_listed.is_empty() {
        String::new()
    } else {
        format!(
            " {ROSTER_SOURCE} still lists {}: this controller applies the revocation to every \
             namespace that resolves to legacy-roster-v1, but an older controller reached by \
             rollback reads only the roster and would trust them: re-create the roster without \
             them before any rollback (docs/keys.md).",
            roster_listed.join(", ")
        )
    };

    let blocking: Vec<String> = guard
        .blocking()
        .map(|h| {
            format!(
                "{} (still listed by {})",
                h.key_id,
                h.still_listed_by.join(", ")
            )
        })
        .collect();
    if policy.metadata.deletion_timestamp.is_some() && !blocking.is_empty() {
        return (
            true,
            REASON_DELETION_BLOCKED,
            format!(
                "this policy is being deleted and is HELD by the finalizer {COMPROMISE_FINALIZER}: \
                 it is the only live record of the KeyCompromise revocation of {}, and deleting \
                 it would let those sources trust the key again. Any ONE of these releases it on \
                 the next pass: apply a TrustPolicy recording the same key(s) as state: Revoked, \
                 revocationReason: KeyCompromise; revoke the key(s) for KeyCompromise on each \
                 TrustPolicy named (a policy cannot drop a key, so it becomes a carrier), or \
                 delete it; re-create TrustRoster/default without the key(s) when it is named \
                 (docs/keys.md, Replacing a TrustPolicy). Removing the finalizer by hand (anyone \
                 holding patch on trustpolicies can) trusts the key(s) again wherever they are \
                 still listed.{inherited_clause}",
                blocking.join("; ")
            ),
        );
    }
    if guard.guards_anything() {
        let ids: Vec<&str> = guard.held.iter().map(|h| h.key_id.as_str()).collect();
        return (
            true,
            REASON_COMPROMISE_RECORDED,
            format!(
                "this policy records the KeyCompromise revocation of {}. This controller applies \
                 it to every namespace, whichever policy or roster the namespace resolves \
                 through, and holds the finalizer {COMPROMISE_FINALIZER}: a deletion waits until \
                 another TrustPolicy records the same revocation or nothing lists the key any \
                 more (docs/keys.md, Replacing a TrustPolicy).{roster_clause}{inherited_clause}",
                ids.join(", ")
            ),
        );
    }
    if !inherited.is_empty() {
        return (true, REASON_COMPROMISE_INHERITED, {
            let mut first = inherited_sentence;
            first.replace_range(..1, "T");
            first
        });
    }
    (
        false,
        REASON_NO_COMPROMISE_RECORDED,
        "this policy records no KeyCompromise revocation and lists no key another policy revoked \
         for compromise"
            .to_string(),
    )
}

/// The key's `effectiveState`, with `Unparseable` overriding the clock.
///
/// The override is the point: [`logweir_core::trust::effective_state`] never
/// sees key material and cannot return `Unparseable`, and a key whose PEM does
/// not parse must not read `Active` because its window is open.
fn key_state(key: &ResolvedKey, now: DateTime<Utc>) -> EffectiveState {
    if key.parsed.is_err() || key.declared_id_matches.is_err() {
        return EffectiveState::Unparseable;
    }
    effective_state(&key.trust, now)
}

/// The first thing wrong with one key, as a `(reason, message)` pair.
///
/// Ordered: material, then identity, then the declared algorithm. A PEM that
/// does not parse has no id or algorithm to check.
fn first_fault(key: &ResolvedKey, spec: &TrustedKey) -> Option<(&'static str, String)> {
    if let Err(e) = &key.parsed {
        return Some((
            REASON_UNPARSEABLE_KEY,
            format!(
                "spec.keys entry keyId {} carries an spkiPem that is not a P-256 or Ed25519 \
                 public key ({e}); it verifies nothing and is reported Unparseable",
                spec.key_id
            ),
        ));
    }
    if let Err(computed) = &key.declared_id_matches {
        return Some((
            REASON_KEY_ID_MISMATCH,
            format!(
                "spec.keys entry declares keyId {} but its own spkiPem hashes to {computed}; a \
                 key id that is not the hash of its own material is not an id a signature can be \
                 looked up by",
                spec.key_id
            ),
        ));
    }
    if let Err(detail) = algorithm_agrees(key) {
        return Some((
            REASON_ALGORITHM_MISMATCH,
            format!("spec.keys entry keyId {} {detail}", spec.key_id),
        ));
    }
    None
}

/// `Loaded`, and whether it is true.
fn loaded_condition(
    keys: &[KeyVerdict],
    fault: Option<&(&'static str, String)>,
) -> (bool, (bool, &'static str, String)) {
    match fault {
        Some((reason, message)) => (false, (false, reason, message.clone())),
        None => (
            true,
            (
                true,
                REASON_LOADED,
                format!(
                    "every spkiPem on this policy parsed, hashes to its own keyId and matches its \
                     declared algorithm: {} keys",
                    keys.len()
                ),
            ),
        ),
    }
}

/// `Bound`.
///
/// A `default: true` policy reports no namespace list: it governs every
/// namespace no other policy names, including ones that do not exist yet.
/// Listing them would need `list` on `namespaces`, a verb this controller must
/// not acquire for a status field, and would be wrong at the next namespace.
fn bound_condition(
    policy: &TrustPolicy,
    bound: &[String],
    conflicts: &[NamespaceConflict],
) -> (bool, &'static str, String) {
    if !conflicts.is_empty() && bound.is_empty() && !policy.spec.default {
        return (
            false,
            REASON_CONFLICT,
            format!(
                "every namespace this policy names is claimed by another policy as well, so each \
                 of them resolves to NOTHING and every approval and verification there is refused \
                 with {}: {}",
                crate::trust::REASON_TRUST_POLICY_CONFLICT,
                describe(conflicts)
            ),
        );
    }
    let contested = if conflicts.is_empty() {
        String::new()
    } else {
        format!(
            "; contested and therefore ungoverned: {}",
            describe(conflicts)
        )
    };
    if policy.spec.default {
        let shared = conflicts.iter().any(|c| c.namespace == DEFAULT_SENTINEL);
        if shared {
            return (
                false,
                REASON_CONFLICT,
                format!(
                    "more than one TrustPolicy sets spec.default: true, so no namespace falls to \
                     a default at all and each is refused with {}: {}",
                    crate::trust::REASON_TRUST_POLICY_CONFLICT,
                    describe(conflicts)
                ),
            );
        }
        return (
            true,
            REASON_DEFAULT_POLICY,
            format!(
                "this is the cluster default: it governs every namespace no other TrustPolicy \
                 names explicitly, which is why boundNamespaces lists only its own {} explicit \
                 namespace(s){contested}",
                bound.len()
            ),
        );
    }
    if bound.is_empty() {
        return (
            false,
            REASON_NOT_BOUND,
            format!(
                "this policy names no namespace and does not set spec.default, so it governs \
                 nothing{contested}"
            ),
        );
    }
    (
        true,
        REASON_BOUND,
        format!(
            "this policy governs {} namespace(s){contested}",
            bound.len()
        ),
    )
}

/// `ExpiringSoon`.
fn expiring_condition(keys: &[ResolvedKey], now: DateTime<Utc>) -> (bool, &'static str, String) {
    let horizon = now + chrono::Duration::days(EXPIRING_SOON_DAYS);
    let mut soon: Vec<&str> = keys
        .iter()
        .filter(|k| {
            k.is_usable() && matches!(effective_state(&k.trust, now), EffectiveState::Active)
        })
        .filter(|k| k.trust.not_after <= horizon)
        .map(|k| k.trust.key_id.as_str())
        .collect();
    soon.sort_unstable();
    if soon.is_empty() {
        return (
            false,
            REASON_NOT_EXPIRING,
            format!("no Active key reaches its notAfter within {EXPIRING_SOON_DAYS} days"),
        );
    }
    (
        true,
        REASON_EXPIRING_SOON,
        format!(
            "{} Active key(s) reach notAfter within {EXPIRING_SOON_DAYS} days: {}. Nothing is \
             blocked — add the successor key as Active, point the runner at it, then retire this \
             one (docs/keys.md, rotation)",
            soon.len(),
            soon.join(", ")
        ),
    )
}

/// `namespace (policy-a, policy-b)`, for a condition message.
fn describe(conflicts: &[NamespaceConflict]) -> String {
    conflicts
        .iter()
        .map(|c| format!("{} ({})", c.namespace, c.policies.join(", ")))
        .collect::<Vec<_>>()
        .join("; ")
}

/// One condition, with the `metav1.Condition` `lastTransitionTime` contract
/// applied against whatever the object already carries.
fn condition(
    policy: &TrustPolicy,
    r#type: &str,
    (status, reason, message): (bool, &'static str, String),
    now: DateTime<Utc>,
) -> Condition {
    merge_condition(
        current_condition(
            policy.status.as_ref().and_then(|s| s.conditions.as_ref()),
            r#type,
        ),
        Condition {
            r#type: r#type.to_string(),
            status: if status { "True" } else { "False" }.to_string(),
            observed_generation: policy.metadata.generation,
            last_transition_time: Some(now),
            reason: Some(reason.to_string()),
            message: Some(message),
        },
    )
}

/// The `/status` body one verdict produces, with the `evaluatedAt` debounce
/// applied.
///
/// `now` is written when the substance changed or when [`HEARTBEAT`] has
/// elapsed since the stored value; otherwise the stored value is written back
/// so [`status_unchanged`] can skip the patch. See the module header.
#[must_use]
pub fn status_for(
    policy: &TrustPolicy,
    verdict: &PolicyVerdict,
    now: DateTime<Utc>,
) -> TrustPolicyStatus {
    let stored = policy.status.as_ref();
    let substance_unchanged = stored.is_some_and(|s| {
        s.observed_generation == policy.metadata.generation
            && s.loaded == Some(verdict.loaded)
            && s.keys.as_deref() == Some(verdict.keys.as_slice())
            && s.bound_namespaces.as_deref() == Some(verdict.bound.as_slice())
            && s.conflicts.as_deref() == Some(verdict.conflicts.as_slice())
    });
    let stored_at = stored.and_then(|s| s.evaluated_at);
    let evaluated_at = match (substance_unchanged, stored_at) {
        (true, Some(at))
            if now.signed_duration_since(at)
                < chrono::Duration::from_std(HEARTBEAT)
                    .unwrap_or_else(|_| chrono::Duration::zero()) =>
        {
            at
        }
        _ => now,
    };

    TrustPolicyStatus {
        observed_generation: policy.metadata.generation,
        evaluated_at: Some(evaluated_at),
        loaded: Some(verdict.loaded),
        key_count: Some(verdict.keys.len() as i64),
        keys: Some(verdict.keys.clone()),
        bound_namespaces: Some(verdict.bound.clone()),
        conflicts: Some(verdict.conflicts.clone()),
        conditions: Some(verdict.conditions.clone()),
    }
}

/// Reconcile one `TrustPolicy` and patch **only** its `/status`, then report
/// `Superseded` on the roster it replaces.
///
/// # Errors
///
/// [`ReconcileError`] for anything that is not a verdict.
pub async fn reconcile_policy(
    policy: &TrustPolicy,
    client: &kube::Client,
) -> Result<PolicyVerdict, ReconcileError> {
    reconcile_policy_at(policy, client, Utc::now()).await
}

/// [`reconcile_policy`] at `now`, the caller's one clock read, so a key's
/// expiry and the condition reporting it cannot straddle two instants, and a
/// row can judge a policy at a fixed instant (FX-12, Global Constraint 1).
///
/// # Errors
///
/// [`ReconcileError`] for anything that is not a verdict.
pub async fn reconcile_policy_at(
    policy: &TrustPolicy,
    client: &kube::Client,
    now: chrono::DateTime<Utc>,
) -> Result<PolicyVerdict, ReconcileError> {
    let name = policy.name_any();

    // Cluster-scoped, the whole set (the conflict rule needs it), from a LIST
    // rather than the watch cache, because the finalizer's release must see
    // every other policy's `deletionTimestamp` as it is now.
    let api: Api<TrustPolicy> = Api::all(client.clone());
    let all = api
        .list(&ListParams::default())
        .await
        .map_err(ReconcileError::Api)?;
    // The roster is read before anything is decided, and a failed read fails
    // the pass: "could not read it" must never pass for "it lists nothing" when
    // deciding whether a compromise record may go.
    let rosters: Api<TrustRoster> = Api::all(client.clone());
    let roster = rosters
        .get_opt(ROSTER_NAME)
        .await
        .map_err(ReconcileError::Api)?;
    let verdict = evaluate_in(policy, &all.items, roster.as_ref().map(|r| &r.spec), now);
    let status = status_for(policy, &verdict, now);

    // The finalizer first: it is what makes a compromise record durable, so a
    // failing status patch or `supersede_roster` must not keep it from being
    // placed. Release stays fail-closed either way.
    let (action, patched) = reconcile_finalizer(policy, &verdict.guard, &api).await?;

    // Seam S7's precondition, so a pass computing from a stale cache copy gets
    // `409` rather than overwriting a newer verdict. When the finalizer patch
    // moved `resourceVersion`, the precondition is the object it returned.
    let latest = patched.as_ref().unwrap_or(&policy.metadata);
    let patch = with_precondition(latest, &name, json!({ "status": status }))?;
    // No write when nothing changed: with the debounce, a steady policy skips
    // the patch on four reconciles out of five.
    if status_unchanged(
        policy
            .status
            .as_ref()
            .and_then(|s| serde_json::to_value(s).ok())
            .as_ref(),
        &patch,
    ) {
        debug!(
            policy = %name,
            "the computed status equals the one on the object; no patch is sent"
        );
    } else {
        match api
            .patch_status(&name, &PatchParams::default(), &Patch::Merge(patch))
            .await
        {
            Ok(_) => {}
            // Releasing the last finalizer of a terminating object deletes it, so
            // its status is not there to write.
            Err(kube::Error::Api(e)) if e.code == 404 && action == FinalizerAction::Release => {}
            Err(e) => return Err(ReconcileError::Api(e)),
        }
    }

    supersede_roster(client, roster.as_ref(), &all.items, now).await?;

    if verdict.loaded {
        info!(
            policy = %name,
            keys = verdict.keys.len(),
            bound = verdict.bound.len(),
            conflicts = verdict.conflicts.len(),
            "trust policy loaded"
        );
    } else {
        warn!(policy = %name, "trust policy not loaded");
    }
    Ok(verdict)
}

/// The `Superseded` condition `TrustRoster/default` should carry, given every
/// `TrustPolicy` in the cluster (D3 §7.5).
///
/// "Once a matching policy exists" means the default: a policy governing only
/// `team-a` leaves every other namespace on `legacy-roster-v1`, synthesised
/// from this roster, and a roster that advertised itself replaced could be
/// abandoned or deleted, silently un-trusting every unbound namespace. Only a
/// `spec.default: true` policy displaces the roster everywhere, since
/// resolution reaches the roster only after the default. Two defaults are not a
/// match: they contest each other and govern nobody.
///
/// A condition with two states, not a flag: deleting every policy returns
/// `Superseded=False/RosterStillConsulted`, and [`super::trust_roster`] writes
/// it too from its own requeue, so clearing does not depend on a policy
/// existing.
#[must_use]
pub fn superseded_condition(
    roster: &TrustRoster,
    policies: &[TrustPolicy],
    now: DateTime<Utc>,
) -> Condition {
    let defaults: Vec<String> = policies
        .iter()
        .filter(|p| p.spec.default)
        .map(kube::ResourceExt::name_any)
        .collect();
    let (status, reason, message) = match defaults.as_slice() {
        [only] => (
            true,
            REASON_SUPERSEDED_BY_TRUST_POLICY,
            format!(
                "TrustPolicy/{only} is the cluster default, so every namespace resolves through                  a policy and this roster is no longer consulted. Nothing here is deleted or                  edited — a rollback reads it unchanged (docs/keys.md, rollback)"
            ),
        ),
        [] if policies.is_empty() => (
            false,
            REASON_ROSTER_STILL_CONSULTED,
            "no TrustPolicy exists, so every namespace resolves to legacy-roster-v1,              synthesised from this roster"
                .to_string(),
        ),
        [] => (
            false,
            REASON_ROSTER_STILL_CONSULTED,
            format!(
                "{} TrustPolicy object(s) exist and none sets spec.default: true, so every                  namespace they do not name explicitly still resolves to legacy-roster-v1,                  synthesised from THIS roster. Do not stop maintaining it",
                policies.len()
            ),
        ),
        many => (
            false,
            REASON_ROSTER_STILL_CONSULTED,
            format!(
                "{} TrustPolicy objects set spec.default: true ({}), so they contest every                  fallback namespace and each resolves to NOTHING rather than to either of them.                  A roster superseded by a conflict would be superseded by something that governs                  nobody",
                many.len(),
                many.join(", ")
            ),
        ),
    };
    merge_condition(
        current_condition(
            roster.status.as_ref().and_then(|s| s.conditions.as_ref()),
            CONDITION_SUPERSEDED,
        ),
        Condition {
            r#type: CONDITION_SUPERSEDED.to_string(),
            status: if status { "True" } else { "False" }.to_string(),
            observed_generation: roster.metadata.generation,
            last_transition_time: Some(now),
            reason: Some(reason.to_string()),
            message: Some(message),
        },
    )
}

/// Write [`superseded_condition`] onto `TrustRoster/default`.
///
/// The whole condition list is carried: an RFC 7386 merge patch replaces
/// arrays, so a patch carrying only `[Superseded]` would delete the roster
/// reconciler's `Loaded`, which would put it back and delete this one, forever.
///
/// # Errors
///
/// A `kube::Error` from the patch. A roster that was **not found** (`None`,
/// read once by [`reconcile_policy`]) is not an error: a cluster that never
/// had one has nothing to supersede.
async fn supersede_roster(
    client: &kube::Client,
    roster: Option<&TrustRoster>,
    policies: &[TrustPolicy],
    now: DateTime<Utc>,
) -> Result<(), ReconcileError> {
    let api: Api<TrustRoster> = Api::all(client.clone());
    let Some(roster) = roster else {
        return Ok(());
    };
    let existing = roster.status.as_ref().and_then(|s| s.conditions.as_ref());
    let superseded = superseded_condition(roster, policies, now);
    let mut conditions: Vec<Condition> = existing
        .map(|c| {
            c.iter()
                .filter(|c| c.r#type != CONDITION_SUPERSEDED)
                .cloned()
                .collect()
        })
        .unwrap_or_default();
    conditions.push(superseded);
    let patch = with_precondition(
        &roster.metadata,
        ROSTER_NAME,
        json!({ "status": { "conditions": conditions } }),
    )?;
    if status_unchanged(
        roster
            .status
            .as_ref()
            .and_then(|s| serde_json::to_value(s).ok())
            .as_ref(),
        &patch,
    ) {
        return Ok(());
    }
    api.patch_status(ROSTER_NAME, &PatchParams::default(), &Patch::Merge(patch))
        .await
        .map_err(ReconcileError::Api)?;
    Ok(())
}

/// Place, hold or release [`COMPROMISE_FINALIZER`] on `policy`, as
/// [`finalizer_action`] decides: the one write this reconciler makes to a
/// `TrustPolicy` that is not `/status`, with [`finalizer_patch`]'s body. It
/// runs first in [`reconcile_policy`] and returns the patched object's
/// metadata, which the status patch takes its precondition from.
///
/// # Errors
///
/// A `kube::Error` from the patch — a `409` on a stale copy included, which
/// requeues and is decided again from a fresh read.
pub async fn reconcile_finalizer(
    policy: &TrustPolicy,
    guard: &DeletionGuard,
    api: &Api<TrustPolicy>,
) -> Result<(FinalizerAction, Option<kube::api::ObjectMeta>), ReconcileError> {
    let name = policy.name_any();
    let action = finalizer_action(policy, guard);
    // One object, read once: the list carried forward and the precondition come
    // from the copy `finalizer_action` decided on.
    let current: Vec<String> = policy.metadata.finalizers.clone().unwrap_or_default();
    let next: Vec<String> = match action {
        FinalizerAction::None => return Ok((action, None)),
        FinalizerAction::Hold => {
            let blocking: Vec<&str> = guard.blocking().map(|h| h.key_id.as_str()).collect();
            info!(
                policy = %name,
                keys = %blocking.join(","),
                "trust policy deletion held: it is the only live record of a KeyCompromise \
                 revocation that another trust source still lists"
            );
            return Ok((action, None));
        }
        // Every other controller's finalizer is carried forward: an RFC 7386
        // merge patch replaces the list, so naming only this one would strip,
        // say, a GitOps tool's.
        FinalizerAction::Add => {
            let mut next = current.clone();
            if !next.iter().any(|f| f == COMPROMISE_FINALIZER) {
                next.push(COMPROMISE_FINALIZER.to_string());
            }
            next
        }
        FinalizerAction::Release => current
            .iter()
            .filter(|f| f.as_str() != COMPROMISE_FINALIZER)
            .cloned()
            .collect(),
    };
    let body = finalizer_patch(&policy.metadata, &name, &next)?;
    let patched = api
        .patch(&name, &PatchParams::default(), &Patch::Merge(body))
        .await
        .map_err(ReconcileError::Api)?;
    info!(
        policy = %name,
        action = ?action,
        "trust policy compromise-revocation finalizer updated"
    );
    Ok((action, Some(patched.metadata)))
}

/// The merge patch [`reconcile_finalizer`] sends: `metadata.name`,
/// `metadata.resourceVersion` (seam S7's precondition) and the WHOLE
/// `metadata.finalizers` list — an RFC 7386 merge patch replaces an array, so
/// the list carries every other controller's finalizer forward.
///
/// Nothing else, and a test pins it: a `spec` key here would be the controller
/// editing trust.
///
/// # Errors
///
/// [`ReconcileError::NoUid`] naming the object when it carries no
/// `resourceVersion`.
pub fn finalizer_patch(
    meta: &kube::api::ObjectMeta,
    name: &str,
    finalizers: &[String],
) -> Result<serde_json::Value, ReconcileError> {
    let mut patch = with_precondition(meta, name, json!({}))?;
    patch["metadata"]["finalizers"] = json!(finalizers);
    Ok(patch)
}

/// Add seam **S7**'s optimistic-concurrency precondition to a `/status` merge
/// patch.
///
/// Kubernetes applies `metadata.resourceVersion` in a patch body as an update
/// precondition and answers `409 Conflict` on a mismatch; the name beside it
/// ties the precondition to the object the request path names. It matters most
/// here: `TrustRoster/default`'s `status.conditions` has two read-modify-write
/// writers on an array a merge patch replaces, and without it a stale roster
/// reconcile would delete `Superseded`.
///
/// # Errors
///
/// [`ReconcileError::NoUid`] naming the object when it carries no
/// `resourceVersion` — unreachable for anything that came from the API server,
/// named rather than unwrapped.
pub fn with_precondition(
    meta: &kube::api::ObjectMeta,
    name: &str,
    mut patch: serde_json::Value,
) -> Result<serde_json::Value, ReconcileError> {
    let resource_version = meta
        .resource_version
        .clone()
        .ok_or_else(|| ReconcileError::NoUid(format!("{name} (no metadata.resourceVersion)")))?;
    patch
        .as_object_mut()
        .expect("a status patch is always a JSON object")
        .insert(
            "metadata".to_string(),
            json!({ "name": name, "resourceVersion": resource_version }),
        );
    Ok(patch)
}

/// The `kube::runtime` reconcile entry point.
async fn reconcile(policy: Arc<TrustPolicy>, ctx: Arc<Context>) -> Result<Action, ReconcileError> {
    // The pass's one clock read: the verdict and the requeue below are judged
    // at the same instant.
    let now = Utc::now();
    let verdict = reconcile_policy_at(&policy, &ctx.client, now).await?;
    // A held deletion is released by another object's event, which does not
    // wake this one, so it is looked at again on a short, bounded timer.
    if finalizer_action(&policy, &verdict.guard) == FinalizerAction::Hold {
        return Ok(Action::requeue(HOLD_RECHECK));
    }
    // A policy's verdict is a function of its spec and of the clock: a key
    // whose `notAfter` passes at 03:00 becomes `Expired` with nobody editing
    // the object. The heartbeat keeps `evaluatedAt` moving for D3 §7.7's
    // staleness rule, and the deadline makes the boundary itself exact.
    // The policy's own keys, from the object this pass reconciled: no second
    // API call, and the translation `reconcile_policy` used.
    let deadline = next_clock_change(&crate::trust::from_policy(&policy).keys, now);
    Ok(Action::requeue(requeue_before(deadline, now)))
}

/// Requeue on an error, naming it.
fn error_policy(policy: Arc<TrustPolicy>, err: &ReconcileError, _ctx: Arc<Context>) -> Action {
    warn!(
        policy = %policy.name_any(),
        error = %err,
        "trust policy reconcile failed; requeueing"
    );
    Action::requeue(Duration::from_secs(30))
}

/// The other policies a policy event must re-evaluate: every policy's
/// `status.keys[]` and `CompromiseGuard` carry the whole set's compromise
/// records (`from_policy_in`), and the `Controller`'s watch reconciles only the
/// object that changed. Only an event that touches a compromise record fans out
/// (`PolicyScopeMemory::observe` returning
/// [`crate::trust::PolicyScope::Everything`]), so a status heartbeat costs
/// nothing here.
#[must_use]
pub fn others_to_reevaluate(
    all: Vec<Arc<TrustPolicy>>,
    scopes: &crate::trust::PolicyScopeMemory,
    policy: &TrustPolicy,
) -> Vec<kube::runtime::reflector::ObjectRef<TrustPolicy>> {
    if scopes.observe(policy) != crate::trust::PolicyScope::Everything {
        return Vec::new();
    }
    let me = policy.name_any();
    all.iter()
        .filter(|p| p.name_any() != me)
        .map(|p| kube::runtime::reflector::ObjectRef::from_obj(&**p))
        .collect()
}

/// Run the `TrustPolicy` controller until the process ends.
pub fn controller(client: kube::Client) -> impl std::future::Future<Output = ()> + Send {
    let api: Api<TrustPolicy> = Api::all(client.clone());
    let peers: Api<TrustPolicy> = Api::all(client.clone());
    let scopes = Arc::new(crate::trust::PolicyScopeMemory::default());
    // No archive handle and no runner Job, spelled out so the one context field
    // that is a capability is visible at every construction site.
    let ctx = Arc::new(Context {
        client,
        archive: None,
        runner_image: crate::job::RunnerImage::default(),
    });
    async move {
        let controller = Controller::new(api, watcher::Config::default());
        let store = controller.store();
        controller
            // A compromise recorded on one policy re-evaluates the others.
            .watches(peers, watcher::Config::default(), move |policy| {
                others_to_reevaluate(store.state(), &scopes, &policy)
            })
            .run(reconcile, error_policy, ctx)
            .for_each(|_| std::future::ready(()))
            .await;
    }
}
