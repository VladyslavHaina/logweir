//! The `TrustPolicy` reconciler: parse the keys, resolve each one against the
//! clock, say which namespaces this policy governs and which it contests, and
//! retire the roster it replaces (PLAT-19.1, decision D3 §7.1).
//!
//! # It writes only status, and the status is the point
//!
//! Like [`super::trust_roster`], this reconciler has no side effect to
//! reconcile: no Job, no topic, no object key. What it has is three claims an
//! operator cannot check by reading the YAML — every `spkiPem` is a key this
//! build can parse and hashes to the `keyId` beside it, this is where each key
//! sits against the clock RIGHT NOW, and these are the namespaces this policy
//! actually governs once conflicts are removed.
//!
//! # `evaluatedAt` is a HEARTBEAT, and that is why it is written on a timer
//!
//! D3 §7.7's keys view renders `unknown` — never `valid` — when `status` is
//! absent, when `observedGeneration` lags `metadata.generation`, or when
//! `evaluatedAt` is older than fifteen minutes. That rule only means something
//! if a healthy controller keeps `evaluatedAt` moving, so this is the one
//! status field in the crate that is deliberately refreshed when nothing has
//! changed.
//!
//! It is also exactly the field shape that spun `TrustRoster` at 133
//! reconciles a second (erratum **E11(d)**, measured live: 12,107 patches in
//! 91.2 s), because a reconciler's own status patch is what wakes it. The
//! resolution is a DEBOUNCE, not a removal: `evaluatedAt` moves when the
//! substance changed, or when [`HEARTBEAT`] has elapsed since the stored
//! value, and otherwise the stored value is written back unchanged so
//! [`status_unchanged`] skips the patch entirely. With the 300 s requeue that
//! is one write per policy per five minutes, and fifteen minutes of staleness
//! tolerance is three missed heartbeats — not one.
//!
//! # `Superseded` on the roster, and why this controller writes it
//!
//! D3 §7.5: once a matching policy exists, `TrustRoster/default` gets
//! `Superseded=True` and stops being consulted for bound namespaces. The
//! roster's own reconciler cannot write that, because it watches rosters and a
//! policy being created is not a roster event — it would report `Superseded`
//! only the next time the roster's 300 s requeue came round, or never if the
//! requeue were removed. So the fact is written by the controller that
//! observes the cause.
//!
//! The roster is NOT deleted and its `spec` is NOT touched: rollback reads it
//! unchanged (D3 §7.5), and a condition is the one status write that says "you
//! have moved on" without taking anything away.
//!
//! # No private key material, here or anywhere
//!
//! `spkiPem` is a public key. This reconciler parses public keys and compares
//! timestamps; it reads no Secret (`tests/linkage.rs`'s
//! `the_controller_never_reads_a_secret`) and this crate links the VERIFYING
//! half of the DSSE machinery and never the signing half (Global Constraint
//! 27).

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
use crate::crds::trust_roster::TrustRoster;
use crate::crds::Condition;
use crate::trust::{algorithm_agrees, bound_namespaces, conflicts, ResolvedKey, DEFAULT_SENTINEL};
use logweir_core::trust::{
    effective_state, usable_for_new_signatures, usable_for_verification, EffectiveState,
};

/// How often the reconciler re-evaluates, and the floor under an `evaluatedAt`
/// rewrite. Both, deliberately: see the module header.
pub const HEARTBEAT: Duration = Duration::from_secs(300);

/// How long before `notAfter` an `Active` key raises `ExpiringSoon`.
///
/// THIRTY DAYS, AND IT IS A PLANNING HORIZON, NOT AN ALARM. Rotation
/// (`docs/keys.md` §7.6) is four steps, one of which is "wait for in-flight
/// Jobs"; a week's warning is a week in which somebody has to interrupt what
/// they are doing. Nothing is blocked by this condition — a key inside the
/// window still signs — so the cost of it being generous is a line in
/// `kubectl get`, and the cost of it being tight is an expiry nobody planned.
pub const EXPIRING_SOON_DAYS: i64 = 30;

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
    /// The three conditions, ready to merge.
    pub conditions: Vec<Condition>,
}

/// Parse and resolve one policy against the whole set and a clock — **pure**.
///
/// A PURE FUNCTION, for the same reason [`super::trust_roster::evaluate`] is
/// one: the property is a function of the objects and the clock, and a test
/// that needs a route table to reach it is a test about `kube`.
///
/// `policies` is every `TrustPolicy` in the cluster, INCLUDING this one: the
/// conflict rule is not answerable from one object.
#[must_use]
pub fn evaluate(
    policy: &TrustPolicy,
    policies: &[TrustPolicy],
    now: DateTime<Utc>,
) -> PolicyVerdict {
    let resolved = crate::trust::from_policy(policy);

    // ---- the per-key verdicts -------------------------------------------
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
            // A KEY THAT DID NOT PARSE IS NEITHER. Reporting `false`/`None`
            // rather than omitting the fields keeps a consumer from reading an
            // absent value as "not evaluated" — the exact confusion
            // `status.expiredKeyIds` could not resolve (D3 §7.2).
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

    // ---- binding and conflicts -------------------------------------------
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

    // ---- the three conditions --------------------------------------------
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
    ];

    PolicyVerdict {
        loaded,
        keys,
        bound,
        conflicts: mine,
        conditions,
    }
}

/// The key's `effectiveState`, with `Unparseable` overriding the clock.
///
/// THE OVERRIDE IS THE POINT. [`logweir_core::trust::effective_state`] cannot
/// return `Unparseable` — the pure layer never sees key material — and a key
/// whose PEM does not parse must not be reported as `Active` merely because
/// its declared window is open.
fn key_state(key: &ResolvedKey, now: DateTime<Utc>) -> EffectiveState {
    if key.parsed.is_err() || key.declared_id_matches.is_err() {
        return EffectiveState::Unparseable;
    }
    effective_state(&key.trust, now)
}

/// The first thing wrong with one key, as a `(reason, message)` pair.
///
/// ORDERED: material, then identity, then the declared algorithm. A PEM that
/// does not parse has no id to compare and no algorithm to check, so reporting
/// all three would name two faults that were never measured.
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
/// # Why a `default: true` policy reports no namespace list
///
/// It governs every namespace no other policy names — which includes
/// namespaces that do not exist yet. Enumerating that would need `list` on
/// `namespaces`, a verb this controller does not have and must not acquire in
/// order to fill in a status field, and the list would be wrong the moment a
/// namespace was created. The condition says so in words instead.
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
    let name = policy.name_any();
    // ONE CLOCK READ, for the reason `trust_roster::reconcile_roster` records:
    // two reads could put a key's expiry and the condition reporting it on
    // either side of the same instant.
    let now = Utc::now();

    // CLUSTER-SCOPED: `Api::all`, no namespace. The whole set, because the
    // conflict rule is not answerable from one object.
    let api: Api<TrustPolicy> = Api::all(client.clone());
    let all = api
        .list(&ListParams::default())
        .await
        .map_err(ReconcileError::Api)?;
    let verdict = evaluate(policy, &all.items, now);
    let status = status_for(policy, &verdict, now);

    // SEAM S7's precondition (review finding F6): the body carries
    // `metadata.resourceVersion`, so a pass computing from a stale watch-cache
    // copy gets `409 Conflict` rather than overwriting a newer verdict.
    let patch = with_precondition(&policy.metadata, &name, json!({ "status": status }))?;
    // NO WRITE WHEN NOTHING CHANGED — erratum E11(d). With the debounce above,
    // a steady policy reaches this branch on four reconciles out of five.
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
        api.patch_status(&name, &PatchParams::default(), &Patch::Merge(patch))
            .await
            .map_err(ReconcileError::Api)?;
    }

    supersede_roster(client, &all.items, now).await?;

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
/// # "Once a **matching** policy exists" — and the matching one is the DEFAULT
///
/// Review finding F2. The first version of this wrote `Superseded=True` as soon
/// as ANY policy existed, which is broader than the contract and is wrong in a
/// way an operator acts on: a policy governing only `team-a` leaves every other
/// namespace resolving to `legacy-roster-v1`, **synthesised from this very
/// roster**, while the roster advertises that it has been replaced. An operator
/// reading that reasonably stops maintaining it — or deletes it — and silently
/// un-trusts every unbound namespace.
///
/// The only policy that displaces the roster for every namespace is the one
/// with `spec.default: true`, because the resolution order (§7.1) reaches the
/// roster only after the default has been tried. So that, and nothing else, is
/// a match. Two defaults are NOT a match either: they contest each other, every
/// fallback namespace resolves to nothing rather than to either of them, and a
/// roster marked superseded by a conflict would be superseded by something that
/// governs nobody.
///
/// # It is a CONDITION WITH TWO STATES, not a flag that gets set
///
/// The other half of F2: `reconcile_policy` runs only for a policy that exists,
/// so deleting every policy left `Superseded=True` on the roster forever —
/// rollback-in-place advertised the opposite of what was happening. Returning
/// `Superseded=False/RosterStillConsulted` makes the condition self-clearing,
/// and [`super::trust_roster`] writes it too, from its own 300 s requeue, so
/// the clearing pass does not depend on a policy existing to run it.
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
/// # Why the whole condition list is carried
///
/// A JSON merge patch REPLACES arrays (RFC 7386), so a patch carrying only
/// `[Superseded]` would DELETE the `Loaded` condition [`super::trust_roster`]
/// wrote — and that reconciler would put it back, deleting this one, forever.
/// The existing list is read and merged, exactly as `verification::carry_verified`
/// does for the same reason, and the roster reconciler carries conditions it
/// does not own forward.
///
/// # Errors
///
/// A `kube::Error` from the read or the patch. A roster that is **not found**
/// is not an error: a cluster that never had one has nothing to supersede.
async fn supersede_roster(
    client: &kube::Client,
    policies: &[TrustPolicy],
    now: DateTime<Utc>,
) -> Result<(), ReconcileError> {
    let api: Api<TrustRoster> = Api::all(client.clone());
    let roster = match api
        .get_opt(ROSTER_NAME)
        .await
        .map_err(ReconcileError::Api)?
    {
        Some(r) => r,
        None => return Ok(()),
    };
    let existing = roster.status.as_ref().and_then(|s| s.conditions.as_ref());
    let superseded = superseded_condition(&roster, policies, now);
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

/// Add seam **S7**'s optimistic-concurrency precondition to a `/status` merge
/// patch.
///
/// Review finding F6. Kubernetes applies `metadata.resourceVersion` in a patch
/// body as an update precondition and answers `409 Conflict` on a mismatch;
/// the object name beside it makes the body self-identifying and ties the
/// precondition to the object the request path names. This is
/// `backup_schedule::status_patch_with_preconditions`'s shape, kept separate
/// because that one is typed to `BackupSchedule`.
///
/// IT MATTERS MORE HERE THAN ANYWHERE ELSE IN THE CRATE, and that is why it
/// went in for a `low`. `TrustRoster/default`'s `status.conditions` now has
/// **two** writers doing read-modify-write on an array an RFC 7386 merge patch
/// REPLACES: without the precondition, a roster reconcile computing from a
/// watch-cache copy that predates [`supersede_roster`]'s write silently deletes
/// `Superseded`, and the roster advertises the wrong thing for up to 300 s.
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
    reconcile_policy(&policy, &ctx.client).await?;
    // A policy's verdict is a function of its spec AND OF THE CLOCK: a key
    // whose `notAfter` passes at 03:00 must become `Expired` without anybody
    // editing the object, and `evaluatedAt` must keep moving for D3 §7.7's
    // fifteen-minute staleness rule to distinguish "stale" from "stopped".
    Ok(Action::requeue(HEARTBEAT))
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

/// Run the `TrustPolicy` controller until the process ends.
pub fn controller(client: kube::Client) -> impl std::future::Future<Output = ()> + Send {
    let api: Api<TrustPolicy> = Api::all(client.clone());
    // This reconciler holds NO archive handle and creates no runner Job —
    // spelled out rather than defaulted, so the one context field that is a
    // capability is visible at every construction site (`super::Context`).
    let ctx = Arc::new(Context {
        client,
        archive: None,
        runner_image: crate::job::RunnerImage::default(),
    });
    async move {
        Controller::new(api, watcher::Config::default())
            .run(reconcile, error_policy, ctx)
            .for_each(|_| std::future::ready(()))
            .await;
    }
}
