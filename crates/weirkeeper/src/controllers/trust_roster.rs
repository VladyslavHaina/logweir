//! The `TrustRoster` reconciler: parse both lists, report expiry, and say so
//! in one condition.
//!
//! # Small by design
//!
//! The roster has no side effect to reconcile. It carries a claim an operator
//! cannot check by reading the YAML: every `spkiPem` parses, and these ids are
//! past their `notAfter`. This reconciler is what makes the `LOADED` and
//! `EXPIRED` printer columns mean something.
//!
//! # Why expiry is reported here and not derived by each consumer
//!
//! `status.expiredKeyIds` exists so no consumer derives expiry from a clock it
//! does not share with the controller; the keys page renders it.
//! [`evaluate`] (the approval one) still checks the matched key's `notAfter`
//! against its own `now`, because a status field is part of nothing anyone
//! signed and an approval must never be admitted on one.
//!
//! # An unparseable PEM is `Loaded=False`, and every approval fails with it
//!
//! `evaluate` refuses every approval against a roster with one bad entry (its
//! check 2), so this condition and that refusal are two views of one rule: a
//! partially loaded roster is not a roster.
//!
//! # No private key material, here or anywhere
//!
//! `spkiPem` is a **public** key. This reconciler parses public keys and
//! compares timestamps; it reads no Secret (`tests/linkage.rs`'s
//! `the_controller_never_reads_a_secret`) and this crate links the verifying
//! half of the DSSE machinery and never the signing half (Global Constraint
//! 27).
//!
//! [`evaluate`]: super::approval::evaluate

use std::sync::Arc;

use chrono::{DateTime, Utc};
use futures::StreamExt as _;
use kube::api::{Patch, PatchParams};
use kube::runtime::controller::Action;
use kube::runtime::{watcher, Controller};
use kube::{Api, ResourceExt};
use logweir_verify::VerifyingKey;
use serde_json::json;
use tracing::{debug, info, warn};

use super::approval::{ReconcileError, ROSTER_NAME};
use super::trust_policy::{superseded_condition, with_precondition, CONDITION_SUPERSEDED};
use super::Context;
use crate::conditions::{current_condition, merge_condition, status_unchanged};
use crate::crds::trust_policy::TrustPolicy;
use crate::crds::trust_roster::{KeyEntry, TrustRoster, TrustRosterSpec, TrustRosterStatus};
use crate::crds::Condition;

/// The condition type this reconciler owns.
pub const CONDITION_LOADED: &str = "Loaded";

/// The `reason` written when every entry on both lists parsed.
pub const REASON_LOADED: &str = "Loaded";

/// The `reason` written when an entry's `spkiPem` did not parse.
pub const REASON_UNPARSEABLE_KEY: &str = "UnparseableKey";

/// Which list an entry came from, for the message.
const APPROVER_KEYS: &str = "approverKeys";
/// Which list an entry came from, for the message.
const SIGNING_KEYS: &str = "signingKeys";

/// What one roster reconcile decided.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RosterVerdict {
    /// Whether every entry on **both** lists parsed.
    pub loaded: bool,
    /// The `keyId`s past their `notAfter`, from **both** lists, in
    /// `approverKeys`-then-`signingKeys` order.
    pub expired_key_ids: Vec<String>,
    /// The condition `reason`.
    pub reason: &'static str,
    /// The condition `message`.
    pub message: String,
}

/// Parse both lists, collect expiry, and say which.
///
/// Pure, like [`super::approval::evaluate`]. Expiry is collected even when a
/// PEM fails to parse: `notAfter` needs no key material, so an operator with
/// one bad PEM still learns which other keys have lapsed.
#[must_use]
pub fn evaluate(spec: &TrustRosterSpec, now: DateTime<Utc>) -> RosterVerdict {
    let mut expired_key_ids = Vec::new();
    for (_, entry) in lists(spec) {
        if entry.not_after.is_some_and(|not_after| not_after <= now) {
            expired_key_ids.push(entry.key_id.clone());
        }
    }

    for (list, entry) in lists(spec) {
        if let Err(e) = VerifyingKey::from_pem_str(&entry.spki_pem) {
            return RosterVerdict {
                loaded: false,
                expired_key_ids,
                reason: REASON_UNPARSEABLE_KEY,
                message: format!(
                    "spec.{list} entry keyId {} carries an spkiPem that is not a P-256 or \
                     Ed25519 public key ({e}); a partially loaded roster is not a roster, so no \
                     approval is accepted against it",
                    entry.key_id
                ),
            };
        }
    }

    RosterVerdict {
        message: format!(
            "every spkiPem on this roster parsed: {} approverKeys, {} signingKeys, {} expired",
            spec.approver_keys.len(),
            spec.signing_keys.len(),
            expired_key_ids.len()
        ),
        loaded: true,
        expired_key_ids,
        reason: REASON_LOADED,
    }
}

/// Both lists, in `approverKeys`-then-`signingKeys` order, each tagged with
/// the field name a message should name.
///
/// One iterator, so neither list can be forgotten
/// (`the_roster_reconciler_expires_keys_from_both_lists`).
fn lists(spec: &TrustRosterSpec) -> impl Iterator<Item = (&'static str, &KeyEntry)> {
    spec.approver_keys
        .iter()
        .map(|e| (APPROVER_KEYS, e))
        .chain(spec.signing_keys.iter().map(|e| (SIGNING_KEYS, e)))
}

/// The `/status` body one verdict produces.
#[must_use]
pub fn status_for(
    roster: &TrustRoster,
    verdict: &RosterVerdict,
    policies: &[TrustPolicy],
    now: DateTime<Utc>,
) -> TrustRosterStatus {
    let existing = roster.status.as_ref().and_then(|s| s.conditions.as_ref());
    // Every condition this reconciler does not own is carried forward: a JSON
    // merge patch replaces arrays, and `controllers::trust_policy` writes
    // `Superseded` here, so without the carry the two reconcilers would delete
    // each other's condition on every pass.
    let mut conditions: Vec<Condition> = existing
        .map(|c| {
            c.iter()
                .filter(|c| c.r#type != CONDITION_LOADED && c.r#type != CONDITION_SUPERSEDED)
                .cloned()
                .collect()
        })
        .unwrap_or_default();
    conditions.push(merge_condition(
        current_condition(existing, CONDITION_LOADED),
        Condition {
            r#type: CONDITION_LOADED.to_string(),
            status: if verdict.loaded { "True" } else { "False" }.to_string(),
            observed_generation: roster.metadata.generation,
            last_transition_time: Some(now),
            reason: Some(verdict.reason.to_string()),
            message: Some(verdict.message.clone()),
        },
    ));
    // `Superseded` is recomputed here, not just carried:
    // `trust_policy::reconcile_policy` runs only for a policy that exists, so
    // deleting every policy would leave `Superseded=True` forever. This
    // reconciler requeues whether or not a policy exists, and both writers call
    // the same function. After `Loaded`, so this reconciler's own condition
    // stays `conditions[0]`, which `approval_controller.rs`'s transition-time
    // assertions read.
    conditions.push(superseded_condition(roster, policies, now));
    TrustRosterStatus {
        loaded: Some(verdict.loaded),
        expired_key_ids: Some(verdict.expired_key_ids.clone()),
        conditions: Some(conditions),
    }
}

/// Reconcile one `TrustRoster` and patch **only** its `/status`.
///
/// # Errors
///
/// [`ReconcileError`] for anything that is not a verdict.
pub async fn reconcile_roster(
    roster: &TrustRoster,
    client: &kube::Client,
) -> Result<RosterVerdict, ReconcileError> {
    reconcile_roster_at(roster, client, Utc::now()).await
}

/// [`reconcile_roster`] at `now`, the caller's one clock read, so a key's
/// expiry and the condition reporting it cannot straddle two instants (FX-12,
/// Global Constraint 1).
///
/// # Errors
///
/// [`ReconcileError`] for anything that is not a verdict.
pub async fn reconcile_roster_at(
    roster: &TrustRoster,
    client: &kube::Client,
    now: chrono::DateTime<Utc>,
) -> Result<RosterVerdict, ReconcileError> {
    let name = roster.name_any();
    let verdict = evaluate(&roster.spec, now);
    // The whole policy set: whether one displaces this roster is not answerable
    // from one object.
    let policies: Api<TrustPolicy> = Api::all(client.clone());
    let all = policies
        .list(&kube::api::ListParams::default())
        .await
        .map_err(ReconcileError::Api)?;
    let status = status_for(roster, &verdict, &all.items, now);

    // Cluster-scoped: `TrustRoster` is the one kind in this group that is not
    // namespaced, and in Kubernetes the strong form of "a separate file the
    // drill spec cannot widen" is "a different RBAC subject".
    let api: Api<TrustRoster> = Api::all(client.clone());
    // Seam S7's precondition: `status.conditions` has two read-modify-write
    // writers on an array a merge patch replaces, so a pass computing from a
    // stale cache copy must be refused rather than delete the other's
    // condition.
    let patch = with_precondition(&roster.metadata, &name, json!({ "status": status }))?;
    // No write when nothing changed: this reconciler's own status patch wakes
    // it, so a clock-only change would spin it. The verdict is still logged on
    // every reconcile.
    if status_unchanged(
        roster
            .status
            .as_ref()
            .and_then(|s| serde_json::to_value(s).ok())
            .as_ref(),
        &patch,
    ) {
        debug!(
            roster = %name,
            "the computed status equals the one on the object; no patch is sent"
        );
    } else {
        api.patch_status(&name, &PatchParams::default(), &Patch::Merge(patch))
            .await
            .map_err(ReconcileError::Api)?;
    }

    if verdict.loaded {
        info!(
            roster = %name,
            expired = verdict.expired_key_ids.len(),
            "trust roster loaded"
        );
    } else {
        warn!(roster = %name, reason = verdict.reason, "trust roster not loaded");
    }
    if name != ROSTER_NAME {
        // Not a refusal, but `Approval` verification resolves
        // `trustrosters/default` only, so a roster under another name
        // authorises nothing and the operator is told rather than shown a
        // `LOADED true` that changes no behaviour.
        warn!(
            roster = %name,
            resolved = ROSTER_NAME,
            "this TrustRoster is not the one approvals resolve: the name is fixed at \
             '{ROSTER_NAME}' (see docs/kubernetes.md install step 1)"
        );
    }
    Ok(verdict)
}

/// The `kube::runtime` reconcile entry point.
async fn reconcile(roster: Arc<TrustRoster>, ctx: Arc<Context>) -> Result<Action, ReconcileError> {
    reconcile_roster(&roster, &ctx.client).await?;
    // A roster's verdict depends on the clock: a key whose `notAfter` passes
    // at 03:00 must appear in `expiredKeyIds` with nobody editing the object,
    // so the requeue keeps expiry honest.
    Ok(Action::requeue(std::time::Duration::from_secs(300)))
}

/// Requeue on an error, naming it.
fn error_policy(roster: Arc<TrustRoster>, err: &ReconcileError, _ctx: Arc<Context>) -> Action {
    warn!(
        roster = %roster.name_any(),
        error = %err,
        "trust roster reconcile failed; requeueing"
    );
    Action::requeue(std::time::Duration::from_secs(30))
}

/// Run the `TrustRoster` controller until the process ends.
pub fn controller(client: kube::Client) -> impl std::future::Future<Output = ()> + Send {
    let api: Api<TrustRoster> = Api::all(client.clone());
    // No archive handle, spelled out so the one context field that is a
    // capability is visible at every construction site.
    let ctx = Arc::new(Context {
        client,
        archive: None,
        // No runner Job, so the runner image is unused here; the default
        // means "unused", never "no override configured" (see
        // `super::Context::runner_image`).
        runner_image: crate::job::RunnerImage::default(),
    });
    async move {
        Controller::new(api, watcher::Config::default())
            .run(reconcile, error_policy, ctx)
            .for_each(|_| std::future::ready(()))
            .await;
    }
}
