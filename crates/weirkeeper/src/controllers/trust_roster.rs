//! The `TrustRoster` reconciler: parse both lists, report expiry, and say so
//! in one condition.
//!
//! # It is small, and that is the design
//!
//! The roster has no side effects to reconcile — no Job, no topic, no object
//! key. What it has is a claim an operator cannot check by reading the YAML:
//! *every* `spkiPem` on it is a key this build can actually parse, and these
//! particular ids are past their `notAfter`. `kubectl get trustroster` renders
//! `LOADED` and `EXPIRED` from
//! `crates/weirkeeper/src/crds/trust_roster.rs`'s printer columns, so this
//! reconciler is what makes those two columns mean something.
//!
//! # Why expiry is reported here and not derived by each consumer
//!
//! `status.expiredKeyIds` is declared *so no consumer has to derive expiry
//! itself from a clock it does not share with the controller*
//! (`crds/trust_roster.rs`). Task 27's keys page renders the column; it does
//! not recompute it. [`evaluate`] — the *approval* one — still checks the
//! matched key's `notAfter` against its own `now`, because a status field is
//! not part of anything anyone signed and an approval must never be admitted
//! on the strength of one.
//!
//! # An unparseable PEM is `Loaded=False`, and every approval fails with it
//!
//! Not a warning, and not a skipped entry. `evaluate` refuses **every**
//! approval against a roster with one bad entry (its check 2), so this
//! condition and that refusal are two views of one rule: a partially loaded
//! roster is not a roster.
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
/// A PURE FUNCTION, for the same reason [`super::approval::evaluate`] is one:
/// the property is a function of a spec and a clock, and a test that has to
/// build a route table to reach it is a test about `kube`.
///
/// EXPIRY IS COLLECTED EVEN WHEN A PEM FAILS TO PARSE. `notAfter` is a
/// timestamp on the entry and needs no key material, so an operator with one
/// bad PEM still gets told which of their other keys have lapsed instead of
/// having to fix one problem to see the next.
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
/// ONE ITERATOR SO NEITHER LIST CAN BE FORGOTTEN. The first draft of this
/// reconciler could have walked `approverKeys` only and looked complete —
/// `the_roster_reconciler_expires_keys_from_both_lists` is the test that would
/// have caught it, and this helper is what makes both walks above use the same
/// source.
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
    // EVERY CONDITION THIS RECONCILER DOES NOT OWN, CARRIED FORWARD — PLAT-19.1.
    //
    // A JSON merge patch REPLACES arrays (RFC 7386), so a status carrying only
    // `[Loaded]` DELETES whatever else is on the object. Until PLAT-19.1
    // nothing else wrote here and the array could be built from scratch;
    // `controllers::trust_policy` now writes `Superseded` (D3 §7.5), and
    // without this carry the two reconcilers would delete each other's
    // condition on every pass — the same hot loop `verification::carry_verified`
    // was written for, measured at 133 reconciles a second (erratum E11(d)).
    let mut conditions: Vec<Condition> = existing
        .map(|c| {
            c.iter()
                .filter(|c| c.r#type != CONDITION_LOADED && c.r#type != CONDITION_SUPERSEDED)
                .cloned()
                .collect()
        })
        .unwrap_or_default();
    // `Superseded` IS RECOMPUTED HERE, NOT JUST CARRIED — PLAT-19.1 review
    // finding F2, second half. `trust_policy::reconcile_policy` runs only for a
    // policy that EXISTS, so deleting every policy left `Superseded=True` on
    // this roster forever: a rollback-in-place advertised the opposite of what
    // was happening. This reconciler requeues every 300 s whether or not a
    // policy exists, so it is the one that can clear it. Both writers call the
    // same function, so they cannot disagree about what the condition means.
    conditions.push(superseded_condition(roster, policies, now));
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
    let name = roster.name_any();
    // ONE CLOCK READ. Two reads — one for the verdict, one for the status —
    // could put a key's expiry and the condition that reports it on either
    // side of the same instant.
    let now = Utc::now();
    let verdict = evaluate(&roster.spec, now);
    // THE WHOLE POLICY SET, because `Superseded` is a statement about which of
    // them displaces this roster and that is not answerable from one object.
    // The `trustpolicies` `list` grant PLAT-19.1 added already covers it.
    let policies: Api<TrustPolicy> = Api::all(client.clone());
    let all = policies
        .list(&kube::api::ListParams::default())
        .await
        .map_err(ReconcileError::Api)?;
    let status = status_for(roster, &verdict, &all.items, now);

    // CLUSTER-SCOPED: `Api::all`, no namespace. `TrustRoster` is the one kind
    // in this group that is not namespaced, and cluster scope is the point —
    // in Kubernetes the strong form of "a separate file argument the drill
    // spec cannot widen" is "a different RBAC subject".
    let api: Api<TrustRoster> = Api::all(client.clone());
    // SEAM S7's precondition (PLAT-19.1 review finding F6). This roster's
    // `status.conditions` now has TWO writers doing read-modify-write on an
    // array an RFC 7386 merge patch REPLACES, so a pass computing from a stale
    // watch-cache copy must be refused rather than silently delete the other
    // writer's condition.
    let patch = with_precondition(&roster.metadata, &name, json!({ "status": status }))?;
    // NO WRITE WHEN NOTHING CHANGED — plan erratum E11(d), review finding H-1.
    // This reconciler's own status patch is what wakes it, so a patch that
    // changed nothing but the clock spun it at 133 reconciles a second on an
    // object nobody had touched (12,107 in 91.2 s, measured live).
    //
    // THE VERDICT IS STILL LOGGED. The skip is about the WRITE, not about
    // observability: an operator reading the log still sees one line per
    // reconcile, and the reconcile rate is now the requeue's (one per 300 s)
    // rather than the loop's.
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
        // NOT A REFUSAL — this reconciler reports on whatever roster exists.
        // But `Approval` verification resolves `trustrosters/default` and
        // nothing else, so a roster under any other name authorises nothing
        // and an operator has to be told rather than left with a `LOADED
        // true` column that changes no behaviour.
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
    // A roster's verdict is a function of its spec AND OF THE CLOCK: a key
    // whose `notAfter` passes at 03:00 must appear in `expiredKeyIds` without
    // anybody editing the object. `await_change()` would leave the column
    // stale until the next edit, so the requeue is what keeps expiry honest.
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
    // Task 19: this reconciler holds NO archive handle. Spelled out
    // rather than defaulted, so the one context field that is a
    // capability is visible at every construction site.
    let ctx = Arc::new(Context {
        client,
        archive: None,
        // Task 33 (and Task 37's pull policy beside it): this reconciler
        // creates no runner Job, so the image and policy this process was
        // handed would be carried and never read. The default here is
        // "unused", never "no override is configured" — see
        // `super::Context::runner_image`.
        runner_image: crate::job::RunnerImage::default(),
    });
    async move {
        Controller::new(api, watcher::Config::default())
            .run(reconcile, error_policy, ctx)
            .for_each(|_| std::future::ready(()))
            .await;
    }
}
