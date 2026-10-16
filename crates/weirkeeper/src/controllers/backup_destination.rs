//! The `BackupDestination` reconciler: say whether a saved destination is
//! usable, and publish the two digests every other path compares against.
//!
//! # It creates nothing and dials nothing
//!
//! D2 §3.1: **there is no periodic health probe.** This reconciler dials no
//! endpoint, lists no bucket and creates no Job. A destination is exercised by
//! the operations that use it and by an explicit `Preflight` with
//! `operation: DestinationAccess`: cached health rendered as readiness (a
//! `VALID true` that meant "reachable four minutes ago") is a false claim.
//!
//! What it does is answer what CEL cannot: rules R0–R9 are compiled into the
//! CRD, while two answers here move underneath a fixed schema.
//!
//! * The engine version decides whether `VirtualHosted` with a custom endpoint
//!   can be honoured. The pinned engine forces path-style whenever an endpoint
//!   is set, so the setting is refused here rather than silently ignored at run
//!   time.
//! * The CA `ConfigMap` is another object, which CEL cannot read: its
//!   existence, key, size and whether its bytes are certificates are facts
//!   about the cluster, not this spec.
//!
//! # It reads a ConfigMap and never a Secret
//!
//! A CA certificate is public material, which is why `spec.transport.caBundle`
//! names a `ConfigMap`. The four access grants name Secrets, and the controller
//! holds no verb on `secrets`
//! (`tests/linkage.rs::the_controller_never_reads_a_secret`): D2 §3.8 option B,
//! scoped Secret `get` by `resourceNames`, is rejected because a controller
//! that could read every tenant's object-store credential aggregates exactly
//! the blast radius destinations exist to separate.
//!
//! So a grant is validated for shape and spelling only (a DNS-1123 object name
//! in this namespace, legal Secret data keys). A Secret that does not exist is
//! reported by the kubelet, to the pod that needed it, as
//! [`logweir_core::check_contract::CheckCode::CredentialSecretNotFound`].
//!
//! # The 300 s requeue is about the CA, not about the spec
//!
//! A spec edit wakes this loop anyway; the `ConfigMap` behind
//! `spec.transport.caBundle` does not. An administrator rotating a private CA
//! edits an object this reconciler does not watch, and without the requeue
//! `status.caBundleSha256` would name the old bytes until somebody touched the
//! destination.

use std::sync::Arc;

use chrono::Utc;
use futures::StreamExt as _;
use kube::api::{Patch, PatchParams};
use kube::runtime::controller::Action;
use kube::runtime::{watcher, Controller};
use kube::{Api, ResourceExt};
use serde_json::json;
use tracing::{debug, info, warn};

use super::approval::ReconcileError;
use super::Context;
use crate::conditions::{current_condition, merge_condition, replacing, status_unchanged};
use crate::crds::backup_destination::{BackupDestination, BackupDestinationStatus};
use crate::crds::Condition;
use crate::destination::{
    evaluate, observed_at_for, read_ca_bundle, CaObservation, DestinationVerdict, CONDITION_VALID,
};

/// How long before a destination is looked at again, so a rotated CA
/// `ConfigMap` is observed (D2 §3.3).
pub const REQUEUE_SECONDS: u64 = 300;

/// How long before a destination whose reconcile errored is looked at again.
pub const ERROR_REQUEUE_SECONDS: u64 = 30;

/// The `/status` body one verdict produces.
///
/// Every field on every verdict, refusals included: `canonicalUrl` and
/// `locationDigest` are functions of the location alone, and an operator
/// debugging a `CaBundleNotFound` still needs to see which bucket they pointed
/// at. `caBundleSha256` is `None` on a refusal: there were no bytes to digest.
#[must_use]
pub fn status_for(
    dest: &BackupDestination,
    verdict: &DestinationVerdict,
    now: chrono::DateTime<Utc>,
) -> BackupDestinationStatus {
    let previous = dest.status.as_ref();
    BackupDestinationStatus {
        observed_generation: dest.metadata.generation,
        reason: Some(verdict.reason.to_string()),
        canonical_url: Some(verdict.canonical_url.clone()),
        location_digest: Some(verdict.location_digest.clone()),
        ca_bundle_sha256: verdict.ca_bundle_sha256.clone(),
        observed_at: Some(observed_at_for(previous, verdict, now)),
        // A pure function of the UID and the immutable location, so it never
        // moves on an unchanged object and costs no write of its own.
        credential_binding: dest
            .metadata
            .uid
            .as_deref()
            .filter(|u| !u.trim().is_empty())
            .map(|uid| {
                logweir_core::credential_binding::destination_binding(
                    uid,
                    &crate::destination::location_of(dest).archive_storage_url(),
                )
            }),
        conditions: Some(vec![merge_condition(
            current_condition(
                previous.and_then(|s| s.conditions.as_ref()),
                CONDITION_VALID,
            ),
            Condition {
                r#type: CONDITION_VALID.to_string(),
                status: if verdict.valid { "True" } else { "False" }.to_string(),
                observed_generation: dest.metadata.generation,
                last_transition_time: Some(now),
                reason: Some(verdict.reason.to_string()),
                message: Some(verdict.message.clone()),
            },
        )]),
    }
}

/// Reconcile one `BackupDestination` and patch **only** its `/status`.
///
/// # Errors
///
/// [`ReconcileError`] for anything that is not a verdict — a missing namespace,
/// or an API server that would not answer.
pub async fn reconcile_destination(
    dest: &BackupDestination,
    client: &kube::Client,
) -> Result<DestinationVerdict, ReconcileError> {
    reconcile_destination_at(dest, client, Utc::now()).await
}

/// [`reconcile_destination`] at `now`, the caller's one clock read: two could
/// put a verdict and the condition that reports it on either side of the same
/// instant (FX-12, Global Constraint 1).
///
/// # Errors
///
/// As [`reconcile_destination`].
pub async fn reconcile_destination_at(
    dest: &BackupDestination,
    client: &kube::Client,
    now: chrono::DateTime<Utc>,
) -> Result<DestinationVerdict, ReconcileError> {
    let name = dest.name_any();
    let namespace = dest
        .namespace()
        .filter(|n| !n.is_empty())
        .ok_or_else(|| ReconcileError::NoNamespace(name.clone()))?;

    // The CA read is the only I/O, and it happens before the verdict, so
    // `evaluate` stays a pure function of a spec and one observation. No bundle
    // named, no read.
    let observation = match dest.spec.transport.ca_bundle.as_ref() {
        None => CaObservation::NotDeclared,
        Some(_) => {
            let resolved = crate::destination::ResolvedDestination {
                name: name.clone(),
                namespace: namespace.clone(),
                uid: dest.uid().unwrap_or_default(),
                generation: dest.metadata.generation.unwrap_or(0),
                role: crate::destination::DestinationRole::ArchiveWrite,
                location: crate::destination::location_of(dest),
                location_digest: String::new(),
                canonical_url: String::new(),
                ca_bundle: dest.spec.transport.ca_bundle.as_ref().map(|c| {
                    crate::destination::CaBundleReference {
                        config_map_name: c.config_map_name.clone(),
                        key: c.key.clone(),
                    }
                }),
                ca_sha256: None,
                ca_pem: None,
                grant: crate::destination::ResolvedGrant::NotConfigured,
                inline_archive: false,
            };
            read_ca_bundle(client, &resolved)
                .await
                .map_err(ReconcileError::Api)?
        }
    };

    let verdict = evaluate(dest, &observation);
    let status = status_for(dest, &verdict, now);

    let api: Api<BackupDestination> = Api::namespaced(client.clone(), &namespace);
    // `replacing`: every status field is optional and omitted when `None`, and
    // a merge patch keeps an omitted key, so a CA `ConfigMap` that went away
    // would leave the old `caBundleSha256` beside `CaBundleNotFound`, move
    // `observedAt` on every pass and wake this reconciler with each write. This
    // controller is the status's only writer, so a key the stored status has
    // and this verdict does not is sent as `null`.
    let stored = dest
        .status
        .as_ref()
        .and_then(|s| serde_json::to_value(s).ok());
    let patch = json!({
        "status": replacing(
            serde_json::to_value(&status).unwrap_or(serde_json::Value::Null),
            stored.as_ref(),
        )
    });
    // No write when nothing changed: this reconciler's own status patch wakes
    // it, and a patch that changed only the clock spins the loop.
    // `observed_at_for` keeps the timestamp stable across an unchanged verdict.
    if status_unchanged(stored.as_ref(), &patch) {
        debug!(
            destination = %name,
            namespace = %namespace,
            "the computed status equals the one on the object; no patch is sent"
        );
    } else {
        // Seam S7 in both halves: a merge patch, never `Api::replace_status`
        // (authorised as `update`, which this role grants on nothing), and
        // `metadata.resourceVersion` in the body as the update precondition.
        // Without it, two replicas (or another writer) lose one write
        // silently: last-write-wins picks the stale verdict, and `VALID` names
        // the wrong CA until the 300 s requeue. With it the loser gets 409 and
        // reconciles again from what is stored.
        let body =
            status_patch_with_preconditions(dest, patch).map_err(|e| ReconcileError::Api(*e))?;
        if let Err(kube::Error::Api(e)) = api
            .patch_status(&name, &PatchParams::default(), &Patch::Merge(body))
            .await
        {
            // A 409 is the precondition working: something wrote this status
            // between the read and the write, so the next reconcile reads the
            // newer object.
            if e.code == 409 {
                debug!(
                    destination = %name,
                    namespace = %namespace,
                    "the status changed under this reconcile (409); the next pass reads it"
                );
                return Ok(verdict);
            }
            return Err(ReconcileError::Api(kube::Error::Api(e)));
        }
    }

    if verdict.valid {
        info!(
            destination = %name,
            namespace = %namespace,
            canonical_url = %verdict.canonical_url,
            location_digest = %verdict.location_digest,
            transport = dest.spec.transport.security.as_str(),
            addressing = dest.spec.storage.addressing.as_str(),
            "backup destination valid"
        );
    } else {
        warn!(
            destination = %name,
            namespace = %namespace,
            reason = verdict.reason,
            message = %verdict.message,
            "backup destination not valid"
        );
    }
    Ok(verdict)
}

/// Add the optimistic-concurrency precondition to a `/status` merge patch
/// (seam **S7**).
///
/// The same body shape as
/// `controllers::backup_schedule::status_patch_with_preconditions`: the API
/// server applies a `metadata.resourceVersion` in a patch body as an update
/// precondition and answers `409 Conflict` on a mismatch, a compare-and-set
/// without the `update` verb. `metadata.name` ties the precondition to the
/// object the request path names.
///
/// # Errors
///
/// A [`kube::Error`] shaped as the API server's own "no resourceVersion" answer,
/// for an object that carries none. Unreachable for anything that came from a
/// watch or a `get`; named rather than unwrapped.
fn status_patch_with_preconditions(
    dest: &BackupDestination,
    mut patch: serde_json::Value,
) -> Result<serde_json::Value, Box<kube::Error>> {
    let name = dest.name_any();
    let resource_version = dest
        .metadata
        .resource_version
        .clone()
        .filter(|v| !v.is_empty())
        .ok_or_else(|| {
            Box::new(kube::Error::Discovery(
                kube::error::DiscoveryError::MissingResource(format!(
                    "BackupDestination {name} carries no metadata.resourceVersion, which a \
                     /status compare-and-set needs (D-SEAMS S7)"
                )),
            ))
        })?;
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
async fn reconcile(
    dest: Arc<BackupDestination>,
    ctx: Arc<Context>,
) -> Result<Action, ReconcileError> {
    // PROD-19.1: the object's workspace, and every object and Secret it
    // names, before anything else; a refusal is terminal and creates nothing.
    if let Some(action) = crate::workspace::gate(&ctx.client, dest.as_ref()).await? {
        return Ok(action);
    }
    reconcile_destination(&dest, &ctx.client).await?;
    Ok(destination_action())
}

/// The [`Action`] every reconciled destination returns: a timed requeue of
/// [`REQUEUE_SECONDS`], never `await_change()`.
///
/// The requeue is about the CA `ConfigMap`, which this controller does not
/// watch, and is the only thing that notices a rotated bundle
/// (`destination_controller::a_rotated_ca_is_read_by_the_timed_requeue`).
#[must_use]
pub fn destination_action() -> Action {
    Action::requeue(std::time::Duration::from_secs(REQUEUE_SECONDS))
}

/// The [`Action`] a failed pass returns: a timed requeue of
/// [`ERROR_REQUEUE_SECONDS`].
#[must_use]
pub fn destination_error_action() -> Action {
    Action::requeue(std::time::Duration::from_secs(ERROR_REQUEUE_SECONDS))
}

/// Requeue on an error, naming it.
fn error_policy(dest: Arc<BackupDestination>, err: &ReconcileError, _ctx: Arc<Context>) -> Action {
    warn!(
        destination = %dest.name_any(),
        error = %err,
        "backup destination reconcile failed; requeueing"
    );
    destination_error_action()
}

/// Run the `BackupDestination` controller until the process ends.
///
/// All namespaces (`Api::all`) by default: a destination is a namespaced
/// object and the controller reconciles every namespace it is granted.
pub fn controller(client: kube::Client) -> impl std::future::Future<Output = ()> + Send {
    controller_in(client)
}

/// The one watch of [`controller`], in the install's namespace.
fn controller_in(client: kube::Client) -> impl std::future::Future<Output = ()> + Send {
    let api: Api<BackupDestination> =
        Api::namespaced(client.clone(), crate::workspace::namespace());
    let ctx = Arc::new(Context {
        client,
        // No archive handle: this reconciler validates a reference and digests
        // a CA. The `ControllerIdentity` evidence handles
        // (`crate::evidence_store::StoreCache`) are built by the paths that
        // verify evidence.
        archive: None,
        // No runner image: it creates no Job (see `super::Context::runner_image`
        // for why the default means "unused").
        runner_image: crate::job::RunnerImage::default(),
    });
    async move {
        Controller::new(api, watcher::Config::default())
            .run(reconcile, error_policy, ctx)
            .for_each(|_| std::future::ready(()))
            .await;
    }
}
