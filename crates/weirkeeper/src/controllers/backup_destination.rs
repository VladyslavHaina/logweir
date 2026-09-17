//! The `BackupDestination` reconciler: say whether a saved destination is
//! usable, and publish the two digests every other path compares against.
//!
//! # It creates nothing, and it is exercised by nothing
//!
//! D2 §3.1: **there is no periodic health probe.** This reconciler dials no
//! endpoint, lists no bucket and creates no Job. A destination is exercised by
//! the operations that use it and by an explicit `Preflight` with
//! `operation: DestinationAccess` — because cached health rendered as readiness
//! is precisely the defect PLAT-03 names, and a `VALID true` column that meant
//! "reachable four minutes ago" would be that defect with a new spelling.
//!
//! What it DOES is answer the question CEL cannot: rules R0–R9 are compiled
//! into the CRD and cannot change without a CRD upgrade, while two of the
//! answers here move underneath a fixed schema.
//!
//! * The ENGINE VERSION decides whether `VirtualHosted` with a custom endpoint
//!   can be honoured (grounding **G4**, defect ENGINE-PATHSTYLE). Engine 0.21.0
//!   forces path-style whenever an endpoint is set, so the setting is refused
//!   here rather than silently ignored at run time.
//! * The CA `ConfigMap` is ANOTHER OBJECT, which CEL cannot read at all: its
//!   existence, its key, its size and whether its bytes are certificates are
//!   facts about the cluster and not about this spec.
//!
//! # It reads a ConfigMap and never a Secret
//!
//! A CA certificate is PUBLIC material by construction, which is why
//! `spec.transport.caBundle` names a `ConfigMap`. The four access grants name
//! Secrets, and this reconciler does not read any of them: the controller holds
//! no verb on `secrets` (`tests/linkage.rs::the_controller_never_reads_a_secret`),
//! and D2 §3.8 option B — scoped Secret `get` by `resourceNames` — is recorded
//! as REJECTED, because a controller that could read every tenant's
//! object-store credential aggregates in one process exactly the blast radius
//! destinations exist to separate.
//!
//! What it validates about a grant is therefore its SHAPE and its SPELLING —
//! that the reference is a DNS-1123 object name in this namespace and that the
//! data keys are legal Secret keys. A Secret that does not exist is reported by
//! the kubelet, to the pod that needed it, as
//! [`logweir_core::check_contract::CheckCode::CredentialSecretNotFound`].
//!
//! # The 300 s requeue is about the CA, not about the spec
//!
//! `spec` is CEL-immutable in its load-bearing half and a spec edit wakes this
//! loop anyway. The `ConfigMap` behind `spec.transport.caBundle` is neither: an
//! administrator rotating a private CA edits an object this reconciler does not
//! watch, and without the requeue `status.caBundleSha256` would keep naming the
//! old bytes until somebody touched the destination.

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
use crate::conditions::{current_condition, merge_condition, status_unchanged};
use crate::crds::backup_destination::{BackupDestination, BackupDestinationStatus};
use crate::crds::Condition;
use crate::destination::{
    evaluate, observed_at_for, read_ca_bundle, CaObservation, DestinationVerdict, CONDITION_VALID,
};

/// How long before a destination is looked at again, so a rotated CA
/// `ConfigMap` is observed — D2 §3.3.
pub const REQUEUE_SECONDS: u64 = 300;

/// How long before a destination whose reconcile ERRORED is looked at again.
pub const ERROR_REQUEUE_SECONDS: u64 = 30;

/// The `/status` body one verdict produces.
///
/// EVERY FIELD ON EVERY VERDICT, including the refusals. `canonicalUrl` and
/// `locationDigest` are functions of the location alone and are true whatever
/// the CA turned out to be, and an operator debugging a `CaBundleNotFound`
/// still needs to see which bucket they were pointing at. `caBundleSha256` is
/// the one field that is `None` on a refusal, because there were no bytes to
/// digest.
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
    let name = dest.name_any();
    let namespace = dest
        .namespace()
        .filter(|n| !n.is_empty())
        .ok_or_else(|| ReconcileError::NoNamespace(name.clone()))?;

    // ONE CLOCK READ, for the reason the roster reconciler takes one: two reads
    // could put a verdict and the condition that reports it on either side of
    // the same instant.
    let now = Utc::now();

    // The CA read is the ONLY I/O this reconciler performs, and it happens
    // BEFORE the verdict, so `evaluate` stays a pure function of a spec and one
    // observation. A destination that names no bundle performs no read at all.
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
            };
            read_ca_bundle(client, &resolved)
                .await
                .map_err(ReconcileError::Api)?
        }
    };

    let verdict = evaluate(dest, &observation);
    let status = status_for(dest, &verdict, now);

    let api: Api<BackupDestination> = Api::namespaced(client.clone(), &namespace);
    let patch = json!({ "status": status });
    // NO WRITE WHEN NOTHING CHANGED — erratum E11(d). This reconciler's own
    // status patch is what wakes it, and a patch that changed nothing but the
    // clock spins the loop at whatever rate the API server will serve.
    // `observed_at_for` is what keeps the timestamp stable across an unchanged
    // verdict, so this comparison has something to compare.
    if status_unchanged(
        dest.status
            .as_ref()
            .and_then(|s| serde_json::to_value(s).ok())
            .as_ref(),
        &patch,
    ) {
        debug!(
            destination = %name,
            namespace = %namespace,
            "the computed status equals the one on the object; no patch is sent"
        );
    } else {
        // A MERGE PATCH CARRYING NO `resourceVersion` IS STILL NOT AN `update`
        // — seam S7. Nothing in this crate calls `Api::replace_status`, and the
        // role grants `patch` on `backupdestinations/status` and nothing else.
        api.patch_status(&name, &PatchParams::default(), &Patch::Merge(patch))
            .await
            .map_err(ReconcileError::Api)?;
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

/// The `kube::runtime` reconcile entry point.
async fn reconcile(
    dest: Arc<BackupDestination>,
    ctx: Arc<Context>,
) -> Result<Action, ReconcileError> {
    reconcile_destination(&dest, &ctx.client).await?;
    // See the module header: the requeue is about the CA `ConfigMap`, which
    // this controller does not watch, and not about the spec.
    Ok(Action::requeue(std::time::Duration::from_secs(
        REQUEUE_SECONDS,
    )))
}

/// Requeue on an error, naming it.
fn error_policy(dest: Arc<BackupDestination>, err: &ReconcileError, _ctx: Arc<Context>) -> Action {
    warn!(
        destination = %dest.name_any(),
        error = %err,
        "backup destination reconcile failed; requeueing"
    );
    Action::requeue(std::time::Duration::from_secs(ERROR_REQUEUE_SECONDS))
}

/// Run the `BackupDestination` controller until the process ends.
///
/// ALL NAMESPACES (`Api::all`), like every other controller in this directory:
/// a destination is a namespaced object and the controller reconciles every
/// namespace it is granted.
pub fn controller(client: kube::Client) -> impl std::future::Future<Output = ()> + Send {
    let api: Api<BackupDestination> = Api::all(client.clone());
    let ctx = Arc::new(Context {
        client,
        // NO ARCHIVE HANDLE. This reconciler reads no archive: it validates a
        // reference and digests a CA. The `ControllerIdentity` evidence handles
        // live in `crate::evidence_store::StoreCache`, are per-destination, and
        // are built by the paths that VERIFY evidence — never here.
        archive: None,
        // AND NO RUNNER IMAGE. It creates no Job at all; see
        // `super::Context::runner_image` for why the default here means
        // "unused" and never "no override is configured".
        runner_image: crate::job::RunnerImage::default(),
    });
    async move {
        Controller::new(api, watcher::Config::default())
            .run(reconcile, error_policy, ctx)
            .for_each(|_| std::future::ready(()))
            .await;
    }
}
