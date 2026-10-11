//! Finding a check's pod (seam **S6**).
//!
//! # A label is not an identity
//!
//! `batch.kubernetes.io/job-name` is set by the Job controller and also by
//! anything that can create a pod in the namespace, and a check's stdout is a
//! document this controller writes into a status and, through it, into the API
//! and the UI. So the label is a **selector** and never a **decision**: it
//! narrows the list (a Job's pod name cannot be known in advance), and every
//! candidate is then checked against the Job's own `metadata.uid` through its
//! controller `ownerReference`.
//!
//! # What the owner check establishes
//!
//! `ownerReferences` is **ordinary metadata written by whoever creates the
//! pod**. The API server does not check that the named owner exists, that the
//! UID is right, or that the creator is entitled to claim it;
//! `OwnerReferencesPermissionEnforcement` is not in the default admission
//! chain, and where it is enabled it checks `delete` on the owner, never the
//! UID. `pod.metadata.uid` is unforgeable; `ownerReferences[].uid` is not.
//!
//! The check is therefore a **raised bar, not a proof**: from "anybody who can
//! create a pod in this namespace" to "anybody who can create a pod **and**
//! read the Job's `metadata.uid`" (the label is guessable from the name, the
//! UID is not).
//!
//! So ambiguity is refused rather than ranked. `backoffLimit: 0` plus
//! `restartPolicy: Never` ([`crate::job`]) means the job controller cannot
//! produce two pods for one Job, so a second claimant is illegitimate, and a
//! forged one is by construction the newer: a newest-wins tie-break would hand
//! every contest to the planter. [`claimants`] reads nothing when more than one
//! pod claims the Job, and the reconcilers turn that into a named terminal
//! state. The operator-side control for the residual is in `docs/kubernetes.md`
//! §10: do not grant pod-create in a namespace where runs execute.
//!
//! **No legacy-label fallback here.** The execution paths also try the
//! unprefixed `job-name` for a 1.29 cluster that has not backfilled the
//! prefixed one ([`crate::controllers::backup::pod_selectors`]). The check
//! framework does not need it (`batch.kubernetes.io/job-name` is set since
//! 1.27, below this project's floor), and a second selector is a second chance
//! for a foreign pod to be considered.
//!
//! # One selection rule
//!
//! The execution controllers (`Backup`, `Restore`, `KafkaCluster`) and the
//! check framework all call [`find_owned_pod_by_selectors`], so the selection
//! rule lives in one function and only the selectors differ. A caller that
//! wants a different narrowing passes a different selector list; it does not
//! get a different notion of ownership.

use std::collections::BTreeSet;

use k8s_openapi::api::batch::v1::Job;
use k8s_openapi::api::core::v1::Pod;
use kube::api::{Api, ListParams};
use kube::{Resource, ResourceExt as _};
use tracing::{debug, warn};

use logweir_core::check_contract::CheckCode;

use crate::controllers::backup::JOB_NAME_LABEL;

/// The one selector a check's pod is listed with.
#[must_use]
pub fn pod_selector(job_name: &str) -> String {
    format!("{JOB_NAME_LABEL}={job_name}")
}

/// The `apiVersion` a `batch/v1` Job's owner reference carries.
///
/// `Job` is not a `batch/v1`-exclusive kind (`volcano.sh/v1alpha1`,
/// `kubeflow.org/v1` and others ship one), and a bare `kind == "Job"` test
/// would adopt a pod controlled by one of them on a UID collision.
pub const JOB_API_VERSION: &str = "batch/v1";

/// Whether this pod's **controller** owner reference is the Job with this UID.
///
/// All four conditions matter:
///
/// * `api_version == "batch/v1"`: see [`JOB_API_VERSION`];
/// * `kind == "Job"`: a pod owned by a `ReplicaSet` that happens to share a
///   UID string is not this Job's pod;
/// * `uid == job_uid`: the Job's API-server-minted identity, not the name,
///   which a deleted-and-recreated Job reuses;
/// * `controller == Some(true)`: a non-controller reference is an association
///   somebody else made, and adopting on it is how a pod with an added
///   `ownerReferences` entry gets its stdout read.
///
/// **A narrowing, not a proof**: the whole reference is written by whoever
/// created the pod (see the module header, and [`claimants`] for why a second
/// claimant is refused rather than ranked).
#[must_use]
pub fn is_owned_by_job(pod: &Pod, job_uid: &str) -> bool {
    pod.meta()
        .owner_references
        .as_deref()
        .unwrap_or_default()
        .iter()
        .any(|o| {
            o.api_version == JOB_API_VERSION
                && o.kind == "Job"
                && o.uid == job_uid
                && o.controller == Some(true)
        })
}

/// Split a listing into the pods this Job owns and the pods it does not.
///
/// Pure, and returning both halves rather than filtering: a foreign pod that
/// carries the Job's label is worth logging as [`CheckCode::ForeignPodIgnored`].
#[must_use]
pub fn partition_by_owner<'a>(pods: &'a [Pod], job_uid: &str) -> (Vec<&'a Pod>, Vec<&'a Pod>) {
    pods.iter().partition(|p| is_owned_by_job(p, job_uid))
}

/// The newest of a set of pods, by a **total** order.
///
/// **Not how a contested Job is resolved.** An `ownerReference` is
/// author-written, so a forged second claimant is by construction the newer,
/// and newest-wins would hand it the contest silently; [`claimants`] refuses
/// instead. This stays for a caller whose Job can own several pods
/// (`parallelism > 1` or `backoffLimit > 0`, which this repository does not
/// create), so that its answer does not depend on the listing order: two
/// reconciles over one cluster state must not write two exit codes.
///
/// The order: newest `metadata.creationTimestamp`, then the lexically greatest
/// `metadata.name` (a `Time` is second-granular, so ties happen). A pod with no
/// creation timestamp sorts below every pod that has one: the API server always
/// sets it, so its absence means a fabricated object.
#[must_use]
pub fn newest<'a>(pods: &[&'a Pod]) -> Option<&'a Pod> {
    pods.iter().copied().max_by(|a, b| {
        a.creation_timestamp()
            .cmp(&b.creation_timestamp())
            .then_with(|| a.name_any().cmp(&b.name_any()))
    })
}

/// What one listing said about a Job's pod.
///
/// "Nobody claims this Job" and "several do" are different facts: the first is
/// an ordinary `NoExitCode`, the second is `PodOwnershipContested` and needs an
/// operator.
#[derive(Debug, Default)]
pub struct Claimants<'a> {
    /// The pod to read: `Some` **only** when exactly one pod claimed the Job.
    pub owned: Option<&'a Pod>,
    /// Every pod that claimed the Job when more than one did, `owned` being
    /// `None` in that case. Empty otherwise.
    pub contested: Vec<&'a Pod>,
    /// Candidates that wore the label and claimed some other Job, or nothing.
    pub foreign: Vec<&'a Pod>,
}

/// Split one listing into the Job's pod, the rival claimants, and the rest.
///
/// Pure, and returning all three groups: every pod that wore the label and was
/// not read is worth logging as [`CheckCode::ForeignPodIgnored`].
///
/// # Fail closed on ambiguity
///
/// More than one claimant means `owned: None`. A Job built by
/// [`crate::job::build`] pins `backoffLimit: 0` and `restartPolicy: Never`, so
/// a second claimant is a forged `ownerReferences` entry or a state this code
/// was not designed for, and neither is something to read an exit code out of.
///
/// A pod that fails [`is_owned_by_job`] is not a claimant and cannot contest
/// the Job; otherwise anyone able to set the label could stop every run in the
/// namespace.
#[must_use]
pub fn claimants<'a>(pods: &'a [Pod], job_uid: &str) -> Claimants<'a> {
    let (owned, foreign) = partition_by_owner(pods, job_uid);
    match owned.len() {
        0 => Claimants {
            owned: None,
            contested: Vec::new(),
            foreign,
        },
        1 => Claimants {
            owned: owned.into_iter().next(),
            contested: Vec::new(),
            foreign,
        },
        _ => Claimants {
            owned: None,
            contested: owned,
            foreign,
        },
    }
}

/// The pod a check Job produced, or `None`.
///
/// The check framework has no per-kind terminal state for a contested Job, so
/// it collapses [`FoundPod::contested`] into `None` (still fail-closed) and
/// logs the claimants.
///
/// # Errors
///
/// [`kube::Error`] from the `list`. A present but unowned pod is `Ok(None)`
/// plus a logged [`CheckCode::ForeignPodIgnored`]: "not produced yet",
/// "somebody else's pod wears this label" and "two pods claim this Job" are
/// all states a reconciler continues from.
pub async fn find_owned_pod(
    client: &kube::Client,
    namespace: &str,
    job: &Job,
) -> Result<Option<Pod>, kube::Error> {
    let job_name = job.name_any();
    Ok(find_owned_pod_by_selectors(
        client,
        namespace,
        &job_name,
        job.uid().as_deref(),
        &[pod_selector(&job_name)],
    )
    .await?
    .pod)
}

/// What [`find_owned_pod_by_selectors`] found.
///
/// A struct rather than an enum so `Pod` need not be boxed and a caller can
/// take `.pod` without a `match`.
#[derive(Debug, Default, Clone, PartialEq)]
pub struct FoundPod {
    /// The pod to read, `Some` only when exactly one pod claimed the Job.
    pub pod: Option<Pod>,
    /// The names of the pods that claimed the Job when more than one did, in
    /// listing order. Non-empty means **refuse**: `pod` is `None`, nothing may
    /// be read, and the caller writes a named terminal state.
    pub contested: Vec<String>,
}

/// [`find_owned_pod`] with the narrowing selectors as a parameter.
///
/// The selectors are tried in order until one settles the question: a listing
/// with nothing owned falls through to the next; one with one claimant or
/// several stops there, because a second selector cannot un-contest a Job.
///
/// `job_uid` is an `Option` because `metadata.uid` is optional in the type.
/// `None` is **not** a licence to fall back to the label: nothing can be proved
/// to belong to a Job with no identity, so nothing is listed or adopted.
///
/// # Errors
///
/// [`kube::Error`] from any `list`. Unowned candidates are an empty `pod` and
/// one logged [`CheckCode::ForeignPodIgnored`] per distinct pod name across all
/// selectors (on a 1.29 cluster both labels are set, and one impostor would
/// otherwise be reported twice).
pub async fn find_owned_pod_by_selectors(
    client: &kube::Client,
    namespace: &str,
    job_name: &str,
    job_uid: Option<&str>,
    selectors: &[String],
) -> Result<FoundPod, kube::Error> {
    let Some(job_uid) = job_uid else {
        // A Job with no UID did not come from the API server: nothing can be
        // proved to belong to it, so the pod list is not even read.
        warn!(
            job = %job_name,
            namespace = %namespace,
            "the Job carries no metadata.uid, so no pod can be proved to be its own; none is \
             adopted and no pod is listed"
        );
        return Ok(FoundPod::default());
    };
    let pods: Api<Pod> = Api::namespaced(client.clone(), namespace);
    let mut ignored: BTreeSet<String> = BTreeSet::new();
    let mut found = FoundPod::default();
    for selector in selectors {
        let list = pods
            .list(&ListParams::default().labels(selector))
            .await?
            .items;
        let seen = claimants(&list, job_uid);
        ignored.extend(seen.foreign.iter().map(|p| p.name_any()));
        if !seen.contested.is_empty() {
            let names: Vec<String> = seen.contested.iter().map(|p| p.name_any()).collect();
            // Every claimant is reported: which one is the real runner pod is
            // exactly what this controller cannot tell.
            ignored.extend(names.iter().cloned());
            warn!(
                job = %job_name,
                namespace = %namespace,
                selector = %selector,
                pods = %names.join(","),
                claimant_count = names.len(),
                code = CheckCode::ForeignPodIgnored.as_str(),
                "more than one pod claims this Job as its controller owner. The Job pins \
                 backoffLimit: 0 and restartPolicy: Never, so it cannot have produced two — an \
                 ownerReference is author-written and at least one of these was minted by \
                 somebody who read the Job's UID. NO log and NO exit code is read from any of them"
            );
            found.pod = None;
            found.contested = names;
            break;
        }
        if let Some(pod) = seen.owned {
            found.pod = Some(pod.clone());
            break;
        }
        debug!(
            job = %job_name,
            namespace = %namespace,
            selector = %selector,
            "no pod owned by this Job matched this selector; trying the next"
        );
    }
    for pod in &ignored {
        warn!(
            job = %job_name,
            namespace = %namespace,
            pod = %pod,
            code = CheckCode::ForeignPodIgnored.as_str(),
            "a pod carries this Job's name label but is not the one pod this Job owns; its exit \
             code and its log are not read"
        );
    }
    Ok(found)
}
