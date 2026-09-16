//! Finding a check's pod — D-SEAMS **S6**, defect `SEC-PODLOG`.
//!
//! # A LABEL IS NOT AN IDENTITY
//!
//! `batch.kubernetes.io/job-name` is set by the Job controller, and it is also
//! settable by anything that can create a pod in the namespace. A controller
//! that reads the exit code and the stdout of "the pod with this label" will
//! read whichever pod a namespace tenant put that label on — and a check's
//! stdout is a document this controller then writes into a custom resource's
//! status and, through it, into the API and the UI.
//!
//! So the label is a **selector** and never a **decision**: the list is
//! narrowed with it, because a Job's pod name is generated and cannot be known
//! in advance, and then every candidate is checked against the Job's own
//! `metadata.uid` through its controller `ownerReference`. A UID is minted by
//! the API server and cannot be forged by a pod author.
//!
//! **There is no legacy-label fallback here.** The execution paths try
//! `batch.kubernetes.io/job-name` and then the unprefixed `job-name`, because
//! they must work on a 1.29 cluster that has not backfilled the prefixed one
//! ([`crate::controllers::backup::pod_selectors`]). New code does not need
//! that: `batch.kubernetes.io/job-name` has been set since 1.27, which is below
//! this project's floor, and a second selector is a second chance for a foreign
//! pod to be considered at all.

use k8s_openapi::api::batch::v1::Job;
use k8s_openapi::api::core::v1::Pod;
use kube::api::{Api, ListParams};
use kube::{Resource, ResourceExt as _};
use tracing::warn;

use logweir_core::check_contract::CheckCode;

use crate::controllers::backup::JOB_NAME_LABEL;

/// The ONE selector a check's pod is listed with.
#[must_use]
pub fn pod_selector(job_name: &str) -> String {
    format!("{JOB_NAME_LABEL}={job_name}")
}

/// Whether this pod's **controller** owner reference is the Job with this UID.
///
/// All three conditions, and each one matters:
///
/// * `kind == "Job"` — a pod owned by a `ReplicaSet` that happens to share a
///   UID string is not this Job's pod;
/// * `uid == job_uid` — the API-server-minted identity, not the name, which a
///   deleted-and-recreated Job reuses;
/// * `controller == Some(true)` — a pod may carry several owner references and
///   only one of them is the controller. A non-controller reference is an
///   association somebody else made, and adopting on it is how a pod with an
///   added `ownerReferences` entry gets its stdout read.
#[must_use]
pub fn is_owned_by_job(pod: &Pod, job_uid: &str) -> bool {
    pod.meta()
        .owner_references
        .as_deref()
        .unwrap_or_default()
        .iter()
        .any(|o| o.kind == "Job" && o.uid == job_uid && o.controller == Some(true))
}

/// Split a listing into the pods this Job owns and the pods it does not.
///
/// PURE, and returning BOTH halves rather than filtering: a foreign pod that
/// carries the Job's label is a fact worth logging as
/// [`CheckCode::ForeignPodIgnored`], and a function that silently dropped it
/// would leave the operator of a namespace where that happened with nothing to
/// look at.
#[must_use]
pub fn partition_by_owner<'a>(pods: &'a [Pod], job_uid: &str) -> (Vec<&'a Pod>, Vec<&'a Pod>) {
    pods.iter().partition(|p| is_owned_by_job(p, job_uid))
}

/// The pod a check Job produced, or `None`.
///
/// # Errors
///
/// [`kube::Error`] from the `list`. A pod that is present but not owned is not
/// an error: it is `Ok(None)` plus a logged [`CheckCode::ForeignPodIgnored`],
/// because "the Job has not produced its pod yet" and "somebody else's pod
/// wears this label" are both states a reconciler continues from.
pub async fn find_owned_pod(
    client: &kube::Client,
    namespace: &str,
    job: &Job,
) -> Result<Option<Pod>, kube::Error> {
    let Some(job_uid) = job.uid() else {
        // A Job with no UID did not come from the API server. Nothing can be
        // proved to belong to it, so nothing is adopted.
        warn!(
            job = %job.name_any(),
            namespace = %namespace,
            "the check Job carries no metadata.uid, so no pod can be proved to be its own; \
             none is adopted"
        );
        return Ok(None);
    };
    let job_name = job.name_any();
    let pods: Api<Pod> = Api::namespaced(client.clone(), namespace);
    let list = pods
        .list(&ListParams::default().labels(&pod_selector(&job_name)))
        .await?;
    let (owned, foreign) = partition_by_owner(&list.items, &job_uid);
    for p in &foreign {
        warn!(
            job = %job_name,
            namespace = %namespace,
            pod = %p.name_any(),
            code = CheckCode::ForeignPodIgnored.as_str(),
            "a pod carries this check Job's name label but is not owned by it; its output is \
             not read. A label is writable by anything that can create a pod; a controller \
             ownerReference UID is not"
        );
    }
    Ok(owned.into_iter().next().cloned())
}
