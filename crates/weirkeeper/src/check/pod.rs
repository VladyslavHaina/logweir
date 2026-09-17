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
//!
//! # ONE IMPLEMENTATION, FOUR CALLERS (defect `SEC-PODLOG`)
//!
//! The three execution controllers — `Backup`, `Restore` and `KafkaCluster` —
//! used to select their run's pod by label alone and take the first result, or
//! (on the `Backup` side) fall back to the first pod with no Job owner at all.
//! Both shapes read a stranger's log. They now call
//! [`find_owned_pod_by_selectors`], which is this module's [`find_owned_pod`]
//! with the selector list as a parameter, so the *selection rule* lives in
//! exactly one function and only the *selectors* differ between the check
//! framework (one, prefixed) and the execution paths (two, prefixed then
//! legacy). A fifth caller that wants a different narrowing passes a different
//! selector list; it does not get a different notion of ownership.

use std::collections::BTreeSet;

use k8s_openapi::api::batch::v1::Job;
use k8s_openapi::api::core::v1::Pod;
use kube::api::{Api, ListParams};
use kube::{Resource, ResourceExt as _};
use tracing::{debug, warn};

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

/// The newest of a set of pods, **deterministically**.
///
/// `backoffLimit: 0` plus `restartPolicy: Never` yields exactly one pod per
/// Job, and that is the case every caller here is in. But "exactly one" is a
/// property of the job controller, not of the listing: a Job whose pod was
/// evicted and replaced, or one observed mid-replacement, can legitimately show
/// two pods it owns, and the answer must not depend on the order the API server
/// happened to return them in — a selection that varies between two reconciles
/// over the same cluster state writes two different exit codes onto the same
/// object.
///
/// So: the **newest by `metadata.creationTimestamp`**, and the lexically
/// greatest `metadata.name` among pods created in the same second, because a
/// `Time` is second-granular and a tie is therefore not exotic. Newest rather
/// than oldest because a replacement pod is the run's current attempt, and the
/// stale one is what the operator is *not* asking about.
///
/// A pod with no creation timestamp sorts below every pod that has one: the
/// API server always sets it, so its absence means a fabricated object, and a
/// fabricated object does not win a tie-break.
#[must_use]
pub fn newest<'a>(pods: &[&'a Pod]) -> Option<&'a Pod> {
    pods.iter().copied().max_by(|a, b| {
        a.creation_timestamp()
            .cmp(&b.creation_timestamp())
            .then_with(|| a.name_any().cmp(&b.name_any()))
    })
}

/// The pod of the Job with this UID out of one listing, and the ones ignored.
///
/// PURE. The second half of the pair is every candidate that wore the label
/// and failed [`is_owned_by_job`]; the caller logs it, because a foreign pod
/// carrying a run's job-name label is the thing `SEC-PODLOG` is about and
/// silence is what made it invisible.
#[must_use]
pub fn choose_owned<'a>(pods: &'a [Pod], job_uid: &str) -> (Option<&'a Pod>, Vec<&'a Pod>) {
    let (owned, foreign) = partition_by_owner(pods, job_uid);
    if owned.len() > 1 {
        warn!(
            owned = owned.len(),
            "this Job owns more than one pod; the newest by creationTimestamp is the one read"
        );
    }
    (newest(&owned), foreign)
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
    let job_name = job.name_any();
    find_owned_pod_by_selectors(
        client,
        namespace,
        &job_name,
        job.uid().as_deref(),
        &[pod_selector(&job_name)],
    )
    .await
}

/// [`find_owned_pod`] with the narrowing selectors as a parameter.
///
/// The selectors are tried **in order and only until one of them yields a pod
/// this Job owns**: a listing that answers with nothing, or with nothing owned,
/// falls through to the next. That is what lets the execution controllers pass
/// `[batch.kubernetes.io/job-name=<job>, job-name=<job>]` and the check
/// framework pass the prefixed one alone, off one selection rule.
///
/// `job_uid` is an `Option` because a caller holds a `Job` it read from the API
/// server and `metadata.uid` is optional in the type. `None` is **not** a
/// licence to fall back to the label: nothing can be proved to belong to a Job
/// with no identity, so nothing is adopted.
///
/// # Errors
///
/// [`kube::Error`] from any of the `list` calls. Unowned candidates are not an
/// error — they are `Ok(None)` and a logged [`CheckCode::ForeignPodIgnored`]
/// per distinct pod name, once across all selectors rather than once per
/// listing, because on a 1.29 cluster both labels are set and the same
/// impostor would otherwise be reported twice.
pub async fn find_owned_pod_by_selectors(
    client: &kube::Client,
    namespace: &str,
    job_name: &str,
    job_uid: Option<&str>,
    selectors: &[String],
) -> Result<Option<Pod>, kube::Error> {
    let Some(job_uid) = job_uid else {
        // A Job with no UID did not come from the API server. Nothing can be
        // proved to belong to it, so nothing is adopted — and the pod list is
        // not even read, because there is no question a listing could answer.
        warn!(
            job = %job_name,
            namespace = %namespace,
            "the Job carries no metadata.uid, so no pod can be proved to be its own; none is \
             adopted and no pod is listed"
        );
        return Ok(None);
    };
    let pods: Api<Pod> = Api::namespaced(client.clone(), namespace);
    let mut ignored: BTreeSet<String> = BTreeSet::new();
    let mut found: Option<Pod> = None;
    for selector in selectors {
        let list = pods
            .list(&ListParams::default().labels(selector))
            .await?
            .items;
        let (owned, foreign) = choose_owned(&list, job_uid);
        ignored.extend(foreign.iter().map(|p| p.name_any()));
        if let Some(pod) = owned {
            found = Some(pod.clone());
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
            "a pod carries this Job's name label but is not owned by it; its exit code and its \
             log are not read. A label is writable by anything that can create a pod; a \
             controller ownerReference UID is not"
        );
    }
    Ok(found)
}
