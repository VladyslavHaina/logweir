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
//! `metadata.uid` through its controller `ownerReference`.
//!
//! # WHAT THE OWNER CHECK ACTUALLY ESTABLISHES, SAID EXACTLY
//!
//! `ownerReferences` is **ordinary metadata written by whoever creates the
//! pod**. The API server does not check that the named owner exists, that the
//! UID is right, or that the creator is entitled to claim it;
//! `OwnerReferencesPermissionEnforcement` is not in the default admission
//! chain, and where it is enabled it checks `delete` on the owner, never the
//! UID. `pod.metadata.uid` is unforgeable; `ownerReferences[].uid` is not.
//!
//! What the check buys is therefore a **raised bar, not a proof**: from
//! "anybody who can create a pod in this namespace" to "anybody who can create
//! a pod **and** read the Job's `metadata.uid`". That is a real reduction —
//! the label is guessable from the object's name, the UID is not — and it is
//! the whole of it.
//!
//! Which is why ambiguity is refused rather than ranked. `backoffLimit: 0`
//! plus `restartPolicy: Never` ([`crate::job`]) means the job controller
//! CANNOT produce two pods for one Job, so a second claimant is illegitimate
//! by construction — and a forged one is by construction the newer, so a
//! newest-wins tie-break would decide every contest in the planter's favour.
//! [`claimants`] therefore reads NOTHING when more than one pod claims the
//! Job, and the reconcilers turn that into a named terminal state. The
//! operator-side control that closes the residual is in `docs/kubernetes.md`
//! §10: do not grant pod-create in a namespace where runs execute.
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

/// The `apiVersion` a `batch/v1` Job's owner reference carries.
///
/// `Job` IS NOT A `batch/v1`-EXCLUSIVE KIND. `volcano.sh/v1alpha1`,
/// `kubeflow.org/v1` and others ship a `Job` too, and a bare `kind == "Job"`
/// test would adopt a pod controlled by one of them on a UID collision. The
/// group costs one string comparison, so there is no reason to leave the
/// `kind` half of the check narrower than the object it names.
pub const JOB_API_VERSION: &str = "batch/v1";

/// Whether this pod's **controller** owner reference is the Job with this UID.
///
/// All four conditions, and each one matters:
///
/// * `api_version == "batch/v1"` — see [`JOB_API_VERSION`];
/// * `kind == "Job"` — a pod owned by a `ReplicaSet` that happens to share a
///   UID string is not this Job's pod;
/// * `uid == job_uid` — the API-server-minted identity of the JOB, not the
///   name, which a deleted-and-recreated Job reuses;
/// * `controller == Some(true)` — a pod may carry several owner references and
///   only one of them is the controller. A non-controller reference is an
///   association somebody else made, and adopting on it is how a pod with an
///   added `ownerReferences` entry gets its stdout read.
///
/// **This is a narrowing, not a proof.** The whole reference is written by
/// whoever created the pod; see the module header for what the check does and
/// does not establish, and [`claimants`] for why a second claimant is refused
/// rather than ranked.
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
/// PURE, and returning BOTH halves rather than filtering: a foreign pod that
/// carries the Job's label is a fact worth logging as
/// [`CheckCode::ForeignPodIgnored`], and a function that silently dropped it
/// would leave the operator of a namespace where that happened with nothing to
/// look at.
#[must_use]
pub fn partition_by_owner<'a>(pods: &'a [Pod], job_uid: &str) -> (Vec<&'a Pod>, Vec<&'a Pod>) {
    pods.iter().partition(|p| is_owned_by_job(p, job_uid))
}

/// The newest of a set of pods, by a **total** order.
///
/// # THIS IS NOT HOW A CONTESTED JOB IS RESOLVED, AND IT USED TO BE
///
/// An earlier version of this module handed a multi-claimant listing to this
/// function. That is a security bug and the review found it (**R1**): an
/// `ownerReference` is author-written, so a tenant who can read the Job's UID
/// can mint a second "owned" pod, and the forged one is **by construction the
/// newer** — newest-wins decides every contest for the planter, silently, with
/// no `ForeignPodIgnored` line because the pod passed the check. With
/// `backoffLimit: 0` a second claimant cannot be legitimate, so [`claimants`]
/// refuses instead of ranking.
///
/// It stays, exercised and documented, for a caller whose Job genuinely can
/// own several pods (a `parallelism > 1` or `backoffLimit > 0` Job this
/// repository does not create today). Such a caller gets an answer that does
/// not depend on the order the API server returned the listing in — a
/// selection that varied between two reconciles over one cluster state would
/// write two different exit codes onto one object.
///
/// The order: **newest by `metadata.creationTimestamp`**, then the lexically
/// greatest `metadata.name`, because a `Time` is second-granular and a tie is
/// therefore not exotic. A pod with no creation timestamp sorts below every
/// pod that has one — the API server always sets it, so its absence means a
/// fabricated object, and a fabricated object does not win a tie-break.
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
/// Three outcomes, and they are NOT two: "nobody claims this Job" and "several
/// do" are different facts about a namespace and the reconcilers report them
/// differently — the first is an ordinary `NoExitCode`, the second is
/// `PodOwnershipContested` and something an operator has to look at.
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
/// PURE, and returning all three groups rather than an `Option`: every pod
/// that wore the label and was not read is a fact worth logging as
/// [`CheckCode::ForeignPodIgnored`], and a function that silently dropped them
/// would leave the operator of the namespace where it happened with nothing to
/// look at — which is how `SEC-PODLOG` stayed invisible.
///
/// # FAIL CLOSED ON AMBIGUITY
///
/// `> 1` claimant ⇒ `owned: None`. A Job built by [`crate::job::build`] pins
/// `backoffLimit: 0` and `restartPolicy: Never`, so the job controller cannot
/// produce a second pod for it; a second claimant is therefore either a forged
/// `ownerReferences` entry or a cluster state this code has never been
/// designed against, and neither is something to read a run's exit code out
/// of. See [`newest`] for the ranking this deliberately does not do.
///
/// A pod that FAILS [`is_owned_by_job`] is not a claimant and cannot contest
/// the Job — otherwise anyone able to set the label could shut every run in
/// the namespace down.
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
/// The check framework has no per-kind terminal-state vocabulary to put a
/// contested Job into, so it collapses [`FoundPod::contested`] into `None` —
/// the same fail-closed answer, minus the named condition the three execution
/// reconcilers write. The claimants are still logged.
///
/// # Errors
///
/// [`kube::Error`] from the `list`. A pod that is present but not owned is not
/// an error: it is `Ok(None)` plus a logged [`CheckCode::ForeignPodIgnored`],
/// because "the Job has not produced its pod yet", "somebody else's pod wears
/// this label" and "two pods claim this Job" are all states a reconciler
/// continues from.
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
/// A struct rather than an enum so `Pod`'s size does not have to be boxed, and
/// so a caller that only wants the pod can take `.pod` without a `match` that
/// would silently keep compiling after a new variant was added.
#[derive(Debug, Default, Clone, PartialEq)]
pub struct FoundPod {
    /// The pod to read, `Some` only when exactly one pod claimed the Job.
    pub pod: Option<Pod>,
    /// The NAMES of the pods that claimed the Job when more than one did,
    /// in listing order. Non-empty means **refuse**: `pod` is `None`, nothing
    /// may be read, and the caller writes a named terminal state rather than
    /// the ordinary "no pod yet".
    pub contested: Vec<String>,
}

/// [`find_owned_pod`] with the narrowing selectors as a parameter.
///
/// The selectors are tried **in order and only until one of them settles the
/// question**: a listing that answers with nothing, or with nothing owned,
/// falls through to the next; a listing with one claimant or several stops
/// there, because a second selector over the same namespace cannot un-contest
/// a contested Job. That is what lets the execution controllers pass
/// `[batch.kubernetes.io/job-name=<job>, job-name=<job>]` and the check
/// framework pass the prefixed one alone, off one selection rule.
///
/// `job_uid` is an `Option` because a caller holds a `Job` it read from the API
/// server and `metadata.uid` is optional in the type. `None` is **not** a
/// licence to fall back to the label: nothing can be proved to belong to a Job
/// with no identity, so nothing is adopted and nothing is even listed.
///
/// # Errors
///
/// [`kube::Error`] from any of the `list` calls. Unowned candidates are not an
/// error — they are an empty `pod` and a logged [`CheckCode::ForeignPodIgnored`]
/// per distinct pod name, once across all selectors rather than once per
/// listing, because on a 1.29 cluster both labels are set and the same
/// impostor would otherwise be reported twice.
pub async fn find_owned_pod_by_selectors(
    client: &kube::Client,
    namespace: &str,
    job_name: &str,
    job_uid: Option<&str>,
    selectors: &[String],
) -> Result<FoundPod, kube::Error> {
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
            // EVERY CLAIMANT IS REPORTED, including the one a newest-wins
            // tie-break would have chosen: which of them is the real runner
            // pod is exactly what this controller cannot tell, and naming one
            // would suggest otherwise.
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
