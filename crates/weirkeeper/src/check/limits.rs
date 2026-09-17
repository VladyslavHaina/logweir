//! Check concurrency — D2 §4.3's `limits.rs` row and §4.4's `checks` block.
//!
//! # Why a check has a limit at all
//!
//! Every check is a pod that dials somebody's broker and somebody's object
//! store with a projected credential. An interactive surface that creates one
//! per click, with no ceiling, is a way to exhaust a namespace's quota, a
//! node's capacity, or a broker's connection budget from a browser. The
//! ceilings are the administrator's ([`super::policy::ChecksPolicy`]), not a
//! tenant's.
//!
//! # What "active" means, and why it is counted from Jobs
//!
//! A Job with no `Complete` and no `Failed` condition. Not a custom resource
//! phase: a phase is this controller's own writing and would count a check
//! whose status patch failed as finished. The Job is the API server's record of
//! the same fact, and it is the object the quota is actually spent on.
//!
//! They are found by the label [`super::job::LABEL_COMPONENT`] =
//! [`super::job::COMPONENT_CHECK`], which [`super::job::labels`] writes on
//! every check Job. **A label is the right tool HERE and the wrong tool for pod
//! identity** (see [`super::pod`]): counting is an approximation whose worst
//! outcome is a queued check, while reading a pod's stdout is a trust decision
//! whose worst outcome is publishing somebody else's output.
//!
//! # The overshoot is accepted and written down
//!
//! Two reconciles that count concurrently can both admit. D2 §4.3 accepts that
//! explicitly: the alternative is a lease or a lock in the control plane for a
//! bound whose purpose is to be approximately right. A small overshoot costs a
//! pod; a lock costs a liveness failure mode.

use k8s_openapi::api::batch::v1::Job;
use kube::api::{Api, ListParams};
use kube::ResourceExt as _;

use logweir_core::check_contract::{CheckCode, CheckPlanKind};

use super::policy::ChecksPolicy;

/// How long a queued check waits before it is looked at again — D2 §4.3.
pub const QUEUED_REQUEUE_SECS: u64 = 10;

/// The selector every active-check count is taken with.
#[must_use]
pub fn check_selector() -> String {
    format!(
        "{}={}",
        super::job::LABEL_COMPONENT,
        super::job::COMPONENT_CHECK
    )
}

/// The selector that finds a `Backup`'s OWN per-run topic discovery Jobs
/// (PLAT-09.2).
///
/// # Why they are a second listing and not a second label on the first
///
/// A run's discovery deliberately does not wear
/// [`super::job::LABEL_COMPONENT`]`=`[`super::job::COMPONENT_CHECK`]: nothing
/// admits it ([`admit`] is never called for it — D1 §7.2 defines no admission
/// for work an operator already scheduled), so wearing that label would spend
/// the interactive pool without ever being bounded by it, and a browser click
/// would queue behind a nightly schedule. What it SHOULD spend is the
/// per-connection ceiling, which exists to bound simultaneous connections to
/// one broker — and that is a different question from "how many checks is this
/// installation running".
///
/// A Kubernetes label selector ANDs its terms and cannot express "either key",
/// so the two populations are two listings. [`check_jobs`] deliberately still
/// makes exactly one — see [`run_discovery_jobs`].
#[must_use]
pub fn run_discovery_selector() -> String {
    format!(
        "{}={}",
        crate::controllers::backup_selection::LABEL_PURPOSE,
        crate::controllers::backup_selection::PURPOSE_TOPIC_DISCOVERY
    )
}

/// Whether a check Job is still occupying a slot.
///
/// The INVERSE of [`crate::controllers::backup::job_finished`], and it calls
/// it rather than re-deriving: "finished" is `Complete` or `Failed` with
/// `status: "True"`, and two definitions of that in one crate is how a count
/// and a state machine come to disagree.
#[must_use]
pub fn is_active(job: &Job) -> bool {
    !crate::controllers::backup::job_finished(job)
}

/// What one count found.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ActiveCounts {
    /// Active check Jobs in the candidate's namespace, excluding evidence
    /// fetches.
    pub namespace: u32,
    /// Active check Jobs across the installation, excluding evidence fetches.
    pub total: u32,
    /// Active `topicInventory` Jobs against the candidate's connection UID.
    pub per_connection: u32,
    /// Active `evidenceFetch` Jobs in the candidate's namespace — a SEPARATE
    /// pool, so verification cannot be starved by interactive checks
    /// (D2 §4.3).
    pub evidence_namespace: u32,
}

/// Count the active check Jobs relevant to one candidate — **pure**.
///
/// `connection_uid` is `None` for a kind that has no connection, and then
/// `per_connection` stays zero: the per-connection cap is a discovery rule.
#[must_use]
pub fn count(jobs: &[Job], namespace: &str, connection_uid: Option<&str>) -> ActiveCounts {
    let mut counts = ActiveCounts::default();
    for job in jobs.iter().filter(|j| is_active(j)) {
        let labels = job.labels();
        // A `Backup`'S OWN DISCOVERY SPENDS THE CONNECTION AND NOTHING ELSE.
        // It is not an interactive check: nothing admits it, so counting it
        // against `namespace`/`total` would let a nightly schedule queue a
        // browser click without ever being queued itself. The ceiling it
        // genuinely belongs to is the per-connection one, which bounds
        // simultaneous dials at one broker — and eight dynamic schedules
        // firing at 02:00 against one `KafkaCluster` is exactly what that
        // ceiling is for. See `run_discovery_selector`.
        if labels
            .get(crate::controllers::backup_selection::LABEL_PURPOSE)
            .map(String::as_str)
            == Some(crate::controllers::backup_selection::PURPOSE_TOPIC_DISCOVERY)
        {
            if let Some(uid) = connection_uid {
                if labels
                    .get(super::job::LABEL_CHECK_CONNECTION_UID)
                    .map(String::as_str)
                    == Some(uid)
                {
                    counts.per_connection += 1;
                }
            }
            continue;
        }
        // `CheckPlanKind` has no `parse` — the closed-vocabulary macro is for
        // `CheckCode`/`CheckId` — so the label is matched against `ALL`'s own
        // spellings. A label this build does not recognise is `None`, which
        // counts against the general pools and against no kind-specific one:
        // an unknown check still occupies a slot, and that is the safe
        // direction for a ceiling.
        let kind = labels
            .get(super::job::LABEL_CHECK_KIND)
            .map(String::as_str)
            .and_then(|s| CheckPlanKind::ALL.iter().copied().find(|k| k.as_str() == s));
        let is_evidence = kind == Some(CheckPlanKind::EvidenceFetch);
        let same_namespace = job.namespace().as_deref() == Some(namespace);

        if is_evidence {
            if same_namespace {
                counts.evidence_namespace += 1;
            }
            // An evidence fetch spends only its own pool — that is the whole
            // point of the pool.
            continue;
        }
        counts.total += 1;
        if same_namespace {
            counts.namespace += 1;
        }
        if kind == Some(CheckPlanKind::TopicInventory) {
            if let Some(uid) = connection_uid {
                if labels
                    .get(super::job::LABEL_CHECK_CONNECTION_UID)
                    .map(String::as_str)
                    == Some(uid)
                {
                    counts.per_connection += 1;
                }
            }
        }
    }
    counts
}

/// Whether a candidate check may start now.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Admission {
    /// Create the Job.
    Admit,
    /// Do not. The status is `phase: Queued` with this reason, requeued after
    /// [`QUEUED_REQUEUE_SECS`].
    Queued(CheckCode),
}

impl Admission {
    /// Whether this admits.
    #[must_use]
    pub fn is_admitted(self) -> bool {
        matches!(self, Self::Admit)
    }
}

/// Apply the policy to a count — **pure**, and the ONE place the four ceilings
/// are compared.
///
/// D2 §4.3 gives every over-limit outcome the single reason
/// [`CheckCode::ConcurrencyLimited`]: from a user's side "the installation is
/// busy, this is queued" is one fact, and four reasons would be four strings a
/// UI has to carry remedies for. Which ceiling bit belongs in the condition
/// MESSAGE, which the caller composes from [`ActiveCounts`].
#[must_use]
pub fn admit(counts: &ActiveCounts, policy: &ChecksPolicy, kind: CheckPlanKind) -> Admission {
    if kind == CheckPlanKind::EvidenceFetch {
        // THE SEPARATE POOL, and ONLY that pool: an evidence fetch is how a
        // verification reads what it needs, and making it compete with
        // interactive discovery is how verification stops happening on a busy
        // installation.
        return if counts.evidence_namespace >= policy.max_evidence_fetch_active_per_namespace {
            Admission::Queued(CheckCode::ConcurrencyLimited)
        } else {
            Admission::Admit
        };
    }
    if counts.namespace >= policy.max_active_per_namespace
        || counts.total >= policy.max_active_total
    {
        return Admission::Queued(CheckCode::ConcurrencyLimited);
    }
    if kind == CheckPlanKind::TopicInventory
        && counts.per_connection >= policy.max_active_discoveries_per_connection
    {
        return Admission::Queued(CheckCode::ConcurrencyLimited);
    }
    Admission::Admit
}

/// Every check Job in the installation, active or not.
///
/// `Api::all` plus the component label — the same `list` verb the three
/// existing reconcilers already hold on Jobs. The filtering to "active" is
/// [`is_active`]'s, in memory, because a Job's conditions are not a field
/// selector the API server offers.
///
/// # Errors
///
/// [`kube::Error`] from the `list`.
pub async fn check_jobs(client: &kube::Client) -> Result<Vec<Job>, kube::Error> {
    let jobs: Api<Job> = Api::all(client.clone());
    let list = jobs
        .list(&ListParams::default().labels(&check_selector()))
        .await?;
    Ok(list.items)
}

/// Every per-run topic discovery Job in the installation, active or not —
/// [`run_discovery_selector`]'s population.
///
/// **A SECOND LISTING, AND A SEPARATE FUNCTION ON PURPOSE.** [`check_jobs`]
/// makes exactly one request and `check_framework`'s
/// `the_active_count_lists_by_the_component_label_and_nothing_else` asserts
/// that it does; a label selector cannot express "either key", so folding the
/// two populations into one call is not available. A caller that wants the
/// per-connection ceiling to see a `Backup`'s own discoveries concatenates the
/// two lists before calling [`count`], which already attributes each population
/// correctly.
///
/// # Errors
///
/// [`kube::Error`] from the `list`.
pub async fn run_discovery_jobs(client: &kube::Client) -> Result<Vec<Job>, kube::Error> {
    let jobs: Api<Job> = Api::all(client.clone());
    let list = jobs
        .list(&ListParams::default().labels(&run_discovery_selector()))
        .await?;
    Ok(list.items)
}
