//! Check concurrency (D2 §4.3's `limits.rs` row and §4.4's `checks` block).
//!
//! Every check is a pod that dials somebody's broker and object store with a
//! projected credential; with no ceiling, an interactive surface could exhaust
//! a namespace's quota, a node or a broker's connection budget from a browser.
//! The ceilings are the administrator's ([`super::policy::ChecksPolicy`]).
//!
//! # What "active" means, and why it is counted from Jobs
//!
//! A Job with no `Complete` and no `Failed` condition, not a custom resource
//! phase: a phase is this controller's own writing and would count a check
//! whose status patch failed as finished, while the Job is the API server's
//! record and the object the quota is spent on.
//!
//! They are found by [`super::job::LABEL_COMPONENT`], which a `Backup`'s own
//! per-run topic discovery Job wears with the other value (`run-discovery`),
//! so one [`active_selector`] request finds both populations and [`count`]
//! attributes each: an interactive check spends the namespace and installation
//! pools, a run's discovery only the per-connection ceiling. **A label is the
//! right tool here and the wrong one for pod identity** ([`super::pod`]): a
//! miscount's worst outcome is a queued check, a misread pod's is publishing
//! somebody else's output.
//!
//! # The overshoot is accepted
//!
//! Two reconciles counting concurrently can both admit. D2 §4.3 accepts that:
//! the alternative is a lock in the control plane for a bound meant to be
//! approximately right, and an overshoot costs a pod where a lock costs a
//! liveness failure mode.

use k8s_openapi::api::batch::v1::Job;
use kube::api::ListParams;
use kube::ResourceExt as _;

use logweir_core::check_contract::{CheckCode, CheckPlanKind};

use super::policy::ChecksPolicy;

/// How long a queued check waits before it is looked at again (D2 §4.3).
pub const QUEUED_REQUEUE_SECS: u64 = 10;

/// The interactive pool's membership: `app.kubernetes.io/component=check`,
/// which the namespace and installation ceilings are about. [`check_jobs`]
/// lists with [`active_selector`] instead, which also finds a `Backup`'s
/// per-run discoveries.
#[must_use]
pub fn check_selector() -> String {
    format!(
        "{}={}",
        super::job::LABEL_COMPONENT,
        super::job::COMPONENT_CHECK
    )
}

/// The selector [`check_jobs`] lists with: **both populations, one request**.
///
/// `app.kubernetes.io/component in (check, run-discovery)`, set-based: a label
/// selector ANDs its terms and cannot express "either key", so the two
/// populations share one key with two values, and [`count`] tells them apart
/// from the label it already has.
///
/// # Why they are not both `check`
///
/// Nothing calls [`admit`] for a run's discovery (D1 §7.2 defines no admission
/// for work an operator already scheduled, and a run waiting on a
/// browser-driven ceiling would miss its slot). A Job counted against a pool
/// that never admits it spends somebody else's budget for free, so a run's
/// discovery spends only the per-connection ceiling, which bounds simultaneous
/// dials at a broker.
#[must_use]
pub fn active_selector() -> String {
    format!(
        "{} in ({},{})",
        super::job::LABEL_COMPONENT,
        super::job::COMPONENT_CHECK,
        crate::controllers::backup_selection::COMPONENT_RUN_DISCOVERY
    )
}

/// Whether a check Job is still occupying a slot.
///
/// The inverse of [`crate::controllers::backup::job_finished`], calling it
/// rather than re-deriving: two definitions of "finished" in one crate is how a
/// count and a state machine come to disagree.
#[must_use]
pub fn is_active(job: &Job) -> bool {
    !crate::controllers::backup::job_finished(job)
}

/// What one count found.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ActiveCounts {
    /// Active check Jobs in the candidate's workspace, excluding evidence
    /// fetches.
    pub workspace: u32,
    /// Active check Jobs across the installation, excluding evidence fetches.
    pub total: u32,
    /// Active `topicInventory` Jobs against the candidate's connection UID.
    pub per_connection: u32,
    /// Active `evidenceFetch` Jobs in the candidate's workspace: a separate
    /// pool, so verification cannot be starved by interactive checks
    /// (D2 §4.3).
    pub evidence_workspace: u32,
}

/// Count the active check Jobs relevant to one candidate — **pure**.
///
/// `connection_uid` is `None` for a kind that has no connection, and then
/// `per_connection` stays zero: the per-connection cap is a discovery rule.
#[must_use]
pub fn count(jobs: &[Job], workspace: &str, connection_uid: Option<&str>) -> ActiveCounts {
    let mut counts = ActiveCounts::default();
    for job in jobs.iter().filter(|j| is_active(j)) {
        let labels = job.labels();
        // A `Backup`'s own discovery spends the connection ceiling and nothing
        // else: nothing admits it, so counting it against the interactive pools
        // would let a nightly schedule queue a browser click without ever being
        // queued itself. See `active_selector`. Told apart by the same key the
        // listing selects on, so the listed and attributed populations cannot
        // drift; `logweir.dev/purpose` stays on the Job for `kubectl`.
        if labels.get(super::job::LABEL_COMPONENT).map(String::as_str)
            == Some(crate::controllers::backup_selection::COMPONENT_RUN_DISCOVERY)
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
        // `CheckPlanKind` has no `parse`, so the label is matched against
        // `ALL`'s spellings. An unrecognised label is `None`, which counts
        // against the general pools and no kind-specific one: an unknown check
        // still occupies a slot, the safe direction for a ceiling.
        let kind = labels
            .get(super::job::LABEL_CHECK_KIND)
            .map(String::as_str)
            .and_then(|s| CheckPlanKind::ALL.iter().copied().find(|k| k.as_str() == s));
        let is_evidence = kind == Some(CheckPlanKind::EvidenceFetch);
        // By the Job's workspace label (PROD-19.1 §3): two checks running in
        // `dev` must not queue `prd`'s.
        let same_workspace =
            labels.get(crate::workspace::LABEL).map(String::as_str) == Some(workspace);

        if is_evidence {
            if same_workspace {
                counts.evidence_workspace += 1;
            }
            // An evidence fetch spends only its own pool.
            continue;
        }
        counts.total += 1;
        if same_workspace {
            counts.workspace += 1;
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
    Admit,
    /// Do not. The status is `phase: Queued` with this reason, requeued after
    /// [`QUEUED_REQUEUE_SECS`].
    Queued(CheckCode),
}

impl Admission {
    #[must_use]
    pub fn is_admitted(self) -> bool {
        matches!(self, Self::Admit)
    }
}

/// Apply the policy to a count (**pure**; the one place the four ceilings are
/// compared).
///
/// D2 §4.3 gives every over-limit outcome the one reason
/// [`CheckCode::ConcurrencyLimited`]: for a user "the installation is busy,
/// this is queued" is one fact. Which ceiling bit belongs in the condition
/// message, which the caller composes from [`ActiveCounts`].
#[must_use]
pub fn admit(counts: &ActiveCounts, policy: &ChecksPolicy, kind: CheckPlanKind) -> Admission {
    if kind == CheckPlanKind::EvidenceFetch {
        // The separate pool, and only that pool: making an evidence fetch
        // compete with interactive discovery is how verification stops
        // happening on a busy installation.
        return if counts.evidence_workspace >= policy.max_evidence_fetch_active_per_workspace {
            Admission::Queued(CheckCode::ConcurrencyLimited)
        } else {
            Admission::Admit
        };
    }
    if counts.workspace >= policy.max_active_per_workspace
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
/// The install's one namespace (the candidate's own), with the component
/// label. The installation-wide ceiling counts the Jobs this
/// controller could have created. The "active" filter is [`is_active`]'s, in
/// memory, because a Job's conditions are not a field selector.
///
/// # Errors
///
/// [`kube::Error`] from the `list`.
pub async fn check_jobs(client: &kube::Client, namespace: &str) -> Result<Vec<Job>, kube::Error> {
    let jobs: kube::Api<Job> = kube::Api::namespaced(client.clone(), namespace);
    Ok(jobs
        .list(&ListParams::default().labels(&active_selector()))
        .await?
        .items)
}
