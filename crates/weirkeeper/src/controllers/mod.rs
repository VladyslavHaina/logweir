//! The reconcilers.
//!
//! One module per kind, each thin on purpose: read what the API server says,
//! hand it to a pure function in the same module, and patch `/status` with what
//! that function returned. A verdict reachable only through a `kube::Client` can
//! only be tested through one, and an ordering whose order is the whole
//! property (see [`approval::evaluate`]) needs tests that name a check, not a
//! route table.
//!
//! # What a reconciler may do
//!
//! * **To the object it reconciles: patch its `/status`, and nothing else.** It
//!   never patches a `spec` (every `spec` here is CEL-sealed) and never deletes:
//!   a refused `Approval` stays as the audit trail of a rejected attempt
//!   (`approval_reconcile_patches_only_status`,
//!   `a_refused_approval_is_not_deleted`).
//! * **Create other objects** under a name that is a pure function of the
//!   trigger, so a duplicate `POST` is a 409 rather than a second run (guard
//!   **G-SLOT**): [`backup_schedule`] creates `Backup`s.
//! * **To a `Job` it created**: patch `ttlSecondsAfterFinished`, and only after
//!   the status patch carrying the exit code returned, because the exit code
//!   lives on the pod and pod garbage collection must not race its read.
//! * **Refuse before it creates anything**: [`restore::admit`] is a pure
//!   function of the `Restore`, its `Approval` and the target, and an
//!   unapproved plan creates nothing at all. It recomputes
//!   `sha256_prefixed(spec.planBytes)` and compares it with the `plan_hash` in
//!   `Approval.spec.approvalBytes`, never with `Approval.status`, which is a
//!   cache. "The approval has not arrived yet" is a thirty-second requeue
//!   (interface **I19**), so a `Restore` and its `Approval` can be created in
//!   either order.
//! * **Answer what CEL cannot**: [`backup_destination`] reports a verdict about
//!   another object's usability (engine-dependent addressing, a CA bundle in a
//!   `ConfigMap`), dials nothing and reads no Secret.
//! * **Advise without authorising**: [`preflight`]'s result authorises nothing;
//!   no execution path reads a `Preflight` (D2 §6.8,
//!   `no_execution_path_reads_preflight_or_discovery`), and a check writes
//!   nothing against a subject's target.
//!
//! Condition reasons are variant names: `Verified`'s `reason` is the name of
//! the [`approval::ApprovalRefusal`] variant that produced it and its `message`
//! that variant's `Display`, and [`approval::ApprovalRefusal::reason`] is a
//! `match` with no wildcard, so a new variant fails to compile until its
//! reason is named.

pub mod approval;
pub mod backup;
pub mod backup_destination;
pub mod backup_schedule;
pub mod backup_selection;
pub mod kafka_cluster;
pub mod preflight;
pub mod protection_policy;
pub mod recovery_catalog;
pub mod restore;
pub mod retention_policy;
pub mod schedule_history;
pub mod topic_discovery;
pub mod trust_policy;
pub mod trust_roster;

use std::future::Future;
use std::pin::Pin;

/// What every reconciler in this directory needs, and nothing more.
///
/// A struct rather than a bare `kube::Client`, because `kube::runtime`'s
/// `reconcile` takes exactly one context argument.
#[derive(Clone)]
pub struct Context {
    /// The client every `Api` in this directory is built from.
    pub client: kube::Client,
    /// The controller's ONE read-only archive handle, or `None` when
    /// [`crate::retention::ARCHIVE_URL_ENV`] is unset (interface **I13**).
    ///
    /// `Arc<Store>`, not a URL: `Store` drives its own current-thread runtime,
    /// so a handle rebuilt inside a reconcile panics ("Cannot start a runtime
    /// from within a runtime") and discards the connection pool, so it is built
    /// once in `main`, before the tokio runtime, and every call goes through
    /// `tokio::task::spawn_blocking`. It cannot write: `main` builds it with
    /// `Store::read_only_from_url`.
    pub archive: Option<std::sync::Arc<logweir_store::Store>>,
    /// The image and pull policy the runner Jobs this controller creates will
    /// carry (interface **I15**); `None` in either field is the compiled-in
    /// constant. Read once each in `main` through
    /// `crate::job::configured_runner_image` and
    /// `crate::job::configured_runner_pull_policy`, so the decision is
    /// testable; `main` refuses to start on a pull policy the API server would
    /// reject. One field for both, because they are two halves of one decision.
    ///
    /// `approval`, `trust_roster` and `backup_schedule` create no Job and pass
    /// the default, which means "unused", not "no override configured": one
    /// that gains a Job must thread the value in.
    pub runner_image: crate::job::RunnerImage,
}

/// The cluster-scoped `TrustRoster`'s `spec`, or `None` when it could not be
/// read (interface **I16**): the verifying side's one fetch point, beside
/// [`approval::load_roster`]'s for the authorising side.
///
/// A roster that is not found and one whose `signingKeys` is empty are the same
/// fact to a verifier (no signing key material), and
/// `verification::verify_evidence` gives both the same `NotAttempted` detail.
/// An API failure is neither, and returns `None` so the caller can say so,
/// rather than send an operator to edit an object that was already right.
pub async fn roster_spec(
    client: &kube::Client,
) -> Result<crate::crds::trust_roster::TrustRosterSpec, kube::Error> {
    match approval::load_roster(client).await? {
        approval::RosterLoad::Found(roster) => Ok(roster.spec),
        approval::RosterLoad::NotFound => Ok(crate::crds::trust_roster::TrustRosterSpec {
            approver_keys: Vec::new(),
            signing_keys: Vec::new(),
            allowed_cluster_ids: Vec::new(),
        }),
    }
}

/// The type `main`'s registration point holds.
///
/// Re-exported here so `main.rs`'s registration point names one type from one
/// place.
pub type ControllerTask = Pin<Box<dyn Future<Output = ()> + Send>>;
