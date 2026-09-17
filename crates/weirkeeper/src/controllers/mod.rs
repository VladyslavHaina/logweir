//! The reconcilers.
//!
//! ONE MODULE PER KIND, and each of them is THIN on purpose. Every reconciler
//! in this directory is the same shape: read what the API server says, hand
//! the bytes to a **pure function** in the same module, and patch `/status`
//! with what that function returned. The verdict is never computed inside an
//! `async` block that also performs I/O, because a verdict that can only be
//! reached through a `kube::Client` can only be tested through one — and a
//! five-check ordering whose order is the whole property (see
//! [`approval::evaluate`]) needs tests that name a check, not tests that name
//! a route table.
//!
//! WHAT A RECONCILER IN THIS DIRECTORY MAY DO TO AN OBJECT: patch its
//! `/status`, and nothing else. It never patches a `spec` — every `spec` in
//! this group is sealed by a CEL rule, so an attempted spec patch is a 422 at
//! best and a silently-widened approval at worst — and it never DELETES.
//! A refused `Approval` stays in the cluster as the audit trail of a rejected
//! attempt; `approval_reconcile_patches_only_status` and
//! `a_refused_approval_is_not_deleted` are the tests that say so, over a
//! double that records every request and panics on one it was not given a
//! route for.
//!
//! WHAT A RECONCILER IN THIS DIRECTORY MAY **CREATE**. Task 18 adds the first
//! reconciler that creates an object rather than only patching one:
//! [`backup_schedule`] `POST`s a `Backup` for a due slot. The rule above is
//! unchanged for the object it reconciles — it patches the `BackupSchedule`'s
//! `/status` and nothing else — and the object it creates is a DIFFERENT kind,
//! named by a pure function of the trigger so that a duplicate `POST` is a 409
//! rather than a second archive run (guard **G-SLOT**).
//!
//! WHY THE CONDITION REASONS ARE VARIANT NAMES. `Verified`'s `reason` is the
//! name of the [`approval::ApprovalRefusal`] variant that produced it and its
//! `message` is that variant's `Display`. That makes the machine-readable half
//! of the condition a closed set the compiler enumerates, rather than a string
//! a reconcile arm invented — and it is why
//! [`approval::ApprovalRefusal::reason`] is a `match` with no wildcard: a new
//! variant fails to compile until someone names its reason.

//! WHAT A RECONCILER IN THIS DIRECTORY MAY DO TO A `Job` (Task 17).
//! [`backup`] is the first reconciler that creates a `batch/v1` Job and then
//! patches it — with `ttlSecondsAfterFinished`, and ONLY after the status
//! patch carrying the exit code has returned 200. That ordering is the whole
//! property: pod garbage collection must never race the exit-code read, and
//! the exit code lives on the POD, not on the Job. It still deletes nothing —
//! not the Job, not the pod, and not an orphaned scorecard.

//! WHAT A RECONCILER IN THIS DIRECTORY MAY DO BEFORE IT CREATES ANYTHING
//! (Task 20). [`restore`] is the first reconciler whose FIRST act is a
//! refusal: [`restore::admit`] is a pure function of the `Restore`, the
//! `Approval` it names and the target `KafkaCluster`, it runs before the first
//! `POST`, and **an unapproved plan creates nothing at all** — no ConfigMap,
//! no Job, zero `POST`s (Global Constraint 6's operator half). It recomputes
//! `sha256_prefixed(spec.planBytes)` there and then and compares it against
//! the `plan_hash` inside `Approval.spec.approvalBytes`, never against
//! `Approval.status`: a spec schema change invalidates every approval, and a
//! status is a cache. The one admission outcome that is NOT a verdict —
//! "the approval has not arrived yet" — is a thirty-second requeue
//! (interface **I19**), which is what makes a `Restore` and its `Approval`
//! creatable in either order.

//! WHAT A RECONCILER IN THIS DIRECTORY MAY DO WHEN IT IS THE ONLY THING THAT
//! CAN ANSWER (D2 W7). [`backup_destination`] is the first reconciler whose
//! whole output is a VERDICT ABOUT ANOTHER OBJECT'S USABILITY, and it is here
//! because CEL cannot reach either half of the question: whether the pinned
//! engine can honour the declared addressing depends on the ENGINE VERSION, and
//! whether the declared CA bundle exists, carries its key and holds
//! certificates depends on a `ConfigMap` a CEL rule may not read. It creates
//! nothing, dials nothing and probes nothing on a timer — a `VALID` column that
//! meant "reachable four minutes ago" is the defect PLAT-03 names — and it
//! reads a `ConfigMap` and never a Secret, because a CA certificate is public
//! material and a credential value is not the controller's to hold (D2 §3.8
//! option B, rejected).

//! WHAT A RECONCILER IN THIS DIRECTORY MAY DO WHEN ITS WHOLE OUTPUT IS
//! ADVISORY (D2 W9). [`preflight`] is the first reconciler that creates a Job
//! whose result AUTHORISES NOTHING. Every execution-time guard still runs and
//! none of them reads a `Preflight` (D2 §6.8) — `no_execution_path_reads_
//! preflight_or_discovery` is the source scan that says so, over
//! `controllers/{backup,backup_schedule,restore,approval}.rs` and both runner
//! paths. It also performs NO WRITE against a subject's target: the
//! validate-only `CreateTopics` D2 §6.7 calls for happens inside the check pod,
//! and this controller patches its own `/status`, its own Job's TTL, and
//! nothing else.

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
pub mod topic_discovery;
pub mod trust_policy;
pub mod trust_roster;

use std::future::Future;
use std::pin::Pin;

/// What every reconciler in this directory needs, and nothing more.
///
/// A struct rather than a bare `kube::Client` because `kube::runtime`'s
/// `reconcile` takes exactly one context argument and later tasks in chain O
/// add fields to it (Task 19's shared `Arc<Store>`, interface **I13**). A
/// client-shaped context would have to be reshaped by whichever task needed
/// the second field first.
#[derive(Clone)]
pub struct Context {
    /// The client every `Api` in this directory is built from.
    pub client: kube::Client,
    /// The controller's ONE read-only archive handle, or `None` when
    /// [`crate::retention::ARCHIVE_URL_ENV`] is unset — interface **I13**.
    ///
    /// `Arc<Store>` AND NOT A URL, AND THAT IS THE GUARD. `Store` drives its
    /// own current-thread runtime, so a handle rebuilt inside a reconcile
    /// both panics (*Cannot start a runtime from within a runtime*) and
    /// discards the connection pool on every reconcile; the handle is
    /// therefore built ONCE, in `main`, before the tokio runtime exists, and
    /// shared. Every call on it goes through `tokio::task::spawn_blocking`.
    ///
    /// IT CANNOT WRITE. `main` builds it with `Store::read_only_from_url`,
    /// whose `read_only` flag makes every put method refuse before it checks
    /// anything else. `Option` because a controller with no archive
    /// configured holds no archive handle at all, which is the shape every
    /// gate in this task runs in.
    pub archive: Option<std::sync::Arc<logweir_store::Store>>,
    /// The image the runner Jobs THIS controller creates will name AND the
    /// pull policy they will carry — Task 33's interface **I15** runtime half,
    /// grown by Task 37 into the pair `crate::job::RunnerImage` holds. Each
    /// field is `None` for the compiled-in constant
    /// (`crate::job::RUNNER_IMAGE`, `crate::job::IMAGE_PULL_POLICY`).
    ///
    /// READ ONCE EACH IN `main`, out of `crate::job::RUNNER_IMAGE_ENV` and
    /// `crate::job::RUNNER_PULL_POLICY_ENV`, through
    /// `crate::job::configured_runner_image` and
    /// `crate::job::configured_runner_pull_policy` — the same arrangement
    /// [`Context::archive`] has, and for the same reason: a decision behind
    /// `fn main` is reachable from no test at all. `main` logs which answer it
    /// got for each, once, at startup, and REFUSES TO START on a pull policy
    /// the API server would reject.
    ///
    /// ONE FIELD CARRYING BOTH, and not two beside each other: the image and
    /// its pull policy are two halves of one decision (a `latest` tag must be
    /// pulled; a loaded tag must not be), and two parallel `Option`s threaded
    /// through three reconcilers are how the halves come to disagree.
    ///
    /// **THE THREE CONTROLLERS THAT CREATE NO RUNNER JOBS PASS THE DEFAULT
    /// HERE, AND THAT IS NOT A STATEMENT ABOUT THE ENVIRONMENT.** `approval`,
    /// `trust_roster` and `backup_schedule` create no `Job` at all — a
    /// `BackupSchedule` creates `Backup` objects, and the `Backup` reconciler
    /// is what turns those into Jobs — so the value would be carried and never
    /// read. A task that gives one of them a Job must thread the value in
    /// rather than reading the default as "no override is configured".
    pub runner_image: crate::job::RunnerImage,
}

/// The cluster-scoped `TrustRoster`'s `spec`, or `None` when it could not be
/// read — Task 24, interface **I16**.
///
/// ONE FETCH POINT FOR THE VERIFYING SIDE, beside
/// [`approval::load_roster`]'s for the authorising side. Both resolve
/// [`crate::ROSTER_NAME`] and nothing else; this one flattens the two
/// "absent" cases together on purpose:
///
/// * a roster that is **not found** and
/// * a roster whose `signingKeys` is **empty**
///
/// are the same fact to a verifier — there is no signing key material in this
/// cluster — and `verification::verify_evidence` gives both the same
/// `NotAttempted` detail, which names the field to add the key to.
///
/// An API FAILURE IS NOT EITHER OF THOSE, and it returns `None` here so the
/// caller can say so: telling an operator to "add the runner's public key"
/// when the real problem is a 503 sends them to edit an object that was
/// already right.
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
/// Re-exported here so a task appending a `controllers.push(…)` line to
/// `main.rs` names one type from one place.
pub type ControllerTask = Pin<Box<dyn Future<Output = ()> + Send>>;
