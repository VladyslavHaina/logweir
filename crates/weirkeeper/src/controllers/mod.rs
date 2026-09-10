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

pub mod approval;
pub mod backup_schedule;
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
}

/// The type `main`'s registration point holds.
///
/// Re-exported here so a task appending a `controllers.push(…)` line to
/// `main.rs` names one type from one place.
pub type ControllerTask = Pin<Box<dyn Future<Output = ()> + Send>>;
