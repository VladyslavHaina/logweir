//! **FX-31 review F2 — one memory budget for every archive read the
//! controller makes.**
//!
//! FX-31 capped each read: a receipt or scorecard at 1 MiB, a manifest at
//! 64 MiB (`logweir_store::caps`). A cap bounds ONE read, and the controller
//! makes them concurrently. Every `BackupSchedule` reconcile evaluates its
//! retention report, and every finishing run reads its evidence, and the kube
//! runtime runs reconciles with no concurrency limit. The review measured
//! eight retention evaluations of 63 MiB manifests at 348–395 MB peak RSS in
//! one process. The chart's controller limit is 512Mi, and every controller
//! start reconciles every schedule at once.
//!
//! So every controller read RESERVES its worst case out of one process-wide
//! budget, [`CONTROLLER_READ_BUDGET_BYTES`], before it reads. It holds the
//! reservation until the bytes AND everything parsed from them are dropped.
//! A read that does not fit waits on its blocking thread until earlier
//! reservations are released. The sum of in-flight worst cases therefore never
//! exceeds the budget, however many reconciles run.
//!
//! # What each read reserves
//!
//! | read | reserves | why |
//! |---|---|---|
//! | a receipt or scorecard (`verify_evidence`, `observe_archive`, `observe_scorecard`, `read_signing_time`) | [`DOCUMENT_READ_COST_BYTES`] (40 MiB) | the 1 MiB cap, plus the `serde_json::Value` the controller parses before any digest check. A document of tiny values parses into about 37× its size, measured by `tests/read_caps.rs`. |
//! | one manifest in a retention report | [`MANIFEST_READ_COST_BYTES`] (64 MiB) | the cap's bytes are buffered. The fold over them keeps nothing per value. |
//!
//! With a 128 MiB budget, at most three documents, or two manifests, or one
//! manifest and one document, are read at once. A read takes milliseconds
//! against a healthy store. A degraded store makes the controller's evidence
//! reads wait on one another for every namespace. That is the same trade the
//! four-permit `evidence_fetch::controller_read_permits` pool already makes,
//! and the store's own request timeout bounds it.
//!
//! # Blocking, on purpose
//!
//! Every read this guards runs inside `tokio::task::spawn_blocking` (interface
//! I13: a `Store` call drives its own runtime). So the reservation is a
//! plain mutex and condition variable, waited on by a blocking-pool thread. It
//! never parks a reconciler's async task, and it needs no `.await` threaded
//! through five call sites. No guarded function reserves while holding a
//! reservation, so a wait can never wait on itself.

use std::sync::{Condvar, Mutex, PoisonError};

/// The process-wide budget: 128 MiB, a quarter of the chart's 512Mi
/// controller limit, so in-flight reads stay well under it beside the
/// controller's own caches.
pub const CONTROLLER_READ_BUDGET_BYTES: u64 = 128 << 20;

/// What one receipt or scorecard read reserves: its 1 MiB cap plus the
/// `serde_json::Value` it may be parsed into before any digest check (about
/// 37× its size at worst, measured).
pub const DOCUMENT_READ_COST_BYTES: u64 = 40 << 20;

/// What one manifest read reserves: the controller's manifest cap, whose bytes
/// are held while the window is folded.
pub const MANIFEST_READ_COST_BYTES: u64 = logweir_store::caps::CONTROLLER_MANIFEST;

/// A byte budget shared by the reads that reserve from it.
#[derive(Debug)]
pub struct ReadBudget {
    total: u64,
    in_use: Mutex<u64>,
    released: Condvar,
}

impl ReadBudget {
    /// A budget of `total` bytes. The controller uses ONE,
    /// [`ReadBudget::controller`]. A test builds its own to compare against it.
    #[must_use]
    pub const fn new(total: u64) -> Self {
        Self {
            total,
            in_use: Mutex::new(0),
            released: Condvar::new(),
        }
    }

    /// The controller's one budget, [`CONTROLLER_READ_BUDGET_BYTES`].
    #[must_use]
    pub fn controller() -> &'static ReadBudget {
        static BUDGET: ReadBudget = ReadBudget::new(CONTROLLER_READ_BUDGET_BYTES);
        &BUDGET
    }

    /// Wait, on this blocking thread, until `bytes` fit, and reserve them.
    /// A request larger than the whole budget reserves the whole budget, so
    /// it waits for every other read and then runs alone, never forever.
    pub fn reserve(&self, bytes: u64) -> Reservation<'_> {
        let bytes = bytes.min(self.total);
        let mut in_use = self.in_use.lock().unwrap_or_else(PoisonError::into_inner);
        while in_use.saturating_add(bytes) > self.total {
            in_use = self
                .released
                .wait(in_use)
                .unwrap_or_else(PoisonError::into_inner);
        }
        *in_use += bytes;
        Reservation {
            budget: self,
            bytes,
        }
    }

    /// The bytes reserved right now.
    #[must_use]
    pub fn in_use(&self) -> u64 {
        *self.in_use.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// The budget's size.
    #[must_use]
    pub fn total(&self) -> u64 {
        self.total
    }
}

/// Bytes reserved out of a [`ReadBudget`], released when this is dropped.
#[derive(Debug)]
#[must_use = "a reservation released at once guards nothing"]
pub struct Reservation<'a> {
    budget: &'a ReadBudget,
    bytes: u64,
}

impl Drop for Reservation<'_> {
    fn drop(&mut self) {
        let mut in_use = self
            .budget
            .in_use
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        *in_use = in_use.saturating_sub(self.bytes);
        drop(in_use);
        self.budget.released.notify_all();
    }
}
