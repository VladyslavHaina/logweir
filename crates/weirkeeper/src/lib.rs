#![forbid(unsafe_code)]
//! `weirkeeper` — the Logweir control plane.
//!
//! This half of the crate is deliberately almost empty. It exists so that the
//! three risks that come with introducing a Kubernetes client — the dependency
//! decision, the one network fetch this plan performs, and the linkage
//! property that keeps the controller away from the signer — are settled and
//! tested on their own, before a single CRD field or reconciler arrives. The
//! six kinds landed in Task 15b, in [`crds`], and the first reconciler lands
//! in Task 16; both append to what is here rather than reshaping it.
//!
//! WHAT TASK 15B ADDED. [`crds`] holds the six kinds of
//! `logweir.dev/v1alpha1`, the CEL immutability seals and the deterministic
//! emitter whose output is checked in under `config/crd/` and diffed in CI;
//! [`job`] holds the one constant that names the runner image, and nothing
//! else until Task 17 grows `job::build` around it.
//!
//! WHAT TASK 18 ADDED. [`slot`] — the trigger, and the names that are a pure
//! function of it (guard **G-SLOT**): a hand-written five-field [`slot::Cron`]
//! parser that refuses what it does not understand naming the field, and the
//! three name functions [`slot::slot_name`], [`slot::scheduled_backup_name`]
//! and [`slot::backup_id_for`]. Nothing in that module reads a clock, which is
//! what makes a duplicate reconcile compute the same object name and get **409
//! `AlreadyExists`** from the API server instead of writing a second, partial
//! archive. [`controllers::backup_schedule`] is the reconciler that uses them.
//!
//! WHAT TASK 16 ADDED. [`controllers`] holds the first two reconcilers —
//! [`controllers::approval`], whose five checks in a stated order are what
//! turn an `Approval` object into an authorisation, and
//! [`controllers::trust_roster`], which makes the roster's `LOADED` and
//! `EXPIRED` printer columns mean something. [`ROSTER_NAME`] is re-exported
//! here (interface **I16**) because `weirkeeper::ROSTER_NAME` is the one path
//! every consumer of it names.
//!
//! WHAT TASK 19 ADDED. [`retention`] — the retention **report**, and guard
//! **G-RET**. It lists an archive's manifests through a handle the caller
//! built with `Store::read_only_from_url`, works out which backup sets a
//! `{keepLast, keepDays}` policy WOULD remove, renders the exact `aws s3 rm`
//! and `mc rm` commands an operator would run, and **deletes nothing** —
//! Global Constraint 6 stands unamended, and no Logweir component in tag 1
//! holds any delete capability against object storage. The report lands on
//! `BackupSchedule.status.retentionReport`, refreshed on every reconcile.
//! Interface **I13** is stated in that module's header: `Store` is blocking,
//! so every call from a reconciler goes through `tokio::task::spawn_blocking`
//! and the handle is built once, in `main`, and shared as `Arc<Store>`.
//!
//! WHAT THIS CRATE LINKS, AND WHAT IT MUST NOT. `logweir-verify` — the
//! verifying half of the DSSE machinery — and never `logweir-evidence`, which
//! keeps `SigningKey` and `sign_detached`. Global Constraint 27 states the
//! narrowed position exactly: no control-plane CRATE LINKS the signer, and the
//! CAPABILITY to sign is unbroken while this controller holds Job CRUD over
//! the signing key's namespace. Both halves of that sentence matter, and
//! `tests/linkage.rs` tests the half a test can reach.
//!
//! WHY THE DOUBLE IS IN `src/` AND NOT IN `tests/`. Every reconciler from
//! Task 16 onward is tested against [`testing::mock_client`], and a
//! `tests/`-local helper cannot be shared across test binaries, let alone
//! across crates. Shipping it in the library is what makes "the reconciler
//! called nothing its test did not record" a property every later task
//! inherits instead of re-implements — and it is why `tower`, `http` and
//! `http-body-util` are normal dependencies here rather than
//! `[dev-dependencies]`.

//! WHAT TASK 17 ADDED. [`conditions`] — Global Constraint 11's five exit
//! codes as wire strings, plus the ten terminal states that are NOT an exit
//! code; [`job::build`], which turns a [`job::RunnerJobSpec`] into the one
//! `Job` shape that keeps an exit code readable; and
//! [`controllers::backup`], the reconciler that creates that Job, lifts the
//! code off `pod.status.containerStatuses[].state.terminated.exitCode`, reads
//! the receipt keys off the final two stdout lines, and writes a TERMINAL
//! status for the case nothing else in the corpus handled — a Job that
//! finished with no terminated state at all.

pub mod conditions;
pub mod controllers;
pub mod crds;
pub mod job;
pub mod retention;
pub mod slot;
pub mod testing;

/// The name of the one cluster-scoped `TrustRoster` — interface **I16**.
///
/// RE-EXPORTED HERE BECAUSE `weirkeeper::ROSTER_NAME` IS THE ONE PATH TASKS
/// 20, 21, 24, 27 AND 28 NAME. The declaration lives beside the reconciler
/// that resolves it ([`controllers::approval::ROSTER_NAME`]); this line is
/// what makes the interface register's spelling the spelling every consumer
/// writes, so a later task cannot reach the constant by a second path and then
/// have that path move.
pub use controllers::approval::ROSTER_NAME;

/// Install the process-level rustls [`CryptoProvider`] this binary's TLS stack
/// needs, and report whether this call was the one that installed it.
///
/// WHY THIS FUNCTION EXISTS. rustls 0.23 selects a process-level provider from
/// its own crate features, and only when EXACTLY ONE of `ring` / `aws-lc-rs`
/// is enabled; with two enabled `from_crate_features()` returns `None` and
/// `ClientConfig::builder()` panics at
/// `rustls-0.23.43/src/crypto/mod.rs:249`. This workspace's unified graph
/// enables both — `aws-lc-rs` through `object_store` → `reqwest`, `ring`
/// through `ureq 2.12.1` — so **two is the same as none**. Measured on the
/// shipped binary before this function existed: `weirkeeper` with no argv
/// aborted at **exit 101** inside rustls, before `kube::Client::try_default()`
/// could return anything. `main`'s `error!("no Kubernetes client…")` branch was
/// therefore unreachable, and a pod would have CrashLoopBackOff'd on a raw
/// panic with no structured log line at all.
///
/// WHY IT IS A LIBRARY FUNCTION AND NOT THREE LINES INSIDE `main`. `main.rs`
/// is a binary: an integration test cannot call into it. Putting the install
/// here is what lets
/// `tests/linkage.rs::the_startup_path_builds_a_client_from_a_kubeconfig_without_panicking`
/// drive the exact call `main` makes, in-process and without a socket, so the
/// property has a test at assertion time rather than only a process-level
/// observation.
///
/// WHY `ring`. It is the provider kube 0.99 itself pairs with the `rustls-tls`
/// feature this crate takes — kube's own `default` set is `["client", "ring"]`
/// — and `ring 0.17.14` is already in `Cargo.lock`, so declaring it adds no
/// package (Global Constraint 38). `Cargo.toml`'s entry carries the full
/// reasoning, including why `aws-lc-rs` was declined and why a fifth kube
/// feature would not have fixed this.
///
/// IDEMPOTENT BY CONSTRUCTION. `install_default` is a `OnceLock::set`: a
/// `false` return means some other caller got there first, which is a race
/// this function is allowed to lose and not an error. The return value is
/// reported rather than discarded so a caller — and the test — can say which
/// happened.
///
/// [`CryptoProvider`]: rustls::crypto::CryptoProvider
pub fn install_default_crypto_provider() -> bool {
    rustls::crypto::ring::default_provider()
        .install_default()
        .is_ok()
}
