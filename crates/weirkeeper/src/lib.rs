#![forbid(unsafe_code)]
//! `weirkeeper`, the Logweir control plane: the custom resources ([`crds`],
//! emitted to `charts/logweir/crds/` and diffed in CI), their reconcilers
//! ([`controllers`]) and the pure modules those reconcilers decide with.
//!
//! # Properties that hold across the crate
//!
//! * **No clock in the naming and cadence code.** [`slot`] and [`cadence`] are
//!   pure functions of their arguments (guard **G-SLOT**), so a duplicate
//!   reconcile computes the same object name and gets 409 `AlreadyExists`
//!   instead of writing a second, partial archive. [`cadence`] is the one
//!   cadence evaluator: the scheduler and the API's
//!   `GET /api/v1/cadence-previews` both read it.
//! * **The controller never dials a broker and never reads a Secret** (spec
//!   §9). Every execution, probe and check is a short-lived runner Job
//!   ([`job::build`]) whose credentials the kubelet projects from references
//!   [`connection`] and [`destination`] resolve; the controller reads the
//!   result off the pod log.
//! * **Read-only object storage (guard G-RET).** The one archive handle is
//!   built in `main` with `Store::read_only_from_url`, and no control-plane
//!   component holds a delete capability against object storage.
//!   [`evidence_store`] is the only other place a `Store` is constructed.
//!   `Store` is blocking, so every reconciler call goes through
//!   `tokio::task::spawn_blocking` (interface **I13**).
//! * **No signer is linked.** This crate links `logweir-verify` and never
//!   `logweir-evidence`, which holds `SigningKey` (Global Constraint 27): no
//!   control-plane crate links the signer, while the capability to sign stays
//!   with whoever holds Job CRUD over the signing key's namespace.
//!   `tests/linkage.rs` tests the half a test can reach.
//!
//! The Kubernetes test double ([`testing::mock_client`]) ships in `src/` so
//! every reconciler's tests, in any test binary or crate, can assert that the
//! reconciler called nothing its test did not record; that is why `tower`,
//! `http` and `http-body-util` are normal dependencies.

/// The installation's approval-policy document, read once at startup; the
/// contract is `logweir_core::approval_policy`'s.
pub mod approval_policy;
pub mod backup_execution;
pub mod cadence;
pub mod catalog_view;
pub mod check;
pub mod conditions;
pub mod connection;
pub mod controllers;
pub mod crds;
pub mod destination;
/// D3 §2.3 — the one diagnostic derivation for every Job-backed run.
pub mod diagnostics;
/// D2 §3.9 — the evidence-fetch check Job, for evidence only a pod may read.
pub mod evidence_fetch;
pub mod evidence_store;
pub mod health;
pub mod identity;
pub mod job;
pub mod policy;
/// D3 §§3.2–3.4: the freshness definition, the alert vocabulary and the
/// deduplication ledger, as pure functions over objects the caller already
/// read. `controllers::protection_policy` is the thin half.
pub mod protection;
pub mod read_budget;
/// Why a guard refused a run, read off the runner's one `refusal-detail=` line
/// by a bounded read made once, validated and cleaned before it reaches a
/// `Restore`'s or a `Backup`'s terminal condition.
pub mod refusal;
pub mod retention;
pub mod retention_plan;
/// The manual-run pool: how many "Back up now" and manual-restore runner Jobs
/// one namespace may have active at once, and the FIFO order the rest wait in.
/// Pure; `controllers::{backup,restore}` are the thin halves.
pub mod run_pool;
pub mod runner_log;
/// `runnerResources`: the validation a `Restore`'s requests and limits pass
/// before they reach the runner container, and the ceilings they are refused
/// above.
pub mod runner_resources;
pub mod scope;
pub mod slot;
/// PROD-05.1a: the read of a Strimzi cluster's `KafkaTopic` resources.
pub mod strimzi_topics;
pub mod testing;
pub mod verification;

/// The name of the one cluster-scoped `TrustRoster` (interface **I16**),
/// declared beside its reconciler ([`controllers::approval::ROSTER_NAME`]) and
/// re-exported so `weirkeeper::ROSTER_NAME` is the one path every consumer
/// names.
pub use controllers::approval::ROSTER_NAME;

/// D3 §7.1 and §7.5: which `TrustPolicy` a namespace resolves to, the
/// synthesised `legacy-roster-v1`, and the two seams (`decide_for`,
/// `may_sign_new_for`) that `verification` and `controllers::approval` call in
/// place of reading the roster directly.
pub mod trust;

/// Install the process-level rustls [`CryptoProvider`] this binary's TLS stack
/// needs, and report whether this call was the one that installed it.
///
/// rustls 0.23 selects a provider from its crate features only when exactly one
/// of `ring` / `aws-lc-rs` is enabled, and this workspace enables both
/// (`aws-lc-rs` through `object_store` → `reqwest`, `ring` through `ureq`), so
/// without this `ClientConfig::builder()` panics and the controller exits 101
/// before it can log anything. A library function, so
/// `tests/linkage.rs::the_startup_path_builds_a_client_from_a_kubeconfig_without_panicking`
/// can drive the call `main` makes. `ring`, because kube pairs it with the
/// `rustls-tls` feature and it is already in `Cargo.lock` (Global Constraint
/// 38); `Cargo.toml` carries the full reasoning. Idempotent: a `false` return
/// means another caller installed it first, which is not an error.
///
/// [`CryptoProvider`]: rustls::crypto::CryptoProvider
pub fn install_default_crypto_provider() -> bool {
    rustls::crypto::ring::default_provider()
        .install_default()
        .is_ok()
}
