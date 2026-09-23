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
//! WHAT D1 W1 ADDED. [`cadence`] — the zone a cron expression is read in, the
//! policy inputs that decide whether a due slot may still run, and the
//! next-run preview the console shows (decision D1 §4, PLAT-04.2). It is the
//! ONE cadence evaluator in the product: the scheduler, the API's
//! `GET /api/v1/cadence-previews` and, through them, the browser all read this
//! module, so there is no second answer to "when does my backup run". An
//! absent `spec.timeZone` takes [`cadence::Zone::Utc`], which delegates to
//! [`slot::Cron`] itself rather than re-deriving it, so every schedule that
//! exists today keeps the slots it has today. Nothing here reads a clock
//! either — guard **G-SLOT** covers both modules.
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

//! WHAT TASK 24 ADDED. [`verification`] — the controller's own reading of the
//! evidence the UI renders, and the two green-badge rules (interface **I21**).
//! `verify_evidence` fetches the payload and its detached sidecar through the
//! read-only evidence handle, checks the digest the status recorded, and
//! verifies the DSSE signature against `TrustRoster.spec.signingKeys[]`
//! (interface **I17**) — returning `NotAttempted`, never `Invalid`, when no
//! credential is configured, when storage would not answer, or when the roster
//! carries no key material. `Backup` is green on `Valid` + `exitCode == 0`;
//! `Restore` on `Valid` + `outcome == pass`; there is no `outcome` on the
//! `Backup` path at all, which is why the rule is two rules. Both reconcilers
//! write it in a SECOND, separate `/status` patch after the terminal one, so a
//! verification failure can never prevent the exit code from being recorded.
//!
//! WHAT TASK 15C ADDED. [`controllers::kafka_cluster`] — the FIRST loop whose
//! whole output is an observation, and the producer of the
//! `KafkaCluster.status.reachable` field Task 15b declared and nobody wrote.
//! It runs `logweir cluster-probe` (interface **I14**) as a short-lived Job,
//! reads that subcommand's two stdout lines BY KEY NAME from a bounded tail
//! (erratum **E4**), and writes `reachable`, `clusterId`, `observedAt`, one
//! `Reachable` condition and the scalar `reason`. **It never dials a broker
//! itself and never reads a Secret** (spec §9), which is why a probe is a Job;
//! and it deletes nothing — the re-probe cadence is the probe Job's own
//! `ttlSecondsAfterFinished`, so the API server collects the finished Job and
//! the next reconcile finds none.
//!
//! WHAT PLAT-07.1 ADDED. [`connection`] — the saved-connection resolver. One
//! function turns a `KafkaCluster` into the public settings and the Secret and
//! ConfigMap REFERENCES a runner needs, and the probe, backup and restore Jobs
//! are all built from that one resolution; a conflicting or unusable connection
//! is refused before any Job exists. [`connection::credential`] is the
//! write-only credential-entry contract a product API builds its create-only
//! Secret with — a library the controller itself never calls.

//!
//! WHAT D2 W7 ADDED. [`destination`] — the controller half of a saved
//! `BackupDestination`: the resolver that turns one into the COMPLETE, explicit
//! `AWS_*` set and plan storage block one operation needs, the frozen
//! [`destination::ResolvedDestinationSnapshot`] a run records (seam **S4**), and
//! the G14 retention guard. Not one function in it reads `std::env::var`, which
//! is what closes defect SEC-ENVHTTP: the controller's own `AWS_ALLOW_HTTP` can
//! no longer reach a runner Job and enable plaintext transport for a plan that
//! forbids it (D-SEAMS **S5**). [`evidence_store`] holds the bounded,
//! allowlisted `ControllerIdentity` handles of D2 §3.10 — the SECOND and last
//! place in this crate that constructs a `Store`, inside `spawn_blocking`
//! (interface **I13**), and [`controllers::backup_destination`] is the
//! reconciler that publishes each destination's `Valid` condition, canonical
//! URL and two digests.

/// PLAT-19.2: the installation's approval-policy document, read once at
/// startup; the contract is `logweir_core::approval_policy`'s.
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
/// PLAT-14.2 / decision D3 §§3.2–3.4: the freshness definition, the alert
/// vocabulary and the deduplication ledger, as pure functions over objects the
/// caller already read. `controllers::protection_policy` is the thin half.
pub mod protection;
/// PLAT-14.3 / decision D3 §4: the pure half of a recurring recovery rehearsal
/// — the template digest a standing authorization binds, the qualifying-point
/// filter chain, the rendered plan and the scope the controller proves it falls
/// inside. `controllers::rehearsal_schedule` is the thin half.
pub mod rehearsal;
pub mod retention;
pub mod retention_plan;
pub mod scope;
pub mod slot;
pub mod testing;
pub mod verification;

/// The name of the one cluster-scoped `TrustRoster` — interface **I16**.
///
/// RE-EXPORTED HERE BECAUSE `weirkeeper::ROSTER_NAME` IS THE ONE PATH TASKS
/// 20, 21, 24, 27 AND 28 NAME. The declaration lives beside the reconciler
/// that resolves it ([`controllers::approval::ROSTER_NAME`]); this line is
/// what makes the interface register's spelling the spelling every consumer
/// writes, so a later task cannot reach the constant by a second path and then
/// have that path move.
pub use controllers::approval::ROSTER_NAME;

/// PLAT-19.1 / decision D3 §7.1 and §7.5: which `TrustPolicy` a namespace
/// resolves to, the synthesised `legacy-roster-v1`, and the two seams
/// (`decide_for`, `may_sign_new_for`) that `verification` and
/// `controllers::approval` call in place of reading the roster directly.
pub mod trust;

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
