#![forbid(unsafe_code)]
//! The `weirkeeper` binary.
//!
//! THREE ARGV FORMS AND NO ARGUMENT PARSER. `--version`, `--help`, or nothing
//! at all; anything else exits 1 naming what it got. `clap` is deliberately
//! not added: Global Constraint 38 closes the workspace graph, and a match on
//! `std::env::args()` is the whole requirement. `--version` exists because
//! Task 23's `scripts/check-image-weirkeeper.sh` check 2 runs
//! `/usr/local/bin/weirkeeper --version` against the built image, and an image
//! check needs something to run that touches no cluster.
//!
//! THE ARGV MATCH COMES FIRST, BEFORE THE CLIENT. `--version` inside a
//! container has no kubeconfig, no service-account token and no API server to
//! reach; if it built a client first it would exit non-zero in exactly the
//! place it is used.
//!
//! ONE TYPE ALIAS, NOT TWO (Task 18). Task 15 declared a local
//! `type ControllerTask` here and Task 16 added
//! `weirkeeper::controllers::ControllerTask` beside it; two spellings of one
//! type is how the registration point below comes to be typed against the
//! wrong one. The local alias is GONE and this file names the library's, which
//! is the one `controllers/mod.rs` documents as "the type `main`'s registration
//! point holds".
//!
//! ONE THING PRECEDES THE ARGV MATCH: THE rustls PROVIDER INSTALL. It is not a
//! client and it is not I/O — it builds a struct and sets a `OnceLock` — and
//! it has to be unconditional, because it is the difference between this
//! binary's no-argv path logging a named error and ABORTING at exit 101 inside
//! rustls (review finding H1). See
//! [`weirkeeper::install_default_crypto_provider`] for the mechanism and
//! `Cargo.toml`'s `rustls` entry for why the provider is `ring`. `--version`'s
//! guarantee is unchanged: it still builds no client and touches no cluster,
//! which is what Task 23's `scripts/check-image-weirkeeper.sh` check 2 runs.

use std::process::ExitCode;

use tracing::{error, info};
use weirkeeper::controllers::ControllerTask;

const HELP: &str = "weirkeeper — the Logweir control plane. It watches the six \
logweir.dev/v1alpha1 kinds and runs each restore, drill and backup as a Job; it \
holds a read-only archive handle and verifies the DSSE signatures the UI renders, \
and it links the verifying half of the evidence machinery and never the signer. \
Takes no arguments: `--version` prints the version, `--help` prints this, and no \
argument at all runs the controller until SIGTERM.";

fn main() -> ExitCode {
    // BEFORE ANY kube CLIENT IS BUILT — and before the argv match, because a
    // provider install is neither a client nor a syscall and an unconditional
    // one cannot be skipped by a future argv form that does build a client.
    // Losing the race to another installer is a `false`, not an error.
    let _installed = weirkeeper::install_default_crypto_provider();

    let argv: Vec<String> = std::env::args().skip(1).collect();
    let flags: Vec<&str> = argv.iter().map(String::as_str).collect();
    match flags.as_slice() {
        [] => run(),
        ["--version"] => {
            println!("weirkeeper {}", env!("CARGO_PKG_VERSION"));
            ExitCode::SUCCESS
        }
        ["--help"] => {
            println!("{HELP}");
            ExitCode::SUCCESS
        }
        _ => {
            // Names what it got, so an operator reading a CrashLoopBackOff log
            // can see the typo rather than a usage screen.
            eprintln!(
                "weirkeeper: unrecognised argument{} {} — this binary takes `--version`, \
                 `--help`, or nothing at all.",
                if argv.len() == 1 { "" } else { "s" },
                argv.iter()
                    .map(|a| format!("`{a}`"))
                    .collect::<Vec<_>>()
                    .join(" ")
            );
            ExitCode::FAILURE
        }
    }
}

/// Run the controller until SIGTERM.
///
/// `clippy::vec_init_then_push` IS ALLOWED FOR THE REGISTRATION POINT BELOW,
/// AND THE REASON IS A CONTRACT AND NOT A PREFERENCE. The `controllers` vector
/// is the one place every chain-O task from Task 16 onward appends ONE line —
/// `controllers.push(Box::pin(…));` — with nothing else in this file changing.
/// Collapsing the two current entries into the `vec![…]` literal clippy
/// suggests would turn each of those one-line additions into an edit of the
/// same expression, which is precisely the rewrite the plain `Vec` was written
/// to avoid. The allow sits on the function because the lint's span is the
/// whole statement group, not the `let`.
#[allow(clippy::vec_init_then_push)]
fn run() -> ExitCode {
    // JSON to stdout, filter from `RUST_LOG`: the pod log API has no stream
    // selector (Global Constraint 11), so everything a controller wants read
    // back goes to stdout in one machine-readable shape.
    tracing_subscriber::fmt()
        .json()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    // THE ONE READ-ONLY ARCHIVE HANDLE, BUILT BEFORE THE TOKIO RUNTIME EXISTS
    // — interface I13, and the ORDER is load-bearing rather than tidy.
    // `Store`'s constructors build and drive their OWN current-thread runtime
    // (`crates/logweir-store/src/lib.rs`'s `new_rt`, and the `rt.block_on` in
    // `build_backend`), and `Runtime::block_on` from a thread already driving
    // a runtime panics with *Cannot start a runtime from within a runtime*.
    // Constructing this inside `rt.block_on` below — or worse, inside a
    // reconcile — is that panic. Here, on a thread driving nothing, it is a
    // plain synchronous call.
    //
    // AND IT IS BUILT EXACTLY ONCE. `Arc<Store>` is cloned into the schedule
    // controller's context; every later call on it goes through
    // `tokio::task::spawn_blocking`. A handle rebuilt per reconcile discards
    // the connection pool each time and reintroduces the nested-runtime panic,
    // and `tests/retention.rs::no_store_call_is_made_outside_spawn_blocking`
    // asserts this file is the only place in the crate that constructs one.
    //
    // `read_only_from_url` AND NEVER `from_url`. The archive is never under
    // `logweir/`, so the write-path constructor's `LOGWEIR_ROOT` guard would
    // refuse to build a handle over it at all; the handle this returns
    // physically cannot put (Global Constraint 6, guard G-RET).
    // `scripts/check-no-archive-write.sh` fails if the other constructor is
    // named anywhere under `crates/weirkeeper/src/`.
    let archive = match std::env::var(weirkeeper::retention::ARCHIVE_URL_ENV) {
        Err(_) => {
            info!(
                env = weirkeeper::retention::ARCHIVE_URL_ENV,
                "no archive configured: this controller holds no archive handle and writes no \
                 retention report"
            );
            None
        }
        Ok(url) => match weirkeeper::retention::storage_url_for(&url) {
            Err(e) => {
                // NOT FATAL, AND NAMED. A malformed archive URL must not stop
                // the approval and schedule reconcilers, which need no
                // archive at all; retention is the only thing that degrades,
                // and it degrades loudly.
                error!(
                    env = weirkeeper::retention::ARCHIVE_URL_ENV,
                    error = %e,
                    "the configured archive URL is not readable as an object-store location; \
                     this controller holds no archive handle and writes no retention report"
                );
                None
            }
            Ok(loc) => match logweir_store::Store::read_only_from_url(&loc) {
                Ok(store) => {
                    info!(
                        archive_url = %url,
                        read_only = true,
                        "built the controller's one archive handle; it cannot write (Global \
                         Constraint 6, guard G-RET)"
                    );
                    Some(std::sync::Arc::new(store))
                }
                Err(e) => {
                    error!(
                        archive_url = %url,
                        error = %e,
                        "could not build the read-only archive handle; this controller writes no \
                         retention report"
                    );
                    None
                }
            },
        },
    };

    let rt = match tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
    {
        Ok(rt) => rt,
        Err(e) => {
            error!(error = %e, "could not build the tokio runtime");
            return ExitCode::FAILURE;
        }
    };

    rt.block_on(async {
        // In-cluster environment first, kubeconfig second — that is exactly
        // what `Client::try_default` does, and the order matters: a developer's
        // kubeconfig must never win inside a pod.
        let client = match kube::Client::try_default().await {
            Ok(client) => client,
            Err(e) => {
                error!(
                    error = %e,
                    "no Kubernetes client: neither the in-cluster environment nor a kubeconfig \
                     resolved"
                );
                return ExitCode::FAILURE;
            }
        };

        // THE RECONCILER-REGISTRATION POINT. Every chain-O task from Task 16
        // onward appends ONE line here — `controllers.push(Box::pin(…));` —
        // and nothing else in this file changes. Keeping it a plain `Vec` is
        // what makes those serial edits one-line additions instead of a
        // rewrite of `main`.
        //
        // Task 16 pushed the first two, so the `#[allow(unused_mut)]` Task 15
        // left here for exactly this moment is gone. Task 18 pushed the third
        // — the `BackupSchedule` cron reconciler — as one line, which is what
        // this shape is for, and Task 17 pushed the FOURTH, the `Backup`
        // reconciler that runs each archive capture as one Job and lifts its
        // exit code onto the status. `tests/linkage.rs`'s `"controllers":4`
        // moved in this same commit.
        //
        // `Vec::new()` + `push`, AND NOT `vec![…]` — see the
        // `clippy::vec_init_then_push` allow on `run` for why.
        let mut controllers: Vec<ControllerTask> = Vec::new();
        controllers.push(Box::pin(weirkeeper::controllers::trust_roster::controller(
            client.clone(),
        )));
        controllers.push(Box::pin(weirkeeper::controllers::approval::controller(
            client.clone(),
        )));
        // Task 19 threads the ONE read-only archive handle through this call
        // rather than adding a fourth `controllers.push(…)` line: retention is
        // a REPORT on a `BackupSchedule`, refreshed by the reconciler that
        // already owns that object's status, so the registered count stays
        // three (`tests/linkage.rs` asserts `"controllers":3`).
        controllers.push(Box::pin(
            weirkeeper::controllers::backup_schedule::controller(client.clone(), archive.clone()),
        ));
        // Task 17's fix round threads the SAME one read-only handle here: the
        // `Backup` reconciler's archive oracle reads a finished run's receipt
        // and its sidecar to fill `status.windowCovered` and to detect an
        // orphaned scorecard (interface I13 and I22). `archive.clone()` is an
        // `Option<Arc<Store>>` clone, not a second constructor — the handle is
        // built exactly once, above, before this runtime existed.
        controllers.push(Box::pin(weirkeeper::controllers::backup::controller(
            client.clone(),
            archive.clone(),
        )));
        // Task 20 pushes the FIFTH — the `Restore` reconciler that admits a
        // run only against a `Verified=True` approval whose own bytes carry
        // the hash of `spec.planBytes`, recomputed here at Job-creation time,
        // and then runs it as one Job. `archive.clone()` is the same
        // `Option<Arc<Store>>` clone the two above take, not a second
        // constructor: its oracle reads the signed scorecard the run put, for
        // `status.{outcome,objectives,integrity,measured}`. `tests/linkage.rs`'s
        // `"controllers":5` moved in this same commit.
        controllers.push(Box::pin(weirkeeper::controllers::restore::controller(
            client.clone(),
            archive.clone(),
        )));

        let registered = controllers.len();
        info!(
            controllers = registered,
            default_namespace = client.default_namespace(),
            "weirkeeper started"
        );

        let handles: Vec<_> = controllers.into_iter().map(tokio::spawn).collect();

        // Exit 0 on SIGTERM: a controller that exits non-zero on an ordinary
        // `kubectl delete pod` turns a rollout into a CrashLoopBackOff.
        match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            Ok(mut sigterm) => {
                sigterm.recv().await;
                info!(controllers = registered, "SIGTERM — shutting down");
            }
            Err(e) => {
                error!(error = %e, "could not install the SIGTERM handler");
                return ExitCode::FAILURE;
            }
        }

        for h in &handles {
            h.abort();
        }
        ExitCode::SUCCESS
    })
}
