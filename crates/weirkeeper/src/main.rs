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
//! ONE THING PRECEDES THE ARGV MATCH: THE rustls PROVIDER INSTALL. It is not a
//! client and it is not I/O — it builds a struct and sets a `OnceLock` — and
//! it has to be unconditional, because it is the difference between this
//! binary's no-argv path logging a named error and ABORTING at exit 101 inside
//! rustls (review finding H1). See
//! [`weirkeeper::install_default_crypto_provider`] for the mechanism and
//! `Cargo.toml`'s `rustls` entry for why the provider is `ring`. `--version`'s
//! guarantee is unchanged: it still builds no client and touches no cluster,
//! which is what Task 23's `scripts/check-image-weirkeeper.sh` check 2 runs.

use std::future::Future;
use std::pin::Pin;
use std::process::ExitCode;

use tracing::{error, info};

/// One registered reconciler, spawned for the life of the process.
type ControllerTask = Pin<Box<dyn Future<Output = ()> + Send>>;

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
fn run() -> ExitCode {
    // JSON to stdout, filter from `RUST_LOG`: the pod log API has no stream
    // selector (Global Constraint 11), so everything a controller wants read
    // back goes to stdout in one machine-readable shape.
    tracing_subscriber::fmt()
        .json()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

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
        // `mut` is unused until Task 16 pushes the first reconciler; the
        // binding is written in its final shape now rather than being reshaped
        // by whichever task happens to be first.
        #[allow(unused_mut)]
        let mut controllers: Vec<ControllerTask> = Vec::new();

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
