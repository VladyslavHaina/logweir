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
    //
    // AN EMPTY VALUE IS UNSET, AND THAT IS PLAN ERRATUM **E19(e)**. The
    // shipped `config/manager/deployment.yaml` carries
    // `LOGWEIR_ARCHIVE_URL: ""` — the default install, with retention and
    // verification display switched off — and this match used to send that
    // empty string to `storage_url_for`, which correctly refused a URL with no
    // `://` and made the controller log an **ERROR** on every clean start. An
    // empty archive URL is a DOCUMENTED SWITCH, not a misconfiguration, and it
    // reads as the absent variable it means. (Invisible until Task 24 pinned
    // `RUST_LOG: info` on the same Deployment, which is how a wrong ERROR line
    // survives: nothing was printing it.)
    //
    // THE PREDICATE ITSELF LIVES IN `retention::configured_archive_url` and
    // this function only supplies the read — a two-line extraction made in
    // Task 24's fix round, because a decision behind `fn main` is reachable
    // from no test at all, and `crates/weirkeeper/tests/retention.rs::
    // an_empty_archive_url_is_unset_and_not_an_error` is the row that now
    // holds it.
    let archive = match weirkeeper::retention::configured_archive_url(std::env::var(
        weirkeeper::retention::ARCHIVE_URL_ENV,
    )) {
        None => {
            info!(
                env = weirkeeper::retention::ARCHIVE_URL_ENV,
                "no archive configured: this controller holds no archive handle and writes no \
                 retention report"
            );
            None
        }
        Some(url) => match weirkeeper::retention::storage_url_for(&url) {
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

    // THE RUNNER IMAGE THIS PROCESS WILL PUT IN EVERY JOB IT CREATES — Task
    // 33, and the SECOND thing this file reads out of the environment.
    //
    // WHY IT EXISTS. `weirkeeper::job::RUNNER_IMAGE` is a compile-time digest,
    // measured on the machine that built the image, and plan erratum E19(a)
    // says a local digest changes on every build. On the laptop that is
    // survivable — the operator's own build IS the pinned bytes, and one
    // `docker tag` puts them under the pinned repository name (E19b). On a
    // cluster that did not build the pins it is not: a GitHub runner builds
    // both images minutes before the demo, at digests nothing in the tree
    // names, and the shipped repository is not pullable (Global Constraint 37).
    // A controller that could only ever name the compile-time pin would create
    // Jobs no such node can start.
    //
    // THE SAME ARRANGEMENT AS THE ARCHIVE URL ABOVE, AND FOR THE SAME REASON:
    // the read is here and the DECISION is a pure predicate in the library, so
    // a test can hand it the empty string a Kubernetes `env:` entry with an
    // empty `value:` actually produces (plan erratum E19(e)) without touching
    // process-global state. This is the ONLY `std::env::var` of
    // `weirkeeper::job::RUNNER_IMAGE_ENV` in the crate, and
    // `crates/weirkeeper/tests/crd_shape.rs::main_reads_the_runner_image_override_once`
    // asserts exactly that.
    //
    // ONE LINE, NAMING THE IMAGE AND WHERE IT CAME FROM. A controller that
    // silently used a different image from the one the install file names is
    // the failure this variable is meant to fix, not to create.
    let runner_image =
        weirkeeper::job::configured_runner_image(std::env::var(weirkeeper::job::RUNNER_IMAGE_ENV));
    info!(
        runner_image = runner_image
            .as_deref()
            .unwrap_or(weirkeeper::job::RUNNER_IMAGE),
        source = if runner_image.is_some() {
            weirkeeper::job::RUNNER_IMAGE_ENV
        } else {
            "shipped pin"
        },
        "the runner image every Job this controller creates will name"
    );

    // AND THE PULL POLICY THOSE JOBS WILL CARRY — Task 37, and the THIRD thing
    // this file reads out of the environment.
    //
    // WHY IT EXISTS. The owner decided, on 2026-09-12, that `charts/logweir`'s
    // defaults name the two Logweir images by the `latest` TAG rather than by a
    // digest. A mutable tag under `weirkeeper::job::IMAGE_PULL_POLICY` — which
    // is `Never`, and stays the compiled-in default for every path that LOADS
    // an image onto the node — is a reference the kubelet cannot resolve, so
    // the policy had to become configurable alongside the image. The chart
    // renders this variable from its own `runnerImagePullPolicy` value beside
    // the image one, so the two halves of one decision travel together.
    //
    // SAME ARRANGEMENT AS THE TWO READS ABOVE: the read is here and the
    // DECISION is a pure predicate in the library, so a test can hand it the
    // empty string an `env:` entry with an empty `value:` actually produces
    // (plan erratum E19(e)) without touching process-global state.
    //
    // AND ON A BAD VALUE THIS PROCESS REFUSES TO START. `imagePullPolicy` is a
    // closed set the API server validates at Job CREATE, so a controller that
    // started under `always` or `Sometimes` would turn every `Backup` and every
    // `Restore` into a rejected Job, forever, with nothing but an API error per
    // object to say why. One legible refusal at startup is the whole of the
    // difference.
    let runner_pull_policy = match weirkeeper::job::configured_runner_pull_policy(std::env::var(
        weirkeeper::job::RUNNER_PULL_POLICY_ENV,
    )) {
        Ok(policy) => policy,
        Err(message) => {
            error!(error = %message, "refusing to start: the runner pull policy is not a policy");
            return ExitCode::FAILURE;
        }
    };
    info!(
        runner_pull_policy = runner_pull_policy
            .as_deref()
            .unwrap_or(weirkeeper::job::IMAGE_PULL_POLICY),
        source = if runner_pull_policy.is_some() {
            weirkeeper::job::RUNNER_PULL_POLICY_ENV
        } else {
            "shipped constant"
        },
        "the runner pull policy every Job this controller creates will carry"
    );

    // The pair, threaded as ONE value from here on.
    let runner = weirkeeper::job::RunnerImage {
        image: runner_image,
        image_pull_policy: runner_pull_policy,
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
        // PLAT-19.1 / decision D3 §7.1 — ONE LINE, at the registration point
        // this file's header reserves for exactly that. The `TrustPolicy`
        // reconciler parses each key, resolves it against the clock, reports
        // which namespaces this policy governs and which two policies contest,
        // and writes `Superseded` onto the roster it replaces. It holds no
        // archive handle and creates no Job, so it takes only the client.
        controllers.push(Box::pin(weirkeeper::controllers::trust_policy::controller(
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
            runner.clone(),
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
            runner.clone(),
        )));
        // Task 15c pushes the SIXTH — the `KafkaCluster` probe reconciler that
        // makes `status.reachable` mean something, by running `logweir
        // cluster-probe` as a short-lived Job and reading interface I14's two
        // stdout lines off the pod log. It takes NO archive handle: a probe
        // reads no archive, so it is the one controller here whose context
        // needs nothing but a client. `tests/linkage.rs`'s `"controllers":6`
        // moved in this same commit.
        controllers.push(Box::pin(
            weirkeeper::controllers::kafka_cluster::controller(client.clone(), runner.clone()),
        ));
        // D2 W7 pushes the SEVENTH — the `BackupDestination` reconciler that
        // publishes each saved destination's `Valid` condition, canonical URL
        // and two digests. It takes NEITHER the archive handle NOR the runner
        // image: it creates no Job and reads no archive, so a client is all its
        // context needs. `tests/linkage.rs`'s `"controllers":7` moved in this
        // same commit (D2 §13.4: W8 and W9 take it to 8 and 9).
        controllers.push(Box::pin(
            weirkeeper::controllers::backup_destination::controller(client.clone()),
        ));
        // D2 W8 pushes the NINTH — the `TopicDiscovery` reconciler that turns
        // one bounded discovery request into one isolated check Job, stores the
        // inventory as owned immutable ConfigMap chunks and publishes the
        // completeness verdict. It takes the runner image, because it CREATES
        // Jobs, and the installation policy reference, because the attestation
        // that can upgrade a result to `attestedComplete` lives in a ConfigMap
        // only a release-namespace administrator can write. It takes no archive
        // handle: a discovery reads no archive. `tests/linkage.rs`'s
        // `"controllers":8` moved to 9 in this same commit (D2 §13.4: W9 takes
        // it further).
        controllers.push(Box::pin(
            weirkeeper::controllers::topic_discovery::controller(
                client.clone(),
                runner.clone(),
                weirkeeper::controllers::topic_discovery::configured_policy_ref(),
            ),
        ));
        // D3 W8 pushes the TENTH — the `RecoveryCatalog` reconciler that runs a
        // bounded `catalogSync` check Job and materialises its result as an
        // immutable, Job-owned, TTL-collected view. It takes the runner image
        // (it creates a Job) and NO archive handle: the sync reads object
        // storage with the destination's own credential inside that Job, never
        // from this process. `tests/linkage.rs`'s `"controllers":10` moved in
        // this same commit (PLAT-19.1's `trust_policy` took it to eight and
        // D2 W8's `topic_discovery` to nine).
        controllers.push(Box::pin(
            weirkeeper::controllers::recovery_catalog::controller(client.clone(), runner.clone()),
        ));

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
