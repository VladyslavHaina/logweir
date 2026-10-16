#![forbid(unsafe_code)]
//! The `weirkeeper` binary.
//!
//! Four argv forms and no argument parser: `--version`, `--help`,
//! `--probe live|ready`, or nothing; anything else exits 1 naming what it got.
//! `--probe` is the kubelet's exec probe: it asks the running controller's
//! loopback health listener one question and exits 0 or 1, building no client
//! ([`weirkeeper::health`]). No `clap`: Global Constraint 38 closes the
//! workspace graph, and a match on `std::env::args()` is the whole requirement.
//! `--version` is what `scripts/check-image-weirkeeper.sh` runs against the
//! built image.
//!
//! The argv match comes before any client: `--version` in a container has no
//! kubeconfig, token or API server, and would fail exactly where it is used.
//! Only the rustls provider install precedes it, unconditionally: it is no
//! client and no I/O, and without it the no-argv path aborts at exit 101 inside
//! rustls instead of logging a named error (see
//! [`weirkeeper::install_default_crypto_provider`]).

use std::process::ExitCode;

use tracing::{error, info};
use weirkeeper::controllers::ControllerTask;

const HELP: &str = "weirkeeper — the Logweir control plane. It watches the six \
logweir.dev/v1alpha1 kinds and runs each restore, drill and backup as a Job; it \
holds a read-only archive handle and verifies the DSSE signatures the UI renders, \
and it links the verifying half of the evidence machinery and never the signer. \
Takes no arguments: `--version` prints the version, `--help` prints this, \
`--probe live` or `--probe ready` asks the running controller's loopback health listener \
(LOGWEIR_HEALTH_ADDR, default 127.0.0.1:8081) and exits 0 when it answers ok, and no \
argument at all runs the controller until SIGTERM.";

fn main() -> ExitCode {
    // Before any kube client and before the argv match, so no future argv form
    // can skip it. Losing the race to another installer is `false`, not an
    // error.
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
        // The kubelet's exec probe: no client, no subscriber, no runtime, one
        // loopback connection with a two-second deadline on each step.
        ["--probe", which] => {
            let outcome = weirkeeper::health::Probe::from_arg(which).and_then(|probe| {
                let addr = weirkeeper::health::configured_addr(std::env::var(
                    weirkeeper::health::HEALTH_ADDR_ENV,
                ))?;
                weirkeeper::health::probe(addr, probe)
            });
            match outcome {
                Ok(()) => ExitCode::SUCCESS,
                Err(reason) => {
                    eprintln!("weirkeeper --probe: {reason}");
                    ExitCode::FAILURE
                }
            }
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
/// `clippy::vec_init_then_push` is allowed for the registration point below:
/// each reconciler is one `controllers.push(Box::pin(…));` line, which a
/// `vec![…]` literal would turn into edits of one expression. The allow sits on
/// the function because the lint's span is the whole statement group.
#[allow(clippy::vec_init_then_push)]
fn run() -> ExitCode {
    // JSON to stdout, filter from `RUST_LOG`: the pod log API has no stream
    // selector (Global Constraint 11), so everything a controller wants read
    // back goes to stdout in one machine-readable shape.
    tracing_subscriber::fmt()
        .json()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    // The one read-only archive handle, built before the tokio runtime exists
    // (interface I13). `Store`'s constructors drive their own current-thread
    // runtime, and `Runtime::block_on` from a thread already driving one panics
    // ("Cannot start a runtime from within a runtime"), so this must not move
    // inside `rt.block_on` or a reconcile. Built exactly once: `Arc<Store>` is
    // cloned into each controller's context and every call on it goes through
    // `tokio::task::spawn_blocking`
    // (`tests/retention.rs::no_store_call_is_made_outside_spawn_blocking`
    // asserts this file is the only construction site).
    //
    // `read_only_from_url`, never `from_url`: the handle physically cannot put
    // (Global Constraint 6, guard G-RET), and `scripts/check-no-archive-write.sh`
    // fails if the other constructor is named under `crates/weirkeeper/src/`.
    //
    // An empty value is unset: the chart may render `LOGWEIR_ARCHIVE_URL: ""`
    // (no inline archive), which is a documented switch, not a
    // misconfiguration. The decision is `retention::configured_archive_url`
    // (`tests/retention.rs::an_empty_archive_url_is_unset_and_not_an_error`);
    // this only supplies the read.
    let archive = match weirkeeper::retention::configured_archive_url(std::env::var(
        weirkeeper::retention::ARCHIVE_URL_ENV,
    )) {
        None => {
            info!(
                env = weirkeeper::retention::ARCHIVE_URL_ENV,
                "no archive configured: this controller holds no archive handle"
            );
            None
        }
        Some(url) => match weirkeeper::retention::storage_url_for(&url) {
            Err(e) => {
                // Not fatal, and named: a malformed archive URL must not stop
                // the reconcilers that need no archive.
                error!(
                    env = weirkeeper::retention::ARCHIVE_URL_ENV,
                    error = %e,
                    "the configured archive URL is not readable as an object-store location; \
                     this controller holds no archive handle"
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
                        "could not build the read-only archive handle; this controller holds no \
                         archive handle"
                    );
                    None
                }
            },
        },
    };

    // The runner image every Job this process creates will name. The compiled
    // `weirkeeper::job::RUNNER_IMAGE` is a digest of a local build that a
    // cluster which did not build it cannot start, so the chart (or an
    // operator) hands the controller its image. The decision is
    // `job::configured_runner_image`, so a test can pass the empty string an
    // empty `env:` value produces; this is the only read of
    // `weirkeeper::job::RUNNER_IMAGE_ENV` in the crate
    // (`crates/weirkeeper/tests/crd_shape.rs::main_reads_the_runner_image_override_once`).
    // One log line names the image and where it came from.
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

    // The pull policy those Jobs carry. The chart names images by the `latest`
    // tag, which `job::IMAGE_PULL_POLICY` (`Never`, right for loaded images)
    // cannot resolve, so the chart renders this beside the image. A bad value
    // refuses to start: `imagePullPolicy` is a closed set the API server
    // validates at every Job create, and one legible refusal beats a rejected
    // Job per object.
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

    // The installation's approval policies. The decision is
    // `weirkeeper::approval_policy::configured_policy`. Absent is every
    // namespace on `legacy-governed-v1`; a file that cannot be read or does not
    // validate refuses to start, because an operator who bound a namespace
    // Governed and got legacy would be told nothing.
    let approval_policies = match weirkeeper::approval_policy::configured_policy(
        std::env::var(weirkeeper::approval_policy::APPROVAL_POLICY_FILE_ENV),
        |path| std::fs::read_to_string(path),
    ) {
        Ok(set) => std::sync::Arc::new(set),
        Err(message) => {
            error!(error = %message, "refusing to start: the approval-policy document is invalid");
            return ExitCode::FAILURE;
        }
    };
    info!(
        approval_policy_digest = %approval_policies.digest(),
        bound_workspaces = ?approval_policies.bound_workspaces(),
        allow_ordinary_confirmation = approval_policies.allows_ordinary_confirmation(),
        default_mode = ?approval_policies.default_mode(),
        "the approval policies this controller enforces; an unbound workspace is \
         legacy-governed-v1 unless defaultMode or the fresh-install marker says confirm"
    );
    // Where the fresh-install marker is read: both variables or neither (the
    // release namespace and the public identity ConfigMap the identity hook
    // writes it on). Neither is an install with no managed identity.
    let marker_source = std::env::var(weirkeeper::approval_policy::IDENTITY_PUBLIC_CONFIGMAP_ENV)
        .ok()
        .filter(|v| !v.trim().is_empty())
        .zip(
            std::env::var(weirkeeper::check::policy::INSTALLATION_NAMESPACE_ENV)
                .ok()
                .filter(|v| !v.trim().is_empty()),
        );
    let marker = weirkeeper::approval_policy::MarkerHandle::default();
    let approval_policies =
        weirkeeper::approval_policy::PolicySource::new(approval_policies, marker.clone());

    // The pair, threaded as ONE value from here on.
    let runner = weirkeeper::job::RunnerImage {
        image: runner_image,
        image_pull_policy: runner_pull_policy,
    };

    // The install: one namespace, where every watch runs and every object
    // lives, and the workspaces inside it (PROD-19.1). Both are required; a
    // malformed list, or an approval binding naming a workspace the list does
    // not, refuses to start rather than leave objects unreconciled.
    let Some(install_namespace) =
        std::env::var(weirkeeper::check::policy::INSTALLATION_NAMESPACE_ENV)
            .ok()
            .filter(|v| !v.trim().is_empty())
    else {
        error!(
            env = weirkeeper::check::policy::INSTALLATION_NAMESPACE_ENV,
            "refusing to start: the install's namespace is not set"
        );
        return ExitCode::FAILURE;
    };
    let workspaces = match weirkeeper::workspace::Workspaces::configured(std::env::var(
        weirkeeper::workspace::WORKSPACES_ENV,
    )) {
        Ok(workspaces) => workspaces,
        Err(message) => {
            error!(error = %message, "refusing to start: the workspace list is malformed");
            return ExitCode::FAILURE;
        }
    };
    if let Some(unlisted) = approval_policies
        .effective()
        .bound_workspaces()
        .into_iter()
        .find(|bound| !workspaces.list().iter().any(|w| w.as_str() == bound))
    {
        error!(
            workspace = %unlisted,
            "refusing to start: the approval policy binds a workspace that is not in {}",
            weirkeeper::workspace::WORKSPACES_ENV
        );
        return ExitCode::FAILURE;
    }
    info!(
        namespace = %install_namespace,
        workspaces = %workspaces
            .list()
            .iter()
            .map(weirkeeper::workspace::Workspace::as_str)
            .collect::<Vec<_>>()
            .join(","),
        "watching one namespace; cluster-scoped trust kinds are read cluster-wide"
    );
    weirkeeper::workspace::install(install_namespace, workspaces);

    // The health listener's address. The decision is
    // `weirkeeper::health::configured_addr`; a non-loopback address refuses to
    // start rather than publish the listener.
    let health_addr = match weirkeeper::health::configured_addr(std::env::var(
        weirkeeper::health::HEALTH_ADDR_ENV,
    )) {
        Ok(addr) => addr,
        Err(message) => {
            error!(error = %message, "refusing to start: the health listener address is invalid");
            return ExitCode::FAILURE;
        }
    };
    let health = weirkeeper::health::Health::new();

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
        // The listener comes up first, so `--probe live` answers while the
        // client is built and `--probe ready` says `starting` until every
        // controller is registered. A port that cannot be bound refuses to
        // start: inside a pod it is a configuration error worth a restart loop
        // that names it.
        let listener = match weirkeeper::health::bind(health_addr).await {
            Ok(listener) => listener,
            Err(e) => {
                error!(addr = %health_addr, error = %e, "could not bind the health listener");
                return ExitCode::FAILURE;
            }
        };
        info!(
            addr = %listener.local_addr().map_or(health_addr, |a| a),
            "health listener: /livez and /readyz, loopback only, for `weirkeeper --probe`"
        );
        tokio::spawn(weirkeeper::health::serve(listener, health.clone()));

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

        // The fresh-install marker, re-read every `MARKER_POLL_SECONDS` into
        // the one handle every Approval and Restore reconcile resolves against.
        // Not a reconciler: it writes nothing.
        if let Some((configmap, namespace)) = marker_source.clone() {
            info!(
                configmap = %format!("{namespace}/{configmap}"),
                "reading the installation's fresh-install marker"
            );
            tokio::spawn(weirkeeper::approval_policy::poll_marker(
                client.clone(),
                namespace,
                configmap,
                marker.clone(),
            ));
        }

        // The reconciler-registration point: one `controllers.push` line per
        // reconciler (see the `clippy::vec_init_then_push` allow on `run`).
        // `tests/linkage.rs` counts them.
        let mut controllers: Vec<ControllerTask> = Vec::new();
        controllers.push(Box::pin(weirkeeper::controllers::trust_roster::controller(
            client.clone(),
        )));
        // The `TrustPolicy` reconciler takes only the client: it holds no
        // archive handle and creates no Job.
        controllers.push(Box::pin(weirkeeper::controllers::trust_policy::controller(
            client.clone(),
        )));
        controllers.push(Box::pin(weirkeeper::controllers::approval::controller(
            client.clone(),
            approval_policies.clone(),
        )));
        // The schedule reconciler takes the handle on its context and does
        // not read it.
        controllers.push(Box::pin(
            weirkeeper::controllers::backup_schedule::controller(client.clone(), archive.clone()),
        ));
        // The same read-only handle (an `Option<Arc<Store>>` clone, not a
        // second constructor): the `Backup` reconciler reads a finished run's
        // receipt and sidecar for `status.windowCovered` and to detect an
        // orphaned scorecard.
        controllers.push(Box::pin(weirkeeper::controllers::backup::controller(
            client.clone(),
            archive.clone(),
            runner.clone(),
        )));
        // The `Restore` reconciler admits a run only against a `Verified=True`
        // approval whose bytes carry the hash of `spec.planBytes`, and reads the
        // signed scorecard through the same handle.
        controllers.push(Box::pin(weirkeeper::controllers::restore::controller(
            client.clone(),
            archive.clone(),
            runner.clone(),
            approval_policies.clone(),
        )));
        // The `KafkaCluster` probe reconciler creates probe Jobs and reads no
        // archive, so it takes no handle.
        controllers.push(Box::pin(
            weirkeeper::controllers::kafka_cluster::controller(client.clone(), runner.clone()),
        ));
        // The `BackupDestination` reconciler creates no Job and reads no
        // archive, so a client is all it needs.
        controllers.push(Box::pin(
            weirkeeper::controllers::backup_destination::controller(client.clone()),
        ));
        // The `TopicDiscovery` reconciler creates check Jobs, so it takes the
        // runner image, and the installation policy reference, whose
        // attestations can upgrade a result to `attestedComplete`. No archive
        // handle.
        controllers.push(Box::pin(
            weirkeeper::controllers::topic_discovery::controller(
                client.clone(),
                runner.clone(),
                weirkeeper::controllers::topic_discovery::configured_policy_ref(),
            ),
        ));
        // The `RecoveryCatalog` reconciler creates sync Jobs and no archive
        // handle: the sync reads object storage with the destination's own
        // credential inside the Job.
        controllers.push(Box::pin(
            weirkeeper::controllers::recovery_catalog::controller(client.clone(), runner.clone()),
        ));
        // The `Preflight` reconciler creates check Jobs in the execution pod's
        // shape; every archive read happens inside that pod. Its verdict
        // authorises nothing (D2 §6.8).
        controllers.push(Box::pin(weirkeeper::controllers::preflight::controller(
            client.clone(),
            runner.clone(),
        )));
        // The `ProtectionPolicy` reconciler creates no Job and holds no archive
        // handle: it reads `Backup` status and the catalog's view.
        controllers.push(Box::pin(
            weirkeeper::controllers::protection_policy::controller(client.clone()),
        ));
        // The `RetentionPolicy` reconciler creates one deletion Job (running
        // `logweir-retention`) only once an administrator selected `Enforce` and
        // approved the current plan digest. No archive handle: this process
        // neither reads nor deletes an object, and
        // `scripts/check-no-archive-write.sh` proves the deleting crate is
        // unreachable from here.
        controllers.push(Box::pin(
            weirkeeper::controllers::retention_policy::controller(client.clone(), runner.clone()),
        ));
        // The `Switchover` reconciler (PROD-04.2) creates one runner Job per
        // admitted object, with the restore's target and evidence access; it
        // reads no archive and never dials Kafka.
        controllers.push(Box::pin(weirkeeper::controllers::switchover::controller(
            client.clone(),
            runner.clone(),
            approval_policies.clone(),
        )));

        let registered = controllers.len();
        info!(
            controllers = registered,
            default_namespace = client.default_namespace(),
            "weirkeeper started"
        );

        // Each task is watched: one that RETURNS leaves a kind unreconciled
        // behind a pod that still reads Running, so it fails `/livez` and the
        // kubelet restarts the pod. The number is the registration order above.
        let handles: Vec<_> = controllers
            .into_iter()
            .enumerate()
            .map(|(index, task)| {
                tokio::spawn(weirkeeper::health::watched(
                    format!("#{}", index + 1),
                    task,
                    health.clone(),
                ))
            })
            .collect();
        health.mark_started();

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
