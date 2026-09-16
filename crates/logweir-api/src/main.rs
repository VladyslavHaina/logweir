#![forbid(unsafe_code)]
//! The `logweir-api` binary.
//!
//! `logweir-api --config <file>` runs the service; `--version` and `--help`
//! print and exit 0 without reading anything. Exit codes: 0 after a clean
//! SIGTERM/SIGINT shutdown, 2 when the configuration is refused (before any
//! Kubernetes client or socket exists), 1 for any other startup or runtime
//! failure.
//!
//! THE ORDER IS THE SECURITY PROPERTY. The configuration is validated first —
//! including the refusal of any non-loopback listener in localAdmin mode — so
//! a refused configuration never binds a socket or builds a client.

use std::path::PathBuf;
use std::process::ExitCode;

const HELP: &str = "logweir-api — the bounded Logweir product API. Serves the static UI at /ui/ \
and a typed JSON API at /api/v1 on one loopback origin, and creates Logweir custom resources \
through one Kubernetes adapter. It is not a Kubernetes proxy. Usage: `logweir-api --config \
<file>`; `--version` prints the version, `--help` prints this. Only `mode: localAdmin` is \
accepted in this release, and it binds loopback addresses only.";

fn main() -> ExitCode {
    let argv: Vec<String> = std::env::args().skip(1).collect();
    let flags: Vec<&str> = argv.iter().map(String::as_str).collect();
    let config_path = match flags.as_slice() {
        ["--version"] => {
            println!("logweir-api {}", env!("CARGO_PKG_VERSION"));
            return ExitCode::SUCCESS;
        }
        ["--help"] => {
            println!("{HELP}");
            return ExitCode::SUCCESS;
        }
        ["--config", path] => PathBuf::from(path),
        _ => {
            eprintln!("logweir-api: expected `--config <file>`, `--version` or `--help`");
            return ExitCode::from(2);
        }
    };

    let config = match logweir_api::config::Config::load(&config_path) {
        Ok(config) => config,
        Err(error) => {
            eprintln!("logweir-api: refusing to start: {error}");
            return ExitCode::from(2);
        }
    };

    let preflight = match logweir_api::preflight(&config) {
        Ok(preflight) => preflight,
        Err(reason) => {
            eprintln!("logweir-api: refusing to start: {reason}");
            return ExitCode::from(2);
        }
    };

    tracing_subscriber::fmt()
        .json()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let runtime = match tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
    {
        Ok(runtime) => runtime,
        Err(error) => {
            eprintln!("logweir-api: cannot build the runtime: {error}");
            return ExitCode::FAILURE;
        }
    };
    runtime.block_on(run(config, preflight))
}

async fn run(config: logweir_api::config::Config, preflight: logweir_api::Preflight) -> ExitCode {
    let client = match logweir_api::kube::build_client(&config.kubernetes).await {
        Ok(client) => client,
        Err(reason) => {
            tracing::error!(%reason, "refusing to start: no Kubernetes client");
            return ExitCode::FAILURE;
        }
    };
    let state = logweir_api::state_from_parts(&config, preflight, client);
    let listener = match tokio::net::TcpListener::bind(config.listen).await {
        Ok(listener) => listener,
        Err(error) => {
            tracing::error!(listen = %config.listen, %error, "cannot bind the listener");
            return ExitCode::FAILURE;
        }
    };
    tracing::info!(
        listen = %config.listen,
        public_origin = %config.public_origin,
        mode = "localAdmin",
        namespaces = config.namespaces.len(),
        ui_assets = state.assets().paths().len(),
        "logweir-api started"
    );
    let app = logweir_api::app::router(state);
    match axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await
    {
        Ok(()) => {
            tracing::info!("logweir-api stopped");
            ExitCode::SUCCESS
        }
        Err(error) => {
            tracing::error!(%error, "the server failed");
            ExitCode::FAILURE
        }
    }
}

async fn shutdown_signal() {
    let terminate = async {
        match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            Ok(mut signal) => {
                signal.recv().await;
            }
            Err(_) => std::future::pending::<()>().await,
        }
    };
    tokio::select! {
        _ = terminate => {}
        _ = tokio::signal::ctrl_c() => {}
    }
}
