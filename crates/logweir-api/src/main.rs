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
use std::sync::Arc;
use std::time::Duration;

use hyper::body::Incoming;
use hyper_util::rt::{TokioExecutor, TokioIo, TokioTimer};
use hyper_util::server::conn::auto;
use hyper_util::server::graceful::GracefulShutdown;
use hyper_util::service::TowerToHyperService;
use tokio::sync::Semaphore;

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
    serve(listener, logweir_api::app::router(state)).await
}

/// How long a connection may take to send its request headers.
///
/// Hyper's default is NO deadline: a client that opens a connection and sends
/// one header byte a minute holds a task and a file descriptor indefinitely.
/// Ten seconds is generous for a browser on loopback and for a kubelet probe,
/// and it bounds the classic slow-loris shape.
const HEADER_READ_TIMEOUT: Duration = Duration::from_secs(10);

/// The largest request head hyper will buffer, 32 KiB.
///
/// The BODY is already bounded, in the place that knows what a body means:
/// `http::read_json` reads through `http_body_util::Limited` at
/// `MAX_JSON_BODY` (1 MiB) and answers 413. Nothing bounded the HEAD, and a
/// request line plus headers is read into a buffer before any route matches.
const MAX_HEADER_BYTES: usize = 32 * 1024;

/// The most connections served at once.
///
/// Reached, the accept loop stops taking new connections and they wait in the
/// kernel's backlog rather than each becoming a task. On a single-administrator
/// loopback listener this is a ceiling, never a working limit.
const MAX_CONNECTIONS: usize = 256;

/// How long in-flight connections have to finish after a shutdown signal.
///
/// A graceful shutdown that waits forever is not a shutdown: one client holding
/// a connection open could keep the process alive past any supervisor's patience
/// and turn a clean stop into a SIGKILL. Past this, the remaining connections
/// are dropped and the process still exits 0 — it stopped when it was told to.
const SHUTDOWN_GRACE: Duration = Duration::from_secs(10);

/// Run the router on `listener` until a shutdown signal, under the four limits
/// above.
///
/// WHY THIS IS NOT `axum::serve`. That helper builds its hyper connection as
/// `Builder::new(TokioExecutor::new())` and gives no access to it, so a
/// header-read timeout and a header-size cap cannot be set through it at all.
/// The loop below is the same shape — accept, wrap, spawn, watch for shutdown —
/// with those two configured and with a permit and a shutdown deadline added.
async fn serve(listener: tokio::net::TcpListener, router: axum::Router) -> ExitCode {
    let mut builder = auto::Builder::new(TokioExecutor::new());
    builder
        .http1()
        // The timer is not optional: hyper PANICS on a `header_read_timeout`
        // configured without one.
        .timer(TokioTimer::new())
        .header_read_timeout(HEADER_READ_TIMEOUT)
        .max_buf_size(MAX_HEADER_BYTES);

    let permits = Arc::new(Semaphore::new(MAX_CONNECTIONS));
    let graceful = GracefulShutdown::new();
    let mut shutdown = std::pin::pin!(shutdown_signal());

    loop {
        // The permit is taken BEFORE the accept, so at the ceiling the loop
        // stops accepting rather than accepting and then queueing.
        let permit = tokio::select! {
            permit = permits.clone().acquire_owned() => match permit {
                Ok(permit) => permit,
                Err(_) => break,
            },
            () = &mut shutdown => break,
        };
        let stream = tokio::select! {
            accepted = listener.accept() => match accepted {
                Ok((stream, _peer)) => stream,
                Err(error) => {
                    // A per-connection accept error (a descriptor limit, a
                    // client that vanished between SYN and accept) is not a
                    // reason to stop serving. The sleep keeps a persistent one
                    // from becoming a busy loop.
                    tracing::warn!(%error, "accept failed");
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    continue;
                }
            },
            () = &mut shutdown => break,
        };

        let service = TowerToHyperService::new(router.clone().into_service::<Incoming>());
        // `into_owned` ends the borrow of `builder`, so the connection can be
        // moved into a task while the loop keeps configuring the next one.
        let connection = builder
            .serve_connection_with_upgrades(TokioIo::new(stream), service)
            .into_owned();
        let connection = graceful.watch(connection);
        tokio::spawn(async move {
            // Held for the connection's life; dropped with it, which is what
            // returns the permit.
            let _permit = permit;
            if let Err(error) = connection.await {
                // Every HTTP-level answer is the router's; this is a transport
                // failure — a reset, or the header deadline above.
                tracing::debug!(error = %error, "connection ended");
            }
        });
    }

    // Stop accepting before waiting, or a client could keep the wait alive.
    drop(listener);
    tokio::select! {
        () = graceful.shutdown() => tracing::info!("logweir-api stopped"),
        () = tokio::time::sleep(SHUTDOWN_GRACE) => tracing::warn!(
            grace_seconds = SHUTDOWN_GRACE.as_secs(),
            "connections were still open at the shutdown deadline; dropping them"
        ),
    }
    ExitCode::SUCCESS
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
