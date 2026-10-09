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
use hyper::server::conn::http1;
use hyper_util::rt::{TokioIo, TokioTimer};
use hyper_util::server::graceful::GracefulShutdown;
use hyper_util::service::TowerToHyperService;
use logweir_api::transport::{Admission, PeerLimit, StallBody, StallGuard};
use tokio::sync::Semaphore;

const HELP: &str = "logweir-api — the bounded Logweir product API. Serves the static UI at /ui/ \
and a typed JSON API at /api/v1 on one origin, and creates Logweir custom resources through one \
Kubernetes adapter. It is not a Kubernetes proxy. Usage: `logweir-api --config <file>`; \
`--version` prints the version, `--help` prints this. `mode: localAdmin` binds loopback \
addresses only; `mode: shared` requires an HTTPS publicBaseUrl, an OIDC issuer, mounted \
session and cursor keys, and exact role bindings.";

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
        // NOT a bare `try_from_default_env`: see `audit::SILENCED_TARGETS`. A
        // dependency that logs the upstream error body at DEBUG would undo
        // this service's redaction the moment someone set RUST_LOG=debug.
        .with_env_filter(logweir_api::audit::log_filter())
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
    // What the provider's TLS certificate is verified against (chart gap G1),
    // read before the preflight is consumed: how many private anchors the
    // bundle added, and whether the system roots are consulted too.
    let (oidc_extra_roots, oidc_system_roots) = preflight.shared.as_ref().map_or((0, true), |s| {
        (
            s.tls_trust.extra_root_count(),
            s.tls_trust.uses_system_roots(),
        )
    });
    let state = match logweir_api::state_from_parts(&config, preflight, client) {
        Ok(state) => state,
        Err(reason) => {
            tracing::error!(%reason, "refusing to start");
            return ExitCode::FAILURE;
        }
    };
    // CHART GAP G6: the ingress controller's serving endpoints, re-read every
    // few seconds, are the entry point's trusted peers. The first read happens
    // before the first request can arrive in practice; until it lands,
    // `/readyz` is false and a browser request is 421.
    if let Some(shared) = state.shared() {
        if let Some(service) = shared.trusted_proxies.service() {
            tracing::info!(
                namespace = %service.namespace,
                service = %service.name,
                "trusting the serving endpoints of this Service as the entry point's proxy"
            );
            tokio::spawn(std::sync::Arc::clone(&shared.trusted_proxies).run(state.kube().clone()));
        }
    }
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
        mode = config.mode.as_str(),
        namespaces = config.namespaces.len(),
        ui_assets = state.assets().paths().len(),
        role_bindings = config.shared().map_or(0, |s| s.roles.bindings.len()),
        binding_revision = config.shared().map_or("", |s| s.roles.revision.as_str()),
        issuer = config.shared().map_or("", |s| s.oidc.issuer.as_str()),
        oidc_extra_roots,
        oidc_system_roots,
        // PLAT-19.2 readiness (D0: "API and controller consume the same content
        // hash and expose it"): the controller logs the same digest at start.
        approval_policy_digest = %state.approval().policies.digest(),
        confirmation_key_id = state
            .approval()
            .confirmation
            .as_ref()
            .map_or("", |k| k.key_id()),
        // PROD-16.1: a managed key the identity hook has not written yet is
        // read on first use; and the fresh-install marker is read per request.
        confirmation_key_pending = state.approval().pending_confirmation_file.is_some(),
        installation_marker_source = state
            .approval()
            .installation
            .as_ref()
            .map_or(String::new(), |i| format!("{}/{}", i.namespace, i.config_map)),
        "logweir-api started"
    );
    // FX-24c: the per-peer cap exists only where a trusted-proxy set does, so
    // the ingress is never capped as one peer (see `PeerLimit`). localAdmin
    // mode has no such set, and every peer it serves is this machine anyway.
    let peer_limit = state
        .shared()
        .and_then(|shared| PeerLimit::outside(&shared.trusted_proxies, MAX_CONNECTIONS_PER_PEER));
    serve(listener, logweir_api::app::router(state), peer_limit).await
}

/// How long a connection may take to send its request headers, counted from
/// the moment it is accepted — so a connection that sends NOTHING is closed at
/// this deadline too — and again from the end of each answer on a keep-alive
/// connection.
///
/// Without one, a client that opens a connection and sends one header byte a
/// minute holds a task, a file descriptor and a connection permit
/// indefinitely. Ten seconds is generous for a browser on loopback, for an
/// ingress controller's pooled connection and for a kubelet probe, and it
/// bounds the classic slow-loris shape (review finding R4) and the silent
/// socket (FX-24). See [`serve`] on why the deadline starts at the accept.
const HEADER_READ_TIMEOUT: Duration = Duration::from_secs(10);

/// The progress window on a connection waiting on its client: a pending
/// write of an answer the client is not reading fast enough, or a pending read
/// of a request body it is not sending fast enough (FX-24b; the floor,
/// [`IO_MIN_PROGRESS`], is FX-24c's). A window that passes without its floor
/// of progress fails the operation `TimedOut`, the connection ends and its
/// permit comes back. See `logweir_api::transport` for the two guards and why
/// the connection's other reads are not timed.
///
/// A WINDOW, NOT A TOTAL, AND NOT THE HEADER DEADLINE. A window opens only
/// while a write or a body read is pending, and closes once the floor has
/// moved or the output has caught up, so a slow but steady reader keeps its
/// connection for as long as the answer takes, and an event stream between
/// heartbeats (a write every 15 s, nothing pending in between) never opens
/// one. The header deadline is ten seconds because a client that has sent
/// nothing has no excuse; this one is longer because a client in the middle of
/// a transfer can be held up by the network: a lossy link's retransmission
/// backoff alone can stall a live TCP connection for well over ten seconds.
/// Thirty seconds is past that, still under the ingress controllers' common
/// 60-second response timeouts, and it is how long 256 clients that stopped
/// reading, or that read below the floor, can hold every connection slot,
/// which is the outage this bounds. Each test row that measures it reads it
/// back out of this file.
const IO_STALL_TIMEOUT: Duration = Duration::from_secs(30);

/// The least a connection that is behind must move in each
/// [`IO_STALL_TIMEOUT`] window: 32 KiB, about 1.1 KiB/s or 9 kbit/s (FX-24c).
///
/// WHY A FLOOR. With FX-24b's window closed by any progress at all, a client
/// that read one byte every twenty seconds kept its connection past 100 s
/// (FX-24b review, measured on the built binary), so 256 such clients held
/// every connection for the price of a dozen bytes a minute each.
///
/// WHY THIS FLOOR. It is below a 9.6 kbit/s GSM data call, the slowest link a
/// browser has used in twenty years, so a real client that is reading never
/// meets it: a slow mobile client is behind the ingress, which reads the
/// answer as fast as its own buffers let it, and the console sees the
/// client's rate only once those are full. It is far above what a
/// deliberately slow client wants to spend: to hold every connection with
/// reads at the floor takes 256 × 1.1 KiB/s, about 2.2 Mbit/s of reading,
/// sustained, and at least [`MAX_CONNECTIONS`] / [`MAX_CONNECTIONS_PER_PEER`]
/// addresses wherever the per-peer cap applies. A body counts the same way:
/// one still arriving after a window must have brought the floor in it.
const IO_MIN_PROGRESS: usize = 32 * 1024;

/// The most connections one peer outside the trusted-proxy set may hold at
/// once (FX-24c; `logweir_api::transport::PeerLimit`). A connection over it is
/// closed as soon as it is accepted.
///
/// A peer that is not the ingress is one machine — a kubelet probe, a
/// `kubectl port-forward`, a pod — and a browser opens at most six HTTP/1.1
/// connections to a host, so 32 is five browsers' worth. It is one eighth of
/// [`MAX_CONNECTIONS`]: no single address outside the set can hold more than
/// that share, at any rate. The ingress, inside the set, is never capped.
const MAX_CONNECTIONS_PER_PEER: usize = 32;

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

/// Run the router on `listener` until a shutdown signal, under the limits
/// above, and `peer_limit`'s cap when there is one.
///
/// WHY THIS IS NOT `axum::serve`. That helper builds its hyper connection as
/// `Builder::new(TokioExecutor::new())` and gives no access to it, so a
/// header-read timeout and a header-size cap cannot be set through it at all.
/// The loop below is the same shape — accept, wrap, spawn, watch for shutdown —
/// with those two configured and with a permit and a shutdown deadline added.
///
/// HTTP/1.1 ONLY, AND THAT IS WHAT BOUNDS THE FIRST BYTE (FX-24). This loop
/// used hyper-util's `auto` builder, which reads a connection's first bytes to
/// choose HTTP/1 or HTTP/2 — with no timer — and only then builds the HTTP/1
/// connection that owns [`HEADER_READ_TIMEOUT`]. A socket that sent nothing
/// therefore never reached a deadline: measured on the built binary
/// (2026-10-08), a partial head closed at 10.0 s and a silent socket was still
/// open at 16 s, and 256 silent sockets held every permit until the client let
/// go. A timeout around that version read alone would not have been enough,
/// because hyper's HTTP/2 server has no header deadline of its own: a client
/// that sent the 24-byte HTTP/2 preface and stopped was still open at 16 s
/// too, on the same binary. hyper's HTTP/1 connection arms the deadline in its
/// first read of the head, which runs when the spawned task first polls it,
/// so serving HTTP/1 directly bounds zero bytes, a partial head and an idle
/// keep-alive alike, with one mechanism.
///
/// Nothing that talks to this listener speaks HTTP/2: browsers use it only
/// over TLS, and this listener is plain HTTP (TLS terminates at the ingress in
/// shared mode, and localAdmin mode is loopback); an ingress controller dials
/// a plain-HTTP backend with HTTP/1.1 unless told the Service speaks `h2c`,
/// which the chart never says; kubelet probes are HTTP/1.1. A client that
/// tries HTTP/2 with prior knowledge is closed at its first request line,
/// because `PRI * HTTP/2.0` is not an HTTP/1 request.
async fn serve(
    listener: tokio::net::TcpListener,
    router: axum::Router,
    peer_limit: Option<Arc<PeerLimit>>,
) -> ExitCode {
    let mut builder = http1::Builder::new();
    builder
        // The timer is not optional: without one hyper ignores the default
        // deadline and PANICS on a configured one.
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
        let (stream, peer) = tokio::select! {
            accepted = listener.accept() => match accepted {
                Ok((stream, peer)) => (stream, peer),
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

        // THE PEER ADDRESS, PER CONNECTION. It is the only client address this
        // service ever trusts: the rate limiter keys on it and the audit line
        // records it, while `X-Forwarded-For` is recorded only when this peer
        // is inside a configured trusted-proxy range and is read by no decision
        // anywhere.
        let peer_ip = peer.ip();

        // THE PER-PEER CAP (FX-24c), before anything is read: a peer outside
        // the trusted-proxy set that already holds its share has this
        // connection closed now, and the permit above comes back with it.
        let peer_slot = match peer_limit.as_ref().map(|limit| limit.admit(peer_ip)) {
            Some(Admission::Refused) => {
                drop(stream);
                continue;
            }
            Some(Admission::Counted(slot)) => Some(slot),
            Some(Admission::Trusted) | None => None,
        };

        let router_for_connection = router.clone();
        let service = TowerToHyperService::new(tower::service_fn(
            move |mut request: hyper::Request<Incoming>| {
                request
                    .extensions_mut()
                    .insert(logweir_api::http::PeerAddr(peer_ip));
                // THE BODY'S PROGRESS WINDOW (FX-24b, FX-24c), for every
                // route and whoever reads the body: a handler waiting on a
                // body the client stopped sending, or trickles, gets
                // `TimedOut` instead of waiting for ever. `http::read_json`
                // adds a total on top.
                let request =
                    request.map(|body| StallBody::new(body, IO_STALL_TIMEOUT, IO_MIN_PROGRESS));
                let router = router_for_connection.clone();
                async move {
                    use tower::ServiceExt as _;
                    router
                        .into_service::<StallBody<Incoming>>()
                        .oneshot(request)
                        .await
                }
            },
        ));
        // No `with_upgrades`: no route here switches protocols (the event
        // stream is server-sent events, an ordinary HTTP/1.1 response), so a
        // request asking to upgrade is answered like any other.
        //
        // THE OUTPUT'S PROGRESS WINDOW (FX-24b, FX-24c): an answer the client
        // has stopped reading, or reads below the floor, fails its pending
        // write at the window's end, and the connection ends with it — the
        // permit below comes back then.
        let io = StallGuard::new(TokioIo::new(stream), IO_STALL_TIMEOUT, IO_MIN_PROGRESS);
        let connection = builder.serve_connection(io, service);
        let connection = graceful.watch(connection);
        tokio::spawn(async move {
            // Held for the connection's life; dropped with it, which is what
            // returns the permit, and the peer's place under its cap.
            let _permit = permit;
            let _peer_slot = peer_slot;
            if let Err(error) = connection.await {
                // Every HTTP-level answer is the router's; this is a transport
                // failure — a reset, the header deadline or the stall deadline
                // above.
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
