//! Chart gap G5: the controller answers a liveness and a readiness probe, and
//! the liveness probe FAILS when the runtime is wedged or a controller task
//! has ended — the two states a kubelet restart can fix.
//!
//! Every socket is a `127.0.0.1` listener this file binds; every probe carries
//! the module's two-second deadline, so no row can hang.

use std::net::SocketAddr;
use std::time::{Duration, Instant};

use weirkeeper::health::{probe, serve, watched, Health, Probe};

/// A current-thread runtime on its own OS thread, the controller's shape, with
/// the health listener spawned on it. Returns the address and a handle that
/// runs `extra` on the same runtime.
fn controller_like_runtime(
    health: std::sync::Arc<Health>,
) -> (SocketAddr, tokio::sync::mpsc::UnboundedSender<Duration>) {
    let (addr_tx, addr_rx) = std::sync::mpsc::channel();
    let (block_tx, mut block_rx) = tokio::sync::mpsc::unbounded_channel::<Duration>();
    std::thread::spawn(move || {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("a runtime");
        rt.block_on(async move {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
                .await
                .expect("a loopback port");
            addr_tx.send(listener.local_addr().unwrap()).unwrap();
            tokio::spawn(serve(listener, health));
            // A "reconciler" that, when told to, blocks the one runtime thread
            // the way a synchronous call inside a reconcile would.
            while let Some(block) = block_rx.recv().await {
                std::thread::sleep(block);
            }
        });
    });
    let addr = addr_rx
        .recv_timeout(Duration::from_secs(5))
        .expect("the listener came up");
    (addr, block_tx)
}

#[test]
fn live_from_the_start_ready_once_started() {
    let health = Health::new();
    let (addr, _block) = controller_like_runtime(health.clone());
    probe(addr, Probe::Live).expect("live as soon as the listener answers");
    let starting = probe(addr, Probe::Ready).expect_err("not ready before the controllers");
    assert!(
        starting.contains("503") && starting.contains("starting"),
        "{starting}"
    );
    health.mark_started();
    probe(addr, Probe::Ready).expect("ready once every controller is registered");
}

#[test]
fn an_ended_controller_task_fails_liveness_and_readiness() {
    let health = Health::new();
    let (addr, _block) = controller_like_runtime(health.clone());
    health.mark_started();
    health.mark_ended("#6");
    let live = probe(addr, Probe::Live).expect_err("a controller that returned is not alive");
    assert!(live.contains("controller task ended: #6"), "{live}");
    assert!(probe(addr, Probe::Ready).is_err());
}

/// **A wedged runtime fails the liveness probe**, which is the property the
/// probe exists for: the listener shares the reconcilers' one thread, so while
/// that thread is blocked nothing answers and the probe's deadline expires.
/// NEGATIVE CONTROL: the same runtime answers again once the block ends.
#[test]
fn a_wedged_runtime_fails_liveness_within_the_deadline() {
    let health = Health::new();
    let (addr, block) = controller_like_runtime(health.clone());
    probe(addr, Probe::Live).expect("healthy before the wedge");
    block.send(Duration::from_secs(4)).unwrap();
    std::thread::sleep(Duration::from_millis(100));
    let started = Instant::now();
    let wedged = probe(addr, Probe::Live).expect_err("a blocked runtime cannot answer");
    assert!(
        started.elapsed() < Duration::from_secs(4),
        "the probe must give up on its own deadline, not wait for the runtime: {:?}",
        started.elapsed()
    );
    assert!(
        wedged.contains("no answer") || wedged.contains("/livez"),
        "{wedged}"
    );
    std::thread::sleep(Duration::from_secs(4));
    probe(addr, Probe::Live).expect("the runtime answers again after the block");
}

#[test]
fn nothing_listening_is_a_failed_probe_not_a_hang() {
    let port = std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap();
    let started = Instant::now();
    let refused = probe(port, Probe::Live).expect_err("no listener");
    assert!(refused.contains("no health listener"), "{refused}");
    assert!(started.elapsed() < Duration::from_secs(3));
}

/// **The wrapper `main` puts around every controller task records one that
/// returns.** NEGATIVE CONTROL: while the task is still pending, live.
#[test]
fn a_watched_task_that_returns_is_recorded() {
    let health = Health::new();
    health.mark_started();
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(async {
        let (tx, rx) = tokio::sync::oneshot::channel::<()>();
        let handle = tokio::spawn(watched(
            "#3".to_string(),
            async move {
                let _ = rx.await;
            },
            health.clone(),
        ));
        tokio::task::yield_now().await;
        assert!(health.check(Probe::Live).is_ok(), "a running task is alive");
        tx.send(()).unwrap();
        handle.await.unwrap();
    });
    let ended = health.check(Probe::Live).unwrap_err();
    assert!(ended.contains("#3"), "{ended}");
}
