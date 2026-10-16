//! **PROD-07.2: a stop reaches the engine.** As a pod's PID 1 the runner has
//! no signal handler of its own, so a SIGTERM (a cancel's Job suspension, a
//! pod deletion, a drain) killed it and left the engine writing. [`install`]
//! gives it one: SIGTERM or SIGINT calls
//! [`logweir_engine_oso::subprocess::request_stop`], which forwards SIGTERM to
//! the engine and SIGKILLs it after ten seconds; the run then signs nothing
//! and exits 1 with `failure-reason=Cancelled`.

/// Watch for SIGTERM and SIGINT on a thread of its own. Both handlers are in
/// place when this returns. A failure to install them is logged, never fatal:
/// the run then behaves as it did before.
pub fn install() {
    use tokio::signal::unix::{signal, SignalKind};
    let runtime = match tokio::runtime::Builder::new_current_thread()
        .enable_io()
        .build()
    {
        Ok(rt) => rt,
        Err(e) => {
            tracing::warn!(error = %e, "no signal handler: the runtime could not be built");
            return;
        }
    };
    let streams = {
        let _guard = runtime.enter();
        signal(SignalKind::terminate()).and_then(|t| Ok((t, signal(SignalKind::interrupt())?)))
    };
    let (mut term, mut int) = match streams {
        Ok(s) => s,
        Err(e) => {
            tracing::warn!(error = %e, "no signal handler: SIGTERM/SIGINT could not be registered");
            return;
        }
    };
    std::thread::spawn(move || {
        runtime.block_on(async {
            tokio::select! {
                _ = term.recv() => {}
                _ = int.recv() => {}
            }
        });
        tracing::warn!(
            "stop requested (SIGTERM or SIGINT): the engine is stopped and nothing is signed"
        );
        logweir_engine_oso::subprocess::request_stop();
        // Keep absorbing further signals: the kubelet's SIGKILL ends the
        // process if the stop takes longer than the pod's grace.
        runtime.block_on(async {
            loop {
                tokio::select! {
                    _ = term.recv() => {}
                    _ = int.recv() => {}
                }
            }
        });
    });
}
