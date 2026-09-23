//! The controller's liveness and readiness, for the kubelet (chart gap G5).
//!
//! THE PROBLEM. The `weirkeeper` Deployment had no probes and the controller
//! served no health endpoint, so a controller whose runtime was wedged — a
//! reconciler blocked in a synchronous call on the one runtime thread, a
//! deadlock — was never restarted, and a controller task that had ENDED left a
//! kind nobody reconciles behind a pod that still read Running.
//!
//! THE SHAPE: A LOOPBACK LISTENER AND AN EXEC PROBE. The process serves two
//! paths on [`DEFAULT_ADDR`] (`LOGWEIR_HEALTH_ADDR` overrides it), and the
//! kubelet runs `weirkeeper --probe live|ready` INSIDE the container, which
//! connects to that loopback address. It is loopback because nothing outside
//! the pod has any business asking, and because a kubelet `httpGet` probe
//! would come from the node to the pod IP — a listener the NetworkPolicy and
//! the Service graph would then have to reason about. [`configured_addr`]
//! refuses a non-loopback address, so a typo cannot publish it. No request is
//! parsed beyond its first line, no header is read, and no answer carries
//! anything but `ok` or the reason it is not: no secret, no object, no
//! configuration.
//!
//! WHAT EACH ANSWER MEANS.
//! - `/livez` — the runtime answered (the listener runs on the SAME
//!   current-thread runtime as every reconciler, so a thread blocked by one of
//!   them cannot accept, the probe times out, and the kubelet restarts the pod),
//!   and no controller task has ended.
//! - `/readyz` — live, and the Kubernetes client was built and every controller
//!   registered.
//!
//! What it does not claim: that the API server is reachable right now (a
//! controller rides out an API-server outage with its own backoff, and a
//! restart would not help), or that any reconcile succeeded.

use std::net::{IpAddr, SocketAddr};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

/// The environment variable that overrides [`DEFAULT_ADDR`].
pub const HEALTH_ADDR_ENV: &str = "LOGWEIR_HEALTH_ADDR";

/// Where the health listener binds unless told otherwise.
pub const DEFAULT_ADDR: &str = "127.0.0.1:8081";

/// How long the listener waits for a request line, and the probe for an
/// answer. The kubelet's own `timeoutSeconds` is above it.
pub const IO_DEADLINE: Duration = Duration::from_secs(2);

/// The listener address from the environment variable's read.
///
/// Unset or empty is [`DEFAULT_ADDR`]. Anything else must parse as a socket
/// address on a LOOPBACK interface; port 0 is accepted (an ephemeral port,
/// for tests that run several controllers on one host).
///
/// # Errors
///
/// A sentence naming the variable and what was wrong.
pub fn configured_addr(read: Result<String, std::env::VarError>) -> Result<SocketAddr, String> {
    let raw = read.unwrap_or_default();
    let raw = if raw.trim().is_empty() {
        DEFAULT_ADDR.to_string()
    } else {
        raw.trim().to_string()
    };
    let addr: SocketAddr = raw
        .parse()
        .map_err(|_| format!("{HEALTH_ADDR_ENV}={raw:?} is not an address:port"))?;
    let loopback = match addr.ip() {
        IpAddr::V4(v4) => v4.is_loopback(),
        IpAddr::V6(v6) => v6.is_loopback(),
    };
    if !loopback {
        return Err(format!(
            "{HEALTH_ADDR_ENV}={raw:?} is not a loopback address; the health listener answers \
             only the exec probe inside the container, and must not be reachable from the \
             network"
        ));
    }
    Ok(addr)
}

/// Bind the health listener — the ONE way `main` binds it.
///
/// The address was already refused by [`configured_addr`] if it was not
/// loopback; this checks again at the bind site, and checks what the kernel
/// actually bound, because a listener on `0.0.0.0` would let any pod in the
/// cluster hold its connections (`serve` answers one at a time, two seconds
/// each) and starve the kubelet's exec probe into a restart loop.
/// `tests/health.rs` pins both halves and that `main.rs` binds through here.
///
/// # Errors
///
/// A non-loopback address (`InvalidInput`), or the bind's own error.
pub async fn bind(addr: SocketAddr) -> std::io::Result<tokio::net::TcpListener> {
    let refuse = |what: SocketAddr| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!("the health listener binds loopback only, not {what}"),
        )
    };
    if !addr.ip().is_loopback() {
        return Err(refuse(addr));
    }
    let listener = tokio::net::TcpListener::bind(addr).await?;
    let bound = listener.local_addr()?;
    if !bound.ip().is_loopback() {
        return Err(refuse(bound));
    }
    Ok(listener)
}

/// The two probes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Probe {
    /// `/livez`.
    Live,
    /// `/readyz`.
    Ready,
}

impl Probe {
    /// The argv word (`live`, `ready`).
    ///
    /// # Errors
    ///
    /// The word, when it is neither.
    pub fn from_arg(arg: &str) -> Result<Self, String> {
        match arg {
            "live" => Ok(Self::Live),
            "ready" => Ok(Self::Ready),
            other => Err(format!(
                "`--probe {other}`: the probes are `live` and `ready`"
            )),
        }
    }

    const fn path(self) -> &'static str {
        match self {
            Self::Live => "/livez",
            Self::Ready => "/readyz",
        }
    }
}

/// What the listener answers from. Shared between `main` and the listener.
#[derive(Debug, Default)]
pub struct Health {
    started: AtomicBool,
    ended: Mutex<Vec<String>>,
}

impl Health {
    /// A fresh state: not started, nothing ended.
    #[must_use]
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// The client is built and every controller is registered.
    pub fn mark_started(&self) {
        self.started.store(true, Ordering::Release);
    }

    /// A controller task returned. It never should: each runs until the
    /// process stops.
    pub fn mark_ended(&self, controller: &str) {
        self.ended
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(controller.to_string());
    }

    /// The answer to one probe: `Ok(())` or the reason it is not healthy.
    ///
    /// # Errors
    ///
    /// The reason, for the response body.
    pub fn check(&self, probe: Probe) -> Result<(), String> {
        let ended = self
            .ended
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        if !ended.is_empty() {
            return Err(format!("controller task ended: {}", ended.join(",")));
        }
        if probe == Probe::Ready && !self.started.load(Ordering::Acquire) {
            return Err("starting".to_string());
        }
        Ok(())
    }
}

/// Run one controller task and, if it ever RETURNS, record it so `/livez`
/// fails. Each controller runs until the process stops; one that returned
/// leaves its kind unreconciled behind a pod that still reads Running.
pub async fn watched<F>(which: String, task: F, health: Arc<Health>)
where
    F: std::future::Future<Output = ()>,
{
    task.await;
    tracing::error!(controller = %which, "a controller task ended; /livez now fails");
    health.mark_ended(&which);
}

/// Answer probes on `listener` until the process ends.
///
/// One connection at a time, each bounded by [`IO_DEADLINE`]: only processes
/// inside the pod can connect, and a probe is one short request.
pub async fn serve(listener: tokio::net::TcpListener, health: Arc<Health>) {
    loop {
        let Ok((stream, _)) = listener.accept().await else {
            continue;
        };
        let _ = tokio::time::timeout(IO_DEADLINE, answer(stream, &health)).await;
    }
}

async fn answer(mut stream: tokio::net::TcpStream, health: &Health) -> std::io::Result<()> {
    use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
    let mut buf = [0u8; 512];
    let mut len = 0;
    while len < buf.len() {
        let n = stream.read(&mut buf[len..]).await?;
        if n == 0 {
            break;
        }
        len += n;
        if buf[..len].windows(2).any(|w| w == b"\r\n") {
            break;
        }
    }
    let line = String::from_utf8_lossy(&buf[..len]);
    let path = line.split_whitespace().nth(1).unwrap_or("");
    let verdict = match path {
        "/livez" => Some(health.check(Probe::Live)),
        "/readyz" => Some(health.check(Probe::Ready)),
        _ => None,
    };
    let (status, body) = match verdict {
        Some(Ok(())) => ("200 OK", "ok\n".to_string()),
        Some(Err(reason)) => ("503 Service Unavailable", format!("{reason}\n")),
        None => (
            "404 Not Found",
            "the paths are /livez and /readyz\n".to_string(),
        ),
    };
    let response = format!(
        "HTTP/1.1 {status}\r\ncontent-type: text/plain\r\ncontent-length: {}\r\n\
         connection: close\r\n\r\n{body}",
        body.len()
    );
    stream.write_all(response.as_bytes()).await?;
    stream.shutdown().await
}

/// Ask the listener at `addr` one probe, synchronously, with [`IO_DEADLINE`]
/// on the connect, the write and the read. This is `weirkeeper --probe`.
///
/// # Errors
///
/// A sentence: the connection failed, the answer was not `200`, or it was
/// late.
pub fn probe(addr: SocketAddr, which: Probe) -> Result<(), String> {
    use std::io::{Read as _, Write as _};
    let mut stream = std::net::TcpStream::connect_timeout(&addr, IO_DEADLINE)
        .map_err(|e| format!("no health listener at {addr}: {e}"))?;
    stream
        .set_read_timeout(Some(IO_DEADLINE))
        .and_then(|()| stream.set_write_timeout(Some(IO_DEADLINE)))
        .map_err(|e| format!("could not bound the probe: {e}"))?;
    stream
        .write_all(format!("GET {} HTTP/1.1\r\nhost: localhost\r\n\r\n", which.path()).as_bytes())
        .map_err(|e| format!("could not ask {addr}: {e}"))?;
    let mut answer = Vec::new();
    stream
        .take(1024)
        .read_to_end(&mut answer)
        .map_err(|e| format!("no answer from {addr} within {IO_DEADLINE:?}: {e}"))?;
    let text = String::from_utf8_lossy(&answer);
    if text.starts_with("HTTP/1.1 200 ") {
        return Ok(());
    }
    let status = text.lines().next().unwrap_or("no status line");
    let reason = text.split("\r\n\r\n").nth(1).unwrap_or("").trim();
    Err(format!("{}: {status}: {reason}", which.path()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_address_defaults_to_loopback_and_refuses_the_network() {
        assert_eq!(
            configured_addr(Err(std::env::VarError::NotPresent)).unwrap(),
            DEFAULT_ADDR.parse::<SocketAddr>().unwrap()
        );
        assert_eq!(
            configured_addr(Ok("  ".into())).unwrap(),
            DEFAULT_ADDR.parse::<SocketAddr>().unwrap()
        );
        assert!(configured_addr(Ok("[::1]:9000".into())).is_ok());
        assert!(configured_addr(Ok("127.0.0.1:0".into())).is_ok());
        for bad in [
            "0.0.0.0:8081",
            "10.1.2.3:8081",
            "[::]:8081",
            "localhost:8081",
            "8081",
        ] {
            assert!(configured_addr(Ok(bad.into())).is_err(), "{bad}");
        }
    }

    #[test]
    fn readiness_waits_for_the_start_and_an_ended_task_fails_both() {
        let health = Health::new();
        assert!(
            health.check(Probe::Live).is_ok(),
            "live from the first instant"
        );
        assert_eq!(health.check(Probe::Ready), Err("starting".into()));
        health.mark_started();
        assert!(health.check(Probe::Ready).is_ok());
        health.mark_ended("restore");
        let live = health.check(Probe::Live).unwrap_err();
        assert!(live.contains("restore"), "{live}");
        assert!(health.check(Probe::Ready).is_err());
    }

    #[test]
    fn the_probe_words_are_the_two_and_only_the_two() {
        assert_eq!(Probe::from_arg("live"), Ok(Probe::Live));
        assert_eq!(Probe::from_arg("ready"), Ok(Probe::Ready));
        assert!(Probe::from_arg("healthy").is_err());
    }
}
