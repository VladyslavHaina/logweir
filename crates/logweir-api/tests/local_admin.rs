//! The shipped binary: what it refuses before it binds anything, and what it
//! serves on loopback when it starts.
//!
//! Runs the binary cargo has ALREADY built for this test target
//! (`CARGO_BIN_EXE_…`), never `cargo run`, which would re-enter cargo while
//! the workspace build lock is held — the idiom
//! `crates/logweir/tests/guard_cli.rs` and `crates/weirkeeper/tests/linkage.rs`
//! establish.
//!
//! THE KUBERNETES ENDPOINT IN EVERY FIXTURE IS `https://127.0.0.1:1`:
//! privileged, unused and never contacted, so a regression that dialled would
//! fail instantly rather than hang.
//!
//! EVERY CHILD HERE IS TIME-BOUNDED AND ALWAYS REAPED, and that is a
//! regression fix, not a precaution. On 2026-09-15 a mutation run removed the
//! loopback check in `config::parse_loopback_listen` to prove
//! [`a_non_loopback_listener_is_refused_before_anything_is_bound`] can fail.
//! With the check gone the binary did what the mutant asked — it bound
//! `0.0.0.0` and served — and the test, which collected the child with
//! `Command::output()`, waited for a server to close its stdout. It waited 13
//! hours. Killing the test runner then left the server orphaned on
//! `0.0.0.0:18484` (PPID 1) for another 14 hours, holding a fixed port that
//! the next run of this same file needed.
//!
//! Three rules follow, and [`bounded_output`] and [`Reaped`] are how they are
//! kept:
//!
//! 1. **No child is waited on without a deadline.** Past [`CHILD_LIMIT`] the
//!    child is killed and the test FAILS with what it printed. A surviving
//!    mutant must cost seconds, not a working day.
//! 2. **No child outlives the test.** [`Reaped`] kills on `Drop`, so a panic
//!    between spawn and the intended stop cannot leak a listener.
//! 3. **No fixed ports.** Every port comes from [`free_port`], so a stray
//!    listener — this suite's or another worker's — can neither fail this file
//!    nor let a bind assertion pass for the wrong reason.

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Command, Output, Stdio};
use std::time::{Duration, Instant};

/// How long any child spawned here may run before it is killed and the test
/// fails. Startup is milliseconds and every refusal path is immediate; this is
/// a ceiling for a wedged process, not an expected wait.
const CHILD_LIMIT: Duration = Duration::from_secs(30);

/// How long the served process is given to reach its listening socket, and to
/// exit after SIGTERM.
const SERVE_LIMIT: Duration = Duration::from_secs(20);

fn binary() -> &'static str {
    env!("CARGO_BIN_EXE_logweir-api")
}

fn repo_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .nth(2)
        .expect("crates/logweir-api sits two levels under the workspace root")
        .to_path_buf()
}

/// Run `command` to completion within [`CHILD_LIMIT`], or kill it and fail.
///
/// Both pipes are drained by their own threads for the whole life of the
/// child. Without that a child which filled the ~64 KiB pipe buffer would
/// block inside `write` and never reach exit, and the deadline below would be
/// killing a process that was only waiting for this test to read — a hang
/// diagnosed as a timeout, which is worse than either.
fn bounded_output(mut command: Command, label: &str) -> Output {
    command
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let mut child = command
        .spawn()
        .unwrap_or_else(|error| panic!("{label}: the binary does not start: {error}"));
    let mut out_pipe = child.stdout.take().expect("stdout was piped");
    let mut err_pipe = child.stderr.take().expect("stderr was piped");
    let out_reader = std::thread::spawn(move || {
        let mut buffer = Vec::new();
        let _ = out_pipe.read_to_end(&mut buffer);
        buffer
    });
    let err_reader = std::thread::spawn(move || {
        let mut buffer = Vec::new();
        let _ = err_pipe.read_to_end(&mut buffer);
        buffer
    });
    let collect = |reader: std::thread::JoinHandle<Vec<u8>>| reader.join().unwrap_or_default();

    let started = Instant::now();
    let status = loop {
        if let Some(status) = child.try_wait().expect("the child is waitable") {
            break status;
        }
        if started.elapsed() >= CHILD_LIMIT {
            // SIGKILL: a process that is ignoring its own shutdown path is
            // exactly the case this deadline exists for.
            let _ = child.kill();
            let _ = child.wait();
            let stdout = String::from_utf8_lossy(&collect(out_reader)).into_owned();
            let stderr = String::from_utf8_lossy(&collect(err_reader)).into_owned();
            panic!(
                "{label}: the process was still running after {CHILD_LIMIT:?} and was killed. A \
                 logweir-api that does not exit here is a TEST FAILURE, not something to wait \
                 for — most likely it accepted a listener it must have refused.\nstdout: \
                 {stdout}\nstderr: {stderr}"
            );
        }
        std::thread::sleep(Duration::from_millis(20));
    };
    Output {
        status,
        stdout: collect(out_reader),
        stderr: collect(err_reader),
    }
}

/// A child that is killed when it leaves scope, whatever happens to the test
/// thread — a panicking assertion included. See rule 2 in the module
/// documentation.
struct Reaped(std::process::Child);

impl Drop for Reaped {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

/// A scratch directory holding a configuration, a cursor key and a kubeconfig.
struct Fixture(PathBuf);

impl Fixture {
    fn new(tag: &str) -> Self {
        let stamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let dir =
            std::env::temp_dir().join(format!("logweir-api-{tag}-{}-{stamp}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let fixture = Self(dir);
        std::fs::write(fixture.0.join("cursor.key"), [0x42; 32]).unwrap();
        std::fs::write(
            fixture.0.join("kubeconfig.yaml"),
            "apiVersion: v1\nkind: Config\nclusters:\n- name: fixture\n  cluster:\n    server: \
             https://127.0.0.1:1\ncontexts:\n- name: fixture\n  context:\n    cluster: fixture\n    \
             user: fixture\nusers:\n- name: fixture\n  user: {}\n",
        )
        .unwrap();
        fixture
    }

    fn config(&self, body: &str) -> PathBuf {
        let path = self.0.join("config.yaml");
        std::fs::write(&path, body).unwrap();
        path
    }

    fn path(&self, name: &str) -> String {
        self.0.join(name).display().to_string()
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn config_text(fixture: &Fixture, listen: &str, origin: &str, context: &str) -> String {
    format!(
        "mode: localAdmin\nlisten: \"{listen}\"\npublicOrigin: \"{origin}\"\nuiDirectory: {}\n\
         localAdmin:\n  subject: admin\nnamespaces: [team-a]\nkubernetes:\n  source: kubeconfig\n  \
         kubeconfig: {}\n  context: {context}\ncursorKeyFile: {}\n",
        repo_root().join("ui").display(),
        fixture.path("kubeconfig.yaml"),
        fixture.path("cursor.key"),
    )
}

/// Start the binary with `config` and require it to exit within
/// [`CHILD_LIMIT`]. Every caller of this helper is testing a REFUSAL, so a
/// process that keeps running has already failed the assertion the caller was
/// about to make.
fn run(config: &Path, label: &str) -> Output {
    let mut command = Command::new(binary());
    command.arg("--config").arg(config).env_remove("KUBECONFIG");
    bounded_output(command, label)
}

/// A port nothing is listening on right now, taken fresh for every case.
fn free_port() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    listener.local_addr().unwrap().port()
}

#[test]
fn version_and_help_touch_nothing() {
    for flag in ["--version", "--help"] {
        let mut command = Command::new(binary());
        command.arg(flag);
        let out = bounded_output(command, flag);
        assert!(out.status.success(), "{flag}");
        let text = String::from_utf8_lossy(&out.stdout);
        assert!(!text.is_empty());
        if flag == "--version" {
            assert!(text.starts_with("logweir-api "), "{text}");
        } else {
            assert!(text.contains("not a Kubernetes proxy"), "{text}");
        }
    }
    let mut command = Command::new(binary());
    command.arg("--serve-everything");
    let out = bounded_output(command, "--serve-everything");
    assert_eq!(out.status.code(), Some(2));
}

/// The refusal that makes localAdmin mode an administrator mode: the listener
/// must be loopback, and the refusal happens before a socket or a client.
///
/// REGRESSION REASON. Remove the `is_loopback` check in
/// `config::parse_loopback_listen` and the first case binds `0.0.0.0` and
/// serves. [`run`] then kills it at [`CHILD_LIMIT`] and this test fails with
/// the child's own output; it does not wait for the process to end, because
/// the whole point of the mutant is that it never would.
#[test]
fn a_non_loopback_listener_is_refused_before_anything_is_bound() {
    let fixture = Fixture::new("bind");
    for (listen_host, origin_host) in [
        ("0.0.0.0", "127.0.0.1"),
        ("[::]", "[::1]"),
        ("192.168.1.10", "127.0.0.1"),
        ("10.0.0.7", "127.0.0.1"),
        ("localhost", "localhost"),
    ] {
        let port = free_port();
        let listen = format!("{listen_host}:{port}");
        let origin = format!("http://{origin_host}:{port}");
        let config = fixture.config(&config_text(&fixture, &listen, &origin, "fixture"));
        let out = run(&config, &listen);
        assert_eq!(out.status.code(), Some(2), "{listen} started");
        let stderr = String::from_utf8_lossy(&out.stderr);
        assert!(
            stderr.contains("configuration field `listen`"),
            "{listen}: {stderr}"
        );
        assert!(
            stderr.contains("loopback") || stderr.contains("IP:port"),
            "{listen}: {stderr}"
        );
        // Nothing is listening on that port afterwards.
        assert!(
            TcpStream::connect_timeout(
                &format!("127.0.0.1:{port}").parse::<SocketAddr>().unwrap(),
                Duration::from_millis(200)
            )
            .is_err(),
            "{listen} left a listener behind"
        );
    }
}

#[test]
fn a_kubeconfig_without_an_explicit_context_is_refused() {
    let fixture = Fixture::new("context");
    let port = free_port();
    let mut text = config_text(
        &fixture,
        &format!("127.0.0.1:{port}"),
        &format!("http://127.0.0.1:{port}"),
        "fixture",
    );
    text = text.replace("  context: fixture\n", "");
    let out = run(&fixture.config(&text), "no context");
    assert_eq!(out.status.code(), Some(2));
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(stderr.contains("kubernetes.context"), "{stderr}");
    assert!(stderr.contains("current-context is never used"), "{stderr}");
}

#[test]
fn a_context_that_is_not_in_the_kubeconfig_is_refused() {
    let fixture = Fixture::new("badcontext");
    let port = free_port();
    let config = fixture.config(&config_text(
        &fixture,
        &format!("127.0.0.1:{port}"),
        &format!("http://127.0.0.1:{port}"),
        "docker-desktop",
    ));
    let out = run(&config, "unknown context");
    assert_eq!(
        out.status.code(),
        Some(1),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let text = format!(
        "{}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(text.contains("docker-desktop"), "{text}");
}

#[test]
fn a_short_cursor_key_is_refused() {
    let fixture = Fixture::new("mode");
    let port = free_port();
    let listen = format!("127.0.0.1:{port}");
    let origin = format!("http://127.0.0.1:{port}");
    std::fs::write(fixture.0.join("cursor.key"), b"too-short").unwrap();
    let out = run(
        &fixture.config(&config_text(&fixture, &listen, &origin, "fixture")),
        "short cursor key",
    );
    assert_eq!(out.status.code(), Some(2));
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(stderr.contains("cursorKeyFile"), "{stderr}");
}

/// **The shipped binary refuses every shared-mode configuration D0 says it
/// must, before it binds a socket or builds a Kubernetes client.**
///
/// These are the refusals whose whole value is that they happen at STARTUP: a
/// console that comes up on plain HTTP, or with a key file someone rewrote
/// without telling it, or with a `*` in a role binding, is worse than one that
/// does not come up at all, because the first two look like they are working.
/// Exit 2 is the configuration-refusal code, and it is asserted separately from
/// the message so that a refusal cannot degrade into a generic failure.
#[test]
fn shared_mode_refuses_plain_http_a_rotated_key_and_a_wildcard_binding() {
    let fixture = Fixture::new("shared");
    let port = free_port();
    write_shared_material(&fixture, 1);

    let base = |url: &str| shared_config_text(&fixture, &format!("127.0.0.1:{port}"), url);

    // 1. TLS at the shared entry point is not a recommendation.
    let out = run(
        &fixture.config(&base("http://console.example")),
        "plain http",
    );
    assert_eq!(out.status.code(), Some(2));
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        stderr.contains("publicBaseUrl") && stderr.contains("HTTPS"),
        "{stderr}"
    );

    // A trailing slash or a path would make the derived redirect URI disagree
    // with the one registered at the provider.
    let out = run(
        &fixture.config(&base("https://console.example/")),
        "trailing slash",
    );
    assert_eq!(out.status.code(), Some(2));
    assert!(
        String::from_utf8_lossy(&out.stderr).contains("publicBaseUrl"),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );

    // 2. The session key file carries version 1; the configuration below will
    //    say 9. That is the "unexpectedly rotated" refusal.
    let rotated =
        base("https://console.example").replace("expectedVersion: 1", "expectedVersion: 9");
    let out = run(&fixture.config(&rotated), "rotated session key");
    assert_eq!(out.status.code(), Some(2));
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        stderr.contains("version 1") && stderr.contains("expects 9"),
        "{stderr}"
    );

    // 3. A missing session key file.
    std::fs::remove_file(fixture.0.join("session.key.yaml")).unwrap();
    let out = run(
        &fixture.config(&base("https://console.example")),
        "missing key",
    );
    assert_eq!(out.status.code(), Some(2));
    assert!(
        String::from_utf8_lossy(&out.stderr).contains("session.key.yaml"),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    write_shared_material(&fixture, 1);

    // 4. Bindings are exact strings. A `*` would match nothing, so it is
    //    refused by name rather than silently granting nothing.
    let wildcard = base("https://console.example").replace("[lw-viewers]", "[\"*\"]");
    let out = run(&fixture.config(&wildcard), "wildcard binding");
    assert_eq!(out.status.code(), Some(2));
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        stderr.contains("roles.bindings") && stderr.contains("EXACT"),
        "{stderr}"
    );

    // 5. An empty client secret file.
    std::fs::write(fixture.0.join("client.secret"), "\n").unwrap();
    let out = run(
        &fixture.config(&base("https://console.example")),
        "empty secret",
    );
    assert_eq!(out.status.code(), Some(2));
    assert!(
        String::from_utf8_lossy(&out.stderr).contains("client.secret"),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
}

/// The session key, cursor key and client secret a shared-mode fixture mounts.
fn write_shared_material(fixture: &Fixture, version: u32) {
    let key = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=";
    std::fs::write(
        fixture.0.join("session.key.yaml"),
        format!("version: {version}\nkey: \"{key}\"\n"),
    )
    .unwrap();
    std::fs::write(
        fixture.0.join("cursor.key.yaml"),
        format!("version: {version}\nkey: \"{key}\"\n"),
    )
    .unwrap();
    std::fs::write(fixture.0.join("client.secret"), "a-client-secret\n").unwrap();
}

fn shared_config_text(fixture: &Fixture, listen: &str, public_base_url: &str) -> String {
    format!(
        "mode: shared\nlisten: \"{listen}\"\npublicBaseUrl: \"{public_base_url}\"\n\
         uiDirectory: {ui}\n\
         oidc:\n  issuer: https://idp.example/realms/logweir\n  clientId: logweir-console\n  \
         clientSecretFile: {secret}\n\
         roles:\n  revision: r1\n  bindings:\n  - role: viewer\n    namespace: team-a\n    \
         groups: [lw-viewers]\n\
         sessionKey:\n  file: {session}\n  expectedVersion: 1\n\
         cursorKey:\n  file: {cursor}\n  expectedVersion: 1\n\
         namespaces: [team-a]\nkubernetes:\n  source: kubeconfig\n  kubeconfig: {kubeconfig}\n  \
         context: fixture\n",
        ui = repo_root().join("ui").display(),
        secret = fixture.path("client.secret"),
        session = fixture.path("session.key.yaml"),
        cursor = fixture.path("cursor.key.yaml"),
        kubeconfig = fixture.path("kubeconfig.yaml"),
    )
}

#[test]
fn a_kubeconfig_user_that_impersonates_is_refused() {
    let fixture = Fixture::new("impersonate");
    std::fs::write(
        fixture.0.join("kubeconfig.yaml"),
        "apiVersion: v1\nkind: Config\nclusters:\n- name: fixture\n  cluster:\n    server: \
         https://127.0.0.1:1\ncontexts:\n- name: fixture\n  context:\n    cluster: fixture\n    \
         user: fixture\nusers:\n- name: fixture\n  user:\n    as: system:admin\n",
    )
    .unwrap();
    let port = free_port();
    let config = fixture.config(&config_text(
        &fixture,
        &format!("127.0.0.1:{port}"),
        &format!("http://127.0.0.1:{port}"),
        "fixture",
    ));
    let out = run(&config, "impersonating kubeconfig");
    assert_eq!(out.status.code(), Some(1));
    let text = format!(
        "{}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(text.contains("impersonates another identity"), "{text}");
}

/// One request/response over a real socket, with a deadline on connect, write
/// and read: a server that accepts and then says nothing must fail this test,
/// not stall it.
fn http_get(port: u16, path: &str, host: &str) -> String {
    let address: SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();
    let mut stream = TcpStream::connect_timeout(&address, Duration::from_secs(5))
        .expect("the server accepts the connection");
    stream
        .set_write_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    write!(
        stream,
        "GET {path} HTTP/1.1\r\nHost: {host}\r\nConnection: close\r\n\r\n"
    )
    .unwrap();
    let mut response = Vec::new();
    stream
        .read_to_end(&mut response)
        .expect("the server answers within five seconds");
    String::from_utf8_lossy(&response).into_owned()
}

/// The started process, end to end on a real loopback socket: it serves the
/// UI and `/healthz`, refuses a foreign Host, reports not-ready without a
/// cluster, and exits 0 on SIGTERM.
#[test]
fn the_binary_serves_loopback_and_stops_on_sigterm() {
    let fixture = Fixture::new("serve");
    let port = free_port();
    let config = fixture.config(&config_text(
        &fixture,
        &format!("127.0.0.1:{port}"),
        &format!("http://127.0.0.1:{port}"),
        "fixture",
    ));
    // `Reaped`, not a bare `Child`: every assertion below can panic, and a
    // panic here must not leave a listener on this host.
    let mut child = Reaped(
        Command::new(binary())
            .arg("--config")
            .arg(&config)
            .env_remove("KUBECONFIG")
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("the binary starts"),
    );

    let address: SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();
    let deadline = Instant::now() + SERVE_LIMIT;
    let mut listening = false;
    while Instant::now() < deadline {
        if let Some(exit) = child.0.try_wait().expect("the child is waitable") {
            panic!("the server exited before it listened: {exit}");
        }
        if TcpStream::connect_timeout(&address, Duration::from_millis(200)).is_ok() {
            listening = true;
            break;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    assert!(
        listening,
        "the server did not listen on 127.0.0.1:{port} within {SERVE_LIMIT:?}"
    );

    let health = http_get(port, "/healthz", &format!("127.0.0.1:{port}"));
    assert!(health.starts_with("HTTP/1.1 200"), "{health}");
    assert!(health.contains("\"status\":\"ok\""), "{health}");
    assert!(
        health.contains("content-security-policy: default-src 'self'"),
        "{health}"
    );
    assert!(health.contains("x-request-id:"), "{health}");

    let ui = http_get(port, "/ui/", &format!("127.0.0.1:{port}"));
    assert!(
        ui.starts_with("HTTP/1.1 200"),
        "{}",
        &ui[..ui.len().min(400)]
    );
    assert!(ui.contains("<title>Logweir</title>"), "the index is served");

    // The proxy path the current UI uses is not served by this binary.
    let apis = http_get(
        port,
        "/apis/logweir.dev/v1alpha1/namespaces/team-a/backups",
        &format!("127.0.0.1:{port}"),
    );
    assert!(apis.starts_with("HTTP/1.1 404"), "{apis}");
    assert!(apis.contains("application/problem+json"), "{apis}");

    // A foreign Host is refused even on loopback (DNS rebinding).
    let foreign = http_get(port, "/api/v1/session", "evil.example");
    assert!(foreign.starts_with("HTTP/1.1 421"), "{foreign}");

    // Readiness without a reachable cluster is 503 and says nothing else.
    let ready = http_get(port, "/readyz", &format!("127.0.0.1:{port}"));
    assert!(ready.starts_with("HTTP/1.1 503"), "{ready}");
    assert!(
        !ready.contains("127.0.0.1:1"),
        "the endpoint leaked: {ready}"
    );

    let mut kill = Command::new("kill");
    kill.arg("-TERM").arg(child.0.id().to_string());
    assert!(
        bounded_output(kill, "kill -TERM").status.success(),
        "the TERM signal was not delivered"
    );
    let deadline = Instant::now() + SERVE_LIMIT;
    let exit = loop {
        if let Some(exit) = child.0.try_wait().unwrap() {
            break exit;
        }
        assert!(
            Instant::now() < deadline,
            "the process did not stop on SIGTERM within {SERVE_LIMIT:?}"
        );
        std::thread::sleep(Duration::from_millis(100));
    };
    assert_eq!(exit.code(), Some(0), "SIGTERM must be a clean stop");
}

/// Start the server on a free port and return it with the port. The caller
/// keeps the `Reaped` alive for as long as it wants the server.
fn start_server(fixture: &Fixture) -> (Reaped, u16) {
    let port = free_port();
    let config = fixture.config(&config_text(
        fixture,
        &format!("127.0.0.1:{port}"),
        &format!("http://127.0.0.1:{port}"),
        "fixture",
    ));
    let child = Reaped(
        Command::new(binary())
            .arg("--config")
            .arg(&config)
            .env_remove("KUBECONFIG")
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("the binary starts"),
    );
    let address: SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();
    let deadline = Instant::now() + SERVE_LIMIT;
    while Instant::now() < deadline {
        if TcpStream::connect_timeout(&address, Duration::from_millis(200)).is_ok() {
            return (child, port);
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    panic!("the server did not listen on 127.0.0.1:{port} within {SERVE_LIMIT:?}");
}

/// **A connection that never finishes its headers is closed, and does not hold
/// the server.**
///
/// REGRESSION REASON (review finding R4). `axum::serve` builds its hyper
/// connection with no header-read deadline, so a client that opened a socket and
/// sent one byte held a task and a descriptor for as long as it liked — the
/// slow-loris shape. `main::HEADER_READ_TIMEOUT` is ten seconds; this asserts
/// that the connection really is closed after it, and that a normal request on
/// another connection is answered throughout.
#[test]
fn a_connection_that_never_sends_its_headers_is_closed() {
    let fixture = Fixture::new("slowloris");
    let (_server, port) = start_server(&fixture);
    let address: SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();

    let mut slow = TcpStream::connect_timeout(&address, Duration::from_secs(5)).unwrap();
    // A request line with no terminating blank line: hyper is still waiting for
    // the head when the deadline expires.
    slow.write_all(b"GET /healthz HTTP/1.1\r\nHost: 127.0.0.1\r\n")
        .unwrap();

    // While it hangs there, the server still serves.
    let health = http_get(port, "/healthz", &format!("127.0.0.1:{port}"));
    assert!(health.starts_with("HTTP/1.1 200"), "{health}");

    // And the hung connection is closed by the server, not by us. Read to EOF
    // with a read timeout well past the ten-second deadline: a server that
    // never closed would time out here instead of returning.
    slow.set_read_timeout(Some(Duration::from_secs(25)))
        .unwrap();
    let started = Instant::now();
    let mut sink = Vec::new();
    let read = slow.read_to_end(&mut sink);
    let elapsed = started.elapsed();
    assert!(
        read.is_ok(),
        "the server never closed the headerless connection: {read:?} after {elapsed:?}"
    );
    assert!(
        elapsed < Duration::from_secs(25),
        "the connection outlived the header deadline by too much: {elapsed:?}"
    );

    // The server is still healthy afterwards.
    let health = http_get(port, "/healthz", &format!("127.0.0.1:{port}"));
    assert!(health.starts_with("HTTP/1.1 200"), "{health}");
}

/// **An oversized request head is refused rather than buffered.**
///
/// The BODY has always been bounded, by `http::read_json`. The HEAD was not:
/// hyper reads the request line and headers into a buffer before any route
/// matches, so the cap has to be set on the connection.
#[test]
fn an_oversized_request_head_is_refused() {
    let fixture = Fixture::new("bighead");
    let (_server, port) = start_server(&fixture);
    let address: SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();

    let mut stream = TcpStream::connect_timeout(&address, Duration::from_secs(5)).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(10)))
        .unwrap();
    stream
        .set_write_timeout(Some(Duration::from_secs(10)))
        .unwrap();
    let padding = "x".repeat(4096);
    let mut head = format!("GET /healthz HTTP/1.1\r\nHost: 127.0.0.1:{port}\r\n");
    // Comfortably past the 32 KiB cap.
    for i in 0..32 {
        head.push_str(&format!("X-Pad-{i}: {padding}\r\n"));
    }
    head.push_str("\r\n");
    // A refused head may close the connection mid-write; that is the refusal.
    let _ = stream.write_all(head.as_bytes());
    let mut response = Vec::new();
    let _ = stream.read_to_end(&mut response);
    let text = String::from_utf8_lossy(&response);
    assert!(
        text.is_empty() || !text.starts_with("HTTP/1.1 200"),
        "an oversized head was served: {}",
        &text[..text.len().min(200)]
    );

    // The cap is per connection and the server carries on.
    let health = http_get(port, "/healthz", &format!("127.0.0.1:{port}"));
    assert!(health.starts_with("HTTP/1.1 200"), "{health}");
}

/// **At the connection ceiling the server stops accepting, and recovers the
/// moment a connection closes.**
///
/// `main::MAX_CONNECTIONS` is 256. The permit is taken BEFORE the accept, so at
/// the ceiling further connections wait in the kernel backlog instead of each
/// becoming a task — which is the difference between a bounded server and one
/// that runs out of memory politely.
///
/// The test is deterministic rather than timing-based in the part that matters:
/// the pending request is unanswered while every permit is held, and answered
/// after exactly one connection is dropped.
#[test]
fn the_connection_ceiling_holds_and_then_releases() {
    const CEILING: usize = 256;
    let fixture = Fixture::new("ceiling");
    let (_server, port) = start_server(&fixture);
    let address: SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();

    // Fill every permit with connections that are accepted and then idle.
    let mut held = Vec::with_capacity(CEILING);
    for i in 0..CEILING {
        let stream = TcpStream::connect_timeout(&address, Duration::from_secs(5))
            .unwrap_or_else(|e| panic!("connection {i}: {e}"));
        held.push(stream);
    }
    // Give the accept loop time to take all of them.
    std::thread::sleep(Duration::from_millis(500));

    // A further request connects (the backlog accepts the TCP handshake) but is
    // not served, because no permit is free.
    let mut pending = TcpStream::connect_timeout(&address, Duration::from_secs(5)).unwrap();
    pending
        .set_write_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    write!(
        pending,
        "GET /healthz HTTP/1.1\r\nHost: 127.0.0.1:{port}\r\nConnection: close\r\n\r\n"
    )
    .unwrap();
    pending
        .set_read_timeout(Some(Duration::from_secs(2)))
        .unwrap();
    let mut buffer = [0u8; 64];
    assert!(
        pending.read(&mut buffer).is_err(),
        "the server answered past its connection ceiling"
    );

    // Free exactly one permit.
    drop(held.pop().expect("one to drop"));

    // Now it is served. The read timeout is the assertion: a permit that never
    // came back would fail here.
    pending
        .set_read_timeout(Some(Duration::from_secs(10)))
        .unwrap();
    let mut response = Vec::new();
    pending
        .read_to_end(&mut response)
        .expect("the freed permit lets the waiting connection through");
    let text = String::from_utf8_lossy(&response);
    assert!(text.starts_with("HTTP/1.1 200"), "{text}");
    assert!(text.contains("\"status\":\"ok\""), "{text}");
}

/// **A client holding a connection open cannot hold the shutdown open.**
///
/// REGRESSION REASON (review finding R4). Graceful shutdown with no deadline is
/// not a shutdown: one idle keep-alive connection could keep the process past
/// any supervisor's patience and turn a clean stop into a SIGKILL.
/// `main::SHUTDOWN_GRACE` is ten seconds, and the exit is still 0 — the process
/// stopped when it was told to.
#[test]
fn a_held_connection_does_not_block_shutdown_past_the_grace_period() {
    let fixture = Fixture::new("shutdown");
    let (mut server, port) = start_server(&fixture);
    let address: SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();

    // A complete request on a keep-alive connection, answered, then held open:
    // hyper is waiting for the next request on a connection that will never
    // send one.
    let mut held = TcpStream::connect_timeout(&address, Duration::from_secs(5)).unwrap();
    held.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    write!(
        held,
        "GET /healthz HTTP/1.1\r\nHost: 127.0.0.1:{port}\r\n\r\n"
    )
    .unwrap();
    let mut first = [0u8; 12];
    held.read_exact(&mut first).unwrap();
    assert!(
        String::from_utf8_lossy(&first).starts_with("HTTP/1.1 200"),
        "{:?}",
        String::from_utf8_lossy(&first)
    );

    let mut kill = Command::new("kill");
    kill.arg("-TERM").arg(server.0.id().to_string());
    assert!(bounded_output(kill, "kill -TERM").status.success());

    // Past the grace period plus slack, but nowhere near forever.
    let started = Instant::now();
    let deadline = started + Duration::from_secs(25);
    let exit = loop {
        if let Some(exit) = server.0.try_wait().unwrap() {
            break exit;
        }
        assert!(
            Instant::now() < deadline,
            "a single held connection kept the process alive past the shutdown grace period"
        );
        std::thread::sleep(Duration::from_millis(100));
    };
    assert_eq!(
        exit.code(),
        Some(0),
        "stopping with a connection held open is still a clean stop"
    );
    assert!(
        started.elapsed() < Duration::from_secs(20),
        "the held connection delayed the exit past the grace period: {:?}",
        started.elapsed()
    );
    drop(held);
}

// WHAT THIS PAIR DOES AND DOES NOT PROVE. The test above holds an IDLE
// keep-alive connection, and hyper closes those at once — between requests
// there is nothing in flight to finish — so it exits in milliseconds rather
// than after the grace period. That is the behaviour worth having, and it is
// what the assertion checks: a held connection cannot delay the exit.
//
// The deadline itself — `main::SHUTDOWN_GRACE` elapsing and the remaining
// connections being dropped — is NOT exercised here, because every handler in
// this service answers in microseconds and none of them can be made to hang
// from the outside. Reaching it would need a deliberately slow route, and
// adding one to the product router to test the server would be the wrong
// trade. It is recorded as unexercised in the task report rather than implied
// by a passing test.
