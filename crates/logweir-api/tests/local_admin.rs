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
//! 3. **No fixed ports, AND no assumption that a free port stays free.** Every
//!    port comes from [`free_port`], which can only report a port nothing is
//!    listening on right now — it cannot reserve one. Between that answer and
//!    the child's own `bind`, any process on this host may take it, and under
//!    concurrency one regularly does.
//!
//! Rule 3's second half is a regression fix too, from 2026-09-17
//! (`FLAKE-APISHUTDOWN`). Five concurrent runs of THIS test binary failed 5
//! times in 15, every failure a socket that belonged to another run:
//! `ConnectionReset` reading a response, `ConnectionRefused` connecting to a
//! server that had just been confirmed listening, an RST on the 48th
//! connection of the ceiling test, and `0.0.0.0:64169 left a listener behind`
//! for a listener this binary never opened. None of them was a timeout, so
//! none of them would have been fixed by a longer deadline. What was wrong was
//! the identification: a connect that succeeds proves only that SOMEONE is
//! listening.
//!
//! So no test here infers a server from a port. It waits for its OWN child to
//! print [`STARTED`] naming that exact port ([`start_server`]), starts again on
//! a fresh port when the child reports [`BIND_FAILED`] instead, retries a
//! request until [`SERVE_LIMIT`] rather than reading one socket once
//! ([`http_get`]), and asserts "nothing was bound" from the child's own output
//! rather than from the port
//! ([`a_non_loopback_listener_is_refused_before_anything_is_bound`]).

use std::io::{ErrorKind, Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Command, ExitStatus, Output, Stdio};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// How long any child spawned here may run before it is killed and the test
/// fails. Startup is milliseconds and every refusal path is immediate; this is
/// a ceiling for a wedged process, not an expected wait.
///
/// WIDENED FROM 30 s FOR A LOADED HOST, not for a slow product. Two tests in
/// this suite flaked during a concurrent mutant build, both on reaching or
/// leaving the socket rather than on any property they assert. This bound and
/// [`SERVE_LIMIT`] exist to stop a WEDGED process, so doubling them costs a red
/// build nothing; the assertions about the shutdown grace period below are
/// untouched and still compare against `main::SHUTDOWN_GRACE`'s own ten
/// seconds.
///
/// THE WIDENING DID NOT BUY IMMUNITY TO A BUSY MACHINE, and the sentence that
/// once claimed it did is deleted rather than softened. Those same two tests
/// went on flaking at these limits, because they were never waiting too
/// briefly — see [`SERVE_LIMIT`] and rule 3.
const CHILD_LIMIT: Duration = Duration::from_secs(60);

/// How long the served process is given to reach its listening socket, to
/// answer a request, and to exit after SIGTERM. See [`CHILD_LIMIT`] on why this
/// is 40 s and not 20.
///
/// IT IS DELIBERATELY NOT RAISED AGAIN for `FLAKE-APISHUTDOWN`. Every one of
/// the five failures reproduced on 2026-09-17 happened in under 13 s, on a
/// socket that answered immediately — with the wrong answer, from the wrong
/// process. A deadline cannot fix a misidentification, and a bigger one only
/// makes a real hang cost more; 40 s is already several hundred times the
/// startup this binary needs on an idle host. What changed instead is what the
/// suite waits FOR: see rule 3 in the module documentation.
const SERVE_LIMIT: Duration = Duration::from_secs(40);

/// The one line the binary writes AFTER its listener is bound, and never
/// before — `main::run` logs it between `TcpListener::bind` and `serve`. It
/// carries the bound address, so it identifies the port as well as the process:
/// `{"…","message":"logweir-api started","listen":"127.0.0.1:PORT",…}`.
const STARTED: &str = "logweir-api started";

/// The line the binary writes when the port it was configured with was taken by
/// someone else first (`Address already in use`), after which it exits 1. For
/// this suite that is never a product failure — it is [`free_port`]'s answer
/// going stale — so [`start_server`] starts again on a fresh port.
const BIND_FAILED: &str = "cannot bind the listener";

/// How many fresh ports [`start_server`] will try before giving up. Losing the
/// same race five times running is no longer a race.
const START_ATTEMPTS: usize = 5;

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

/// A [`Reaped`] child whose stdout and stderr are drained into one buffer by
/// their own threads for its whole life.
///
/// The draining is the same requirement [`bounded_output`] documents — a child
/// blocked writing into a full pipe never reaches exit — but here it is also
/// the only way to hear the child SPEAK while it runs, which is how a test
/// tells this server apart from another run's server on the same port.
struct Watched {
    child: Reaped,
    output: Arc<Mutex<Vec<u8>>>,
}

impl Watched {
    /// Start the binary with `config`, draining both pipes from this moment on.
    fn spawn(config: &Path) -> Self {
        let mut child = Command::new(binary())
            .arg("--config")
            .arg(config)
            // This harness identifies its child by the INFO startup record.
            // Do not let a developer's ambient filter make a listening server
            // indistinguishable from one that never reached its bind.
            .env("RUST_LOG", "info")
            .env_remove("KUBECONFIG")
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("the binary starts");
        let output = Arc::new(Mutex::new(Vec::new()));
        let pipes: [Box<dyn Read + Send>; 2] = [
            Box::new(child.stdout.take().expect("stdout was piped")),
            Box::new(child.stderr.take().expect("stderr was piped")),
        ];
        for mut pipe in pipes {
            let sink = Arc::clone(&output);
            std::thread::spawn(move || {
                let mut buffer = [0u8; 4096];
                while let Ok(read) = pipe.read(&mut buffer) {
                    if read == 0 {
                        break;
                    }
                    sink.lock()
                        .unwrap_or_else(|poisoned| poisoned.into_inner())
                        .extend_from_slice(&buffer[..read]);
                }
            });
        }
        Self {
            child: Reaped(child),
            output,
        }
    }

    fn id(&self) -> u32 {
        self.child.0.id()
    }

    fn try_wait(&mut self) -> Option<ExitStatus> {
        self.child.0.try_wait().expect("the child is waitable")
    }

    /// Everything the child has said so far. Quoted into every failure in this
    /// file: a test that fails on a socket must show whose socket it thought it
    /// was.
    fn log(&self) -> String {
        let buffer = self
            .output
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        String::from_utf8_lossy(&buffer).into_owned()
    }
}

/// What a start attempt came to.
enum Start {
    /// The child itself said it is listening on the port it was given.
    Listening,
    /// The child could not bind: [`free_port`]'s answer went stale. Not a
    /// product failure, and not this suite's to assert — try another port.
    PortTaken,
}

/// Wait until `server` announces a listener on `port`, or fails to bind it.
///
/// Panics for anything else, quoting the child, because everything else IS the
/// product failing to start.
fn wait_until_listening(server: &mut Watched, port: u16) -> Start {
    // Both halves matter: the message proves the bind succeeded, the address
    // proves it is THIS port and not one a previous attempt used.
    let announced = format!("\"listen\":\"127.0.0.1:{port}\"");
    let deadline = Instant::now() + SERVE_LIMIT;
    loop {
        let log = server.log();
        if log.contains(STARTED) && log.contains(&announced) {
            return Start::Listening;
        }
        if let Some(exit) = server.try_wait() {
            // The reader threads can be a moment behind the child, and the
            // reason for the exit is in what they have not copied yet.
            let flushed = Instant::now() + Duration::from_secs(2);
            while Instant::now() < flushed && server.log().is_empty() {
                std::thread::sleep(Duration::from_millis(20));
            }
            let log = server.log();
            if log.contains(BIND_FAILED) {
                return Start::PortTaken;
            }
            panic!("the server exited before it listened on 127.0.0.1:{port} ({exit}):\n{log}");
        }
        if Instant::now() >= deadline {
            panic!(
                "the server did not announce a listener on 127.0.0.1:{port} within \
                 {SERVE_LIMIT:?}:\n{log}"
            );
        }
        std::thread::sleep(Duration::from_millis(20));
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
///
/// READ THE TENSE. "Right now" is the whole guarantee: this binds, asks the
/// kernel what it got, and lets go. It does not reserve the port, and nothing
/// can — a port held open is a port the child cannot bind. Every caller must
/// therefore survive losing it, which is what [`START_ATTEMPTS`] is for.
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
        // NOTHING WAS BOUND — read off the child, not off the port.
        //
        // This replaces a connect to `127.0.0.1:{port}` that asserted nothing
        // was listening afterwards, which was the third `FLAKE-APISHUTDOWN`
        // failure: on 2026-09-17 it reported `0.0.0.0:64169 left a listener
        // behind` for a listener this binary had never opened, because by then
        // `port` belonged to a concurrent run. It could only ever have gone
        // that way. The child is dead before the assertion runs — `run` waits
        // for its exit or kills it — and a dead process holds no socket, so a
        // listener seen here is by construction somebody else's, and a listener
        // this binary opened and closed again is invisible.
        //
        // The child's stdout answers the real question, and answers it for the
        // bound-then-refused case too: `STARTED` is written between a
        // successful `bind` and `serve`, `BIND_FAILED` when a bind was tried
        // and refused, and neither can appear at all here — `main` installs the
        // logging subscriber only after the configuration has been accepted, so
        // a refusal this early leaves stdout empty.
        let stdout = String::from_utf8_lossy(&out.stdout);
        assert!(
            !stdout.contains(STARTED) && !stdout.contains(BIND_FAILED),
            "{listen}: the listener was bound before the address was judged: {stdout}"
        );
        assert!(
            stdout.trim().is_empty(),
            "{listen}: the refusal came after startup got as far as logging: {stdout}"
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
    write_shared_material(&fixture, 1);

    // 6. Chart gap G1: a CA bundle that cannot be read fails CLOSED, before a
    //    socket — never a console that starts over the system roots alone and
    //    then cannot reach the issuer it was told to trust.
    let unreadable = base("https://console.example").replace(
        "  clientSecretFile:",
        &format!(
            "  caBundleFile: {}\n  clientSecretFile:",
            fixture.path("absent-ca.crt")
        ),
    );
    let out = run(&fixture.config(&unreadable), "unreadable CA bundle");
    assert_eq!(out.status.code(), Some(2));
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        stderr.contains("OIDC CA bundle") && stderr.contains("absent-ca.crt"),
        "{stderr}"
    );

    // 7. Dropping the system roots with no bundle would trust nothing at all.
    let nothing = base("https://console.example").replace(
        "  clientSecretFile:",
        "  systemRoots: false\n  clientSecretFile:",
    );
    let out = run(&fixture.config(&nothing), "no trust anchor at all");
    assert_eq!(out.status.code(), Some(2));
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(stderr.contains("oidc.systemRoots"), "{stderr}");
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

/// One answered request over a real socket, retried on a TRANSPORT failure
/// until [`SERVE_LIMIT`].
///
/// Every caller asserts something about the ANSWER — a status, a header, a
/// body — and none of them is about this particular TCP connection. A refused
/// connect or a reset mid-response is therefore not the answer being wrong, it
/// is not having got one yet, and under concurrency it is usually somebody
/// else's socket ([`free_port`]); retrying on a new connection is what makes
/// the assertion about the property.
///
/// It stays bounded, and it still fails on a server that accepts and says
/// nothing — just at [`SERVE_LIMIT`] rather than at five seconds, quoting the
/// last transport error. The fast path is unchanged: the first attempt
/// normally succeeds and nothing sleeps.
fn http_get(port: u16, path: &str, host: &str) -> String {
    let address: SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();
    let deadline = Instant::now() + SERVE_LIMIT;
    let mut last;
    loop {
        match one_http_get(&address, path, host) {
            Ok(response) => return response,
            Err(error) => last = error,
        }
        if Instant::now() >= deadline {
            panic!(
                "GET {path} (Host: {host}) on 127.0.0.1:{port} never completed within \
                 {SERVE_LIMIT:?}; last transport failure: {last}"
            );
        }
        std::thread::sleep(Duration::from_millis(100));
    }
}

/// A single attempt, with a deadline on connect, write and read. Any transport
/// failure comes back as a message for [`http_get`] to retry or report; an
/// answered request, however it answered, comes back as the answer.
fn one_http_get(address: &SocketAddr, path: &str, host: &str) -> Result<String, String> {
    let mut stream = TcpStream::connect_timeout(address, Duration::from_secs(5))
        .map_err(|error| format!("connect: {error}"))?;
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
    .map_err(|error| format!("write: {error}"))?;
    let mut response = Vec::new();
    stream
        .read_to_end(&mut response)
        .map_err(|error| format!("read: {error}"))?;
    if response.is_empty() {
        return Err("the connection closed without a byte of answer".to_owned());
    }
    Ok(String::from_utf8_lossy(&response).into_owned())
}

/// The started process, end to end on a real loopback socket: it serves the
/// UI and `/healthz`, refuses a foreign Host, reports not-ready without a
/// cluster, and exits 0 on SIGTERM.
#[test]
fn the_binary_serves_loopback_and_stops_on_sigterm() {
    let fixture = Fixture::new("serve");
    // `start_server`, not a hand-rolled spawn-and-connect: the child is
    // `Watched` (so a panic below cannot leave a listener on this host, and
    // every failure can quote the server), and the port is the one the child
    // itself reported binding rather than the one `free_port` guessed.
    let (mut child, port) = start_server(&fixture);

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
    kill.arg("-TERM").arg(child.id().to_string());
    assert!(
        bounded_output(kill, "kill -TERM").status.success(),
        "the TERM signal was not delivered"
    );
    let deadline = Instant::now() + SERVE_LIMIT;
    let exit = loop {
        if let Some(exit) = child.try_wait() {
            break exit;
        }
        assert!(
            Instant::now() < deadline,
            "the process did not stop on SIGTERM within {SERVE_LIMIT:?}:\n{}",
            child.log()
        );
        std::thread::sleep(Duration::from_millis(100));
    };
    assert_eq!(
        exit.code(),
        Some(0),
        "SIGTERM must be a clean stop:\n{}",
        child.log()
    );
}

/// Start the server and return it with the port IT SAID it is listening on.
/// The caller keeps the [`Watched`] alive for as long as it wants the server.
///
/// THIS IS THE FIX FOR `FLAKE-APISHUTDOWN`, so it is worth being plain about
/// what it replaced. The old helper connected to the port [`free_port`] had
/// suggested and returned as soon as the connect succeeded — which proves that
/// SOMETHING is listening there, not that this child is. When several runs of
/// this binary share a host they draw ports from one kernel range, and the
/// stale answer is common enough to have produced every failure recorded in
/// rule 3: the caller then held a stranger's socket while its own child was
/// dying of `Address already in use` three lines below.
///
/// The child's own [`STARTED`] line carries the bound address and is written
/// only after the bind returns, so waiting for it settles both questions at
/// once. [`BIND_FAILED`] settles the third: the port went, take another.
fn start_server(fixture: &Fixture) -> (Watched, u16) {
    let mut lost = Vec::new();
    for _ in 0..START_ATTEMPTS {
        let port = free_port();
        let config = fixture.config(&config_text(
            fixture,
            &format!("127.0.0.1:{port}"),
            &format!("http://127.0.0.1:{port}"),
            "fixture",
        ));
        let mut server = Watched::spawn(&config);
        match wait_until_listening(&mut server, port) {
            Start::Listening => return (server, port),
            // Dropped here, before the next attempt rewrites the configuration
            // this child was started with.
            Start::PortTaken => {
                drop(server);
                lost.push(port);
            }
        }
    }
    panic!(
        "the server lost the port it was given {START_ATTEMPTS} times running ({lost:?}). That is \
         no longer another process winning a race — suspect a listener this suite leaked, or a \
         fixed port somewhere it should not be."
    );
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

/// How many of [`the_connection_ceiling_holds_and_then_releases`]'s connections
/// may be waiting in the kernel's listen queue, not yet accepted, at any one
/// time.
///
/// THE LISTEN QUEUE IS SMALLER THAN THE CEILING. `kern.ipc.somaxconn` is 128
/// on macOS, and the kernel caps the listener's backlog to it whatever the
/// server asks for; when the queue overflows, XNU resets a queued connection.
/// Opening all 256 connections back to back therefore assumed the server's
/// accept loop would keep pace with the client, which is a statement about the
/// scheduler, not about the server. On a host at load average 20-40 the loop
/// fell behind by more than 128 and the test failed with `connection N:
/// Connection reset by peer`, N between 129 and 213 (FX-3, FX-8, FX-13 and
/// FX-16's workspace runs, 2026-10-07/08; FX-18). A quarter of the queue
/// leaves room for whatever else this host is connecting to the same port.
const UNACCEPTED_AT_ONCE: usize = 32;

/// GET `/healthz` on a fresh connection, retried until [`SERVE_LIMIT`], and
/// how long it took to be answered.
///
/// Used as a FIFO PROBE: the kernel hands connections to `accept` in the order
/// their handshakes completed, so an answer on a connection opened after
/// `ahead` others proves the server has accepted every one of them. That is
/// the condition [`the_connection_ceiling_holds_and_then_releases`] waits on,
/// instead of sleeping and hoping.
fn answered_after(port: u16, ahead: usize) -> Duration {
    let started = Instant::now();
    let answer = http_get(port, "/healthz", &format!("127.0.0.1:{port}"));
    assert!(
        answer.starts_with("HTTP/1.1 200"),
        "the probe behind {ahead} held connections was answered with: {answer}"
    );
    started.elapsed()
}

/// **At the connection ceiling the server stops accepting, and recovers the
/// moment a connection closes.**
///
/// `main::MAX_CONNECTIONS` is 256. The permit is taken BEFORE the accept, so at
/// the ceiling further connections wait in the kernel backlog instead of each
/// becoming a task — which is the difference between a bounded server and one
/// that runs out of memory politely.
///
/// NOTHING HERE WAITS ON THE SCHEDULER (FX-18). The connections that hold the
/// permits are opened [`UNACCEPTED_AT_ONCE`] at a time, and after each batch a
/// probe request ([`answered_after`]) must be answered before another batch is
/// opened, so the listen queue never holds more than one batch however slowly
/// the server runs. The fixed 500 ms sleep that used to stand in for "the
/// accept loop has taken them all" is gone; the probe IS that condition.
///
/// The held connections send nothing, and since FX-24 the server closes such
/// a connection at the header deadline ([`HEADER_DEADLINE`], counted from its
/// accept), which frees its permit by itself. So both assertions finish before
/// the FIRST held connection's deadline: until then every permit is held by a
/// connection this test opened, and only this test can free one.
///
/// The two assertions:
///
/// - **Held.** The pending request is not answered while every permit is
///   held. Absence of an answer can only be shown over a window, so the window
///   is MEASURED: four times the slowest probe answer, and never less than the
///   two seconds it used to be. A server without a ceiling answers within
///   about one probe's time, so on a slow host a fixed two seconds could have
///   expired before a broken server answered, and passed it. The window must
///   end [`RELEASE_MARGIN`] before the first held connection's deadline; a host
///   too slow for that fails as inconclusive, by name, rather than passing.
/// - **Released.** After one accepted connection is dropped, the pending
///   request is answered before that deadline, so the permit came back from
///   the drop and not from the deadline; one that never came back fails
///   there.
#[test]
fn the_connection_ceiling_holds_and_then_releases() {
    const CEILING: usize = 256;
    let fixture = Fixture::new("ceiling");
    let (_server, port) = start_server(&fixture);
    let address: SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();
    let connect = |i: usize| {
        TcpStream::connect_timeout(&address, Duration::from_secs(5))
            .unwrap_or_else(|e| panic!("connection {i}: {e}"))
    };

    // Every permit but one, a batch at a time, each batch proven accepted by a
    // probe. A probe needs a free permit to be answered, which is why the last
    // permit is taken separately below.
    let mut held = Vec::with_capacity(CEILING);
    let mut slowest_answer = Duration::ZERO;
    let first = Instant::now();
    while held.len() < CEILING - 1 {
        let batch = (CEILING - 1 - held.len()).min(UNACCEPTED_AT_ONCE);
        for _ in 0..batch {
            held.push(connect(held.len()));
        }
        slowest_answer = slowest_answer.max(answered_after(port, held.len()));
    }
    // The last permit. Nothing proves this one accepted, and nothing needs to:
    // the pending request below is queued behind it either way.
    held.push(connect(held.len()));

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
    let window = (slowest_answer * 4).max(Duration::from_secs(2));
    let budget = HEADER_DEADLINE
        .saturating_sub(first.elapsed())
        .saturating_sub(RELEASE_MARGIN);
    assert!(
        window <= budget,
        "inconclusive, not a product failure: filling the ceiling took {:?} and the slowest \
         probe {slowest_answer:?}, which leaves {budget:?} before the first held connection's \
         header deadline for a {window:?} window",
        first.elapsed()
    );
    pending.set_read_timeout(Some(window)).unwrap();
    let mut buffer = [0u8; 64];
    // Only an expired read timeout is "unanswered". A reset or an EOF is the
    // server (or the kernel) doing something with the connection, which is not
    // what a held ceiling does.
    match pending.read(&mut buffer) {
        Err(e) if matches!(e.kind(), ErrorKind::WouldBlock | ErrorKind::TimedOut) => {}
        other => panic!(
            "the server answered past its connection ceiling within {window:?} (slowest probe \
             {slowest_answer:?}): {other:?} {:?}",
            String::from_utf8_lossy(&buffer)
        ),
    }

    // Free exactly one permit: the FIRST connection, which the first probe
    // proved accepted.
    drop(held.swap_remove(0));

    // Now it is served, before any held connection's own deadline could have
    // freed a permit. A permit that never came back fails here.
    let before_deadline = HEADER_DEADLINE.saturating_sub(first.elapsed());
    pending
        .set_read_timeout(Some(before_deadline.max(Duration::from_millis(1))))
        .unwrap();
    let mut response = Vec::new();
    pending.read_to_end(&mut response).unwrap_or_else(|e| {
        panic!(
            "the freed permit did not let the waiting connection through within \
             {before_deadline:?}, before the held connections' own deadline: {e}"
        )
    });
    let text = String::from_utf8_lossy(&response);
    assert!(text.starts_with("HTTP/1.1 200"), "{text}");
    assert!(text.contains("\"status\":\"ok\""), "{text}");
    drop(held);
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

    // A complete request on a keep-alive connection, answered, then held open:
    // hyper is waiting for the next request on a connection that will never
    // send one. Setting that up is not the property under test, so it is
    // retried rather than unwrapped — see `hold_an_answered_keep_alive`.
    let mut held = hold_an_answered_keep_alive(&mut server, port);

    let mut kill = Command::new("kill");
    kill.arg("-TERM").arg(server.id().to_string());
    assert!(bounded_output(kill, "kill -TERM").status.success());

    // Past the grace period plus slack, but nowhere near forever.
    let started = Instant::now();
    let deadline = started + Duration::from_secs(25);
    let exit = loop {
        if let Some(exit) = server.try_wait() {
            break exit;
        }
        assert!(
            Instant::now() < deadline,
            "a single held connection kept the process alive past the shutdown grace period:\n{}",
            server.log()
        );
        std::thread::sleep(Duration::from_millis(100));
    };
    assert_eq!(
        exit.code(),
        Some(0),
        "stopping with a connection held open is still a clean stop:\n{}",
        server.log()
    );
    assert!(
        started.elapsed() < Duration::from_secs(20),
        "the held connection delayed the exit past the grace period: {:?}",
        started.elapsed()
    );

    // AND THE RESET IS THE PASS. The server took this connection down with it,
    // so reading it now ends — at EOF, or with `ConnectionReset` if the kernel
    // answered for a process that is already gone. Either is the shutdown
    // doing its job, and neither is an error to unwrap.
    //
    // Which way round matters. A reset BEFORE the signal is the server dropping
    // a live client, and `hold_an_answered_keep_alive` fails on it. A read that
    // neither ends nor resets after it is the hang, and the deadline above has
    // already caught that.
    held.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    let mut rest = Vec::new();
    match held.read_to_end(&mut rest) {
        Ok(_) => {}
        Err(error) if error.kind() == std::io::ErrorKind::ConnectionReset => {}
        Err(error) => {
            panic!("the held connection neither closed nor reset after the process exited: {error}")
        }
    }
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

/// Open a keep-alive connection to `port`, get one request answered on it, and
/// return it STILL OPEN.
///
/// Every failure here is a failure to set the test up, not a failure of the
/// thing the test asserts, and until 2026-09-17 they were the same panic: the
/// read `unwrap`ed, so a `ConnectionReset` from another run's socket came out
/// as `FLAKE-APISHUTDOWN`. It is retried on a fresh connection while the server
/// is alive and the deadline holds, and only then reported — as what it is, a
/// server resetting a live connection before anyone asked it to stop.
fn hold_an_answered_keep_alive(server: &mut Watched, port: u16) -> TcpStream {
    let address: SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();
    let deadline = Instant::now() + SERVE_LIMIT;
    let mut last;
    loop {
        if let Some(exit) = server.try_wait() {
            panic!(
                "the server exited before it was told to stop ({exit}):\n{}",
                server.log()
            );
        }
        match answered_keep_alive(&address, port) {
            Ok(held) => return held,
            Err(error) => last = error,
        }
        if Instant::now() >= deadline {
            panic!(
                "no request was answered on a held keep-alive connection to 127.0.0.1:{port} \
                 within {SERVE_LIMIT:?}; last failure: {last}. A reset here is BEFORE the \
                 shutdown signal, so it is the server dropping a live connection — only a reset \
                 after the signal is the behaviour this test is about.\n{}",
                server.log()
            );
        }
        std::thread::sleep(Duration::from_millis(100));
    }
}

/// One attempt at [`hold_an_answered_keep_alive`]. No `Connection: close`, so
/// the connection stays open behind the answer.
fn answered_keep_alive(address: &SocketAddr, port: u16) -> Result<TcpStream, String> {
    let mut held = TcpStream::connect_timeout(address, Duration::from_secs(5))
        .map_err(|error| format!("connect: {error}"))?;
    held.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    held.set_write_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    write!(
        held,
        "GET /healthz HTTP/1.1\r\nHost: 127.0.0.1:{port}\r\n\r\n"
    )
    .map_err(|error| format!("write: {error}"))?;
    let mut first = [0u8; 12];
    held.read_exact(&mut first)
        .map_err(|error| format!("read: {error}"))?;
    let status = String::from_utf8_lossy(&first).into_owned();
    if !status.starts_with("HTTP/1.1 200") {
        return Err(format!("the answer began {status:?}"));
    }
    Ok(held)
}

/// `main::HEADER_READ_TIMEOUT`, which a test cannot import from a binary.
/// [`the_header_deadline_these_rows_measure_is_mains`] reads it back out of
/// `src/main.rs`, so the rows below cannot go on measuring a deadline the
/// server no longer has.
const HEADER_DEADLINE: Duration = Duration::from_secs(10);

/// How much of [`HEADER_DEADLINE`] [`the_connection_ceiling_holds_and_then_releases`]
/// keeps for its release after its held window: the dropped connection's
/// permit must reach the pending request before any held connection's own
/// deadline could. An answer takes milliseconds; two seconds is for a loaded
/// host.
const RELEASE_MARGIN: Duration = Duration::from_secs(2);

/// How long past [`HEADER_DEADLINE`] the FX-24 rows wait for the server to act
/// before calling it a failure.
///
/// FIVE SECONDS, NOT THE R4 ROW'S FIFTEEN (FX-24 review L1). The server's own
/// timer fires on time under load (10.0 s measured at load 15-42), and the
/// slack is what tells the documented ten seconds from a deadline doubled at
/// the call site: with fifteen, a twenty-second deadline passed every row. Half
/// the deadline is still several hundred times what closing a socket takes.
const DEADLINE_SLACK: Duration = Duration::from_secs(5);

/// `count` connections to `port` that send nothing, every one proven ACCEPTED
/// but the last, plus how long the slowest FIFO probe took and when the first
/// of them connected.
///
/// The ceiling row's fill: [`UNACCEPTED_AT_ONCE`] at a time, each batch proven
/// accepted by an [`answered_after`] FIFO probe before the next is opened —
/// and being accepted is when the server's deadline for a connection starts.
/// The probe needs a free permit to be answered, which is why the last
/// connection is opened after the loop, unproven: whatever is queued behind it
/// is queued behind it either way.
fn hold_silent_connections(port: u16, count: usize) -> (Vec<TcpStream>, Duration, Instant) {
    let address: SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();
    let mut held = Vec::with_capacity(count);
    let mut slowest = Duration::ZERO;
    let first = Instant::now();
    while held.len() < count - 1 {
        for _ in 0..(count - 1 - held.len()).min(UNACCEPTED_AT_ONCE) {
            let i = held.len();
            held.push(
                TcpStream::connect_timeout(&address, Duration::from_secs(5))
                    .unwrap_or_else(|e| panic!("silent connection {i}: {e}")),
            );
        }
        slowest = slowest.max(answered_after(port, held.len()));
    }
    held.push(
        TcpStream::connect_timeout(&address, Duration::from_secs(5))
            .unwrap_or_else(|e| panic!("silent connection {}: {e}", count - 1)),
    );
    (held, slowest, first)
}

/// Read `stream` until the server ends it, for at most `limit`: `Ok(elapsed)`
/// when it closed or reset the connection, and `Err` with what happened
/// otherwise — bytes the server sent, or the read timing out with the
/// connection still open.
fn closed_by_the_server(stream: &mut TcpStream, limit: Duration) -> Result<Duration, String> {
    stream
        .set_read_timeout(Some(limit.max(Duration::from_millis(1))))
        .unwrap();
    let started = Instant::now();
    let mut sink = Vec::new();
    match stream.read_to_end(&mut sink) {
        Ok(_) if sink.is_empty() => Ok(started.elapsed()),
        Ok(_) => Err(format!(
            "the server answered before closing: {:?}",
            String::from_utf8_lossy(&sink)
        )),
        Err(error) if error.kind() == std::io::ErrorKind::ConnectionReset => Ok(started.elapsed()),
        Err(error) => Err(format!(
            "still open after {:?} ({error}), {} bytes received",
            started.elapsed(),
            sink.len()
        )),
    }
}

/// The deadline the rows below measure is the one `src/main.rs` configures,
/// and the one its connection builder is handed.
///
/// BOTH HALVES (FX-24 review L1). Pinning only the constant's text let
/// `.header_read_timeout(HEADER_READ_TIMEOUT * 2)` through: the constant still
/// read ten seconds while the server waited twenty. The call site must hand
/// the builder the constant itself, once, and nothing else may set a header
/// deadline. ([`DEADLINE_SLACK`] makes the behavioural rows catch the same
/// mutant.)
#[test]
fn the_header_deadline_these_rows_measure_is_mains() {
    let main =
        std::fs::read_to_string(Path::new(env!("CARGO_MANIFEST_DIR")).join("src/main.rs")).unwrap();
    assert!(
        main.contains(&format!(
            "const HEADER_READ_TIMEOUT: Duration = Duration::from_secs({});",
            HEADER_DEADLINE.as_secs()
        )),
        "src/main.rs no longer sets HEADER_READ_TIMEOUT to {HEADER_DEADLINE:?}; update \
         HEADER_DEADLINE in this file with it"
    );
    let code: String = main
        .lines()
        .filter(|line| !line.trim_start().starts_with("//"))
        .collect::<Vec<_>>()
        .join("\n");
    assert_eq!(
        code.matches(".header_read_timeout(").count(),
        1,
        "src/main.rs must configure the header deadline in exactly one place"
    );
    assert_eq!(
        code.matches(".header_read_timeout(HEADER_READ_TIMEOUT)")
            .count(),
        1,
        "the connection builder must be handed HEADER_READ_TIMEOUT itself, not a value \
         derived from it"
    );
}

/// **A connection that sends nothing at all is closed at the header deadline
/// (FX-24).**
///
/// REGRESSION REASON. The accept loop used hyper-util's `auto` builder, which
/// reads the first bytes to choose HTTP/1 or HTTP/2 with no timer; the header
/// deadline belonged to the HTTP/1 connection that exists only after them. So
/// the R4 row above, whose client sends a partial head, passed, while a socket
/// that never sent a byte was still open at 16 s on the built binary
/// (2026-10-08). The server now serves HTTP/1.1 directly, and hyper arms the
/// deadline in its first read.
///
/// - **Closed.** The silent connection ends from the server's side no sooner
///   than the deadline (counted from our connect, which precedes the accept)
///   and no later than the deadline plus [`DEADLINE_SLACK`].
/// - **NEGATIVE CONTROL: a client that sends in time is served.** A second
///   connection, opened at the same moment and silent for three seconds,
///   then sends a complete request and is answered `200` — so the bound is a
///   deadline, not a refusal of every slow starter.
#[test]
fn a_connection_that_sends_nothing_is_closed_at_the_header_deadline() {
    let fixture = Fixture::new("silent");
    let (_server, port) = start_server(&fixture);
    let address: SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();

    let mut silent = TcpStream::connect_timeout(&address, Duration::from_secs(5)).unwrap();
    let opened = Instant::now();
    let mut prompt = TcpStream::connect_timeout(&address, Duration::from_secs(5)).unwrap();

    std::thread::sleep(Duration::from_secs(3));
    prompt
        .set_write_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    prompt
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    write!(
        prompt,
        "GET /healthz HTTP/1.1\r\nHost: 127.0.0.1:{port}\r\nConnection: close\r\n\r\n"
    )
    .unwrap();
    let mut answer = Vec::new();
    prompt
        .read_to_end(&mut answer)
        .expect("a request sent three seconds after the connect is answered");
    let answer = String::from_utf8_lossy(&answer);
    assert!(
        answer.starts_with("HTTP/1.1 200"),
        "a client that sent in time was refused: {answer}"
    );

    let limit = (HEADER_DEADLINE + DEADLINE_SLACK).saturating_sub(opened.elapsed());
    let closed = closed_by_the_server(&mut silent, limit).map(|_| opened.elapsed());
    let closed_at = closed.unwrap_or_else(|why| {
        panic!(
            "a connection that sent nothing was not closed within {:?} of its connect: {why}",
            HEADER_DEADLINE + DEADLINE_SLACK
        )
    });
    assert!(
        closed_at >= HEADER_DEADLINE.saturating_sub(Duration::from_secs(1)),
        "the silent connection was closed after {closed_at:?}, before the {HEADER_DEADLINE:?} \
         header deadline"
    );

    let health = http_get(port, "/healthz", &format!("127.0.0.1:{port}"));
    assert!(health.starts_with("HTTP/1.1 200"), "{health}");
}

/// **Silent connections at the connection ceiling are released by the header
/// deadline, and the request queued behind them is answered (FX-24).**
///
/// This is the denial of service the finding described, end to end: every
/// one of `main::MAX_CONNECTIONS` (256) permits held by a socket that sends
/// nothing, and a real request waiting in the kernel's queue behind them. On
/// main `cee79f42` that request was still unanswered 25 s after it was sent.
///
/// - **Held first.** The request is not answered while the silent sockets
///   hold every permit, for a window measured as in the ceiling row (four
///   times the slowest probe, at least two seconds) that must end inside the
///   deadline. Without this the row would pass on a server with no ceiling at
///   all, and prove nothing about the deadline.
/// - **Then released.** It is answered `200` within the deadline plus
///   [`DEADLINE_SLACK`] of the last silent connect.
/// - **By the server.** Every silent socket has been closed or reset from the
///   server's side by then.
#[test]
fn silent_connections_cannot_hold_the_ceiling_past_the_header_deadline() {
    const CEILING: usize = 256;
    let fixture = Fixture::new("silentceiling");
    let (_server, port) = start_server(&fixture);
    let address: SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();

    let (mut held, slowest, first) = hold_silent_connections(port, CEILING);
    let last = Instant::now();

    let mut queued = TcpStream::connect_timeout(&address, Duration::from_secs(5)).unwrap();
    queued
        .set_write_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    write!(
        queued,
        "GET /healthz HTTP/1.1\r\nHost: 127.0.0.1:{port}\r\nConnection: close\r\n\r\n"
    )
    .unwrap();

    // Held first. The first silent connection's deadline is the earliest any
    // permit can come back by itself; the window must close a second before it.
    let window = (slowest * 4).max(Duration::from_secs(2));
    let budget = HEADER_DEADLINE
        .saturating_sub(first.elapsed())
        .saturating_sub(Duration::from_secs(1));
    assert!(
        window <= budget,
        "inconclusive, not a product failure: opening {CEILING} connections took {:?} and the \
         slowest probe {slowest:?}, which leaves {budget:?} before the first one's deadline for \
         a {window:?} window",
        first.elapsed()
    );
    queued.set_read_timeout(Some(window)).unwrap();
    let mut buffer = [0u8; 64];
    match queued.read(&mut buffer) {
        Err(e)
            if matches!(
                e.kind(),
                std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
            ) => {}
        other => panic!(
            "the queued request was not held behind {CEILING} silent connections for \
             {window:?}: {other:?} {:?}",
            String::from_utf8_lossy(&buffer)
        ),
    }

    // Then released, by the deadline and not by us: nothing here closes a
    // silent connection before the queued request is answered.
    let limit = (HEADER_DEADLINE + DEADLINE_SLACK).saturating_sub(last.elapsed());
    queued.set_read_timeout(Some(limit)).unwrap();
    let mut response = Vec::new();
    let read = queued.read_to_end(&mut response);
    let text = String::from_utf8_lossy(&response);
    assert!(
        read.is_ok() && text.starts_with("HTTP/1.1 200"),
        "the request queued behind {CEILING} silent connections was not answered within {:?} \
         of the last one: {read:?} {text:?}",
        HEADER_DEADLINE + DEADLINE_SLACK
    );

    // By the server: every silent connection was ended from its side.
    for (i, stream) in held.iter_mut().enumerate() {
        let limit = (HEADER_DEADLINE + DEADLINE_SLACK).saturating_sub(last.elapsed());
        if let Err(why) = closed_by_the_server(stream, limit) {
            panic!("silent connection {i} of {CEILING} was not closed by the server: {why}");
        }
    }
}

/// **A client that speaks HTTP/2 with prior knowledge is closed at once, not
/// served and not held (FX-24).**
///
/// The server is HTTP/1.1 only. hyper's HTTP/2 server has no header deadline,
/// so under the old `auto` builder a client that sent the 24-byte preface and
/// stopped was answered with a SETTINGS frame and was still open at 16 s
/// (2026-10-08): bounding only the version read would have left that hole.
/// Now the preface is an HTTP/1 request line with version `HTTP/2.0`, which
/// hyper refuses at once. "At once" is asserted as well under half the header
/// deadline, so a server that held the connection until the deadline fails
/// too, and the server goes on answering afterwards.
#[test]
fn http2_prior_knowledge_is_closed_at_once() {
    let fixture = Fixture::new("h2");
    let (_server, port) = start_server(&fixture);
    let address: SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();

    let mut h2 = TcpStream::connect_timeout(&address, Duration::from_secs(5)).unwrap();
    h2.set_write_timeout(Some(Duration::from_secs(5))).unwrap();
    h2.write_all(b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n").unwrap();
    h2.set_read_timeout(Some(HEADER_DEADLINE / 2)).unwrap();
    let started = Instant::now();
    let mut response = Vec::new();
    let read = h2.read_to_end(&mut response);
    let elapsed = started.elapsed();
    let ended = match &read {
        Ok(_) => true,
        Err(e) => e.kind() == std::io::ErrorKind::ConnectionReset,
    };
    assert!(
        ended && elapsed < HEADER_DEADLINE / 2,
        "an HTTP/2 prior-knowledge connection was not closed at once: {read:?} after \
         {elapsed:?}, {} bytes received {:?}",
        response.len(),
        &response[..response.len().min(16)]
    );
    // An HTTP/1 refusal or nothing; never an HTTP/2 frame (a SETTINGS frame
    // starts with a 3-byte length and type 0x04).
    assert!(
        response.is_empty() || response.starts_with(b"HTTP/1.1 4"),
        "the server answered HTTP/2 prior knowledge with {:?}",
        &response[..response.len().min(16)]
    );

    let health = http_get(port, "/healthz", &format!("127.0.0.1:{port}"));
    assert!(health.starts_with("HTTP/1.1 200"), "{health}");
}

// ---------------------------------------------------------------------------
// FX-24b: the stall deadline on a connection's output and a request body, and
// the total on a JSON body.
// ---------------------------------------------------------------------------

/// `main::IO_STALL_TIMEOUT`, which a test cannot import from a binary.
/// [`the_stall_and_body_deadlines_these_rows_measure_are_mains`] reads it back
/// out of `src/main.rs`.
const STALL_DEADLINE: Duration = Duration::from_secs(30);

/// How long past [`STALL_DEADLINE`] the non-reading-clients row waits for the
/// server to act, counted from the LAST client's connect.
///
/// LONGER THAN [`DEADLINE_SLACK`], AND WHY. The stall clock starts at the last
/// byte the KERNEL took, not at the last byte the client read, and a client
/// that stops reading does not stop the kernel at once: macOS grows a
/// connection's receive buffer on its own while data arrives (a socket set to
/// 4 KiB read back 326,640 bytes once connected, 2026-10-08), so the server's
/// writes keep progressing for a few seconds after the client's last read.
/// Measured on the built binary: a single non-reading client ended 35.1 s
/// after its request, 5 s of ramp plus the 30 s deadline. Fifteen seconds
/// covers that ramp three times over and still fails a deadline doubled to
/// sixty seconds.
const STALL_SLACK: Duration = Duration::from_secs(15);

/// `logweir_api::http::JSON_BODY_DEADLINE`, WRITTEN OUT rather than imported:
/// a row that took the bound from the code it measures would follow a mutant
/// that raised it, and pass at the new value. The pin row compares the two.
const BODY_DEADLINE: Duration = Duration::from_secs(60);

/// The deadline the FX-24b rows measure is the one `src/main.rs` configures
/// and hands to both guards, and the JSON body's total is the documented one.
///
/// THE CALL SITES TOO (the FX-24 review's L1 lesson): a constant that still
/// reads thirty seconds proves nothing if a guard is built with something
/// else, so each guard must be handed `IO_STALL_TIMEOUT` itself, once — and,
/// since FX-24c, the body's guard `BODY_MIN_PROGRESS` itself as its floor
/// ([`the_rate_floor_and_peer_cap_these_rows_measure_are_mains`]).
#[test]
fn the_stall_and_body_deadlines_these_rows_measure_are_mains() {
    let main =
        std::fs::read_to_string(Path::new(env!("CARGO_MANIFEST_DIR")).join("src/main.rs")).unwrap();
    assert!(
        main.contains(&format!(
            "const IO_STALL_TIMEOUT: Duration = Duration::from_secs({});",
            STALL_DEADLINE.as_secs()
        )),
        "src/main.rs no longer sets IO_STALL_TIMEOUT to {STALL_DEADLINE:?}; update \
         STALL_DEADLINE in this file with it"
    );
    let code: String = main
        .lines()
        .filter(|line| !line.trim_start().starts_with("//"))
        .collect::<Vec<_>>()
        .join("\n");
    for (call, what) in [
        (
            "StallGuard::new(TokioIo::new(stream), IO_STALL_TIMEOUT)",
            "the connection's IO",
        ),
        (
            "StallBody::new(body, IO_STALL_TIMEOUT, BODY_MIN_PROGRESS)",
            "each request body",
        ),
    ] {
        assert_eq!(
            code.matches(call).count(),
            1,
            "{what} must be guarded with IO_STALL_TIMEOUT (and a body with \
             BODY_MIN_PROGRESS) itself, exactly once: `{call}`"
        );
    }
    assert_eq!(
        code.matches("StallGuard::new(").count() + code.matches("StallBody::new(").count(),
        2,
        "src/main.rs builds a guard somewhere else too"
    );
    assert_eq!(
        logweir_api::http::JSON_BODY_DEADLINE,
        BODY_DEADLINE,
        "the JSON body's total deadline moved; update BODY_DEADLINE, the docs and the release \
         notes with it"
    );
    assert!(
        BODY_DEADLINE > STALL_DEADLINE + DEADLINE_SLACK,
        "the body rows tell the stall from the total only while the total is the later one"
    );
}

/// A current-thread runtime for the few socket options `std` does not offer.
fn socket_runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("a current-thread runtime")
}

/// A blocking connection to `address` whose kernel receive buffer was set to
/// `receive_buffer` bytes before the handshake.
///
/// WHY. A client that stops reading stops the server's writes only once the
/// server's send buffer and the client's receive buffer are both full. With
/// the defaults that is about half a MiB per connection on macOS (measured
/// 2026-10-08: 556,016 bytes), 140 MB of kernel memory for 256 of them; with a
/// 4 KiB receive buffer it is the server's send buffer alone.
fn connect_with_receive_buffer(
    runtime: &tokio::runtime::Runtime,
    address: SocketAddr,
    receive_buffer: u32,
) -> Result<TcpStream, String> {
    runtime.block_on(async move {
        let socket = tokio::net::TcpSocket::new_v4().map_err(|e| format!("socket: {e}"))?;
        socket
            .set_recv_buffer_size(receive_buffer)
            .map_err(|e| format!("SO_RCVBUF: {e}"))?;
        let stream = tokio::time::timeout(Duration::from_secs(5), socket.connect(address))
            .await
            .map_err(|_| "connect: timed out".to_owned())?
            .map_err(|e| format!("connect: {e}"))?;
        let stream = stream.into_std().map_err(|e| format!("into_std: {e}"))?;
        stream
            .set_nonblocking(false)
            .map_err(|e| format!("blocking: {e}"))?;
        Ok(stream)
    })
}

/// Read until the server ends `stream`, for at most `limit`, keeping what it
/// sent. `Ok((elapsed, bytes))` on EOF or reset; `Err` naming what happened
/// otherwise.
fn answered_then_closed(
    stream: &mut TcpStream,
    limit: Duration,
) -> Result<(Duration, Vec<u8>), String> {
    let started = Instant::now();
    let mut received = Vec::new();
    let mut buffer = [0u8; 16 * 1024];
    loop {
        let left = limit.saturating_sub(started.elapsed());
        if left.is_zero() {
            return Err(format!(
                "still open after {:?}, {} bytes received: {:?}",
                started.elapsed(),
                received.len(),
                String::from_utf8_lossy(&received[..received.len().min(200)])
            ));
        }
        stream
            .set_read_timeout(Some(left.min(Duration::from_millis(500))))
            .unwrap();
        match stream.read(&mut buffer) {
            Ok(0) => return Ok((started.elapsed(), received)),
            Ok(n) => received.extend_from_slice(&buffer[..n]),
            Err(e) if matches!(e.kind(), ErrorKind::WouldBlock | ErrorKind::TimedOut) => {}
            Err(e) if e.kind() == ErrorKind::ConnectionReset => {
                return Ok((started.elapsed(), received))
            }
            Err(e) => return Err(format!("read failed after {:?}: {e}", started.elapsed())),
        }
    }
}

/// The size of the largest answer the binary can give without a cluster:
/// `ui/render.js`, served whole.
fn render_js_size() -> usize {
    usize::try_from(
        std::fs::metadata(repo_root().join("ui/render.js"))
            .expect("ui/render.js exists")
            .len(),
    )
    .unwrap()
}

/// **Clients that stop reading cannot hold the ceiling past the stall
/// deadline, and the request queued behind them is answered (FX-24b).**
///
/// The FX-24 review's reproduction, as a row: every one of the 256 connection
/// permits held by a client that pipelined requests for a 156 KiB asset and
/// reads nothing, and a real request waiting in the kernel's queue behind
/// them. Before FX-24b that request was unanswered at 30 s and every
/// non-reading connection was still open at 32.9 s (the review's probe on the
/// built binary).
///
/// - **Held first.** The queued request is not answered while the
///   non-reading clients hold every permit, for a measured window (as in the
///   ceiling rows), so the row cannot pass on a server with no ceiling.
/// - **Released by the stall deadline, not by something else.** It is
///   answered no sooner than the deadline (less a second) after the first
///   non-reading client connected, and within the deadline plus
///   [`STALL_SLACK`] of the last. A release at ten seconds would be the
///   header deadline, meaning the answers had fitted in the kernel's buffers
///   and the row had stalled nothing, and it fails here by name.
/// - **By the server, cut short.** By the same bound every non-reading
///   connection has been ended from the server's side, each before its
///   sixteen answers were all sent, and the server still answers afterwards.
///   Nothing reads a non-reading connection before then: a read is progress,
///   and would keep it alive.
#[test]
fn clients_that_stop_reading_cannot_hold_the_ceiling_past_the_stall_deadline() {
    const CEILING: usize = 256;
    // Answers far past the server's send buffer, so every connection's write
    // is pending: sixteen of the asset is about 2.5 MB requested per
    // connection, against a send buffer of a few hundred KiB at most. The
    // server never writes more than its buffer holds, so this costs nothing.
    const PIPELINED: usize = 16;
    let fixture = Fixture::new("slowreaders");
    let (_server, port) = start_server(&fixture);
    let address: SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();
    let runtime = socket_runtime();
    let request = format!("GET /ui/render.js HTTP/1.1\r\nHost: 127.0.0.1:{port}\r\n\r\n");
    let requests = request.repeat(PIPELINED);
    let open = |i: usize| {
        let mut stream = connect_with_receive_buffer(&runtime, address, 4096)
            .unwrap_or_else(|e| panic!("non-reading connection {i}: {e}"));
        stream
            .set_write_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        stream
            .write_all(requests.as_bytes())
            .unwrap_or_else(|e| panic!("non-reading connection {i}: write: {e}"));
        stream
    };

    // The ceiling rows' fill: a batch at a time, each proven accepted by a
    // FIFO probe, the last connection opened unproven.
    let mut held = Vec::with_capacity(CEILING);
    let mut slowest = Duration::ZERO;
    let first = Instant::now();
    while held.len() < CEILING - 1 {
        for _ in 0..(CEILING - 1 - held.len()).min(UNACCEPTED_AT_ONCE) {
            held.push(open(held.len()));
        }
        slowest = slowest.max(answered_after(port, held.len()));
    }
    held.push(open(CEILING - 1));
    let last = Instant::now();

    let mut queued = TcpStream::connect_timeout(&address, Duration::from_secs(5)).unwrap();
    queued
        .set_write_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    write!(
        queued,
        "GET /healthz HTTP/1.1\r\nHost: 127.0.0.1:{port}\r\nConnection: close\r\n\r\n"
    )
    .unwrap();

    // Held first.
    let window = (slowest * 4).max(Duration::from_secs(2));
    let budget = STALL_DEADLINE
        .saturating_sub(first.elapsed())
        .saturating_sub(Duration::from_secs(1));
    assert!(
        window <= budget,
        "inconclusive, not a product failure: filling the ceiling took {:?} and the slowest \
         probe {slowest:?}, which leaves {budget:?} before the first connection's stall \
         deadline for a {window:?} window",
        first.elapsed()
    );
    queued.set_read_timeout(Some(window)).unwrap();
    let mut buffer = [0u8; 64];
    match queued.read(&mut buffer) {
        Err(e) if matches!(e.kind(), ErrorKind::WouldBlock | ErrorKind::TimedOut) => {}
        other => panic!(
            "the queued request was not held behind {CEILING} non-reading connections for \
             {window:?}: {other:?} {:?}",
            String::from_utf8_lossy(&buffer)
        ),
    }

    // Then released, by the stall deadline: nothing here reads or closes a
    // non-reading connection before the queued request is answered.
    let limit = (STALL_DEADLINE + STALL_SLACK).saturating_sub(last.elapsed());
    queued.set_read_timeout(Some(limit)).unwrap();
    let mut response = Vec::new();
    let read = queued.read_to_end(&mut response);
    let answered_at = first.elapsed();
    let text = String::from_utf8_lossy(&response);
    assert!(
        read.is_ok() && text.starts_with("HTTP/1.1 200"),
        "the request queued behind {CEILING} non-reading connections was not answered within \
         {:?} of the last one: {read:?} {text:?}",
        STALL_DEADLINE + STALL_SLACK
    );
    assert!(
        answered_at >= STALL_DEADLINE.saturating_sub(Duration::from_secs(1)),
        "the queued request was answered {answered_at:?} after the first non-reading client \
         connected, before the {STALL_DEADLINE:?} stall deadline: a permit came back some \
         other way (at about ten seconds, the header deadline: the answers fitted in the \
         kernel's buffers and no write was ever pending)"
    );

    // By the server, cut short. Wait out the same bound first, reading
    // nothing; then each connection must already be over: a reset, or EOF
    // after what the kernel had buffered, in under two seconds and short of
    // the sixteen answers. A connection the server never ended would instead
    // be read to the end of all sixteen, or still be open.
    std::thread::sleep((STALL_DEADLINE + STALL_SLACK).saturating_sub(last.elapsed()));
    let whole = PIPELINED * render_js_size();
    for (i, stream) in held.iter_mut().enumerate() {
        match answered_then_closed(stream, Duration::from_secs(2)) {
            Ok((_, received)) if received.len() < whole => {}
            Ok((_, received)) => panic!(
                "non-reading connection {i} of {CEILING} was sent all {} bytes of its answers \
                 once read: the server never ended it",
                received.len()
            ),
            Err(why) => panic!(
                "non-reading connection {i} of {CEILING} was not ended by the server within {:?} \
                 of the last connect: {why}",
                STALL_DEADLINE + STALL_SLACK
            ),
        }
    }
    let health = http_get(port, "/healthz", &format!("127.0.0.1:{port}"));
    assert!(health.starts_with("HTTP/1.1 200"), "{health}");
}

/// **One client that stops reading is ended by the stall deadline (FX-24b).**
///
/// The ceiling row above proves the permit comes back, but it opens 256
/// sockets, and a connection flood may not run on a host that is serving a
/// compose stack or the PoC (WORKER-RULES, 2026-10-08). Without this row the
/// mutants that drop or blunt the OUTPUT clock were caught at the binary level
/// only by that flood row (FX-24b review L3). So: one connection, through a
/// 4 KiB receive buffer, pipelines sixteen requests for the 156 KiB asset and
/// reads nothing. Nothing reads it before the stall deadline plus
/// [`STALL_SLACK`] — a read is progress — and then it must already be over:
/// EOF or reset within two seconds, short of its sixteen answers. A server
/// without the output clock is still writing, and hands over every answer once
/// read. A request on another connection is answered meanwhile, so the wait is
/// this connection's, not the server's.
#[test]
fn a_client_that_stops_reading_is_ended_at_the_stall_deadline() {
    const PIPELINED: usize = 16;
    let fixture = Fixture::new("nonreader");
    let (_server, port) = start_server(&fixture);
    let address: SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();
    let runtime = socket_runtime();
    let mut stopped = connect_with_receive_buffer(&runtime, address, 4096).unwrap();
    stopped
        .set_write_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    let request = format!("GET /ui/render.js HTTP/1.1\r\nHost: 127.0.0.1:{port}\r\n\r\n");
    stopped
        .write_all(request.repeat(PIPELINED).as_bytes())
        .unwrap();
    let sent = Instant::now();

    let health = http_get(port, "/healthz", &format!("127.0.0.1:{port}"));
    assert!(health.starts_with("HTTP/1.1 200"), "{health}");

    std::thread::sleep((STALL_DEADLINE + STALL_SLACK).saturating_sub(sent.elapsed()));
    let whole = PIPELINED * render_js_size();
    match answered_then_closed(&mut stopped, Duration::from_secs(2)) {
        Ok((_, received)) if received.len() < whole => {}
        Ok((_, received)) => panic!(
            "the connection that stopped reading was sent all {} bytes of its answers once \
             read: the server never ended it",
            received.len()
        ),
        Err(why) => panic!(
            "the connection that stopped reading was not ended by the server within {:?} of \
             its requests: {why}",
            STALL_DEADLINE + STALL_SLACK
        ),
    }
    let health = http_get(port, "/healthz", &format!("127.0.0.1:{port}"));
    assert!(health.starts_with("HTTP/1.1 200"), "{health}");
}

/// **A slow but steady reader outlasts the stall deadline (FX-24b).**
///
/// NEGATIVE CONTROL for the row above: the bound is on a STALL. One client
/// pipelines fourteen requests for the 156 KiB asset (the last asks to
/// close) and reads 8 KiB every 150 ms through a 16 KiB receive buffer, so
/// the server's writes are pending nearly the whole time and the transfer
/// takes well past the deadline in all; every byte arrives, and the server
/// ends the connection only after the last answer, at the client's request.
/// A deadline that counted the whole answer, or the connection's age, fails
/// here.
#[test]
fn a_slow_but_steady_reader_outlasts_the_stall_deadline() {
    const ANSWERS: usize = 14;
    let fixture = Fixture::new("steadyreader");
    let (_server, port) = start_server(&fixture);
    let address: SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();
    let runtime = socket_runtime();
    let mut stream = connect_with_receive_buffer(&runtime, address, 16 * 1024).unwrap();
    stream
        .set_write_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    let keep = format!("GET /ui/render.js HTTP/1.1\r\nHost: 127.0.0.1:{port}\r\n\r\n");
    let close = format!(
        "GET /ui/render.js HTTP/1.1\r\nHost: 127.0.0.1:{port}\r\nConnection: close\r\n\r\n"
    );
    stream
        .write_all(format!("{}{close}", keep.repeat(ANSWERS - 1)).as_bytes())
        .unwrap();

    let started = Instant::now();
    let mut received = 0usize;
    let mut buffer = [0u8; 8 * 1024];
    stream
        .set_read_timeout(Some(Duration::from_secs(10)))
        .unwrap();
    let ended = loop {
        std::thread::sleep(Duration::from_millis(150));
        match stream.read(&mut buffer) {
            Ok(0) => break Ok(()),
            Ok(n) => received += n,
            Err(e) => break Err(e),
        }
        assert!(
            started.elapsed() < STALL_DEADLINE * 4,
            "the transfer is still running after {:?}",
            started.elapsed()
        );
    };
    let elapsed = started.elapsed();
    assert!(
        ended.is_ok(),
        "a steady reader's connection was cut after {elapsed:?} and {received} bytes: {ended:?}"
    );
    assert!(
        received >= ANSWERS * render_js_size(),
        "the connection ended after {received} bytes, before {ANSWERS} answers of {} bytes",
        render_js_size()
    );
    assert!(
        elapsed > STALL_DEADLINE + DEADLINE_SLACK,
        "the transfer took only {elapsed:?}, so a deadline on the whole answer would not have \
         cut it either: the row proves nothing about a stall"
    );
}

/// A Kubernetes API server, plain HTTP on loopback, that serves one running
/// `Backup`, `team-a/sse`, for as long as it is alive, and 404 for anything
/// else. kube-client dials `http://` servers (`https_or_http`).
struct FakeKube {
    port: u16,
    reads: Arc<std::sync::atomic::AtomicUsize>,
    stop: Arc<std::sync::atomic::AtomicBool>,
}

impl FakeKube {
    fn start() -> Self {
        use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
        let path =
            repo_root().join("crates/logweir-api/tests/fixtures/backup-succeeded-verified.json");
        let mut object: serde_json::Value =
            serde_json::from_str(&std::fs::read_to_string(path).unwrap()).unwrap();
        object["metadata"]["name"] = "sse".into();
        object["metadata"]["namespace"] = "team-a".into();
        // Unsettled, as `operation_stream.rs` makes one: the stream stays open.
        object["status"]["phase"] = "Running".into();
        object["status"]["progress"]["stage"] = "Running".into();
        for key in ["exitCode", "exitReason", "evidence"] {
            object["status"].as_object_mut().unwrap().remove(key);
        }
        let backup = object.to_string();
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let port = listener.local_addr().unwrap().port();
        let reads = Arc::new(AtomicUsize::new(0));
        let stop = Arc::new(AtomicBool::new(false));
        let (thread_reads, thread_stop) = (Arc::clone(&reads), Arc::clone(&stop));
        std::thread::spawn(move || {
            // Bounded twice: by the owner's stop flag, and by a ceiling well
            // past any row that uses it.
            let until = Instant::now() + Duration::from_secs(300);
            while !thread_stop.load(Ordering::Relaxed) && Instant::now() < until {
                let Ok((mut stream, _)) = listener.accept() else {
                    std::thread::sleep(Duration::from_millis(20));
                    continue;
                };
                let backup = backup.clone();
                let reads = Arc::clone(&thread_reads);
                std::thread::spawn(move || {
                    let _ = stream.set_nonblocking(false);
                    let _ = stream.set_read_timeout(Some(Duration::from_secs(5)));
                    let mut head = Vec::new();
                    let mut byte = [0u8; 1];
                    while !head.ends_with(b"\r\n\r\n") && head.len() < 16 * 1024 {
                        match stream.read(&mut byte) {
                            Ok(1) => head.push(byte[0]),
                            _ => return,
                        }
                    }
                    let line = String::from_utf8_lossy(&head);
                    let path = line.split_whitespace().nth(1).unwrap_or("");
                    let (status, body) = if path
                        .split('?')
                        .next()
                        .is_some_and(|p| p.ends_with("/namespaces/team-a/backups/sse"))
                    {
                        reads.fetch_add(1, Ordering::Relaxed);
                        ("200 OK", backup)
                    } else {
                        (
                            "404 Not Found",
                            r#"{"kind":"Status","apiVersion":"v1","metadata":{},"status":"Failure","reason":"NotFound","code":404}"#
                                .to_owned(),
                        )
                    };
                    let _ = write!(
                        stream,
                        "HTTP/1.1 {status}\r\ncontent-type: application/json\r\ncontent-length: \
                         {}\r\nconnection: close\r\n\r\n{body}",
                        body.len()
                    );
                });
            }
        });
        Self { port, reads, stop }
    }

    fn reads(&self) -> usize {
        self.reads.load(std::sync::atomic::Ordering::Relaxed)
    }
}

impl Drop for FakeKube {
    fn drop(&mut self) {
        self.stop.store(true, std::sync::atomic::Ordering::Relaxed);
    }
}

/// **An event stream that heartbeats outlives the stall deadline (FX-24b).**
///
/// The console's one long-lived answer is the operation event stream: up to
/// 300 s, a heartbeat every 15 s of silence. Between heartbeats the server
/// has nothing to write, and for the whole stream hyper keeps a read pending
/// on the socket to notice the client leaving. So the stream is cut at the
/// deadline by either of the two easy mistakes: timing the connection's
/// reads, or timing the answer as a whole. The client here reads normally,
/// through the shipped binary against a fake API server that serves a
/// running backup, for the deadline plus 25 s: the stream must still be open,
/// with no `end` frame, and must have delivered a heartbeat after the
/// deadline had passed.
#[test]
fn an_event_stream_that_heartbeats_outlives_the_stall_deadline() {
    let kube = FakeKube::start();
    let fixture = Fixture::new("eventstream");
    std::fs::write(
        fixture.0.join("kubeconfig.yaml"),
        format!(
            "apiVersion: v1\nkind: Config\nclusters:\n- name: fixture\n  cluster:\n    server: \
             http://127.0.0.1:{}\ncontexts:\n- name: fixture\n  context:\n    cluster: fixture\n    \
             user: fixture\nusers:\n- name: fixture\n  user: {{}}\n",
            kube.port
        ),
    )
    .unwrap();
    let (server, port) = start_server(&fixture);
    let address: SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();

    let mut stream = TcpStream::connect_timeout(&address, Duration::from_secs(5)).unwrap();
    stream
        .set_write_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    write!(
        stream,
        "GET /api/v1/namespaces/team-a/operations/backup/sse/events HTTP/1.1\r\nHost: \
         127.0.0.1:{port}\r\nAccept: text/event-stream\r\n\r\n"
    )
    .unwrap();
    let opened = Instant::now();
    let watch = STALL_DEADLINE + Duration::from_secs(25);
    let mut received = Vec::new();
    let mut heartbeats = Vec::new();
    let mut buffer = [0u8; 16 * 1024];
    let mut ended = None;
    while opened.elapsed() < watch {
        stream
            .set_read_timeout(Some(Duration::from_millis(500)))
            .unwrap();
        match stream.read(&mut buffer) {
            Ok(0) => {
                ended = Some("EOF".to_owned());
                break;
            }
            Ok(n) => {
                received.extend_from_slice(&buffer[..n]);
                let beats = String::from_utf8_lossy(&received)
                    .matches("event: heartbeat")
                    .count();
                while heartbeats.len() < beats {
                    heartbeats.push(opened.elapsed());
                }
            }
            Err(e) if matches!(e.kind(), ErrorKind::WouldBlock | ErrorKind::TimedOut) => {}
            Err(e) => {
                ended = Some(e.to_string());
                break;
            }
        }
    }
    let text = String::from_utf8_lossy(&received).into_owned();
    assert!(
        text.starts_with("HTTP/1.1 200") && text.contains("text/event-stream"),
        "the stream did not open: {:?}\n{}",
        &text[..text.len().min(300)],
        server.log()
    );
    assert!(
        text.contains("event: operation"),
        "no snapshot frame (the fake API server answered {} reads): {:?}",
        kube.reads(),
        &text[..text.len().min(600)]
    );
    assert!(
        ended.is_none(),
        "the event stream was ended after {:?} ({}), with heartbeats at {heartbeats:?}",
        opened.elapsed(),
        ended.unwrap_or_default()
    );
    assert!(
        !text.contains("event: end"),
        "the stream sent `end` within {watch:?}: {text:?}"
    );
    assert!(
        heartbeats
            .iter()
            .any(|at| *at > STALL_DEADLINE + Duration::from_secs(1)),
        "no heartbeat arrived after the {STALL_DEADLINE:?} deadline had passed (heartbeats at \
         {heartbeats:?})"
    );
}

/// The head of a localAdmin `POST .../backups` that passes every check before
/// the handler reads its body — the configured origin, JSON, an idempotency
/// key — and announces `length` body bytes.
fn backup_post_head(port: u16, length: usize, key: &str) -> String {
    format!(
        "POST /api/v1/namespaces/team-a/backups HTTP/1.1\r\nHost: 127.0.0.1:{port}\r\nOrigin: \
         http://127.0.0.1:{port}\r\nContent-Type: application/json\r\nIdempotency-Key: \
         {key}\r\nContent-Length: {length}\r\n\r\n"
    )
}

/// **A signed-in client that stops sending its body is answered and closed at
/// the stall deadline (FX-24b).**
///
/// The local administrator is the signed-in actor in localAdmin mode, and
/// `POST .../backups` reads its body only after authorizing it. The client
/// announces 4096 bytes, sends ten and stops: the body read is pending with
/// nothing arriving, so the stall deadline ends it with `400
/// malformed_request`, "stopped arriving", and the server closes the
/// connection after the answer. It must be the STALL, at thirty seconds and
/// not at the sixty-second total: a server whose body has no stall clock of
/// its own fails here, as does one with no deadline at all.
///
/// NEGATIVE CONTROL: a second client, opened at the same moment, waits three
/// seconds before sending a complete body; it is read and judged on its
/// content, not refused for being slow to start.
#[test]
fn a_signed_in_body_that_stops_is_closed_at_the_stall_deadline() {
    let fixture = Fixture::new("bodystop");
    let (_server, port) = start_server(&fixture);
    let address: SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();

    let mut stopped = TcpStream::connect_timeout(&address, Duration::from_secs(5)).unwrap();
    stopped
        .set_write_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    stopped
        .write_all(
            format!(
                "{}{{\"topics\":",
                backup_post_head(port, 4096, "fx24b-stop-0001")
            )
            .as_bytes(),
        )
        .unwrap();
    let sent = Instant::now();

    let mut prompt = TcpStream::connect_timeout(&address, Duration::from_secs(5)).unwrap();
    prompt
        .set_write_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    let body = "{}";
    prompt
        .write_all(backup_post_head(port, body.len(), "fx24b-prompt-0001").as_bytes())
        .unwrap();
    std::thread::sleep(Duration::from_secs(3));
    prompt.write_all(body.as_bytes()).unwrap();
    let (_, answer) = answered_then_closed(&mut prompt, Duration::from_secs(20))
        .unwrap_or_else(|why| panic!("a complete body sent after three seconds: {why}"));
    let answer = String::from_utf8_lossy(&answer);
    assert!(
        answer.starts_with("HTTP/1.1 ")
            && !answer.contains("stopped arriving")
            && !answer.contains("not received within"),
        "a body sent three seconds after its head was refused as late: {answer}"
    );

    let limit = (STALL_DEADLINE + DEADLINE_SLACK).saturating_sub(sent.elapsed());
    let (_, response) = answered_then_closed(&mut stopped, limit).unwrap_or_else(|why| {
        panic!(
            "a body that stopped was not answered and closed within {:?}: {why}",
            STALL_DEADLINE + DEADLINE_SLACK
        )
    });
    let closed_at = sent.elapsed();
    let response = String::from_utf8_lossy(&response);
    assert!(
        response.starts_with("HTTP/1.1 400")
            && response.contains("\"code\":\"malformed_request\"")
            && response.contains("stopped arriving"),
        "a body that stopped was answered with: {response}"
    );
    assert!(
        closed_at >= STALL_DEADLINE.saturating_sub(Duration::from_secs(1)),
        "a body that stopped was ended after {closed_at:?}, before the {STALL_DEADLINE:?} stall \
         deadline"
    );
    let health = http_get(port, "/healthz", &format!("127.0.0.1:{port}"));
    assert!(health.starts_with("HTTP/1.1 200"), "{health}");
}

/// **A signed-in client that trickles its body is answered and closed at the
/// total deadline (FX-24b).**
///
/// 8 KiB of spaces (which JSON allows) every four seconds: 60 KiB a window,
/// past the 32 KiB floor (FX-24c), so the body's progress window never ends it
/// and only `read_json`'s total can — at sixty seconds, with `400
/// malformed_request`, "not received within 60 seconds", and the connection
/// closed after the answer. A server with no total fails here; so does one
/// whose total is the window's. (Before FX-24c the drip was one byte, which
/// the floor now ends at the window instead:
/// [`a_signed_in_body_below_the_rate_floor_is_closed_at_the_window`].)
///
/// THE DRIPS STOP EIGHT SECONDS SHORT OF THE TOTAL (FX-24b review L1). The
/// first version dripped on its five-second read timeout, which divides sixty,
/// so the twelfth byte was due the instant the server gave up on the body. A
/// byte that lands after the server dropped the body is unread when it closes
/// the socket, the kernel answers with a reset, and the next socket option
/// call on the reset socket failed with `EINVAL` on macOS (2 of 8 runs under
/// load). Now the last byte is sent at 52 s and is read long before the
/// server's 60 s; the 8 s gap after it is still far inside the 30 s stall. The
/// read timeout is set once, before the first byte, and the loop paces itself
/// on the clock.
#[test]
fn a_signed_in_body_that_trickles_is_closed_at_the_total_deadline() {
    const DRIP_EVERY: Duration = Duration::from_secs(4);
    const LAST_DRIP_BEFORE_THE_TOTAL: Duration = Duration::from_secs(8);
    const DRIP: usize = 8 * 1024;
    let fixture = Fixture::new("bodytrickle");
    let (_server, port) = start_server(&fixture);
    let address: SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();

    let mut trickle = TcpStream::connect_timeout(&address, Duration::from_secs(5)).unwrap();
    trickle
        .set_write_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    trickle
        .set_read_timeout(Some(Duration::from_millis(250)))
        .unwrap();
    // Announced past everything the drips send, and under the 1 MiB cap.
    trickle
        .write_all(backup_post_head(port, 256 * 1024, "fx24b-trickle-0001").as_bytes())
        .unwrap();
    let drip = vec![b' '; DRIP];
    let sent = Instant::now();
    let limit = BODY_DEADLINE + DEADLINE_SLACK;
    let last_drip = BODY_DEADLINE.saturating_sub(LAST_DRIP_BEFORE_THE_TOTAL);
    let mut next_drip = DRIP_EVERY;
    let mut received = Vec::new();
    let mut buffer = [0u8; 4096];
    let mut dripped = 0;
    let closed_at = loop {
        assert!(
            sent.elapsed() < limit,
            "a trickling body was not answered and closed within {limit:?} ({dripped} drips \
             sent, {} received: {:?})",
            received.len(),
            String::from_utf8_lossy(&received)
        );
        if received.is_empty() && next_drip <= last_drip && sent.elapsed() >= next_drip {
            trickle
                .write_all(&drip)
                .unwrap_or_else(|e| panic!("drip at {:?}: {e}", sent.elapsed()));
            dripped += 1;
            next_drip += DRIP_EVERY;
        }
        match trickle.read(&mut buffer) {
            Ok(0) => break sent.elapsed(),
            Ok(n) => received.extend_from_slice(&buffer[..n]),
            Err(e) if matches!(e.kind(), ErrorKind::WouldBlock | ErrorKind::TimedOut) => {}
            Err(e) if e.kind() == ErrorKind::ConnectionReset => break sent.elapsed(),
            Err(e) => panic!(
                "the trickling connection failed after {:?}: {e}",
                sent.elapsed()
            ),
        }
    };
    let response = String::from_utf8_lossy(&received);
    assert!(
        response.starts_with("HTTP/1.1 400")
            && response.contains("\"code\":\"malformed_request\"")
            && response.contains(&format!(
                "not received within {} seconds",
                BODY_DEADLINE.as_secs()
            )),
        "a trickling body was answered with: {response}"
    );
    assert!(
        closed_at >= BODY_DEADLINE.saturating_sub(Duration::from_secs(1)),
        "a trickling body was ended after {closed_at:?}, before the {BODY_DEADLINE:?} total \
         deadline ({dripped} drips sent)"
    );
    // Every drip from 4 s to 52 s was sent: thirteen.
    let expected = (last_drip.as_secs() / DRIP_EVERY.as_secs()) as usize;
    assert_eq!(
        dripped, expected,
        "{dripped} drips were sent, not {expected}: the body was not kept moving"
    );
}

// ---------------------------------------------------------------------------
// FX-24c: what the stall does to a client that reads a byte at a time, the
// rate floor on a request body, and the cap on the connections one peer
// outside the trusted-proxy set may hold.
// ---------------------------------------------------------------------------

/// `main::BODY_MIN_PROGRESS`, which a test cannot import from a binary.
/// [`the_rate_floor_and_peer_cap_these_rows_measure_are_mains`] reads it back
/// out of `src/main.rs`.
const RATE_FLOOR: usize = 32 * 1024;

/// `main::MAX_CONNECTIONS_PER_PEER`, read back the same way.
const PEER_CAP: usize = 32;

/// The slow-rate client's pace: ONE byte every twenty seconds, the shape of
/// the FX-24b review's measurement M1.
const SLOW_READ_EVERY: Duration = Duration::from_secs(20);

/// The floor and the cap the FX-24c rows measure are the ones `src/main.rs`
/// configures, handed over at their one call site each.
///
/// The body guard's call site, with `BODY_MIN_PROGRESS` as its floor, is
/// pinned by [`the_stall_and_body_deadlines_these_rows_measure_are_mains`];
/// this row pins the two constants and the cap's call site.
#[test]
fn the_rate_floor_and_peer_cap_these_rows_measure_are_mains() {
    let main =
        std::fs::read_to_string(Path::new(env!("CARGO_MANIFEST_DIR")).join("src/main.rs")).unwrap();
    for (constant, expected) in [
        (
            "BODY_MIN_PROGRESS",
            format!(
                "const BODY_MIN_PROGRESS: usize = {} * 1024;",
                RATE_FLOOR / 1024
            ),
        ),
        (
            "MAX_CONNECTIONS_PER_PEER",
            format!("const MAX_CONNECTIONS_PER_PEER: usize = {PEER_CAP};"),
        ),
    ] {
        assert!(
            main.contains(&expected),
            "src/main.rs no longer reads `{expected}`; update this file's copy of {constant} \
             with it"
        );
    }
    let code: String = main
        .lines()
        .filter(|line| !line.trim_start().starts_with("//"))
        .collect::<Vec<_>>()
        .join("\n");
    assert_eq!(
        code.matches("PeerLimit::outside(&shared.trusted_proxies, MAX_CONNECTIONS_PER_PEER)")
            .count(),
        1,
        "the cap must be built once, from the shared mode's own trusted-proxy set and \
         MAX_CONNECTIONS_PER_PEER itself"
    );
    assert_eq!(
        code.matches("PeerLimit::outside(").count(),
        1,
        "src/main.rs builds a peer cap somewhere else too"
    );
    assert_eq!(
        code.matches(".admit(peer_ip)").count(),
        1,
        "every accepted connection is admitted once, by its socket peer"
    );
    assert!(
        PEER_CAP < 256,
        "a cap at or above the 256-connection ceiling caps nothing"
    );
}

/// **A client that reads one byte every twenty seconds is ended at the stall
/// deadline (FX-24c, correcting the FX-24b review's M1).**
///
/// M1's shape, as a row: one connection pipelines sixteen requests for the
/// 156 KiB asset through a 4 KiB receive buffer, then reads ONE byte every
/// twenty seconds. The review's probe reported it "still open at 100 s", but
/// that probe read a byte at a time out of the half a megabyte the kernel had
/// buffered, and could never reach the end-of-stream behind it: the server's
/// own log shows the connection ended at 35.1 s (`claude/fx-24c.result.md`).
/// The kernel stops waking a writer whose client takes almost nothing, so the
/// stall ends it like a client that stopped. This row drains the connection
/// after the bound instead: EOF or reset within two seconds, short of the
/// sixteen answers. A stall clock that restarted on a mere poll, or a
/// deadline far past thirty seconds, leaves it open and hands over every
/// answer once drained. A request on another connection is answered
/// meanwhile, so the wait is this connection's, not the server's.
#[test]
fn a_client_reading_one_byte_every_twenty_seconds_is_ended_at_the_stall_deadline() {
    const PIPELINED: usize = 16;
    let fixture = Fixture::new("slowrate");
    let (_server, port) = start_server(&fixture);
    let address: SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();
    let runtime = socket_runtime();
    let mut slow = connect_with_receive_buffer(&runtime, address, 4096).unwrap();
    slow.set_write_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    let request = format!("GET /ui/render.js HTTP/1.1\r\nHost: 127.0.0.1:{port}\r\n\r\n");
    slow.write_all(request.repeat(PIPELINED).as_bytes())
        .unwrap();
    let sent = Instant::now();

    let health = http_get(port, "/healthz", &format!("127.0.0.1:{port}"));
    assert!(health.starts_with("HTTP/1.1 200"), "{health}");

    // One byte every twenty seconds, up to the bound.
    let bound = STALL_DEADLINE + STALL_SLACK;
    let mut read = 0usize;
    let mut next = SLOW_READ_EVERY;
    while next < bound {
        std::thread::sleep(next.saturating_sub(sent.elapsed()));
        slow.set_read_timeout(Some(Duration::from_secs(2))).unwrap();
        let mut one = [0u8; 1];
        match slow.read(&mut one) {
            Ok(1) => read += 1,
            // Ended already: what is checked below is that it is ended.
            Ok(_) => break,
            Err(e) if e.kind() == ErrorKind::ConnectionReset => break,
            Err(e) => panic!(
                "the slow reader's one-byte read at {:?} failed: {e}",
                sent.elapsed()
            ),
        }
        next += SLOW_READ_EVERY;
    }
    assert!(
        read >= 1,
        "the slow reader never read its byte: the row measured a non-reader"
    );

    std::thread::sleep(bound.saturating_sub(sent.elapsed()));
    let whole = PIPELINED * render_js_size();
    match answered_then_closed(&mut slow, Duration::from_secs(2)) {
        Ok((_, received)) if received.len() + read < whole => {}
        Ok((_, received)) => panic!(
            "the connection that read one byte every {SLOW_READ_EVERY:?} was sent all {} bytes \
             of its answers once drained: the server never ended it",
            received.len() + read
        ),
        Err(why) => panic!(
            "the connection that read one byte every {SLOW_READ_EVERY:?} was not ended by the \
             server within {bound:?} of its requests: {why}"
        ),
    }
    let health = http_get(port, "/healthz", &format!("127.0.0.1:{port}"));
    assert!(health.starts_with("HTTP/1.1 200"), "{health}");
}

/// **Clients that read one byte every twenty seconds cannot hold the ceiling
/// past the stall deadline, and the request queued behind them is answered
/// (FX-24c).**
///
/// THE CI-SCALE ROW: 256 sockets. Under the flood rule (WORKER-RULES,
/// 2026-10-08) it does not run on a host serving a compose stack or the PoC;
/// CI's Linux `cargo test --workspace` runs it, and the FX-24c worker ran it
/// at a ceiling of eight (a temporary `MAX_CONNECTIONS` edit).
///
/// [`clients_that_stop_reading_cannot_hold_the_ceiling_past_the_stall_deadline`]
/// with one change: every client READS one byte every twenty seconds — the
/// ledger's slow-rate client. The bounds are that row's: held for a measured
/// window first; then answered no sooner than the deadline (less a second)
/// after the first client connected, and within the deadline plus
/// [`STALL_SLACK`] of the last; and by then every slow connection ended by the
/// server, short of its sixteen answers.
#[test]
fn slow_rate_clients_cannot_hold_the_ceiling_past_the_stall_deadline() {
    const CEILING: usize = 256;
    const PIPELINED: usize = 16;
    let fixture = Fixture::new("slowrateceiling");
    let (_server, port) = start_server(&fixture);
    let address: SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();
    let runtime = socket_runtime();
    let request = format!("GET /ui/render.js HTTP/1.1\r\nHost: 127.0.0.1:{port}\r\n\r\n");
    let requests = request.repeat(PIPELINED);
    let open = |i: usize| {
        let mut stream = connect_with_receive_buffer(&runtime, address, 4096)
            .unwrap_or_else(|e| panic!("slow connection {i}: {e}"));
        stream
            .set_write_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        stream
            .write_all(requests.as_bytes())
            .unwrap_or_else(|e| panic!("slow connection {i}: write: {e}"));
        stream
    };

    let mut held = Vec::with_capacity(CEILING);
    let mut slowest = Duration::ZERO;
    let first = Instant::now();
    while held.len() < CEILING - 1 {
        for _ in 0..(CEILING - 1 - held.len()).min(UNACCEPTED_AT_ONCE) {
            held.push(open(held.len()));
        }
        slowest = slowest.max(answered_after(port, held.len()));
    }
    held.push(open(CEILING - 1));
    let last = Instant::now();

    let mut queued = TcpStream::connect_timeout(&address, Duration::from_secs(5)).unwrap();
    queued
        .set_write_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    write!(
        queued,
        "GET /healthz HTTP/1.1\r\nHost: 127.0.0.1:{port}\r\nConnection: close\r\n\r\n"
    )
    .unwrap();

    // Held first.
    let window = (slowest * 4).max(Duration::from_secs(2));
    let budget = STALL_DEADLINE
        .saturating_sub(first.elapsed())
        .saturating_sub(Duration::from_secs(1));
    assert!(
        window <= budget,
        "inconclusive, not a product failure: filling the ceiling took {:?} and the slowest \
         probe {slowest:?}, which leaves {budget:?} before the first connection's stall \
         deadline for a {window:?} held window",
        first.elapsed()
    );
    queued.set_read_timeout(Some(window)).unwrap();
    let mut buffer = [0u8; 64];
    match queued.read(&mut buffer) {
        Err(e) if matches!(e.kind(), ErrorKind::WouldBlock | ErrorKind::TimedOut) => {}
        other => panic!(
            "the queued request was not held behind {CEILING} slow connections for {window:?}: \
             {other:?} {:?}",
            String::from_utf8_lossy(&buffer)
        ),
    }

    // Then released by the stall deadline, while every slow client keeps
    // reading its byte every twenty seconds.
    let limit = STALL_DEADLINE + STALL_SLACK;
    let mut next_round = SLOW_READ_EVERY;
    let mut response = Vec::new();
    let answered_at = loop {
        assert!(
            last.elapsed() < limit,
            "the request queued behind {CEILING} connections that read a byte every \
             {SLOW_READ_EVERY:?} was not answered within {limit:?} of the last one ({} bytes \
             received)",
            response.len()
        );
        if first.elapsed() >= next_round {
            for stream in &mut held {
                stream
                    .set_read_timeout(Some(Duration::from_millis(20)))
                    .unwrap();
                let mut one = [0u8; 1];
                let _ = stream.read(&mut one);
            }
            next_round += SLOW_READ_EVERY;
        }
        queued
            .set_read_timeout(Some(Duration::from_millis(250)))
            .unwrap();
        match queued.read(&mut buffer) {
            Ok(0) => break first.elapsed(),
            Ok(n) => response.extend_from_slice(&buffer[..n]),
            Err(e) if matches!(e.kind(), ErrorKind::WouldBlock | ErrorKind::TimedOut) => {}
            Err(e) => panic!("the queued request failed: {e}"),
        }
    };
    let text = String::from_utf8_lossy(&response);
    assert!(
        text.starts_with("HTTP/1.1 200"),
        "the queued request was answered with: {text:?}"
    );
    assert!(
        answered_at >= STALL_DEADLINE.saturating_sub(Duration::from_secs(1)),
        "the queued request was answered {answered_at:?} after the first slow client \
         connected, before the {STALL_DEADLINE:?} stall deadline: a permit came back some \
         other way"
    );

    std::thread::sleep(limit.saturating_sub(last.elapsed()));
    let whole = PIPELINED * render_js_size();
    for (i, stream) in held.iter_mut().enumerate() {
        match answered_then_closed(stream, Duration::from_secs(2)) {
            Ok((_, received)) if received.len() < whole => {}
            Ok((_, received)) => panic!(
                "slow connection {i} of {CEILING} was sent all {} bytes of its answers once \
                 drained: the server never ended it",
                received.len()
            ),
            Err(why) => panic!(
                "slow connection {i} of {CEILING} was not ended by the server within {limit:?} \
                 of the last connect: {why}"
            ),
        }
    }
    let health = http_get(port, "/healthz", &format!("127.0.0.1:{port}"));
    assert!(health.starts_with("HTTP/1.1 200"), "{health}");
}

/// **A signed-in body that arrives below the rate floor is answered and
/// closed at the window (FX-24c).**
///
/// The localAdmin actor announces 4096 bytes and sends ONE every twenty
/// seconds. Before FX-24c each byte restarted the body's stall clock, so only
/// the sixty-second total ended it. Now its window wants [`RATE_FLOOR`] bytes:
/// it is answered `400 malformed_request`, "arrived too slowly", and closed at
/// thirty seconds, not sixty. A body floor of one byte fails here.
#[test]
fn a_signed_in_body_below_the_rate_floor_is_closed_at_the_window() {
    let fixture = Fixture::new("bodyslow");
    let (_server, port) = start_server(&fixture);
    let address: SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();

    let mut slow = TcpStream::connect_timeout(&address, Duration::from_secs(5)).unwrap();
    slow.set_write_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    slow.set_read_timeout(Some(Duration::from_millis(250)))
        .unwrap();
    slow.write_all(backup_post_head(port, 4096, "fx24c-slowbody-0001").as_bytes())
        .unwrap();
    let sent = Instant::now();
    let limit = STALL_DEADLINE + DEADLINE_SLACK;
    let mut next_drip = SLOW_READ_EVERY;
    let mut received = Vec::new();
    let mut buffer = [0u8; 4096];
    let mut dripped = 0;
    let closed_at = loop {
        assert!(
            sent.elapsed() < limit,
            "a body arriving at one byte every {SLOW_READ_EVERY:?} was not answered and closed \
             within {limit:?} ({dripped} bytes sent, {} received: {:?})",
            received.len(),
            String::from_utf8_lossy(&received)
        );
        if received.is_empty() && sent.elapsed() >= next_drip {
            slow.write_all(b" ")
                .unwrap_or_else(|e| panic!("drip at {:?}: {e}", sent.elapsed()));
            dripped += 1;
            next_drip += SLOW_READ_EVERY;
        }
        match slow.read(&mut buffer) {
            Ok(0) => break sent.elapsed(),
            Ok(n) => received.extend_from_slice(&buffer[..n]),
            Err(e) if matches!(e.kind(), ErrorKind::WouldBlock | ErrorKind::TimedOut) => {}
            Err(e) if e.kind() == ErrorKind::ConnectionReset => break sent.elapsed(),
            Err(e) => panic!(
                "the slow body's connection failed after {:?}: {e}",
                sent.elapsed()
            ),
        }
    };
    let response = String::from_utf8_lossy(&received);
    assert!(
        response.starts_with("HTTP/1.1 400")
            && response.contains("\"code\":\"malformed_request\"")
            && response.contains("arrived too slowly"),
        "a body below the floor was answered with: {response}"
    );
    assert!(
        closed_at >= STALL_DEADLINE.saturating_sub(Duration::from_secs(1)),
        "a body below the floor was ended after {closed_at:?}, before its {STALL_DEADLINE:?} \
         window"
    );
    assert_eq!(
        dripped, 1,
        "the body's byte at {SLOW_READ_EVERY:?} was not sent: the row measured a stopped body"
    );
}

// ------------------------------------------------- the per-peer cap (shared)

/// Start the binary in SHARED mode, with `extra` appended to its
/// configuration, and return it with the port it said it is listening on.
/// Shared mode reaches nothing at start: the identity provider is read on
/// the first sign-in or readiness probe, and the cluster at
/// `https://127.0.0.1:1` never.
fn start_shared_server(fixture: &Fixture, extra: &str) -> (Watched, u16) {
    write_shared_material(fixture, 1);
    let mut lost = Vec::new();
    for _ in 0..START_ATTEMPTS {
        let port = free_port();
        let config = fixture.config(&format!(
            "{}{extra}",
            shared_config_text(
                fixture,
                &format!("127.0.0.1:{port}"),
                "https://console.example"
            )
        ));
        let mut server = Watched::spawn(&config);
        match wait_until_listening(&mut server, port) {
            Start::Listening => return (server, port),
            Start::PortTaken => {
                drop(server);
                lost.push(port);
            }
        }
    }
    panic!("the shared-mode server lost the port it was given {START_ATTEMPTS} times ({lost:?})");
}

/// The warning the server logs when it closes a connection over its peer's
/// cap (`transport::PeerLimit`).
const CAP_REFUSAL: &str = "closed a connection at once";

/// `GET /healthz` on fresh connections until one is answered, or `deadline`.
fn answered_before(port: u16, deadline: Instant) -> Result<String, String> {
    let address: SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();
    let mut last = String::from("never tried");
    while Instant::now() < deadline {
        match one_http_get(&address, "/healthz", &format!("127.0.0.1:{port}")) {
            Ok(answer) => return Ok(answer),
            Err(error) => last = error,
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    Err(last)
}

/// **A peer outside the trusted-proxy set holds at most its share, and gets
/// a place back when one of its connections ends (FX-24c).**
///
/// Shared mode, with TEST-NET-1 as the trusted proxy, so loopback — where
/// this test connects from — is outside the set and capped at [`PEER_CAP`].
/// - **Every place held.** [`PEER_CAP`] − 1 connections that send nothing,
///   then a keep-alive request answered and held open: its answer proves every
///   connection opened before it was accepted (the kernel hands them over in
///   order), so all [`PEER_CAP`] places are taken.
/// - **One more is refused.** A further connection from the same peer is
///   closed by the server within two seconds, unanswered — long before the
///   held connections' own ten-second header deadline — and the server says
///   why in its log. A server with no cap answers it.
/// - **A place given back is a place.** One held connection is dropped, and
///   a request from the peer is answered before any held connection's own
///   deadline could have freed one. A cap whose places never come back fails
///   there.
#[test]
fn a_peer_outside_the_trusted_set_is_capped_at_its_share() {
    let fixture = Fixture::new("peercap");
    let (mut server, port) =
        start_shared_server(&fixture, "trustedProxyCidrs: [\"192.0.2.0/24\"]\n");
    let address: SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();
    let connect = |i: usize| {
        TcpStream::connect_timeout(&address, Duration::from_secs(5))
            .unwrap_or_else(|e| panic!("connection {i}: {e}"))
    };

    let first = Instant::now();
    let mut silent: Vec<TcpStream> = (0..PEER_CAP - 1).map(connect).collect();
    let answered = hold_an_answered_keep_alive(&mut server, port);

    let mut over = connect(PEER_CAP);
    over.set_write_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    // The write may meet a connection the server has already closed.
    let _ = write!(
        over,
        "GET /healthz HTTP/1.1\r\nHost: 127.0.0.1:{port}\r\nConnection: close\r\n\r\n"
    );
    let refused = closed_by_the_server(&mut over, Duration::from_secs(2));
    assert!(
        refused.is_ok(),
        "connection {} from one peer outside the trusted set, past its cap of {PEER_CAP}, was \
         not closed at once: {refused:?}\n{}",
        PEER_CAP + 1,
        server.log()
    );
    let budget = HEADER_DEADLINE
        .saturating_sub(first.elapsed())
        .saturating_sub(RELEASE_MARGIN);
    assert!(
        !budget.is_zero(),
        "inconclusive, not a product failure: holding the cap took {:?}, past the held \
         connections' header deadline",
        first.elapsed()
    );
    for (i, held) in silent.iter_mut().enumerate() {
        held.set_nonblocking(true).unwrap();
        let mut byte = [0u8; 1];
        match held.read(&mut byte) {
            Err(e) if e.kind() == ErrorKind::WouldBlock => {}
            other => panic!(
                "held connection {i} was ended before its header deadline ({other:?}): the cap \
                 refused a connection inside it"
            ),
        }
        held.set_nonblocking(false).unwrap();
    }

    drop(silent.remove(0));
    let deadline = first + HEADER_DEADLINE.saturating_sub(RELEASE_MARGIN);
    let answer = answered_before(port, deadline).unwrap_or_else(|last| {
        panic!(
            "the peer got no place back from a connection it dropped, before its other \
             connections' own header deadline ({last})\n{}",
            server.log()
        )
    });
    assert!(answer.starts_with("HTTP/1.1 200"), "{answer}");
    assert!(
        server.log().contains(CAP_REFUSAL),
        "the server closed a connection over the cap without saying why:\n{}",
        server.log()
    );
    drop(answered);
}

/// [`PEER_CAP`] + 8 connections from loopback that send nothing, opened in
/// two batches each proven accepted by a probe answered before the first
/// one's header deadline; then every one of them must still be open, and the
/// server must not have refused any. Used where loopback must NOT be capped.
fn held_past_the_cap(server: &Watched, port: u16, who: &str) {
    const PAST: usize = PEER_CAP + 8;
    let address: SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();
    let first = Instant::now();
    let deadline = first + HEADER_DEADLINE.saturating_sub(RELEASE_MARGIN);
    let mut held = Vec::with_capacity(PAST);
    for batch in [PAST / 2, PAST - PAST / 2] {
        for _ in 0..batch {
            let i = held.len();
            held.push(
                TcpStream::connect_timeout(&address, Duration::from_secs(5))
                    .unwrap_or_else(|e| panic!("connection {i}: {e}")),
            );
        }
        let answer = answered_before(port, deadline).unwrap_or_else(|last| {
            panic!(
                "{who} holding {} connections got no answer to one more before the header \
                 deadline ({last}): it was capped as one peer\n{}",
                held.len(),
                server.log()
            )
        });
        assert!(answer.starts_with("HTTP/1.1 200"), "{answer}");
    }
    assert!(
        Instant::now() < deadline,
        "inconclusive, not a product failure: opening {PAST} connections took {:?}",
        first.elapsed()
    );
    for (i, stream) in held.iter_mut().enumerate() {
        stream.set_nonblocking(true).unwrap();
        let mut byte = [0u8; 1];
        match stream.read(&mut byte) {
            Err(e) if e.kind() == ErrorKind::WouldBlock => {}
            other => panic!(
                "connection {i} of {PAST} from {who} was ended at once ({other:?}): it was capped \
                 as one peer at {PEER_CAP}\n{}",
                server.log()
            ),
        }
    }
    assert!(
        !server.log().contains(CAP_REFUSAL),
        "the server refused a connection from {who}:\n{}",
        server.log()
    );
}

/// **The trusted proxy is never capped as one peer (FX-24c).** Every browser
/// behind the ingress arrives from the ingress's address, so capping it would
/// cap the console's whole audience at [`PEER_CAP`] connections. Loopback is
/// the trusted proxy here, and holds [`PEER_CAP`] + 8 connections.
#[test]
fn the_trusted_proxy_is_never_capped_as_one_peer() {
    let fixture = Fixture::new("peercaptrusted");
    let (server, port) = start_shared_server(&fixture, "trustedProxyCidrs: [\"127.0.0.1/32\"]\n");
    held_past_the_cap(&server, port, "the trusted proxy");
}

/// **With no trusted-proxy set, no peer is capped (FX-24c).** The console
/// cannot then tell its ingress from any other peer, and the chart's default
/// names none: a cap here would cap an unconfigured install's ingress as one
/// peer.
#[test]
fn without_a_trusted_proxy_set_no_peer_is_capped() {
    let fixture = Fixture::new("peercapnone");
    let (server, port) = start_shared_server(&fixture, "");
    held_past_the_cap(&server, port, "a peer of a console with no trusted proxy");
}

/// **One address outside the trusted set cannot hold the ceiling, however it
/// reads (FX-24c).**
///
/// THE CI-SCALE CAP ROW: 256 sockets, so the flood rule applies as above.
///
/// 256 clients from loopback, outside the trusted set, each pipelining
/// sixteen requests for the 156 KiB asset and reading nothing — the clients a
/// stall ends only after thirty seconds, and that no window can end at all if
/// they read just fast enough to keep the kernel moving. The cap holds them to
/// [`PEER_CAP`]: the server closes every one past it within two seconds of
/// its connect, unanswered, and keeps [`PEER_CAP`] open. Where the host can
/// dial from a second loopback address (Linux routes all of `127.0.0.0/8`;
/// macOS configures only `127.0.0.1`), a request from `127.0.0.2` is then
/// answered at once — not after the stall deadline — because the other 224
/// permits are free; elsewhere the row says it skipped that half.
#[test]
fn one_address_outside_the_trusted_set_cannot_hold_the_ceiling() {
    const CLIENTS: usize = 256;
    const PIPELINED: usize = 16;
    let fixture = Fixture::new("peercapceiling");
    let (server, port) = start_shared_server(&fixture, "trustedProxyCidrs: [\"192.0.2.0/24\"]\n");
    let address: SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();
    let runtime = socket_runtime();
    let requests =
        format!("GET /ui/render.js HTTP/1.1\r\nHost: 127.0.0.1:{port}\r\n\r\n").repeat(PIPELINED);

    // Opened in batches, so the listen queue never holds more than one.
    let first = Instant::now();
    let mut clients = Vec::with_capacity(CLIENTS);
    while clients.len() < CLIENTS {
        for _ in 0..(CLIENTS - clients.len()).min(UNACCEPTED_AT_ONCE) {
            let i = clients.len();
            let mut stream = connect_with_receive_buffer(&runtime, address, 4096)
                .unwrap_or_else(|e| panic!("client {i}: {e}"));
            stream
                .set_write_timeout(Some(Duration::from_secs(5)))
                .unwrap();
            // A client the server has already closed may refuse the write.
            let _ = stream.write_all(requests.as_bytes());
            clients.push(stream);
        }
        std::thread::sleep(Duration::from_millis(100));
    }

    // Every client past the cap is closed, unanswered; the cap's share is not.
    let mut open = 0;
    let mut refused = 0;
    for (i, stream) in clients.iter_mut().enumerate() {
        stream
            .set_read_timeout(Some(Duration::from_secs(2)))
            .unwrap();
        let mut byte = [0u8; 1];
        match stream.read(&mut byte) {
            Ok(1) => open += 1,
            Ok(_) => refused += 1,
            Err(e) if e.kind() == ErrorKind::ConnectionReset => refused += 1,
            Err(e) => panic!("client {i}: neither answered nor closed within 2 s: {e}"),
        }
    }
    assert!(
        first.elapsed() < STALL_DEADLINE,
        "inconclusive, not a product failure: counting took {:?}, past the stall deadline \
         that ends the held clients on its own",
        first.elapsed()
    );
    assert_eq!(
        (open, refused),
        (PEER_CAP, CLIENTS - PEER_CAP),
        "one address outside the trusted set held {open} connections and had {refused} \
         closed; its cap is {PEER_CAP}\n{}",
        server.log()
    );

    // Another address is served at once, from the permits the cap left free.
    let other = runtime.block_on(async {
        let socket = tokio::net::TcpSocket::new_v4().ok()?;
        socket.bind("127.0.0.2:0".parse().unwrap()).ok()?;
        let stream = tokio::time::timeout(Duration::from_secs(5), socket.connect(address))
            .await
            .ok()?
            .ok()?;
        let stream = stream.into_std().ok()?;
        stream.set_nonblocking(false).ok()?;
        Some(stream)
    });
    match other {
        Some(mut stream) => {
            let asked = Instant::now();
            stream
                .set_write_timeout(Some(Duration::from_secs(5)))
                .unwrap();
            write!(
                stream,
                "GET /healthz HTTP/1.1\r\nHost: 127.0.0.1:{port}\r\nConnection: close\r\n\r\n"
            )
            .unwrap();
            stream
                .set_read_timeout(Some(Duration::from_secs(5)))
                .unwrap();
            let mut answer = Vec::new();
            let read = stream.read_to_end(&mut answer);
            let text = String::from_utf8_lossy(&answer);
            assert!(
                read.is_ok() && text.starts_with("HTTP/1.1 200"),
                "a request from 127.0.0.2, beside {CLIENTS} clients of 127.0.0.1, was not \
                 answered within 5 s: {read:?} {text:?}"
            );
            assert!(
                asked.elapsed() < Duration::from_secs(5),
                "answered after {:?}",
                asked.elapsed()
            );
        }
        None => eprintln!(
            "one_address_outside_the_trusted_set_cannot_hold_the_ceiling: this host cannot dial \
             from 127.0.0.2, so the second-address half was skipped (it runs on Linux)"
        ),
    }
}
