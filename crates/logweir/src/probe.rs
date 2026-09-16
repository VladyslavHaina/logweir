//! `logweir cluster-probe` — interface **I14**: **exactly two stdout lines**,
//! `cluster-id=<id>` then `reachable=true|false`, and exit **0** when a broker
//! answered, **1** when none did.
//!
//! # Why `logweir doctor` cannot be this, four ways
//!
//! Each of the four is provable against the shipped `doctor`, and each is
//! answered by the shape of this module:
//!
//! 1. **`doctor` has two more MANDATORY flags.** `cli::Command::Doctor`
//!    declares `--allowed-clusters` and `--approver-key` as non-optional
//!    `PathBuf`s, so `logweir doctor --spec … --strict` is a clap usage error.
//!    This subcommand's only required flag is `--bootstrap`.
//! 2. **`doctor` cannot be told which auth to use.** Its dial is a fixed
//!    plaintext one at one call site. This module builds its auth from its own
//!    flags plus `LOGWEIR_SOURCE_PASSWORD`, through the ONE construction site
//!    [`AuthConfig::from_spec`] (interface **I1**).
//! 3. **`doctor` enforces drill preconditions a `KafkaCluster` need not
//!    satisfy** — the target's cluster id has to be in a cluster allowlist and
//!    a healthy marker topic has to exist. A `role: source` cluster has
//!    neither, so **every source probe would report unreachable**. This module
//!    reads no cluster allowlist, opens no approver public key, and asserts no
//!    marker topic: those are drill-time guards belonging to phase 0, not to a
//!    liveness probe.
//! 4. **`doctor` prints no machine-readable cluster-id line.** The observed id
//!    reaches its output only inside prose (a failure string, and a "cluster
//!    {id}, marker topic present" summary reached only after 3's two refusals
//!    have both passed). A controller cannot parse either. [`CLUSTER_ID_LINE`]
//!    is a key=value contract.
//!
//! # The shape: a pure verdict, an impure wrapper
//!
//! [`outcome`] is a pure function of a `Result<String, KafkaError>` — the
//! formatting AND the exit mapping — so both arms of the contract are testable
//! with no socket at all. [`probe`] is the seam a `ClusterReader` double
//! drives: one `cluster_id()` call and nothing else. [`run`] is the ONLY
//! function here that constructs a client, which is why this file is on
//! `tests/no_network_in_unit_tests.rs`'s allow-list (STANDING RULE 18): the
//! probe dials BY DESIGN — it is the whole subcommand.
//!
//! # Nothing but the two lines reaches stdout
//!
//! The diagnostic goes to **stderr**, and so does this subcommand's tracing
//! subscriber ([`install_diagnostics`]), because the two stdout lines are the
//! whole machine contract. A pod log merges the two streams in nondeterministic
//! order (plan erratum **E4**), so a controller reads the two lines BY KEY NAME
//! from a bounded tail and never by position — and the two contract lines are
//! the last thing this process writes.

use std::io::Write;
use std::sync::mpsc;
use std::time::Duration;

use logweir_kafka::reader::{AuthConfig, ClusterReader, KafkaError};

use crate::exit::ExitCode;

/// Interface **I14**'s FIRST stdout line, as a prefix. The value is the id the
/// broker returned, or **empty** when none did.
pub const CLUSTER_ID_LINE: &str = "cluster-id=";

/// Interface **I14**'s SECOND stdout line, as a prefix. The value is exactly
/// `true` or exactly `false`.
pub const REACHABLE_LINE: &str = "reachable=";

/// `--auth-mode plaintext`.
pub const AUTH_MODE_PLAINTEXT: &str = "plaintext";

/// `--auth-mode scramSha512`. **The camelCase spelling is the contract**: it is
/// byte-identical to `logweir_core::spec::AuthSpec`'s serde spelling and to the
/// `KafkaCluster` CRD's `auth.mode` enum, so a reconciler can copy the value it
/// read straight onto this argv.
pub const AUTH_MODE_SCRAM_SHA_512: &str = "scramSha512";

/// The environment variable the SASL password is read from, and the only place
/// it is read from.
///
/// **THERE IS NO FLAG FOR IT, AT ANY COMMAND.** A secret on an argv is visible
/// in `/proc/<pid>/cmdline`, in a shell history and in every process listing on
/// the host — and it would land in the Job spec a controller creates, where
/// anyone with pod read can see it.
pub const SOURCE_PASSWORD_ENV: &str = "LOGWEIR_SOURCE_PASSWORD";

/// The environment variable naming a projected private CA file (PLAT-07.1).
///
/// A probe reads ONE cluster, so — exactly as for the password — it reads the
/// source side's variable whatever the object's `spec.role` says. The path
/// goes to librdkafka's `ssl.ca.location`; this process does not open it.
pub const SOURCE_TLS_CA_FILE_ENV: &str = crate::tls_ca::SOURCE_TLS_CA_FILE_VAR;

/// How long the dial is given before it is reported as unreachable.
///
/// **TEN SECONDS, AND THE BOUND IS THIS MODULE'S OWN.** The brief names no
/// timeout, so this is the dispatch's default, recorded here rather than left
/// implicit. It is deliberately SHORTER than the 20-second metadata budget
/// `logweir-kafka`'s rdkafka reader gives `fetch_cluster_id`: that call returns
/// NULL only after its whole allotted timespan, and a SASL handshake that the
/// broker rejects makes librdkafka RETRY rather than return, so without a bound
/// of its own a wrong password would present as a 20-second stall and a wrong
/// port as another one. A probe that hangs is worse than a probe that says
/// `false`: interface **I14** has an answer for unreachable and no answer for
/// "still thinking".
pub const DIAL_TIMEOUT: Duration = Duration::from_secs(10);

/// The parsed `cluster-probe` command line.
///
/// `bootstrap` is the raw comma-separated value of `--bootstrap`, split by
/// [`bootstrap_servers`]: the reconciler joins `spec.bootstrapServers` with
/// commas into ONE argv element, so the split belongs here and not at the
/// caller.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProbeArgs {
    /// `--bootstrap <csv>`.
    pub bootstrap: String,
    /// `--auth-mode plaintext|scramSha512`.
    pub auth_mode: String,
    /// `--username <u>`. Required in practice at `scramSha512` and ignored
    /// otherwise.
    pub username: Option<String>,
    /// `--tls`. Whether the transport is TLS — INDEPENDENT of the mode, exactly
    /// as `KafkaCluster.spec.auth.tls` is: SASL/SCRAM over PLAINTEXT and
    /// SASL/SCRAM over SSL are two `security.protocol` values for one mechanism
    /// (Global Constraint 29).
    pub tls: bool,
    /// `--marker-topic <t>`. **ACCEPTED AND NEVER ASSERTED** — see
    /// [`probe`].
    pub marker_topic: Option<String>,
}

/// What one probe decided: the two stdout lines, the stderr diagnostic, and the
/// exit code.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProbeOutcome {
    /// The COMPLETE stdout, both lines and their newlines. Nothing else is ever
    /// written to stdout by this subcommand.
    pub stdout: String,
    /// The one line written to stderr, when there is something to say. `None`
    /// on the reachable arm: a probe that worked has no diagnostic.
    pub diagnostic: Option<String>,
    /// `0` when a broker answered, `1` when none did — Global Constraint 11's
    /// `Ok` and `Operational`.
    pub code: ExitCode,
}

/// The two lines and the exit code, as a **pure function of the observation**.
///
/// This is the whole of interface **I14**'s contract, and it dials nothing:
/// `Ok(id)` is `cluster-id=<id>` / `reachable=true` / exit **0**, and any `Err`
/// is `cluster-id=` (EMPTY) / `reachable=false` / exit **1**, with the error's
/// own text as the diagnostic.
///
/// # The order is asserted, not incidental
///
/// `cluster-id=` FIRST, `reachable=` SECOND. A controller reads both by key
/// name (erratum **E4**), so the order is not what makes the parse work — it is
/// what makes the CONTRACT one thing rather than two spellings of it, and
/// `cluster_probe_prints_the_cluster_id_and_exits_zero` compares the whole
/// string rather than two `contains` calls for exactly that reason.
///
/// # The empty id on the failure arm is deliberate
///
/// Not "unknown", not the string `none`, not the flag's own value: an EMPTY
/// value, so a reader that splits on `=` gets an empty string and a reader
/// that requires a non-empty id sees the absence. Nothing is guessed, and no
/// id from a previous run can leak into a failed probe's output.
#[must_use]
pub fn outcome(observed: &Result<String, KafkaError>) -> ProbeOutcome {
    match observed {
        Ok(id) => ProbeOutcome {
            stdout: format!("{CLUSTER_ID_LINE}{id}\n{REACHABLE_LINE}true\n"),
            diagnostic: None,
            code: ExitCode::Ok,
        },
        Err(e) => ProbeOutcome {
            stdout: format!("{CLUSTER_ID_LINE}\n{REACHABLE_LINE}false\n"),
            diagnostic: Some(format!("cluster-probe: no cluster id was read: {e}")),
            code: ExitCode::Operational,
        },
    }
}

/// One `cluster_id()` read, turned into an outcome — the seam a
/// `ClusterReader` double drives.
///
/// # `marker_topic` is accepted and NEVER asserted
///
/// It is taken so the caller need not branch: the reconciler passes
/// `spec.markerTopic` through unconditionally, and a probe that changed its
/// answer because of it would report `reachable: false` for every `role:
/// source` cluster in the fleet — a source cluster has no marker topic, and
/// none is required of it. The marker-topic check is a drill-time guard: phase
/// 0 refuses a `mode: scratch` restore whose target cannot prove it is
/// scratch, which is a different question asked at a different time about a
/// different object. `the_probe_never_refuses_on_a_marker_topic` asserts the
/// output and the exit code are byte-identical with the flag and without it,
/// over a reader whose topic list is EMPTY.
///
/// **One call, and it is `cluster_id()`.** No `list_topics`, no `end_offsets`,
/// no `topic_configs`: the probe's question is "did a broker answer", and
/// every extra call is another way for a healthy cluster to fail a liveness
/// check.
#[must_use]
pub fn probe(reader: &dyn ClusterReader, marker_topic: Option<&str>) -> ProbeOutcome {
    // Named `_marker_topic` rather than dropped from the signature: the
    // parameter is part of the contract above, and a signature that did not
    // take it would let the reconciler's unconditional pass-through look like
    // a caller error.
    let _marker_topic = marker_topic;
    outcome(&reader.cluster_id())
}

/// Write an outcome to its two streams.
///
/// The writer seam exists so a test can assert the EXACT BYTES on each stream —
/// `the_probe_prints_exactly_two_stdout_lines` counts the newlines on one and
/// finds the diagnostic on the other — instead of trusting a `println!` nobody
/// can observe. It is the same arrangement `exit::print_refusal_reason_to` uses
/// and for the same reason.
///
/// # Errors
///
/// Whatever the writers return. [`run`] discards it: a closed stdout is not a
/// reason to change an exit code interface **I14** has already decided.
pub fn write_outcome<O: Write, E: Write>(
    out: &mut O,
    err: &mut E,
    o: &ProbeOutcome,
) -> std::io::Result<()> {
    if let Some(d) = o.diagnostic.as_deref() {
        // THE DIAGNOSTIC FIRST, so the two contract lines are the LAST thing
        // this process writes. A pod log merges the streams (erratum E4) and a
        // controller scans a bounded tail; putting the diagnostic ahead of the
        // contract keeps both lines inside that tail however long the
        // diagnostic is.
        writeln!(err, "{d}")?;
        err.flush()?;
    }
    out.write_all(o.stdout.as_bytes())?;
    out.flush()
}

/// `--bootstrap <csv>` split into addresses.
///
/// Comma-separated, each entry trimmed, empties dropped. ONE argv element
/// rather than a repeated flag because that is what interface **I14** spells,
/// and because a reconciler that joins `spec.bootstrapServers` with commas
/// writes one string it can compare against the object it read.
#[must_use]
pub fn bootstrap_servers(csv: &str) -> Vec<String> {
    csv.split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string)
        .collect()
}

/// The `AuthSpec` this command line asks for, or the reason it asks for
/// nothing usable.
///
/// **IT RETURNS A SPEC AND NOT AN `AuthConfig`.** Interface **I1** says
/// `AuthConfig::from_spec` is the ONE construction site for a client's auth,
/// and this function's whole job is to reach it: the flags become the shape an
/// adopter writes, and the mapping onto the client's own enum stays where every
/// other call site finds it. A `probe.rs` that built `AuthConfig::ScramSha512
/// { … }` itself would be a second construction site with a second chance to
/// pin the plaintext arm — which is the defect interface I1 exists to prevent,
/// and which `probe_builds_its_auth_through_the_one_construction_site` asserts
/// over this file's source.
///
/// # Errors
///
/// A mode this build does not know (anything but the two constants above), or
/// `scramSha512` with no `--username`. Both are reported as an unreachable
/// probe by [`run`] rather than as a usage error, so the two contract lines
/// exist for every command line clap accepted.
pub fn auth_spec(
    mode: &str,
    username: Option<&str>,
    tls: bool,
) -> Result<logweir_core::spec::AuthSpec, KafkaError> {
    match mode {
        // TLS WITHOUT SASL IS REFUSED, NOT DOWNGRADED (PLAT-07.1). This arm used
        // to ignore `--tls` and dial PLAINTEXT, so an object that said TLS was
        // probed in the clear. `AuthSpec::Plaintext` has no TLS field for a
        // restore plan to carry either, so the controller refuses the same
        // shape before any Job exists; this is the runner's half.
        AUTH_MODE_PLAINTEXT if tls => Err(KafkaError::Client(format!(
            "--auth-mode {AUTH_MODE_PLAINTEXT} with --tls (TLS without SASL) is not supported; it \
             is refused rather than dialled without TLS. Use --auth-mode \
             {AUTH_MODE_SCRAM_SHA_512} over TLS, or drop --tls for a plaintext listener"
        ))),
        AUTH_MODE_PLAINTEXT => Ok(logweir_core::spec::AuthSpec::Plaintext),
        AUTH_MODE_SCRAM_SHA_512 => match username {
            Some(u) if !u.trim().is_empty() => Ok(logweir_core::spec::AuthSpec::ScramSha512 {
                username: u.to_string(),
                tls,
            }),
            _ => Err(KafkaError::Client(format!(
                "--auth-mode {AUTH_MODE_SCRAM_SHA_512} needs --username: SASL/SCRAM authenticates \
                 as a named principal, and a probe that dialled as nobody would report a \
                 configuration mistake as an unreachable broker"
            ))),
        },
        other => Err(KafkaError::Client(format!(
            "--auth-mode {other} is not a mode this build knows; it accepts \
             {AUTH_MODE_PLAINTEXT} and {AUTH_MODE_SCRAM_SHA_512}"
        ))),
    }
}

/// Install this subcommand's diagnostics — **on stderr, and nowhere else**.
///
/// WHY A SUBSCRIBER AT ALL. A rejected SASL handshake is reported by
/// librdkafka's own client log and by nothing else: `cluster_id()` sees only
/// that no id arrived, so without this the one honest sentence about a wrong
/// password ("authentication failed") would be dropped on the floor and the
/// operator would be left with a `reachable=false` that could mean anything.
///
/// WHY STDERR. Interface **I14**'s two stdout lines are the whole machine
/// contract; a log line on stdout would be a third line, which
/// `the_probe_prints_exactly_two_stdout_lines` forbids.
///
/// WHY `warn` BY DEFAULT. At `info` the rdkafka client emits routine broker
/// bookkeeping, and this process's useful output is two lines long. `RUST_LOG`
/// still wins whenever it is set to anything non-blank, and a BLANK value is
/// treated as unset — an empty `value:` on a Kubernetes env entry is a real
/// shape, and `EnvFilter::new("")` parses to the empty directive set without
/// erroring, so an `unwrap_or_else` fallback would never fire.
///
/// `try_init`, not `init`: a global subscriber already installed by an embedder
/// is not a reason to abort a probe.
pub fn install_diagnostics() {
    let filter = match std::env::var("RUST_LOG") {
        Ok(v) if !v.trim().is_empty() => tracing_subscriber::EnvFilter::new(v),
        _ => tracing_subscriber::EnvFilter::new("warn"),
    };
    let _ = tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_env_filter(filter)
        .try_init();
}

/// Dial, read the cluster id, print the two lines, exit 0 or 1.
///
/// **THE ONLY FUNCTION IN THIS FILE THAT CONSTRUCTS A CLIENT.** Everything
/// above it is pure or takes a `&dyn ClusterReader`, which is what keeps the
/// whole contract inside the default `cargo test` suite with no socket
/// (Global Constraint 22) while the shipped subcommand really dials.
///
/// # The dial is bounded, and the bound is not the reader's
///
/// The read runs on its own thread and the answer is collected with a
/// [`DIAL_TIMEOUT`] deadline; a deadline that expires IS an unreachable probe
/// and is reported as one. Returning from `main` ends the process, so a thread
/// still waiting on librdkafka cannot keep the runner alive past its answer.
#[must_use]
pub fn run(args: &ProbeArgs) -> ExitCode {
    install_diagnostics();

    let o = dial(args);
    let _ = write_outcome(
        &mut std::io::stdout().lock(),
        &mut std::io::stderr().lock(),
        &o,
    );
    o.code
}

/// The bounded dial: build the auth through interface **I1**, connect, and hand
/// the live reader to [`probe`].
///
/// **IT RETURNS A [`ProbeOutcome`] AND NOT A `Result`, SO THERE IS EXACTLY ONE
/// VERDICT PATH.** The shipped subcommand reaches its two lines through the same
/// [`probe`] the tests drive over a double — including the `marker_topic`
/// pass-through — rather than through a second, parallel arm that a test could
/// never see. Everything a live dial can go wrong at (an unknown mode, a
/// missing username, a missing password, a refused connect, a deadline) becomes
/// an `Err` handed to the same [`outcome`].
fn dial(args: &ProbeArgs) -> ProbeOutcome {
    let servers = bootstrap_servers(&args.bootstrap);
    if servers.is_empty() {
        return outcome(&Err(KafkaError::Client(
            "--bootstrap named no address; it takes a comma-separated list of host:port".into(),
        )));
    }
    let spec = match auth_spec(&args.auth_mode, args.username.as_deref(), args.tls) {
        Ok(s) => s,
        Err(e) => return outcome(&Err(e)),
    };
    // The password is read from the environment HERE and handed to the one
    // construction site as a value, so nothing below this line has to know
    // which variable it came from. An EMPTY value is treated as unset: a
    // projected Secret key that exists and is blank is not a credential, and
    // `from_spec`'s named "no SASL password was projected" error is the honest
    // report of it.
    let password = std::env::var(SOURCE_PASSWORD_ENV)
        .ok()
        .filter(|p| !p.is_empty());
    // The projected CA, if the connection names one, for librdkafka's
    // `ssl.ca.location` (PLAT-07.1). `with_tls_ca_file` refuses a CA for a
    // transport that is not TLS, so a CA can never sit beside a clear dial.
    let ca_file = match crate::tls_ca::projected_ca_file(SOURCE_TLS_CA_FILE_ENV) {
        Ok(c) => c,
        Err(e) => return outcome(&Err(KafkaError::Client(e))),
    };
    let auth =
        match AuthConfig::from_spec(&spec, password).and_then(|a| a.with_tls_ca_file(ca_file)) {
            Ok(a) => a,
            Err(e) => return outcome(&Err(e)),
        };

    let (tx, rx) = mpsc::channel();
    let marker_topic = args.marker_topic.clone();
    // DETACHED ON PURPOSE — the `JoinHandle` is dropped. A `join` would
    // reintroduce exactly the unbounded wait this thread exists to bound, and
    // returning from `main` ends the process, so a thread still inside
    // librdkafka cannot keep the runner alive past its answer.
    std::thread::spawn(move || {
        let answer = match logweir_kafka::rdkafka_reader::RdKafkaReader::connect(&servers, auth) {
            Ok(reader) => probe(&reader, marker_topic.as_deref()),
            Err(e) => outcome(&Err(e)),
        };
        // A receiver that has already timed out makes this a discarded `Err`,
        // which is correct: nobody is listening any more.
        let _ = tx.send(answer);
    });

    rx.recv_timeout(DIAL_TIMEOUT)
        .unwrap_or_else(|_| outcome(&Err(KafkaError::Timeout(DIAL_TIMEOUT))))
}
