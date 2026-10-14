//! `logweir cluster-probe` — interface **I14**: two stdout lines,
//! `cluster-id=<id>` then `reachable=true|false`, then (FX-46) a third,
//! `brokers-answered=<answered>/<listed>`, whenever the brokers the cluster
//! advertises were asked; exit **0** when the cluster can be reached, **1**
//! when it cannot.
//!
//! # Reachable means every advertised broker answered (FX-46)
//!
//! The bootstrap address naming a cluster id is not enough: a listener that
//! advertises an address nobody can reach answers the bootstrap connection,
//! and every backup through it fails a minute later (PROD-01.2's `OFFNET`
//! row). So once the bootstrap address has named its cluster, the probe asks
//! EVERY broker that cluster lists, on its advertised address, for its
//! ApiVersions answer (`logweir_kafka::inventory::observe_broker_view`, the
//! observation the capability rows read). A listed broker that does not
//! answer makes the probe `reachable=false`, exit 1, with
//! `brokers-answered=<answered>/<listed>` and a diagnostic naming the broker
//! and its address. An observation that could not be made at all (not a
//! silent broker: a log it could not read) leaves the answer the bootstrap
//! gave, and says so on stderr.
//!
//! An older controller reads the first two lines by key and ignores the
//! third; this controller reads it into `AdvertisedBrokersUnreachable`.
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
//! `tests/no_network_in_unit_tests.rs`'s allow-list: the
//! probe dials BY DESIGN — it is the whole subcommand.
//!
//! # Nothing but the two lines reaches stdout
//!
//! The diagnostic goes to **stderr**, and so does this subcommand's tracing
//! subscriber ([`install_diagnostics`]), because the two stdout lines are the
//! whole machine contract. A pod log merges the two streams in nondeterministic
//! order, so a controller reads the two lines BY KEY NAME
//! from a bounded tail and never by position — and the two contract lines are
//! the last thing this process writes.

use std::io::Write;
use std::sync::mpsc;
use std::time::Duration;

use logweir_kafka::api_versions::{ApiVersions, NotObserved, Partial};
use logweir_kafka::reader::{AuthConfig, ClusterReader, KafkaError};

use crate::exit::ExitCode;

/// Interface **I14**'s FIRST stdout line, as a prefix. The value is the id the
/// broker returned, or **empty** when none did.
pub const CLUSTER_ID_LINE: &str = "cluster-id=";

/// Interface **I14**'s SECOND stdout line, as a prefix. The value is exactly
/// `true` or exactly `false`.
pub const REACHABLE_LINE: &str = "reachable=";

/// FX-46's THIRD stdout line, as a prefix: how many of the brokers the
/// cluster lists answered this probe on their advertised addresses, as
/// `<answered>/<listed>`, or `0/?` when the bootstrap address named the
/// cluster and the list of brokers it advertises could not be read at all
/// (see [`with_brokers`]). Printed only when the brokers were asked and the
/// answer says something about them; absent on every other arm.
pub const BROKERS_ANSWERED_LINE: &str = "brokers-answered=";

/// The `<listed>` half of [`BROKERS_ANSWERED_LINE`] when the cluster's broker
/// list could not be read.
pub const LISTED_UNKNOWN: &str = "?";

/// How long the probe waits for every broker the cluster lists to answer,
/// after the bootstrap address named its cluster: the observation's own
/// ceiling (`logweir_kafka::inventory::API_VERSIONS_BUDGET`). A healthy
/// cluster answers in one quiet period.
pub const BROKERS_TIMEOUT: Duration = Duration::from_secs(10);

/// What the probe waits beyond [`BROKERS_TIMEOUT`] for the observation's own
/// last read (one quiet period) before it gives up on it.
const BROKERS_MARGIN: Duration = Duration::from_secs(2);

/// `--auth-mode plaintext`.
pub const AUTH_MODE_PLAINTEXT: &str = "plaintext";

/// `--auth-mode scramSha512`. **The camelCase spelling is the contract**: it is
/// byte-identical to `logweir_core::spec::AuthSpec`'s serde spelling and to the
/// `KafkaCluster` CRD's `auth.mode` enum, so a reconciler can copy the value it
/// read straight onto this argv.
pub const AUTH_MODE_SCRAM_SHA_512: &str = "scramSha512";

/// `--auth-mode scramSha256`.
pub const AUTH_MODE_SCRAM_SHA_256: &str = logweir_core::connection::AUTH_MODE_SCRAM_SHA_256;

/// `--auth-mode plain` — SASL/PLAIN, and only with `--tls`.
pub const AUTH_MODE_PLAIN: &str = logweir_core::connection::AUTH_MODE_PLAIN;

/// `--auth-mode mtls` — a TLS client certificate, with `--tls`.
/// The certificate and key reach the probe as projected files named by
/// `LOGWEIR_SOURCE_TLS_CERT_FILE` / `LOGWEIR_SOURCE_TLS_KEY_FILE`, never as
/// flags.
pub const AUTH_MODE_MTLS: &str = logweir_core::connection::AUTH_MODE_MTLS;

/// The environment variable the SASL password is read from, and the only place
/// it is read from.
///
/// **THERE IS NO FLAG FOR IT, AT ANY COMMAND.** A secret on an argv is visible
/// in `/proc/<pid>/cmdline`, in a shell history and in every process listing on
/// the host — and it would land in the Job spec a controller creates, where
/// anyone with pod read can see it.
pub const SOURCE_PASSWORD_ENV: &str = "LOGWEIR_SOURCE_PASSWORD";

/// The environment variable naming a projected private CA file.
///
/// A probe reads ONE cluster, so — exactly as for the password — it reads the
/// source side's variable whatever the object's `spec.role` says. The path
/// goes to librdkafka's `ssl.ca.location`; this process does not open it.
pub const SOURCE_TLS_CA_FILE_ENV: &str = crate::tls_ca::SOURCE_TLS_CA_FILE_VAR;

/// How long the dial is given before it is reported as unreachable.
///
/// Ten seconds, shorter than the 20-second metadata budget `logweir-kafka`'s
/// rdkafka reader gives `fetch_cluster_id`: that call returns only after its
/// whole timespan, and a rejected SASL handshake makes librdkafka retry rather
/// than return, so a wrong password or a wrong port would otherwise present as
/// a 20-second stall. Interface **I14** has an answer for unreachable and none
/// for "still thinking".
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
    /// `--auth-mode plaintext|scramSha512|scramSha256|plain|mtls`.
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
    /// when every broker answered: a probe that worked has no diagnostic. A
    /// reachable probe whose brokers' answers were not all established says
    /// so here ([`with_brokers`]). Written
    /// on ONE line whatever it holds ([`crate::exit::one_line`]): its text
    /// can repeat a broker address or a broker's message.
    pub diagnostic: Option<String>,
    /// `refusal-reason=CredentialBindingMismatch` when the credential's
    /// binding was refused: written on stderr after the diagnostic, as its own
    /// line, and sealed with the Job's token beside the two contract lines
    /// under FX-43's flag.
    pub refusal: Option<String>,
    /// `0` when a broker answered, `1` when none did — Global Constraint 11's
    /// `Ok` and `Operational`.
    pub code: ExitCode,
}

/// The two lines and the exit code, as a **pure function of the bootstrap
/// address's answer**, before any broker it advertises was asked.
///
/// It dials nothing: `Ok(id)` is `cluster-id=<id>` / `reachable=true` / exit
/// **0**, and any `Err` is `cluster-id=` (EMPTY) / `reachable=false` / exit
/// **1**, with the error's own text as the diagnostic. The shipped probe
/// reaches the `Ok` arm only through [`with_brokers`] (FX-46), which asks the
/// advertised brokers first; this arm is what a cluster id with no broker
/// observation would print.
///
/// # The order is asserted, not incidental
///
/// `cluster-id=` FIRST, `reachable=` SECOND. A controller reads both by key
/// name, so the order is not what makes the parse work — it is
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
            refusal: None,
            code: ExitCode::Ok,
        },
        Err(e) => ProbeOutcome {
            stdout: format!("{CLUSTER_ID_LINE}\n{REACHABLE_LINE}false\n"),
            diagnostic: Some(format!("cluster-probe: no cluster id was read: {e}")),
            refusal: None,
            code: ExitCode::Operational,
        },
    }
}

/// **The verdict once the bootstrap address named cluster `id`: what the
/// brokers it advertises answered** (FX-46). Pure, like [`outcome`].
///
/// * Every listed broker answered: `cluster-id=<id>`, `reachable=true`,
///   `brokers-answered=<n>/<n>`, exit 0, no diagnostic.
/// * A listed broker did not: `cluster-id=<id>` (it was observed),
///   `reachable=false`, `brokers-answered=<answered>/<listed>`, exit 1, and a
///   diagnostic naming each silent broker by id and advertised address, and
///   the remedy, `advertised.listeners`.
/// * The bootstrap address answered ApiVersions and the cluster's broker list
///   could not be read through it within [`BROKERS_TIMEOUT`]
///   (`Partial::NoBrokerList`): `reachable=false`, `brokers-answered=0/?`,
///   exit 1. This is the advertised-address failure seen from the other side
///   of a race: librdkafka retires a bootstrap address as soon as the cluster
///   has advertised its brokers (`rd_kafka_metadata_decommission_unavailable_
///   brokers`, `rdkafka_metadata.c:476-535` in rdkafka-sys 4.10.0+2.12.1),
///   so when its own "connected" refresh is answered first, every later
///   metadata request goes to the advertised brokers, and none answered
///   (measured on the `confluent` profile's OFFNET listener).
/// * Every listed broker answered and a bootstrap address did not: reachable,
///   with the silent address named on stderr. The cluster says it has those
///   brokers, and each one answered.
/// * Any other observation that could not be made or read: the bootstrap's
///   answer stands (`reachable=true`, two lines), and stderr says the brokers
///   were not established. Not observed is never "unreachable".
#[must_use]
pub fn with_brokers(id: &str, brokers: &Result<ApiVersions, NotObserved>) -> ProbeOutcome {
    let lines = |reachable: bool, answered: Option<(usize, Option<usize>)>| {
        let mut out = format!("{CLUSTER_ID_LINE}{id}\n{REACHABLE_LINE}{reachable}\n");
        if let Some((a, l)) = answered {
            let l = l.map_or_else(|| LISTED_UNKNOWN.to_string(), |l| l.to_string());
            out.push_str(&format!("{BROKERS_ANSWERED_LINE}{a}/{l}\n"));
        }
        out
    };
    let remedy = "Every broker must advertise a host and port this pod can reach: check \
                  advertised.listeners for the listener this bootstrap address belongs to, or \
                  use the bootstrap address of a listener meant for this network";
    let unreachable = |answered: Option<(usize, Option<usize>)>, diagnostic: String| ProbeOutcome {
        stdout: lines(false, answered),
        diagnostic: Some(diagnostic),
        refusal: None,
        code: ExitCode::Operational,
    };
    let reachable =
        |answered: Option<(usize, Option<usize>)>, diagnostic: Option<String>| ProbeOutcome {
            stdout: lines(true, answered),
            diagnostic,
            refusal: None,
            code: ExitCode::Ok,
        };
    match brokers {
        Ok(view) => reachable(Some((view.brokers(), Some(view.brokers()))), None),
        Err(why) => match why {
            NotObserved::Partial(Partial::NotEveryone {
                answered,
                listed,
                silent,
                ..
            }) if !silent.is_empty() => unreachable(
                Some((*answered, Some(listed.len()))),
                format!(
                    "cluster-probe: the bootstrap address answered and named cluster {id}, and \
                     {} of the {} broker(s) it advertises did not answer: {why} {remedy}",
                    silent.len(),
                    listed.len()
                ),
            ),
            NotObserved::Partial(Partial::NoBrokerList) => unreachable(
                Some((0, None)),
                format!(
                    "cluster-probe: the bootstrap address answered and named cluster {id}, and \
                     no broker it advertises answered a metadata request within \
                     {BROKERS_TIMEOUT:?} (a client stops using a bootstrap address once the \
                     cluster has advertised its brokers). {remedy}"
                ),
            ),
            NotObserved::Partial(Partial::NotEveryone { listed, .. }) => reachable(
                Some((listed.len(), Some(listed.len()))),
                Some(format!(
                    "cluster-probe: every broker cluster {id} advertises answered, and a \
                     bootstrap address did not: {why}"
                )),
            ),
            _ => reachable(
                None,
                Some(format!(
                    "cluster-probe: the bootstrap address answered and named cluster {id}; \
                     whether every broker it advertises can be reached was not established: \
                     {why}"
                )),
            ),
        },
    }
}

/// The outcome of a probe whose credential binding was refused: interface
/// **I14**'s unreachable lines on stdout,
/// unchanged, and on stderr the diagnostic followed by
/// `refusal-reason=CredentialBindingMismatch` — the line the `KafkaCluster`
/// controller reads off the pod log into the `Reachable` condition's reason.
/// Nothing was dialled.
#[must_use]
pub fn refused_by_binding(
    refusal: &logweir_core::connection::CredentialBindingRefusal,
) -> ProbeOutcome {
    let mut o = outcome(&Err(KafkaError::Client(refusal.to_string())));
    o.refusal = Some(logweir_core::guard::refusal_reason_line(
        &refusal.to_string(),
    ));
    o
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
/// **Two calls: `cluster_id()`, then, once the bootstrap address named a
/// cluster, `broker_view()`** (FX-46): every broker the cluster lists, asked on
/// its advertised address. No `list_topics`, no `end_offsets`, no
/// `topic_configs`: the probe's question is "can this cluster be reached", and
/// every other call is another way for a healthy cluster to fail a liveness
/// check.
#[must_use]
pub fn probe(reader: &dyn ClusterReader, marker_topic: Option<&str>) -> ProbeOutcome {
    // Named `_marker_topic` rather than dropped from the signature: the
    // parameter is part of the contract above, and a signature that did not
    // take it would let the reconciler's unconditional pass-through look like
    // a caller error.
    let _marker_topic = marker_topic;
    match reader.cluster_id() {
        Ok(id) => with_brokers(&id, &reader.broker_view(BROKERS_TIMEOUT)),
        Err(e) => outcome(&Err(e)),
    }
}

/// Write an outcome to its two streams.
///
/// The writer seam exists so a test can assert the EXACT BYTES on each stream —
/// `the_probe_prints_the_contract_lines_and_nothing_else` counts the newlines on one and
/// finds the diagnostic on the other — instead of trusting a `println!` nobody
/// can observe. It is the same arrangement `exit::print_refusal_to` uses
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
        // this process writes. A pod log merges the streams and a
        // controller scans a bounded tail; putting the diagnostic ahead of the
        // contract keeps both lines inside that tail however long the
        // diagnostic is. On ONE line: the error text can repeat a broker
        // address or a broker's message, and a line break in it would start a
        // line a controller reads by key.
        writeln!(err, "{}", crate::exit::one_line(d))?;
    }
    if let Some(r) = o.refusal.as_deref() {
        writeln!(err, "{r}")?;
    }
    err.flush()?;
    out.write_all(o.final_lines().render(o.code).as_bytes())?;
    out.flush()
}

impl ProbeOutcome {
    /// The lines a controller reads, for [`crate::exit::FinalLines`]: the two
    /// contract lines, and the binding refusal sealed beside them (its plain
    /// line is on stderr). With no FX-43 token the block is exactly
    /// [`ProbeOutcome::stdout`].
    #[must_use]
    pub fn final_lines(&self) -> crate::exit::FinalLines {
        let mut last = crate::exit::FinalLines::default();
        last.extend(self.stdout.lines().map(str::to_string));
        if let Some(r) = &self.refusal {
            last.vouch(r.clone());
        }
        last
    }
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
/// pin the plaintext arm, which is the defect interface I1 exists to prevent.
///
/// # Errors
///
/// A mode this build does not know (anything but the constants above), a SASL
/// mode (`scramSha512`, `scramSha256`, `plain`) with no `--username`, or
/// `plain`/`mtls` without `--tls`. Both are reported as an unreachable
/// probe by [`run`] rather than as a usage error, so the two contract lines
/// exist for every command line clap accepted.
pub fn auth_spec(
    mode: &str,
    username: Option<&str>,
    tls: bool,
) -> Result<logweir_core::spec::AuthSpec, KafkaError> {
    let sasl_username = || match username {
        Some(u) if !u.trim().is_empty() => Ok(u.to_string()),
        _ => Err(KafkaError::Client(format!(
            "--auth-mode {mode} needs --username: SASL authenticates as a named principal, and \
             a probe that dialled as nobody would report a configuration mistake as an \
             unreachable broker"
        ))),
    };
    let spec = match mode {
        // TLS without SASL is refused, not downgraded: `AuthSpec::Plaintext`
        // has no TLS field, so dialling it would probe an object that says TLS
        // in the clear. The controller refuses the same shape before any Job
        // exists; this is the runner's half.
        AUTH_MODE_PLAINTEXT if tls => {
            return Err(KafkaError::Client(format!(
                "--auth-mode {AUTH_MODE_PLAINTEXT} with --tls (TLS without SASL) is not \
                 supported; it is refused rather than dialled without TLS. Use --auth-mode \
                 {AUTH_MODE_SCRAM_SHA_512} over TLS, --auth-mode {AUTH_MODE_MTLS} for a client \
                 certificate, or drop --tls for a plaintext listener"
            )))
        }
        AUTH_MODE_PLAINTEXT => logweir_core::spec::AuthSpec::Plaintext,
        AUTH_MODE_SCRAM_SHA_512 => logweir_core::spec::AuthSpec::ScramSha512 {
            username: sasl_username()?,
            tls,
        },
        AUTH_MODE_SCRAM_SHA_256 => logweir_core::spec::AuthSpec::ScramSha256 {
            username: sasl_username()?,
            tls,
        },
        AUTH_MODE_PLAIN => logweir_core::spec::AuthSpec::Plain {
            username: sasl_username()?,
            tls,
        },
        AUTH_MODE_MTLS => logweir_core::spec::AuthSpec::Mtls { tls },
        other => {
            return Err(KafkaError::Client(format!(
                "--auth-mode {other} is not a mode this build knows; it accepts {}",
                logweir_core::connection::AUTH_MODES.join(", ")
            )))
        }
    };
    // `plain` (PlainWithoutTls) and `mtls` without `--tls` are
    // refused here, before any client exists — the probe's half of the rule
    // the controller and the CRD state.
    if let Some(refusal) = spec.transport_refusal() {
        return Err(KafkaError::Client(refusal));
    }
    Ok(spec)
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
/// `the_probe_prints_the_contract_lines_and_nothing_else` forbids.
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
/// VERDICT PATH.** The shipped subcommand reaches its lines through the same
/// pure [`outcome`] and [`with_brokers`] that [`probe`] reaches over a double,
/// rather than through a second, parallel arm that a test could never see.
/// Everything a live dial can go wrong at (an unknown mode, a missing
/// username, a missing password, a refused connect, a deadline) becomes an
/// `Err` handed to the same [`outcome`].
fn dial(args: &ProbeArgs) -> ProbeOutcome {
    let servers = bootstrap_servers(&args.bootstrap);
    if servers.is_empty() {
        return outcome(&Err(KafkaError::Client(
            "--bootstrap named no address; it takes a comma-separated list of host:port".into(),
        )));
    }
    // FX-43: an address holding a control character or a line break is
    // refused before any client exists, so none logs it.
    if let Err(refusal) =
        logweir_core::guard::reject_line_breaking_bootstrap_servers("--bootstrap", &servers)
    {
        return outcome(&Err(KafkaError::Client(refusal.0)));
    }
    let spec = match auth_spec(&args.auth_mode, args.username.as_deref(), args.tls) {
        Ok(s) => s,
        Err(e) => return outcome(&Err(e)),
    };
    // A credential whose binding does not name
    // this `KafkaCluster` is refused before the password or key is read and
    // before any client exists. The probe's two stdout lines stay its
    // contract (`reachable=false`); the named reason travels on STDERR as
    // `refusal-reason=CredentialBindingMismatch`, which the controller reads
    // off the pod log into the `Reachable` condition.
    if let Err(refusal) = crate::credential_binding::check_side(crate::tls_ca::Side::Source) {
        return refused_by_binding(&refusal);
    }
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
    // `ssl.ca.location`. `with_tls_ca_file` refuses a CA for a
    // transport that is not TLS, so a CA can never sit beside a clear dial.
    let ca_file = match crate::tls_ca::projected_ca_file(SOURCE_TLS_CA_FILE_ENV) {
        Ok(c) => c,
        Err(e) => return outcome(&Err(KafkaError::Client(e))),
    };
    // The `mtls` pair, from the source side's two variables.
    let client_certificate =
        match crate::tls_ca::projected_client_certificate(crate::tls_ca::Side::Source) {
            Ok(files) => files,
            Err(e) => return outcome(&Err(KafkaError::Client(e))),
        };
    let auth = match AuthConfig::from_spec(&spec, password)
        .and_then(|a| a.with_tls_ca_file(ca_file))
        .and_then(|a| a.with_client_certificate(client_certificate))
    {
        Ok(a) => a,
        Err(e) => return outcome(&Err(e)),
    };

    // TWO ANSWERS, EACH WITH ITS OWN BOUND: the cluster id within
    // `DIAL_TIMEOUT`, then what the advertised brokers answered within
    // `BROKERS_TIMEOUT` (FX-46). One bound over both would either stretch a
    // wrong password's stall or cut the brokers' observation short. The
    // verdict is the same pair of pure functions `probe` reaches over a
    // double: `outcome` for no cluster id, `with_brokers` after one.
    enum Step {
        Id(Result<String, KafkaError>),
        Brokers(Result<ApiVersions, NotObserved>),
    }
    let (tx, rx) = mpsc::channel();
    // `--marker-topic` is accepted and never asserted, as `probe` documents.
    let _marker_topic = args.marker_topic.as_deref();
    // DETACHED ON PURPOSE — the `JoinHandle` is dropped. A `join` would
    // reintroduce exactly the unbounded wait this thread exists to bound, and
    // returning from `main` ends the process, so a thread still inside
    // librdkafka cannot keep the runner alive past its answer.
    std::thread::spawn(move || {
        let reader = match logweir_kafka::rdkafka_reader::RdKafkaReader::connect(&servers, auth) {
            Ok(reader) => reader,
            Err(e) => {
                let _ = tx.send(Step::Id(Err(e)));
                return;
            }
        };
        let id = reader.cluster_id();
        let named = id.is_ok();
        // A receiver that has already timed out makes this a discarded `Err`,
        // which is correct: nobody is listening any more.
        if tx.send(Step::Id(id)).is_ok() && named {
            let _ = tx.send(Step::Brokers(reader.broker_view(BROKERS_TIMEOUT)));
        }
    });

    let id = match rx.recv_timeout(DIAL_TIMEOUT) {
        Ok(Step::Id(Ok(id))) => id,
        Ok(Step::Id(Err(e))) => return outcome(&Err(e)),
        Ok(Step::Brokers(_)) | Err(_) => return outcome(&Err(KafkaError::Timeout(DIAL_TIMEOUT))),
    };
    let brokers = match rx.recv_timeout(BROKERS_TIMEOUT + BROKERS_MARGIN) {
        Ok(Step::Brokers(view)) => view,
        Ok(Step::Id(_)) | Err(_) => Err(NotObserved::Other(format!(
            "the advertised brokers' answers were not read within {:?}",
            BROKERS_TIMEOUT + BROKERS_MARGIN
        ))),
    };
    with_brokers(&id, &brokers)
}
