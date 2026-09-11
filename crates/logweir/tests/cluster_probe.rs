//! `logweir cluster-probe` — interface **I14**, the whole contract.
//!
//! NOTHING HERE DIALS. The two lines and the exit code are a pure function of a
//! `Result<String, KafkaError>` ([`probe::outcome`]), and the one call the probe
//! makes on a cluster goes through a `&dyn ClusterReader` ([`probe::probe`]), so
//! every arm of the contract is asserted over a [`StubReader`] with no socket,
//! no 20-second metadata timeout and nothing approaching Global Constraint 22's
//! 15 s per-test bound. `crates/logweir/src/probe.rs` is on
//! `tests/no_network_in_unit_tests.rs`'s allow-list because its `run`/`dial`
//! pair really does construct a client — that is the subcommand — and this file
//! is what makes the allow-list affordable.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use clap::Parser;
use logweir::cli;
use logweir::exit::ExitCode;
use logweir::probe::{
    auth_spec, bootstrap_servers, outcome, probe, write_outcome, ProbeArgs, ProbeOutcome,
    AUTH_MODE_PLAINTEXT, AUTH_MODE_SCRAM_SHA_512, CLUSTER_ID_LINE, DIAL_TIMEOUT, REACHABLE_LINE,
    SOURCE_PASSWORD_ENV,
};
use logweir_kafka::reader::{ClusterReader, ConsumedRecord, KafkaError, TopicMeta};

/// The cluster id every reachable arm expects. The same 22-character shape a
/// real Kafka `ClusterId` has, so a test cannot pass by accident on a
/// one-character value.
const CLUSTER_ID: &str = "ALLOWED0000000000000000";

// ---------------------------------------------------------------------------
// The double
// ---------------------------------------------------------------------------

/// A `ClusterReader` that answers `cluster_id()` from a canned `Result` and
/// reports an EMPTY topic list.
///
/// THE EMPTY TOPIC LIST IS THE POINT OF THE MARKER-TOPIC ROW. A probe that
/// checked `--marker-topic` against this reader would find the topic absent and
/// refuse; one that does not check is byte-identical with the flag and without
/// it. Every other trait method panics: reaching one is a probe doing more than
/// reading a cluster id, and a panic names which call it was.
struct StubReader {
    id: Result<String, KafkaError>,
}

impl StubReader {
    fn reachable() -> Self {
        Self {
            id: Ok(CLUSTER_ID.to_string()),
        }
    }
    fn unreachable() -> Self {
        Self {
            id: Err(KafkaError::Unreachable(
                "no broker returned a ClusterId within 20s".to_string(),
            )),
        }
    }
}

impl ClusterReader for StubReader {
    fn cluster_id(&self) -> Result<String, KafkaError> {
        self.id.clone()
    }
    fn list_topics(&self) -> Result<Vec<TopicMeta>, KafkaError> {
        // EMPTY, AND ANSWERED RATHER THAN PANICKED. `list_topics` is the call a
        // marker-topic check would make, so the marker-topic row needs it to
        // succeed-and-say-nothing: a panic here would make that row pass for
        // the wrong reason (the mutant would abort instead of refusing).
        Ok(Vec::new())
    }
    fn end_offsets(&self, topic: &str) -> Result<Vec<(i32, i64)>, KafkaError> {
        panic!("cluster-probe called end_offsets({topic}); it reads a cluster id and nothing else")
    }
    fn topic_configs(&self, topic: &str) -> Result<BTreeMap<String, String>, KafkaError> {
        panic!(
            "cluster-probe called topic_configs({topic}); the target-topic config guard is phase \
             0's, not a liveness probe's"
        )
    }
    fn broker_configs(&self) -> Result<BTreeMap<String, String>, KafkaError> {
        panic!("cluster-probe called broker_configs(); it reads a cluster id and nothing else")
    }
    fn consume_range(
        &self,
        topic: &str,
        partition: i32,
        from: i64,
        max: usize,
    ) -> Result<Vec<ConsumedRecord>, KafkaError> {
        panic!(
            "cluster-probe called consume_range({topic}, {partition}, {from}, {max}); it reads no \
             records"
        )
    }
}

/// `write_outcome` into two byte sinks, so a row can assert the EXACT bytes on
/// each stream instead of trusting a `println!` nobody can observe.
fn streams(o: &ProbeOutcome) -> (String, String) {
    let mut out: Vec<u8> = Vec::new();
    let mut err: Vec<u8> = Vec::new();
    write_outcome(&mut out, &mut err, o).expect("a Vec sink never fails");
    (
        String::from_utf8(out).expect("stdout is UTF-8"),
        String::from_utf8(err).expect("stderr is UTF-8"),
    )
}

fn probe_src() -> String {
    std::fs::read_to_string(
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("src")
            .join("probe.rs"),
    )
    .expect("crates/logweir/src/probe.rs is readable")
}

/// `src` with comment lines removed. The file legitimately DISCUSSES what it
/// must not do, and a scan that could not tell prose from code would force the
/// reasoning out of the source.
fn code_only(src: &str) -> String {
    src.lines()
        .filter(|l| {
            let t = l.trim_start();
            !(t.starts_with("//") || t.starts_with("///") || t.starts_with("//!"))
        })
        .collect::<Vec<_>>()
        .join("\n")
}

// ---------------------------------------------------------------------------
// Interface I14 — the two lines and the exit map
// ---------------------------------------------------------------------------

/// **Interface I14, both arms.** A reachable cluster prints
/// `cluster-id=<id>` then `reachable=true` and exits **0**; an unreachable one
/// prints `cluster-id=` (EMPTY) then `reachable=false` and exits **1**.
///
/// THE WHOLE STRING IS COMPARED, IN ORDER. Two `contains` calls would pass on
/// the lines printed in the other order, which is one of this task's mutants —
/// and a controller that reads by key name would survive the swap while the
/// CONTRACT would have silently become two spellings of itself.
///
/// NEITHER ARM DIALS: the formatting and the exit mapping are a pure function
/// of the `Result<String, KafkaError>`, and the reachable arm reaches it through
/// a `&dyn ClusterReader` double.
#[test]
fn cluster_probe_prints_the_cluster_id_and_exits_zero() {
    let o = probe(&StubReader::reachable(), None);
    let (out, err) = streams(&o);
    assert_eq!(
        out,
        format!("{CLUSTER_ID_LINE}{CLUSTER_ID}\n{REACHABLE_LINE}true\n"),
        "interface I14: `cluster-id=<id>` FIRST, `reachable=true` SECOND, and nothing else"
    );
    assert_eq!(o.code, ExitCode::Ok, "a reached broker is exit 0");
    assert!(
        err.is_empty(),
        "a probe that worked has no diagnostic; got {err:?}"
    );

    // ARM 2 — unreachable. `KafkaError::Unreachable` is what
    // `rdkafka_reader`'s `cluster_id()` returns when no broker answered inside
    // its own budget.
    let o = probe(&StubReader::unreachable(), None);
    let (out, err) = streams(&o);
    assert_eq!(
        out,
        format!("{CLUSTER_ID_LINE}\n{REACHABLE_LINE}false\n"),
        "an unreachable probe prints an EMPTY cluster id and `reachable=false` — nothing is \
         guessed and no id can leak from a previous run"
    );
    assert_eq!(
        o.code,
        ExitCode::Operational,
        "an unreachable broker is exit 1: the probe could not be performed, and NO artifact was \
         written (Global Constraint 11)"
    );
    assert!(
        err.contains("unreachable"),
        "the error text goes to STDERR, because the two stdout lines are the whole machine \
         contract; got {err:?}"
    );
}

/// **Exactly two stdout lines, and a third is a defect.**
///
/// Counted as `\n`-terminated lines rather than with `lines().count()`, which
/// cannot tell a missing final newline from a present one — a runner whose last
/// line is unterminated is a runner whose last line a line-oriented reader may
/// drop.
///
/// The diagnostic is asserted to reach the STDERR sink, on the same outcome, so
/// "nothing else on stdout" is proved against a run that had something to say.
#[test]
fn the_probe_prints_exactly_two_stdout_lines() {
    for o in [
        probe(&StubReader::reachable(), None),
        probe(&StubReader::unreachable(), None),
    ] {
        let (out, err) = streams(&o);
        assert_eq!(
            out.matches('\n').count(),
            2,
            "interface I14 is exactly two `\\n`-terminated stdout lines; got {out:?}"
        );
        assert!(
            out.ends_with('\n'),
            "the second line is terminated, so a line-oriented reader does not drop it: {out:?}"
        );
        let lines: Vec<&str> = out.trim_end_matches('\n').split('\n').collect();
        assert_eq!(lines.len(), 2, "no third line: {out:?}");
        assert!(
            lines[0].starts_with(CLUSTER_ID_LINE) && lines[1].starts_with(REACHABLE_LINE),
            "the two lines are the two contract keys, in order: {lines:?}"
        );
        // The diagnostic never appears on stdout, at either arm.
        if let Some(d) = o.diagnostic.as_deref() {
            assert!(
                !out.contains(d),
                "the diagnostic reached stdout, which is a third line by another name: {out:?}"
            );
            assert!(
                err.contains(d),
                "the diagnostic must reach the stderr sink instead; got {err:?}"
            );
        }
    }
}

/// **`--marker-topic` is accepted and never asserted.**
///
/// Over a reader whose topic list is EMPTY — so a marker-topic check would find
/// `absent-topic` missing — the output and the exit code are BYTE-IDENTICAL with
/// the flag and without it.
///
/// WHY THIS ROW EXISTS AT ALL. `logweir doctor` fails unless a healthy marker
/// topic exists on the cluster it checked. `KafkaCluster.spec.markerTopic` is
/// optional and a `role: source` cluster has none, so a probe that inherited
/// that check would report **`reachable: false` for every source cluster in the
/// fleet** — a liveness probe whose answer is decided by a drill precondition.
#[test]
fn the_probe_never_refuses_on_a_marker_topic() {
    let without = probe(&StubReader::reachable(), None);
    let with = probe(&StubReader::reachable(), Some("absent-topic"));
    assert_eq!(
        streams(&with).0,
        streams(&without).0,
        "the stdout bytes must not depend on --marker-topic"
    );
    assert_eq!(
        with.code, without.code,
        "and neither must the exit code: the marker-topic check is phase 0's guard against a \
         target that cannot prove it is scratch, not a statement about reachability"
    );
    assert_eq!(with, without, "the whole outcome is identical");

    // And the unreachable arm too, so the flag cannot flip an answer in either
    // direction.
    assert_eq!(
        probe(&StubReader::unreachable(), Some("absent-topic")),
        probe(&StubReader::unreachable(), None),
        "nor on the unreachable arm"
    );
}

/// **The probe reads no cluster allowlist and opens no approver key.**
///
/// A source-reading row over `crates/logweir/src/probe.rs`: it names none of
/// the three tokens by which `doctor`'s two extra mandatory paths reach the
/// code. THE RAW SOURCE IS SCANNED, comments included — this file's prose
/// deliberately discusses the checks it does not perform in words that are not
/// these identifiers, so the stronger scan costs nothing and closes the hole a
/// comment-stripping version would leave.
#[test]
fn the_probe_reads_no_allowlist_and_no_approver_key() {
    let src = probe_src();
    for token in ["allowed_clusters", "approver_key", "AllowedClusters"] {
        assert!(
            !src.contains(token),
            "crates/logweir/src/probe.rs names `{token}`. `logweir doctor` cannot be the probe \
             precisely because those two paths are MANDATORY flags on it and a `KafkaCluster` \
             need not satisfy either: a restore-target allowlist and an approver's public key \
             are drill-time guards belonging to phase 0, checked against the plan a restore is \
             about to run."
        );
    }
    // And the flags are not on the subcommand either — the other half of the
    // same claim, asserted against the parser rather than against the source.
    //
    // THE KIND IS ASSERTED AND NOT MERELY `is_err()`, and the mutation round is
    // why: a plant that added ONE of the two as a required flag made the whole
    // command line fail for the OTHER one's absence, so `is_err()` was true and
    // the row passed while `doctor`'s first mandatory path had landed on the
    // probe. `UnknownArgument` is the claim — this parser has never heard of the
    // flag — and nothing else satisfies it.
    for flag in ["--allowed-clusters", "--approver-key"] {
        // `match` and not `expect_err`: `Cli` derives no `Debug`, so the `Ok`
        // arm has nothing to print.
        let Err(err) = cli::Cli::try_parse_from([
            "logweir",
            "cluster-probe",
            "--bootstrap",
            "kafka:9092",
            flag,
            "/dev/null",
        ]) else {
            panic!("`cluster-probe {flag}` parsed; it must not accept a drill-time path")
        };
        assert_eq!(
            err.kind(),
            clap::error::ErrorKind::UnknownArgument,
            "`cluster-probe {flag}` must be UNKNOWN to this parser, not merely part of a command \
             line that failed for some other reason; accepting it would be the first half of \
             inheriting the check. Got {err}"
        );
    }
}

// THE ONE-CONSTRUCTION-SITE ROW THAT STOOD HERE IS GONE, AND IT IS NOT LOST.
//
// Task 15c wrote `probe_builds_its_auth_through_the_one_construction_site` in
// this file because Task 6's guard iterated a FIXED three-file list and so did
// not extend to `probe.rs` — disclosed at the time, and raised by Task 15c's
// review as finding **M-2**. Task 16b replaced that list with a DERIVED walk
// over `crates/logweir/src/**` and `crates/weirkeeper/src/**`:
// `no_network_in_unit_tests.rs::the_one_construction_site_rule_is_derived_from_the_tree`.
//
// Every claim this row made is asserted there, for `probe.rs` among the four
// sanctioned sites and no longer only for it: the file builds its auth through
// `AuthConfig::from_spec`, it pins neither `AuthConfig` arm in code, and
// EXACTLY ONE function in it names the dialling constructor — `fn dial(`, the
// impure wrapper, not `outcome`, not `probe`, not `run`. The `fn_bodies`
// brace-counter that made the last claim possible moved with it, verbatim.
//
// ONE GUARD AND ONE ALLOWLIST was the point: a tree carrying two guards for
// one rule is a tree where the next site is added to neither.

/// The dial is BOUNDED, and the bound is shorter than the reader's own.
///
/// `logweir-kafka`'s rdkafka reader gives `fetch_cluster_id` a 20-second budget
/// and returns NULL only after the whole of it; a rejected SASL handshake makes
/// librdkafka retry rather than return. So a probe with no bound of its own
/// presents a wrong password as a stall, and interface **I14** has an answer for
/// unreachable and none for "still thinking".
#[test]
fn the_dial_is_bounded_at_ten_seconds() {
    assert_eq!(
        DIAL_TIMEOUT,
        std::time::Duration::from_secs(10),
        "the brief names no timeout, so 10 s is this task's recorded default"
    );
    assert!(
        DIAL_TIMEOUT < std::time::Duration::from_secs(20),
        "a bound that is not shorter than `rdkafka_reader`'s own `const T` bounds nothing"
    );
    // A timeout IS an unreachable probe, reported as one.
    let o = outcome(&Err(KafkaError::Timeout(DIAL_TIMEOUT)));
    assert_eq!(o.code, ExitCode::Operational);
    assert_eq!(
        streams(&o).0,
        format!("{CLUSTER_ID_LINE}\n{REACHABLE_LINE}false\n"),
        "a deadline that expired prints the same two lines as any other failure to reach a broker"
    );
}

/// `--auth-mode` maps onto the spec's own two spellings, and every other value
/// is an unreachable probe naming the mode rather than a usage error.
///
/// BYTE-IDENTICAL SPELLINGS ARE THE CONTRACT: `plaintext` and `scramSha512` are
/// what `logweir_core::spec::AuthSpec` serialises to and what the `KafkaCluster`
/// CRD's `auth.mode` enum accepts, so the reconciler copies the value it read
/// onto the argv without translating it.
#[test]
fn the_auth_mode_flag_maps_onto_the_spec_spellings() {
    assert_eq!(AUTH_MODE_PLAINTEXT, "plaintext");
    assert_eq!(AUTH_MODE_SCRAM_SHA_512, "scramSha512");
    assert_eq!(
        logweir_core::spec::AuthSpec::Plaintext.mode_str(),
        AUTH_MODE_PLAINTEXT,
        "the flag value and the spec's own mode string must be the same bytes"
    );
    assert_eq!(
        logweir_core::spec::AuthSpec::ScramSha512 {
            username: "u".to_string(),
            tls: false,
        }
        .mode_str(),
        AUTH_MODE_SCRAM_SHA_512,
        "and so must the other one — these are also the `KafkaCluster` CRD's `auth.mode` enum, \
         byte for byte (interface I33), which is what lets the reconciler copy the value it read \
         onto this argv untranslated"
    );

    assert_eq!(
        auth_spec(AUTH_MODE_PLAINTEXT, None, false).expect("plaintext needs no username"),
        logweir_core::spec::AuthSpec::Plaintext
    );
    assert_eq!(
        auth_spec(AUTH_MODE_SCRAM_SHA_512, Some("logweir"), true).expect("scram with a username"),
        logweir_core::spec::AuthSpec::ScramSha512 {
            username: "logweir".to_string(),
            tls: true,
        },
        "`tls` is INDEPENDENT of the mode and comes from its own flag, exactly as \
         `KafkaCluster.spec.auth.tls` sits beside `auth.mode`"
    );

    // scramSha512 with no username: an unreachable probe, naming the flag.
    let err = auth_spec(AUTH_MODE_SCRAM_SHA_512, None, false).expect_err("no username");
    assert!(
        err.to_string().contains("--username"),
        "the diagnostic names the missing flag; got {err}"
    );
    let o = outcome(&Err(err));
    assert_eq!(
        streams(&o).0,
        format!("{CLUSTER_ID_LINE}\n{REACHABLE_LINE}false\n"),
        "the two contract lines exist for every command line clap accepted, so a controller \
         reading the log always finds an answer"
    );

    // An unknown mode: the same shape, naming the mode and the two it knows.
    let err = auth_spec("mtls", None, false).expect_err("mtls is not a tag-1 mode");
    let text = err.to_string();
    assert!(
        text.contains("mtls") && text.contains(AUTH_MODE_SCRAM_SHA_512),
        "the diagnostic names what it got and what it accepts; got {text}"
    );
}

/// `--bootstrap` is ONE comma-separated value, split here.
#[test]
fn bootstrap_is_one_comma_separated_value() {
    assert_eq!(
        bootstrap_servers("a:9092,b:9092"),
        vec!["a:9092".to_string(), "b:9092".to_string()]
    );
    assert_eq!(
        bootstrap_servers(" a:9092 , b:9092 ,"),
        vec!["a:9092".to_string(), "b:9092".to_string()],
        "entries are trimmed and empties dropped: a controller joining a list with commas may \
         leave a trailing one"
    );
    assert!(
        bootstrap_servers(" , ").is_empty(),
        "a value naming no address yields no address, and `run` reports that as unreachable \
         rather than dialling `\"\"`"
    );
}

/// The subcommand IS `cluster-probe`, with interface **I14**'s flag list, and it
/// is a subcommand of THIS binary.
///
/// GC3 IS UNTOUCHED BY IT. The engine's four reachable subcommands are
/// `backup`, `restore`, `validate-restore` and `validation run`;
/// `cluster-probe` is a `logweir` subcommand that spawns no engine at all, which
/// is why `scripts/check-no-oso.sh` — whose GC3 block scans the balanced
/// expressions around `run_engine(` and a direct spawn of the engine binary —
/// never sees it.
#[test]
fn the_subcommand_is_cluster_probe_with_the_interface_flags() {
    let parsed = cli::Cli::try_parse_from([
        "logweir",
        "cluster-probe",
        "--bootstrap",
        "a:9092,b:9092",
        "--auth-mode",
        AUTH_MODE_SCRAM_SHA_512,
        "--username",
        "logweir",
        "--tls",
        "--marker-topic",
        "logweir.scratch",
    ])
    .expect("interface I14's flag list parses");
    match parsed.command {
        cli::Command::ClusterProbe {
            bootstrap,
            auth_mode,
            username,
            tls,
            marker_topic,
        } => {
            assert_eq!(bootstrap, "a:9092,b:9092");
            assert_eq!(auth_mode, AUTH_MODE_SCRAM_SHA_512);
            assert_eq!(username.as_deref(), Some("logweir"));
            assert!(tls);
            assert_eq!(marker_topic.as_deref(), Some("logweir.scratch"));
        }
        _ => panic!("`cluster-probe` parsed as another subcommand"),
    }

    // `--bootstrap` alone is enough — the property `doctor` cannot have.
    let minimal = cli::Cli::try_parse_from(["logweir", "cluster-probe", "--bootstrap", "a:9092"])
        .expect("one flag is the whole requirement");
    match minimal.command {
        cli::Command::ClusterProbe {
            auth_mode,
            username,
            tls,
            marker_topic,
            ..
        } => {
            assert_eq!(
                auth_mode, AUTH_MODE_PLAINTEXT,
                "the default mode is plaintext"
            );
            assert!(username.is_none() && marker_topic.is_none() && !tls);
        }
        _ => panic!("`cluster-probe` parsed as another subcommand"),
    }

    // And there is no flag for the password, at this command or any other.
    assert!(
        cli::Cli::try_parse_from([
            "logweir",
            "cluster-probe",
            "--bootstrap",
            "a:9092",
            "--password",
            "hunter2",
        ])
        .is_err(),
        "there is deliberately NO password flag: a secret on an argv is visible in \
         /proc/<pid>/cmdline, in a shell history, in every process listing on the host, and in \
         the Job spec a controller creates"
    );
}

/// The password comes from `LOGWEIR_SOURCE_PASSWORD` and from nowhere else, and
/// an absent one at `scramSha512` is an unreachable probe rather than a hang.
///
/// Asserted through `AuthConfig::from_spec` — the site that owns the decision —
/// so this row states the division of labour rather than restating a string.
#[test]
fn the_password_comes_from_the_environment_and_an_absent_one_is_not_a_hang() {
    assert_eq!(SOURCE_PASSWORD_ENV, "LOGWEIR_SOURCE_PASSWORD");
    let code = code_only(&probe_src());
    assert!(
        code.contains("std::env::var(SOURCE_PASSWORD_ENV)"),
        "probe.rs reads the password from the environment, through that constant and not \
         through a second spelling of the variable's name"
    );
    assert_eq!(
        code.matches("SOURCE_PASSWORD_ENV").count(),
        2,
        "twice in code: the declaration and the one read. A third occurrence is a second place \
         the variable is consulted"
    );

    let spec = logweir_core::spec::AuthSpec::ScramSha512 {
        username: "logweir".to_string(),
        tls: true,
    };
    let err = logweir_kafka::reader::AuthConfig::from_spec(&spec, None)
        .expect_err("scramSha512 with no projected password");
    let o = outcome(&Err(err));
    assert_eq!(
        o.code,
        ExitCode::Operational,
        "operational (exit 1), not a guard refusal (exit 3): nothing was refused — project the \
         Secret and re-probe"
    );
    assert_eq!(
        streams(&o).0,
        format!("{CLUSTER_ID_LINE}\n{REACHABLE_LINE}false\n"),
        "and it is `reachable=false`, printed at once, never a dial that waits for a handshake \
         it has no credential for"
    );
}

/// `ProbeArgs` is the parsed command line and nothing more — it holds no path.
///
/// The structural half of "it reads no files": a probe that opened anything
/// would need somewhere to keep the path, and this row is what a future editor
/// trips over before the source scan above does.
#[test]
fn the_probe_arguments_hold_no_path() {
    let args = ProbeArgs {
        bootstrap: "a:9092".to_string(),
        auth_mode: AUTH_MODE_PLAINTEXT.to_string(),
        username: None,
        tls: false,
        marker_topic: None,
    };
    // Compiles only while every field is a `String`/`bool`/`Option<String>`.
    let _: &str = &args.bootstrap;
    let code = code_only(&probe_src());
    for token in ["PathBuf", "File::open(", "read_to_string("] {
        assert!(
            !code.contains(token),
            "probe.rs names `{token}` in code; the probe opens no file at all"
        );
    }
    // Named so the unused import that would otherwise be needed for the claim
    // above is not smuggled in: this test owns the only `PathBuf` mention.
    let _unused: Option<PathBuf> = None;
}
