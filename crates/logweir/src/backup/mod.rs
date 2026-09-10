//! `logweir backup run` — GC18's phase −1, the source-side capture.
//!
//! # Why this module exists at all
//!
//! `scripts/e2e-seed.sh:124-125` runs `kafka-backup backup --config
//! /config/backup-drill.yaml` from a HARNESS, against a config checked in at
//! `e2e/compose/config/backup-drill.yaml:14-39`. So every archive this
//! product has ever restored from was produced by a shell script, not by
//! Logweir, and `source.captured_by_logweir` is `false` in every scorecard
//! the tree has ever emitted. Tag 1's first half is "scheduled backups" and
//! there was no code path from a Logweir spec to an archive. This is it.
//!
//! The engine's `backup` takes `--config` and nothing else
//! [VERIFIED U:crates/kafka-backup-cli/src/main.rs:35-39,559-561], so the
//! entire surface is the rendered YAML — which Task 2 built
//! (`logweir_engine_oso::render_backup`).
//!
//! # The four rails of GC18(c), and where each one is
//!
//! 1. **A mandatory named-topic allowlist with no wildcard** —
//!    `phase_minus1_admit`'s steps 2 and 3 (**G-GLOB**, plus the non-empty
//!    check), backed by `render_backup::render`'s own call.
//! 2. **A read-only assertion** — structural, and asserted from two ends: the
//!    rendered document names no write key (Task 2's
//!    `backup_document_names_no_write_key`), and this module never sets a
//!    scratch namespace on its reader, so the topic-deleting call refuses
//!    every name it is given (`rdkafka_reader.rs:446-460`) while the consumer
//!    keeps `enable.auto.commit = false` (`:81-82`) and
//!    `allow.auto.create.topics = false` (`:45`).
//!    `the_backup_path_never_scopes_a_deleter` reads this directory's raw
//!    source text and pins it, so the claim cannot drift back silently.
//!
//!    **That test is why nothing under `crates/logweir/src/backup/` names the
//!    scratch-namespace setter or the deleting method by identifier, even in
//!    prose.** The citation above is a file and a line range instead. A test
//!    over raw source cannot tell a comment from a call, and the honest way to
//!    keep it unambiguous is to leave the identifiers out rather than to teach
//!    the test to strip comments — a guard that has to parse Rust to decide
//!    what it is looking at is a guard with a parser bug in its future.
//! 3. **`purge_topics` and `dry_run` refused in the rendered document** —
//!    `phase_minus1_admit`'s step 1 over the SPEC TEXT, and
//!    `render_backup::render_and_digest`'s fail-closed re-scan over the
//!    rendered bytes.
//! 4. **The source `cluster_id` recorded and re-asserted `!= target`** —
//!    `phase_minus1_admit`'s steps 4 and 5. Stated ONCE, there.
//!
//! # The shape of this module, and why
//!
//! `run` builds three real handles and delegates. `run_with` and
//! `execute_with` take those handles as parameters and construct nothing, so
//! every named test runs in process against a `ClusterReader` double, a
//! `DataEngine` double and `Store::in_memory` — no socket, no subprocess, no
//! 20-second `rdkafka_reader.rs:16` metadata timeout, and no test in the
//! default suite anywhere near GC22's 15 s per-test bound. The tree already
//! measured the alternative: `crates/logweir/tests/no_network_in_unit_tests.rs:11-13`
//! records `tests/doctor.rs::check_7_…` at **26.60 s** for one dialling test.
//! A binary CANNOT be handed an in-process double, which is why the one
//! binary-level argv assertion lives under `e2e/` (`e2e/tests/backup_argv.rs`).
pub mod phase_minus1_admit;
pub mod phase_run;

use crate::exit::ExitCode;
use logweir_core::engine::{AuthRender, BackupFacts, BackupPlan, DataEngine};
use logweir_core::spec::{AllowedClusters, AuthSpec, BackupSpec};
use logweir_engine_oso::storage::Store;
use logweir_kafka::reader::ClusterReader;
use std::collections::BTreeMap;
use std::path::PathBuf;

/// `logweir backup run`'s flag set. **I10** (`--backup-id-override`) is this
/// task's; Task 18's `BackupSchedule` reconciler only passes it.
pub struct BackupRunArgs {
    pub spec: PathBuf,
    pub allowed_clusters: PathBuf,
    /// Declared and threaded here; **read by Task 5b**, which signs the
    /// backup receipt with it (**I6**).
    ///
    /// Deliberately NOT opened by this task. Task 4 emits no signed document,
    /// and reading a private key that nothing uses is handling key material
    /// for no purpose. `--receipt-out` below is what refuses, so an operator
    /// who asks for the document the key would sign gets a message rather
    /// than a silent success.
    pub signing_key: PathBuf,
    /// Copied into `BackupReceipt` by Task 5b.
    pub triggered_by: Option<String>,
    pub out: Option<PathBuf>,
    pub receipt_out: Option<PathBuf>,
    /// **I10.** Replaces the derived `backup_id` in both `BackupPlan` and
    /// `BackupOutcome`. Task 18 passes `<schedule>-<slot>`; nothing about a
    /// schedule is decided here.
    pub backup_id_override: Option<String>,
}

/// What one `logweir backup run` established. Task 5b turns this into the
/// signed `BackupReceipt`; Task 7's `captured_by_logweir` and Task 12's
/// reader consume it by these names.
#[derive(Debug, Clone)]
pub struct BackupOutcome {
    pub backup_id: String,
    pub run_id: String,
    pub source_cluster_id: String,
    /// Filled from `spec.source.auth`; Task 5b puts it in the receipt.
    pub source_auth: AuthRender,
    pub manifest_key: String,
    /// `"sha256:<hex>"`, over the exact manifest bytes this run READ back.
    pub manifest_sha256: String,
    pub records_per_topic: BTreeMap<String, u64>,
    pub covered_from_ms: i64,
    pub covered_to_ms: i64,
    pub facts: BackupFacts,
}

/// The backup path's error type, with the same GC11 mapping discipline
/// `DrillError` has: ONE `exit_code` match, and nothing else in this module
/// may map a `BackupError` to an `ExitCode`.
///
/// `Guard` is the only variant that reaches exit 3, and it is the only one
/// that can be constructed before a socket exists — which is what makes
/// "refused by a guard, BEFORE anything runs" true rather than aspirational.
#[derive(Debug, thiserror::Error)]
pub enum BackupError {
    /// The backup could not be attempted or continued for a reason that says
    /// nothing about the source cluster or the archive. Exit 1, no document.
    #[error("operational: {0}")]
    Operational(String),
    /// A guard refused the plan before anything ran. Exit 3.
    #[error("guard: {0}")]
    Guard(#[from] logweir_core::guard::GuardRefusal),
    /// The source cluster could not be read. Exit 1: the plan may be perfectly
    /// fine and the correct action is to retry — it is NOT a refusal.
    #[error("kafka: {0}")]
    Kafka(#[from] logweir_kafka::reader::KafkaError),
    /// The engine failed, or refused to render the document. Exit 1 (ruling
    /// R-E): by the time the renderer is reached, phase −1's guards have
    /// already run, so GC11's exit 3 ("refused before anything runs") does not
    /// describe it.
    #[error("engine: {0}")]
    Engine(#[from] logweir_core::engine::EngineError),
}

impl BackupError {
    /// Global Constraint 11, in ONE place — the `DrillError::exit_code`
    /// pattern (`crates/logweir/src/drill/mod.rs:87-113`). Exit 2 and exit 4
    /// are unreachable from this command in tag 1: exit 2 means "a signed
    /// scorecard exists and does not pass", and a backup emits no scorecard;
    /// exit 4 means "signing failed", and Task 5b owns the only signing this
    /// command will ever do.
    pub fn exit_code(&self) -> ExitCode {
        match self {
            BackupError::Guard(_) => ExitCode::GuardRefused, // 3
            BackupError::Operational(_) | BackupError::Kafka(_) | BackupError::Engine(_) => {
                ExitCode::Operational // 1
            }
        }
    }
}

impl From<BackupError> for ExitCode {
    fn from(e: BackupError) -> Self {
        e.exit_code()
    }
}

/// `spec.source.auth` as the plan and the receipt carry it.
///
/// Task 4 shipped this as an explicit `match`, because `AuthSpec::to_render()`
/// was Task 6's; Task 6 replaced the body with that call and changed nothing
/// else, so there is now ONE mapping from a spec's auth to a plan's auth and
/// the backup and restore paths cannot drift apart on it (interface **I1**).
///
/// The thin wrapper is kept rather than inlined at its two call sites: it is
/// the name `BackupOutcome::source_auth`'s own doc comment points at, and it
/// is where the paragraph below belongs.
///
/// It maps and does not refuse, and since Task 6 there is nothing left to
/// refuse: both `AuthRender` arms render (`crate::yaml::
/// render_security_block`), so a SCRAM spec is recorded faithfully here AND
/// rendered faithfully downstream. Before Task 6 it was recorded here and
/// then refused by the renderer as `RenderError::UnsupportedAuthMode` —
/// exit 1, which told Task 18's cron reconciler to retry a plan this build
/// could never accept. That arm is now reachable and passes.
pub fn source_auth_render(auth: &AuthSpec) -> AuthRender {
    auth.to_render()
}

/// A spec the guards have accepted becomes a plan. Never the other way round:
/// `render_backup` consumes a `BackupPlan` precisely so an unvalidated topic
/// list cannot reach a rendered document (`spec.rs`'s own note on
/// `BackupSpec`).
///
/// `backup_id` is passed in rather than read off the spec, because **I10**'s
/// `--backup-id-override` decides it and this is the one place both the plan
/// and the outcome take it from.
pub fn build_plan(spec: &BackupSpec, backup_id: &str) -> BackupPlan {
    BackupPlan {
        backup_id: backup_id.to_string(),
        source_bootstrap: spec.source.bootstrap_servers.clone(),
        source_auth: source_auth_render(&spec.source.auth),
        topics: spec.source.topics.clone(),
        storage: spec.storage.clone(),
        compression: spec.backup.compression.clone(),
        segment_max_records: spec.backup.segment_max_records,
        segment_max_bytes: spec.backup.segment_max_bytes,
        max_concurrent_partitions: spec.backup.max_concurrent_partitions,
    }
}

/// The parsed inputs, read from the two file arguments. Local I/O only — no
/// socket, no bucket.
struct Inputs {
    spec: BackupSpec,
    spec_text: String,
    allowed: AllowedClusters,
}

fn read_inputs(args: &BackupRunArgs) -> Result<Inputs, BackupError> {
    let spec_text = std::fs::read_to_string(&args.spec)
        .map_err(|e| BackupError::Operational(format!("{}: {e}", args.spec.display())))?;
    let spec: BackupSpec = serde_yaml::from_str(&spec_text)
        .map_err(|e| BackupError::Operational(format!("backup spec does not parse: {e}")))?;
    let allowed_text = std::fs::read_to_string(&args.allowed_clusters).map_err(|e| {
        BackupError::Operational(format!("{}: {e}", args.allowed_clusters.display()))
    })?;
    let allowed: AllowedClusters = serde_json::from_str(&allowed_text)
        .map_err(|e| BackupError::Operational(format!("allowed-clusters does not parse: {e}")))?;
    Ok(Inputs {
        spec,
        spec_text,
        allowed,
    })
}

/// The TESTABLE seam's inner half: everything one `backup run` does, over
/// handles it does not build, returning the OUTCOME rather than an exit code.
///
/// `run_with` below is the `ExitCode`-returning wrapper. Both are the seam and
/// both are public on purpose: a refusal test asserts an `ExitCode` and calls
/// `run_with`; an outcome test asserts a `BackupOutcome` FIELD (the source
/// cluster id, the overridden backup id, the recorded auth) and has to be able
/// to see the value, which an `ExitCode` cannot carry. Neither of them names a
/// constructor.
pub fn execute_with(
    args: &BackupRunArgs,
    run_id: &str,
    reader: &dyn ClusterReader,
    engine: &dyn DataEngine,
    store: &Store,
) -> Result<BackupOutcome, BackupError> {
    let inputs = read_inputs(args)?;
    // Phase −1. EVERY guard, before the engine and before any document is
    // written. The reader is already built by the time this function is
    // called — `run` builds it — but the refusals that do not need it come
    // first inside `phase_minus1_admit::run`, so a locally-refusable plan
    // never reaches a metadata call. **Interface I6's refusal is one of
    // them**: it is `local`'s step 4 (review F-3), so `--out`/`--receipt-out`
    // is answered with no I/O of any kind rather than after a 20 s metadata
    // timeout against the source cluster.
    let admitted = phase_minus1_admit::run(
        args,
        &inputs.spec,
        &inputs.spec_text,
        &inputs.allowed,
        reader,
    )?;

    // **I10.** The derived id is the spec's own `backup_id`; the override
    // replaces it in the plan AND in the outcome, from this one binding.
    let backup_id = args
        .backup_id_override
        .clone()
        .unwrap_or_else(|| inputs.spec.backup_id.clone());
    let plan = build_plan(&inputs.spec, &backup_id);
    let mut obs = crate::metrics::PhaseLogger::new(run_id);
    let ran = phase_run::run(&plan, engine, store, &mut obs)?;

    Ok(BackupOutcome {
        backup_id,
        run_id: run_id.to_string(),
        source_cluster_id: admitted.source_cluster_id,
        // From the SPEC, by the explicit match — the only field here that is
        // not measured, because there is nothing to measure it against: a
        // broker does not report which mechanism a client chose.
        source_auth: source_auth_render(&inputs.spec.source.auth),
        manifest_key: ran.manifest_key,
        manifest_sha256: ran.manifest_sha256,
        records_per_topic: ran.records_per_topic,
        covered_from_ms: ran.covered_from_ms,
        covered_to_ms: ran.covered_to_ms,
        facts: ran.facts,
    })
}

/// The TESTABLE seam. Every named test in Tasks 4, 5b and 6 calls this (or
/// `execute_with`, its outcome-returning half), in process, with doubles.
/// **Nothing here constructs a client.**
pub fn run_with(
    args: &BackupRunArgs,
    reader: &dyn ClusterReader,
    engine: &dyn DataEngine,
    store: &Store,
) -> ExitCode {
    let run_id = crate::ids::new_run_id();
    let outcome = execute_with(args, &run_id, reader, engine, store);
    report(&run_id, outcome)
}

/// Everything a terminal path owes the outside world. Split from `run_with`
/// for the same reason `drill::report` is split from `drill::run`: the
/// exit-code decision is the product's primary output and must be reachable
/// from a test.
fn report(run_id: &str, outcome: Result<BackupOutcome, BackupError>) -> ExitCode {
    // [I9] The GuardRefusal's OWN message, not `BackupError`'s `Display`:
    // `logweir_core::guard::terminal_state` matches a PREFIX, and the wrapped
    // form (`guard: <message>`) would classify every state as the default
    // `GuardRefused`.
    let refusal_message: Option<String> = match &outcome {
        Err(BackupError::Guard(refusal)) => Some(refusal.0.clone()),
        _ => None,
    };
    // NO `ExitCode` literal but `Ok` — `BackupError::exit_code` is the single
    // place the GC11 contract lives for this command.
    let code = match &outcome {
        Ok(o) => {
            tracing::info!(
                run_id = %run_id,
                backup_id = %o.backup_id,
                source_cluster_id = %o.source_cluster_id,
                manifest_key = %o.manifest_key,
                "backup captured"
            );
            println!("{}", summary_line(o));
            ExitCode::Ok
        }
        Err(e) => {
            // `run_id` on the EVENT and not only on an entered span: a
            // single-line consumer reads the event object.
            tracing::error!(run_id = %run_id, error = %e, "backup failed");
            eprintln!("{e}");
            e.exit_code()
        }
    };
    exiting(run_id, code, refusal_message.as_deref())
}

/// One line on stdout so `backup run` is not silent, quoting only measured
/// values.
fn summary_line(o: &BackupOutcome) -> String {
    let records: u64 = o.records_per_topic.values().sum();
    format!(
        "backup {} captured {} record(s) across {} topic(s) from cluster {} — manifest {} {}",
        o.backup_id,
        records,
        o.records_per_topic.len(),
        o.source_cluster_id,
        o.manifest_key,
        o.manifest_sha256
    )
}

/// Logs the exit code and what it means, then returns it unchanged — and, for
/// exit 3 ONLY, prints `refusal-reason=<TerminalState>` as the process's FINAL
/// stdout line (**I9**, Task 3's contract).
///
/// A backup-specific twin of `drill::exiting` rather than a call into it: the
/// `meaning` strings are the operator-facing description of what happened, and
/// "the drill passed" / "a drill ran and did not pass" are false on this
/// command. `drill::exiting` is also inside `drill/mod.rs`'s emitter-closure
/// gate region, which this task must leave untouched.
fn exiting(run_id: &str, code: ExitCode, refusal_message: Option<&str>) -> ExitCode {
    let meaning = match code {
        ExitCode::Ok => "the backup completed and its archive was read back",
        ExitCode::Operational => "logweir could not take the backup; NO receipt was written",
        // Unreachable from this command in tag 1 — see `BackupError::exit_code`.
        // Named rather than folded into a catch-all so that a future variant
        // reaching either code gets a truthful line instead of the wrong one.
        ExitCode::DrillNotPass => "a result that is not a pass (not produced by backup run)",
        ExitCode::GuardRefused => {
            "the plan was refused before anything ran; no archive was written"
        }
        ExitCode::SigningOrLock => "the run's result is unattested; nothing uploaded",
    };
    tracing::info!(run_id = %run_id, exit_code = code as u8 as i64, meaning, "backup finished");
    // [I9] AFTER the tracing line, so the reason is the LAST thing on stdout.
    // `unwrap_or("")` is the fail-safe direction: an empty message classifies
    // as `GuardRefused`, so a future path reaching exit 3 without one still
    // satisfies GC11 rather than printing nothing.
    if code == ExitCode::GuardRefused {
        crate::exit::print_refusal_reason(refusal_message.unwrap_or(""));
    }
    code
}

/// The log filter `backup run` installs when `RUST_LOG` is unset or blank.
///
/// A bare `"info"`, and NOT `drill::DEFAULT_LOG_DIRECTIVE`'s narrowed form,
/// for two reasons. (a) That constant is the drill's per-line `run_id`
/// guarantee — it pins every third-party emitter to `warn` because an INFO
/// event from a tokio worker thread cannot carry the thread-local `drill`
/// span; the backup path makes no such published claim in tag 1, and Task 5b
/// owns its log contract. (b) Pinning `rdkafka=warn` here would suppress
/// exactly the lines `backup_run_refuses_without_opening_a_socket` asserts are
/// ABSENT — a filter that hides the evidence a test looks for turns that test
/// into a check that cannot fail.
const DEFAULT_LOG_DIRECTIVE: &str = "info";

/// The thin wrapper `main.rs` calls: constructs the three real handles and
/// delegates. **This is the ONLY function in `crates/logweir/src/backup/` that
/// names `RdKafkaReader::connect(` or `Store::read_only_from_url(`** —
/// `only_the_wrapper_constructs_a_client` reads this directory's source and
/// asserts it.
pub fn run(args: &BackupRunArgs) -> ExitCode {
    let run_id = crate::ids::new_run_id();
    // `try_init`, not `init`: `init` PANICS when a global subscriber is
    // already installed, and this is a library entry point an embedder may
    // call more than once. `EnvFilter::new` is `parse_lossy`, so a malformed
    // RUST_LOG is dropped with a note on stderr and never unwrapped into a
    // panic. A BLANK `RUST_LOG` is treated as unset — `env::var` returns
    // `Ok("")` for `env: - name: RUST_LOG` with an empty `value:`, a
    // Kubernetes-manifest reality, and that parses to the empty directive set
    // rather than to an error.
    let filter = match std::env::var("RUST_LOG") {
        Ok(v) if !v.trim().is_empty() => tracing_subscriber::EnvFilter::new(v),
        _ => tracing_subscriber::EnvFilter::new(DEFAULT_LOG_DIRECTIVE),
    };
    let _ = tracing_subscriber::fmt()
        .json()
        .with_current_span(true)
        .with_env_filter(filter)
        .try_init();
    let _span = tracing::info_span!("backup", run_id = %run_id).entered();

    // The two file arguments are read TWICE on the success path — once here to
    // learn where to point the two handles, once inside `execute_with` to
    // guard the exact bytes. That is deliberate and cheap: `execute_with` must
    // own the bytes it scans (a second parse feeding a phase would be a second
    // source of truth), and this function must know `spec.source.bootstrap_servers`
    // and `spec.storage` before it can build anything at all.
    let inputs = match read_inputs(args) {
        Ok(i) => i,
        Err(e) => return report(&run_id, Err(e)),
    };

    // **EVERY GUARD THAT CAN REFUSE LOCALLY, BEFORE A CLIENT EXISTS.**
    //
    // `RdKafkaReader::connect` builds two librdkafka handles whose broker
    // threads start dialling the bootstrap list immediately, so constructing
    // the reader first — which is what `drill::context` still does, and which
    // is recorded as a carried finding rather than fixed here — means a plan
    // refused by a purely local guard has already opened a socket to the
    // source cluster. On the BACKUP path that is the source cluster, i.e. the
    // production one, which is the last cluster a refused plan should touch.
    //
    // `execute_with` calls `phase_minus1_admit::run`, which runs these same
    // local checks again before its network step. They are pure string and
    // list predicates over data already in memory (microseconds), and running
    // them twice is what lets BOTH entry points be correct on their own: the
    // seam is complete without this call, and this call makes the wrapper
    // ordering observable — `backup_run_refuses_without_opening_a_socket` and
    // `a_receipt_flag_is_refused_without_opening_a_socket` each assert a
    // refusal emits no rdkafka line at all.
    //
    // `args` is passed because `local`'s step 4 is interface I6's refusal
    // (review F-3): `--out`/`--receipt-out` is a flag this build cannot
    // honour, knowable with zero I/O, and it must not cost a metadata
    // timeout against the SOURCE cluster to say so.
    if let Err(e) = phase_minus1_admit::local(args, &inputs.spec, &inputs.spec_text) {
        return report(&run_id, Err(e));
    }

    // NO SCRATCH NAMESPACE IS SET ON THIS READER, ever — GC18(c) rail 2. An
    // unscoped reader can delete nothing at all: the deleting method refuses
    // every name it is handed until a scratch namespace has been configured
    // (`crates/logweir-kafka/src/rdkafka_reader.rs:446-460`). On the BACKUP
    // path that is not a degraded mode, it IS the requirement — a backup that
    // could delete a topic on the source cluster is the one thing a backup
    // must never be able to do. (Identifiers omitted on purpose; see the
    // module doc's rail 2.)
    //
    // **Interface I1**, the third of the three construction sites. It used to
    // hard-code the plaintext arm and say that Logweir's own source-cluster
    // client got its SASL wiring in Task 6; this is that wiring.
    //
    // **The whole of this happens AFTER `phase_minus1_admit::local` and
    // BEFORE `RdKafkaReader::connect`**, which is what makes the two exit
    // codes mean what GC11 says. `validated_password` refuses an unrenderable
    // projected value with a `GuardRefusal` — exit 3,
    // `refusal-reason=CredentialNotRenderable`, before any librdkafka handle
    // exists and therefore before a single packet reaches the SOURCE cluster,
    // i.e. the production one. `from_spec` reports an ABSENT variable under
    // `mode: scramSha512` as a `KafkaError::Client` — exit 1, operational,
    // because nothing was refused and the fix is to project the Secret.
    //
    // Logweir's client and the engine's client authenticate as the SAME
    // principal from the SAME spec field: this call and
    // `render_backup`'s `security:` block both read `spec.source.auth`, and
    // the password both use is the one value in `$LOGWEIR_SOURCE_PASSWORD` —
    // Logweir reads it here, the engine expands it out of the environment it
    // inherits.
    let source_auth = match logweir_kafka::reader::AuthConfig::from_spec(
        &inputs.spec.source.auth,
        match crate::drill::validated_password(crate::drill::SOURCE_PASSWORD_VAR) {
            Ok(p) => p,
            Err(refusal) => return report(&run_id, Err(refusal.into())),
        },
    ) {
        Ok(a) => a,
        Err(e) => {
            return report(
                &run_id,
                Err(
                    crate::drill::naming_the_password_var(e, crate::drill::SOURCE_PASSWORD_VAR)
                        .into(),
                ),
            )
        }
    };
    let reader = match logweir_kafka::rdkafka_reader::RdKafkaReader::connect(
        &inputs.spec.source.bootstrap_servers,
        source_auth,
    ) {
        Ok(r) => r,
        Err(e) => return report(&run_id, Err(e.into())),
    };

    // READ-ONLY over the archive (Global Constraint 6): the handle physically
    // cannot put, and `Store::from_url` would refuse to build over an archive
    // prefix at all, since an archive is never under `logweir/`. Two handles,
    // because `Store` is not `Clone` and `OsoCliEngine` takes ownership of the
    // one it reads through while `phase_run` reads the manifest bytes through
    // the other. Both are read-only.
    let store = match Store::read_only_from_url(&inputs.spec.storage) {
        Ok(s) => s,
        Err(e) => return report(&run_id, Err(BackupError::Operational(e.to_string()))),
    };
    let engine_archive = match Store::read_only_from_url(&inputs.spec.storage) {
        Ok(s) => s,
        Err(e) => return report(&run_id, Err(BackupError::Operational(e.to_string()))),
    };

    let engine = match build_engine(engine_archive) {
        Ok(e) => e,
        Err(e) => return report(&run_id, Err(e)),
    };

    let outcome = execute_with(args, &run_id, &reader, &engine, &store);
    report(&run_id, outcome)
}

/// The engine handle, built exactly as `drill::context` builds it: the ONE
/// resolution `doctor` and both run commands consult
/// (`crate::engine_bin::engine_path`), the identity read from the environment
/// the image sets, and a pod-local workdir for the rendered document.
///
/// The identity is NOT refused when empty, unlike the drill's
/// `assert_engine_identity`: that refusal exists because an empty
/// `engine.version`/`engine.digest` would enter a SIGNED scorecard naming no
/// engine, and this task signs nothing. Task 5b, which does, owns that check
/// for the receipt.
fn build_engine(archive: Store) -> Result<logweir_engine_oso::engine::OsoCliEngine, BackupError> {
    let binary = crate::engine_bin::engine_path();
    let version = std::env::var("LOGWEIR_ENGINE_VERSION").unwrap_or_default();
    let digest = std::env::var("LOGWEIR_ENGINE_DIGEST").unwrap_or_default();
    let workdir = std::env::temp_dir().join(format!("logweir-{}", std::process::id()));
    std::fs::create_dir_all(&workdir)
        .map_err(|e| BackupError::Operational(format!("{}: {e}", workdir.display())))?;
    Ok(logweir_engine_oso::engine::OsoCliEngine::new(
        binary, version, digest, workdir, archive,
    ))
}
