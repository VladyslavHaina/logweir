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
use std::path::{Path, PathBuf};

/// `logweir backup run`'s flag set. **I10** (`--backup-id-override`) is this
/// task's; Task 18's `BackupSchedule` reconciler only passes it.
pub struct BackupRunArgs {
    /// **D2 §3.5's store-contract handshake.** `Some("1")` is a
    /// destination-backed Job: every store setting arrives explicitly and the
    /// ambient environment contributes no location, no addressing and no
    /// transport. `None` is a legacy or standalone invocation, unchanged.
    ///
    /// An OLDER `logweir` binary does not know this flag at all and exits on
    /// the clap parse error, which is the point: a new controller can never
    /// drive an old runner into building its stores out of whatever `AWS_*`
    /// happens to be in the pod.
    pub store_contract_version: Option<String>,
    pub spec: PathBuf,
    pub allowed_clusters: PathBuf,
    /// The key the backup receipt is signed with (**I6**). Loaded, exercised
    /// and self-verified after local admission but before the production
    /// runner constructs any client; the same parsed key is retained through
    /// receipt signing, so an execution never reopens rotated material after
    /// the engine has produced an archive.
    pub signing_key: PathBuf,
    /// Copied into `BackupReceipt.triggered_by` verbatim. Absent becomes the
    /// empty string: the field is required in the document (a receipt says
    /// what triggered it or says nothing, never `null`), and an operator who
    /// passes no `--triggered-by` has said nothing rather than said "".
    pub triggered_by: Option<String>,
    /// Where the receipt is written locally (**I6**), with its DSSE sidecar
    /// beside it under the extension `.sig`. `receipt_out` takes precedence
    /// when both are given; two different paths for the one document this
    /// command writes is refused locally, before anything runs.
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
    /// When the run was requested — read ONCE, at the top of `execute_with`,
    /// and carried from there into `BackupReceipt.requested_at`. Not measured
    /// inside the receipt builder, which is a pure projection of this struct.
    pub requested_at: chrono::DateTime<chrono::Utc>,
    /// `--triggered-by`, or `""` when the operator passed none.
    pub triggered_by: String,
    pub source_cluster_id: String,
    /// The bootstrap list the plan named, for `BackupReceipt.source`.
    pub bootstrap_servers: Vec<String>,
    /// The named topic allowlist (GC18(c) rail 1: named, never a pattern).
    pub topics: Vec<String>,
    /// The engine that took the backup — id, version and DIGEST (GC7 pins by
    /// digest and never by tag, and a receipt naming only a version would be
    /// satisfied by any binary claiming it).
    pub engine: logweir_core::engine::EngineId,
    /// The archive prefix everything the engine wrote lives under.
    pub archive_prefix: String,
    /// WHERE the archive is — the spec's own `storage` location, carried
    /// whole.
    ///
    /// Added for PLAT-15.1: the catalog point record publishes an
    /// `archive.location_id` (`s3://<bucket>/<prefix>`), and `archive_prefix`
    /// above is only half of that — a prefix with no bucket names no place. It
    /// is the value `execute_with` was already given, copied rather than
    /// re-derived, so the record and the plan cannot disagree about which
    /// bucket the archive is in.
    pub storage: logweir_core::engine::StorageUrl,
    /// Filled from `spec.source.auth`; `phase_run::build_receipt` renders it
    /// into `BackupReceipt.source.auth`.
    pub source_auth: AuthRender,
    pub manifest_key: String,
    /// `"sha256:<hex>"`, over the exact manifest bytes this run READ back.
    pub manifest_sha256: String,
    pub records_per_topic: BTreeMap<String, u64>,
    pub covered_from_ms: i64,
    /// **EXCLUSIVE** (interface I22) — see `phase_run::Ran::covered_to_ms`,
    /// which is where the manifest's inclusive bound is converted.
    pub covered_to_ms: i64,
    pub facts: BackupFacts,
    /// `logweir/backups/<backup_id>/<run_id>.receipt.json` (**GC6**), the key
    /// the receipt was PUT to. Printed as the runner's penultimate stdout line
    /// (**I7**) and read by Task 17's reconciler.
    pub receipt_key: String,
    /// `logweir/backups/<backup_id>/<run_id>.receipt.sig`. The runner's FINAL
    /// stdout line (**I7**).
    pub sidecar_key: String,
    /// Digest of the exact signed receipt bytes. This is a capture fact, not a
    /// verification verdict, and is printed for a credential-isolated
    /// controller that cannot fetch those bytes itself.
    pub receipt_sha256: String,
    /// `logweir/catalog/v1/points/<pointId>/record.json` when the recovery
    /// catalog point was written, `None` when the catalog write failed
    /// (PLAT-15.1, D3 §5.2).
    ///
    /// `None` is NOT a failure of the run: `phase_run::write_catalog_point`
    /// warns and returns it, because the archive and its signed receipt are
    /// already in the bucket by then. Printed as the CONDITIONAL
    /// `catalog-key=` line — see `exiting` for why it comes BEFORE interface
    /// I7's two lines and not after them.
    pub catalog_key: Option<String>,
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
    /// The signing prerequisite failed, or the receipt could not be
    /// validated, signed or uploaded. **Exit 4**
    /// (Global Constraint 11: "signing or lock-proof failed, nothing
    /// uploaded"), and Task 5b is the task that makes this variant reachable —
    /// Task 4's `exit_code` comment said as much.
    ///
    /// It covers the whole atomic step, deliberately. A key that will not load
    /// or exercise is caught before the engine runs; a later receipt
    /// validation failure (Logweir measured a document its own reader
    /// refuses), a signature that will not compute, or a refused create-only
    /// put may leave an archive with no verifiable evidence. GC11 gives all of
    /// those states one code.
    #[error("signing: {0}")]
    Signing(String),
}

impl BackupError {
    /// Global Constraint 11, in ONE place — the `DrillError::exit_code`
    /// pattern (`crates/logweir/src/drill/mod.rs:87-113`). Exit 2 is
    /// unreachable from this command in tag 1: it means "a signed scorecard
    /// exists and does not pass", and a backup emits no scorecard. Exit 4 is
    /// reachable since Task 5b, through `Signing`, which is the only signing
    /// this command does.
    pub fn exit_code(&self) -> ExitCode {
        match self {
            BackupError::Guard(_) => ExitCode::GuardRefused, // 3
            BackupError::Signing(_) => ExitCode::SigningOrLock, // 4
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

/// [`build_plan`] with the projected private CA attached to the source auth
/// (PLAT-07.1), so the engine document carries `ssl_ca_location`.
///
/// `None` is `build_plan` exactly, byte for byte in the rendered document.
///
/// # Errors
///
/// `BackupError::Operational` when a CA is supplied for a source whose auth is
/// not SCRAM over TLS — `AuthRender::with_tls_ca_file`'s refusal. Exit 1: the
/// controller never projects that shape, so it is a hand-built Job or a spec
/// edited against its connection, and nothing was dialled.
pub fn build_plan_with_tls_ca(
    spec: &BackupSpec,
    backup_id: &str,
    tls_ca_file: Option<String>,
) -> Result<BackupPlan, BackupError> {
    let plan = build_plan(spec, backup_id);
    let source_auth = plan
        .source_auth
        .clone()
        .with_tls_ca_file(tls_ca_file)
        .map_err(|e| BackupError::Operational(format!("source.auth: {e}")))?;
    Ok(BackupPlan {
        source_auth,
        ..plan
    })
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
/// cluster id, the overridden backup id, the recorded auth, the two evidence
/// keys) and has to be able to see the value, which an `ExitCode` cannot
/// carry. Neither of them names a constructor.
///
/// # TWO store handles, and why they are not one (Global Constraint 6)
///
/// `store` is the ARCHIVE — read-only on the live path
/// (`Store::read_only_from_url`), so it physically cannot put, which is what
/// makes "a backup never writes to the archive" a property of the handle
/// rather than of this code's good behaviour. `evidence` is the writable
/// handle over the `logweir/` root, and it is the ONLY thing this command
/// writes through. Collapsing them into one would mean a writable handle over
/// the adopter's archive prefix, which is the single outcome GC6 exists to
/// prevent.
///
/// `run_with` passes ONE handle for both, which is correct for the in-process
/// seam: `Store::in_memory("logweir/")` is the only writable store a test can
/// build without a backend, so an in-memory ARCHIVE fixture is seeded under
/// that root too (see `crates/logweir/tests/backup_run.rs`'s
/// `ARCHIVE_PREFIX`). Nothing in production reads or writes an archive there.
/// D3 §2.4's five named backup steps, as a closed list.
///
/// `-1` and named steps rather than numbers because the backup path genuinely
/// has no numbered phases after admission: inventing `0..4` here would put two
/// unrelated numbering schemes on one channel, and a controller reading
/// `progress-phase=2:` would have no way to know which runner it came from.
pub const PROGRESS_STEP_ADMIT: &str = "admit";
pub const PROGRESS_STEP_ENGINE: &str = "engine";
pub const PROGRESS_STEP_READBACK: &str = "readback";
pub const PROGRESS_STEP_SIGN: &str = "sign";
pub const PROGRESS_STEP_UPLOAD: &str = "upload";
/// Every step this command announces, in the order it announces them.
pub const PROGRESS_STEPS: [&str; 5] = [
    PROGRESS_STEP_ADMIT,
    PROGRESS_STEP_ENGINE,
    PROGRESS_STEP_READBACK,
    PROGRESS_STEP_SIGN,
    PROGRESS_STEP_UPLOAD,
];

/// One `progress-phase=-1:<step>` line, through the pure filter that decides
/// whether it may be said at all — see `drill::print_progress_phase` for why
/// the channel is a filter and not a formatter.
pub fn print_progress_step(step: &str) {
    crate::drill::print_progress_phase(-1, step);
}

pub fn execute_with(
    args: &BackupRunArgs,
    run_id: &str,
    reader: &dyn ClusterReader,
    engine: &dyn DataEngine,
    store: &Store,
    evidence: &Store,
) -> Result<BackupOutcome, BackupError> {
    execute_with_signer(args, run_id, reader, engine, store, evidence, None)
}

/// The common execution path. Production supplies the signer it validated
/// before constructing any runner clients; the in-process seam validates at
/// the same logical boundary, immediately before its first engine operation.
fn execute_with_signer(
    args: &BackupRunArgs,
    run_id: &str,
    reader: &dyn ClusterReader,
    engine: &dyn DataEngine,
    store: &Store,
    evidence: &Store,
    validated_signer: Option<&crate::signer::ValidatedSigner>,
) -> Result<BackupOutcome, BackupError> {
    // READ ONCE, and FIRST: `requested_at` is when the run was requested, so
    // it is measured before the guards rather than after them — a plan refused
    // at phase −1 took no time it should be credited with, and a receipt whose
    // `requested_at` were taken after the engine ran would understate the
    // elapsed time an auditor reads.
    let requested_at = chrono::Utc::now();
    // **D3 §2.4's progress channel.** The backup path has no numbered phases
    // after admission, so every line it emits is `progress-phase=-1:<step>`
    // over the four named steps §2.4 fixes (`engine`, `readback`, `sign`,
    // `upload`) plus this one. The channel version comes first, once: a Backup
    // Job carries no execution-contract environment on this build, so the only
    // true answer is the version this BINARY implements.
    println!(
        "{}",
        logweir_core::execution_contract::progress_contract_line(
            logweir_core::execution_contract::ContractVersion::V2
        )
    );
    print_progress_step(PROGRESS_STEP_ADMIT);
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

    // Resolve the signing prerequisite before making the first call on the
    // engine. `run` already did this before constructing its real clients and
    // passes that stable in-memory signer here; direct seam callers load it at
    // this same boundary. In both cases receipt persistence reuses the value
    // instead of reopening a file that may have disappeared or rotated after
    // the engine wrote archive data.
    let loaded_signer;
    let signer = match validated_signer {
        Some(signer) => signer,
        None => {
            loaded_signer = phase_run::load_signer(&args.signing_key)?;
            &loaded_signer
        }
    };

    // **I10.** The derived id is the spec's own `backup_id`; the override
    // replaces it in the plan AND in the outcome, from this one binding.
    let backup_id = args
        .backup_id_override
        .clone()
        .unwrap_or_else(|| inputs.spec.backup_id.clone());
    // PLAT-07.1: the engine's `ssl_ca_location`, from the same variable `run`
    // attached to the reader's `ssl.ca.location` (an environment read is stable
    // for the life of the process, so the two clients see one path). Unset, the
    // plan is exactly `build_plan`'s.
    let plan = build_plan_with_tls_ca(
        &inputs.spec,
        &backup_id,
        crate::tls_ca::projected_ca_file(crate::tls_ca::SOURCE_TLS_CA_FILE_VAR)
            .map_err(BackupError::Operational)?,
    )?;

    // **The engine identity, BEFORE the engine runs** (Task 4 left this to
    // Task 5b in as many words: "The identity is NOT refused when empty …
    // Task 5b, which does, owns that check for the receipt").
    //
    // An empty `engine.version`/`engine.digest` would enter a SIGNED receipt
    // naming no engine, and GC7 pins by digest precisely so that a receipt
    // says WHICH binary took the backup. Checked here, before the subprocess,
    // so a misconfigured image costs an operator nothing but a message —
    // refusing after the archive exists would leave an unattested backup
    // behind for a fact that was knowable from the environment.
    //
    // Exit 1 and the same wording discipline as the drill's
    // `assert_engine_identity`: nothing about the PLAN was found wanting, so
    // GC11's exit 3 does not describe it.
    let engine_id = engine.id();
    for (field, value, var) in [
        (
            "engine.version",
            &engine_id.version,
            "LOGWEIR_ENGINE_VERSION",
        ),
        ("engine.digest", &engine_id.digest, "LOGWEIR_ENGINE_DIGEST"),
    ] {
        if value.trim().is_empty() {
            return Err(BackupError::Operational(format!(
                "{field} is empty; a signed backup receipt must name the engine image it                  ran. Set {var} to the value of the digest-pinned image this binary was                  extracted from. NO backup was taken: this is refused before the engine is                  spawned."
            )));
        }
    }

    let mut obs = crate::metrics::PhaseLogger::new(run_id);
    let ran = phase_run::run(&plan, engine, store, &mut obs)?;

    let mut outcome = BackupOutcome {
        backup_id,
        run_id: run_id.to_string(),
        requested_at,
        triggered_by: args.triggered_by.clone().unwrap_or_default(),
        source_cluster_id: admitted.source_cluster_id,
        bootstrap_servers: inputs.spec.source.bootstrap_servers.clone(),
        topics: inputs.spec.source.topics.clone(),
        engine: engine_id,
        archive_prefix: inputs.spec.storage.prefix().to_string(),
        storage: inputs.spec.storage.clone(),
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
        // Filled by `persist_receipt` below, from the one function that
        // derives them. Empty here for exactly as long as it takes to put the
        // two objects, and never observable empty: `execute_with` returns the
        // outcome only after the puts succeeded, and a failure is an `Err`.
        receipt_key: String::new(),
        sidecar_key: String::new(),
        receipt_sha256: String::new(),
        catalog_key: None,
    };

    // **I6 / I7 / GC6.** The archive exists; now it gets evidence. Validate,
    // sign, put both objects under `logweir/`, and write the local pair when
    // `--receipt-out` (or `--out`) asked for it.
    let persisted = phase_run::persist_receipt(&outcome, signer, receipt_out_path(args), evidence)?;
    outcome.receipt_key = persisted.receipt_key;
    outcome.sidecar_key = persisted.sidecar_key;
    outcome.receipt_sha256 = persisted.receipt_sha256;
    outcome.catalog_key = persisted.catalog_key;

    Ok(outcome)
}

/// The ONE local path the receipt is written to, or `None`.
///
/// `--receipt-out` wins over `--out`; the two naming DIFFERENT paths is
/// refused in phase −1's local step, before any I/O, because this command
/// writes exactly one document and an operator who named two paths for it has
/// asked for something that cannot happen. Both flags are honoured because
/// `logweir --help` documents both (`crates/logweir/src/cli.rs`).
pub fn receipt_out_path(args: &BackupRunArgs) -> Option<&Path> {
    args.receipt_out.as_deref().or(args.out.as_deref())
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
    // ONE handle for both roles — see `execute_with`'s doc comment. The
    // signature is deliberately unchanged from Task 4's: every named test in
    // Tasks 4, 5b and 6 calls this function, and an in-memory store is both
    // the archive fixture and the evidence bucket for all of them.
    let outcome = execute_with(args, &run_id, reader, engine, store, store);
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
    // **I7.** The two evidence keys, carried out of the outcome so `exiting`
    // can print them as the process's FINAL two stdout lines — for the reason
    // Task 3's I9 prints `refusal-reason=` there: `tracing_subscriber::fmt`
    // writes its JSON to STDOUT, so a line printed before the "backup
    // finished" event is not the last line of stdout, and the pod log API has
    // no stream selector for a controller to separate them with.
    let evidence_keys: Option<(String, String)> = match &outcome {
        Ok(o) => Some((o.receipt_key.clone(), o.sidecar_key.clone())),
        Err(_) => None,
    };
    // PLAT-15.1. Carried separately from the pair above because it is
    // CONDITIONAL: a run whose catalog write failed still has both evidence
    // keys, and folding the three into one value would make the optional one
    // look like part of interface I7's mandatory pair.
    let catalog_key: Option<String> = match &outcome {
        Ok(o) => o.catalog_key.clone(),
        Err(_) => None,
    };
    let receipt_sha256 = outcome.as_ref().ok().map(|o| o.receipt_sha256.clone());
    let code = match &outcome {
        Ok(o) => {
            tracing::info!(
                run_id = %run_id,
                backup_id = %o.backup_id,
                source_cluster_id = %o.source_cluster_id,
                manifest_key = %o.manifest_key,
                receipt_key = %o.receipt_key,
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
    exiting(
        run_id,
        code,
        refusal_message.as_deref(),
        evidence_keys,
        catalog_key,
        receipt_sha256,
    )
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
fn exiting(
    run_id: &str,
    code: ExitCode,
    refusal_message: Option<&str>,
    evidence_keys: Option<(String, String)>,
    catalog_key: Option<String>,
    receipt_sha256: Option<String>,
) -> ExitCode {
    let meaning = match code {
        ExitCode::Ok => {
            "the backup completed, its archive was read back, and the receipt is signed and \
             uploaded"
        }
        // TRUE ON EVERY PATH THAT REACHES IT (Task 4's carried finding). The
        // old wording was "logweir could not take the backup; NO receipt was
        // written", and half of that is not knowable here: exit 1 is also
        // reached AFTER the engine ran — a backup set that is not in the
        // archive, a manifest that bounds no window, a `--receipt-out` path
        // that could not be written once the evidence was already uploaded. So
        // the line says exactly what this code does establish: no receipt was
        // written LOCALLY by this run's flags, and an archive may exist. The
        // receipt's own absence or presence is the bucket's answer, and the
        // two evidence keys are printed only on the success path.
        ExitCode::Operational => {
            "logweir could not complete the backup; an archive may exist and the two evidence \
             keys were not printed, so treat this run as unattested until the bucket says \
             otherwise"
        }
        // Unreachable from this command in tag 1 — see `BackupError::exit_code`.
        // Named rather than folded into a catch-all so that a future variant
        // reaching either code gets a truthful line instead of the wrong one.
        ExitCode::DrillNotPass => "a result that is not a pass (not produced by backup run)",
        ExitCode::GuardRefused => {
            "the plan was refused before anything ran; no archive was written"
        }
        // Reachable since Task 5b, through `BackupError::Signing`. "Nothing
        // uploaded" is about the EVIDENCE and is exact: early signer
        // validation and the later validate → sign → put order both precede
        // every evidence write. The archive may exist for failures discovered
        // after engine work; a signing-prerequisite error explicitly states
        // that no engine data operation started.
        ExitCode::SigningOrLock => {
            "the backup's result is unattested: no receipt was signed or uploaded, though the \
             archive may exist"
        }
    };
    tracing::info!(run_id = %run_id, exit_code = code as u8 as i64, meaning, "backup finished");
    // [I9] AFTER the tracing line, so the reason is the LAST thing on stdout.
    // `unwrap_or("")` is the fail-safe direction: an empty message classifies
    // as `GuardRefused`, so a future path reaching exit 3 without one still
    // satisfies GC11 rather than printing nothing.
    if code == ExitCode::GuardRefused {
        crate::exit::print_refusal_reason(refusal_message.unwrap_or(""));
    }
    // **I7, and the ORDER is the contract.** `receipt-key=` then
    // `sidecar-key=`, as the FINAL two stdout lines of a successful run, with
    // nothing after them — Task 20 reads exactly this and a controller cannot
    // tell stdout from stderr through the pod log API. Printed only on exit 0:
    // on any other code there is no pair of keys to name, and a line naming a
    // key nothing was written to would be the worst possible output.
    // **PLAT-15.1's conditional line, and it comes BEFORE I7's pair.**
    //
    // I7's contract is "`receipt-key=` then `sidecar-key=`, as the FINAL two
    // stdout lines of a successful run, WITH NOTHING AFTER THEM", and two
    // landed tests read it that way — `crates/logweir/tests/backup_run.rs`'s
    // I7 row takes the last two lines, and `e2e/tests/backup_argv.rs` asserts
    // the final stdout line starts `sidecar-key=`. A new line after them would
    // break a published interface for the sake of a metadata index, so the
    // optional one goes in front: a reader looking for `catalog-key=` finds it
    // by PREFIX (which is how every controller reads these lines,
    // `weirkeeper::controllers::backup::SIDECAR_KEY_PREFIX`), and the two
    // mandatory lines stay exactly where they were.
    //
    // ABSENT when the catalog write failed, and that is the point of it being
    // conditional: a line naming a key nothing was written to would be the
    // worst possible output. The warning on the log says what to do instead.
    if code == ExitCode::Ok {
        if let Some(catalog_key) = catalog_key {
            println!("catalog-key={catalog_key}");
        }
        if let Some(receipt_sha256) = receipt_sha256 {
            println!("receipt-sha256={receipt_sha256}");
        }
    }
    if let (ExitCode::Ok, Some((receipt_key, sidecar_key))) = (code, evidence_keys) {
        println!("receipt-key={receipt_key}");
        println!("sidecar-key={sidecar_key}");
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

    // THE STORE CONTRACT, BEFORE ANYTHING IS READ OR DIALLED — D2 §3.5. A
    // version this build does not implement is exit 3 here, with no spec read,
    // no signing key opened and no socket to the source cluster, for the reason
    // `phase_minus1_admit::local` runs where it does: a run that cannot be
    // executed as described must not touch the production cluster to say so.
    let contract = match store_contract::admit(args.store_contract_version.as_deref()) {
        Ok(contract) => contract,
        Err(refusal) => return report(&run_id, Err(refusal.into())),
    };

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

    // Signing is a prerequisite for starting a backup, not a postcondition
    // checked after the engine has written an archive. Load, exercise and
    // self-verify it before constructing the Kafka reader, archive stores or
    // engine runner, then keep this exact parsed key for receipt persistence.
    let signer = match phase_run::load_signer(&args.signing_key) {
        Ok(signer) => signer,
        Err(e) => return report(&run_id, Err(e)),
    };

    // === THE THREE OBJECT STORES, BEFORE ANY KAFKA CLIENT EXISTS ===
    //
    // **MOVED IN FRONT OF `RdKafkaReader::connect` DELIBERATELY**, for this
    // function's own stated rule: *every guard that can refuse locally, before
    // a client exists*. Building a store is a purely local act — a location
    // parsed, a credential provider chosen, a CA file read — and under the
    // store contract it is also a REFUSAL point: a run whose store
    // configuration is not what the contract describes exits 3 here. Below the
    // reader, that refusal came after librdkafka's broker threads had already
    // begun dialling the SOURCE cluster, i.e. the production one, which is the
    // last cluster a refused run should touch. It is the same argument
    // `phase_minus1_admit::local` and the pinned-approver guard are placed by.
    //
    // It is also what makes the contract END-TO-END testable without a broker:
    // `weirkeeper::tests::schedule_controller` drives this binary with the env
    // a rendered Job carries and asserts it gets PAST these constructors —
    // which is where a Job missing `LOGWEIR_EVIDENCE_CREDENTIALS` used to exit
    // 3 with the archive already open and nothing archived.
    //
    // READ-ONLY over the archive (Global Constraint 6): the handle physically
    // cannot put, and `Store::from_url` would refuse to build over an archive
    // prefix at all, since an archive is never under `logweir/`. Two handles,
    // because `Store` is not `Clone` and `OsoCliEngine` takes ownership of the
    // one it reads through while `phase_run` reads the manifest bytes through
    // the other. Both are read-only.
    //
    // UNDER THE STORE CONTRACT, EXPLICITLY (D2 §3.5). `read_only_with` takes
    // the credential provider the controller NAMED and the CA it projected;
    // location, region, endpoint, addressing and transport come from the plan's
    // own `storage` block at both arities. `read_only_from_url` is the legacy
    // constructor and still honours `AWS_ENDPOINT_URL` and friends — which is
    // exactly why a destination-backed run does not use it.
    let (store, engine_archive) = if contract {
        let options = match store_contract::archive_options() {
            Ok(options) => options,
            Err(refusal) => return report(&run_id, Err(refusal.into())),
        };
        match (
            Store::read_only_with(&inputs.spec.storage, &options),
            Store::read_only_with(&inputs.spec.storage, &options),
        ) {
            (Ok(a), Ok(b)) => (a, b),
            (Err(e), _) | (_, Err(e)) => {
                return report(&run_id, Err(store_error(e)));
            }
        }
    } else {
        match (
            Store::read_only_from_url(&inputs.spec.storage),
            Store::read_only_from_url(&inputs.spec.storage),
        ) {
            (Ok(a), Ok(b)) => (a, b),
            (Err(e), _) | (_, Err(e)) => {
                return report(&run_id, Err(BackupError::Operational(e.to_string())))
            }
        }
    };

    // THE EVIDENCE HANDLE — the only writable store this command holds, and
    // the only one it puts through (**GC6**).
    //
    // It is derived from the archive's own location with the prefix replaced
    // by `logweir/`, because `Backup.spec` (`config/crd/backups.yaml`) carries
    // ONE object-store URL — the archive root — and `BackupSpec` mirrors it.
    // There is no second bucket to name, and inventing a spec field for one
    // would be a format change this task has no mandate for. `Store::from_url`
    // refuses any evidence prefix that is not exactly `logweir/`
    // (`crates/logweir-store/src/lib.rs:194-205`), so the derivation cannot
    // point at the archive's own keys even by accident.
    let evidence_url = evidence_location(&inputs.spec.storage);
    let evidence = if contract {
        let archive_options = match store_contract::archive_options() {
            Ok(options) => options,
            Err(refusal) => return report(&run_id, Err(refusal.into())),
        };
        let options = match store_contract::evidence_options(&archive_options) {
            Ok(options) => options,
            Err(refusal) => return report(&run_id, Err(refusal.into())),
        };
        match Store::from_url_with(&evidence_url, &options) {
            Ok(s) => s,
            Err(e) => return report(&run_id, Err(store_error(e))),
        }
    } else {
        match Store::from_url(&evidence_url) {
            Ok(s) => s,
            Err(e) => return report(&run_id, Err(BackupError::Operational(e.to_string()))),
        }
    };

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
    // PLAT-07.1: the projected private CA, read ONCE, and handed to both TLS
    // clients — this reader's `ssl.ca.location` here, and the engine's
    // `ssl_ca_location` through `execute_with_signer` below.
    let source_tls_ca =
        match crate::tls_ca::projected_ca_file(crate::tls_ca::SOURCE_TLS_CA_FILE_VAR) {
            Ok(ca) => ca,
            Err(e) => return report(&run_id, Err(BackupError::Operational(e))),
        };
    let source_auth = match logweir_kafka::reader::AuthConfig::from_spec(
        &inputs.spec.source.auth,
        match crate::drill::validated_password(crate::drill::SOURCE_PASSWORD_VAR) {
            Ok(p) => p,
            Err(refusal) => return report(&run_id, Err(refusal.into())),
        },
    )
    .and_then(|auth| auth.with_tls_ca_file(source_tls_ca.clone()))
    {
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

    let engine = match build_engine(engine_archive) {
        Ok(e) => e,
        Err(e) => return report(&run_id, Err(e)),
    };

    let outcome = execute_with_signer(
        args,
        &run_id,
        &reader,
        &engine,
        &store,
        &evidence,
        Some(&signer),
    );
    report(&run_id, outcome)
}

// ---------------------------------------------------------------------------
// The store contract — D2 §3.5's runner half
// ---------------------------------------------------------------------------

/// How a destination-backed Job tells this runner what its stores are, and how
/// this runner refuses a contract it does not implement — D2 §3.5.
///
/// # THE NAMES ARE DECLARED TWICE ON PURPOSE
///
/// `weirkeeper::destination` declares the same constants. This crate must not
/// depend on `weirkeeper` — the dependency runs the other way, and the
/// `logweir` binary ships without a Kubernetes client at all — so the coupling
/// is two declarations pinned by a test that hands a controller-built argv to
/// this binary's real parser
/// (`weirkeeper::tests::schedule_controller::the_backup_runner_argv_is_one_the_cli_accepts`
/// and its destination-backed sibling). Erratum **E20** is why that shape is
/// used rather than a literal compared against a literal in the same
/// repository.
///
/// # What the contract actually promises
///
/// That EVERY store setting arrives explicitly: the location in the plan's own
/// `storage` block, the credential named by `LOGWEIR_*_CREDENTIALS`, the CA by
/// path. No `AmazonS3Builder::from_env()` sweep, no `AWS_ENDPOINT_URL`, no
/// ambient `AWS_ALLOW_HTTP` — defect **SEC-ENVHTTP**, closed on this side by
/// W2's `StoreOptions` and on the other side by the controller rendering the
/// complete set. The version handshake is what stops a NEW controller from
/// driving an OLD runner that would have improvised instead.
pub mod store_contract {
    use logweir_core::guard::GuardRefusal;
    use logweir_engine_oso::storage::{CredentialSource, StoreOptions};

    /// The one store-contract version this build implements.
    pub const VERSION: &str = "1";
    /// The argv flag the controller writes.
    pub const VERSION_ARG: &str = "--store-contract-version";
    /// The environment variable carrying the same value.
    pub const VERSION_ENV: &str = "LOGWEIR_STORE_CONTRACT_VERSION";
    /// Which provider the ARCHIVE store is built with.
    pub const ARCHIVE_CREDENTIALS_ENV: &str = "LOGWEIR_ARCHIVE_CREDENTIALS";
    /// Which provider the EVIDENCE store is built with.
    pub const EVIDENCE_CREDENTIALS_ENV: &str = "LOGWEIR_EVIDENCE_CREDENTIALS";
    /// `LOGWEIR_*_CREDENTIALS` for the three projected `AWS_*` variables.
    pub const CREDENTIALS_STATIC: &str = "static";
    /// `LOGWEIR_*_CREDENTIALS` for an injected workload identity.
    pub const CREDENTIALS_WORKLOAD_IDENTITY: &str = "workloadIdentity";
    /// `LOGWEIR_EVIDENCE_CREDENTIALS` when the evidence grant IS the archive's.
    pub const CREDENTIALS_ARCHIVE: &str = "archive";
    /// The archive store's extra trust root, as a path into the plan mount.
    pub const ARCHIVE_CA_FILE_ENV: &str = "LOGWEIR_ARCHIVE_CA_FILE";
    /// The evidence store's twin.
    pub const EVIDENCE_CA_FILE_ENV: &str = "LOGWEIR_EVIDENCE_CA_FILE";
    /// The three variables a `static` archive grant projects.
    pub const EVIDENCE_ACCESS_KEY_ID_ENV: &str = "LOGWEIR_EVIDENCE_AWS_ACCESS_KEY_ID";
    /// See [`EVIDENCE_ACCESS_KEY_ID_ENV`].
    pub const EVIDENCE_SECRET_ACCESS_KEY_ENV: &str = "LOGWEIR_EVIDENCE_AWS_SECRET_ACCESS_KEY";
    /// See [`EVIDENCE_ACCESS_KEY_ID_ENV`].
    pub const EVIDENCE_SESSION_TOKEN_ENV: &str = "LOGWEIR_EVIDENCE_AWS_SESSION_TOKEN";

    /// Whether this invocation is driven under the store contract, and whether
    /// this build implements the version it was handed.
    ///
    /// # THREE ANSWERS, AND THE MIDDLE ONE IS THE POINT
    ///
    /// * `Ok(false)` — no flag, no variable: a legacy or standalone
    ///   invocation. Stores are built exactly as they were before destinations
    ///   existed, from the plan's `storage` block and the ambient environment.
    /// * `Ok(true)` — the flag and the variable both say `1`.
    /// * `Err` — anything else. A version this build does not implement is
    ///   REFUSED (exit 3) rather than approximated: the whole reason the
    ///   handshake exists is that a runner improvising its store configuration
    ///   is how an approved plan gets executed against a different bucket. A
    ///   flag and a variable that DISAGREE are refused for the same reason —
    ///   two sources of truth about which contract is in force is no contract.
    ///
    /// An UNKNOWN flag never reaches this function at all: clap refuses it and
    /// the process exits before dispatch, which is what makes an OLD image
    /// handed a destination-backed Job fail loudly instead of silently.
    ///
    /// # Errors
    ///
    /// [`GuardRefusal`], which the caller reports as exit 3.
    pub fn admit(flag: Option<&str>) -> Result<bool, GuardRefusal> {
        let from_env = std::env::var(VERSION_ENV).ok().filter(|v| !v.is_empty());
        match (flag, from_env.as_deref()) {
            (None, None) => Ok(false),
            (Some(a), Some(b)) if a == b && a == VERSION => Ok(true),
            (Some(a), None) if a == VERSION => Err(GuardRefusal(format!(
                "{VERSION_ARG} {a} was passed and {VERSION_ENV} is unset; the controller sets \
                 both, so one without the other is a Job this runner did not receive whole"
            ))),
            (None, Some(b)) => Err(GuardRefusal(format!(
                "{VERSION_ENV} is {b} and no {VERSION_ARG} was passed; the controller writes both \
                 and an argv without the flag is one an older controller built"
            ))),
            (Some(a), Some(b)) if a != b => Err(GuardRefusal(format!(
                "{VERSION_ARG} is {a} and {VERSION_ENV} is {b}; two answers to which store \
                 contract is in force is no contract, and nothing is built"
            ))),
            (Some(a), _) => Err(GuardRefusal(format!(
                "store contract version {a} is not one this build implements (it implements \
                 {VERSION}); the runner refuses rather than improvising a store configuration, \
                 because an improvised one is how an approved plan reaches a different bucket"
            ))),
        }
    }

    /// The archive store's options, from the environment the controller
    /// rendered — D2 §3.5.
    ///
    /// # `StaticFromEnv` AND NOT `Ambient`
    ///
    /// `Ambient` is object_store's whole chain over every `AWS_*` it finds.
    /// `StaticFromEnv` is the three NAMED variables the kubelet projected from
    /// the grant's Secret and nothing else — so a stray `AWS_ENDPOINT_URL` or
    /// `AWS_PROFILE` in the image cannot contribute. The LOCATION never comes
    /// from here at any variant: it is the plan's own `storage` block.
    ///
    /// # Errors
    ///
    /// [`GuardRefusal`] for a `LOGWEIR_ARCHIVE_CREDENTIALS` value this build
    /// does not know, or a CA file it cannot read. Both are exit 3: a store
    /// that cannot be built as the plan describes must not be built some other
    /// way.
    pub fn archive_options() -> Result<StoreOptions, GuardRefusal> {
        let mode = std::env::var(ARCHIVE_CREDENTIALS_ENV).unwrap_or_default();
        let credentials = match mode.as_str() {
            CREDENTIALS_STATIC => CredentialSource::StaticFromEnv,
            CREDENTIALS_WORKLOAD_IDENTITY => CredentialSource::WorkloadIdentity,
            other => {
                return Err(GuardRefusal(format!(
                    "{ARCHIVE_CREDENTIALS_ENV} is `{other}`, and this build knows \
                     `{CREDENTIALS_STATIC}` and `{CREDENTIALS_WORKLOAD_IDENTITY}`"
                )))
            }
        };
        with_ca(
            StoreOptions {
                credentials,
                ..StoreOptions::default()
            },
            ARCHIVE_CA_FILE_ENV,
        )
    }

    /// The evidence store's options — D2 §3.5's three cases.
    ///
    /// * `archive` — the same grant: the archive's own options, CA included.
    ///   Nothing second is projected and nothing second is read.
    /// * `static` — a DIFFERENT Secret, arriving under `LOGWEIR_EVIDENCE_AWS_*`
    ///   so neither store's credential can shadow the other's.
    ///   [`CredentialSource::Static`] with the values read here, because
    ///   `StaticFromEnv` would read `AWS_*` — the ARCHIVE's.
    /// * `workloadIdentity` beside a static archive grant — the identity ONLY.
    ///   object_store's chain puts static keys first, so an `Ambient` source
    ///   here would silently use the archive's keys for the evidence store
    ///   (grounding **G16**).
    ///
    /// # Errors
    ///
    /// [`GuardRefusal`] for an unknown mode, a `static` mode missing one of its
    /// two mandatory variables, or an unreadable CA file.
    pub fn evidence_options(archive: &StoreOptions) -> Result<StoreOptions, GuardRefusal> {
        let mode = std::env::var(EVIDENCE_CREDENTIALS_ENV).unwrap_or_default();
        let options = match mode.as_str() {
            CREDENTIALS_ARCHIVE => archive.clone(),
            CREDENTIALS_WORKLOAD_IDENTITY => StoreOptions {
                credentials: CredentialSource::WorkloadIdentity,
                ..StoreOptions::default()
            },
            CREDENTIALS_STATIC => {
                let access_key_id = required(EVIDENCE_ACCESS_KEY_ID_ENV)?;
                let secret_access_key = required(EVIDENCE_SECRET_ACCESS_KEY_ENV)?;
                StoreOptions {
                    credentials: CredentialSource::Static {
                        access_key_id,
                        secret_access_key,
                        session_token: std::env::var(EVIDENCE_SESSION_TOKEN_ENV)
                            .ok()
                            .filter(|v| !v.is_empty()),
                    },
                    ..StoreOptions::default()
                }
            }
            other => {
                return Err(GuardRefusal(format!(
                    "{EVIDENCE_CREDENTIALS_ENV} is `{other}`, and this build knows \
                     `{CREDENTIALS_ARCHIVE}`, `{CREDENTIALS_STATIC}` and \
                     `{CREDENTIALS_WORKLOAD_IDENTITY}`"
                )))
            }
        };
        // THE `archive` CASE ALREADY CARRIES THE ARCHIVE'S ROOT. Adding the
        // evidence CA on top of it is correct and not redundant: the two
        // destinations may be different objects with different bundles even
        // when the CREDENTIAL is shared.
        with_ca(options, EVIDENCE_CA_FILE_ENV)
    }

    /// One mandatory projected value, named when it is absent.
    fn required(name: &str) -> Result<String, GuardRefusal> {
        std::env::var(name)
            .ok()
            .filter(|v| !v.is_empty())
            .ok_or_else(|| {
                // THE NAME AND NEVER THE VALUE. This function's whole subject
                // is a credential.
                GuardRefusal(format!(
                    "CredentialNotRenderable: {name} is unset or empty, and the store contract \
                     says a `static` evidence grant projects it"
                ))
            })
    }

    /// `options` with the PEM bundle at `$var`, when one was projected.
    ///
    /// An UNREADABLE path is a refusal and not a shrug: the controller sets the
    /// variable only when the destination declares a bundle and the bytes were
    /// frozen into the run's own plan `ConfigMap`, so a path that cannot be
    /// read means the mount is not what the plan says it is — and building the
    /// store against the platform trust store instead would quietly widen what
    /// the run accepts.
    fn with_ca(options: StoreOptions, var: &str) -> Result<StoreOptions, GuardRefusal> {
        let Some(path) = std::env::var(var).ok().filter(|v| !v.is_empty()) else {
            return Ok(options);
        };
        let pem = std::fs::read(&path).map_err(|e| {
            GuardRefusal(format!(
                "{var} names {path} and it cannot be read ({e}); the bundle is frozen into this \
                 run's own plan ConfigMap, so an unreadable path means the mount is not what the \
                 plan describes"
            ))
        })?;
        Ok(options.with_root_certificate(pem))
    }
}

/// A store that could not be BUILT as the plan describes.
///
/// # A MISSING WORKLOAD IDENTITY IS A REFUSAL, NOT AN OUTAGE
///
/// D2 §3.5: "the runner refuses with exit 3 and
/// `refusal-reason=WorkloadIdentityNotInjected` unless `AWS_WEB_IDENTITY_TOKEN_FILE`
/// plus `AWS_ROLE_ARN`, or `AWS_CONTAINER_CREDENTIALS_FULL_URI` plus its token
/// file, is present". Exit 1 would say "retry me"; there is nothing to retry
/// until an administrator wires the identity, and `AWS_METADATA_ENDPOINT` is
/// pinned at a dead loopback precisely so the run cannot fall through to the
/// node's instance role instead (**G16**).
///
/// Every other build failure is exit 1: a store that will not build because the
/// endpoint is unresolvable says nothing about the plan.
fn store_error(e: logweir_engine_oso::storage::StoreError) -> BackupError {
    if logweir_engine_oso::storage::is_workload_identity_not_injected(&e) {
        // THE STATE NAME OPENS THE MESSAGE. `logweir_core::guard::terminal_state`
        // matches a prefix against its own closed list, which this state is not
        // on, so the printed `refusal-reason=` is the generic `GuardRefused` —
        // the specific name reaches the operator through the message. Adding it
        // to that list is `logweir-core`'s to do.
        return logweir_core::guard::GuardRefusal(format!(
            "{}: the store contract asked for an injected workload identity and none is present \
             in this pod ({e}); no fall-back to a node instance role is attempted",
            logweir_engine_oso::storage::WORKLOAD_IDENTITY_NOT_INJECTED
        ))
        .into();
    }
    BackupError::Operational(e.to_string())
}

/// The archive's location, with the key prefix replaced by Global Constraint
/// 6's `logweir/` root — the evidence side of the same bucket.
///
/// PURE, and separate from `run`, so it is testable without a backend: the
/// claim that the receipt cannot land outside `logweir/` is asserted twice,
/// here by construction and again inside `Store::put_create_only`.
/// `Filesystem` carries no prefix at all (its variant is `{path}`), so it is
/// returned unchanged and `Store::from_url` exempts it for that reason.
pub fn evidence_location(
    archive: &logweir_core::engine::StorageUrl,
) -> logweir_core::engine::StorageUrl {
    use logweir_core::engine::StorageUrl as U;
    // `logweir_engine_oso::storage` IS `logweir_store` (`lib.rs:16`'s re-export);
    // this crate depends on the engine crate, not on the store crate directly.
    use logweir_engine_oso::storage::LOGWEIR_ROOT;
    match archive.clone() {
        U::S3 {
            bucket,
            region,
            endpoint,
            path_style,
            allow_http,
            ..
        } => U::S3 {
            bucket,
            prefix: LOGWEIR_ROOT.to_string(),
            region,
            endpoint,
            path_style,
            allow_http,
        },
        U::Azure {
            account_name,
            container_name,
            ..
        } => U::Azure {
            account_name,
            container_name,
            prefix: LOGWEIR_ROOT.to_string(),
        },
        U::Gcs { bucket, .. } => U::Gcs {
            bucket,
            prefix: LOGWEIR_ROOT.to_string(),
        },
        U::Filesystem { path } => U::Filesystem { path },
    }
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
