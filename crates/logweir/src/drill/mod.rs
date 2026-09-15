//! The eleven-phase orchestrator (-1..=9). Phase -1 is the `--from-cluster`
//! source-side capture — Global Constraint 18 (reversed 2026-09-03) and
//! docs/architecture.md#adr-0007-source-capture-scope put it in v0.1 scope, and global
//! ruling GR4 Part B defers its execution path to Task 24, so it is NOT wired
//! here. The slot domain is `-1..=9`; the ten modules below are phases 0
//! through 9.
pub mod phase0_admit;
pub mod phase1_approval;
pub mod phase2_target;
pub mod phase3_diff;
pub mod phase4_sample;
pub mod phase5_preflight;
pub mod phase6_restore;
pub mod phase7_verify;
pub mod phase8_score;
pub mod phase9_teardown;

use crate::exit::ExitCode;
use crate::signer::ValidatedSigner;
use logweir_core::engine::{
    BackupSetFacts, BackupSetRef, DataEngine, RestorePlan, WindowFloorSource,
};
use logweir_core::guard::GuardRefusal;
use logweir_core::outcome::{IntegrityLevel, IntegrityResult, LeverState, MatrixVerdict, Outcome};
use logweir_core::scorecard::{
    ApprovalInfo, AuthSummary, EngineInfo, EvidenceInfo, Integrity, Levers, Measured, Objectives,
    PhaseRecord, SampleInfo, Scorecard, SourceInfo, TargetDiffSummary, TargetInfo, TopicParity,
};
use logweir_core::spec::{AllowedClusters, DrillSpec};
use logweir_engine_oso::storage::Store;
use logweir_kafka::reader::{AuthConfig, ClusterReader, TopicCreator, TopicDeleter};
use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;

#[derive(Debug, thiserror::Error)]
pub enum DrillError {
    /// The drill could not be attempted or continued, for a reason that says
    /// nothing about the archive. Maps to ExitCode::Operational (1).
    #[error("operational: {0}")]
    Operational(String),
    /// A guard refused the plan before anything ran. Maps to
    /// ExitCode::GuardRefused (3).
    #[error("guard: {0}")]
    Guard(#[from] logweir_core::guard::GuardRefusal),
    #[error("kafka: {0}")]
    Kafka(#[from] logweir_kafka::reader::KafkaError),
    #[error("engine: {0}")]
    Engine(#[from] logweir_core::engine::EngineError),
    /// A drill RESULT, not an operational failure (Task 18 fix round 1,
    /// review finding F1): the restore subprocess ran, exited 0, the target
    /// cluster was read successfully for every selected destination topic,
    /// and every partition was still at end offset <= 0. That is a
    /// positively established fact ABOUT the archive — the backup does not
    /// actually restore — so it must NOT be routed like `Operational` (exit
    /// 1, no artifact). It mirrors phase 5's `Verdict::Block`: the
    /// orchestrator (Task 21a) must catch this variant at the phase-6 call
    /// site, BEFORE the generic `record(...)?` short-circuit, build and sign
    /// a scorecard from it, and return `DrillError::NotPass(Box::new(signed))`
    /// so it reaches ExitCode::DrillNotPass (2). See `task-21a-addendum.md`
    /// ruling A8 for the required orchestrator wiring — implemented in
    /// `execute_with`'s phase-6 branch. If this variant ever reaches
    /// `impl From<DrillError> for ExitCode` unhandled (or handled by a
    /// catch-all that maps it to `Operational`), that is the exact defect
    /// this comment exists to prevent.
    #[error("drill-not-pass: {0}")]
    RestoreNoOp(String),
    /// A drill RESULT that is not a pass. The scorecard is carried out so it
    /// is still signed and uploaded — exit 1 here would produce no artifact,
    /// and this signed document is the NIS2 IR 4.2.3 evidence.
    #[error("drill did not pass")]
    NotPass(Box<Scorecard>),
    /// The drill RAN, and its result could not be signed or its lock proof
    /// could not be obtained. That is neither a pass nor an operational
    /// failure: "the result exists but is unattested" is its own outcome, and
    /// the exit contract reserves 4 for it. Constructed in exactly two places
    /// Signing readiness also reaches this variant before any data work. The
    /// phase-8 and phase-9 persistence paths sign before they put, so a
    /// document whose signing fails still leaves the bucket untouched.
    #[error("signing or lock proof failed: {0}")]
    SigningOrLock(String),
    /// Startup signing readiness failed before a client, work directory, or
    /// output existed. Kept distinct from [`Self::SigningOrLock`] because a
    /// later unattested drill result still owes terminal metrics/notification
    /// handling, while a failed prerequisite must cause no execution output
    /// or network side effect at all.
    #[error("signing or lock proof failed: {0}")]
    SigningPrerequisite(String),
}

impl DrillError {
    /// Global Constraint 11 / spec §6 C5, in ONE place — the match body that
    /// used to live in `impl From<DrillError> for ExitCode`, moved here so a
    /// caller can know the code while still BORROWING the scorecard a
    /// `NotPass` carries. The `From` impl below delegates, so the by-value and
    /// by-reference forms can never disagree. A blanket `map_err` at any call
    /// site would move the exit-4 contract out of here, so nothing else in the
    /// crate may map a `DrillError` to an `ExitCode`.
    pub fn exit_code(&self) -> ExitCode {
        match self {
            // A guard refused the plan before anything ran.
            DrillError::Guard(_) => ExitCode::GuardRefused, // 3
            // A drill RESULT that is not a pass. A scorecard IS written and
            // signed — `execute_with` does that before ever constructing this
            // variant, which is what makes exit 2's promise true.
            DrillError::NotPass(_) => ExitCode::DrillNotPass, // 2
            // The drill ran; its result is unattested.
            DrillError::SigningOrLock(_) | DrillError::SigningPrerequisite(_) => {
                ExitCode::SigningOrLock // 4
            }
            // A drill RESULT that is not a pass. It must have been intercepted
            // by the orchestrator, scored, signed and uploaded before any exit
            // code was derived — reaching here means the artifact was never
            // built, so exit 2 ("a scorecard IS written and signed") would be a
            // lie and exit 1 would discard a positively established fact about
            // the archive. Fail loudly instead of choosing between two wrong
            // answers (task-21a-addendum.md ruling A8).
            DrillError::RestoreNoOp(_) => unreachable!(
                "RestoreNoOp must be intercepted in the orchestrator before reaching \
                 ExitCode conversion — see task-21a-addendum.md ruling A8"
            ),
            // A Kafka or engine failure says nothing about the archive.
            DrillError::Operational(_) | DrillError::Kafka(_) | DrillError::Engine(_) => {
                ExitCode::Operational // 1
            }
        }
    }
}

impl From<DrillError> for ExitCode {
    fn from(e: DrillError) -> Self {
        e.exit_code()
    }
}

/// Runs one phase and pushes its `PhaseRecord` BEFORE returning, so a crash or
/// an error leaves a truthful partial record. `last_phase_completed` is updated
/// in exactly one place, which is why it can never drift from `phases`.
///
/// This is also the ONE caller of `PhaseObserver::phase_started` /
/// `phase_finished`. Before Task 13 those two methods were live code with no
/// caller anywhere in the product: `PhaseLogger` implemented them, a test
/// invoked them directly, and no production path ever did — so the per-phase
/// progress an operator was documented to see did not exist. Wiring them here
/// rather than at fifteen call sites is what keeps `record`'s signature and
/// every caller unchanged: the run id the lines carry is `sc.run_id`, which
/// `record` already has in hand, and which is the same id `run()` minted and
/// put on the `drill` span.
///
/// The pair is what an operator sees on a phase that has NOTHING to report —
/// phase 9 on a clean teardown, most obviously, where the metric reads `0` and
/// the summary line says nothing. Without these lines a clean phase 9 is
/// indistinguishable in the log from a phase 9 that never ran.
pub fn record<T>(
    sc: &mut Scorecard,
    phase: i8,
    name: &str,
    f: impl FnOnce() -> Result<T, DrillError>,
) -> Result<T, DrillError> {
    use logweir_core::engine::PhaseObserver;
    let mut obs = crate::metrics::PhaseLogger::new(&sc.run_id);
    obs.phase_started(phase, name);
    // Global Constraint 1: the clock is read HERE, in `crates/logweir`.
    let at = chrono::Utc::now();
    let t0 = std::time::Instant::now();
    let r = f();
    let outcome = match &r {
        Ok(_) => "ok".to_string(),
        Err(e) => format!("failed: {e}"),
    };
    // The SAME string the `PhaseRecord` carries, so the log and the scorecard
    // can never disagree about how a phase ended.
    obs.phase_finished(phase, &outcome);
    sc.phases.push(PhaseRecord {
        phase,
        name: name.to_string(),
        at,
        outcome,
        duration_ms: t0.elapsed().as_millis() as u64,
        notes: vec![],
    });
    if r.is_ok() {
        sc.last_phase_completed = phase;
    }
    r
}

pub struct RunArgs {
    pub execution_contract_version: Option<String>,
    pub spec: PathBuf,
    pub approval: PathBuf,
    pub approver_key: PathBuf,
    pub allowed_clusters: PathBuf,
    pub signing_key: PathBuf,
    pub triggered_by: Option<String>,
    pub out: Option<PathBuf>,
    pub metrics_file: Option<PathBuf>,
    /// `--offset-report-out`. Where the ENGINE writes its offset-mapping
    /// report, which phase 8 then uploads beside the scorecard. Absent means
    /// `offsets.json` in this run's workdir (`run_workdir`), beside the
    /// checkpoint.
    pub offset_report_out: Option<PathBuf>,
    /// `--approver-key-ids`, repeated. The approver key ids this run accepts.
    ///
    /// **EMPTY MEANS NOT PINNED**, which is today's behaviour exactly: the
    /// flag is additive and no existing adopter breaks. A non-empty set is
    /// checked by `phase1_approval::admit_pinned_approver_key_id` from
    /// `drill::execute`, ahead of `context` and therefore before any Kafka
    /// client exists — which is what makes "BEFORE phase 0 dials anything"
    /// true at the socket layer and not merely at the phase layer. An approver
    /// key outside the set is exit 3.
    ///
    /// The operator fills this from the `TrustRoster`
    /// (`weirkeeper::controllers::restore::approver_key_ids`: every
    /// `spec.approverKeys[].keyId` that `status.expiredKeyIds` does not name,
    /// in roster order), which is how the one source of truth reaches a pod
    /// that holds no cluster credential.
    pub approver_key_ids: Vec<String>,
}

/// Controller-pinned identity and byte digests carried by a new Restore Job's
/// immutable pod template.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ExecutionContract {
    pub subject_api_version: String,
    pub subject_kind: String,
    pub subject_name: String,
    pub subject_namespace: String,
    pub subject_uid: String,
    pub approval_name: String,
    pub approval_uid: String,
    pub plan_sha256: String,
    pub approval_sha256: String,
    pub approval_sidecar_sha256: String,
    pub approver_key_sha256: String,
    pub allowed_clusters_sha256: String,
}

/// Parse the all-or-nothing execution contract without reading process-global
/// state in tests. No variables means a compatible standalone invocation;
/// one or more variables means every field and the current version are
/// mandatory.
pub fn execution_contract_from(
    mut get: impl FnMut(&str) -> Option<String>,
) -> Result<Option<ExecutionContract>, DrillError> {
    use logweir_core::execution_contract as wire;

    let values: BTreeMap<&str, Option<String>> = wire::ALL_ENV
        .into_iter()
        .map(|name| (name, get(name)))
        .collect();
    if values.values().all(Option::is_none) {
        return Ok(None);
    }
    let required = |name: &'static str| -> Result<String, DrillError> {
        values
            .get(name)
            .and_then(Clone::clone)
            .filter(|value| !value.trim().is_empty())
            .ok_or_else(|| {
                GuardRefusal(format!(
                    "incomplete Restore execution contract: {name} is missing; no data \
                     operation was started"
                ))
                .into()
            })
    };
    let version = required(wire::VERSION_ENV)?;
    if version != wire::VERSION {
        return Err(GuardRefusal(format!(
            "unsupported Restore execution contract version {version:?}; expected {:?}; no \
             data operation was started",
            wire::VERSION
        ))
        .into());
    }
    Ok(Some(ExecutionContract {
        subject_api_version: required(wire::SUBJECT_API_VERSION_ENV)?,
        subject_kind: required(wire::SUBJECT_KIND_ENV)?,
        subject_name: required(wire::SUBJECT_NAME_ENV)?,
        subject_namespace: required(wire::SUBJECT_NAMESPACE_ENV)?,
        subject_uid: required(wire::SUBJECT_UID_ENV)?,
        approval_name: required(wire::APPROVAL_NAME_ENV)?,
        approval_uid: required(wire::APPROVAL_UID_ENV)?,
        plan_sha256: required(wire::PLAN_SHA256_ENV)?,
        approval_sha256: required(wire::APPROVAL_SHA256_ENV)?,
        approval_sidecar_sha256: required(wire::APPROVAL_SIDECAR_SHA256_ENV)?,
        approver_key_sha256: required(wire::APPROVER_KEY_SHA256_ENV)?,
        allowed_clusters_sha256: required(wire::ALLOWED_CLUSTERS_SHA256_ENV)?,
    }))
}

/// Bind a new Job's mandatory argv handshake to its immutable environment
/// contract. Both absent is the explicit legacy/standalone shape; every other
/// invocation must carry a complete current contract through both channels.
pub fn execution_contract_for_invocation(
    cli_version: Option<&str>,
    mut get: impl FnMut(&str) -> Option<String>,
) -> Result<Option<ExecutionContract>, DrillError> {
    use logweir_core::execution_contract as wire;

    let values: BTreeMap<&str, Option<String>> = wire::ALL_ENV
        .into_iter()
        .map(|name| (name, get(name)))
        .collect();
    let has_environment_contract = values.values().any(Option::is_some);
    match (cli_version, has_environment_contract) {
        (None, false) => return Ok(None),
        (None, true) => {
            return Err(GuardRefusal(format!(
                "incomplete Restore execution contract: {} is missing while contract \
                 environment is present; no data operation was started",
                wire::VERSION_ARG
            ))
            .into())
        }
        (Some(_), false) => {
            return Err(GuardRefusal(format!(
                "incomplete Restore execution contract: {} was supplied without the required \
                 contract environment; no data operation was started",
                wire::VERSION_ARG
            ))
            .into())
        }
        (Some(_), true) => {}
    }

    let cli_version = cli_version.expect("the exhaustive match established a CLI version");
    if cli_version != wire::VERSION {
        return Err(GuardRefusal(format!(
            "unsupported Restore execution contract argv version {cli_version:?}; expected {:?}; \
             no data operation was started",
            wire::VERSION
        ))
        .into());
    }
    let environment_version = values
        .get(wire::VERSION_ENV)
        .and_then(Option::as_deref)
        .filter(|value| !value.trim().is_empty());
    if environment_version != Some(cli_version) {
        return Err(GuardRefusal(format!(
            "Restore execution contract version mismatch: {} carries {:?} but {} carries \
             {cli_version:?}; no data operation was started",
            wire::VERSION_ENV,
            environment_version,
            wire::VERSION_ARG
        ))
        .into());
    }

    execution_contract_from(|name| values.get(name).and_then(Clone::clone))
}

/// Exact projected bytes captured once at process startup.
#[derive(Clone, Debug)]
pub struct ApprovalBundleBytes {
    pub plan: Vec<u8>,
    pub approval: Vec<u8>,
    pub approval_sidecar: Vec<u8>,
    pub approver_key: Vec<u8>,
    pub allowed_clusters: Vec<u8>,
}

/// Independently compare every mounted public input with the immutable Job
/// template before those bytes are parsed or any client is constructed.
pub fn validate_execution_contract(
    contract: &ExecutionContract,
    triggered_by: Option<&str>,
    bundle: &ApprovalBundleBytes,
) -> Result<(), DrillError> {
    if contract.subject_api_version != "logweir.dev/v1alpha1" || contract.subject_kind != "Restore"
    {
        return Err(GuardRefusal(
            "the execution contract does not identify a logweir.dev/v1alpha1 Restore; no data \
             operation was started"
                .to_string(),
        )
        .into());
    }
    let expected_trigger = format!("approval/{}", contract.approval_name);
    if triggered_by != Some(expected_trigger.as_str()) {
        return Err(GuardRefusal(format!(
            "the execution contract names Approval {} but --triggered-by is {:?}; no data \
             operation was started",
            contract.approval_name, triggered_by
        ))
        .into());
    }
    let checks = [
        ("plan", &contract.plan_sha256, bundle.plan.as_slice()),
        (
            "approval",
            &contract.approval_sha256,
            bundle.approval.as_slice(),
        ),
        (
            "approval sidecar",
            &contract.approval_sidecar_sha256,
            bundle.approval_sidecar.as_slice(),
        ),
        (
            "approver public key",
            &contract.approver_key_sha256,
            bundle.approver_key.as_slice(),
        ),
        (
            "allowed-clusters",
            &contract.allowed_clusters_sha256,
            bundle.allowed_clusters.as_slice(),
        ),
    ];
    for (label, expected, bytes) in checks {
        let actual = logweir_core::ids::sha256_prefixed(bytes);
        if &actual != expected {
            return Err(GuardRefusal(format!(
                "the mounted {label} bytes hash to {actual}, not the controller-pinned \
                 {expected}; no data operation was started"
            ))
            .into());
        }
    }
    Ok(())
}

/// The log filter used when `RUST_LOG` is unset or blank.
///
/// SCOPED ON PURPOSE, and the scope is the documented guarantee. A bare
/// `"info"` sets the default level for EVERY target, not only Logweir's, and
/// the third-party crates that emit `tracing` here run on tokio worker
/// threads. The `drill` span carrying `run_id` is entered with
/// `info_span!(..).entered()`, and an `EnteredSpan` is THREAD-LOCAL — so an
/// INFO event from a worker thread would render with neither `fields.run_id`
/// nor (with `.with_current_span(true)`, which renders only the CURRENT span)
/// `span.run_id`. README's and `docs/kubernetes.md`'s "every line Logweir
/// emits at the default level carries the run id" would then be a universal
/// claim over a stream containing lines that cannot carry it — exactly the
/// documented-guarantee-versus-code defect this stage exists to close.
/// `task-12-brief.md` §5 step 2a blesses this narrowing by name.
///
/// The WARN list is not a guess. It is every crate in `Cargo.lock` that can
/// put an event into THIS stream, and there are TWO ways to do that, not one:
///
/// * a direct `tracing` dependency (`h2`, `hyper_util`, `object_store`); and
/// * a direct `log` dependency (`rdkafka`, `reqwest`, `rustls`,
///   `rustls_platform_verifier`, `ureq`, `iana_time_zone`). `try_init()` below
///   installs `tracing_log::LogTracer` — tracing-subscriber 0.3's `try_init`
///   does it under `#[cfg(feature = "tracing-log")]`, and that feature is on
///   through `tracing-subscriber`'s default features — so every `log` record
///   is converted into a `tracing` event carrying the emitting crate's target
///   and then passes through this same `EnvFilter`. The earlier version of
///   this list closed over `tracing` dependants only, so `rustls` and `rdkafka`
///   INFO chatter reached the default stream from tokio worker threads with no
///   `fields.run_id` and no `span` object at all.
///
/// Excluded: this workspace's own crates (they MUST stay at `info`),
/// `tracing` (the facade — every event carries the EMITTING module's target,
/// never `tracing`), `tracing-subscriber` (the subscriber, which emits
/// nothing) and `tracing-log` (the bridge itself, likewise). Target names are
/// MODULE paths, so hyphens become underscores.
///
/// Also excluded, and this is the second half of the derivation: a crate that
/// is in `Cargo.lock` but **not in the resolved graph**. The lockfile records
/// optional dependencies no feature activates — `quinn`, `quinn-proto` and
/// `quinn-udp` arrive through `reqwest`'s HTTP/3 feature and are compiled by
/// nothing (`cargo tree -p logweir -i quinn -e normal` prints "nothing to
/// print"), and `jni` likewise. They were pinned here and are now gone: naming
/// a crate that is not in the binary is decoration, and decoration in a
/// security-adjacent constant reads as coverage it does not provide.
///
/// `every_tracing_emitting_dependency_in_the_lockfile_is_pinned_to_warn`
/// re-derives the whole set — both bridges, narrowed to what this build
/// actually compiled — on every run, so a new emitter arriving with a future
/// dependency bump fails a test instead of silently widening the stream.
///
/// FIVE NAMES ADDED BY TASK 15, and this is the "future dependency bump" the
/// paragraph above was written for. `crates/weirkeeper` took `kube` and
/// `k8s-openapi`, and five crates that were already in `Cargo.lock` became
/// emitters as a result: `hyper_rustls`, `kube_client`, `kube_runtime`, `tower`
/// and `tower_http`. Their `tracing` edge is OPTIONAL upstream — `tower`'s and
/// `tower_http`'s behind their tracing/log features, `hyper_rustls`'s behind
/// `logging` — and nothing in the workspace activated it until `kube-client`
/// did, which is why they sat in the lockfile for several tasks without
/// appearing here. That is the gate doing its job: the emitters arrived
/// without a version bump anywhere, and the test named them on the first run
/// after the client landed.
///
/// `weirkeeper` itself is NOT here, and not because it cannot emit — it
/// depends on `tracing` directly. It is one of this workspace's own crates,
/// which the paragraph above excludes because they MUST stay at `info`, and it
/// installs its own subscriber in `crates/weirkeeper/src/main.rs`; the
/// `logweir` binary never links it. The exclusion predicate in the test below
/// names it for that reason.
const DEFAULT_LOG_DIRECTIVE: &str = "info,\
     h2=warn,\
     hyper_rustls=warn,\
     hyper_util=warn,\
     iana_time_zone=warn,\
     kube_client=warn,\
     kube_runtime=warn,\
     object_store=warn,\
     rdkafka=warn,\
     reqwest=warn,\
     rustls=warn,\
     rustls_platform_verifier=warn,\
     tower=warn,\
     tower_http=warn,\
     ureq=warn";

/// The observer phase 6 hands to the engine, carrying THIS run's id.
///
/// A named function rather than an inline `PhaseLogger::new(run_id)` at the
/// call site so the join between the run's identity and the engine's captured
/// output — the line that makes §2.6's stream real in production — is
/// reachable from a test without a live broker. `PhaseLogger` re-emits every
/// captured child line under this id; the child is never told it (GC3).
fn phase_observer(run_id: &str) -> crate::metrics::PhaseLogger {
    crate::metrics::PhaseLogger::new(run_id)
}

/// WHICH of the command's two names the operator typed.
///
/// `logweir restore run` is the name (interface **I20**/**I8**);
/// `logweir drill run` is the tag-0 name, kept working so that every
/// checked-in invocation, every doc and every CronJob in the wild still runs,
/// and printing exactly one deprecation line. Both parse the same
/// `RestoreSpec` and both reach `run_with`, so there is nothing an alias can
/// do differently by accident.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InvokedAs {
    Restore,
    DrillAlias,
}

/// The one deprecation line `logweir drill run` prints, verbatim.
///
/// A constant rather than a `format!` at the call site so the line a test
/// asserts and the line an operator reads are the same bytes.
pub const DRILL_RUN_DEPRECATION: &str =
    "logweir drill run is the tag-0 name for logweir restore run and will be removed in tag 2";

/// The deprecation line, through a writer seam, for the same reason
/// `crate::exit::print_refusal_reason_to` has one: a `eprintln!` nobody can
/// observe is a contract nothing pins.
///
/// It goes on STDERR and it is the ONLY thing the alias adds. Stdout is
/// interface I8's channel and a deprecation notice on it would land between a
/// controller and the three keys it reads.
pub fn print_deprecation_to<W: std::io::Write>(
    w: &mut W,
    invoked_as: InvokedAs,
) -> std::io::Result<()> {
    match invoked_as {
        InvokedAs::DrillAlias => writeln!(w, "{DRILL_RUN_DEPRECATION}"),
        InvokedAs::Restore => Ok(()),
    }
}

pub fn run(args: RunArgs) -> ExitCode {
    run_named(args, InvokedAs::Restore)
}

/// `run` under whichever of the two names was typed. What `main.rs` calls for
/// both `restore run` and `drill run`.
pub fn run_named(args: RunArgs, invoked_as: InvokedAs) -> ExitCode {
    // Spec §13: structured JSON logs on stdout with run_id on every line.
    let run_id = crate::ids::new_run_id();
    // T0-10. `EnvFilter::from_default_env()` builds with
    // `.with_default_directive(LevelFilter::ERROR)` [VERIFIED
    // tracing-subscriber-0.3.23 `filter/env/mod.rs:289-293`], so with RUST_LOG
    // unset the effective level is ERROR. The `drill` span carrying `run_id` is
    // an INFO span, so at ERROR it is never entered and `with_current_span`
    // below has nothing to render; and `exiting()`'s INFO line — the only place
    // the exit-code MEANING reaches a log aggregator, see its doc comment —
    // disappeared entirely. That is this comment's line breaking the guarantee
    // stated two lines above it.
    //
    // So the default is computed here and RUST_LOG still wins whenever it is
    // set to anything non-blank. `try_from_default_env()` is NOT used: for a
    // BLANK RUST_LOG (`env: - name: RUST_LOG` with an empty `value:`, a
    // Kubernetes-manifest reality) `env::var` returns `Ok("")`, which parses to
    // the empty directive set — Ok, not Err — so an `unwrap_or_else` fallback
    // never fires and the level silently stays ERROR. Blank is treated as
    // unset here instead. `EnvFilter::new` is `parse_lossy` [VERIFIED
    // `filter/env/mod.rs:350-354`]: a malformed RUST_LOG is dropped with a note
    // on stderr, never unwrapped into a panic.
    let filter = match std::env::var("RUST_LOG") {
        Ok(v) if !v.trim().is_empty() => tracing_subscriber::EnvFilter::new(v),
        _ => tracing_subscriber::EnvFilter::new(DEFAULT_LOG_DIRECTIVE),
    };
    // `try_init`, not `init`: `init` PANICS when a global subscriber is
    // already installed, and `run` is a library entry point a test or an
    // embedder may call more than once in one process. A logger that is
    // already configured is not a reason to abort a drill.
    let _ = tracing_subscriber::fmt()
        .json()
        .with_current_span(true)
        .with_env_filter(filter)
        .try_init();
    let _span = tracing::info_span!("drill", run_id = %run_id).entered();
    // The alias's ONE line, before anything else this process says.
    let _ = print_deprecation_to(&mut std::io::stderr().lock(), invoked_as);

    let (outcome, authenticated_spec) = execute_for_reporting(&args, &run_id);
    report(&args, &run_id, authenticated_spec.as_ref(), outcome)
}

/// `run_named` with the run's handles SUPPLIED rather than constructed — the
/// seam a whole invocation can be driven through over doubles.
///
/// `run_named` needs a live broker, a bucket and the engine binary (that is
/// what `context` builds), so nothing could previously assert what one full
/// invocation of either CLI name does without them. This is the same
/// `run`/`run_with` split the crate already uses for `execute`/`execute_with`
/// and `report`/`report_with`, and it is what
/// `crates/logweir/tests/restore_mode.rs::
/// cli_drill_run_is_an_alias_for_restore_run` calls: one function, one args
/// value parsed from either command line, and the deprecation line captured
/// through `stderr` instead of trusted.
///
/// It does NOT install a tracing subscriber and does not enter the `drill`
/// span. Those are process-global and belong to the binary's entry point;
/// installing them here would make two calls in one test process fight over
/// the global default. It also does not send plan-controlled notifications:
/// supplying a `Ctx` bypasses captured-byte contract and approval startup, so
/// this test seam has no authenticated notification authority.
pub fn run_with(
    args: &RunArgs,
    run_id: &str,
    c: &Ctx,
    invoked_as: InvokedAs,
    mut stderr: &mut dyn std::io::Write,
) -> ExitCode {
    let _ = print_deprecation_to(&mut stderr, invoked_as);
    let outcome = execute_with_outcome(args, run_id, c);
    report(args, run_id, None, outcome)
}

/// Everything `run` does with `execute`'s answer, split out so it can be
/// tested. `run` itself builds the real `Ctx` and therefore needs a live
/// broker and the engine binary, and this is the product's PRIMARY OUTPUT:
/// the difference between exit 1 ("Logweir could not do its job; there is no
/// artifact") and exit 2 ("a drill ran and did not pass; the signed scorecard
/// is in the bucket") is the whole point of the tool, and it must not be the
/// one decision with no coverage.
///
/// NOTE the shape: no arm here names an `ExitCode` literal except `Ok`.
/// `DrillError::exit_code` is the single place the contract lives (its own doc
/// comment says so), and a second mapping written out here —
/// `Err(NotPass(..)) => ExitCode::DrillNotPass` — would be exactly the
/// duplication that comment forbids: two places to keep in step, and only one
/// of them under the exit-code tests. So this function decides only what to
/// PRINT and whether a drill result exists to finish; the code comes from the
/// conversion.
fn report(
    args: &RunArgs,
    run_id: &str,
    authenticated_spec: Option<&DrillSpec>,
    outcome: Result<RestoreOutcome, DrillError>,
) -> ExitCode {
    report_with(
        args,
        run_id,
        authenticated_spec,
        outcome,
        &phase7_verify::UreqSink::new(),
    )
}

/// Split from `report` for the same reason `run`/`execute` are split: the test
/// needs to observe the outbound event without a socket (GC17).
fn report_with(
    args: &RunArgs,
    run_id: &str,
    authenticated_spec: Option<&DrillSpec>,
    outcome: Result<RestoreOutcome, DrillError>,
    sink: &dyn phase7_verify::EventSink,
) -> ExitCode {
    let sc: Option<&Scorecard> = match &outcome {
        Ok(o) => Some(&o.scorecard),
        // A drill RESULT: the scorecard was signed and uploaded by phase 8, so
        // the metrics and the summary line are owed.
        Err(DrillError::NotPass(sc)) => Some(sc),
        Err(_) => None,
    };
    // Task 14: `sc.is_none()` is exactly `Err(non-NotPass)`, which is exactly
    // the set the pre-Task-11 `_ =>` arm carried — exits 1, 3 and 4, the three
    // codes on which the PagerDuty route used to say nothing at all.
    let mut failure_message: Option<String> = None;
    // [I9] The GuardRefusal's OWN message, not `DrillError`'s `Display`. The
    // enum wraps it as `guard: plan refused by the admission guard: <message>`
    // (`DrillError::Guard`'s `#[error]` and `GuardRefusal`'s), and
    // `logweir_core::guard::terminal_state` matches a PREFIX — so handing it
    // the wrapped form would classify every credential refusal as a plain
    // `GuardRefused` and the state would never be reported at all.
    let refusal_message: Option<String> = match &outcome {
        Err(DrillError::Guard(refusal)) => Some(refusal.0.clone()),
        _ => None,
    };
    if sc.is_none() {
        // The no-scorecard branch: exits 1, 3 and 4. This replaces the `_ =>`
        // arm of the `3e448da` inner `match &e` — there is no `_ =>` arm any
        // more, and no second place where a DrillError becomes an ExitCode
        // (R-11d). `run_id` is in scope here and stays in scope.
        if let Err(e) = &outcome {
            // `run_id` on the EVENT, not only on the entered span: an operator
            // at `RUST_LOG=warn` has the INFO span disabled, and any
            // single-line consumer (`jq '.fields.run_id'`, a log-aggregator
            // field extractor) reads the event object and not its span. The
            // identity has to be on the line that survives both.
            tracing::error!(run_id = %run_id, error = %e, "drill failed");
            eprintln!("{e}");
            failure_message = Some(e.to_string());
        }
    }
    // The diagnostics above are emitted BEFORE the code is derived, which is
    // the order `aae4c3b` had (the conversion ran last there). `exit_code()`
    // PANICS on a `RestoreNoOp` that leaked past the orchestrator's
    // interception, so deriving the code first would silently swallow the
    // "drill failed" line and the stderr message an operator gets ahead of the
    // panic. Nothing between the two statements emits anything, so this
    // ordering is observable only on that path (review fix round 1, F3).
    let code = match &outcome {
        Ok(_) => ExitCode::Ok,
        Err(e) => e.exit_code(),
    };
    // ONE call site for every terminal path. Exhaustiveness is structural: a
    // path that does not flow through here is a path that does not return an
    // ExitCode from `report`, which the compiler will not let you write.
    publish(args, run_id, code, sc);
    // Task 14: AFTER `publish`, so a hung, refusing or misconfigured alerting
    // endpoint can never delay or prevent Task 11's metrics record — the one
    // artifact the exit-1/3/4 paths are now guaranteed to leave. BEFORE
    // `exiting`, so "drill finished" stays the last line on the stream.
    // `ExitCode` is `Copy`, so passing `code` to both this call and `exiting`
    // is fine. GC11: `notify_failure_with` returns nothing and swallows every
    // transport failure — the code is already decided above and is not
    // reachable from here.
    let signing_prerequisite_failed = matches!(&outcome, Err(DrillError::SigningPrerequisite(_)));
    if !signing_prerequisite_failed {
        if let (Some(msg), Some(s)) = (failure_message.as_deref(), authenticated_spec) {
            phase7_verify::notify_failure_with(
                &s.notifications,
                s.name.as_deref(),
                run_id,
                code,
                msg,
                sink,
            );
        }
    }
    // [I8] Only a successful run has three keys to name — see `exiting`.
    let evidence = match &outcome {
        Ok(o) => Some(&o.evidence),
        Err(_) => None,
    };
    // GUARD **G-TS**'s observation, plan erratum **E10(c)**'s producer half.
    // Available only on `Ok`, because `RestoreOutcome` is what carries it and
    // every other path returns a `DrillError` instead — so a run that was
    // refused at phase 0, or that died before phase 0 finished, prints no
    // preflight line and `Restore.status.topicPreflight` stays absent. That is
    // the truthful answer for a run that never read the target's config.
    let preflight = match &outcome {
        Ok(o) => Some(&o.topic_preflight),
        Err(_) => None,
    };
    exiting(
        run_id,
        code,
        refusal_message.as_deref(),
        evidence,
        preflight,
    )
}

/// Everything a terminal path owes the outside world, whether or not a
/// scorecard exists. This does not sign, does not upload, and does not touch
/// the exit code — GC11 is decided above and passed in.
///
/// T0-7: before this existed, exits 1, 3 and 4 wrote nothing at all, so a
/// CronJob whose pod died and a CronJob that was never scheduled produced the
/// same observation — none — and the dashboard's `1`/`3`/`4` value mappings
/// were unreachable by any code path.
fn publish(args: &RunArgs, run_id: &str, code: ExitCode, sc: Option<&Scorecard>) {
    match sc {
        Some(sc) => finish(args, sc),
        None => {
            // No `--metrics-file`, no file: never a default path (M13).
            let Some(p) = &args.metrics_file else { return };
            if let Err(e) = crate::metrics::write_minimal_textfile(p, None, run_id, code) {
                // GC11 discipline: a metrics write is a local operational
                // side-channel. Its failure is logged and swallowed; it never
                // changes the exit code the drill already decided.
                tracing::warn!(error = %e, path = %p.display(), "metrics textfile not written");
            }
        }
    }
}

/// Logs the exit code and what it means, then returns it unchanged.
///
/// This exists for a delivery-layer reason, verified on a live cluster: a
/// Kubernetes Job's exit code is visible ONLY in
/// `pod.status.containerStatuses[].state.terminated.exitCode` — it is absent
/// from Job status and `kubectl get pods` renders every non-zero code as a
/// generic "Error". So an operator cannot tell exit 1 ("Logweir could not do
/// its job; there is no artifact") from exit 2 ("a drill ran and did not
/// pass; go and read the signed scorecard") from the delivery layer at all.
/// The log line is the one place that distinction survives into a log
/// aggregator. Documenting it for operators is Tasks 22/23's job; emitting it
/// is this one's.
fn exiting(
    run_id: &str,
    code: ExitCode,
    refusal_message: Option<&str>,
    evidence: Option<&EvidenceKeys>,
    topic_preflight: Option<&phase0_admit::TopicPreflight>,
) -> ExitCode {
    let meaning = match code {
        ExitCode::Ok => "the drill passed",
        ExitCode::Operational => "logweir could not do its job; NO scorecard was written",
        ExitCode::DrillNotPass => "a drill ran and did not pass; a SIGNED scorecard was written",
        ExitCode::GuardRefused => "the plan was refused before anything ran; no scorecard",
        ExitCode::SigningOrLock => {
            "signing readiness failed or the drill result is unattested; nothing uploaded"
        }
    };
    tracing::info!(run_id = %run_id, exit_code = code as u8 as i64, meaning, "drill finished");
    // [I9] AFTER the tracing line, so the reason is the LAST thing on stdout —
    // a controller tailing the pod log reads the final line, and the pod log
    // API has no stream selector, so stderr would not be distinguishable at
    // all (`crate::exit::print_refusal_reason`'s doc comment carries the
    // measurement). Global Constraint 11: EVERY guard refusal prints it.
    //
    // `unwrap_or("")` is the fail-safe direction, not a shrug: `GuardRefused`
    // is reachable only from `DrillError::Guard`, which always carries a
    // message, and an empty message classifies as `GuardRefused` — so a future
    // path that reaches exit 3 without one still satisfies the contract
    // instead of printing nothing.
    if code == ExitCode::GuardRefused {
        crate::exit::print_refusal_reason(refusal_message.unwrap_or(""));
    }
    // **[I8] AND THE ORDER IS THE CONTRACT.** `scorecard-key=`, then
    // `sidecar-key=`, then `offset-report-key=`, as the FINAL stdout lines of
    // a successful run with nothing after them — the same shape `logweir
    // backup run` uses for interface I7's two keys
    // (`crate::backup::mod`'s `exiting`), because a controller cannot tell
    // stdout from stderr through the pod log API and reads a bounded tail by
    // KEY NAME (plan erratum E4).
    //
    // AFTER the tracing line and after `finish`'s summary line, both of which
    // are emitted before `exiting` is reached. Printed only on exit 0: on
    // every other code there is no set of keys to name, and a line naming a
    // key nothing was written to would be the worst possible output.
    //
    // The third line is printed only when there IS an offset report — see
    // `phase8_score::Signed::offset_report_key`. Two lines is a truthful
    // answer; a third naming an object that was never put is not.
    // **GUARD G-TS, AND IT GOES BEFORE INTERFACE I8's KEYS.** Plan erratum
    // **E10(c)**: `Restore.status.topicPreflight` was declared with no
    // producer, and this line is it — one key, one line, scanned by name out
    // of the controller's bounded tail exactly as the evidence keys are
    // (erratum E4). It is printed BEFORE the three key lines so I8's "the
    // FINAL stdout lines, with nothing after them" is unchanged; a reader that
    // took the last line would still take a key.
    //
    // Printed only on exit 0, for the same reason the keys are: a refused or
    // crashed run has no completed phase 0 to report, and a line naming a
    // preflight nobody performed would be worse than the absence the operator
    // already renders.
    if let (ExitCode::Ok, Some(p)) = (code, topic_preflight) {
        println!(
            "{}{}",
            phase0_admit::TOPIC_PREFLIGHT_KEY_PREFIX,
            p.status_line_value()
        );
    }
    if let (ExitCode::Ok, Some(e)) = (code, evidence) {
        println!("scorecard-key={}", e.scorecard_key);
        println!("sidecar-key={}", e.sidecar_key);
        if let Some(k) = &e.offset_report_key {
            println!("offset-report-key={k}");
        }
    }
    code
}

/// The scorecard file is written by `write_scorecard_artifact` from the bytes
/// phase 8 signed, before phase 9 can touch the in-memory document. This writes
/// the metrics and the stdout line only.
fn finish(args: &RunArgs, sc: &Scorecard) {
    if let Some(p) = &args.metrics_file {
        if let Err(e) = crate::metrics::write_textfile(p, sc) {
            tracing::warn!(error = %e, path = %p.display(), "metrics textfile not written");
        }
    }
    println!("{}", summary_line(sc));
}

/// The value `last_phase_completed` carries in the SIGNED document.
///
/// `phase8_score::run` is handed a frozen clone, so phase 8's own record — and
/// phase 9's, which happens after the put — are pushed onto the in-memory
/// document AFTER the bytes were signed. Both are legitimate facts about the
/// run and neither can be in the artifact, so a successful drill genuinely
/// ends at `last_phase_completed: 9` in memory and `7` on disk.
///
/// THE SIGNED ARTIFACT IS THE AUTHORITY. An auditor reads the console line
/// beside the artifact, and it printed `last phase completed 9` for a document
/// that says `7` — a contradiction whose only documented resolution
/// (`docs/formats/drill-scorecard.md`) would have led them to conclude
/// teardown never ran, when in fact it did and is attested in a separate
/// signed document. So the line quotes the artifact.
///
/// The definition is exactly `record`'s, restricted to the phases that can be
/// in the signed bytes: the highest phase below 8 that completed. It is right
/// on all three signing paths — the phase-5 `Verdict::Block` jump (5), the
/// phase-6 `RestoreNoOp` interception (5, because phase 6 errored and `record`
/// does not advance on an error) and the normal run (7) — and it is pinned
/// against a real signed document, not against itself, by
/// `the_stdout_line_quotes_the_signed_artifact_never_the_in_memory_copy`.
pub fn signed_last_phase_completed(sc: &Scorecard) -> i8 {
    sc.phases
        .iter()
        .filter(|p| p.phase < 8 && p.outcome == "ok")
        .map(|p| p.phase)
        .max()
        .unwrap_or(-1)
}

/// One line on stdout so `drill run` is not silent.
///
/// This helper carried a "Task 21b replaces this call with
/// `crate::show::render_table(sc)` … this helper is then deleted" marker. Task
/// 21b landed the table, it is tested against the compiled binary, and the
/// replacement was NOT made — `drill run` deliberately prints one line and
/// leaves the fourteen-row table to `drill show`, which is a separate command
/// an operator runs against the artifact. The marker described an edit nobody
/// intends to make, so it is gone rather than left to age.
///
/// Every value here is the SIGNED document's, in the SIGNED document's
/// spelling: `outcome_str` is `Outcome::wire_name`, and the phase number is
/// `signed_last_phase_completed`. A console line an operator cannot reconcile
/// with the artifact it just wrote is worse than no console line.
///
/// T0-11's clause is CONDITIONAL, and that is a requirement rather than a
/// convenience. An unconditional suffix — "teardown left 0 scratch topics
/// behind" — would change the console line of EVERY clean run, which is churn
/// a reviewer cannot distinguish from a regression, and would put a reassuring
/// sentence in front of an operator on the ninety-nine runs where nothing was
/// wrong, teaching them to skim past it on the hundredth. The metric is the
/// surface that must be unconditional (an absent Prometheus series and a clean
/// run are indistinguishable to PromQL); a console line has a reader who is
/// already looking, and for them silence means clean.
pub fn summary_line(sc: &Scorecard) -> String {
    let mut line = format!(
        "run {} — outcome {} — last phase completed {} (as signed; \
         phase 8's own record and phase 9's teardown are written after the \
         bytes are frozen, and teardown is attested separately)",
        sc.run_id,
        crate::metrics::outcome_str(&sc.outcome),
        signed_last_phase_completed(sc)
    );
    let failed = phase9_teardown::failed_count(sc);
    if failed > 0 {
        line.push_str(&format!(
            " — teardown left {failed} scratch {} behind ({})",
            if failed == 1 { "topic" } else { "topics" },
            phase9_teardown::failed_topic_names_from_notes(sc).join(", ")
        ));
    }
    line
}

/// The local artifact is the EXACT byte string phase 8 signed, so `drill verify`
/// on the file recomputes the digest the signature covers. Never a
/// re-serialisation: phase 9 pushes a record onto the in-memory scorecard after
/// this point, and a document that grew a phase after signing does not verify.
///
/// The DSSE sidecar lands beside it with the extension replaced by `.sig`
/// (`crates/logweir/src/cli.rs`'s own `--out` documentation), because a
/// scorecard file nobody can verify locally is not evidence.
fn write_scorecard_artifact(args: &RunArgs, run_id: &str, signed: &phase8_score::Signed) {
    let out = args
        .out
        .clone()
        .unwrap_or_else(|| PathBuf::from(format!("./logweir-{run_id}.json")));
    if let Err(e) = std::fs::write(&out, &signed.bytes) {
        tracing::warn!(error = %e, path = %out.display(), "scorecard artifact not written");
        return;
    }
    let sig = out.with_extension("sig");
    match serde_json::to_vec(&signed.sidecar) {
        Ok(b) => {
            if let Err(e) = std::fs::write(&sig, &b) {
                tracing::warn!(error = %e, path = %sig.display(), "DSSE sidecar not written");
            }
        }
        Err(e) => tracing::warn!(error = %e, "DSSE sidecar could not be serialised"),
    }
}

/// The target-cluster handle the orchestrator holds. Phases 0, 2, 6 and 7 read
/// through `ClusterReader`; phase 9 deletes through `TopicDeleter`. They are
/// two traits on purpose (see `TopicDeleter`'s own doc comment), and this is
/// the one place the drill needs both from a single object — `RdKafkaReader`
/// on the live path, a double in `tests/fixtures/mod.rs`.
pub trait TargetClient: ClusterReader + TopicCreator + TopicDeleter {
    fn as_reader(&self) -> &dyn ClusterReader;
    /// **Guard G-TS**, Task 8. The SECOND write seam, beside `as_deleter`:
    /// since `restore.yaml` renders `create_topics: false`, the target topics
    /// are created here and nowhere else. It is a separate trait rather than a
    /// method on `ClusterReader` for the same reason `TopicDeleter` is — a
    /// reader is read-only — and it is on `TargetClient` so `crates/logweir`
    /// still never takes an rdkafka dependency.
    fn as_creator(&self) -> &dyn TopicCreator;
    fn as_deleter(&self) -> &dyn TopicDeleter;
}

impl<T: ClusterReader + TopicCreator + TopicDeleter> TargetClient for T {
    fn as_reader(&self) -> &dyn ClusterReader {
        self
    }
    fn as_creator(&self) -> &dyn TopicCreator {
        self
    }
    fn as_deleter(&self) -> &dyn TopicDeleter {
        self
    }
}

/// Everything one drill run holds that is NOT produced by a phase. `context`
/// is the only place a broker, a bucket or an engine handle is constructed,
/// which is what keeps the phase functions pure enough to unit-test — and
/// what lets `tests/orchestrator.rs` drive the whole sequence against doubles
/// with no broker and no engine binary.
///
/// Every field here is built WITHOUT a network round trip. That is
/// load-bearing, not incidental: phase 0 must be able to REFUSE a plan (exit
/// 3) on a host whose archive bucket is unreachable and whose engine binary
/// is absent. Reading the archive from here — as an earlier draft of this
/// task did — turns every such refusal into exit 1, "Logweir could not do its
/// job", which is precisely the exit-code confusion this task exists to get
/// right. The archive read therefore lives in `execute_with`, AFTER phase 0
/// and phase 1; `crates/logweir/tests/guard_cli.rs` pins the consequence.
pub struct Ctx {
    pub spec: DrillSpec,
    pub spec_text: String,
    pub allowed: AllowedClusters,
    pub client: Box<dyn TargetClient>,
    pub engine: Box<dyn DataEngine>,
    /// Reads the OSO ARCHIVE. Phase 7 reads segment bytes back through this
    /// handle; `Store::read_only_from_url` builds one that physically cannot
    /// put, so Global Constraint 6 cannot be reached from the archive side.
    pub archive: Store,
    /// Writes the EVIDENCE. Two stores, deliberately: the ARCHIVE is read from
    /// `spec.source.storage` and the EVIDENCE is written to `spec.evidence`,
    /// which Global Constraint 6 forces under `logweir/` and, where the
    /// adopter provides one, a separate bucket with a separate principal.
    /// `Store::from_url` refuses an evidence prefix outside `logweir/` at
    /// construction, so a bad spec fails before phase 0.
    pub store: Store,
}

fn context(spec_text: String, allowed_text: String) -> Result<Ctx, DrillError> {
    let spec: DrillSpec = serde_yaml::from_str(&spec_text)
        .map_err(|e| DrillError::Operational(format!("drill spec does not parse: {e}")))?;
    let allowed: AllowedClusters = serde_json::from_str(&allowed_text)
        .map_err(|e| DrillError::Operational(format!("allowed-clusters does not parse: {e}")))?;

    // **Interface I1.** Was a hard-coded plaintext arm with a comment saying
    // `TargetSpec` carried no auth block to render one from;
    // Task 6 gave `TargetSpec` that block, so the mode is the spec's and this
    // is the one construction site
    // (`logweir_kafka::reader::AuthConfig::from_spec`).
    //
    // The environment read happens ONCE, here, and not inside the closure:
    // the closure is called twice (see the `with_scratch_prefix` fallback
    // below) and `AuthConfig` is `Clone`, so re-reading would be two reads of
    // a Secret for one run. An unrenderable value refuses with exit 3 before
    // any client exists; an ABSENT value under `mode: scramSha512` is exit 1,
    // named by `naming_the_password_var`.
    let target_auth =
        AuthConfig::from_spec(&spec.target.auth, validated_password(TARGET_PASSWORD_VAR)?)
            .map_err(|e| naming_the_password_var(e, TARGET_PASSWORD_VAR))?;
    let connect = || {
        logweir_kafka::rdkafka_reader::RdKafkaReader::connect(
            &spec.target.bootstrap_servers,
            target_auth.clone(),
        )
    };
    // Scope deletion to this drill's own scratch namespace before the handle
    // ever reaches phase 9. An UNSCOPED reader can delete nothing at all
    // (`RdKafkaReader::delete_topics` refuses everything until this is set),
    // which is the safe direction: a degenerate prefix is refused by phase
    // 0's own mapping guard as a REFUSAL (exit 3), and turning it into an
    // exit-1 construction failure here would hide that.
    let reader = match connect()?.with_scratch_prefix(spec.target.topic_mapping_prefix.clone()) {
        Ok(r) => r,
        Err(e) => {
            tracing::warn!(
                error = %e,
                "target.topic_mapping_prefix is not a usable scratch namespace; teardown is \
                 disabled for this run and phase 0 is left to refuse the plan"
            );
            connect()?
        }
    };

    let store =
        Store::from_url(&spec.evidence).map_err(|e| DrillError::Operational(e.to_string()))?;
    let archive = Store::read_only_from_url(&spec.source.storage)
        .map_err(|e| DrillError::Operational(e.to_string()))?;
    // A SECOND read-only handle over the same archive: `Store` is not `Clone`
    // and `OsoCliEngine` takes ownership of the one it reads through, while
    // phase 7 reads segment bytes through `Ctx::archive`. Both are read-only.
    let engine_archive = Store::read_only_from_url(&spec.source.storage)
        .map_err(|e| DrillError::Operational(e.to_string()))?;

    // Engine identity. The binary is extracted at image build time from the
    // digest-pinned image; the version and digest describe THAT image and end
    // up in the signed scorecard, so they are read from the environment the
    // image sets rather than guessed here. `execute_with` refuses an empty
    // version or digest immediately after phase 0 — see `assert_engine_identity`
    // — rather than here, so a plan the guard would REFUSE still exits 3 on a
    // host with no engine environment at all.
    //
    // `crate::engine_bin::engine_path` and NOT a local default: this line used
    // to read `$LOGWEIR_ENGINE_BIN` or fall back to the literal
    // `.engine/kafka-backup`, while `logweir doctor` walked
    // `$LOGWEIR_ENGINE_BIN` -> `/usr/local/bin/kafka-backup` -> a `$PATH` scan.
    // `Command::new` performs no `$PATH` search for a path containing `/`, so
    // the install `README.md` documents (the engine on `$PATH`) gave a green
    // `doctor` — seven `ok` lines, including `ok engine version` — and then a
    // drill that read the target cluster and the archive manifest through
    // phases 0-4 and died at phase 5 the first time it tried to execute the
    // engine. One resolution now, for both commands.
    let binary = crate::engine_bin::engine_path();
    let version = std::env::var("LOGWEIR_ENGINE_VERSION").unwrap_or_default();
    let digest = std::env::var("LOGWEIR_ENGINE_DIGEST").unwrap_or_default();
    // Pod-local scratch for the rendered restore.yaml / validation.yaml. Never
    // uploaded: a crashed restore is not resumable in v0.1 (spec §11).
    let workdir = std::env::temp_dir().join(format!("logweir-{}", std::process::id()));
    std::fs::create_dir_all(&workdir)
        .map_err(|e| DrillError::Operational(format!("{}: {e}", workdir.display())))?;
    let engine = logweir_engine_oso::engine::OsoCliEngine::new(
        binary,
        version,
        digest,
        workdir,
        engine_archive,
    );

    Ok(Ctx {
        spec,
        spec_text,
        allowed,
        client: Box::new(reader),
        engine: Box::new(engine),
        archive,
        store,
    })
}

/// The phase sequence, over handles this function does not build. `pub`
/// because `tests/orchestrator.rs` drives it directly: it is the only check
/// that phases 7 and 8 are wired in the scoring order, and a `Ctx` built from
/// doubles is the only way to reach that check without a live broker and the
/// engine binary (which belong to Task 21c).
/// The two environment variables through which a projected SASL password
/// reaches this process. Named here, once, so the runner's read and Task 6's
/// rendered `${…}` placeholder cannot drift
/// (`logweir_engine_oso::yaml::PLACEHOLDER_SOURCE_PASSWORD`).
pub const SOURCE_PASSWORD_VAR: &str = "LOGWEIR_SOURCE_PASSWORD";
/// The target cluster's twin of `SOURCE_PASSWORD_VAR`.
pub const TARGET_PASSWORD_VAR: &str = "LOGWEIR_TARGET_PASSWORD";

/// Interface **I11**, the runner's half: validate every projected password
/// this process can see, at the moment it reads it, and REFUSE an
/// unrenderable one before anything runs.
///
/// # Why the runner and not the controller
///
/// `weirkeeper` holds no `get` on Secrets anywhere (spec §9), so it never sees
/// the projected value and has nothing to validate. Spec §7 amendment 4 —
/// "the runner refuses, not the controller" — settles it in writing, and
/// Task 20 forbids the controller re-using the predicate. This is the only
/// place in the workspace that calls `credential_is_renderable`.
///
/// # Why here, before `context`
///
/// Global Constraint 11 reserves exit 3 for "refused by a guard, BEFORE
/// anything runs". This runs ahead of `context`, so no broker client and no
/// object-store handle is constructed on the refusal path, and ahead of phase
/// 0, so the refusal cannot be confused with a spec refusal.
///
/// # Scope, precisely
///
/// It validates the variables that are PRESENT, whatever the spec asks for.
/// Since Task 6 a `DrillSpec` CAN ask for SASL (`spec.target.auth`), so a
/// projected value normally has a consumer — but the check is deliberately
/// not conditional on the mode: a value that cannot be substituted into
/// pre-parse text is a fact worth refusing on before anything runs, and a
/// spec switched back to plaintext with the Secret still projected is the
/// case where a mode-conditional check would let it through.
///
/// It runs a SECOND time, per variable, at the moment of use
/// (`validated_password`, which is this function's own body). That is not
/// redundancy to be tidied away: the predicate is pure, and the two calls
/// answer two questions — "is this process's environment renderable at all"
/// and "is the value I am about to hand a client renderable".
///
/// A non-UTF-8 value is refused, not skipped: it cannot be substituted into a
/// text document at all, and `std::env::var`'s `NotUnicode` is exactly the
/// case a bare `if let Ok(..)` would silently admit.
pub fn check_projected_credentials() -> Result<(), logweir_core::guard::GuardRefusal> {
    for var in [SOURCE_PASSWORD_VAR, TARGET_PASSWORD_VAR] {
        validated_password(var)?;
    }
    Ok(())
}

/// Interface **I1**'s environment half: read one variable, validate it, and
/// hand back what `logweir_kafka::reader::AuthConfig::from_spec` takes.
///
/// **`Ok(None)` for an ABSENT variable, deliberately.** Whether absence is
/// fatal depends on the spec's `auth.mode`, which this function does not see;
/// `from_spec` is the half that does, and it answers with an operational
/// `KafkaError` (exit 1), never a refusal. Splitting it that way is what keeps
/// the exit-3 case ("this value can never be rendered") and the exit-1 case
/// ("nobody projected the Secret yet") from collapsing into one code — which
/// would tell Task 18's cron reconciler to retry a plan forever, or to stop
/// retrying a condition an operator is about to fix.
///
/// It refuses (exit 3, `refusal-reason=CredentialNotRenderable`) exactly what
/// `check_projected_credentials` refuses, by calling the same code: this IS
/// that function's per-variable body, so the guard the runner applies at
/// startup and the guard applied at the moment of use cannot drift.
///
/// **The value never reaches a log, an error message or a return path other
/// than the caller's `AuthConfig`.** `CredentialRefusal`'s `Display` is a pure
/// function of the offending character CLASS (`logweir_core::guard`), so two
/// different unrenderable passwords with the same first offending character
/// produce byte-identical messages.
pub fn validated_password(var: &str) -> Result<Option<String>, logweir_core::guard::GuardRefusal> {
    let secret = match std::env::var(var) {
        Ok(v) => v,
        Err(std::env::VarError::NotPresent) => return Ok(None),
        Err(std::env::VarError::NotUnicode(_)) => {
            return Err(logweir_core::guard::GuardRefusal(format!(
                "{}: the value projected into `{var}` is not valid UTF-8, so it cannot be \
                 substituted into the engine's config text at all. The refusal names the \
                 variable and never the value.",
                logweir_core::guard::TERMINAL_STATE_CREDENTIAL_NOT_RENDERABLE
            )))
        }
    };
    if let Err(refusal) = logweir_core::guard::credential_is_renderable(&secret) {
        // The message OPENS with the terminal state, which is what
        // `logweir_core::guard::terminal_state` matches on and therefore
        // what `refusal-reason=CredentialNotRenderable` depends on. The
        // refusal's own `Display` names the character class and is a pure
        // function of that class, so no fragment of the secret can reach
        // this string.
        return Err(logweir_core::guard::GuardRefusal(format!(
            "{}: {refusal} (projected into `{var}`)",
            logweir_core::guard::TERMINAL_STATE_CREDENTIAL_NOT_RENDERABLE
        )));
    }
    Ok(Some(secret))
}

/// `AuthConfig::from_spec`'s "no password was projected" error, re-stated with
/// the variable THIS process actually read.
///
/// `from_spec` lives in `logweir-kafka`, takes `Option<String>` (interface
/// I1's pinned signature) and therefore cannot know which of the two variables
/// a caller took — so the crate that does know says so. Only the `Client` arm
/// is rewritten; every other `KafkaError` is a real broker fact and is passed
/// through untouched, because a message about an environment variable would be
/// a lie about a timeout.
pub fn naming_the_password_var(
    e: logweir_kafka::reader::KafkaError,
    var: &str,
) -> logweir_kafka::reader::KafkaError {
    match e {
        logweir_kafka::reader::KafkaError::Client(_) => {
            logweir_kafka::reader::KafkaError::Client(format!(
                "auth.mode is scramSha512 but ${var} is unset. Nothing was refused: this is \
                 operational (exit 1), not a guard refusal (exit 3) — project the Secret and \
                 re-run. The engine would otherwise substitute the EMPTY STRING for the \
                 placeholder behind nothing but a warning \
                 [U:crates/kafka-backup-cli/src/commands/config.rs:20-27, verified by \
                 execution against the pinned engine] and attempt an unauthenticated \
                 connection, which is why this is checked before the engine is spawned."
            ))
        }
        other => other,
    }
}

/// Returns the whole `RestoreOutcome`, not the scorecard alone, because
/// interface **I8**'s three stdout lines are keys the run PUT AT and `report`
/// is what prints them. `execute_with` stays the scorecard-returning half for
/// the ~40 existing call sites in `tests/orchestrator.rs` and
/// `tests/teardown.rs`.
struct StartupInputs {
    spec_text: String,
    allowed_text: String,
    approved: phase1_approval::Approved,
}

fn read_startup_file(path: &std::path::Path, label: &str) -> Result<Vec<u8>, DrillError> {
    std::fs::read(path)
        .map_err(|error| DrillError::Operational(format!("{label} {}: {error}", path.display())))
}

fn load_startup_inputs(
    args: &RunArgs,
    signing_key: &logweir_evidence::keys::VerifyingKey,
    contract: Option<&ExecutionContract>,
) -> Result<StartupInputs, DrillError> {
    let sidecar_path = args.approval.with_extension("sig");
    let bundle = ApprovalBundleBytes {
        plan: read_startup_file(&args.spec, "restore plan")?,
        approval: read_startup_file(&args.approval, "approval")?,
        approval_sidecar: read_startup_file(&sidecar_path, "approval sidecar")?,
        approver_key: read_startup_file(&args.approver_key, "approver public key")?,
        allowed_clusters: read_startup_file(&args.allowed_clusters, "allowed-clusters")?,
    };
    if let Some(contract) = contract {
        validate_execution_contract(contract, args.triggered_by.as_deref(), &bundle)?;
    }
    phase1_approval::admit_pinned_approver_key_bytes(&bundle.approver_key, &args.approver_key_ids)?;
    let spec_text = String::from_utf8(bundle.plan.clone())
        .map_err(|error| DrillError::Operational(format!("restore plan is not UTF-8: {error}")))?;
    let allowed_text = String::from_utf8(bundle.allowed_clusters.clone()).map_err(|error| {
        DrillError::Operational(format!("allowed-clusters is not UTF-8: {error}"))
    })?;
    let approved = phase1_approval::verify_bytes(
        &spec_text,
        &bundle.approval,
        &bundle.approval_sidecar,
        &bundle.approver_key,
        signing_key,
    )?;
    Ok(StartupInputs {
        spec_text,
        allowed_text,
        approved,
    })
}

pub fn execute(args: &RunArgs, run_id: &str) -> Result<RestoreOutcome, DrillError> {
    execute_for_reporting(args, run_id).0
}

/// Execute while retaining notification authority only after the exact plan
/// bytes have passed the argv/environment contract and approval verification.
/// The retained value is independent of any later projected-file replacement.
fn execute_for_reporting(
    args: &RunArgs,
    run_id: &str,
) -> (Result<RestoreOutcome, DrillError>, Option<DrillSpec>) {
    let contract = match execution_contract_for_invocation(
        args.execution_contract_version.as_deref(),
        |name| std::env::var(name).ok(),
    ) {
        Ok(contract) => contract,
        Err(error) => return (Err(error), None),
    };
    // I11, and BEFORE `context`: no client of any kind is constructed on this
    // refusal path.
    if let Err(error) = check_projected_credentials() {
        return (Err(error.into()), None);
    }
    // The pinned approver set, at the SAME seam and for the same reason (Task
    // 22 fix round 1, review finding MED-1). It began one frame lower, at the
    // top of `execute_with_outcome` — which is already past `context(args)`,
    // and `context` constructs the rdkafka client, whose CONSTRUCTION alone
    // begins bootstrap connections. So a refused run dialled the spec's
    // bootstrap before printing a refusal whose own words say it had not:
    // measured against a closed port, the process emitted
    // `FAIL … Connect to ipv4#127.0.0.1:19092 … Connection refused` on stderr
    // and a `BrokerTransportFailure` line on stdout, and only then the
    // refusal. The guard reads nothing from `Ctx` — only `args.approver_key`
    // (a file on disk) and `args.approver_key_ids` (an argv value) — so there
    // was never a reason for it to sit behind the client. Here the shipped
    // message's "before phase 0 dials anything" is true at the socket layer,
    // which is what `tests/approval.rs::
    // an_unpinned_approver_is_refused_without_dialling_the_bootstrap` asserts
    // over the whole transcript of a run against a closed port.
    if let Err(error) =
        phase1_approval::admit_pinned_approver_key_id(&args.approver_key, &args.approver_key_ids)
    {
        return (Err(error), None);
    }
    // Signing readiness is established before `context` constructs the Kafka
    // client, object stores, or engine runner. Keep this parsed identity for
    // approval comparison and every evidence document the run emits.
    let signer = match load_signer(&args.signing_key) {
        Ok(signer) => signer,
        Err(error) => return (Err(error), None),
    };
    let startup = match load_startup_inputs(args, &signer.verifying_key(), contract.as_ref()) {
        Ok(startup) => startup,
        Err(error) => return (Err(error), None),
    };
    let authenticated_spec: DrillSpec = match serde_yaml::from_str(&startup.spec_text) {
        Ok(spec) => spec,
        Err(error) => {
            return (
                Err(DrillError::Operational(format!(
                    "drill spec does not parse: {error}"
                ))),
                None,
            )
        }
    };
    let outcome = match context(startup.spec_text, startup.allowed_text) {
        Ok(c) => execute_with_prevalidated(args, run_id, &c, &signer, startup.approved),
        Err(error) => Err(error),
    };
    (outcome, Some(authenticated_spec))
}

fn load_signer(path: &std::path::Path) -> Result<ValidatedSigner, DrillError> {
    ValidatedSigner::load(
        path,
        logweir_evidence::PAYLOAD_TYPE_SCORECARD,
        b"logweir restore signing readiness probe v1",
        "No engine data operation was started",
    )
    .map_err(DrillError::SigningPrerequisite)
}

/// Refuses an engine identity that would enter a signed document empty.
///
/// `engine.digest` is the pinned image digest an auditor uses to say WHICH
/// engine produced this restore; `engine.version` is the tag the support
/// matrix is keyed on. An empty string in either is not "unknown", it is a
/// signed document that names no engine at all — so this is a hard refusal,
/// not a warning. It runs after phase 0 on purpose: a plan the admission
/// guard REFUSES must exit 3 even on a host with no engine environment.
fn assert_engine_identity(id: &logweir_core::engine::EngineId) -> Result<(), DrillError> {
    for (field, value, var) in [
        ("engine.version", &id.version, "LOGWEIR_ENGINE_VERSION"),
        ("engine.digest", &id.digest, "LOGWEIR_ENGINE_DIGEST"),
    ] {
        if value.trim().is_empty() {
            return Err(DrillError::Operational(format!(
                "{field} is empty; a signed scorecard must name the engine image it ran. \
                 Set {var} to the value of the digest-pinned image this binary was \
                 extracted from."
            )));
        }
    }
    Ok(())
}

pub fn execute_with(args: &RunArgs, run_id: &str, c: &Ctx) -> Result<Scorecard, DrillError> {
    execute_with_outcome(args, run_id, c).map(|o| o.scorecard)
}

/// The same phase sequence as `execute_with`, returning everything ONE RUN
/// produced rather than the scorecard alone.
///
/// `TopicPreflight` (guard **G-TS**) is deliberately not a scorecard field —
/// Global Constraint 12 as amended permits nested optional fields only and no
/// new top-level property, and the scorecard is frozen at 21/17 — so it needs a
/// carrier out of the run. `RestoreOutcome` is it: spec §10's G-TS row puts the
/// preflight in `Restore.status.topicPreflight`, which the operator writes from
/// the runner's outcome, and it goes no further inside this repository's
/// evidence documents.
///
/// `execute_with` stays the scorecard-returning half so the ~40 existing call
/// sites in `tests/orchestrator.rs` and `tests/teardown.rs` are untouched.
pub fn execute_with_outcome(
    args: &RunArgs,
    run_id: &str,
    c: &Ctx,
) -> Result<RestoreOutcome, DrillError> {
    // A caller supplying already-built doubles still gets the same safety
    // boundary: validate before the first method call on the client or engine.
    let signer = load_signer(&args.signing_key)?;
    execute_with_signer(args, run_id, c, &signer)
}

fn execute_with_signer(
    args: &RunArgs,
    run_id: &str,
    c: &Ctx,
    signer: &ValidatedSigner,
) -> Result<RestoreOutcome, DrillError> {
    execute_with_validated_approval(args, run_id, c, signer, None)
}

fn execute_with_prevalidated(
    args: &RunArgs,
    run_id: &str,
    c: &Ctx,
    signer: &ValidatedSigner,
    approved: phase1_approval::Approved,
) -> Result<RestoreOutcome, DrillError> {
    execute_with_validated_approval(args, run_id, c, signer, Some(approved))
}

fn execute_with_validated_approval(
    args: &RunArgs,
    run_id: &str,
    c: &Ctx,
    signer: &ValidatedSigner,
    prevalidated_approval: Option<phase1_approval::Approved>,
) -> Result<RestoreOutcome, DrillError> {
    // `--approver-key-ids` is deliberately NOT checked here, though phase 1 is
    // where the approval is otherwise handled. This function takes a `&Ctx`,
    // so by the time it runs the rdkafka client already exists and the
    // bootstrap has already been contacted — Global Constraint 11 reserves
    // exit 3 for a refusal made "before anything ran", and that promise cannot
    // be kept from behind a constructed client. The check therefore lives in
    // `execute`, beside I11's `check_projected_credentials()` and ahead of
    // `context`, which is the only place it CAN be kept (Task 22 fix round 1,
    // MED-1). Nothing is lost on this path: every in-repo caller of this
    // function, of `execute_with` and of `run_with` drives it over doubles
    // with an EMPTY `approver_key_ids` (`tests/fixtures/mod.rs`), for which
    // the guard returns `Ok` unconditionally.
    let mut sc = new_scorecard(run_id, args, c);
    // Phase 9 deletes through the SAME client, and since Task 8 phase 0's
    // preflight and the target-topic creation step write through it too. Named
    // here so the layering rule — `logweir-kafka` is the only crate that dials
    // a broker — is visible.
    let deleter: &dyn TopicDeleter = c.client.as_deleter();
    let creator: &dyn TopicCreator = c.client.as_creator();
    let reader: &dyn ClusterReader = c.client.as_reader();

    // 0
    let admitted = record(&mut sc, 0, "admit", || {
        phase0_admit::run(&c.spec, &c.spec_text, &c.allowed, reader, creator, deleter)
    })?;
    let mut topic_preflight = admitted.topic_preflight.clone();
    sc.target = target_info(&c.spec, &admitted)?;
    assert_engine_identity(&c.engine.id())?;

    // 1
    let signing_pub = signer.verifying_key();
    let approved = record(&mut sc, 1, "approval", || match &prevalidated_approval {
        Some(approved) => Ok(approved.clone()),
        None => phase1_approval::verify(
            &c.spec_text,
            &args.approval,
            &args.approver_key,
            &signing_pub,
        ),
    })?;
    sc.approval = approved.approval.clone();
    sc.approval_validated_at = Some(approved.validated_at);

    // The archive read every later phase consumes. Not a phase of its own in
    // spec §9.3, and deliberately placed HERE: phases 0 and 1 are the two
    // gates that can refuse a plan, and neither of them touches the archive,
    // so a refused plan never opens the bucket. `set` is also what
    // `Selection::bind_backup_set` needs below — `BackupSetFacts` does not
    // carry a manifest key, so this is the only binding of it the drill has.
    let set = pick_backup_set(c.engine.as_ref(), &c.spec)?;
    let facts = c.engine.describe(&set)?;
    sc.source = source_info(&facts);

    // 2
    let of_interest: Vec<String> = admitted.topic_mapping.values().cloned().collect();
    let target = record(&mut sc, 2, "target-ready", || {
        phase2_target::run(reader, &of_interest)
    })?;
    // 3 — the diff REACHES A READER, which is the whole point of phase 3
    let diff = record(&mut sc, 3, "target-diff", || {
        Ok(phase3_diff::run(&target, &facts, &admitted.topic_mapping))
    })?;
    sc.target_diff = diff.summarise();
    // 4
    let src_topics: Vec<String> = admitted.topic_mapping.keys().cloned().collect();
    let mut sel = record(&mut sc, 4, "sample-select", || {
        phase4_sample::run(&facts, &c.spec.sample, &src_topics)
    })?;
    // Binding note appended to the brief during Task 16 fix round 1:
    // `phase4_sample::run` emits `per_partition[..].set.manifest_key` EMPTY,
    // and `OsoCliEngine::fingerprints` refuses loudly if it is still empty
    // when phase 7 calls it. This is the only place the real `BackupSetRef`
    // and the `Selection` are both in scope.
    sel.bind_backup_set(&set);
    sc.sample = sample_info(&c.spec.sample, &sel);

    // 5 — the engine's preflight runs INSIDE the phase-5 record, so the
    // record's own `duration_ms` is the number `compute_measured` subtracts
    // to produce `rto_excluding_preflight_seconds`. Measuring it anywhere
    // else would score the header sweep no incident responder performs.
    //
    // T0-14: this single `plan` binding, handed to both the phase-5 closure
    // below and `phase6_restore::run` further down, USED to be the only reason
    // the two rendered `restore.yaml` documents were the same document. It is
    // no longer load-bearing: `OsoCliEngine::preflight` now hashes the exact
    // bytes it writes and `OsoCliEngine::restore` re-renders, re-hashes and
    // refuses a divergence (ruling R-E: `EngineError::Operational`, exit 1, no
    // artifact — see `docs/stability.md`). Building the plan once is now a
    // convenience, not the guarantee, so a future phase-5/phase-6 split cannot
    // dissolve the identity by moving these two call sites apart.
    let plan = build_plan(
        &c.spec,
        &set,
        &admitted.topic_mapping,
        &facts,
        run_id,
        args.offset_report_out.as_deref(),
    )?;
    // The engine writes its restore checkpoint to `plan.checkpoint_state` and
    // does NOT create that file's parent directory. `context` creates the
    // workdir it renders restore.yaml into (`logweir-<pid>`); the checkpoint
    // lives beside it under `logweir-<run_id>`, which nothing created — so
    // `kafka-backup restore` exited 1 with a bare
    // `IO error: No such file or directory (os error 2)` on EVERY run, on any
    // host. Found by Task 21c the first time the orchestrator was pointed at a
    // real archive; before that, no test in the workspace ever reached the
    // engine's `restore` subcommand with a real binary behind it.
    //
    // `Operational`, not a guard refusal: a scratch directory that cannot be
    // created says nothing about the archive.
    if let Some(dir) = plan.checkpoint_state.parent() {
        std::fs::create_dir_all(dir)
            .map_err(|e| DrillError::Operational(format!("{}: {e}", dir.display())))?;
    }
    // And the offset report's parent, for exactly the same reason and with
    // one difference that makes it worse: the engine's checkpoint write
    // surfaces as an error, while its offset-report write is wrapped in a
    // `warn!` [U:crates/kafka-backup-core/src/restore/engine.rs:423-425] — so
    // a missing directory there produces no file, no failure, and no signed
    // `evidence.offset_report_key`. By default this is the same directory the
    // line above just created; it is a separate call because
    // `--offset-report-out` may name any path the operator likes.
    if let Some(dir) = plan.offset_report.parent() {
        std::fs::create_dir_all(dir)
            .map_err(|e| DrillError::Operational(format!("{}: {e}", dir.display())))?;
    }
    let (verdict, report) = record(&mut sc, 5, "preflight", || {
        // **GUARD G-WIN, THE REFUSING HALF**, and it runs BEFORE
        // `engine.preflight` writes anything: `check_rendered_window_floor`
        // renders the document itself, reads the `time_window_start` line back
        // off the bytes, and compares THAT against a floor it re-derives from
        // the manifest. Exit 3, "refused by a guard, before anything ran" —
        // NOT the exit 1 of ruling R-E's phase-5/phase-6 render mismatch,
        // which is a different failure at a later point (see the function's
        // own doc comment).
        phase5_preflight::check_rendered_window_floor(&plan, &facts)?;
        let r = c.engine.preflight(&plan)?;
        Ok((phase5_preflight::adjudicate(&r), r))
    })?;
    // The per-run lever readback (spec §9.3 phase 5(1), §7.2(a)). Observed,
    // never declared: `new_scorecard` starts both fields at their
    // claims-nothing values and only this line can raise them.
    sc.engine.levers.header_preflight = if report.header_preflight_honoured {
        LeverState::Honoured
    } else {
        LeverState::Ignored
    };
    sc.engine.levers.unknown_key_warnings = report.unknown_key_warnings.clone();
    if report.header_preflight_honoured {
        sc.engine.matrix_verdict = MatrixVerdict::Pass;
    }

    if let phase5_preflight::Verdict::Block { findings } = verdict {
        // Straight to phase 8: score, sign, upload. NEVER phase 6, and never
        // exit 1 — a preflight finding is the most valuable result a drill can
        // produce and it must land in a signed artifact.
        sc.outcome = Outcome::PreflightFailed;
        sc.integrity.level = IntegrityLevel::NotAttempted;
        sc.integrity.result = IntegrityResult::Fail;
        sc.measured.rto_seconds = None;
        // Carried obligation from Task 17: `PhaseRecord.notes` exists so a
        // `preflight-failed` scorecard states WHICH ground blocked the
        // restore, not merely that one did. This orchestrator is its sole
        // writer; without this loop phase 5 adjudicates correctly and then
        // discards its reasoning.
        if let Some(p) = sc.phases.iter_mut().find(|p| p.phase == 5) {
            p.notes = findings
                .iter()
                .map(|f| format!("{}/{} {}: {}", f.topic, f.partition, f.state, f.detail))
                .collect();
        }
        // `None`: phase 5 BLOCKED, so `restore` never ran and there is no
        // offset-mapping report to describe. The engine writes one only from a
        // completed restore.
        let signed = sign_and_publish(&mut sc, args, run_id, c, signer, None)?;
        // NO teardown here, deliberately, and the asymmetry with the phase-6
        // branch below is the point: a blocked preflight means `restore` never
        // ran, so this drill created NOTHING on the target. Phase 0 now
        // refuses a plan whose mapped targets already exist (spec §6.1), so
        // those names should be absent here — but "should be" is not a fact
        // this branch established, and deleting a topic this run did not
        // create, on a plan that never executed, would destroy someone else's
        // data to tidy up after a drill that touched nothing.
        return Err(DrillError::NotPass(Box::new(signed.scorecard)));
    }

    // **Guard G-TS**, the creating half — the last thing before the restore,
    // and NOT inside phase 0. `phase0_admit::create_target_topics`'s doc
    // comment carries the reasons; the short version is that phase 1 verifies
    // the approval, phases 2 and 3 must see the target's PRE-EXISTING state,
    // and the partition count is the archive manifest's, which is read after
    // phase 1 by design.
    //
    // AFTER the phase-5 verdict, deliberately. The `Verdict::Block` branch
    // above returns without a teardown, on the stated ground that a blocked
    // preflight means `restore` never ran and this drill created NOTHING on the
    // target — `e2e/tests/full_drill.rs`'s
    // `a_corrupted_segment_yields_exit_2_and_a_signed_preflight_failed_scorecard`
    // asserts `!topic_exists("drill-orders")` and caught an earlier version of
    // this line placed before phase 5. The engine's preflight is
    // `validate-restore`, which force-sets `dry_run = true` and writes nothing,
    // so it needs no target topic; `restore` does.
    //
    // The rendered `restore.yaml` says `create_topics: false`, so this is what
    // creates them, with `TARGET_TOPIC_CONFIGS` on every one.
    phase0_admit::create_target_topics(
        creator,
        &admitted.topic_mapping,
        &facts,
        c.spec.target.default_replication_factor,
        &mut topic_preflight,
    )?;

    // 6 — the topics phase 3 said would be created are carried in through
    // `plan`, which `build_plan` produced from the same mapping.
    let mut obs = phase_observer(run_id);
    let restored = match record(&mut sc, 6, "restore", || {
        phase6_restore::run(
            c.engine.as_ref(),
            &plan,
            reader,
            &admitted.topic_mapping,
            &mut obs,
        )
    }) {
        Ok(r) => r,
        // task-21a-addendum.md ruling A8. Mirrors the phase-5 `Verdict::Block`
        // branch above: a restore that ran, exited 0, and left every selected
        // partition at end offset <= 0 is a positively established fact ABOUT
        // THE ARCHIVE, not an operational failure — never exit 1 for the most
        // valuable negative finding phase 6 can produce.
        Err(DrillError::RestoreNoOp(msg)) => {
            // `FailIntegrity`, not a new `Outcome` variant. A8 leaves the
            // variant open and demands an explicit GC12 determination; here
            // it is. GC12's text is "`format_version` is `1.0.0` and semver:
            // a minor adds optional fields only; a major changes an identity
            // rule. Readers ignore unknown fields and refuse a higher major."
            // A new `Outcome` value is NOT an added optional field: `outcome`
            // is a required field whose JSON Schema pins its allowed value
            // set, and "readers ignore unknown fields" gives no cover for an
            // unknown VALUE of a known field — the auditor's own verifier
            // (docs/verify_scorecard.py) validating against the published
            // 1.0.0 schema would REJECT `outcome: "restore-no-op"` outright.
            // That is a major change, and GC12 fixes the version at 1.0.0, so
            // minting the variant is not available to this task.
            //
            // `FailIntegrity` is also the honest answer, not merely the
            // permitted one: it is exactly what `phase8_score::decide`
            // returns for `integrity.result != Pass`, and the two lines below
            // set precisely that. What makes a no-op restore DISTINGUISHABLE
            // in the signed document from phase 5's block and from a genuine
            // phase-7 reconciliation failure is three facts a reader can
            // check: `outcome` (`fail-integrity` vs `preflight-failed`),
            // `integrity.level` (`not-attempted` vs `byte-fingerprint`), and
            // the phase-6 record itself, whose `outcome` begins
            // "failed: drill-not-pass:" and whose `notes` carry the reason.
            sc.outcome = Outcome::FailIntegrity;
            sc.integrity.level = IntegrityLevel::NotAttempted;
            sc.integrity.result = IntegrityResult::Fail;
            sc.measured.rto_seconds = None;
            if let Some(p) = sc.phases.iter_mut().find(|p| p.phase == 6) {
                p.notes = vec![msg];
            }
            // `None` for the same reason as the phase-5 branch, with one extra
            // fact: `RestoreNoOp` is the engine having exited 0 having produced
            // nothing, so any offset mapping it wrote would describe no records.
            let signed = sign_and_publish(&mut sc, args, run_id, c, signer, None)?;
            // ...and then PHASE 9 STILL RUNS. This branch differs from the
            // phase-5 one in the fact that matters here: the restore actually
            // executed, so whatever it created on the operator's cluster is
            // still there. Returning straight to exit 2 would leave scratch
            // topics behind on the one failure path where a drill wrote to
            // their broker — the residue that makes people stop running
            // drills. A8 requires the interception to reach exit 2 with a
            // signed scorecard; it says nothing about skipping cleanup, and
            // its own snippet is explicitly "not prescribed here as final
            // code". A teardown that itself fails is attested honestly by
            // `phase9_teardown` (`topics_failed`), never swallowed.
            let mut out = signed.scorecard.clone();
            teardown(
                &mut out,
                run_id,
                c,
                signer,
                deleter,
                &admitted.topic_mapping,
                &logweir_core::ids::sha256_prefixed(&signed.bytes),
            );
            return Err(DrillError::NotPass(Box::new(out)));
        }
        Err(e) => return Err(e),
    };
    // Every config key Logweir rendered that the engine dropped, from both
    // readbacks. Phase 5's were recorded above; phase 6's arrive here.
    for w in &restored.unknown_key_warnings {
        if !sc.engine.levers.unknown_key_warnings.contains(w) {
            sc.engine.levers.unknown_key_warnings.push(w.clone());
        }
    }

    // 7
    let verified = record(&mut sc, 7, "verify", || {
        phase7_verify::run(
            c.engine.as_ref(),
            reader,
            &c.archive,
            &facts,
            &sel.per_partition,
            &admitted.topic_mapping,
            &plan,
        )
    })?;
    sc.integrity = verified.integrity.clone();
    sc.topic_parity = verified.topic_parity.clone();
    sc.sample.records_restored = verified.records_restored;

    // 7 -> 8: SCORE BEFORE SIGNING. `phase8_score::run` signs the document it is
    // handed and never recomputes, so `measured`, `outcome` and `objectives`
    // must be final at this point. This is the spec §14 SP1c exit criterion —
    // "a signed scorecard whose measured.rto_seconds and measured.rpo_seconds
    // are numbers" — and it has no other implementing step.
    let timeline = phase8_score::Timeline {
        requested_at: sc.requested_at,
        approval_validated_at: approved.validated_at,
        restore_started_at: restored.started_at,
        restore_finished_at: restored.finished_at,
        phase5_duration_ms: sc
            .phases
            .iter()
            .find(|p| p.phase == 5)
            .map(|p| p.duration_ms)
            .unwrap_or(0),
        verified_at: verified.verified_at,
    };
    sc.measured = phase8_score::compute_measured(
        &timeline,
        // The requested recovery point: the END of the window this plan
        // restores, read off the plan itself so it cannot drift from what
        // `build_plan` bound and `render_restore` printed as
        // `restore.time_window_end`. Since Task 9 that is
        // `spec.restore.point_in_time` when the spec states one and
        // `spec.sample.window_end` otherwise — reading `sample.window_end`
        // here would score a `point_in_time` restore against a recovery point
        // it was never asked to reach.
        plan.time_window.1.timestamp_millis(),
        verified.newest_restored_ts_ms,
    );
    let (outcome, objectives) =
        phase8_score::decide(&sc.measured, &c.spec.objectives, &sc.integrity);
    sc.outcome = outcome;
    sc.objectives = objectives;
    // NOTHING assigns to `sc.integrity.pass_rate_measured` here, and no third
    // return value invites it to. Phase 7 set that field in `roll_up` and is
    // the only writer; the line that used to stand here overwrote phase 7's
    // deliberate `None` with a ratio recomputed from the two counters with
    // `whole_sample_reconciled` dropped — see `phase8_score::decide`'s doc
    // comment for the signed document that produced.

    // 8
    // THE ONE PATH THAT HAS A REPORT: the restore ran to completion, so the
    // engine wrote its offset mapping to `plan.offset_report` and phase 8 puts
    // those exact bytes at `logweir/drills/<run_id>.offsets.json`.
    let signed = sign_and_publish(&mut sc, args, run_id, c, signer, Some(&plan.offset_report))?;
    let signed_bytes_sha256 = logweir_core::ids::sha256_prefixed(&signed.bytes);
    // [I8] The three keys, taken off `Signed` — the value that built each
    // string and put at it — before `signed` is consumed below.
    let evidence = EvidenceKeys {
        scorecard_key: signed.key.clone(),
        sidecar_key: signed.sidecar_key.clone(),
        offset_report_key: signed.offset_report_key.clone(),
    };
    sc = signed.scorecard.clone();
    // 9
    teardown(
        &mut sc,
        run_id,
        c,
        signer,
        deleter,
        &admitted.topic_mapping,
        &signed_bytes_sha256,
    );
    // THE MAINLINE EXIT-2 GATE. A drill that ran every phase, was measured and
    // was scored `fail-objective` or `fail-integrity` is a drill RESULT: exit
    // 2, with the signed scorecard already in the bucket. Deleting this
    // comparison makes Logweir report SUCCESS for a restore that missed its
    // RTO — the review's mutant R3, which survived the whole suite because
    // the two exit-2 paths that were covered both return early from phases 5
    // and 6 and never reach here. Pinned by
    // `a_scored_drill_that_does_not_pass_exits_2_after_running_every_phase`.
    if sc.outcome != Outcome::Pass {
        return Err(DrillError::NotPass(Box::new(sc)));
    }
    Ok(RestoreOutcome {
        scorecard: sc,
        topic_preflight,
        evidence,
    })
}

/// Everything ONE restore run produced. The scorecard is the signed evidence;
/// `topic_preflight` is guard **G-TS**'s observation, which is deliberately not
/// in the scorecard (Global Constraint 12 as amended: nested optional fields
/// only, no new top-level property, and the document stays at 21/17) and is
/// written to `Restore.status.topicPreflight` by the operator instead.
#[derive(Debug, Clone)]
pub struct RestoreOutcome {
    pub scorecard: Scorecard,
    pub topic_preflight: phase0_admit::TopicPreflight,
    /// **Interface I8's three stdout lines**, as the keys the run actually
    /// put at — never reconstructed from `run_id` by whoever prints them.
    pub evidence: EvidenceKeys,
}

/// The three evidence keys a successful restore names on stdout, in the order
/// interface **I8** fixes: `scorecard-key=`, `sidecar-key=`,
/// `offset-report-key=`.
///
/// They come out of `phase8_score::Signed`, which built each string once and
/// put at it, so a controller that fetches what these lines name gets the
/// object this run wrote. `offset_report_key` is `None` when the run had no
/// offset-mapping report — see `Signed::offset_report_key`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EvidenceKeys {
    pub scorecard_key: String,
    pub sidecar_key: String,
    pub offset_report_key: Option<String>,
}

/// Phase 9, in one place, because two paths reach it: the normal end of a run
/// and the phase-6 `RestoreNoOp` interception — both of which have executed a
/// restore and may therefore have created topics on the operator's cluster.
///
/// A teardown failure is a WARNING on the phase record, never an outcome: the
/// drill result is already signed and uploaded, and leaving scratch topics
/// behind is an operational annoyance rather than a false claim. What is NOT
/// allowed is silence — `phase9_teardown::run` records every topic the broker
/// refused in `topics_failed` and never lists it as deleted.
///
/// The attestation is BOUND to the signed bytes, not to the run id again:
/// binding to the run id twice carries no independent information and cannot
/// identify WHICH signed document this teardown accompanies.
fn teardown(
    sc: &mut Scorecard,
    run_id: &str,
    c: &Ctx,
    signer: &ValidatedSigner,
    deleter: &dyn TopicDeleter,
    mapping: &BTreeMap<String, String>,
    scorecard_sha256: &str,
) {
    let attested = record(sc, 9, "teardown", || {
        let a = phase9_teardown::run(
            deleter,
            mapping,
            &c.spec.target.teardown,
            // Global Constraint 19, passed rather than re-derived: phase 9
            // deletes only in `scratch` mode, and the mode travels onto the
            // teardown attestation so a reader can tell "nothing, and nothing
            // was owed" from "nothing, and the policy said keep".
            c.spec.target.mode,
            run_id,
            scorecard_sha256,
        );
        // T0-11, channel 2 of 3. `topics_failed` was correct from the day it
        // was written and reached no `tracing::` call, no `metrics.rs` and no
        // `summary_line()` — so a drill that left five scratch topics on a
        // production broker printed the same line and exited 0 as one that
        // cleaned up.
        //
        // The names travel in the `topics` FIELD and in the message both: a
        // JSON-line consumer reads `fields.topics`, a plain-text consumer reads
        // the message, and a mutant that emits a count without the names must
        // fail on both. `run_id` is on the EVENT and not only on the entered
        // `drill` span, for Task 12's reason — a single-line consumer reads the
        // event object, never its span.
        if let Some(msg) = phase9_teardown::teardown_warning(&a) {
            tracing::warn!(
                run_id = %run_id,
                topics = %phase9_teardown::failed_topic_names(&a).join(","),
                count = a.topics_failed.len(),
                "{msg}"
            );
        }
        if let Err(e) = phase9_teardown::persist_with_signer(&a, signer, &c.store) {
            tracing::warn!(error = %e, "teardown attestation not persisted");
        }
        Ok(a)
    });
    // The notes go on AFTER `record` returns, because `record` pushes the
    // `PhaseRecord` only once the closure has finished — the same shape phase
    // 6's `RestoreNoOp` interception uses to annotate its own record.
    if let Ok(att) = attested {
        let notes = phase9_teardown::failure_notes(&att);
        if !notes.is_empty() {
            if let Some(p) = sc.phases.iter_mut().find(|p| p.phase == 9) {
                p.notes = notes;
            }
        }
    }
}

/// Phase 8, and everything that must happen with the signed bytes still in
/// hand, in ONE place used by all three paths that reach it (phase 5's
/// `Verdict::Block`, phase 6's `RestoreNoOp`, and the normal run). Three
/// copies of this sequence is three chances for one of them to drop a step:
/// the artifact write, the post-put receipt and the notification are all
/// obligations of every signed result, not of the happy path only.
///
/// `record` borrows `sc` mutably, so the document phase 8 signs is frozen
/// here rather than borrowed out from under it.
fn sign_and_publish(
    sc: &mut Scorecard,
    args: &RunArgs,
    run_id: &str,
    c: &Ctx,
    signer: &ValidatedSigner,
    offset_report: Option<&std::path::Path>,
) -> Result<phase8_score::Signed, DrillError> {
    let to_sign = sc.clone();
    let signed = record(sc, 8, "score-and-sign", || {
        phase8_score::run_with_signer(&to_sign, signer, &c.store, offset_report)
    })?;
    write_scorecard_artifact(args, run_id, &signed);
    // Carried obligation from Task 20. Phase 8 signs BEFORE it puts — bytes
    // cannot be signed before they are serialised — so the four storage facts
    // (`create_only_enforced`, `immutable`, `retain_until`, `version_id`) are
    // unknowable at signing time and the signed scorecard neutralises all
    // four. Until this receipt existed, Logweir published NO verifiable
    // evidence that its own upload was create-only, and `docs/stability.md`
    // said so. The receipt is a second, separately signed document carrying
    // the real post-put readback — the same pattern phase 9 uses for the
    // teardown attestation, and for the same reason.
    let receipt = phase8_score::put_receipt(&signed);
    if let Err(e) = phase8_score::persist_put_receipt_with_signer(&receipt, signer, &c.store) {
        // A warning, never an outcome: the drill result is already signed and
        // uploaded, and a receipt that could not be written must not retract
        // a measurement. It also must not be silent.
        //
        // It goes to the LOG and nowhere else, deliberately. The obvious
        // alternative — a note on the phase-8 record — cannot work and would
        // only look like it did: the document was frozen and signed before
        // this line runs, and `execute_with` then adopts that signed document,
        // so any note written here is discarded a few lines later. A field
        // that appears to carry evidence and provably cannot is worse than no
        // field at all. The receipt's ABSENCE from the bucket is the durable
        // signal (`docs/stability.md` says so in those words).
        tracing::warn!(error = %e, "post-put evidence receipt not persisted");
    }
    phase7_verify::notify(
        &c.spec.notifications,
        c.spec.name.as_deref(),
        &signed.scorecard,
    );
    Ok(signed)
}

/// `spec.source.backup` is either `latestCompleted` or a pinned backup id.
/// A pinned id that the archive does not hold is refused by name rather than
/// silently falling back to the newest set — a drill that quietly restored a
/// different backup than the approved plan named would make the whole
/// approval chain meaningless.
fn pick_backup_set(engine: &dyn DataEngine, spec: &DrillSpec) -> Result<BackupSetRef, DrillError> {
    let sets = engine.list_backup_sets(&spec.source.storage)?;
    if sets.is_empty() {
        return Err(DrillError::Operational(
            "the archive holds no backup set at the configured source storage location".into(),
        ));
    }
    if spec.source.backup == "latestCompleted" {
        // `list_manifests` returns them sorted by key, and manifest keys are
        // timestamp-ordered, so the last is the newest.
        return Ok(sets.last().cloned().expect("non-empty"));
    }
    sets.iter()
        .find(|s| s.backup_id == spec.source.backup)
        .cloned()
        .ok_or_else(|| {
            DrillError::Operational(format!(
                "the archive holds no backup set with id `{}`; refusing to fall back to \
                 another set, which would restore something other than the approved plan",
                spec.source.backup
            ))
        })
}

/// The one plan both phase 5 and phase 6 run against, unchanged — that
/// identity is what binds the approved plan to the executed one (see
/// `logweir_engine_oso::render_restore`'s module doc).
///
/// **`pub` for guard G-ID**, and for nothing else. It is the seam where the
/// approved spec BYTES become the plan `plan_hash` covers, so it is the seam
/// where "the rendered `sasl_username` comes from the plan and never from a
/// cluster object" is a testable claim rather than a comment:
/// `crates/logweir/tests/auth_binding.rs::
/// rendered_sasl_username_comes_from_plan_bytes_not_from_the_cluster_object`
/// calls it with a live cluster view in scope and asserts the view cannot
/// reach the document. A test that constructed a `RestorePlan` literal
/// instead would be asserting a property of the test.
pub fn build_plan(
    spec: &DrillSpec,
    set: &BackupSetRef,
    mapping: &BTreeMap<String, String>,
    facts: &BackupSetFacts,
    run_id: &str,
    offset_report_out: Option<&std::path::Path>,
) -> Result<RestorePlan, DrillError> {
    // **GUARD G-WIN, THE BINDING HALF.** `RestorePlan.time_window.0` is
    // computed from the archive set's earliest covered timestamp as recorded
    // in the manifest — never from the spec and never from an inherited
    // tuple. `time_window.1` is `spec.restore.point_in_time` when present,
    // else `spec.sample.window_end`, which preserves every existing drill's
    // behaviour for the END of the window; the START widens to the archive
    // floor for every mode, which cannot lose records and is the direction
    // the guard requires.
    //
    // And the binding lives HERE, where the plan is built, not where it is
    // printed: `logweir_engine_oso::render_restore::render` is a printer, so a
    // mutant applied there is byte-identical to correct output whenever the
    // plan handed to it is already right — which is exactly why spec §10's
    // earlier G-WIN row had two mutants that both passed.
    // The floor is the minimum over the topics THIS RESTORE NAMES — the keys
    // of the admitted topic mapping — so it reports the same instant the
    // backup receipt's `covered.from_ms` reports for the same archive and the
    // same topics (plan erratum E7(b)). Phase 5 re-derives it from the same
    // field of the plan built here.
    let named_topics: BTreeSet<&str> = mapping.keys().map(String::as_str).collect();
    let manifest_floor_ms = facts
        .earliest_covered_timestamp_ms(&named_topics)
        .ok_or_else(|| {
            DrillError::Guard(GuardRefusal(format!(
                "the archive set `{}` records no segment in its manifest for any of the topics \
                 this restore names ({}), so it has no earliest covered timestamp; a Restore's \
                 window start is the archive set's earliest covered timestamp, never the \
                 spec's, so this plan has no floor to bind and is refused before anything runs",
                set.backup_id,
                named_topics
                    .iter()
                    .copied()
                    .collect::<Vec<&str>>()
                    .join(", ")
            )))
        })?;
    let start = chrono::DateTime::from_timestamp_millis(manifest_floor_ms).ok_or_else(|| {
        DrillError::Guard(GuardRefusal(format!(
            "the archive set `{}` records an earliest covered timestamp of epoch-ms \
             {manifest_floor_ms}, which is outside the representable date range",
            set.backup_id
        )))
    })?;
    build_plan_with_floor(
        spec,
        set,
        mapping,
        run_id,
        WindowFloor {
            start,
            source: WindowFloorSource::ArchiveManifest,
            manifest_floor_ms,
        },
        offset_report_out,
    )
}

/// The window's floor, and the CLAIM the plan is about to make about where it
/// came from — the seam `build_plan` exposes so that claim can be checked
/// against the value it describes, by a test as well as at runtime.
///
/// `window_floor_source == ArchiveManifest` while `time_window.0` was read
/// from the spec is a lie the enum cannot catch on its own (critique A F22).
/// Passing the three values TOGETHER is what makes the lie constructible, and
/// therefore refusable: `build_plan_with_floor` ends by checking that
/// `ArchiveManifest` implies `start.timestamp_millis() == manifest_floor_ms`.
#[derive(Debug, Clone, Copy)]
pub struct WindowFloor {
    /// What becomes `RestorePlan.time_window.0`.
    pub start: chrono::DateTime<chrono::Utc>,
    /// What becomes `RestorePlan.window_floor_source`.
    pub source: WindowFloorSource,
    /// The manifest floor `start` is CHECKED against whenever `source` is
    /// `WindowFloorSource::ArchiveManifest`.
    pub manifest_floor_ms: i64,
}

/// The per-run workdir both pod-local engine files live in:
/// `<temp>/logweir-<run_id>`.
///
/// A named function rather than the same `temp_dir().join(...)` written twice,
/// because the two paths have to be siblings — `execute_with_outcome` creates
/// this directory once, from `plan.checkpoint_state.parent()`, and the engine
/// creates neither file's parent
/// [U:crates/kafka-backup-core/src/restore/engine.rs:1382-1388 uses a bare
/// `tokio::fs::write`, whose failure is only a `warn!`]. Two independently
/// spelled paths could drift into two directories, one of which would not
/// exist, and the offset report would then silently never be written.
pub fn run_workdir(run_id: &str) -> std::path::PathBuf {
    std::env::temp_dir().join(format!("logweir-{run_id}"))
}

/// `build_plan` with the floor supplied rather than derived — see `WindowFloor`.
pub fn build_plan_with_floor(
    spec: &DrillSpec,
    set: &BackupSetRef,
    mapping: &BTreeMap<String, String>,
    run_id: &str,
    floor: WindowFloor,
    offset_report_out: Option<&std::path::Path>,
) -> Result<RestorePlan, DrillError> {
    // The window's END: the requested recovery point when the spec states one,
    // else the sample window's end. The FIELD's name travels with the value,
    // because it is the half of the refusal below an operator can act on —
    // the same pairing `phase0_admit::target_topic_preflight` makes for the
    // broker's timestamp bound.
    let (window_end_field, window_end) = match spec.restore.point_in_time {
        Some(t) => ("restore.point_in_time", t),
        None => ("sample.window_end", spec.sample.window_end),
    };
    let plan = RestorePlan {
        set: set.clone(),
        storage: spec.source.storage.clone(),
        target_bootstrap: spec.target.bootstrap_servers.clone(),
        // **G-ID.** The principal is bound into the plan — and therefore into
        // `plan_hash` — from the SPEC BYTES the approver signed off, and is
        // read from nothing else. See `AuthSpec::username`.
        target_auth: spec.target.auth.to_render(),
        topic_mapping: mapping.clone(),
        time_window: (floor.start, window_end),
        window_floor_source: floor.source,
        default_replication_factor: spec.target.default_replication_factor,
        // Pod-local and never uploaded: a crashed restore is NOT resumable in
        // v0.1 (spec §11).
        checkpoint_state: run_workdir(run_id).join("checkpoint.json"),
        checkpoint_interval_secs: 30,
        // Pod-local and UPLOADED, which is the whole difference from the
        // checkpoint above: the offset report is evidence about what the
        // restore mapped, and the pod that holds it is deleted. Default:
        // `offsets.json` beside `checkpoint.json` in the same per-run workdir,
        // which is the one directory `execute_with_outcome` creates.
        offset_report: offset_report_out
            .map(std::path::Path::to_path_buf)
            .unwrap_or_else(|| run_workdir(run_id).join("offsets.json")),
    };
    // **THE ENUM IS CHECKED AGAINST THE VALUE IT CLAIMS** (critique A F22).
    // An EXPLICIT check, deliberately NOT a `debug_assert!`: that macro is
    // compiled out in release, which is the profile every shipped binary is
    // built in, so the one assertion standing between a lying plan and a
    // signed `pass` would exist only in the test profile.
    //
    // Exit 3, before anything runs (Global Constraint 11, `crate::exit`).
    if plan.window_floor_source == WindowFloorSource::ArchiveManifest
        && plan.time_window.0.timestamp_millis() != floor.manifest_floor_ms
    {
        return Err(DrillError::Guard(GuardRefusal(format!(
            "the plan claims its window floor came from the archive manifest, and its \
             time_window start is epoch-ms {} while the manifest floor is epoch-ms {}; a \
             Restore's window start is the archive set's earliest covered timestamp, never \
             the spec's",
            plan.time_window.0.timestamp_millis(),
            floor.manifest_floor_ms
        ))));
    }
    // **AND THE WINDOW IS NEVER INVERTED OR EMPTY** (plan erratum E7(a)).
    // `time_window.0` is the archive's floor and `time_window.1` is the
    // requested recovery point, so a recovery point at or before the floor
    // describes an interval holding no instant at all. The engine validates
    // only `start <= end` [U:crates/kafka-backup-core/src/config.rs:1184-1187],
    // so before this check an inverted window reached `engine.preflight` and
    // came back as the engine's own config-validator text — ruling R-E's
    // `EngineError`, exit **1, operational**, after phases 0-4 had run. That
    // misfiled the one failure an operator can actually fix: asking for a
    // recovery point from before the archive begins is not an operational
    // fault, it is a plan the archive cannot satisfy.
    //
    // `>=`, not `>`: at equality the window is `[t, t]` for a filter the
    // engine reads as `>= start && <= end`, which is one instant wide and
    // restores whatever shares that exact millisecond — almost always nothing,
    // and never the recovery the operator asked for. An empty restore that
    // reports `pass` is precisely G-WIN's silent loss.
    //
    // Exit 3, refused before anything runs (Global Constraint 11), and it
    // names BOTH integers plus the spec field the end came from, so the
    // refusal is actionable without reading the manifest by hand.
    //
    // It lives HERE, at the one place a `RestorePlan` is constructed, so no
    // caller — `build_plan`, a restore mode of Task 9b, or a test — can
    // produce an inverted window at all. Not a `debug_assert!`, for the same
    // reason as the check above: that macro is compiled out in release.
    if plan.time_window.0 >= plan.time_window.1 {
        return Err(DrillError::Guard(GuardRefusal(format!(
            "this plan's {window_end_field} is epoch-ms {end}, at or before the archive set \
             `{set_id}`'s earliest covered timestamp of epoch-ms {start}, so the restore \
             window [{start}, {end}] holds no instant and would restore nothing; a Restore's \
             window start is the archive set's earliest covered timestamp, never the spec's, \
             so a recovery point must be LATER than epoch-ms {start}",
            end = plan.time_window.1.timestamp_millis(),
            start = plan.time_window.0.timestamp_millis(),
            set_id = plan.set.backup_id,
        ))));
    }
    Ok(plan)
}

fn new_scorecard(run_id: &str, args: &RunArgs, c: &Ctx) -> Scorecard {
    let id = c.engine.id();
    Scorecard {
        format_version: logweir_core::FORMAT_VERSION.to_string(),
        run_id: run_id.to_string(),
        // Global Constraint 1: the clock is read HERE, never in logweir-core.
        requested_at: chrono::Utc::now(),
        triggered_by: args.triggered_by.clone(),
        outcome: Outcome::FailIntegrity,
        last_phase_completed: -1,
        approval_validated_at: None,
        engine: EngineInfo {
            id: id.id,
            version: id.version,
            digest: id.digest,
            execution: "subprocess".into(),
            levers: Levers {
                // Claims-nothing starting values, raised only by phase 5's own
                // per-run readback. A scorecard that never reached phase 5
                // must not assert the engine honoured a lever nobody checked.
                header_preflight: LeverState::Ignored,
                // Not observable from DryRunReport at all — see `LeverState`.
                dry_run_check_segments: LeverState::UnknownNotObservable,
                unknown_key_warnings: Vec::new(),
            },
            // The claims-nothing STARTING value, raised only by phase 5's own
            // per-run lever readback and then resolved, at signing time, by
            // `phase8_score::matrix_verdict_for`.
            //
            // It reads "not yet established" here while
            // `docs/support-matrix.md` defines the variant as "the engine
            // accepted a lever and did not act on it", and the gap between
            // those two readings never reaches an artifact: NO scorecard is
            // signed before phase 5 has run. Phases 0-4 fail with exit 1 or 3
            // and write nothing, and all three signing paths (the phase-5
            // `Verdict::Block` jump, the phase-6 `RestoreNoOp` interception
            // and the normal run) are downstream of the readback. So by the
            // time this value can be signed it means exactly what the
            // support matrix says: phase 5 looked, and the engine did not
            // honour the lever.
            matrix_verdict: MatrixVerdict::FailLeverNotHonoured,
            // `matrix_verdict_reason` is REQUIRED only for `fail` and must be
            // null otherwise (`Scorecard::validate_invariants`).
            matrix_verdict_reason: None,
        },
        // `captured_by_logweir` is true iff phase −1 ran (Global Constraint 18,
        // reversed 2026-09-03; global ruling GR4 Part A). `validate_invariants`
        // does NOT refuse `true`: it requires that `true` implies
        // `last_phase_completed >= -1` and a non-null
        // `measured.rpo_source_relative_seconds` with a null
        // `rpo_source_relative_unmeasured_reason`, and that `false` implies the
        // reverse. This orchestrator never runs phase −1 — the `--from-cluster`
        // path is Task 24 (GR4 Part B) — so `false` stands and the invariant
        // holds dormant but correct.
        source: SourceInfo {
            backup_id: String::new(),
            manifest_sha256: String::new(),
            manifest_version_id: None,
            captured_by_logweir: false,
        },
        target: TargetInfo {
            cluster_id: String::new(),
            // The mode is a property of the SPEC, so it is known before
            // anything is measured and the draft can carry it truthfully.
            // `marker_topic` is NOT: it means "phase 0 verified this topic
            // exists on an allowlisted cluster", so it is written only for the
            // mode whose phase 0 checks that — see `target_info` below, which
            // makes the same choice for the document that gets signed.
            mode: c.spec.target.mode,
            marker_topic: marker_topic_for(&c.spec),
            topic_mapping_prefix: c.spec.target.topic_mapping_prefix.clone(),
            topic_mapping_sha256: String::new(),
            topic_mapping_entries: 0,
            // Task 5b declares the SHAPE of `target.auth`; Task 6 fills it.
            // `None` is the honest value for a draft nothing has measured
            // yet — and absent means plaintext, which is what every
            // scorecard this tree has written says by omission.
            auth: None,
        },
        approval: ApprovalInfo {
            approver: String::new(),
            ticket: String::new(),
            plan_hash: String::new(),
            approved_at: chrono::DateTime::UNIX_EPOCH,
            key_id: String::new(),
            self_attested: false,
        },
        phases: Vec::new(),
        measured: Measured {
            rto_seconds: None,
            rto_requested_to_verified_seconds: None,
            rto_restore_only_seconds: None,
            rto_excluding_preflight_seconds: None,
            rpo_seconds: None,
            // v0.1 never contacts the source. This pair is exactly the
            // `captured_by_logweir == false` branch `validate_invariants`
            // requires, and it must hold from the first byte: a scorecard
            // signed on the phase-5 block path never reaches
            // `compute_measured`.
            rpo_source_relative_seconds: None,
            rpo_source_relative_unmeasured_reason: Some("source cluster never contacted".into()),
        },
        // The REQUEST, known from the spec before any phase runs; `met` stays
        // null until `phase8_score::decide` answers it.
        objectives: Objectives {
            rto_seconds: c.spec.objectives.rto_seconds,
            rpo_seconds: c.spec.objectives.rpo_seconds,
            pass_rate: c.spec.objectives.pass_rate,
            met: None,
        },
        sample: SampleInfo {
            window_start: c.spec.sample.window_start,
            window_end: c.spec.sample.window_end,
            topics: 0,
            partitions: 0,
            records_expected: 0,
            records_restored: 0,
            anchor: c.spec.sample.anchor.as_str().to_string(),
            coverage_note: "phase 4 has not run".into(),
        },
        target_diff: TargetDiffSummary::default(),
        integrity: Integrity {
            level: IntegrityLevel::NotAttempted,
            result: IntegrityResult::Fail,
            partial_reason: None,
            records_sampled: 0,
            records_sampled_matching: 0,
            mismatches: 0,
            pass_rate_measured: None,
            restored_principal_could_consume: None,
        },
        topic_parity: TopicParity {
            intentionally_deviated: Vec::new(),
            unexpected_divergence: Vec::new(),
        },
        engine_subreport: None,
        // All four zeroed; `phase8_score::run` zeroes them again immediately
        // before serialising, whatever any caller supplied.
        evidence: EvidenceInfo {
            version_id: None,
            retain_until: None,
            immutable: false,
            create_only_enforced: false,
            // Both None here and BOTH SET BY PHASE 8, from the one file the
            // engine wrote — `new_scorecard` has no offset report to describe
            // because no restore has run yet.
            offset_report_key: None,
            offset_report_sha256: None,
        },
        redactions: Vec::new(),
    }
}

fn source_info(facts: &logweir_core::engine::BackupSetFacts) -> SourceInfo {
    SourceInfo {
        backup_id: facts.backup_id.clone(),
        manifest_sha256: facts.manifest_sha256.clone(),
        manifest_version_id: facts.manifest_version_id.clone(),
        // See `new_scorecard`: phase −1 is Task 24's (GR4 Part B).
        captured_by_logweir: false,
    }
}

/// Fallible only because of **G-EXP**: `render_topic_mapping_block` now
/// escapes through `yaml_scalar_checked`, which REFUSES a `${`. The refusal is
/// unreachable from here — `phase0_admit::run` has already refused a `${` in
/// any selected topic and in `target.topic_mapping_prefix`, and
/// `admitted.topic_mapping` is built from exactly those two — so this is a
/// fail-closed propagation rather than a live path. It is a `?` and not an
/// `expect`: an unreachable refusal that becomes reachable through somebody
/// else's edit must exit 3 with its `refusal-reason=` line, not abort the
/// process.
/// `TargetInfo::marker_topic` for a spec: the spec's value in `Scratch` mode
/// and `None` in `NewTopic` mode.
///
/// ONE owner, called from both `new_scorecard`'s draft and `target_info`'s
/// signed document, because a draft that carried the field and a signed
/// document that did not would be two answers to the same question.
fn marker_topic_for(spec: &DrillSpec) -> Option<String> {
    match spec.target.mode {
        logweir_core::spec::TargetMode::Scratch => Some(spec.target.marker_topic.clone()),
        logweir_core::spec::TargetMode::NewTopic => None,
    }
}

fn target_info(
    spec: &DrillSpec,
    admitted: &phase0_admit::Admitted,
) -> Result<TargetInfo, DrillError> {
    let block =
        logweir_engine_oso::render_restore::render_topic_mapping_block(&admitted.topic_mapping)
            .map_err(|e| logweir_core::guard::GuardRefusal(e.to_string()))?;
    Ok(TargetInfo {
        cluster_id: admitted.target_cluster_id.clone(),
        // The DISCRIMINATOR the signed document was missing (review F1). It
        // is `skip_serializing_if`-absent for `Scratch`, so no byte of any
        // scratch document moves, and present for `NewTopic` — the one run
        // whose phase 0 skipped the marker and allowlist checks.
        mode: spec.target.mode,
        // ABSENT in `newTopic` mode. The field's own doc comment defines it as
        // the phase-0 segregation proof — `cluster_id ∈ allowedClusterIds` AND
        // this topic exists — and in `newTopic` mode neither is checked, so
        // writing the spec's value here would put a verified-sounding claim
        // about an unrun check into a DSSE-signed document. The reviewer
        // measured exactly that: a real run with `logweir.scratch` DELETED and
        // an EMPTY allowlist still reported `target.marker_topic:
        // "logweir.scratch"`.
        marker_topic: marker_topic_for(spec),
        // The prefix PHASE 0 ACTUALLY MAPPED THROUGH, taken off `Admitted`
        // rather than re-read from the spec. In `newTopic` mode the name comes
        // from `target.topic_naming.prefix` or from
        // `spec::default_topic_prefix`, so `spec.target.topic_mapping_prefix`
        // — which stays in the document because an empty scratch prefix maps
        // every topic onto itself — would put the wrong string in the one
        // signed field an auditor uses to re-derive the mapping.
        topic_mapping_prefix: admitted.topic_mapping_prefix.clone(),
        // sha256 over the topic_mapping block AS RENDERED into restore.yaml —
        // the same function that renders it, so the hash and the bytes the
        // engine was handed cannot drift.
        topic_mapping_sha256: logweir_core::ids::sha256_prefixed(block.as_bytes()),
        topic_mapping_entries: admitted.topic_mapping.len() as u32,
        // **Interface I1's scorecard end, now closed on both sides.** Task 5b
        // declares `AuthSummary`/`TargetInfo.auth` and pays Global Constraint
        // 12's price for it (both readers, the corpus, the schema); TASK 6
        // owns the VALUE, and this is the assignment its own `target_info`
        // comment spelled out verbatim while the field did not yet exist on
        // its branch. Applied at the rebase of 5b onto Task 6, which is the
        // first tree where both halves are present.
        //
        // `mode_str()` returns `"plaintext"` or `"scramSha512"` and nothing
        // else — a closed set of two `&'static str` (`spec.rs::mode_str`) that
        // is the `KafkaCluster` CRD's `auth.mode` enum byte for byte, the
        // receipt's `source.auth.mode`, and exactly what BOTH readers accept
        // for this field. `crates/logweir/tests/auth_binding.rs::
        // the_scorecard_auth_block_and_auth_spec_agree` asserts the two
        // strings and the `{mode, username}` round trip; the two readers'
        // `target.auth` arms refuse any third spelling.
        //
        // Always `Some` on a measured drill: a scorecard whose target block
        // says nothing about auth is read as plaintext by omission, and
        // omission is the one thing a SCRAM run must not be recorded as.
        auth: Some(AuthSummary {
            mode: spec.target.auth.mode_str().into(),
            username: spec.target.auth.username().map(str::to_string),
        }),
    })
}

fn sample_info(
    spec: &logweir_core::spec::SampleSpec,
    sel: &phase4_sample::Selection,
) -> SampleInfo {
    SampleInfo {
        window_start: sel.window.0,
        window_end: sel.window.1,
        topics: sel.topics,
        partitions: sel.partitions,
        // THE CANARY SIZE — the number of records this drill set out to
        // reconcile — and deliberately NOT `Selection::records_expected`,
        // which answers a different question (how many records the manifest
        // says the whole window holds). The two share a name and differ by
        // orders of magnitude on any real archive; substituting one for the
        // other would make the signed document overstate what was verified.
        // Both definition sites carry this note (Task 16's parked item).
        // `integrity.records_sampled` is measured against THIS figure.
        records_expected: sel.per_partition.iter().map(|s| s.count as u64).sum(),
        // Phase 7 measures this; phase 4 cannot know it.
        records_restored: 0,
        anchor: spec.anchor.as_str().to_string(),
        coverage_note: if sel.notes.is_empty() {
            "no capture gap or retention-pruned range overlaps the sampled window".into()
        } else {
            sel.notes.join("; ")
        },
    }
}

#[cfg(test)]
mod tests {
    //! `finish` and `summary_line` are private and are reached only from
    //! `run`, which needs a live broker and the engine binary. Their call
    //! sites therefore have no integration-test coverage available to them,
    //! and "the metrics file is never written" is exactly the kind of silent
    //! omission this build keeps shipping — so they are pinned here, inside
    //! the crate, where the private items are nameable.
    use super::*;

    fn a_scorecard() -> Scorecard {
        serde_json::from_str(include_str!("../../../../e2e/fixtures/scorecard-pass.json"))
            .expect("the checked-in fixture parses")
    }

    /// The fixture scorecard as `report` now takes it — a whole
    /// `RestoreOutcome`, because interface I8's three stdout lines are keys the
    /// RUN put at and `report` is what prints them (Task 9b). The keys here are
    /// the ones `phase8_score::run` builds for this fixture's run id.
    fn an_outcome() -> RestoreOutcome {
        let sc = a_scorecard();
        let run_id = sc.run_id.clone();
        RestoreOutcome {
            scorecard: sc,
            topic_preflight: phase0_admit::TopicPreflight {
                timestamp_type: "CreateTime".into(),
                retention_ms: "-1".into(),
                timestamp_bound_ms: None,
                configs_set: Vec::new(),
                topics_created: Vec::new(),
            },
            evidence: EvidenceKeys {
                scorecard_key: format!("logweir/drills/{run_id}.json"),
                sidecar_key: format!("logweir/drills/{run_id}.sig"),
                offset_report_key: Some(format!("logweir/drills/{run_id}.offsets.json")),
            },
        }
    }

    fn args_with(metrics_file: Option<PathBuf>) -> RunArgs {
        RunArgs {
            execution_contract_version: None,
            spec: PathBuf::from("drill.yaml"),
            approval: PathBuf::from("approval.json"),
            approver_key: PathBuf::from("approver.pem"),
            allowed_clusters: PathBuf::from("allowed.json"),
            signing_key: PathBuf::from("signer.pem"),
            triggered_by: None,
            out: None,
            metrics_file,
            offset_report_out: None,
            // NOT PINNED, which is the default every existing caller gets.
            approver_key_ids: Vec::new(),
        }
    }

    #[test]
    fn finish_writes_the_textfile_metrics_when_the_flag_is_set() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("logweir.prom");
        finish(&args_with(Some(p.clone())), &a_scorecard());
        let t = std::fs::read_to_string(&p).expect("--metrics-file was written");
        assert!(t.contains("logweir_drill_runs_total"), "{t}");
        assert!(t.contains("logweir_drill_exit_code"), "{t}");
    }

    #[test]
    fn finish_writes_no_metrics_file_when_the_flag_is_absent() {
        let dir = tempfile::tempdir().unwrap();
        finish(&args_with(None), &a_scorecard());
        assert!(
            std::fs::read_dir(dir.path()).unwrap().next().is_none(),
            "no --metrics-file means no file, not a default path"
        );
    }

    /// A `tracing` sink for one test, scoped to the calling thread.
    ///
    /// `tracing::subscriber::with_default` sets a THREAD-LOCAL dispatcher, so
    /// this captures the warning without touching the global subscriber other
    /// tests (and `run()`) install, and without serialising the suite.
    #[derive(Clone, Default)]
    struct CapturedLogs(std::sync::Arc<std::sync::Mutex<Vec<u8>>>);

    impl CapturedLogs {
        fn text(&self) -> String {
            String::from_utf8_lossy(&self.0.lock().expect("the log buffer is not poisoned"))
                .into_owned()
        }
    }

    impl std::io::Write for CapturedLogs {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.0
                .lock()
                .expect("the log buffer is not poisoned")
                .extend_from_slice(buf);
            Ok(buf.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for CapturedLogs {
        type Writer = CapturedLogs;
        fn make_writer(&'a self) -> Self::Writer {
            self.clone()
        }
    }

    // -----------------------------------------------------------------------
    // Task 12 fix round 1. These three need `DEFAULT_LOG_DIRECTIVE` and
    // `phase_observer`, both module-private, so they live here rather than in
    // `crates/logweir/tests/logging.rs` with their eleven siblings.
    // -----------------------------------------------------------------------

    /// Runs `body` under a thread-local JSON subscriber built with the SAME
    /// directive `run()` uses when `RUST_LOG` is unset, and returns the lines
    /// it emitted. Not a copy of the directive — the constant itself, so a
    /// change to it changes what these tests observe.
    fn under_the_default_directive(body: impl FnOnce()) -> Vec<serde_json::Value> {
        let logs = CapturedLogs::default();
        let subscriber = tracing_subscriber::fmt()
            .json()
            .with_writer(logs.clone())
            .with_env_filter(tracing_subscriber::EnvFilter::new(DEFAULT_LOG_DIRECTIVE))
            .finish();
        tracing::subscriber::with_default(subscriber, body);
        logs.text()
            .lines()
            .filter_map(|l| serde_json::from_str(l).ok())
            .collect()
    }

    /// Review finding M1, widened by Task 12's re-review finding N1 to the
    /// `log` bridge. A dependency's INFO event must NOT reach the stream the
    /// documentation makes a universal claim about.
    ///
    /// Those crates emit from tokio worker threads, where the `drill`
    /// `EnteredSpan` is not current, so such a line could carry neither
    /// `fields.run_id` nor `span.run_id`. The fix is to keep it out of the
    /// stream rather than to weaken the claim. A bare `"info"` directive lets
    /// it through and fails here.
    ///
    /// The last three emissions are the `log`-bridge half. `LogTracer`
    /// converts a `log` record into a `tracing` event whose TARGET is the log
    /// record's target — `rustls::client` for a `log::info!` inside `rustls` —
    /// and that event then meets this same `EnvFilter`. Emitting with an
    /// explicit `target:` is therefore the bridged line as the filter sees it,
    /// and it needs no globally-installed `LogTracer` (`log::set_logger` is
    /// once-per-process and a unit test cannot own it).
    #[test]
    fn a_dependency_info_event_is_filtered_at_the_default_directive() {
        let lines = under_the_default_directive(|| {
            tracing::info!(target: "h2::client", "third-party chatter at INFO");
            tracing::info!(target: "object_store::aws", "third-party chatter at INFO");
            tracing::info!(target: "hyper_util::client::pool", "third-party chatter at INFO");
            // The `log`-crate emitters, as `LogTracer` presents them.
            tracing::info!(target: "rustls::client::hs", "third-party chatter at INFO");
            tracing::info!(target: "rdkafka::client", "third-party chatter at INFO");
            tracing::info!(target: "ureq::stream", "third-party chatter at INFO");
            tracing::warn!(target: "h2::client", "third-party WARN still passes");
            tracing::info!(target: "logweir::drill", run_id = "01TEST", "our own INFO line");
        });
        let messages: Vec<&str> = lines
            .iter()
            .filter_map(|v| v["fields"]["message"].as_str())
            .collect();
        assert!(
            !messages.contains(&"third-party chatter at INFO"),
            "a dependency's INFO event reached the default stream, where it would carry no run id \
             on the event and none on the span either (its thread never entered `drill`), while \
             README and docs/kubernetes.md claim every line Logweir emits at the default level \
             carries the id. Directive under test: {DEFAULT_LOG_DIRECTIVE:?}; lines: {messages:?}"
        );
        assert!(
            messages.contains(&"our own INFO line"),
            "narrowing must not silence Logweir's own INFO lines: {messages:?}"
        );
        assert!(
            messages.contains(&"third-party WARN still passes"),
            "a dependency at WARN is quieted, not muted — a real warning from the storage or HTTP \
             layer is still an operator's business: {messages:?}"
        );
    }

    /// Every crate name this build actually COMPILED, read from the artifact
    /// directory the running test binary lives in.
    ///
    /// `Cargo.lock` is not the resolved graph: it records optional
    /// dependencies that no enabled feature activates, which is why `quinn`,
    /// `quinn-proto`, `quinn-udp` (through `reqwest`'s HTTP/3 feature) and
    /// `jni` appear there while `cargo tree -p logweir -i quinn -e normal`
    /// prints "nothing to print". A crate cargo never compiled cannot emit,
    /// and `DEFAULT_LOG_DIRECTIVE`'s own doc comment refuses to name crates
    /// that cannot emit — so the closure below narrows the lockfile set by
    /// this one.
    ///
    /// Reading the artifact directory rather than shelling out to
    /// `cargo tree` is deliberate: `cargo tree` takes the package-cache lock,
    /// and three agents building concurrently would turn a 15 s per-test
    /// budget into a lock wait. This is a filesystem listing and is instant.
    /// Artifact names are `lib<crate_name>-<16 hex>.<ext>` with the crate name
    /// already in module spelling, so no hyphen mapping is needed here.
    fn crates_compiled_into_this_build() -> std::collections::BTreeSet<String> {
        let exe = std::env::current_exe().expect("the running test binary has a path");
        let deps = exe
            .parent()
            .expect("a test binary lives in <target>/<profile>/deps");
        let mut out = std::collections::BTreeSet::new();
        for entry in std::fs::read_dir(deps).expect("the deps directory is readable") {
            let Ok(entry) = entry else { continue };
            let file = entry.file_name();
            let file = file.to_string_lossy();
            let stem = file.split('.').next().unwrap_or("");
            let Some((name, hash)) = stem.rsplit_once('-') else {
                continue;
            };
            if hash.len() != 16 || !hash.chars().all(|c| c.is_ascii_hexdigit()) {
                continue;
            }
            // Both spellings: a crate literally named `libc` produces
            // `liblibc-<hash>.rlib`, and blindly stripping `lib` would hide it.
            out.insert(name.to_string());
            if let Some(rest) = name.strip_prefix("lib") {
                out.insert(rest.to_string());
            }
        }
        // Anti-vacuity, and it must PANIC rather than silently narrow to
        // nothing: an oracle that returns an empty set would let every
        // assertion below pass without checking anything.
        for certain in ["tracing", "serde_json"] {
            assert!(
                out.contains(certain),
                "the compiled-crate scan of {} did not find `{certain}`, which this very binary \
                 links — the scan is broken, and every narrowing below would be vacuous. \
                 Found {} names.",
                deps.display(),
                out.len()
            );
        }
        out
    }

    /// Closed arithmetic over the WARN list, in this repository's established
    /// idiom: the expected set is DERIVED, never hand-listed twice. A
    /// dependency bump that introduces a new emitter fails here instead of
    /// silently widening the default stream a year from now.
    ///
    /// The name says `tracing_emitting` for the history; the set is wider than
    /// that, because there are TWO routes into this stream. Task 12 closed over
    /// direct `tracing` dependants only, and its own re-review (N1) showed with
    /// a live subscriber that `rustls` and `rdkafka` INFO records arrive
    /// anyway, through the `tracing_log::LogTracer` that `try_init()` installs.
    /// A `log` dependency is therefore counted exactly as a `tracing` one.
    #[test]
    fn every_tracing_emitting_dependency_in_the_lockfile_is_pinned_to_warn() {
        let lock = std::fs::read_to_string(
            std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../Cargo.lock"),
        )
        .expect("Cargo.lock");
        // Our own crates are the ones that MUST stay at info. `tracing-subscriber`
        // is the subscriber and `tracing-log` is the bridge; neither emits an
        // event of its own, and `tracing-log` would otherwise be counted purely
        // for depending on `log`.
        // `weirkeeper` joins this predicate with Task 15 rather than joining
        // the WARN list: it is one of ours, it depends on `tracing` directly,
        // and it installs its OWN subscriber in `crates/weirkeeper/src/main.rs`
        // — the `logweir` binary never links it, so pinning it here would be
        // decoration in a constant whose doc comment refuses decoration. The
        // crate is not named `logweir-*`, which is the only reason the prefix
        // test does not already cover it.
        let ours = |n: &str| n == "logweir" || n.starts_with("logweir-") || n == "weirkeeper";
        // `tracing` — the FACADE — joins this predicate with Task 15, for the
        // same reason the other two are already here: it emits no event of its
        // own. Every `tracing` event carries the EMITTING module's target
        // (`logweir::drill`, `kube_client::client`), never `tracing`, so a
        // `tracing=warn` directive would silence nothing and would be exactly
        // the decoration `DEFAULT_LOG_DIRECTIVE`'s doc comment refuses. It
        // arrived in the oracle because `kube-client` enables `tracing`'s
        // optional `log` feature (`cargo tree -e features -i tracing`:
        // `tracing feature "log" └── kube-client v0.99.0`), which put a direct
        // `"log"` line into `tracing`'s own `Cargo.lock` entry where there was
        // none before. That feature makes tracing forward to the `log` facade
        // only when no subscriber is interested in the event, so it does not
        // widen this stream — the run-id assertions in this module are the
        // check on that, and they stay green.
        let plumbing = |n: &str| n == "tracing" || n == "tracing-subscriber" || n == "tracing-log";
        let compiled = crates_compiled_into_this_build();
        let mut emitters: Vec<String> = vec![];
        let mut skipped_unresolved: Vec<String> = vec![];
        for pkg in lock.split("[[package]]") {
            let Some(name) = pkg.lines().find_map(|l| {
                l.strip_prefix("name = \"")
                    .and_then(|r| r.strip_suffix('"'))
            }) else {
                continue;
            };
            if ours(name) || plumbing(name) {
                continue;
            }
            // The `dependencies = [ … ]` block of THIS package only.
            let deps = pkg
                .split_once("\ndependencies = [")
                .and_then(|(_, rest)| rest.split_once("\n]"))
                .map(|(d, _)| d)
                .unwrap_or("");
            let emits = deps
                .lines()
                .any(|l| l.trim() == "\"tracing\"," || l.trim() == "\"log\",");
            if !emits {
                continue;
            }
            // EnvFilter targets are module paths: `hyper-util` is `hyper_util`.
            let target = name.replace('-', "_");
            if compiled.contains(&target) {
                emitters.push(target);
            } else {
                skipped_unresolved.push(target);
            }
        }
        assert!(
            !emitters.is_empty(),
            "the lockfile walk found no emitters at all — the parse broke, and this test would \
             then pass vacuously forever"
        );
        for target in &emitters {
            assert!(
                DEFAULT_LOG_DIRECTIVE.contains(&format!("{target}=warn")),
                "`{target}` depends directly on `tracing` or on `log` and so can emit at INFO \
                 into this stream — the `log` route through `tracing_log::LogTracer`, which \
                 `try_init()` installs — but the default directive does not pin it to warn. Add \
                 `{target}=warn` to DEFAULT_LOG_DIRECTIVE (and say so in its doc comment), or the \
                 default stream gains lines that cannot carry the run id. Emitters in the \
                 resolved graph: {emitters:?}; in the lockfile but not compiled: \
                 {skipped_unresolved:?}; directive: {DEFAULT_LOG_DIRECTIVE:?}"
            );
        }
        // The other direction: the directive names nothing that is not an
        // emitter in the resolved graph. Without this, `quinn=warn` could
        // return tomorrow and read as coverage it does not provide.
        for pinned in DEFAULT_LOG_DIRECTIVE.split(',').skip(1) {
            let target = pinned.trim_end_matches("=warn");
            assert!(
                emitters.iter().any(|e| e == target),
                "DEFAULT_LOG_DIRECTIVE pins `{target}`, which is not a `tracing`/`log` emitter in \
                 this build's resolved graph. Naming a crate that cannot emit is decoration, and \
                 decoration reads as coverage. Emitters found: {emitters:?}"
            );
        }
    }

    /// Review finding M2: the line that joins the run's identity to the
    /// engine's captured output — `phase_observer(run_id)`, handed to phase 6
    /// — was referenced by no test, so an observer built with the wrong id
    /// could not be killed without a live broker.
    #[test]
    fn the_phase_observer_the_orchestrator_builds_carries_the_runs_id() {
        use logweir_core::engine::PhaseObserver;
        // A real id from the real generator, exactly as `run()` mints it, so
        // this cannot pass by matching a literal against itself.
        let run_id = crate::ids::new_run_id();
        let mut obs = phase_observer(&run_id);
        let lines = under_the_default_directive(|| {
            obs.engine_line("stdout", "Restoring topic orders-restored partition 0");
        });
        let line = lines
            .iter()
            .find(|v| v["fields"]["message"] == "engine output")
            .unwrap_or_else(|| panic!("the observer emitted no engine line: {lines:?}"));
        assert_eq!(
            line["fields"]["run_id"].as_str(),
            Some(run_id.as_str()),
            "the observer phase 6 is handed must carry THIS run's id, not a fresh one: {line}"
        );
    }

    /// GC11's swallow discipline, which nothing asserted: a metrics write that
    /// FAILS is logged and swallowed, and never moves the exit code the drill
    /// already decided. The textfile is a local operational side-channel on a
    /// node-local volume — it is not the artifact and not the upload — so a
    /// full disk, a read-only mount or a deleted `hostPath` must not turn a
    /// pass into an operational error, and must not turn a guard refusal into
    /// one either.
    ///
    /// Both writers are driven, because the mutant that matters makes `publish`
    /// report failure and `report` demote the code on it: `finish` ->
    /// `write_textfile` on exits 0 and 2, and `publish` -> `write_minimal_textfile`
    /// on 1, 3 and 4. A test that drove only one arm would let the other half
    /// through.
    ///
    /// The unwritable sink is a path UNDER A FILE, not a `chmod 500`
    /// directory: `ENOTDIR` comes back whoever the process is, so this does not
    /// quietly stop testing anything when it runs as root.
    #[test]
    fn a_failed_metrics_write_is_swallowed_and_never_moves_the_exit_code() {
        let dir = tempfile::tempdir().unwrap();
        let blocker = dir.path().join("not-a-directory");
        std::fs::write(
            &blocker,
            b"this is a file, so nothing can be created beneath it",
        )
        .unwrap();
        let sink = blocker.join("logweir.prom");

        let mut notpass = a_scorecard();
        notpass.outcome = Outcome::FailIntegrity;
        let cases: Vec<(ExitCode, Result<RestoreOutcome, DrillError>)> = vec![
            (ExitCode::Ok, Ok(an_outcome())),
            (
                ExitCode::DrillNotPass,
                Err(DrillError::NotPass(Box::new(notpass))),
            ),
            (
                ExitCode::Operational,
                Err(DrillError::Operational("broker unreachable".into())),
            ),
            (
                ExitCode::GuardRefused,
                Err(DrillError::Guard(logweir_core::guard::GuardRefusal(
                    "x".into(),
                ))),
            ),
            (
                ExitCode::SigningOrLock,
                Err(DrillError::SigningOrLock("x".into())),
            ),
        ];

        for (want, outcome) in cases {
            let logs = CapturedLogs::default();
            let subscriber = tracing_subscriber::fmt()
                .with_writer(logs.clone())
                .with_max_level(tracing::Level::WARN)
                .with_ansi(false)
                .finish();
            let got = tracing::subscriber::with_default(subscriber, || {
                report(&args_with(Some(sink.clone())), "01TEST", None, outcome)
            });

            assert_eq!(
                got, want,
                "a metrics textfile that could not be written moved the exit code from {} to {}. \
                 The textfile is an operational side-channel, not the artifact: GC11 says exit 1 \
                 means \"no artifact\" and exit 4 means \"nothing uploaded\", and neither claim \
                 is affected by a file that failed to land on a node-local volume.",
                want as u8, got as u8
            );
            assert!(
                !sink.exists(),
                "the sink was supposed to be unwritable; this test is not testing anything"
            );

            let text = logs.text();
            assert!(
                text.contains("metrics textfile not written"),
                "exit {} swallowed the failure SILENTLY. Swallowed is right; silent is not — \
                 the operator has no other way to learn the file they are alerting on was \
                 never written.\ncaptured logs:\n{text}",
                want as u8
            );
            assert!(
                text.contains("WARN"),
                "the failure must be a WARNING, not an error line an alert would treat as a \
                 failed drill:\n{text}"
            );
        }
    }

    /// The other half of "no `--metrics-file` means no file, not a default
    /// path", and the half `finish_writes_no_metrics_file_when_the_flag_is_absent`
    /// structurally cannot see: that test inspects a tempdir the writer was
    /// never told about, so a default invented RELATIVE TO THE WORKING
    /// DIRECTORY — `PathBuf::from("logweir.prom")`, which is how anyone would
    /// actually write this bug — walks straight past it and lands in the
    /// process's CWD instead. This watches the working directory itself, and
    /// it watches it across `finish` AND across every terminal path of
    /// `report`, because since T0-7 there are two places that read the flag.
    #[test]
    fn no_terminal_path_invents_a_metrics_file_when_the_flag_is_absent() {
        fn cwd_entries() -> std::collections::BTreeSet<PathBuf> {
            std::fs::read_dir(".")
                .expect("the test's working directory is readable")
                .map(|e| e.expect("a readable directory entry").path())
                .collect()
        }
        let before = cwd_entries();

        finish(&args_with(None), &a_scorecard());
        let mut notpass = a_scorecard();
        notpass.outcome = Outcome::FailIntegrity;
        for outcome in [
            Ok(an_outcome()),
            Err(DrillError::NotPass(Box::new(notpass))),
            Err(DrillError::Operational("x".into())),
            Err(DrillError::Guard(logweir_core::guard::GuardRefusal(
                "x".into(),
            ))),
            Err(DrillError::SigningOrLock("x".into())),
        ] {
            report(&args_with(None), "01TEST", None, outcome);
        }

        let after = cwd_entries();
        let appeared: Vec<_> = after.difference(&before).collect();
        assert!(
            appeared.is_empty(),
            "no --metrics-file means no file, not a default path. These appeared in \
             the working directory: {appeared:?}"
        );
        // The difference alone is not enough on a second run: a textfile a
        // previous run of this test left behind is in BOTH snapshots and
        // cancels out. A `.prom` in the crate root is the defect either way.
        let stray: Vec<_> = after
            .iter()
            .filter(|p| {
                p.to_string_lossy().ends_with(".prom") || p.to_string_lossy().ends_with(".prom.tmp")
            })
            .collect();
        assert!(
            stray.is_empty(),
            "a Prometheus textfile in the crate root is an invented default path, \
             whichever run wrote it: {stray:?}"
        );
    }

    /// `drill run` must not be silent, and the line must carry the three
    /// facts an operator reads off a terminated pod: which run, what it
    /// concluded, and how far it got.
    #[test]
    fn the_summary_line_names_the_run_the_outcome_and_the_last_phase() {
        let mut sc = a_scorecard();
        sc.outcome = Outcome::PreflightFailed;
        sc.last_phase_completed = 5;
        let line = summary_line(&sc);
        assert!(line.contains(&sc.run_id), "{line}");
        assert!(line.contains("preflight-failed"), "{line}");
        assert!(line.contains("5"), "{line}");
    }

    /// THE routing, and the product's primary output. A drill RESULT must
    /// reach exit 2 with its metrics written; an operational failure must
    /// reach exit 1 — and, since T0-7, must ALSO leave a record. The file no
    /// longer distinguishes the paths by its existence; `logweir_drill_exit_code`
    /// inside it does, and the absence of `logweir_drill_runs_total` says no
    /// scorecard was ever built.
    #[test]
    fn a_drill_result_reports_exit_2_and_an_operational_failure_reports_exit_1() {
        let dir = tempfile::tempdir().unwrap();

        let pass = dir.path().join("pass.prom");
        assert_eq!(
            report(
                &args_with(Some(pass.clone())),
                "01TEST",
                None,
                Ok(an_outcome())
            ),
            ExitCode::Ok
        );
        assert!(pass.exists(), "a pass writes its metrics");

        let notpass = dir.path().join("notpass.prom");
        let mut sc = a_scorecard();
        sc.outcome = Outcome::FailIntegrity;
        assert_eq!(
            report(
                &args_with(Some(notpass.clone())),
                "01TEST",
                None,
                Err(DrillError::NotPass(Box::new(sc)))
            ),
            ExitCode::DrillNotPass,
            "a drill that ran and did not pass is exit 2, never exit 1: the \
             signed scorecard exists and an operator has to be told to read it"
        );
        assert!(
            notpass.exists(),
            "exit 2 owes the same metrics a pass does — it is a real result"
        );

        let op = dir.path().join("operational.prom");
        assert_eq!(
            report(
                &args_with(Some(op.clone())),
                "01TEST",
                None,
                Err(DrillError::Operational("broker unreachable".into()))
            ),
            ExitCode::Operational
        );
        assert!(
            op.exists(),
            "T0-7: exit 1 must leave a minimal textfile. \"the drill did not run\" and \"the \
             drill failed operationally\" were the same observation until this line changed"
        );

        for (e, want, name) in [
            (
                DrillError::Guard(logweir_core::guard::GuardRefusal("x".into())),
                ExitCode::GuardRefused,
                "guard.prom",
            ),
            (
                DrillError::SigningOrLock("x".into()),
                ExitCode::SigningOrLock,
                "signing.prom",
            ),
        ] {
            let p = dir.path().join(name);
            assert_eq!(
                report(&args_with(Some(p.clone())), "01TEST", None, Err(e)),
                want
            );
            assert!(
                p.exists(),
                "T0-7: exit {} leaves no scorecard, which is exactly why it owes a record",
                want as u8
            );
        }
    }

    /// T0-7. Exit 1 wrote nothing at all, so a CronJob whose broker was
    /// unreachable and a CronJob that never ran produced the same observation:
    /// none.
    #[test]
    fn metrics_written_on_operational_failure() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("op.prom");
        assert_eq!(
            report(
                &args_with(Some(p.clone())),
                "01TEST",
                None,
                Err(DrillError::Operational("broker unreachable".into()))
            ),
            ExitCode::Operational
        );
        let t = std::fs::read_to_string(&p).expect("exit 1 must leave a minimal textfile");
        assert!(
            t.contains("logweir_drill_exit_code{cluster=\"unknown\"} 1"),
            "R-11b: the label is always present and is the literal `unknown` on a path \
             that never reached phase 2 — a series that sometimes has a label and \
             sometimes does not is a Prometheus modelling error:\n{t}"
        );
    }

    /// A guard refusal is the earliest terminal path there is: phase 0, before
    /// the target cluster id exists to label anything with.
    #[test]
    fn metrics_written_on_guard_refusal() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("guard.prom");
        assert_eq!(
            report(
                &args_with(Some(p.clone())),
                "01TEST",
                None,
                Err(DrillError::Guard(logweir_core::guard::GuardRefusal(
                    "x".into()
                )))
            ),
            ExitCode::GuardRefused
        );
        let t = std::fs::read_to_string(&p).expect("exit 3 must leave a minimal textfile");
        assert!(
            t.contains("logweir_drill_exit_code{cluster=\"unknown\"} 3"),
            "{t}"
        );
    }

    /// Exit 4 is the one an operator most needs told: the drill ran, and its
    /// result is unattested. Nothing was uploaded, so the textfile is the only
    /// thing there is to find.
    #[test]
    fn metrics_written_on_signing_failure() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("signing.prom");
        assert_eq!(
            report(
                &args_with(Some(p.clone())),
                "01TEST",
                None,
                Err(DrillError::SigningOrLock("x".into()))
            ),
            ExitCode::SigningOrLock
        );
        let t = std::fs::read_to_string(&p).expect("exit 4 must leave a minimal textfile");
        assert!(
            t.contains("logweir_drill_exit_code{cluster=\"unknown\"} 4"),
            "{t}"
        );
    }

    /// EVERY terminal path, enumerated by the compiler rather than by whoever
    /// last edited this file. `representative` matches exhaustively on
    /// `ExitCode`, so a sixth variant stops this test compiling until someone
    /// says what terminal path produces it and what record it leaves.
    #[test]
    fn metrics_terminal_paths_are_exhaustive() {
        // `DrillError::RestoreNoOp` is deliberately not represented: reaching
        // the ExitCode conversion with it is an `unreachable!` (ruling A8), and
        // this task does not change that.
        fn representative(code: ExitCode) -> Result<RestoreOutcome, DrillError> {
            match code {
                ExitCode::Ok => Ok(an_outcome()),
                ExitCode::DrillNotPass => {
                    let mut sc = a_scorecard();
                    sc.outcome = Outcome::FailIntegrity;
                    Err(DrillError::NotPass(Box::new(sc)))
                }
                ExitCode::Operational => Err(DrillError::Operational("x".into())),
                ExitCode::GuardRefused => Err(DrillError::Guard(
                    logweir_core::guard::GuardRefusal("x".into()),
                )),
                ExitCode::SigningOrLock => Err(DrillError::SigningOrLock("x".into())),
            }
        }

        let dir = tempfile::tempdir().unwrap();
        for code in [
            ExitCode::Ok,
            ExitCode::Operational,
            ExitCode::DrillNotPass,
            ExitCode::GuardRefused,
            ExitCode::SigningOrLock,
        ] {
            let p = dir.path().join(format!("exit-{}.prom", code as u8));
            assert_eq!(
                report(
                    &args_with(Some(p.clone())),
                    "01TEST",
                    None,
                    representative(code)
                ),
                code
            );
            let t = std::fs::read_to_string(&p).unwrap_or_else(|e| {
                panic!(
                    "exit {} is a terminal path and left no metrics record at all: {e}",
                    code as u8
                )
            });
            assert!(
                t.lines()
                    .any(|l| l.starts_with("logweir_drill_last_run_timestamp_seconds")),
                "exit {} left a record with no timestamp, so nothing says WHEN:\n{t}",
                code as u8
            );
        }
    }

    /// A pinned `source.backup` that the archive does not hold must be
    /// refused BY NAME. Falling back to the newest set would restore
    /// something other than the document the approver signed, which makes the
    /// whole approval chain meaningless.
    #[test]
    fn a_pinned_backup_id_the_archive_does_not_hold_is_refused_never_substituted() {
        use logweir_core::engine::*;

        struct TwoSets;
        impl DataEngine for TwoSets {
            fn id(&self) -> EngineId {
                unreachable!()
            }
            fn list_backup_sets(&self, _l: &StorageUrl) -> Result<Vec<BackupSetRef>, EngineError> {
                Ok(["older", "newest"]
                    .iter()
                    .map(|b| BackupSetRef {
                        backup_id: (*b).into(),
                        manifest_key: format!("{b}/manifest.json"),
                    })
                    .collect())
            }
            fn describe(&self, _s: &BackupSetRef) -> Result<BackupSetFacts, EngineError> {
                unreachable!()
            }
            fn preflight(&self, _p: &RestorePlan) -> Result<PreflightReport, EngineError> {
                unreachable!()
            }
            fn restore(
                &self,
                _p: &RestorePlan,
                _o: &mut dyn PhaseObserver,
            ) -> Result<RestoreFacts, EngineError> {
                unreachable!()
            }
            fn fingerprints(
                &self,
                _s: &SampleSelection,
            ) -> Result<Vec<RecordFingerprint>, EngineError> {
                unreachable!()
            }
        }

        let mut spec: DrillSpec = serde_yaml::from_str(
            "source:\n  storage:\n    backend: filesystem\n    path: /a\n  topics: [t]\n\
             target:\n  bootstrap_servers: [x:9092]\n  topic_mapping_prefix: \"d-\"\n\
             sample:\n  window_start: \"2026-01-01T00:00:00Z\"\n  window_end: \"2026-01-02T00:00:00Z\"\n\
             objectives: {}\n\
             evidence:\n  backend: filesystem\n  path: /b\n",
        )
        .unwrap();

        spec.source.backup = "latestCompleted".into();
        assert_eq!(
            pick_backup_set(&TwoSets, &spec).unwrap().backup_id,
            "newest",
            "latestCompleted takes the last set list_backup_sets returns"
        );

        spec.source.backup = "older".into();
        assert_eq!(pick_backup_set(&TwoSets, &spec).unwrap().backup_id, "older");

        spec.source.backup = "a-backup-that-was-deleted".into();
        let e = pick_backup_set(&TwoSets, &spec).unwrap_err();
        assert!(
            matches!(e, DrillError::Operational(ref m)
                     if m.contains("a-backup-that-was-deleted") && m.contains("refusing to fall back")),
            "the refusal must name the missing id: {e}"
        );
    }

    /// Every exit code gets a distinct sentence, and 1 and 2 in particular
    /// must not read alike: at the delivery layer (a Kubernetes Job) they are
    /// otherwise indistinguishable.
    #[test]
    fn every_exit_code_logs_a_distinct_meaning() {
        let codes = [
            ExitCode::Ok,
            ExitCode::Operational,
            ExitCode::DrillNotPass,
            ExitCode::GuardRefused,
            ExitCode::SigningOrLock,
        ];
        let mut seen: Vec<u8> = Vec::new();
        for c in codes {
            assert_eq!(
                exiting("01TEST", c, None, None, None),
                c,
                "exiting must not alter the code"
            );
            seen.push(c as u8);
        }
        seen.sort_unstable();
        seen.dedup();
        assert_eq!(seen, vec![0, 1, 2, 3, 4]);
    }

    // ---------------------------------------------------------------- T0-15
    // The notification half of the terminal paths. `report_with` is private,
    // so these live here rather than in `crates/logweir/tests/notify.rs`, where
    // the rest of T0-15's coverage is.

    /// An `EventSink` that records instead of dialling (GC17).
    #[derive(Default)]
    struct RecordingSink {
        posts: std::sync::Mutex<Vec<(String, serde_json::Value)>>,
    }

    impl phase7_verify::EventSink for RecordingSink {
        fn post(&self, url: &str, body: &serde_json::Value) -> Result<(), String> {
            self.posts
                .lock()
                .unwrap()
                .push((url.to_string(), body.clone()));
            Ok(())
        }
    }

    /// The checked-in example spec, plus the notification config a drill that
    /// wants to be paged would carry.
    fn spec_with_a_routing_key() -> DrillSpec {
        let mut spec: DrillSpec =
            serde_yaml::from_str(include_str!("../../../../examples/drill.yaml")).unwrap();
        spec.name = Some("nightly".into());
        spec.notifications.pagerduty_routing_key = Some("R0FAKEFAKEFAKEFAKEFAKEFAKEFAKE0".into());
        spec
    }

    /// **M1.** The exit-1 path pages. This is the whole of T0-15: before it,
    /// `report`'s no-scorecard branch logged and printed and did nothing else,
    /// so the three codes that mean "a human is needed and there is no
    /// artifact to read instead" notified nobody, while the one code that
    /// meant "the drill finished and signed its own scorecard" was loud.
    ///
    /// `metrics_file: None` on purpose: `publish` returns early on that arm,
    /// so this asserts the notification and nothing about Task 11's textfile.
    #[test]
    fn report_pages_on_an_operational_failure() {
        let spec = spec_with_a_routing_key();
        let sink = RecordingSink::default();
        let code = report_with(
            &args_with(None),
            "01TEST",
            Some(&spec),
            Err(DrillError::Operational("broker unreachable".into())),
            &sink,
        );

        assert_eq!(
            code,
            ExitCode::Operational,
            "GC11: the notification must not touch the exit code"
        );
        let posts = sink.posts.lock().unwrap().clone();
        assert_eq!(posts.len(), 1, "exit 1 left no page: {posts:?}");
        assert_eq!(posts[0].0, phase7_verify::PAGERDUTY_US_ENDPOINT);
        assert_eq!(posts[0].1["event_action"], "trigger");
        assert_eq!(
            posts[0].1["dedup_key"],
            serde_json::json!(phase7_verify::failure_dedup_key(Some("nightly")))
        );
    }

    /// The other side of the same branch, in both directions.
    ///
    /// A drill RESULT (exit 0 and exit 2) is phase 8's to notify — it has a
    /// scorecard, and `report` must not page a second time — and a spec that
    /// configured no route is never paged at all.
    #[test]
    fn report_pages_only_where_there_is_no_scorecard() {
        let spec = spec_with_a_routing_key();

        for outcome in [
            Ok(an_outcome()),
            Err(DrillError::NotPass(Box::new(a_scorecard()))),
        ] {
            let sink = RecordingSink::default();
            report_with(&args_with(None), "01TEST", Some(&spec), outcome, &sink);
            assert!(
                sink.posts.lock().unwrap().is_empty(),
                "a drill RESULT was paged from `report`; phase 8 already notified with the \
                 scorecard in hand, and this would be a second, scorecard-less page for one \
                 drill"
            );
        }

        // No spec reachable at all (an unreadable or unparseable spec file is
        // itself an exit-1 operational failure): nothing to page with, and the
        // drill still exits 1.
        let sink = RecordingSink::default();
        let code = report_with(
            &args_with(None),
            "01TEST",
            None,
            Err(DrillError::Operational("spec unreadable".into())),
            &sink,
        );
        assert_eq!(code, ExitCode::Operational);
        assert!(sink.posts.lock().unwrap().is_empty());

        let sink = RecordingSink::default();
        let code = report_with(
            &args_with(None),
            "01TEST",
            Some(&spec),
            Err(DrillError::SigningPrerequisite("invalid key".into())),
            &sink,
        );
        assert_eq!(code, ExitCode::SigningOrLock);
        assert!(
            sink.posts.lock().unwrap().is_empty(),
            "signer prerequisite diagnostics are local-only even if a caller incorrectly supplies \
             notification configuration"
        );
    }

    /// **The signed document says WHICH MODE the run was in, and names a
    /// marker topic only in the mode that verified one** (fix round 1, review
    /// F1).
    ///
    /// Through `target_info`, not through `marker_topic_for` alone: the helper
    /// is the rule's owner but `target_info` is the CALL SITE, and the mutant
    /// this row exists for — write `spec.target.marker_topic` into a
    /// `newTopic` document, which is what shipped — is spellable at either.
    ///
    /// The reviewer's live counterexample is the reason: a real run with
    /// `logweir.scratch` DELETED from the cluster and an EMPTY allowlist
    /// exited 0 and signed `target.marker_topic: "logweir.scratch"` anyway,
    /// with nothing in the document to say the two checks behind that field
    /// had been skipped.
    #[test]
    fn the_target_block_carries_the_mode_and_the_marker_topic_only_in_scratch() {
        use logweir_core::spec::TargetMode;

        let mut spec: DrillSpec = serde_yaml::from_str(
            "source:\n  storage:\n    backend: filesystem\n    path: /a\n  topics: [orders]\n\
             target:\n  bootstrap_servers: [x:9092]\n  marker_topic: logweir.scratch\n  \
             topic_mapping_prefix: \"drill-\"\n\
             sample:\n  window_start: \"2026-01-01T00:00:00Z\"\n  window_end: \"2026-01-02T00:00:00Z\"\n\
             objectives: {}\n\
             evidence:\n  backend: filesystem\n  path: /b\n",
        )
        .unwrap();

        let admitted = phase0_admit::Admitted {
            target_cluster_id: "CLUSTER00000000000000AA".into(),
            topic_mapping: [("orders".to_string(), "drill-orders".to_string())]
                .into_iter()
                .collect(),
            topic_mapping_prefix: "drill-".into(),
            topic_preflight: phase0_admit::TopicPreflight {
                timestamp_type: "CreateTime".into(),
                retention_ms: "-1".into(),
                timestamp_bound_ms: None,
                configs_set: Vec::new(),
                topics_created: Vec::new(),
            },
        };

        // `scratch` — v0.1's document, unchanged, and the mode absent on the
        // wire so the three checked-in signed fixtures keep their bytes.
        let t = target_info(&spec, &admitted).expect("the scratch target block");
        assert!(t.mode.is_scratch());
        assert_eq!(t.marker_topic.as_deref(), Some("logweir.scratch"));
        let wire = serde_json::to_value(&t).expect("a target block serialises");
        assert!(
            wire.get("mode").is_none(),
            "scratch is the default and must add no key: {wire}"
        );

        // `newTopic` — the mode is stated and the marker topic is GONE, key
        // and all, because phase 0 skipped both checks the field stands for.
        spec.target.mode = TargetMode::NewTopic;
        let t = target_info(&spec, &admitted).expect("the newTopic target block");
        assert_eq!(t.mode, TargetMode::NewTopic);
        assert_eq!(
            t.marker_topic, None,
            "a newTopic run verified no marker topic, so its document names none"
        );
        let wire = serde_json::to_value(&t).expect("a target block serialises");
        assert_eq!(
            wire.get("mode").and_then(serde_json::Value::as_str),
            Some("newTopic"),
            "and the mode is on the wire, in the CRD's own spelling: {wire}"
        );
        assert!(
            wire.get("marker_topic").is_none(),
            "an absent marker topic is an absent KEY, not a null one: {wire}"
        );
    }

    /// Reporting uses the authenticated in-memory plan snapshot and never
    /// follows a later replacement at `args.spec`.
    #[test]
    fn late_plan_replacement_cannot_change_the_notification_destination() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("drill.yaml");
        let mut authenticated = spec_with_a_routing_key();
        authenticated.notifications.pagerduty_endpoint =
            Some("https://trusted-notify.example/events".into());
        let mut substituted = authenticated.clone();
        substituted.notifications.pagerduty_endpoint =
            Some("https://substituted.example/events".into());
        std::fs::write(&p, serde_yaml::to_string(&substituted).unwrap()).unwrap();
        let mut args = args_with(None);
        args.spec = p;
        let sink = RecordingSink::default();
        let code = report_with(
            &args,
            "01TEST",
            Some(&authenticated),
            Err(DrillError::Operational("post-auth startup failure".into())),
            &sink,
        );
        assert_eq!(code, ExitCode::Operational);
        let posts = sink.posts.lock().unwrap();
        assert_eq!(posts.len(), 1);
        assert_eq!(posts[0].0, "https://trusted-notify.example/events");
        assert_ne!(posts[0].0, "https://substituted.example/events");
    }
}
