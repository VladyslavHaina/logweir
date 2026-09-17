#![forbid(unsafe_code)]
//! `logweir-retention` — the one supported enforcer (decision D3 §6.5).
//!
//! # Why this is its own binary
//!
//! D-SEAMS **S1** says there is one check runner and that `logweir check run`
//! is it. This is the third recorded exception and the reason is structural:
//! **deletion linkage must not be reachable from the everyday binary.** A
//! `logweir retention run` subcommand would link `logweir-reaper` into the same
//! executable an operator uses to take a backup, verify a receipt or print a
//! scorecard, and the capability gate would have nothing left to prove.
//!
//! # The contract
//!
//! ```text
//! logweir-retention run --plan <path> --retention-contract-version 1 [--dry-run]
//! ```
//!
//! Everything else arrives in the environment, because a Job spec is readable
//! by anyone with `get jobs` and an argv is readable in every process listing
//! on the node:
//!
//! | variable | meaning |
//! |---|---|
//! | `LOGWEIR_RETENTION_PLAN_SHA256` | the digest an administrator approved |
//! | `LOGWEIR_RETENTION_POLICY_UID` | the policy this run is for |
//! | `LOGWEIR_RETENTION_POLICY_GENERATION` | the generation |
//! | `LOGWEIR_RETENTION_SCOPE_PREFIX` | `spec.scope.prefix`, checked against the plan's own |
//! | `LOGWEIR_RETENTION_RUN_ID` | the run id, deterministic in the controller |
//! | `LOGWEIR_RETENTION_APPROVER` | the audit id or subject, or `unattended` |
//! | `LOGWEIR_RETENTION_MAX_DELETIONS` | the per-run point ceiling |
//! | `LOGWEIR_RETENTION_MAX_OBJECTS` | the per-run object ceiling |
//! | `LOGWEIR_RETENTION_LOCATION` | the destination's `DestinationLocation`, as JSON |
//! | `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` / `AWS_SESSION_TOKEN` | the DELETE-capable grant |
//! | `LOGWEIR_EVIDENCE_AWS_ACCESS_KEY_ID` / `…SECRET_ACCESS_KEY` / `…SESSION_TOKEN` | the `evidenceWrite` grant |
//!
//! **Two credentials, and that is the design.** The delete-capable one is
//! scoped to `<prefix>/*` EXCLUDING `logweir/*`; it cannot write the record
//! that makes the deletion attributable. The `evidenceWrite` grant writes under
//! `logweir/` and cannot delete. Neither alone can both remove a point and
//! forge its record.
//!
//! # The exit codes — inside Global Constraint 11's existing four
//!
//! | code | meaning |
//! |---|---|
//! | **0** | every planned point completed, or `--dry-run` finished having deleted nothing |
//! | **1** | at least one point did not complete: `Orphaned` or `Kept`, with its closed code |
//! | **3** | the plan was REFUSED before anything ran — scope violation, digest mismatch, policy mismatch, unreadable plan, no record sink. **Zero objects deleted.** |
//!
//! **2 and 4 are never returned.** Global Constraint 11 reserves 2 for a drill
//! result that is not a pass (a scorecard IS written) and 4 for a signing or
//! lock-proof failure; this binary produces no drill result and signs nothing,
//! so either code would make a deletion failure indistinguishable from a drill
//! outcome to `weirkeeper::conditions::reason_for_exit`.
//!
//! # Stdout
//!
//! Key lines, read by name and never by position, as the runner's other
//! surfaces are:
//!
//! ```text
//! retention-plan=sha256:… points=3 objects=19
//! retention-point=<pointId> state=Deleted objects=7
//! retention-point=<pointId> state=Orphaned objects=3 code=AccessDenied
//! retention-record=logweir/retention/<uid>/<runId>.json sha256=sha256:…
//! retention-result=deleted=2 failed=1 objects=10
//! ```

use std::process::ExitCode;

use chrono::Utc;

use logweir_core::destination::DestinationLocation;
use logweir_reaper::{
    execute, parse_plan, record, record_bytes, validate_plan, ArchiveReaper, Credentials, Limits,
    RecordContext, RunAttribution, RunBinding, SinkError, ThreadSleeper, TombstoneSink,
};
use logweir_store::{Store, StoreError, StoreOptions};

/// The contract version this binary implements. An older controller's Job would
/// pass a different one and be refused before it read the plan.
const CONTRACT_VERSION: &str = "1";

const HELP: &str = "logweir-retention — the Logweir retention worker. It executes one \
administrator-approved deletion plan against one archive prefix and writes the attributable \
record. It is a SEPARATE binary from `logweir` on purpose: deletion linkage must not be reachable \
from the everyday command line. Usage: logweir-retention run --plan <path> \
--retention-contract-version 1 [--dry-run].";

fn main() -> ExitCode {
    let argv: Vec<String> = std::env::args().skip(1).collect();
    let flags: Vec<&str> = argv.iter().map(String::as_str).collect();
    match flags.as_slice() {
        ["--version"] => {
            println!("logweir-retention {}", env!("CARGO_PKG_VERSION"));
            ExitCode::SUCCESS
        }
        ["--help"] | [] => {
            println!("{HELP}");
            ExitCode::SUCCESS
        }
        _ => run(&flags),
    }
}

fn run(flags: &[&str]) -> ExitCode {
    let mut plan_path: Option<&str> = None;
    let mut contract: Option<&str> = None;
    let mut dry_run = false;
    let mut i = 0usize;
    if flags.first() != Some(&"run") {
        eprintln!(
            "logweir-retention: the only subcommand is `run`; got `{}`",
            flags.join(" ")
        );
        return ExitCode::from(3);
    }
    i += 1;
    while i < flags.len() {
        match flags[i] {
            "--plan" => {
                plan_path = flags.get(i + 1).copied();
                i += 2;
            }
            "--retention-contract-version" => {
                contract = flags.get(i + 1).copied();
                i += 2;
            }
            "--dry-run" => {
                dry_run = true;
                i += 1;
            }
            other => {
                eprintln!("logweir-retention: unrecognised argument `{other}`");
                return ExitCode::from(3);
            }
        }
    }
    // THE CONTRACT VERSION BEFORE THE PLAN. A newer controller handing this
    // binary a plan shape it does not implement must be refused BY NAME, not
    // partially obeyed — the same handshake `logweir check run` and execution
    // contract v2 use, and for the same reason.
    if contract != Some(CONTRACT_VERSION) {
        eprintln!(
            "logweir-retention: --retention-contract-version {CONTRACT_VERSION} is required; got \
             {contract:?}. An older worker refuses a newer plan rather than executing the half of \
             it that it understands."
        );
        return ExitCode::from(3);
    }
    let Some(plan_path) = plan_path else {
        eprintln!("logweir-retention: --plan <path> is required");
        return ExitCode::from(3);
    };

    let started_at = Utc::now();
    let env = |name: &str| std::env::var(name).ok().filter(|v| !v.is_empty());

    let (Some(approved), Some(policy_uid), Some(generation), Some(scope), Some(run_id)) = (
        env("LOGWEIR_RETENTION_PLAN_SHA256"),
        env("LOGWEIR_RETENTION_POLICY_UID"),
        env("LOGWEIR_RETENTION_POLICY_GENERATION"),
        env("LOGWEIR_RETENTION_SCOPE_PREFIX"),
        env("LOGWEIR_RETENTION_RUN_ID"),
    ) else {
        eprintln!(
            "logweir-retention: the run binding is incomplete. \
             LOGWEIR_RETENTION_PLAN_SHA256, _POLICY_UID, _POLICY_GENERATION, _SCOPE_PREFIX and \
             _RUN_ID are all required, and a variable that is present and blank is not set."
        );
        return ExitCode::from(3);
    };
    let Ok(generation) = generation.parse::<i64>() else {
        eprintln!("logweir-retention: LOGWEIR_RETENTION_POLICY_GENERATION is not a number");
        return ExitCode::from(3);
    };
    let binding = RunBinding {
        policy_uid: policy_uid.clone(),
        policy_generation: generation,
        scope_prefix: scope,
        max_deletions_per_run: env("LOGWEIR_RETENTION_MAX_DELETIONS")
            .and_then(|v| v.parse().ok())
            .unwrap_or(50),
        max_objects_per_run: env("LOGWEIR_RETENTION_MAX_OBJECTS")
            .and_then(|v| v.parse().ok())
            .unwrap_or(20_000),
    };

    let bytes = match std::fs::read(plan_path) {
        Ok(b) => b,
        Err(e) => {
            eprintln!("logweir-retention: the plan at `{plan_path}` could not be read: {e}");
            return ExitCode::from(3);
        }
    };
    let plan = match parse_plan(&bytes, &approved) {
        Ok(p) => p,
        Err(e) => {
            eprintln!("logweir-retention: {e}");
            return ExitCode::from(3);
        }
    };
    if let Err(e) = validate_plan(&plan, &binding) {
        eprintln!("logweir-retention: {e}");
        return ExitCode::from(3);
    }
    println!(
        "retention-plan={approved} points={} objects={}",
        plan.lines.len(),
        plan.object_keys().len()
    );

    // The destination, from its frozen block and from nothing else.
    let Some(location_json) = env("LOGWEIR_RETENTION_LOCATION") else {
        eprintln!("logweir-retention: LOGWEIR_RETENTION_LOCATION is required");
        return ExitCode::from(3);
    };
    let location: DestinationLocation = match serde_json::from_str(&location_json) {
        Ok(l) => l,
        Err(e) => {
            eprintln!("logweir-retention: LOGWEIR_RETENTION_LOCATION did not parse: {e}");
            return ExitCode::from(3);
        }
    };

    // A DRY RUN NEEDS NEITHER CREDENTIAL AND MUST NOT ASK FOR ONE. The preview
    // is exactly the validation above plus the plan echo, and asking for a
    // delete-capable Secret to produce it would make a preview need the
    // authority it exists to avoid.
    if dry_run {
        for line in &plan.lines {
            println!(
                "retention-point={} state=Kept objects=0 code=DryRun",
                line.point_id
            );
        }
        println!("retention-result=deleted=0 failed=0 objects=0 dryRun=true");
        return ExitCode::SUCCESS;
    }

    // THE RECORD SINK, BEFORE THE DELETER. "Every deletion is attributable" is
    // a precondition: a run that cannot write its record does not delete.
    let sink = match EvidenceSink::open(&location) {
        Ok(s) => s,
        Err(e) => {
            eprintln!(
                "logweir-retention: the enforcement record could not be opened ({e}); nothing is \
                 deleted, because an unattributable deletion is not performed"
            );
            return ExitCode::from(3);
        }
    };
    let archive_url = location.archive_storage_url();
    let allow_http = matches!(&archive_url, logweir_core::engine::StorageUrl::S3 { allow_http, .. } if *allow_http);
    let reaper =
        match ArchiveReaper::new(&archive_url, Credentials::from_env().as_ref(), allow_http) {
            Ok(r) => r,
            Err(e) => {
                eprintln!("logweir-retention: {e}; nothing is deleted");
                return ExitCode::from(3);
            }
        };

    let attribution = RunAttribution {
        run_id: run_id.clone(),
        policy_uid: policy_uid.clone(),
        plan_sha256: approved.clone(),
    };
    let outcome = execute(
        &plan,
        &reaper,
        &ThreadSleeper,
        &sink,
        &reaper,
        &attribution,
        Limits {
            dry_run: false,
            max_objects: binding.max_objects_per_run,
        },
    );

    for point in &outcome.points {
        match point.code.as_deref() {
            Some(code) => println!(
                "retention-point={} state={} objects={} code={code}",
                point.point_id, point.state, point.objects_deleted
            ),
            None => println!(
                "retention-point={} state={} objects={}",
                point.point_id, point.state, point.objects_deleted
            ),
        }
    }

    let exit_code = if outcome.complete() { 0 } else { 1 };
    let doc = record(
        &plan,
        &outcome,
        &RecordContext {
            run_id: run_id.clone(),
            approver: env("LOGWEIR_RETENTION_APPROVER").unwrap_or_else(|| "unattended".to_string()),
            plan_sha256: approved,
            started_at,
            finished_at: Utc::now(),
            exit_code,
        },
    );
    match record_bytes(&doc) {
        Ok((bytes, digest)) => {
            let key = logweir_reaper::record_key(&policy_uid, &run_id);
            match sink.put_create_only(&key, &bytes) {
                Ok(()) => println!("retention-record={key} sha256={digest}"),
                Err(e) => eprintln!(
                    "logweir-retention: the enforcement record at `{key}` was not written: {e}. \
                     The per-point tombstones under the same prefix are the surviving trail."
                ),
            }
        }
        Err(e) => eprintln!("logweir-retention: the enforcement record did not serialise: {e}"),
    }

    println!(
        "retention-result=deleted={} failed={} objects={}",
        outcome.deleted().len(),
        outcome.failed().len(),
        outcome.objects_deleted
    );
    ExitCode::from(u8::try_from(exit_code).unwrap_or(1))
}

/// The create-only sink under `logweir/`, with the `evidenceWrite` grant.
///
/// `logweir_store::Store` and not a second object-store handle: it is the one
/// crate that implements the one write Global Constraint 6 allows, its
/// `put_create_only` asserts the `logweir/` root in code, and it **cannot
/// delete** — the whole reason the deleting is in `logweir-reaper`.
struct EvidenceSink {
    store: Store,
}

impl EvidenceSink {
    fn open(location: &DestinationLocation) -> Result<Self, StoreError> {
        let url = location.evidence_storage_url();
        // THE EVIDENCE GRANT, BY NAMED VARIABLE. Falling back to the archive
        // credential would be exactly the aggregation the two-credential design
        // exists to prevent: the delete-capable principal must not be able to
        // write the record that attributes its own deletes.
        let non_empty = |name: &str| std::env::var(name).ok().filter(|v| !v.is_empty());
        let opts = match (
            non_empty("LOGWEIR_EVIDENCE_AWS_ACCESS_KEY_ID"),
            non_empty("LOGWEIR_EVIDENCE_AWS_SECRET_ACCESS_KEY"),
        ) {
            (Some(id), Some(secret)) => StoreOptions::static_keys(
                id,
                secret,
                non_empty("LOGWEIR_EVIDENCE_AWS_SESSION_TOKEN"),
            ),
            _ => {
                return Err(StoreError::Backend(
                    "no evidenceWrite credential is projected: \
                     LOGWEIR_EVIDENCE_AWS_ACCESS_KEY_ID and \
                     LOGWEIR_EVIDENCE_AWS_SECRET_ACCESS_KEY are both required, and the \
                     delete-capable archive credential is deliberately not a fall-back"
                        .to_string(),
                ))
            }
        };
        Ok(Self {
            store: Store::from_url_with(&url, &opts)?,
        })
    }
}

impl TombstoneSink for EvidenceSink {
    fn put_create_only(&self, key: &str, bytes: &[u8]) -> Result<(), SinkError> {
        self.store
            .put_create_only(key, bytes)
            .map(|_| ())
            .map_err(|e| SinkError(e.to_string()))
    }
}
