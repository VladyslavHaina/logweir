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
//! # Why this crate has a library target (review `d3w9` H6)
//!
//! The first landing put the whole binary in `main.rs`. No other crate can link
//! a `[[bin]]`, so `cargo test -p logweir-retention` ran **zero tests** —
//! against the one executable in this product that deletes archive data. The
//! reviewer proved what that costs: flipping `"--dry-run" => dry_run = true` to
//! `false`, so that a preview deletes for real, left every suite in the
//! workspace green.
//!
//! So the decision-making half lives here, behind two seams:
//!
//! * [`admit`] is **pure**. It takes the argv, an [`Environment`] and a
//!   plan-reading closure, and returns either everything the run needs or the
//!   [`Refusal`] that stops it. **Every exit-3 path in this crate is in it**,
//!   and none of them builds a port, so the whole refusal set is table-testable
//!   with no socket, no bucket and no process environment.
//! * [`execute`] takes the admitted run plus the four injected ports the reaper
//!   already abstracts, so the dry-run arm, the key lines and the exit code are
//!   observable without deleting anything.
//!
//! `main.rs` is what is left: read the real environment, open the real ports,
//! print, exit. Adding a library target does not widen the deletion boundary —
//! `scripts/check-no-archive-write.sh` check 3 walks package edges, so a crate
//! that linked this one to reach the reaper is caught transitively, which the
//! reviewer verified by planting exactly that.
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
//! | `LOGWEIR_RETENTION_POLICY_GENERATION` | the generation, for the record |
//! | `LOGWEIR_RETENTION_SCOPE_PREFIX` | `spec.scope.prefix`, checked against the plan's own |
//! | `LOGWEIR_RETENTION_RUN_ID` | the run id |
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
//! | **3** | the plan was REFUSED before anything ran. **Zero objects deleted.** |
//!
//! **2 and 4 are never returned.** Global Constraint 11 reserves 2 for a drill
//! result that is not a pass (a scorecard IS written) and 4 for a signing or
//! lock-proof failure; this binary produces no drill result and signs nothing,
//! so either code would make a deletion failure indistinguishable from a drill
//! outcome to `weirkeeper::conditions::reason_for_exit`.

use std::collections::BTreeMap;

use logweir_core::destination::DestinationLocation;
use logweir_core::engine::StorageUrl;
use logweir_reaper::{
    Deleter, Limits, Lister, Plan, RunAttribution, RunBinding, Sleeper, TombstoneSink,
};

/// The contract version this binary implements.
pub const CONTRACT_VERSION: &str = "1";

/// Exit 0.
pub const EXIT_OK: i32 = 0;
/// Exit 1 — work remains.
pub const EXIT_INCOMPLETE: i32 = 1;
/// Exit 3 — refused before anything ran.
pub const EXIT_REFUSED: i32 = 3;

// ---------------------------------------------------------------------------
// The environment seam
// ---------------------------------------------------------------------------

/// Where the run's binding comes from.
///
/// A trait and not `std::env::var`, so the thirteen refusal paths can be driven
/// from a map. `None` covers both "unset" and **"present and blank"**:
/// `std::env::var` returns `Ok("")` — not `Err(NotPresent)` — for a Kubernetes
/// `env:` entry with an empty `value:`, and a `secretKeyRef` to a key that
/// exists and is blank projects the same thing. Treating that as configured
/// produces an `AccessDenied` at 04:17 instead of a legible refusal at startup.
pub trait Environment {
    /// The value, or `None` when it is unset or blank.
    fn get(&self, name: &str) -> Option<String>;
}

/// The real process environment.
#[derive(Debug, Clone, Copy, Default)]
pub struct ProcessEnv;

impl Environment for ProcessEnv {
    fn get(&self, name: &str) -> Option<String> {
        std::env::var(name).ok().filter(|v| !v.is_empty())
    }
}

impl Environment for BTreeMap<String, String> {
    fn get(&self, name: &str) -> Option<String> {
        BTreeMap::get(self, name).filter(|v| !v.is_empty()).cloned()
    }
}

/// A map literal an `&[(&str, &str)]` builds, for tests and for `main`'s own
/// diagnostics.
#[must_use]
pub fn env_map(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
    pairs
        .iter()
        .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
        .collect()
}

// ---------------------------------------------------------------------------
// Refusals — every exit-3 path, in one closed enum
// ---------------------------------------------------------------------------

/// Why a run did not start. **Nothing has been deleted when one of these is
/// returned**, which is what makes them exit 3 rather than exit 1.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum Refusal {
    /// The argv does not begin with `run`.
    #[error("the only subcommand is `run`; got `{0}`")]
    NotRun(String),
    /// An argument this binary does not know.
    #[error("unrecognised argument `{0}`")]
    UnknownArgument(String),
    /// `--retention-contract-version` missing or not this build's.
    ///
    /// Checked BEFORE the plan is read: a newer controller handing this binary
    /// a plan shape it does not implement must be refused by name, not
    /// partially obeyed — the same handshake execution contract v2 uses.
    #[error(
        "--retention-contract-version {CONTRACT_VERSION} is required; got {0}. An older worker \
         refuses a newer plan rather than executing the half of it that it understands."
    )]
    ContractVersion(String),
    /// `--plan` missing.
    #[error("--plan <path> is required")]
    NoPlanPath,
    /// The plan file could not be read.
    #[error("the plan at `{path}` could not be read: {error}")]
    PlanUnreadable {
        /// The path.
        path: String,
        /// The I/O error.
        error: String,
    },
    /// A required binding variable is unset or blank.
    #[error(
        "the run binding is incomplete: {0} is required, and a variable that is present and \
         blank is not set"
    )]
    BindingIncomplete(&'static str),
    /// `LOGWEIR_RETENTION_POLICY_GENERATION` is not a number.
    #[error("LOGWEIR_RETENTION_POLICY_GENERATION is not a number: `{0}`")]
    GenerationNotANumber(String),
    /// `LOGWEIR_RETENTION_LOCATION` did not parse.
    #[error("LOGWEIR_RETENTION_LOCATION did not parse: {0}")]
    LocationUnreadable(String),
    /// The reaper refused the plan document.
    #[error("{0}")]
    Plan(#[from] logweir_reaper::Refusal),
    /// No `evidenceWrite` credential is projected.
    ///
    /// **A run that cannot attribute its deletions does not perform them.**
    #[error(
        "no evidenceWrite credential is projected: LOGWEIR_EVIDENCE_AWS_ACCESS_KEY_ID and \
         LOGWEIR_EVIDENCE_AWS_SECRET_ACCESS_KEY are both required, and the delete-capable \
         archive credential is deliberately not a fall-back. Nothing is deleted, because an \
         unattributable deletion is not performed."
    )]
    NoRecordCredential,
    /// A port could not be built.
    #[error("{0}; nothing is deleted")]
    Port(String),
}

impl Refusal {
    /// Every refusal here is exit 3, and saying so once is what keeps that
    /// true.
    #[must_use]
    pub fn exit_code(&self) -> i32 {
        EXIT_REFUSED
    }
}

// ---------------------------------------------------------------------------
// Admission — pure
// ---------------------------------------------------------------------------

/// A static credential, by named variable.
#[derive(Clone, PartialEq, Eq)]
pub struct Keys {
    /// The public half.
    pub access_key_id: String,
    /// The secret half. Never printed.
    pub secret_access_key: String,
    /// The session token, when one is projected.
    pub session_token: Option<String>,
}

impl std::fmt::Debug for Keys {
    /// Lengths and presence, never values.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Keys")
            .field("access_key_id_len", &self.access_key_id.len())
            .field("secret_access_key_len", &self.secret_access_key.len())
            .field("session_token", &self.session_token.is_some())
            .finish()
    }
}

/// Read one credential from its three named variables.
///
/// `prefix` is `""` for the archive grant (`AWS_*`) and `LOGWEIR_EVIDENCE_` for
/// the `evidenceWrite` grant. **The two never fall back to each other**: that
/// aggregation is exactly what the two-credential design exists to prevent.
#[must_use]
pub fn keys_from(env: &dyn Environment, prefix: &str) -> Option<Keys> {
    Some(Keys {
        access_key_id: env.get(&format!("{prefix}AWS_ACCESS_KEY_ID"))?,
        secret_access_key: env.get(&format!("{prefix}AWS_SECRET_ACCESS_KEY"))?,
        session_token: env.get(&format!("{prefix}AWS_SESSION_TOKEN")),
    })
}

/// The `LOGWEIR_EVIDENCE_` prefix, named once.
pub const EVIDENCE_PREFIX: &str = "LOGWEIR_EVIDENCE_";

/// Everything the argv decides.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Args {
    /// `--plan <path>`.
    pub plan_path: String,
    /// `--dry-run`.
    pub dry_run: bool,
}

/// The argv, parsed. **Pure.**
///
/// # Errors
///
/// [`Refusal`] — `NotRun`, `UnknownArgument`, `ContractVersion`, `NoPlanPath`.
pub fn parse_args(argv: &[&str]) -> Result<Args, Refusal> {
    if argv.first() != Some(&"run") {
        return Err(Refusal::NotRun(argv.join(" ")));
    }
    let mut plan_path: Option<String> = None;
    let mut contract: Option<String> = None;
    let mut dry_run = false;
    let mut i = 1usize;
    while i < argv.len() {
        match argv[i] {
            "--plan" => {
                plan_path = argv.get(i + 1).map(|s| (*s).to_string());
                i += 2;
            }
            "--retention-contract-version" => {
                contract = argv.get(i + 1).map(|s| (*s).to_string());
                i += 2;
            }
            "--dry-run" => {
                dry_run = true;
                i += 1;
            }
            other => return Err(Refusal::UnknownArgument(other.to_string())),
        }
    }
    // THE CONTRACT VERSION BEFORE THE PLAN PATH, and before any read.
    if contract.as_deref() != Some(CONTRACT_VERSION) {
        return Err(Refusal::ContractVersion(
            contract.unwrap_or_else(|| "<absent>".to_string()),
        ));
    }
    let plan_path = plan_path.ok_or(Refusal::NoPlanPath)?;
    Ok(Args { plan_path, dry_run })
}

/// A run that passed every pre-port check.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Admitted {
    /// The validated plan.
    pub plan: Plan,
    /// The run binding the Job carried.
    pub binding: RunBinding,
    /// The policy generation, for the record.
    pub policy_generation: i64,
    /// Where the archive is.
    pub location: DestinationLocation,
    /// The attribution every tombstone carries.
    pub attribution: RunAttribution,
    /// The approver reference, or `unattended`.
    pub approver: String,
    /// Whether this is a preview.
    pub dry_run: bool,
    /// The delete-capable credential, when one is projected.
    pub archive_keys: Option<Keys>,
    /// The `evidenceWrite` credential. **`None` is a refusal on a real run**
    /// and merely a missing sink on a dry run, which writes no tombstone.
    pub evidence_keys: Option<Keys>,
}

impl Admitted {
    /// The archive `StorageUrl` the reaper's handle is built from.
    #[must_use]
    pub fn archive_url(&self) -> StorageUrl {
        self.location.archive_storage_url()
    }

    /// The evidence `StorageUrl` the record sink is built from.
    #[must_use]
    pub fn evidence_url(&self) -> StorageUrl {
        self.location.evidence_storage_url()
    }

    /// Whether the archive URL declares plaintext HTTP.
    #[must_use]
    pub fn allow_http(&self) -> bool {
        matches!(self.archive_url(), StorageUrl::S3 { allow_http, .. } if allow_http)
    }

    /// The run's own limits.
    #[must_use]
    pub fn limits(&self) -> Limits {
        Limits {
            dry_run: self.dry_run,
            max_objects: self.binding.max_objects_per_run,
        }
    }
}

/// The five variables a run cannot start without.
const REQUIRED: [&str; 5] = [
    "LOGWEIR_RETENTION_PLAN_SHA256",
    "LOGWEIR_RETENTION_POLICY_UID",
    "LOGWEIR_RETENTION_POLICY_GENERATION",
    "LOGWEIR_RETENTION_SCOPE_PREFIX",
    "LOGWEIR_RETENTION_RUN_ID",
];

/// Everything decided before a port is built. **Pure**: it dials nothing,
/// builds no handle and reads no process state except through `env` and
/// `read_plan`.
///
/// # Errors
///
/// [`Refusal`]. Every variant is exit 3, and **no port has been opened**, so a
/// caller that sees one has deleted nothing.
pub fn admit(
    argv: &[&str],
    env: &dyn Environment,
    read_plan: impl FnOnce(&str) -> std::io::Result<Vec<u8>>,
) -> Result<Admitted, Refusal> {
    let args = parse_args(argv)?;

    for name in REQUIRED {
        if env.get(name).is_none() {
            return Err(Refusal::BindingIncomplete(name));
        }
    }
    let approved = env.get(REQUIRED[0]).expect("checked above");
    let policy_uid = env.get(REQUIRED[1]).expect("checked above");
    let raw_generation = env.get(REQUIRED[2]).expect("checked above");
    let scope_prefix = env.get(REQUIRED[3]).expect("checked above");
    let run_id = env.get(REQUIRED[4]).expect("checked above");
    let policy_generation = raw_generation
        .parse::<i64>()
        .map_err(|_| Refusal::GenerationNotANumber(raw_generation.clone()))?;

    let binding = RunBinding {
        policy_uid: policy_uid.clone(),
        scope_prefix,
        max_deletions_per_run: env
            .get("LOGWEIR_RETENTION_MAX_DELETIONS")
            .and_then(|v| v.parse().ok())
            .unwrap_or(50),
        max_objects_per_run: env
            .get("LOGWEIR_RETENTION_MAX_OBJECTS")
            .and_then(|v| v.parse().ok())
            .unwrap_or(20_000),
    };

    let bytes = read_plan(&args.plan_path).map_err(|e| Refusal::PlanUnreadable {
        path: args.plan_path.clone(),
        error: e.to_string(),
    })?;
    let plan = logweir_reaper::parse_plan(&bytes, &approved)?;
    logweir_reaper::validate_plan(&plan, &binding)?;

    let location_json = env
        .get("LOGWEIR_RETENTION_LOCATION")
        .ok_or(Refusal::BindingIncomplete("LOGWEIR_RETENTION_LOCATION"))?;
    let location: DestinationLocation = serde_json::from_str(&location_json)
        .map_err(|e| Refusal::LocationUnreadable(e.to_string()))?;

    let evidence_keys = keys_from(env, EVIDENCE_PREFIX);
    // THE RECORD SINK'S CREDENTIAL IS A PRECONDITION OF DELETING, not a
    // consequence of it. A dry run deletes nothing and therefore needs no
    // sink; a real run without one is refused here, before a port exists.
    if !args.dry_run && evidence_keys.is_none() {
        return Err(Refusal::NoRecordCredential);
    }

    Ok(Admitted {
        attribution: RunAttribution {
            run_id: run_id.clone(),
            policy_uid,
            plan_sha256: approved,
        },
        plan,
        binding,
        policy_generation,
        location,
        approver: env
            .get("LOGWEIR_RETENTION_APPROVER")
            .unwrap_or_else(|| "unattended".to_string()),
        dry_run: args.dry_run,
        archive_keys: keys_from(env, ""),
        evidence_keys,
    })
}

// ---------------------------------------------------------------------------
// Execution
// ---------------------------------------------------------------------------

/// What one run produced, as the process reports it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Report {
    /// The key lines, in order, exactly as they go to stdout.
    pub lines: Vec<String>,
    /// The process exit code.
    pub exit_code: i32,
    /// The reaper's own outcome, for a caller that wants the structure.
    pub outcome: logweir_reaper::Outcome,
}

/// Run an admitted plan over injected ports.
///
/// The ports are the reaper's four seams, so a caller can drive the whole thing
/// — including the dry-run arm and the key lines — with no bucket.
pub fn execute<D: Deleter, S: Sleeper, T: TombstoneSink, L: Lister>(
    admitted: &Admitted,
    deleter: &D,
    sleeper: &S,
    tombstones: &T,
    lister: &L,
) -> Report {
    let mut lines = vec![format!(
        "retention-plan={} points={} objects={}",
        admitted.attribution.plan_sha256,
        admitted.plan.lines.len(),
        admitted.plan.object_keys().len()
    )];
    let outcome = logweir_reaper::execute(
        &admitted.plan,
        deleter,
        sleeper,
        tombstones,
        lister,
        &admitted.attribution,
        admitted.limits(),
    );
    for point in &outcome.points {
        // ON A DRY RUN THE COUNT IS `remaining_keys`, which the reaper filled
        // by ENUMERATING the set bound (review `d3w9` M1) — the number an
        // administrator needs, rather than the plan's own `1`.
        let objects = if admitted.dry_run {
            point.remaining_keys.len() as i64
        } else {
            point.objects_deleted
        };
        match point.code.as_deref() {
            Some(code) => lines.push(format!(
                "retention-point={} state={} objects={objects} code={code}",
                point.point_id, point.state
            )),
            None => lines.push(format!(
                "retention-point={} state={} objects={objects}",
                point.point_id, point.state
            )),
        }
    }
    let exit_code = if admitted.dry_run || outcome.complete() {
        EXIT_OK
    } else {
        EXIT_INCOMPLETE
    };
    Report {
        lines,
        exit_code,
        outcome,
    }
}

/// The `retention-result=` line, appended once the record has been dealt with.
#[must_use]
pub fn result_line(report: &Report, dry_run: bool) -> String {
    format!(
        "retention-result=deleted={} failed={} objects={}{}",
        report.outcome.deleted().len(),
        report.outcome.failed().len(),
        report.outcome.objects_deleted,
        if dry_run { " dryRun=true" } else { "" }
    )
}

/// The `retention-record=` line.
#[must_use]
pub fn record_line(key: &str, digest: &str) -> String {
    format!("retention-record={key} sha256={digest}")
}
