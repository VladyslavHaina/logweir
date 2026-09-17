//! `logweir check run` — D2 §4.2's ONE check runner (D-SEAMS **S1**).
//!
//! One subcommand serves discovery, operation readiness, restore preflight,
//! destination access and evidence fetch, because a second runner would be a
//! second argv surface, a second classification table and a second place the
//! contract could drift from what `weirkeeper::check::relay` verifies.
//!
//! # The invocation, verbatim
//!
//! ```text
//! argv: check run --plan /check/check-plan.json --check-contract-version 1
//! env:  LOGWEIR_CHECK_CONTRACT_VERSION=1
//!       LOGWEIR_CHECK_PLAN_SHA256=sha256:<64 hex>
//!       LOGWEIR_CHECK_SUBJECT_UID=<uid>
//!       RUST_LOG=warn        TMPDIR=/work
//! ```
//!
//! `weirkeeper::check::job::runner_argv` writes that argv and
//! `runner_job_spec` writes those variables; nothing here duplicates the
//! spelling, and `the_controller_and_the_runner_agree_on_the_invocation` in
//! `crates/logweir/tests/check_cli.rs` reads the controller's own functions to
//! prove it.
//!
//! # Startup order: NO NETWORK BEFORE STEP 5
//!
//! 1. parse argv;
//! 2. read the plan bytes **once**;
//! 3. check their SHA-256 against `LOGWEIR_CHECK_PLAN_SHA256`;
//! 4. parse strictly, require a supported kind, require the subject UID to
//!    equal `LOGWEIR_CHECK_SUBJECT_UID`;
//! 5. build clients.
//!
//! Steps 1–4 are [`load`], which takes no handle, opens nothing and returns
//! before [`execute`] exists. A failure in any of them prints
//! `refusal-reason=CheckContractMismatch` on stdout, prints **no frame**, and
//! exits 3. That is what makes a plan `ConfigMap` swapped under a running Job
//! unusable: the digest is pinned in the pod's environment, which only the
//! controller writes, so bytes that do not hash to it are refused before a
//! credential is read.
//!
//! # The exit contract
//!
//! | code | meaning |
//! |---|---|
//! | **0** | an end line was printed, **whatever the per-check states**. A `notReady` check is a RESULT, not a runner failure. |
//! | **1** | an operational failure before a result existed: no end line, so the controller's decoder refuses the relay and reports `ResultUnreadable`. |
//! | **3** | a contract refusal (steps 1–4): `refusal-reason=CheckContractMismatch`, no frames. |
//! | 2, 4 | never returned. |
//!
//! 2 and 4 are a CONTRACT, not an accident: Global Constraint 11 reserves 2
//! for "a drill result that is not a pass — a scorecard IS written and signed"
//! and 4 for "signing or lock-proof failed". A check signs nothing and writes
//! no artifact, so either code would make a check indistinguishable from a
//! drill result to `weirkeeper::conditions::reason_for_exit`.
//!
//! # What a check may do to the world
//!
//! * It **never invokes the engine binary** — nothing under `check/` names
//!   `run_engine` or an engine subcommand, so `scripts/check-no-oso.sh` passes
//!   unchanged.
//! * It **never writes**, except the optional create-only readiness marker
//!   `logweir/readiness/<destinationUid>.json`
//!   ([`store::marker_key`]), through `Store::put_create_only`.
//! * It **never creates, alters or deletes a topic**: the collision probe is
//!   targeted metadata and a `CreateTopics` with `validate_only = true`
//!   (D2 §6.7, G12).
//! * It **never prints a credential**: every message and remedy is built by
//!   this crate and passes `check_contract::redact`, every fact passes it too,
//!   and no backend or broker error string is ever interpolated into a frame.
//! * Every network call is time-bounded, by [`ProbeTimeouts`] on the broker
//!   side and by `StoreOptions::with_request_timeout` on the object-store
//!   side, both derived from the plan's own `timeoutSeconds`.

pub mod archive;
pub mod catalogue;
pub mod frames;
pub mod kafka;
pub mod kinds;
pub mod store;

use std::io::Write;
use std::path::PathBuf;
use std::time::{Duration, Instant};

use logweir_core::check_contract::{
    topic_tsv_sha256, CheckCode, CheckPlan, CheckPlanError, CheckResult, Stream, TopicEntry,
    TruncationReason, CHECK_CONTRACT_VERSION, DEFAULT_RELAY_BUDGET_BYTES,
};

use crate::exit::ExitCode;
use frames::{EmitError, FrameWriter};

/// `LOGWEIR_CHECK_CONTRACT_VERSION` — D2 §4.2's env.
pub const CONTRACT_VERSION_ENV: &str = "LOGWEIR_CHECK_CONTRACT_VERSION";
/// `LOGWEIR_CHECK_PLAN_SHA256`.
pub const PLAN_SHA256_ENV: &str = "LOGWEIR_CHECK_PLAN_SHA256";
/// `LOGWEIR_CHECK_SUBJECT_UID`.
pub const SUBJECT_UID_ENV: &str = "LOGWEIR_CHECK_SUBJECT_UID";

/// The key a contract refusal is reported under, on stdout.
///
/// Byte-identical to `weirkeeper::controllers::backup::REFUSAL_REASON_PREFIX`,
/// which `weirkeeper::check::relay::refusal_reason` reads BY KEY NAME from a
/// bounded tail. It is spelled here rather than imported because the runner
/// does not depend on the controller crate, and
/// `the_refusal_key_is_the_controllers_key` asserts the two strings are equal.
pub const REFUSAL_REASON_PREFIX: &str = "refusal-reason=";

/// `logweir check run`'s arguments.
#[derive(Debug, Clone)]
pub struct CheckRunArgs {
    /// `--plan`, the mounted check-plan document.
    pub plan: PathBuf,
    /// `--check-contract-version`, which must be
    /// [`CHECK_CONTRACT_VERSION`].
    pub check_contract_version: u32,
}

/// A refused invocation — steps 1–4 of the startup order.
///
/// The CODE is always [`CheckCode::CheckContractMismatch`]: the runner opened
/// nothing and printed no frame, so there is nothing else to report. The
/// `detail` is for the operator's stderr and never reaches stdout.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Refusal {
    pub detail: String,
}

impl Refusal {
    fn new(detail: impl Into<String>) -> Self {
        Self {
            detail: detail.into(),
        }
    }

    /// Always [`CheckCode::CheckContractMismatch`].
    #[must_use]
    pub fn code(&self) -> CheckCode {
        CheckCode::CheckContractMismatch
    }
}

impl From<CheckPlanError> for Refusal {
    fn from(e: CheckPlanError) -> Self {
        Self::new(e.to_string())
    }
}

/// A verified plan, plus the two pinned values every frame carries.
#[derive(Debug, Clone)]
pub struct Loaded {
    pub plan: CheckPlan,
    pub plan_sha256: String,
    pub subject_uid: String,
}

impl Loaded {
    /// The relay budget for topic lines. Only `topicInventory` states one;
    /// every other kind writes no topic line, so the contract default is used
    /// and is never reached.
    #[must_use]
    pub fn relay_budget(&self) -> u64 {
        match &self.plan.request {
            logweir_core::check_contract::CheckRequest::TopicInventory(r) => r.relay_budget_bytes,
            _ => DEFAULT_RELAY_BUDGET_BYTES as u64,
        }
    }
}

/// **Steps 1–4, and nothing else.** No socket, no handle, no file except the
/// plan.
///
/// # Errors
/// [`Refusal`] — exit 3, no frames.
pub fn load(args: &CheckRunArgs, env: &dyn Fn(&str) -> Option<String>) -> Result<Loaded, Refusal> {
    // 1 — argv and env agree with THIS build's contract version. Both are
    // checked: the argv is what an operator or a controller typed, the
    // environment is what the Job template pinned, and a disagreement between
    // them is a rollout that is half-upgraded.
    if args.check_contract_version != CHECK_CONTRACT_VERSION {
        return Err(Refusal::new(format!(
            "--check-contract-version {} is not {CHECK_CONTRACT_VERSION}, which is the only \
             check contract this runner implements",
            args.check_contract_version
        )));
    }
    match env(CONTRACT_VERSION_ENV) {
        Some(v) if v.trim() == CHECK_CONTRACT_VERSION.to_string() => {}
        Some(v) => {
            return Err(Refusal::new(format!(
                "${CONTRACT_VERSION_ENV} is `{v}`, not {CHECK_CONTRACT_VERSION}"
            )))
        }
        None => {
            return Err(Refusal::new(format!(
                "${CONTRACT_VERSION_ENV} is not set; a check Job always sets it"
            )))
        }
    }
    let plan_sha256 = env(PLAN_SHA256_ENV).unwrap_or_default();
    if !logweir_core::check_contract::is_sha256_prefixed(&plan_sha256) {
        return Err(Refusal::new(format!(
            "${PLAN_SHA256_ENV} is not `sha256:<64 lowercase hex>`; without a pinned digest the \
             plan's bytes are whatever the mount happens to hold"
        )));
    }
    let subject_uid = env(SUBJECT_UID_ENV).unwrap_or_default();
    if subject_uid.is_empty() {
        return Err(Refusal::new(format!(
            "${SUBJECT_UID_ENV} is not set; without it a plan cannot be bound to the object it \
             is about"
        )));
    }

    // 2 — the bytes, ONCE. Read before anything is parsed and never re-read,
    // so the digest that is checked is the digest of the bytes that are used.
    let bytes = std::fs::read(&args.plan).map_err(|e| {
        Refusal::new(format!(
            "the check plan at `{}` could not be read ({})",
            args.plan.display(),
            e.kind()
        ))
    })?;

    // 3 + 4 — one call, so no caller can do them out of order.
    let plan = CheckPlan::parse_and_verify(&bytes, &plan_sha256, &subject_uid)?;
    runner_bounds(&plan)?;
    Ok(Loaded {
        plan,
        plan_sha256,
        subject_uid,
    })
}

/// The relay streams an `evidenceFetch` object may name.
///
/// `result` and `details` are the RUNNER's own streams: it writes the result
/// document to the first on every kind, and the restore preflight writes its
/// bounded detail lines to the second.
pub const EVIDENCE_STREAMS: [Stream; 2] = [Stream::EvidencePayload, Stream::EvidenceSidecar];

/// Two bounds the pure contract does not state and this runner needs — still
/// step 4, still before any client.
///
/// **An `evidenceFetch` object may name only an EVIDENCE stream, and no two
/// objects may name the same one.** A stream is relayed as one ordered run of
/// part frames whose count and digest the end frame declares once. Two objects
/// on one stream, or an object on a stream the runner also writes, break in
/// exactly the same way: the bytes are printed to that stream, the runner's own
/// document is printed to it again, the end frame declares one of the two, and
/// the controller's decoder answers `DuplicatePart` — which
/// `weirkeeper::check::relay` reports as `ResultUnreadable` **with no way to
/// say why**.
///
/// Both are REFUSALS rather than silent skips because the plan is written by
/// the controller: either shape is a controller bug, and a runner that quietly
/// relayed one object of a two-object fetch would report a receipt as absent
/// when it was merely not asked for properly. A NAMED refusal at exit 3 sends
/// the operator to the plan; `ResultUnreadable` sends them to the pod log.
///
/// # Errors
/// [`Refusal`] — exit 3, no frames.
pub fn runner_bounds(plan: &CheckPlan) -> Result<(), Refusal> {
    if let logweir_core::check_contract::CheckRequest::EvidenceFetch(r) = &plan.request {
        let mut seen: std::collections::BTreeSet<Stream> = std::collections::BTreeSet::new();
        for (i, o) in r.objects.iter().enumerate() {
            if !EVIDENCE_STREAMS.contains(&o.stream) {
                return Err(Refusal::new(format!(
                    "request.evidenceFetch.objects[{i}] names the relay stream `{}`, which the \
                     runner writes itself; an evidence object may name only `{}` or `{}`",
                    o.stream.as_str(),
                    Stream::EvidencePayload.as_str(),
                    Stream::EvidenceSidecar.as_str()
                )));
            }
            if !seen.insert(o.stream) {
                return Err(Refusal::new(format!(
                    "two evidenceFetch objects name the relay stream `{}`; one stream carries \
                     one ordered run of part frames and the end frame declares its digest once",
                    o.stream.as_str()
                )));
            }
        }
    }
    Ok(())
}

/// Print the refusal line — stdout, and the only line this process prints
/// there.
///
/// # Errors
/// The writer's.
pub fn print_refusal_to<W: Write>(w: &mut W) -> std::io::Result<()> {
    writeln!(
        w,
        "{REFUSAL_REASON_PREFIX}{}",
        CheckCode::CheckContractMismatch
    )
}

/// The name of the one redaction rule that is applied per PATH SEGMENT rather
/// than over the whole string.
///
/// Read out of `check_contract::redaction_rules()` BY NAME and asserted to
/// match exactly one rule, so a rename in the pure layer is a failing test
/// rather than a silently disabled clause.
pub const LONG_RUN_RULE: &str = "long-base64-or-hex-run";

/// [`logweir_core::check_contract::redact`] for a value that is a KEY PATH.
///
/// # Why a key path needs a different application of the same rules
///
/// `redact`'s last rule removes base64-or-hex runs of 40 characters or more,
/// and `/`, `-`, `=` and the digits are all in the base64 alphabet — so an
/// ordinary archive key,
/// `kafka-backups/20260915T030000Z/topics/orders/partition=0/segment-1.bin`,
/// is ONE 70-character run and comes back as `[redacted]`. Applying it whole
/// therefore protects nothing and deletes the only fact the line carried: a
/// `<job>-details` `ConfigMap` reporting "3 missing segments: [redacted],
/// [redacted], [redacted]" is one nobody can act on. That is reviewer finding
/// **F7** arriving inside the fix for **F2**.
///
/// # And why it is NOT simply "split and redact"
///
/// The first version split on `/` and ran the whole rule set over each piece,
/// which broke the URL-userinfo rule: `https://root:hunter2@host` splits into
/// `https:`, `` and `root:hunter2@host`, and that rule is anchored on `://`,
/// so the password survived. A test caught it; the shape of the mistake —
/// weakening a rule while claiming not to — is the one this finding is about.
///
/// So the two halves are applied differently, deliberately:
///
/// * **every SHAPE rule over the WHOLE string**, exactly as `redact` applies
///   it: PEM blocks, S3 XML bodies, URL userinfo, `secret…=value` forms and
///   `AKIA`/`ASIA` key ids. None of them is weakened at all.
/// * **the long-run rule per segment**, which is the only one that cannot tell
///   an archive key from a secret.
///
/// **What that gives up, stated rather than implied:** a base64 secret that
/// happens to contain `/` — base64's 64th character — is split into pieces
/// shorter than 40 and the long-run rule no longer sees it. The values this is
/// applied to are archive object keys and Kafka topic names: a topic name
/// cannot contain `/` at all, and an object key is adopter-chosen structure,
/// not a place a credential is carried. Everything the runner writes that
/// COULD carry one — every message, remedy and fact — still goes through the
/// whole-string `redact`.
///
/// # It does NOT cap, and that is the difference from [`redact`]
///
/// [`redact`] ends with a 512-CHARACTER truncation, which is right for a
/// `message` (a sentence, bounded by `MESSAGE_MAX_CHARS`) and wrong for a
/// value. `details_stream` applies this per LINE, and a line is JSON: cutting
/// one at 512 characters emits an unparseable line into the `details` stream,
/// which D2 §6.7 documents as JSON lines and the controller stores as such.
/// Reviewer probe **R7** found it with an 888-character S3 key of short
/// `/`-separated segments — no run reaches 40 characters, so nothing is
/// redacted, and the line came back cut mid-key.
///
/// So the two bounds are applied where they belong and nowhere else:
///
/// * a MESSAGE is capped by `CheckOutcome::with_message` / `with_remedy`,
///   which already call [`redact`];
/// * a detail SAMPLE is capped as a VALUE by
///   [`crate::check::kinds::readiness::detail`], which truncates the string
///   and lets `serde_json` re-serialise it — so the JSON stays JSON;
/// * the `details` STREAM is capped in bytes by
///   `kinds::restore::details_stream`, which drops WHOLE lines and says it
///   did.
///
/// [`redact`]: logweir_core::check_contract::redact
///
/// # Panics
/// Never in practice: only if [`LONG_RUN_RULE`] names no rule, which
/// `the_long_run_rule_is_the_only_one_applied_per_segment` fails on first.
#[must_use]
pub fn redact_path(value: &str) -> String {
    use logweir_core::check_contract::{apply_rules, redaction_rules};
    let all = redaction_rules();
    let shape: Vec<_> = all
        .iter()
        .filter(|r| r.name != LONG_RUN_RULE)
        .copied()
        .collect();
    let long: Vec<_> = all
        .iter()
        .filter(|r| r.name == LONG_RUN_RULE)
        .copied()
        .collect();
    assert_eq!(
        long.len(),
        1,
        "`{LONG_RUN_RULE}` names {} redaction rules, not one; the per-segment clause is \
         applying the wrong thing",
        long.len()
    );
    let whole = apply_rules(value, &shape);
    whole
        .split('/')
        .map(|segment| apply_rules(segment, &long))
        .collect::<Vec<_>>()
        .join("/")
}

/// What one plan kind produced.
#[derive(Debug, Clone)]
pub struct Emission {
    /// The inventory's entries. `emit_topics` decides whether they are
    /// written at all, which is how "no inventory ran" stays distinct from
    /// "the cluster showed no topic" in the end frame.
    pub topics: Vec<TopicEntry>,
    pub emit_topics: bool,
    pub result: CheckResult,
    /// Streams besides `result`, in the order they are written.
    pub extra: Vec<(Stream, Vec<u8>)>,
}

impl Emission {
    /// A result-only emission.
    #[must_use]
    pub fn of(result: CheckResult) -> Self {
        Self {
            topics: Vec::new(),
            emit_topics: false,
            result,
            extra: Vec::new(),
        }
    }
}

/// Write one emission's frames and its end line.
///
/// The end frame is built from what the writer RECORDED, and the result
/// document is corrected when the relay bound truncated the topic lines — a
/// document declaring more entries than were printed would make the whole
/// relay `ResultUnreadable`.
///
/// # Errors
/// [`EmitError`] — exit 1, because no end line was printed.
pub fn emit<W: Write>(
    writer: &mut FrameWriter<W>,
    mut emission: Emission,
    plan_sha256: &str,
    subject_uid: &str,
) -> Result<(), EmitError> {
    if emission.emit_topics {
        let written = writer.write_topics(&emission.topics)?;
        if written < emission.topics.len() {
            // THE SECOND BOUND BIT. Report it honestly rather than declaring a
            // digest over lines that were not printed.
            let kept = &emission.topics[..written];
            if let Some(inv) = emission.result.inventory.as_mut() {
                inv.truncated = true;
                inv.truncation_reason = Some(TruncationReason::RelayLimit);
                inv.counts.returned = written as u32;
                inv.topics_sha256 = topic_tsv_sha256(kept);
            }
        }
    }
    for (stream, bytes) in &emission.extra {
        writer.write_stream(*stream, bytes)?;
    }
    // `validate` before the document is written: D2 §6.4 caps a result at 64
    // entries, and a declared bound is not a bound.
    emission.result.validate().map_err(|e| {
        EmitError::Frame(logweir_core::check_contract::FrameError::ResultDocument(e))
    })?;
    let doc = emission.result.to_canonical_json().map_err(|_| {
        EmitError::Frame(logweir_core::check_contract::FrameError::Malformed { kind: "result" })
    })?;
    writer.write_stream(Stream::Result, &doc)?;
    writer.finish(plan_sha256, subject_uid)?;
    Ok(())
}

/// A check's wall-clock budget: the plan's `timeoutSeconds`, spent from the
/// moment the plan verified.
///
/// The Job's `activeDeadlineSeconds` is `timeoutSeconds + 90` (D2 §4.3), and
/// that margin is image pull and scheduling — not slack for the runner. A
/// check that overran would be killed with no frames at all, which is strictly
/// worse than one that reports `Timeout` for the work it did not reach.
#[derive(Debug, Clone, Copy)]
pub struct Deadline {
    started: Instant,
    budget: Duration,
}

impl Deadline {
    #[must_use]
    pub fn new(timeout_seconds: u32) -> Self {
        Self {
            started: Instant::now(),
            budget: Duration::from_secs(u64::from(timeout_seconds)),
        }
    }

    /// How much of the budget is left.
    #[must_use]
    pub fn remaining(&self) -> Duration {
        self.budget.saturating_sub(self.started.elapsed())
    }

    /// Whether there is enough budget left to be worth starting another
    /// network call.
    ///
    /// The floor is [`MIN_CALL_BUDGET`] rather than zero: a call started with
    /// 200 ms left cannot succeed and its failure would be reported as a
    /// broker or bucket problem rather than as the clock.
    #[must_use]
    pub fn has_room(&self) -> bool {
        self.remaining() >= MIN_CALL_BUDGET
    }

    /// The budget for one of `parts` remaining pieces of work, never less than
    /// [`MIN_CALL_BUDGET`].
    #[must_use]
    pub fn slice(&self, parts: u32) -> Duration {
        let parts = u64::from(parts.max(1));
        (self.remaining() / parts as u32).max(MIN_CALL_BUDGET)
    }
}

/// The least budget a check will start a network call with.
pub const MIN_CALL_BUDGET: Duration = Duration::from_secs(2);

/// Install this subcommand's diagnostics — **JSON, on stderr, at `warn`**
/// (D2 §4.2).
///
/// WHY STDERR. A check's stdout is the machine contract: topic frames, part
/// frames and one end line. A log line there would be a frame the decoder
/// ignores but the relay budget still pays for.
///
/// WHY `warn` BY DEFAULT. `RUST_LOG=warn` is what the Job template sets; the
/// default here matches it so a check run outside Kubernetes behaves the same
/// way. A BLANK value is treated as unset — an empty `value:` on a Kubernetes
/// env entry is a real shape and `EnvFilter::new("")` parses to the empty
/// directive set without erroring.
///
/// `try_init`, not `init`: a global subscriber an embedder already installed
/// is not a reason to abort a check.
pub fn install_diagnostics() {
    let filter = match std::env::var("RUST_LOG") {
        Ok(v) if !v.trim().is_empty() => tracing_subscriber::EnvFilter::new(v),
        _ => tracing_subscriber::EnvFilter::new("warn"),
    };
    let _ = tracing_subscriber::fmt()
        .json()
        .with_writer(std::io::stderr)
        .with_env_filter(filter)
        .try_init();
}

/// Run one verified plan and emit its frames.
///
/// **Returns [`ExitCode::Ok`] whenever an end line was printed**, whatever the
/// per-check states, and [`ExitCode::Operational`] when emitting failed.
pub fn execute<W: Write>(loaded: &Loaded, out: W) -> ExitCode {
    execute_with(loaded, out, &kinds::Live)
}

/// [`execute`] over a supplied wiring — the seam a whole invocation is driven
/// through with no socket.
///
/// It is `pub` rather than `#[cfg(test)]` because the assertions that matter
/// live in `crates/logweir/tests/check_cli.rs`, a separate crate: the frames a
/// kind produces are only worth asserting as BYTES that
/// `check_contract::frames::Decoder` — the controller's own decoder — accepts,
/// and that needs the whole path from plan to end line.
pub fn execute_with<W: Write>(loaded: &Loaded, out: W, wiring: &dyn kinds::Wiring) -> ExitCode {
    let mut writer = FrameWriter::new(out, loaded.relay_budget());
    let deadline = Deadline::new(loaded.plan.timeout_seconds);
    let emission = kinds::run_kind_with(loaded, deadline, wiring);
    match emit(
        &mut writer,
        emission,
        &loaded.plan_sha256,
        &loaded.subject_uid,
    ) {
        Ok(()) => ExitCode::Ok,
        Err(e) => {
            // The message names the KIND of failure and never a frame's
            // content: a relay that could not be written is not something to
            // paste into a log.
            tracing::warn!(
                code = %e.code(),
                "the check could not emit its frames: {e}"
            );
            ExitCode::Operational
        }
    }
}

/// `logweir check run`.
#[must_use]
pub fn run(args: &CheckRunArgs) -> ExitCode {
    install_diagnostics();
    let loaded = match load(args, &|k| std::env::var(k).ok()) {
        Ok(l) => l,
        Err(refusal) => {
            tracing::warn!(
                code = %refusal.code(),
                "the check plan was refused before any client was built: {}",
                logweir_core::check_contract::redact(&refusal.detail)
            );
            let _ = print_refusal_to(&mut std::io::stdout().lock());
            return ExitCode::GuardRefused;
        }
    };
    execute(&loaded, std::io::stdout().lock())
}
