//! Phase 8 — score the drill, sign it, and upload it create-only.
//!
//! This is where everything the earlier phases established becomes a signed
//! claim. Two orderings in `run` are the mechanism rather than a convention,
//! and both are pinned by named tests in `crates/logweir/tests/score.rs`:
//!
//! - `validate_invariants` runs before the bytes are produced, so a
//!   self-contradicting document is never signed.
//! - Signing runs before every put, so a signing failure leaves the bucket
//!   untouched — and is reported as `DrillError::SigningOrLock`, which is
//!   exit 4, never exit 1. "The drill ran but the result is unattested" is its
//!   own outcome (Global Constraint 11).
//!
//! Global Constraint 1: `logweir-core` reads no clock and touches no I/O. The
//! timestamps in `Timeline` are taken by the phases that own them and passed
//! in here; the upload lives in this crate, not in core.
use crate::drill::DrillError;
use crate::signer::ValidatedSigner;
use base64::Engine as _;
use chrono::{DateTime, Utc};
use logweir_core::outcome::{IntegrityLevel, IntegrityResult, Outcome};
use logweir_core::scorecard::{EngineSubreport, Integrity, Measured, Objectives, Scorecard};
use logweir_core::spec::ObjectivesSpec;
use logweir_engine_oso::storage::{PutOutcome, Store};
use std::path::Path;

/// The spec §6.1 sentence that must travel with any retained engine report.
/// The engine's own `checksums_valid` is a hardcoded constant and its restore
/// timings are all null, so the sub-report corroborates nothing Logweir claims
/// — an auditor who is handed the two documents together has to be told that
/// in the document itself, not in a footnote somewhere else.
pub const CAVEAT_ENGINE_SUBREPORT: &str =
    "The engine's own integrity.checksums_valid is a hardcoded constant true and its restore \
     start_time/end_time/duration_seconds are all null; this sub-report corroborates nothing \
     Logweir claims.";

/// The five wall-clock instants plus the phase-5 duration every RTO number is
/// derived from. Six fields, in this order:
/// `phase5_duration_ms` is not optional, because without it
/// `rto_excluding_preflight_seconds` — the only number compared against the
/// RTO objective — cannot be computed.
pub struct Timeline {
    pub requested_at: DateTime<Utc>,
    pub approval_validated_at: DateTime<Utc>,
    pub restore_started_at: DateTime<Utc>,
    pub restore_finished_at: DateTime<Utc>,
    pub phase5_duration_ms: u64,
    pub verified_at: DateTime<Utc>,
}

/// THE HEADLINE FIELDS, defined once and normatively. Every formula below is
/// computable from scorecard fields alone, so an auditor can re-derive them
/// from the document (spec §9.3 phase 8).
pub fn compute_measured(
    t: &Timeline,
    requested_point_in_time_ms: i64,
    newest_restored_record_ts_ms: i64,
) -> Measured {
    let secs = |a: DateTime<Utc>, b: DateTime<Utc>| (b - a).num_seconds().max(0) as u64;
    let rto = secs(t.approval_validated_at, t.verified_at);
    Measured {
        rto_seconds: Some(rto),
        rto_requested_to_verified_seconds: Some(secs(t.requested_at, t.verified_at)),
        rto_restore_only_seconds: Some(secs(t.restore_started_at, t.restore_finished_at)),
        // Phase 5 sets header_preflight: full, which makes scan_required
        // unconditionally true and opens and decodes every segment in the
        // window to inspect per-record headers, with one HEAD per segment on
        // top. No incident responder performs that sweep, so scoring it
        // against an RTO objective compares unlike things.
        rto_excluding_preflight_seconds: Some(rto.saturating_sub(t.phase5_duration_ms / 1000)),
        // Archive coverage gap at the requested recovery point, not
        // source-relative data loss: `requested - newest`, clamped at zero. A
        // negative gap is meaningless, and `rpo_seconds: -90` in a signed
        // document would read as "no data loss". The field stays `Option<i64>`
        // (frozen at 1.0.0); the clamp makes a negative value unconstructible.
        rpo_seconds: Some(
            ((requested_point_in_time_ms - newest_restored_record_ts_ms) / 1000).max(0),
        ),
        // v0.1 never contacts the source, so this is null with its reason. It
        // becomes a number only under --from-cluster (deferred). This pair is
        // exactly the `captured_by_logweir == false` branch
        // `Scorecard::validate_invariants` requires.
        rpo_source_relative_seconds: None,
        rpo_source_relative_unmeasured_reason: Some("source cluster never contacted".into()),
    }
}

/// Returns `(outcome, objectives-as-REQUESTED)`.
///
/// `integ` is phase 7's verdict, consumed as given — ALL of it, not only
/// `IntegrityResult`. Phase 8 never re-derives or second-guesses any part of
/// it, and it returns no pass rate for a caller to write back, because
/// `integrity.pass_rate_measured` is decided in exactly one place
/// (`phase7_verify::roll_up`) and phase 8 has no business writing to it.
///
/// There is no rate for a caller to republish: `roll_up` withholds the ratio
/// when part of the sample was never reconciled, and a recomputation here
/// would drop that condition. Pinned across the seam by
/// `crates/logweir/tests/verify_phase.rs`'s
/// `phase_8_never_republishes_a_pass_rate_phase_7_withheld` and
/// `the_signed_document_carries_no_pass_rate_beside_a_partial_verdict`.
pub fn decide(m: &Measured, spec: &ObjectivesSpec, integ: &Integrity) -> (Outcome, Objectives) {
    // Phase 7's number, verbatim. Null when the level is consume-only or
    // not-attempted, when the denominator is zero, OR when part of the sample
    // was never reconciled — three cases, all of them `roll_up`'s to decide
    // and none of them re-testable from the two counters alone.
    let pass_rate = integ.pass_rate_measured;

    let rto_ok = match (spec.rto_seconds, m.rto_excluding_preflight_seconds) {
        (Some(want), Some(got)) => Some(got <= want),
        (Some(_), None) => Some(false),
        _ => None,
    };
    let rpo_ok = match (spec.rpo_seconds, m.rpo_seconds) {
        (Some(want), Some(got)) => Some(got <= want),
        (Some(_), None) => Some(false),
        _ => None,
    };
    let rate_ok = match (spec.pass_rate, pass_rate) {
        // `+ f64::EPSILON` is a deliberate one-ULP tolerance, not slop: a
        // measured rate that is a single floating-point step below the
        // requested one (75/75 against a `pass_rate` an adopter wrote as
        // `0.1 + 0.2`) is a rounding artefact of the comparison, not a missed
        // objective.
        (Some(want), Some(got)) => Some(got + f64::EPSILON >= want),
        // Unreachable in effect: this fires exactly when `met` short-circuits
        // to `None` below, so its value never influences the result.
        (Some(_), None) => None,
        _ => None,
    };

    let met = if spec.pass_rate.is_some() && pass_rate.is_none() {
        None // unmeasurable, so the aggregate verdict is unmeasurable
    } else {
        // `all()` over an empty slice is vacuously true, and all three spec
        // fields are optional: no objective requested is not "every objective
        // met", it is `null`, as in `phase7_verify::roll_up` and `metrics.rs`.
        let answered: Vec<bool> = [rto_ok, rpo_ok, rate_ok].into_iter().flatten().collect();
        (!answered.is_empty()).then(|| answered.into_iter().all(|b| b))
    };

    let outcome = if integ.result != IntegrityResult::Pass {
        // `partial` and `fail` share an outcome because neither is a pass; the
        // distinction lives in integrity.result (spec §6 C5).
        Outcome::FailIntegrity
    } else if met == Some(false) {
        Outcome::FailObjective
    } else {
        Outcome::Pass
    };

    // `objectives` is the REQUEST, in all three fields. Returning the measured
    // ratio here would make two of three fields the ask and the third the
    // result, and would silently discard the adopter's `pass_rate: 1.0` — an
    // auditor could not tell from the document what rate was required. The
    // measurement is published separately, in `integrity.pass_rate_measured`,
    // by phase 7 and by nobody else.
    (
        outcome,
        Objectives {
            rto_seconds: spec.rto_seconds,
            rpo_seconds: spec.rpo_seconds,
            pass_rate: spec.pass_rate,
            met,
        },
    )
}

/// The engine-matrix row this RUN establishes, decided once, immediately
/// before the document is validated and signed.
///
/// `docs/support-matrix.md` defines `pass` as "the full drill ran and
/// passed", so a signed `pass` never sits inside a failed drill.
///
/// `observed` is phase 5's per-run lever readback and is never RAISED here: a
/// run that did not see the engine honour the lever keeps
/// `fail(lever-not-honoured)` and its own meaning. Everything else is answered
/// from what the drill actually did:
///
/// | outcome | integrity level | verdict |
/// |---|---|---|
/// | `pass` | `byte-fingerprint` | `pass` |
/// | `pass` | anything else | `pass-degraded` |
/// | anything else | — | `fail`, with a reason naming the outcome |
pub fn matrix_verdict_for(
    outcome: Outcome,
    level: IntegrityLevel,
    observed: logweir_core::outcome::MatrixVerdict,
) -> (logweir_core::outcome::MatrixVerdict, Option<String>) {
    use logweir_core::outcome::MatrixVerdict as V;
    if observed != V::Pass {
        // Phase 5 already lowered it on evidence. Never raise, and never
        // overwrite a lever finding with a weaker drill-level one.
        return (observed, None);
    }
    match (outcome, level) {
        (Outcome::Pass, IntegrityLevel::ByteFingerprint) => (V::Pass, None),
        (Outcome::Pass, _) => (V::PassDegraded, None),
        // `validate_invariants` REQUIRES a reason for `fail`, which is why
        // this arm always builds one rather than leaving it null.
        (other, _) => (
            V::Fail,
            Some(format!(
                "the drill ran and did not pass: outcome {}",
                crate::metrics::outcome_str(&other)
            )),
        ),
    }
}

/// The signed result. `bytes` is what was signed AND what was stored: the
/// orchestrator writes THESE bytes to `--out` and never re-serialises the
/// scorecard after signing.
pub struct Signed {
    pub scorecard: Scorecard,
    pub bytes: Vec<u8>,
    pub sidecar: logweir_evidence::Sidecar,
    /// The object key `bytes` was ACTUALLY put at, carried out rather than
    /// left to be reconstructed by whoever needs to name it.
    ///
    /// `run` builds this string once, puts at it, reads the object lock back
    /// from it, and hands it out here, so the three can never disagree; a
    /// reconstructed key may name a location that does not hold what was
    /// written.
    pub key: String,
    /// The DSSE sidecar's own key — **interface I8's second stdout line**.
    ///
    /// Built and put in the same statement pair as `key`, for the same reason
    /// `key` exists at all: `exiting` prints these strings to a controller
    /// that will go and fetch the objects, and a printed key that was
    /// reconstructed from a run id is a key nothing guarantees was written.
    pub sidecar_key: String,
    /// The offset report's key — **interface I8's third stdout line** — and
    /// `None` when this run had no report to upload.
    ///
    /// `None` is a real state, not a defensive one: the engine writes its
    /// offset report only from the `Ok` arm of a completed restore, and a
    /// failed write there is a `warn!` rather than an error
    /// [U:crates/kafka-backup-core/src/restore/engine.rs:417-427]. So the
    /// paths that sign a document WITHOUT having run a restore — phase 5's
    /// `Verdict::Block`, phase 6's `RestoreNoOp` — have no report by
    /// construction, and both exit 2 rather than 0. Recording a key for an
    /// object that does not exist would be the worst available answer;
    /// printing no third line, and logging why, is the honest one.
    pub offset_report_key: Option<String>,
}

/// Hand-written rather than derived: `bytes` is the whole document and
/// `Debug`-printing it into a test failure message would be unreadable. The
/// impl exists at all because tests call `.unwrap_err()` on
/// `Result<Signed, DrillError>`, which requires the success type to be `Debug`.
impl std::fmt::Debug for Signed {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Signed")
            .field("run_id", &self.scorecard.run_id)
            .finish()
    }
}

/// Every failure inside `run` funnels through here, so exit 4 is decided in one
/// place rather than at each `?`. Never carries key material: the only inputs
/// are error `Display`s from key loading, signing, serialisation and the store.
fn sig<E: std::fmt::Display>(e: E) -> DrillError {
    DrillError::SigningOrLock(e.to_string())
}

/// Phase 8's own warnings have no home in the signed document:
/// `drill::sign_and_publish` freezes a clone before `record(sc, 8, …)` pushes
/// phase 8's record, so the signed document cannot contain the record of its
/// own signing. The note goes to the structured log (`logweir::score`),
/// saying it is not in the signed bytes. The phase-record push serves a
/// caller whose document already carries a phase-8 record
/// (`crates/logweir/tests/score.rs`).
fn note(sc: &mut Scorecard, msg: &str) {
    tracing::warn!(
        target: "logweir::score",
        run_id = %sc.run_id,
        note = msg,
        "phase 8 warning; NOT carried in the signed document, whose phase list ends \
         before phase 8's own record"
    );
    if let Some(p) = sc.phases.iter_mut().find(|p| p.phase == 8) {
        p.notes.push(msg.to_string());
    }
}

/// The engine's offset-mapping report, read off the pod-local path the plan
/// named, together with the digest and key the signed scorecard will carry.
///
/// `None` when there is no readable file there — see
/// `Signed::offset_report_key` for why that is a legitimate state and not an
/// error. The reason is LOGGED, because a silently absent piece of evidence is
/// how a gap becomes permanent.
fn read_offset_report(run_id: &str, path: Option<&Path>) -> Option<(String, String, Vec<u8>)> {
    let path = path?;
    match std::fs::read(path) {
        Ok(bytes) => {
            let digest = logweir_core::ids::sha256_prefixed(&bytes);
            Some((
                format!("logweir/drills/{run_id}.offsets.json"),
                digest,
                bytes,
            ))
        }
        Err(e) => {
            tracing::warn!(
                run_id = %run_id,
                path = %path.display(),
                error = %e,
                "no offset-mapping report at the path the plan named, so the scorecard records \
                 none and nothing is uploaded; the engine writes it only from a completed \
                 restore and its own write failure is a warning"
            );
            None
        }
    }
}

/// `offset_report` is the pod-local path the plan named
/// (`RestorePlan::offset_report`), or `None` on a path that ran no restore.
pub fn run(
    sc: &Scorecard,
    signing_key: &Path,
    store: &Store,
    offset_report: Option<&Path>,
) -> Result<Signed, DrillError> {
    let signer = ValidatedSigner::load(
        signing_key,
        logweir_evidence::PAYLOAD_TYPE_SCORECARD,
        b"logweir restore signing readiness probe v1",
        "No evidence was uploaded",
    )
    .map_err(sig)?;
    run_with_signer(sc, &signer, store, offset_report)
}

pub(crate) fn run_with_signer(
    sc: &Scorecard,
    signer: &ValidatedSigner,
    store: &Store,
    offset_report: Option<&Path>,
) -> Result<Signed, DrillError> {
    let mut sc = sc.clone();
    let run_id = sc.run_id.clone();

    // 1. Retrieve the engine sub-report. `Store::list_manifest_keys` is
    //    manifest-only, so this uses the raw list over the per-run prefix
    //    Logweir itself set in validation.yaml — the run's report is the only
    //    object under it — and carries its bytes VERBATIM. Never
    //    `serde_json::from_slice` then re-emit: OSO's envelope covers the
    //    exact stored report bytes with no canonicalization at verification
    //    time, so a parsed value re-emitted through `to_deterministic_json`
    //    would no longer verify.
    // The evidence store is listed here while `render_validation` writes the
    // same prefix into an archive-storage document: inert while the engine's
    // `validation run` is never invoked, and named at both sites
    // (`crates/logweir-engine-oso/src/render_validation.rs`).
    match store.list_keys(&format!("logweir/{run_id}/engine-validation/")) {
        Ok(keys) if !keys.is_empty() => {
            // `list_keys` sorts, so this is the lexicographically first key,
            // not whatever the backend happened to stream first.
            let key = &keys[0];
            // The EXACT stored bytes, under the engine-document cap.
            let (raw, _vid) = store
                .get_capped(key, logweir_engine_oso::storage::caps::ENGINE_DOCUMENT)
                .map_err(sig)?;
            let sub = EngineSubreport {
                retained_verbatim: true,
                // The exact key that was read, not its prefix: a prefix holding
                // more than one object would not identify the bytes
                // `body_sha256` binds.
                retrieved_from: key.clone(),
                caveat: CAVEAT_ENGINE_SUBREPORT.into(),
                body_b64: base64::engine::general_purpose::STANDARD.encode(&raw),
                body_sha256: logweir_core::ids::sha256_prefixed(&raw),
            };
            sc.engine_subreport = Some(sub);
            // "The run's report is the only object under it" is an assumption
            // the code cannot check; when it does not hold, say so.
            if keys.len() > 1 {
                let n = keys.len();
                let key = key.clone();
                note(
                    &mut sc,
                    &format!(
                        "engine-validation prefix held {n} objects; retained {key} and \
                         dropped the rest"
                    ),
                );
            }
        }
        // An empty prefix is a warning on the phase record, never a failure:
        // the engine's own report is corroboration, not the measurement.
        _ => {
            sc.engine_subreport = None;
            note(
                &mut sc,
                "no engine validation report under the per-run prefix",
            );
        }
    }

    // 2. The engine-matrix row this run establishes. Decided HERE, at the one
    //    chokepoint every signed document passes through — the phase-5
    //    `Verdict::Block` jump, the phase-6 `RestoreNoOp` interception and the
    //    normal run all reach `run` and none of them can miss it — and before
    //    `validate_invariants`, so a `fail` without its required reason is
    //    unconstructible rather than merely unlikely.
    let (verdict, reason) =
        matrix_verdict_for(sc.outcome, sc.integrity.level, sc.engine.matrix_verdict);
    sc.engine.matrix_verdict = verdict;
    sc.engine.matrix_verdict_reason = reason;

    // 3. Never sign a claim that cannot be substantiated at signing time.
    //    These four fields describe the upload that has not happened yet
    //    (conditional put, version id, WORM retention), so the signed artifact
    //    under-claims, unconditionally, on every backend: an `immutable: true`
    //    nothing verified would be a valid signature over an unverified claim.
    //    The real readback lands on `Signed.scorecard` at step 8 and is
    //    published by the separately signed post-put receipt (`put_receipt`).
    //    Signing still precedes every put.
    sc.evidence.create_only_enforced = false;
    sc.evidence.immutable = false;
    sc.evidence.retain_until = None;
    sc.evidence.version_id = None;

    // 3b. THE OFFSET REPORT'S KEY AND DIGEST, BEFORE SIGNING — which is the
    //     opposite of step 3's four fields and for the opposite reason.
    //
    //     Those four describe an event that has not happened yet, so they
    //     cannot be signed. These two describe bytes that ALREADY EXIST on
    //     disk: the digest is computed from the file the engine wrote and the
    //     key is the one step 7 puts those exact bytes at. So they CAN be
    //     signed, and must be — a digest published outside the signature would
    //     bind nothing.
    //
    //     Both or neither, which is what `Scorecard::validate_invariants`'s
    //     arm refuses a document for: a key with no digest names bytes nothing
    //     binds, and a digest with no key binds bytes nobody can fetch.
    let offsets = read_offset_report(&run_id, offset_report);
    sc.evidence.offset_report_key = offsets.as_ref().map(|(k, _, _)| k.clone());
    sc.evidence.offset_report_sha256 = offsets.as_ref().map(|(_, d, _)| d.clone());

    // 4. Refuse to sign a self-contradicting document, over the exact document
    //    step 5 serialises and step 6 signs. After step 3: the invariants
    //    describe the signed scorecard, and they refuse a 1.0.x document whose
    //    four post-put fields are set, which is what step 3 overwrites.
    sc.validate_invariants().map_err(sig)?;

    // 5. The EXACT bytes that will be stored.
    let bytes = logweir_core::det_json::to_deterministic_json(&sc).map_err(sig)?;

    // 6. Sign. Any failure here is exit 4 and NOTHING has been uploaded — this
    //    step precedes every put, which is the mechanism, not a convention.
    let sidecar = signer
        .sign(logweir_evidence::PAYLOAD_TYPE_SCORECARD, &bytes)
        .map_err(sig)?;
    let sidecar_bytes = serde_json::to_vec(&sidecar).map_err(sig)?;

    // 7. Create-only puts. An object that already exists is REFUSED
    //    (`StoreError::AlreadyExists`), never overwritten, so a drill can
    //    never silently replace a previous drill's evidence. A backend that
    //    answers Unsupported takes the HEAD-then-PUT fallback inside
    //    `put_create_only`, which reports create_only_enforced: false —
    //    recorded honestly, never assumed.
    let scorecard_key = format!("logweir/drills/{run_id}.json");
    let sidecar_key = format!("logweir/drills/{run_id}.sig");
    let out: PutOutcome = store.put_create_only(&scorecard_key, &bytes).map_err(sig)?;
    store
        .put_create_only(&sidecar_key, &sidecar_bytes)
        .map_err(sig)?;
    // 7b. The offset report, at the key the SIGNED document above names, with
    //     the bytes the digest above covers. Create-only like the other two,
    //     and its failure is exit 4 through the same `sig` — a signed
    //     scorecard naming an object that was not written is worse than a run
    //     that reports it could not attest itself.
    if let Some((key, _, report_bytes)) = &offsets {
        store.put_create_only(key, report_bytes).map_err(sig)?;
    }

    // 8. Readback only, in memory only; no readback means `immutable: false`.
    //    These land on `Signed.scorecard` after signing, so the signed bytes
    //    under-claim on purpose (step 3). Nothing serialises
    //    `Signed.scorecard`: re-serialising it would invalidate the signature.
    sc.evidence.create_only_enforced = out.create_only_enforced;
    sc.evidence.version_id = out.version_id.clone();
    // A no-op today: `object_lock_readback` returns `None` on every backend
    // `object_store` 0.14 can build, and step 3 already zeroed both fields.
    // Kept as the only path a real readback would take; its honest-`None`
    // contract is pinned in
    // `crates/logweir-engine-oso/tests/storage.rs::object_lock_readback_reports_no_proof_rather_than_guessing`.
    let lock = store.object_lock_readback(&scorecard_key);
    sc.evidence.retain_until = lock.as_ref().and_then(|l| l.retain_until);
    sc.evidence.immutable = lock.map(|l| l.immutable).unwrap_or(false);

    // 9. Any failure at 7 or 8 is also exit 4 (`map_err(sig)` above). `bytes`
    //    is what was signed and stored; the orchestrator writes these to
    //    --out and never re-serialises the scorecard.
    Ok(Signed {
        scorecard: sc,
        bytes,
        sidecar,
        key: scorecard_key,
        sidecar_key,
        offset_report_key: offsets.map(|(k, _, _)| k),
    })
}

/// The post-put storage receipt.
///
/// `run` above signs the scorecard BEFORE it puts it, because a signature
/// covers bytes and the bytes must exist first. The consequence is that the
/// four storage facts describing that put — whether it was conditional, what
/// version id it got, and what the provider says about WORM retention — are
/// unknowable at signing time, so step 3 zeroes all four in the signed
/// document rather than sign a claim the store has never been asked about.
/// Step 8 then performs the real readback and lands it on `Signed.scorecard`,
/// which nothing serialises.
///
/// This is a second signed document carrying it, the pattern
/// `phase9_teardown` uses for the teardown attestation: a fact that becomes
/// true after signing needs its own signature.
///
/// It deliberately does NOT restate the scorecard. `scorecard_sha256` binds it
/// to the exact signed bytes, so a reader can tell WHICH document this receipt
/// describes — binding to `run_id` alone would carry no independent
/// information (the same argument `TeardownAttestation::scorecard_sha256`
/// makes).
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct PutReceipt {
    pub run_id: String,
    /// `sha256:<hex>` of the SIGNED scorecard bytes this receipt describes.
    pub scorecard_sha256: String,
    /// The object key the scorecard was put at — taken from `Signed.key`,
    /// i.e. the exact string `phase8_score::run` handed to `put_create_only`.
    pub scorecard_key: String,
    /// Observed from the put itself: `true` only when the backend performed a
    /// genuine conditional put. A backend that answered `Unsupported` took the
    /// HEAD-then-PUT fallback and reports `false` — recorded, never assumed.
    pub create_only_enforced: bool,
    /// The store's version id for the object, when the backend returned one.
    pub version_id: Option<String>,
    /// Spec §6 C3: `true` ONLY after a provider readback actually answered.
    /// `false` means "no proof obtainable", never "the object is mutable".
    pub immutable: bool,
    pub retain_until: Option<DateTime<Utc>>,
    /// When the readback was taken. Global Constraint 1: the clock is read in
    /// `crates/logweir`, never in `logweir-core`.
    pub observed_at: DateTime<Utc>,
}

/// Reads the post-put facts off `Signed.scorecard` — where `run` step 7 put
/// them — and never off the signed bytes, which by construction carry the
/// zeroed values.
pub fn put_receipt(signed: &Signed) -> PutReceipt {
    let e = &signed.scorecard.evidence;
    PutReceipt {
        run_id: signed.scorecard.run_id.clone(),
        scorecard_sha256: logweir_core::ids::sha256_prefixed(&signed.bytes),
        // The key `run` ACTUALLY put at, never a second reconstruction of it.
        scorecard_key: signed.key.clone(),
        create_only_enforced: e.create_only_enforced,
        version_id: e.version_id.clone(),
        immutable: e.immutable,
        retain_until: e.retain_until,
        observed_at: chrono::Utc::now(),
    }
}

/// Signed with `PAYLOAD_TYPE_PUT_RECEIPT` and put create-only next to the
/// scorecard, exactly as `phase9_teardown::persist` does for its attestation.
/// Signing precedes both puts for the same reason it does everywhere else, and
/// the same `SigningOrLock` variant carries the failure.
///
/// The CALLER treats a failure here as a warning, never as an outcome: the
/// drill result is already signed and uploaded, and a receipt that could not
/// be written must not retract a measurement.
pub fn persist_put_receipt(
    r: &PutReceipt,
    signing_key: &Path,
    store: &Store,
) -> Result<(), DrillError> {
    let signer = ValidatedSigner::load(
        signing_key,
        logweir_evidence::PAYLOAD_TYPE_PUT_RECEIPT,
        b"logweir restore signing readiness probe v1",
        "No evidence was uploaded",
    )
    .map_err(sig)?;
    persist_put_receipt_with_signer(r, &signer, store)
}

pub(crate) fn persist_put_receipt_with_signer(
    r: &PutReceipt,
    signer: &ValidatedSigner,
    store: &Store,
) -> Result<(), DrillError> {
    let bytes = logweir_core::det_json::to_deterministic_json(r).map_err(sig)?;
    let sidecar = signer
        .sign(logweir_evidence::PAYLOAD_TYPE_PUT_RECEIPT, &bytes)
        .map_err(sig)?;
    let sidecar_bytes = serde_json::to_vec(&sidecar).map_err(sig)?;
    store
        .put_create_only(&format!("logweir/drills/{}.receipt.json", r.run_id), &bytes)
        .map_err(sig)?;
    store
        .put_create_only(
            &format!("logweir/drills/{}.receipt.sig", r.run_id),
            &sidecar_bytes,
        )
        .map_err(sig)?;
    Ok(())
}
