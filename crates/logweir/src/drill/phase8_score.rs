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
/// derived from. Six fields, in this order (task-20-addendum.md ruling A1):
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
        // ARCHIVE COVERAGE GAP at the requested recovery point — NOT
        // source-relative data loss. The schema and docs/formats/ say so too.
        rpo_seconds: Some((requested_point_in_time_ms - newest_restored_record_ts_ms) / 1000),
        // v0.1 never contacts the source, so this is null with its reason. It
        // becomes a number only under --from-cluster (deferred). This pair is
        // exactly the `captured_by_logweir == false` branch
        // `Scorecard::validate_invariants` requires.
        rpo_source_relative_seconds: None,
        rpo_source_relative_unmeasured_reason: Some("source cluster never contacted".into()),
    }
}

/// Returns `(outcome, objectives-as-REQUESTED, measured pass rate)`. The caller
/// writes the third element into `integrity.pass_rate_measured`.
///
/// `integ` is phase 7's verdict, consumed as given: `IntegrityResult` is
/// decided in exactly one place (`phase7_verify::roll_up`) and phase 8 never
/// re-derives or second-guesses it.
pub fn decide(
    m: &Measured,
    spec: &ObjectivesSpec,
    integ: &Integrity,
) -> (Outcome, Objectives, Option<f64>) {
    // pass_rate is NULL when integrity.level is consume-only or not-attempted,
    // in which case objectives.met is null rather than true.
    let pass_rate = if integ.level == IntegrityLevel::ByteFingerprint && integ.records_sampled > 0 {
        Some(integ.records_sampled_matching as f64 / integ.records_sampled as f64)
    } else {
        None
    };

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
        (Some(want), Some(got)) => Some(got + f64::EPSILON >= want),
        (Some(_), None) => None,
        _ => None,
    };

    let met = if spec.pass_rate.is_some() && pass_rate.is_none() {
        None // unmeasurable, so the aggregate verdict is unmeasurable
    } else {
        Some([rto_ok, rpo_ok, rate_ok].into_iter().flatten().all(|b| b))
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
    // measurement is published separately, in `integrity.pass_rate_measured`.
    (
        outcome,
        Objectives {
            rto_seconds: spec.rto_seconds,
            rpo_seconds: spec.rpo_seconds,
            pass_rate: spec.pass_rate,
            met,
        },
        pass_rate,
    )
}

/// The signed result. `bytes` is what was signed AND what was stored: the
/// orchestrator writes THESE bytes to `--out` and never re-serialises the
/// scorecard after signing.
pub struct Signed {
    pub scorecard: Scorecard,
    pub bytes: Vec<u8>,
    pub sidecar: logweir_evidence::Sidecar,
}

/// Hand-written rather than derived: `bytes` is the whole document and
/// `Debug`-printing it into a test failure message would be unreadable. The
/// impl exists at all because Task 20's tests call `.unwrap_err()` on
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

pub fn run(sc: &Scorecard, signing_key: &Path, store: &Store) -> Result<Signed, DrillError> {
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
    match store.list_keys(&format!("logweir/{run_id}/engine-validation/")) {
        Ok(keys) if !keys.is_empty() => {
            let key = &keys[0];
            let (raw, _vid) = store.get(key).map_err(sig)?; // the EXACT stored bytes
            let sub = EngineSubreport {
                retained_verbatim: true,
                retrieved_from: format!("logweir/{run_id}/engine-validation"),
                caveat: CAVEAT_ENGINE_SUBREPORT.into(),
                body_b64: base64::engine::general_purpose::STANDARD.encode(&raw),
                body_sha256: logweir_core::ids::sha256_prefixed(&raw),
            };
            sc.engine_subreport = Some(sub);
        }
        // An empty prefix is a warning on the phase record, never a failure:
        // the engine's own report is corroboration, not the measurement.
        _ => {
            sc.engine_subreport = None;
            if let Some(p) = sc.phases.iter_mut().find(|p| p.phase == 8) {
                p.notes
                    .push("no engine validation report under the per-run prefix".into());
            }
        }
    }

    // 2. Refuse to sign a self-contradicting document.
    sc.validate_invariants().map_err(sig)?;

    // 3. The EXACT bytes that will be stored.
    let bytes = logweir_core::det_json::to_deterministic_json(&sc).map_err(sig)?;

    // 4. Sign. Any failure here is exit 4 and NOTHING has been uploaded — this
    //    step precedes every put, which is the mechanism, not a convention.
    let key = logweir_evidence::keys::SigningKey::from_pem_file(signing_key).map_err(sig)?;
    let sidecar = logweir_evidence::sign::sign_detached(
        &key,
        logweir_evidence::PAYLOAD_TYPE_SCORECARD,
        &bytes,
    )
    .map_err(sig)?;
    let sidecar_bytes = serde_json::to_vec(&sidecar).map_err(sig)?;

    // 5. Create-only puts. An object that already exists is REFUSED
    //    (`StoreError::AlreadyExists`), never overwritten, so a drill can
    //    never silently replace a previous drill's evidence. A backend that
    //    answers Unsupported takes the HEAD-then-PUT fallback inside
    //    `put_create_only`, which reports create_only_enforced: false —
    //    recorded honestly, never assumed.
    let out: PutOutcome = store
        .put_create_only(&format!("logweir/drills/{run_id}.json"), &bytes)
        .map_err(sig)?;
    store
        .put_create_only(&format!("logweir/drills/{run_id}.sig"), &sidecar_bytes)
        .map_err(sig)?;

    // 6. Readback ONLY. No readback => immutable: false. Both fields are
    //    ASSIGNED here rather than merged, so a caller's optimistic claim
    //    cannot survive into the signed document.
    sc.evidence.create_only_enforced = out.create_only_enforced;
    sc.evidence.version_id = out.version_id.clone();
    let lock = store.object_lock_readback(&format!("logweir/drills/{run_id}.json"));
    sc.evidence.retain_until = lock.as_ref().and_then(|l| l.retain_until);
    sc.evidence.immutable = lock.map(|l| l.immutable).unwrap_or(false);

    // 7. Any failure at 5 or 6 is also exit 4 — see the `map_err(sig)` above.
    //    NOTE: `bytes` is what was signed AND what was stored. `sc` now carries
    //    the evidence readback, which is why `Signed.bytes` is returned
    //    alongside it: the orchestrator writes THESE bytes to --out and never
    //    re-serialises the scorecard after signing.
    Ok(Signed {
        scorecard: sc,
        bytes,
        sidecar,
    })
}
