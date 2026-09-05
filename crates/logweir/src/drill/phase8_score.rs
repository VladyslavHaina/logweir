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
        //
        // Clamped at zero, and the direction of the subtraction is load-bearing:
        // the gap is how far SHORT of the requested recovery point the newest
        // restored record falls, so it is `requested - newest`. If the newest
        // restored record lands at or beyond the requested point there is no
        // coverage gap at that point, and the answer is 0 — a NEGATIVE gap is
        // not a smaller gap, it is a meaningless one, and a reader meeting
        // `rpo_seconds: -90` in a signed document would most likely read it as
        // "no data loss". `Measured::rpo_seconds` stays `Option<i64>` because
        // Global Constraint 12 freezes the field's type at format_version
        // 1.0.0; the clamp makes a negative value unconstructible HERE, which
        // is the only place v0.1 computes it. Mirrors `secs`'s own `.max(0)`
        // above.
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
        // `+ f64::EPSILON` is a deliberate one-ULP tolerance, not slop: a
        // measured rate that is a single floating-point step below the
        // requested one (75/75 against a `pass_rate` an adopter wrote as
        // `0.1 + 0.2`) is a rounding artefact of the comparison, not a missed
        // objective.
        (Some(want), Some(got)) => Some(got + f64::EPSILON >= want),
        // Provably dead, and kept only because the brief writes it verbatim:
        // this arm fires exactly when `spec.pass_rate.is_some() &&
        // pass_rate.is_none()`, which is the same condition that short-circuits
        // `met` to `None` below, so its value can never influence the result.
        // Left in place with this note so a future reader does not mistake it
        // for live logic and "simplify" the `met` short-circuit away.
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
    /// The object key `bytes` was ACTUALLY put at, carried out rather than
    /// left to be reconstructed by whoever needs to name it.
    ///
    /// Reconstructing a key that is supposed to identify a specific object is
    /// the same defect shape Task 20 fix round 1 removed from
    /// `EngineSubreport.retrieved_from` (which named the prefix, not the key):
    /// the artifact ends up pointing at a location that may not hold what was
    /// written. `run` builds this string once, puts at it, reads the object
    /// lock back from it, and hands it out here, so the three can never
    /// disagree.
    pub key: String,
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
            // `list_keys` sorts, so this is the lexicographically first key,
            // not whatever the backend happened to stream first.
            let key = &keys[0];
            let (raw, _vid) = store.get(key).map_err(sig)?; // the EXACT stored bytes
            let sub = EngineSubreport {
                retained_verbatim: true,
                // The exact KEY that was read, not the prefix it sits under.
                // The brief writes the prefix here, but a prefix does not
                // identify what was retained: if the prefix ever holds more
                // than one object, an artifact naming only the prefix points
                // at a location that may contain something other than the
                // bytes `body_sha256` binds.
                retrieved_from: key.clone(),
                caveat: CAVEAT_ENGINE_SUBREPORT.into(),
                body_b64: base64::engine::general_purpose::STANDARD.encode(&raw),
                body_sha256: logweir_core::ids::sha256_prefixed(&raw),
            };
            sc.engine_subreport = Some(sub);
            // "The run's report is the only object under it" is an assumption
            // of the brief's, not something the code can check. When it does
            // not hold, say so rather than dropping the extras in silence.
            if keys.len() > 1 {
                let n = keys.len();
                if let Some(p) = sc.phases.iter_mut().find(|p| p.phase == 8) {
                    p.notes.push(format!(
                        "engine-validation prefix held {n} objects; retained {key} and \
                         dropped the rest"
                    ));
                }
            }
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

    // 3. Never sign a claim that cannot be substantiated at signing time.
    //
    //    These four fields describe the upload that has NOT HAPPENED YET —
    //    whether the put was conditional, what version id it got, and what the
    //    provider says about WORM retention. At this point they are not facts,
    //    they are hopes, and whatever the caller put in them is a declaration
    //    the store has never been asked about. Signing that declaration would
    //    produce a valid signature over an unverified claim: an auditor reading
    //    `immutable: true` off a correctly-signed scorecard would conclude the
    //    evidence is under WORM retention, `drill verify` would not disagree,
    //    and nothing anywhere would detect it.
    //
    //    So the signed artifact says what is actually true at signing time —
    //    no proof obtainable — and it says that unconditionally, on every
    //    backend and for every caller. It deliberately UNDER-claims: a run
    //    whose put really was conditional still publishes
    //    `create_only_enforced: false`, because phase 8 cannot know that yet
    //    and a scorecard is not the place to guess. The real post-put readback
    //    lands on `Signed.scorecard` at step 6 and belongs in a SECOND signed
    //    receipt written after the put — the pattern phase 9 already uses for
    //    the teardown attestation. Until that receipt exists, the readback is
    //    not auditor-visible, and that is the honest state of v0.1
    //    (docs/stability.md says so in those words).
    //
    //    This does NOT change the brief's ordering: signing still precedes
    //    every put, and the scorecard is still never re-serialised afterwards.
    sc.evidence.create_only_enforced = false;
    sc.evidence.immutable = false;
    sc.evidence.retain_until = None;
    sc.evidence.version_id = None;

    // 4. The EXACT bytes that will be stored.
    let bytes = logweir_core::det_json::to_deterministic_json(&sc).map_err(sig)?;

    // 5. Sign. Any failure here is exit 4 and NOTHING has been uploaded — this
    //    step precedes every put, which is the mechanism, not a convention.
    let key = logweir_evidence::keys::SigningKey::from_pem_file(signing_key).map_err(sig)?;
    let sidecar = logweir_evidence::sign::sign_detached(
        &key,
        logweir_evidence::PAYLOAD_TYPE_SCORECARD,
        &bytes,
    )
    .map_err(sig)?;
    let sidecar_bytes = serde_json::to_vec(&sidecar).map_err(sig)?;

    // 6. Create-only puts. An object that already exists is REFUSED
    //    (`StoreError::AlreadyExists`), never overwritten, so a drill can
    //    never silently replace a previous drill's evidence. A backend that
    //    answers Unsupported takes the HEAD-then-PUT fallback inside
    //    `put_create_only`, which reports create_only_enforced: false —
    //    recorded honestly, never assumed.
    let scorecard_key = format!("logweir/drills/{run_id}.json");
    let out: PutOutcome = store.put_create_only(&scorecard_key, &bytes).map_err(sig)?;
    store
        .put_create_only(&format!("logweir/drills/{run_id}.sig"), &sidecar_bytes)
        .map_err(sig)?;

    // 7. Readback ONLY, and IN-MEMORY ONLY. No readback => immutable: false.
    //
    //    These assignments land on `Signed.scorecard` AFTER the bytes were
    //    signed, so they are deliberately NOT in the signed artifact — see
    //    step 3, which zeroed all four fields precisely so the signed document
    //    carries no unsubstantiated claim about them. The signed bytes and
    //    `Signed.scorecard` therefore disagree here on purpose, and in the
    //    safe direction: the document under-claims, and the readback is the
    //    stronger fact held in memory for a future post-put receipt to
    //    publish. Nothing serialises `Signed.scorecard`; re-serialising it
    //    would invalidate the signature.
    sc.evidence.create_only_enforced = out.create_only_enforced;
    sc.evidence.version_id = out.version_id.clone();
    // The two lines below are LIVE WIRING that is provably a no-op in v0.1, and
    // that is worth stating rather than discovering: `object_lock_readback`
    // returns `None` on every backend `object_store` 0.14 can build, and step 3
    // already zeroed both fields, so both sides are equal today. Deleting them
    // therefore changes nothing observable and no test can catch it (Task 20
    // fix round 1, mutants M12/M16 — knowingly accepted survivors). They are
    // kept because they are the only path by which a real readback would ever
    // reach the scorecard, and a mutant that FABRICATES a readback here is
    // caught (mutant M15). `object_lock_readback`'s honest-`None` contract is
    // pinned at its source, in
    // `crates/logweir-engine-oso/tests/storage.rs::object_lock_readback_reports_no_proof_rather_than_guessing`.
    let lock = store.object_lock_readback(&scorecard_key);
    sc.evidence.retain_until = lock.as_ref().and_then(|l| l.retain_until);
    sc.evidence.immutable = lock.map(|l| l.immutable).unwrap_or(false);

    // 8. Any failure at 6 or 7 is also exit 4 — see the `map_err(sig)` above.
    //    NOTE: `bytes` is what was signed AND what was stored. `sc` now carries
    //    the evidence readback, which is why `Signed.bytes` is returned
    //    alongside it: the orchestrator writes THESE bytes to --out and never
    //    re-serialises the scorecard after signing.
    Ok(Signed {
        scorecard: sc,
        bytes,
        sidecar,
        key: scorecard_key,
    })
}

/// The post-put storage receipt — Task 20's carried obligation, discharged in
/// Task 21a.
///
/// `run` above signs the scorecard BEFORE it puts it, because a signature
/// covers bytes and the bytes must exist first. The consequence is that the
/// four storage facts describing that put — whether it was conditional, what
/// version id it got, and what the provider says about WORM retention — are
/// unknowable at signing time, so step 3 zeroes all four in the signed
/// document rather than sign a claim the store has never been asked about.
/// Step 7 then performs the real readback and lands it on `Signed.scorecard`,
/// which nothing serialises.
///
/// Until this receipt existed, that readback was published NOWHERE an auditor
/// could read it, and `docs/stability.md` recorded the gap in those words:
/// Logweir published no verifiable evidence that its own upload was
/// create-only. This is a SECOND signed document carrying it — the same
/// pattern `phase9_teardown` uses for the teardown attestation, and for the
/// same reason: a fact that becomes true after signing needs its own
/// signature, not a second bite at the first one.
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
    let bytes = logweir_core::det_json::to_deterministic_json(r).map_err(sig)?;
    let key = logweir_evidence::keys::SigningKey::from_pem_file(signing_key).map_err(sig)?;
    let sidecar = logweir_evidence::sign::sign_detached(
        &key,
        logweir_evidence::PAYLOAD_TYPE_PUT_RECEIPT,
        &bytes,
    )
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
