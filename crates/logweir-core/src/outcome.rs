use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Spec §6.1. `drift` is deliberately NOT a v0.1 value — v0.1 collects no
/// metadata. It returns as a minor bump when SP2 ships.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "kebab-case")]
#[schemars(rename_all = "kebab-case")]
pub enum Outcome {
    Pass,
    FailObjective,
    FailIntegrity,
    PreflightFailed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "kebab-case")]
#[schemars(rename_all = "kebab-case")]
pub enum IntegrityLevel {
    /// sha256(key‖value‖headers‖timestamp) reconciled per record.
    ByteFingerprint,
    /// The KBAK decoder returned Err(Unsupported); records were consumed but
    /// not reconciled against archive bytes (spec §11).
    ConsumeOnly,
    NotAttempted,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "kebab-case")]
#[schemars(rename_all = "kebab-case")]
pub enum IntegrityResult {
    Pass,
    /// A selection whose evidence was inconclusive — something in the sample
    /// was not examined, so the drill cannot claim it. NOT a pass.
    ///
    /// A COMPACTED TOPIC DOES NOT REACH THIS in v0.1: it is reported as `fail`,
    /// through the same path a real mismatch takes. See `docs/stability.md`.
    // Full rationale, kept out of the doc comment because schemars publishes
    // doc comments as `description` in schemas/logweir-drill-scorecard-1.0.0.json
    // and this belongs in the code, not in the wire format:
    //
    // What actually reaches `Partial` in v0.1, per `phase7_verify`'s module
    // doc: an archive returning zero or short fingerprints for a selection; a
    // (topic, partition, window) the manifest claims exists but no segment
    // matches; a pre-0.21 segment carrying no sha256; a consume-only selection
    // whose target partition gave back less than the manifest claims. Each
    // names itself in `integrity.partial_reason`.
    //
    // What does NOT reach it, despite the name this variant is usually
    // explained by: a compacted topic. Telling "compaction removed this record
    // on purpose" apart from "the restore or the archive lost it" needs a
    // specific mismatch cross-referenced against `topic_parity`'s
    // `cleanup.policy=compact` flag, and `phase7_verify::run` does not attempt
    // it. That is a declared limitation, not a defect.
    // `fixtures::verify_outcome_for_compacted_topic` pins the SHAPE of such a
    // document for the scoring and rendering layers and describes no path
    // `run` itself can take; Task 21c confirmed against a live cluster that no
    // e2e drill can drive it.
    Partial,
    Fail,
}

/// Per-run readback of an engine lever (spec §9.3 phase 5, §7.2(a)).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "kebab-case")]
#[schemars(rename_all = "kebab-case")]
pub enum LeverState {
    Honoured,
    Ignored,
    /// `dry_run_check_segments` is not observable from DryRunReport; its only
    /// per-run readback is the absence of an unknown-key warning.
    UnknownNotObservable,
}

/// Copied from this engine tag's engine-matrix row; never re-derived per run.
/// Every variant serialises as a FLAT kebab-case string, so the field's schema
/// is a plain enum and the auditor's ~20-line Python verifier never has to
/// handle `string | object`. The reason for a `fail` lives in
/// `EngineInfo.matrix_verdict_reason`, mirroring the
/// `integrity.result`/`partial_reason` pair.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "kebab-case")]
#[schemars(rename_all = "kebab-case")]
pub enum MatrixVerdict {
    Pass,
    PassDegraded,
    Fail,
    FailLeverNotHonoured,
    UnsupportedLeverAbsent,
}
