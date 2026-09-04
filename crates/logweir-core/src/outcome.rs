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
    /// The compacted-topic / non-reconcilable-record case. NOT a pass.
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
