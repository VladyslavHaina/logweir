use crate::outcome::{IntegrityLevel, IntegrityResult, LeverState, MatrixVerdict, Outcome};
use chrono::{DateTime, Utc};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct Scorecard {
    pub format_version: String,
    pub run_id: String,
    pub outcome: Outcome,
    /// Highest phase INDEX completed. v0.1 implements ELEVEN phase slots,
    /// -1 through 9 (Global Constraint 18, panel decision D1, 2026-09-03).
    /// -1 is the source-side capture phase; a value below it is impossible
    /// because the phase-0 admission guard refusing yields exit code 3 with
    /// last_phase_completed = -1 (spec §6.1).
    pub last_phase_completed: i8,
    pub requested_at: DateTime<Utc>,
    #[serde(default)]
    pub approval_validated_at: Option<DateTime<Utc>>,
    #[serde(default)]
    pub triggered_by: Option<String>,
    pub engine: EngineInfo,
    pub source: SourceInfo,
    pub target: TargetInfo,
    pub approval: ApprovalInfo,
    pub phases: Vec<PhaseRecord>,
    pub measured: Measured,
    pub objectives: Objectives,
    pub sample: SampleInfo,
    pub target_diff: TargetDiffSummary,
    pub integrity: Integrity,
    pub topic_parity: TopicParity,
    #[serde(default)]
    pub engine_subreport: Option<EngineSubreport>,
    pub evidence: EvidenceInfo,
    #[serde(default)]
    pub redactions: Vec<Redaction>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct EngineInfo {
    pub id: String,        // "oso-cli"
    pub version: String,   // "v0.21.0"
    pub digest: String,    // "sha256:…" — the image digest the binary came from
    pub execution: String, // "subprocess" in v0.1; "k8s-job" only under weirkeeper (SP5)
    pub levers: Levers,
    pub matrix_verdict: MatrixVerdict,
    /// REQUIRED when `matrix_verdict` is `fail`; null otherwise. Enforced by
    /// `validate_invariants`.
    #[serde(default)]
    pub matrix_verdict_reason: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct Levers {
    pub header_preflight: LeverState,
    pub dry_run_check_segments: LeverState,
    /// Every path the engine logged as `Ignoring unknown config key <path>`.
    /// A match on a Logweir-rendered key aborts the run (spec §7.2(a)).
    #[serde(default)]
    pub unknown_key_warnings: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct SourceInfo {
    pub backup_id: String,
    pub manifest_sha256: String,
    #[serde(default)]
    pub manifest_version_id: Option<String>,
    /// true ONLY under `--from-cluster`. `--from-cluster` is in v0.1 scope
    /// (Global Constraint 18 / docs/adr/0007-from-cluster-in-v0.1.md); its
    /// execution path lands in a follow-up task, so nothing in the main task
    /// line yet sets this true. The invariant below is live regardless.
    pub captured_by_logweir: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct TargetInfo {
    pub cluster_id: String,
    /// The v0.1 segregation proof, verified over the logweir-kafka client:
    /// cluster_id ∈ allowedClusterIds AND this topic exists (spec §9.3 phase 0).
    pub marker_topic: String,
    pub topic_mapping_prefix: String,
    /// sha256 over the rendered restore.yaml topic_mapping block, so an
    /// auditor can re-derive exactly what was written.
    pub topic_mapping_sha256: String,
    pub topic_mapping_entries: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct ApprovalInfo {
    pub approver: String,
    pub ticket: String,
    pub plan_hash: String,
    /// The human's OUT-OF-BAND timestamp. Explicitly NOT an input to any
    /// measured field in v0.1 (spec §9.3 phase 8).
    pub approved_at: DateTime<Utc>,
    pub key_id: String,
    /// true when the approval key equals the signing key. Labelled, never
    /// refused (spec §10).
    pub self_attested: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct PhaseRecord {
    pub phase: i8,
    pub name: String,
    pub at: DateTime<Utc>,
    pub outcome: String,
    pub duration_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct Measured {
    #[serde(default)]
    pub rto_seconds: Option<u64>,
    #[serde(default)]
    pub rto_requested_to_verified_seconds: Option<u64>,
    #[serde(default)]
    pub rto_restore_only_seconds: Option<u64>,
    /// THE value compared against objectives.rto_seconds (spec §9.3 phase 8).
    #[serde(default)]
    pub rto_excluding_preflight_seconds: Option<u64>,
    /// ARCHIVE COVERAGE GAP at the requested recovery point — NOT
    /// source-relative data loss.
    #[serde(default)]
    pub rpo_seconds: Option<i64>,
    #[serde(default)]
    pub rpo_source_relative_seconds: Option<i64>,
    #[serde(default)]
    pub rpo_source_relative_unmeasured_reason: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct Objectives {
    #[serde(default)]
    pub rto_seconds: Option<u64>,
    #[serde(default)]
    pub rpo_seconds: Option<i64>,
    #[serde(default)]
    pub pass_rate: Option<f64>,
    /// false when ANY non-null objective is missed; null when pass_rate is null.
    #[serde(default)]
    pub met: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct SampleInfo {
    pub window_start: DateTime<Utc>,
    pub window_end: DateTime<Utc>,
    pub topics: u32,
    pub partitions: u32,
    pub records_expected: u64,
    pub records_restored: u64,
    pub anchor: String, // head | tail | random
    pub coverage_note: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct Integrity {
    pub level: IntegrityLevel,
    pub result: IntegrityResult,
    #[serde(default)]
    pub partial_reason: Option<String>,
    pub records_sampled: u64,
    pub records_sampled_matching: u64,
    pub mismatches: u64,
    /// MEASURED `records_sampled_matching / records_sampled`. Null when
    /// `level` is not `byte-fingerprint`. The REQUESTED rate stays in
    /// `objectives.pass_rate`, so an auditor can read both the ask and the
    /// result off one document.
    #[serde(default)]
    pub pass_rate_measured: Option<f64>,
    /// SP3 only; null, never false, when not attempted. The wire name is
    /// camelCase because spec §6.1 and §14 SP3 both cite the path
    /// `integrity.restoredPrincipalCouldConsume`, and `format_version` freezes
    /// at 1.0.0 in this task — renaming later would be a MAJOR bump.
    #[serde(default, rename = "restoredPrincipalCouldConsume")]
    #[schemars(rename = "restoredPrincipalCouldConsume")]
    pub restored_principal_could_consume: Option<bool>,
}

/// The published sink for phase 3 — the phase no shipped artifact performs.
/// Without this block the diff would be computed and discarded, and the §4
/// positioning claim would rest on a value that reaches no reader.
#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
pub struct TargetDiffSummary {
    /// Mapped target topics that already existed AND already held records.
    #[serde(default)]
    pub collisions: Vec<String>,
    /// (mapped target topic, partition count the restore created).
    #[serde(default)]
    pub would_create: Vec<(String, i32)>,
    /// "full" in v0.1. Becomes "shallow" only if spec §15 cut 0d is ever taken.
    pub level: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct TopicParity {
    pub intentionally_deviated: Vec<String>,
    pub unexpected_divergence: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct EngineSubreport {
    pub retained_verbatim: bool,
    pub retrieved_from: String,
    pub caveat: String,
    /// The engine's evidence report as the EXACT bytes read from the bucket,
    /// base64 (RFC 4648 standard alphabet, padded).
    ///
    /// NOT a parsed `serde_json::Value`. OSO's envelope "covers the exact,
    /// complete stored report bytes. No canonicalization is performed at
    /// verification time" and "no JSON parsing, re-serialization, or
    /// canonicalization influences the digest or signature check"
    /// [VERIFIED U/kafka-backup/crates/kafka-backup-core/src/evidence/envelope.rs:5,254].
    /// Re-emitting a parsed value through `to_deterministic_json`'s two-space
    /// `PrettyFormatter` would change whitespace, escaping and number
    /// formatting, so the digest OSO signed would no longer match and the SP1c
    /// exit criterion ("the embedded engine sub-report round-trips through
    /// OSO's own `validation evidence-verify`") could never be ticked.
    pub body_b64: String,
    /// `sha256:<hex>` of the DECODED bytes, so an auditor can re-check the
    /// binding without base64-decoding anything.
    pub body_sha256: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct EvidenceInfo {
    #[serde(default)]
    pub version_id: Option<String>,
    #[serde(default)]
    pub retain_until: Option<DateTime<Utc>>,
    /// ONLY after a provider readback; else false (spec §6 C3).
    pub immutable: bool,
    /// false when the backend lacks conditional put (spec §11).
    pub create_only_enforced: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct Redaction {
    /// A JSON pointer into THIS scorecard — never a missing field.
    pub path: String,
    pub reason: String,
    pub present: bool,
}

#[derive(Debug, thiserror::Error)]
#[error("scorecard invariant violated: {0}")]
pub struct InvariantError(pub String);

impl Scorecard {
    /// The invariants spec §6.1 and §9.3 phase 8 state in prose. Called before
    /// signing (Task 20) and by `drill verify` (Task 6), so no signed document
    /// can carry a self-contradicting claim.
    pub fn validate_invariants(&self) -> Result<(), InvariantError> {
        if self.integrity.result == IntegrityResult::Partial
            && self.integrity.partial_reason.is_none()
        {
            return Err(InvariantError(
                "integrity.result is 'partial' but partial_reason is null".into(),
            ));
        }
        // Global Constraint 18(a): captured_by_logweir is a biconditional.
        // true  => last_phase_completed >= -1, a measured source-relative RPO,
        //          and NO unmeasured reason.
        // false => no source-relative RPO, and a reason saying why not.
        if self.source.captured_by_logweir {
            if self.last_phase_completed < -1 {
                return Err(InvariantError(
                    "source.captured_by_logweir is true but last_phase_completed is below -1"
                        .into(),
                ));
            }
            if self.measured.rpo_source_relative_seconds.is_none() {
                return Err(InvariantError(
                    "source.captured_by_logweir is true but rpo_source_relative_seconds is null"
                        .into(),
                ));
            }
            if self
                .measured
                .rpo_source_relative_unmeasured_reason
                .is_some()
            {
                return Err(InvariantError(
                    "source.captured_by_logweir is true but an unmeasured reason is present".into(),
                ));
            }
        } else {
            if self.measured.rpo_source_relative_seconds.is_some() {
                return Err(InvariantError(
                    "rpo_source_relative_seconds is set but the source was never contacted".into(),
                ));
            }
            if self
                .measured
                .rpo_source_relative_unmeasured_reason
                .is_none()
            {
                return Err(InvariantError(
                    "source.captured_by_logweir is false but rpo_source_relative_unmeasured_reason is null".into(),
                ));
            }
        }
        if self.integrity.level != IntegrityLevel::ByteFingerprint
            && self.objectives.pass_rate.is_some()
            && self.objectives.met == Some(true)
        {
            return Err(InvariantError(
                "objectives.met must be null when pass_rate is not measurable".into(),
            ));
        }
        if self.integrity.records_sampled_matching > self.integrity.records_sampled {
            return Err(InvariantError(
                "records_sampled_matching exceeds records_sampled".into(),
            ));
        }
        if self.engine.matrix_verdict == MatrixVerdict::Fail
            && self.engine.matrix_verdict_reason.is_none()
        {
            return Err(InvariantError(
                "engine.matrix_verdict is 'fail' but matrix_verdict_reason is null".into(),
            ));
        }
        if self.integrity.level != IntegrityLevel::ByteFingerprint
            && self.integrity.pass_rate_measured.is_some()
        {
            return Err(InvariantError(
                "integrity.pass_rate_measured is set but the level is not byte-fingerprint".into(),
            ));
        }
        if !(-1..=9).contains(&self.last_phase_completed) {
            return Err(InvariantError("last_phase_completed outside -1..=9".into()));
        }
        Ok(())
    }
}
