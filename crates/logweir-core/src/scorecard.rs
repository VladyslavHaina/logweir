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

/// The leading dot-separated component of a semver string, parsed as an
/// integer. Returns `None` for anything that doesn't start with an integer
/// (so a malformed `format_version` is refused rather than silently treated
/// as major 0).
fn major_version(v: &str) -> Option<u64> {
    v.split('.').next()?.parse().ok()
}

impl Scorecard {
    /// The invariants spec §6.1 and §9.3 phase 8 state in prose. Called before
    /// signing (Task 20) and by `drill verify` (Task 6), so no signed document
    /// can carry a self-contradicting claim.
    pub fn validate_invariants(&self) -> Result<(), InvariantError> {
        // Global Constraint 12: a reader must refuse a `format_version` whose
        // major is newer than the one this binary understands. Checked
        // first, and by string comparison against `crate::FORMAT_VERSION`
        // (never by re-deriving what "this build understands" some other
        // way), so a document from a future major bump is rejected before
        // any other invariant is even evaluated against fields that build may
        // have changed the meaning of.
        let doc_major = major_version(&self.format_version).ok_or_else(|| {
            InvariantError(format!(
                "format_version {:?} is not a parseable semver",
                self.format_version
            ))
        })?;
        let known_major =
            major_version(crate::FORMAT_VERSION).expect("FORMAT_VERSION is a valid semver");
        if doc_major > known_major {
            return Err(InvariantError(format!(
                "format_version {} has a major version newer than this reader understands \
                 (this build knows {})",
                self.format_version,
                crate::FORMAT_VERSION
            )));
        }
        if self.integrity.result == IntegrityResult::Partial
            && self.integrity.partial_reason.is_none()
        {
            return Err(InvariantError(
                "integrity.result is 'partial' but partial_reason is null".into(),
            ));
        }
        // The format's only two float fields. `serde_json::to_value` turns a
        // non-finite f64 into `Value::Null` before `det_json`'s own walk ever
        // sees it (see `det_json.rs`'s module doc comment), so finiteness has
        // to be enforced here, on the typed field, where the error can still
        // name which field was bad.
        if let Some(pass_rate) = self.objectives.pass_rate {
            if !pass_rate.is_finite() {
                return Err(InvariantError(
                    "objectives.pass_rate is not finite (NaN or +/-Inf)".into(),
                ));
            }
        }
        if let Some(pass_rate_measured) = self.integrity.pass_rate_measured {
            if !pass_rate_measured.is_finite() {
                return Err(InvariantError(
                    "integrity.pass_rate_measured is not finite (NaN or +/-Inf)".into(),
                ));
            }
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

#[cfg(test)]
mod tests {
    //! Direct coverage of `Scorecard::validate_invariants`'s per-arm behaviour.
    //! `crates/logweir-core/tests/scorecard_golden.rs` is frozen at exactly
    //! four tests (addendum ruling A1), so this coverage lives here instead.
    //! Every test asserts the SPECIFIC error message, not a bare `is_err()`,
    //! so deleting or merging an arm makes exactly one test fail.
    use super::*;

    /// A scorecard that satisfies every invariant `validate_invariants` checks.
    /// `captured_by_logweir` is false (the false-branch of Global Constraint
    /// 18(a)), matching what v0.1 ever actually produces. Each test below
    /// clones this and overrides only the field(s) needed to trip one arm.
    fn valid_scorecard() -> Scorecard {
        let t = |s: &str| DateTime::parse_from_rfc3339(s).unwrap().with_timezone(&Utc);
        Scorecard {
            format_version: crate::FORMAT_VERSION.to_string(),
            run_id: "01J9X2QK7C4V0R8YB3ZP6MTS5A".into(),
            outcome: Outcome::Pass,
            last_phase_completed: 9,
            requested_at: t("2026-09-03T09:00:00Z"),
            approval_validated_at: None,
            triggered_by: None,
            engine: EngineInfo {
                id: "oso-cli".into(),
                version: "v0.21.0".into(),
                digest: "sha256:0".into(),
                execution: "subprocess".into(),
                levers: Levers {
                    header_preflight: LeverState::Honoured,
                    dry_run_check_segments: LeverState::UnknownNotObservable,
                    unknown_key_warnings: vec![],
                },
                matrix_verdict: MatrixVerdict::Pass,
                matrix_verdict_reason: None,
            },
            source: SourceInfo {
                backup_id: "backup-1".into(),
                manifest_sha256: "sha256:0".into(),
                manifest_version_id: None,
                captured_by_logweir: false,
            },
            target: TargetInfo {
                cluster_id: "cluster-1".into(),
                marker_topic: "logweir.scratch".into(),
                topic_mapping_prefix: "drill-".into(),
                topic_mapping_sha256: "sha256:0".into(),
                topic_mapping_entries: 1,
            },
            approval: ApprovalInfo {
                approver: "sre-oncall@example.com".into(),
                ticket: "CHG-1".into(),
                plan_hash: "sha256:0".into(),
                approved_at: t("2026-09-02T17:40:00Z"),
                key_id: "a".repeat(64),
                self_attested: false,
            },
            phases: vec![],
            measured: Measured {
                rto_seconds: None,
                rto_requested_to_verified_seconds: None,
                rto_restore_only_seconds: None,
                rto_excluding_preflight_seconds: None,
                rpo_seconds: None,
                rpo_source_relative_seconds: None,
                rpo_source_relative_unmeasured_reason: Some(
                    "source cluster never contacted".into(),
                ),
            },
            objectives: Objectives {
                rto_seconds: None,
                rpo_seconds: None,
                pass_rate: None,
                met: None,
            },
            sample: SampleInfo {
                window_start: t("2026-08-29T00:00:00Z"),
                window_end: t("2026-08-30T02:00:00Z"),
                topics: 1,
                partitions: 1,
                records_expected: 0,
                records_restored: 0,
                anchor: "head".into(),
                coverage_note: "no capture gap overlaps the sampled window".into(),
            },
            target_diff: TargetDiffSummary {
                collisions: vec![],
                would_create: vec![],
                level: "full".into(),
            },
            integrity: Integrity {
                level: IntegrityLevel::ByteFingerprint,
                result: IntegrityResult::Pass,
                partial_reason: None,
                records_sampled: 0,
                records_sampled_matching: 0,
                mismatches: 0,
                pass_rate_measured: None,
                restored_principal_could_consume: None,
            },
            topic_parity: TopicParity {
                intentionally_deviated: vec![],
                unexpected_divergence: vec![],
            },
            engine_subreport: None,
            evidence: EvidenceInfo {
                version_id: None,
                retain_until: None,
                immutable: false,
                create_only_enforced: true,
            },
            redactions: vec![],
        }
    }

    #[test]
    fn baseline_is_valid() {
        valid_scorecard()
            .validate_invariants()
            .expect("the test baseline itself must satisfy every invariant");
    }

    // --- Global Constraint 18(a), true branch -----------------------------

    #[test]
    fn captured_by_logweir_true_rejects_last_phase_completed_below_neg1() {
        let mut sc = valid_scorecard();
        sc.source.captured_by_logweir = true;
        sc.last_phase_completed = -2;
        // Needed so the true-branch's OTHER checks would pass if reached —
        // isolates the failure to the last_phase_completed arm specifically.
        sc.measured.rpo_source_relative_seconds = Some(0);
        sc.measured.rpo_source_relative_unmeasured_reason = None;
        let err = sc
            .validate_invariants()
            .expect_err("last_phase_completed below -1 must be rejected");
        assert_eq!(
            err.0,
            "source.captured_by_logweir is true but last_phase_completed is below -1"
        );
    }

    #[test]
    fn captured_by_logweir_true_requires_measured_source_relative_rpo() {
        let mut sc = valid_scorecard();
        sc.source.captured_by_logweir = true;
        sc.last_phase_completed = 9;
        sc.measured.rpo_source_relative_seconds = None;
        sc.measured.rpo_source_relative_unmeasured_reason = None;
        let err = sc.validate_invariants().expect_err(
            "a null rpo_source_relative_seconds must be rejected when captured_by_logweir is true",
        );
        assert_eq!(
            err.0,
            "source.captured_by_logweir is true but rpo_source_relative_seconds is null"
        );
    }

    #[test]
    fn captured_by_logweir_true_rejects_a_lingering_unmeasured_reason() {
        let mut sc = valid_scorecard();
        sc.source.captured_by_logweir = true;
        sc.last_phase_completed = 9;
        sc.measured.rpo_source_relative_seconds = Some(0);
        sc.measured.rpo_source_relative_unmeasured_reason = Some("stale reason".into());
        let err = sc.validate_invariants().expect_err(
            "a non-null unmeasured reason must be rejected when captured_by_logweir is true",
        );
        assert_eq!(
            err.0,
            "source.captured_by_logweir is true but an unmeasured reason is present"
        );
    }

    // --- Global Constraint 18(a), false branch ------------------------------

    #[test]
    fn captured_by_logweir_false_rejects_a_source_relative_rpo() {
        let mut sc = valid_scorecard();
        sc.source.captured_by_logweir = false;
        sc.measured.rpo_source_relative_seconds = Some(0);
        // Left as Some(...) so, if the seconds arm were deleted, the reason
        // arm below would not incidentally catch this case too.
        sc.measured.rpo_source_relative_unmeasured_reason =
            Some("source cluster never contacted".into());
        let err = sc
            .validate_invariants()
            .expect_err("a source-relative RPO with the source never contacted must be rejected");
        assert_eq!(
            err.0,
            "rpo_source_relative_seconds is set but the source was never contacted"
        );
    }

    #[test]
    fn captured_by_logweir_false_requires_an_unmeasured_reason() {
        let mut sc = valid_scorecard();
        sc.source.captured_by_logweir = false;
        sc.measured.rpo_source_relative_seconds = None;
        sc.measured.rpo_source_relative_unmeasured_reason = None;
        let err = sc.validate_invariants().expect_err(
            "a null unmeasured reason must be rejected when captured_by_logweir is false",
        );
        assert_eq!(
            err.0,
            "source.captured_by_logweir is false but rpo_source_relative_unmeasured_reason is null"
        );
    }

    // --- Finiteness of the format's two float fields ------------------------

    #[test]
    fn objectives_pass_rate_must_be_finite() {
        let mut sc = valid_scorecard();
        sc.objectives.pass_rate = Some(f64::NAN);
        let err = sc.validate_invariants().expect_err(
            "a NaN objectives.pass_rate must be rejected before it can be signed as a bare null",
        );
        assert_eq!(err.0, "objectives.pass_rate is not finite (NaN or +/-Inf)");
    }

    #[test]
    fn integrity_pass_rate_measured_must_be_finite() {
        let mut sc = valid_scorecard();
        // records_sampled_matching / records_sampled with records_sampled == 0
        // is exactly the reachable trigger: a drill that samples zero records.
        sc.integrity.pass_rate_measured = Some(f64::NAN);
        let err = sc.validate_invariants().expect_err(
            "a NaN integrity.pass_rate_measured must be rejected before it can be signed as a bare null",
        );
        assert_eq!(
            err.0,
            "integrity.pass_rate_measured is not finite (NaN or +/-Inf)"
        );
    }

    // --- Global Constraint 12: refuse a higher-major format_version --------

    #[test]
    fn format_version_with_a_higher_major_is_refused() {
        let mut sc = valid_scorecard();
        sc.format_version = "9.9.9".into();
        let err = sc
            .validate_invariants()
            .expect_err("a format_version from a future major must be refused");
        assert_eq!(
            err.0,
            "format_version 9.9.9 has a major version newer than this reader understands \
             (this build knows 1.0.0)"
        );
    }

    #[test]
    fn format_version_with_a_lower_or_equal_major_is_accepted() {
        let mut sc = valid_scorecard();
        sc.format_version = "1.9.9".into();
        sc.validate_invariants()
            .expect("a same-major minor/patch bump must not be refused");
        sc.format_version = "0.9.9".into();
        sc.validate_invariants()
            .expect("an older major must not be refused by this check");
    }

    #[test]
    fn format_version_that_does_not_parse_is_refused() {
        let mut sc = valid_scorecard();
        sc.format_version = "not-a-semver".into();
        let err = sc
            .validate_invariants()
            .expect_err("an unparseable format_version must be refused, not treated as major 0");
        assert_eq!(
            err.0,
            "format_version \"not-a-semver\" is not a parseable semver"
        );
    }
}
