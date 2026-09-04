// crates/logweir-core/examples/emit_fixture.rs
use base64::{engine::general_purpose::STANDARD as B64, Engine};
use logweir_core::det_json::to_deterministic_json;
use logweir_core::ids::sha256_prefixed;
use logweir_core::outcome::*;
use logweir_core::scorecard::*;

fn t(s: &str) -> chrono::DateTime<chrono::Utc> {
    chrono::DateTime::parse_from_rfc3339(s)
        .unwrap()
        .with_timezone(&chrono::Utc)
}

fn main() {
    // The literal engine sub-report bytes an auditor would find in the bucket.
    // Held as BYTES, never as a parsed value, for the reason in EngineSubreport.
    let sub: &[u8] = br#"{"schema_version":"1.0.0","report_id":"01J9X2QK7C4V0R8YB3ZP6MTS5A"}"#;

    let sc = Scorecard {
        format_version: logweir_core::FORMAT_VERSION.to_string(),
        run_id: "01J9X2QK7C4V0R8YB3ZP6MTS5A".into(),
        outcome: Outcome::Pass,
        last_phase_completed: 9,
        requested_at: t("2026-09-03T09:00:00Z"),
        approval_validated_at: Some(t("2026-09-03T09:00:30Z")),
        triggered_by: Some("KPMG Q3".into()),
        engine: EngineInfo {
            id: "oso-cli".into(),
            version: "v0.21.0".into(),
            digest: "sha256:0000000000000000000000000000000000000000000000000000000000000000"
                .into(),
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
            backup_id: "backup-2026-08-30T02:00:00Z".into(),
            manifest_sha256: sha256_prefixed(b"manifest"),
            manifest_version_id: None,
            captured_by_logweir: false,
        },
        target: TargetInfo {
            cluster_id: "MkU3OEVBNTcwNTJENDM2Qk".into(),
            marker_topic: "logweir.scratch".into(),
            topic_mapping_prefix: "drill-".into(),
            topic_mapping_sha256: sha256_prefixed(b"orders: drill-orders\n"),
            topic_mapping_entries: 1,
        },
        approval: ApprovalInfo {
            approver: "sre-oncall@example.com".into(),
            ticket: "CHG-40881".into(),
            plan_hash: sha256_prefixed(b"drill.yaml"),
            approved_at: t("2026-09-02T17:40:00Z"),
            key_id: "a".repeat(64),
            self_attested: false,
        },
        phases: vec![PhaseRecord {
            phase: 0,
            name: "admit".into(),
            at: t("2026-09-03T09:00:00Z"),
            outcome: "admitted".into(),
            duration_ms: 41,
            // Empty, not `vec!["...".into()]`: this field is
            // `skip_serializing_if`-omitted when empty specifically so this
            // checked-in, signed fixture's bytes (and therefore its
            // signature) are unaffected by the field's addition in Task 17
            // fix round 1 — see `PhaseRecord::notes`'s doc comment.
            notes: vec![],
        }],
        measured: Measured {
            rto_seconds: Some(512),
            rto_requested_to_verified_seconds: Some(542),
            rto_restore_only_seconds: Some(214),
            rto_excluding_preflight_seconds: Some(300),
            rpo_seconds: Some(0),
            rpo_source_relative_seconds: None,
            rpo_source_relative_unmeasured_reason: Some("source cluster never contacted".into()),
        },
        objectives: Objectives {
            rto_seconds: Some(900),
            rpo_seconds: Some(300),
            pass_rate: Some(1.0),
            met: Some(true),
        },
        sample: SampleInfo {
            window_start: t("2026-08-29T00:00:00Z"),
            window_end: t("2026-08-30T02:00:00Z"),
            topics: 1,
            partitions: 3,
            records_expected: 75,
            records_restored: 75,
            anchor: "head".into(),
            coverage_note: "no capture gap overlaps the sampled window".into(),
        },
        target_diff: TargetDiffSummary {
            collisions: vec![],
            // Empty, not `vec!["drill-orders".into()]`: this field is
            // `skip_serializing_if`-omitted when empty specifically so this
            // checked-in, signed fixture's bytes (and therefore its
            // signature) are unaffected by the field's addition in Task 16
            // fix round 1 — see `TargetDiffSummary::absent`'s doc comment.
            absent: vec![],
            would_create: vec![("drill-orders".to_string(), 3)],
            level: "full".into(),
        },
        integrity: Integrity {
            level: IntegrityLevel::ByteFingerprint,
            result: IntegrityResult::Pass,
            partial_reason: None,
            records_sampled: 75,
            records_sampled_matching: 75,
            mismatches: 0,
            pass_rate_measured: Some(1.0),
            restored_principal_could_consume: None,
        },
        topic_parity: TopicParity {
            intentionally_deviated: vec!["cleanup.policy".into(), "retention.ms".into()],
            unexpected_divergence: vec![],
        },
        engine_subreport: Some(EngineSubreport {
            retained_verbatim: true,
            retrieved_from: "logweir/01J9X2QK7C4V0R8YB3ZP6MTS5A/engine-validation".into(),
            caveat: "The engine's own integrity.checksums_valid is a hardcoded constant \
                     true and its restore start_time/end_time/duration_seconds are all \
                     null; this sub-report corroborates nothing Logweir claims."
                .into(),
            body_b64: B64.encode(sub),
            body_sha256: sha256_prefixed(sub),
        }),
        evidence: EvidenceInfo {
            version_id: None,
            retain_until: None,
            immutable: false,
            create_only_enforced: true,
        },
        redactions: vec![],
    };
    sc.validate_invariants()
        .expect("the shipped fixture must satisfy its own invariants");
    print!(
        "{}",
        String::from_utf8(to_deterministic_json(&sc).unwrap()).unwrap()
    );
}
