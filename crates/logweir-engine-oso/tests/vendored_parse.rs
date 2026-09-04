use logweir_engine_oso::vendored::manifest::{BackupManifest, DryRunReport};
use logweir_engine_oso::vendored::preflight::PartitionCoverageState;

/// A mixed-version bucket must never crash a scan: 0.17 manifests have no
/// `pruned`, and pre-0.21 segments have neither `sha256` nor `uploaded_at`
/// [VERIFIED U/kafka-backup/crates/kafka-backup-core/src/manifest.rs:376-386].
#[test]
fn manifests_from_three_versions_all_parse() {
    for f in ["0.17", "0.19.2", "0.21"] {
        let raw =
            std::fs::read_to_string(format!("../../e2e/fixtures/manifests/{f}.json")).unwrap();
        let m: BackupManifest = serde_json::from_str(&raw).unwrap_or_else(|e| panic!("{f}: {e}"));
        assert!(!m.backup_id.is_empty());
        assert!(!m.topics.is_empty());
    }
}

#[test]
fn a_pre_0_21_segment_reads_back_with_empty_sha256_and_zero_uploaded_at() {
    let raw = std::fs::read_to_string("../../e2e/fixtures/manifests/0.17.json").unwrap();
    let m: BackupManifest = serde_json::from_str(&raw).unwrap();
    let s = &m.topics[0].partitions[0].segments[0];
    assert_eq!(s.sha256, "");
    assert_eq!(s.uploaded_at, 0);
}

#[test]
fn an_unknown_manifest_field_is_kept_in_the_catch_all_not_dropped() {
    let raw = r#"{"backup_id":"b","created_at":1,"topics":[],"a_future_field":{"x":1}}"#;
    let m: BackupManifest = serde_json::from_str(raw).unwrap();
    assert!(m.extra.contains_key("a_future_field"));
}

#[test]
fn dry_run_report_round_trips_including_header_preflight() {
    let raw = std::fs::read_to_string("../../e2e/fixtures/dryrun/data-missing.json").unwrap();
    let r: DryRunReport = serde_json::from_str(&raw).unwrap();
    assert!(!r.valid);
    let hp = r.header_preflight.expect("header_preflight present");
    assert_eq!(hp.mode, "full");
    assert!(hp.scan_performed);
    assert!(hp
        .partitions
        .iter()
        .any(|p| p.state == PartitionCoverageState::DataMissing));
}

#[test]
fn an_unknown_coverage_state_degrades_to_unknown_carrying_the_raw_string() {
    let v: PartitionCoverageState = serde_json::from_str("\"quantum_superposition\"").unwrap();
    assert_eq!(
        v,
        PartitionCoverageState::Unknown("quantum_superposition".into())
    );
}

#[test]
fn the_sibling_consumer_groups_snapshot_parses_and_keeps_unknown_fields() {
    use logweir_engine_oso::vendored::consumer_groups::ConsumerGroupsSnapshot;
    let raw = std::fs::read_to_string("../../e2e/fixtures/consumer-groups-snapshot.json").unwrap();
    let s: ConsumerGroupsSnapshot = serde_json::from_str(&raw).unwrap();
    assert_eq!(s.groups.len(), 2);
    assert!(s.extra.contains_key("a_future_field"));
}
