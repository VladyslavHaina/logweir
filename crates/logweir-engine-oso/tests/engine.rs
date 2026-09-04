//! Tests `impl DataEngine for OsoCliEngine` against the fixture stubs under
//! `e2e/fixtures/fake-engine*.sh` — the real, Docker-extracted `kafka-backup`
//! binary (Task 13) does not exist on this machine (broken daemon proxy), so
//! these are what stand in for it. See task-12-report.md's "what remains
//! unproven" section for exactly what these tests cannot cover.
use logweir_core::engine::{
    BackupSetRef, CoverageState, DataEngine, PhaseObserver, RestorePlan, SampleSelection,
    StorageUrl,
};
use logweir_engine_oso::engine::OsoCliEngine;
use logweir_engine_oso::storage::Store;
use std::path::{Path, PathBuf};

fn unique_dir(tag: &str) -> PathBuf {
    let d = std::env::temp_dir().join(format!(
        "logweir-engine-test-{tag}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&d).unwrap();
    d
}

fn plan() -> RestorePlan {
    RestorePlan {
        set: BackupSetRef {
            backup_id: "b1".into(),
            manifest_key: "b1/manifest.json".into(),
        },
        storage: StorageUrl::Filesystem {
            path: "/archive".into(),
        },
        target_bootstrap: vec!["kafka-broker-1:9092".into()],
        topic_mapping: [("orders".to_string(), "drill-20260903-orders".to_string())]
            .into_iter()
            .collect(),
        time_window: (
            "2026-08-29T00:00:00Z".parse().unwrap(),
            "2026-08-30T02:00:00Z".parse().unwrap(),
        ),
        default_replication_factor: 1,
        checkpoint_state: "/var/lib/logweir/checkpoint.json".into(),
        checkpoint_interval_secs: 30,
    }
}

#[derive(Default)]
struct RecordingObserver {
    lines: Vec<(String, String)>,
    phases: Vec<(i8, String)>,
}

impl PhaseObserver for RecordingObserver {
    fn phase_started(&mut self, phase: i8, name: &str) {
        self.phases.push((phase, format!("started:{name}")));
    }
    fn phase_finished(&mut self, phase: i8, outcome: &str) {
        self.phases.push((phase, format!("finished:{outcome}")));
    }
    fn engine_line(&mut self, stream: &str, line: &str) {
        self.lines.push((stream.to_string(), line.to_string()));
    }
}

fn engine_with(binary: &str, store: Store) -> OsoCliEngine {
    OsoCliEngine::new(
        PathBuf::from(binary),
        "v0.21.0-test".into(),
        "sha256:testdigest".into(),
        unique_dir("workdir"),
        store,
    )
}

#[test]
fn id_reports_the_configured_version_and_digest() {
    let engine = engine_with(
        "../../e2e/fixtures/fake-engine-clean.sh",
        Store::in_memory("logweir"),
    );
    let id = engine.id();
    assert_eq!(id.id, "oso-cli");
    assert_eq!(id.version, "v0.21.0-test");
    assert_eq!(id.digest, "sha256:testdigest");
}

// --- preflight() ---

#[test]
fn preflight_against_a_clean_engine_is_valid_and_honoured_with_no_warnings() {
    let engine = engine_with(
        "../../e2e/fixtures/fake-engine-clean.sh",
        Store::in_memory("logweir"),
    );
    let report = engine.preflight(&plan()).unwrap();
    assert!(report.valid);
    assert!(report.errors.is_empty());
    assert!(report.header_preflight_honoured);
    assert!(report.unknown_key_warnings.is_empty());
    assert_eq!(report.partitions.len(), 1);
    assert_eq!(report.partitions[0].state, CoverageState::Full);
    assert!(report.partitions[0].detail.contains("scanned=2"));
}

/// `render_restore::render` unconditionally writes `header_preflight: full` —
/// an engine that warns it ignored `restore.header_preflight` is below the
/// declared floor and `preflight()` must abort rather than report a false
/// "clean" result. This is the brief's own default fixture (no env vars): its
/// unconditional warning is `restore.header_preflight`, which IS a key we
/// always render.
#[test]
fn preflight_aborts_when_the_engine_drops_a_key_logweir_rendered() {
    let engine = engine_with(
        "../../e2e/fixtures/fake-engine.sh",
        Store::in_memory("logweir"),
    );
    let err = engine.preflight(&plan()).unwrap_err();
    let msg = err.to_string();
    assert!(msg.contains("restore.header_preflight"), "{msg}");
    assert!(msg.contains("ignored"), "{msg}");
}

/// Same abort, different rendered key (`dry_run_check_segments`, also
/// unconditional) and the warning on stdout instead of stderr — proves the
/// leaf-matching is not hardcoded to a single key name.
#[test]
fn preflight_aborts_on_a_different_dropped_key_too() {
    let engine = engine_with(
        "../../e2e/fixtures/fake-engine-dropped-key.sh",
        Store::in_memory("logweir"),
    );
    let err = engine.preflight(&plan()).unwrap_err();
    assert!(err.to_string().contains("restore.dry_run_check_segments"));
}

#[test]
fn preflight_reports_operational_error_on_malformed_stdout() {
    let engine = engine_with(
        "../../e2e/fixtures/fake-engine-fail.sh",
        Store::in_memory("logweir"),
    );
    let err = engine.preflight(&plan()).unwrap_err();
    assert!(err.to_string().contains("printed no JSON object"));
}

/// Proves the argv wiring end to end: `preflight()` writes `restore.yaml` to
/// its workdir and the STUB independently verifies (by reading the file at
/// the `--config` VALUE it received) that the path resolves to that same
/// rendered document. A wrong flag, a stale path, or a config that was never
/// written would make the stub exit 42 instead of returning its clean report.
#[test]
fn preflight_points_config_at_the_file_it_just_wrote() {
    let engine = engine_with(
        "../../e2e/fixtures/fake-engine-argv-check.sh",
        Store::in_memory("logweir"),
    );
    let report = engine.preflight(&plan()).unwrap();
    assert!(report.valid);
}

// --- restore() ---

#[test]
fn restore_against_a_clean_engine_succeeds_with_no_warnings() {
    let engine = engine_with(
        "../../e2e/fixtures/fake-engine-clean.sh",
        Store::in_memory("logweir"),
    );
    let mut obs = RecordingObserver::default();
    let facts = engine.restore(&plan(), &mut obs).unwrap();
    assert_eq!(facts.exit_code, 0);
    assert!(facts.unknown_key_warnings.is_empty());
    assert!(facts.finished_at >= facts.started_at);
    assert!(obs
        .lines
        .iter()
        .any(|(s, l)| s == "stdout" && l.contains("Starting restore")));
}

/// Exit-code mapping for the failure case: the stub's chosen non-zero code
/// and its stderr text must both surface in the returned `Err`.
#[test]
fn restore_maps_a_nonzero_exit_code_to_an_operational_error() {
    let engine = engine_with(
        "../../e2e/fixtures/fake-engine-fail.sh",
        Store::in_memory("logweir"),
    );
    let mut obs = RecordingObserver::default();
    let err = engine.restore(&plan(), &mut obs).unwrap_err();
    let msg = err.to_string();
    assert!(msg.contains("exited 3"), "{msg}");
    assert!(msg.contains("target broker unreachable"), "{msg}");
}

#[test]
fn restore_points_config_at_the_file_it_just_wrote() {
    let engine = engine_with(
        "../../e2e/fixtures/fake-engine-argv-check.sh",
        Store::in_memory("logweir"),
    );
    let mut obs = RecordingObserver::default();
    let facts = engine.restore(&plan(), &mut obs).unwrap();
    assert_eq!(facts.exit_code, 0);
}

// --- list_backup_sets() / describe() / fingerprints(), against a Store built
// with Store::read_only_from_url — proving the archive is read through a
// handle that physically cannot put (controller amendment), not merely one
// that happens not to call put in these tests. ---

const NONE_KBAK_RELATIVE: &str = "b1/topics/orders/partition=0/segment-00000000000000000100.bin";

fn seed_archive(dir: &Path) {
    let manifest = format!(
        r#"{{
  "backup_id": "b1",
  "created_at": 1756425600000,
  "source_cluster_id": "cluster-a",
  "source_brokers": ["broker1:9092"],
  "compression": "zstd",
  "topics": [
    {{
      "name": "orders",
      "original_partition_count": 1,
      "source_replication_factor": 3,
      "configurations": {{"retention.ms": "604800000"}},
      "partitions": [
        {{
          "partition_id": 0,
          "segments": [
            {{
              "key": "{NONE_KBAK_RELATIVE}",
              "start_offset": 100,
              "end_offset": 104,
              "start_timestamp": 1756425600000,
              "end_timestamp": 1756425600004,
              "record_count": 5,
              "sha256": "deadbeef",
              "uploaded_at": 1756425600005
            }}
          ],
          "gaps": [],
          "pruned": []
        }}
      ]
    }}
  ]
}}"#
    );
    let manifest_path = dir.join("b1/manifest.json");
    std::fs::create_dir_all(manifest_path.parent().unwrap()).unwrap();
    std::fs::write(&manifest_path, manifest).unwrap();

    let snapshot = r#"{"backup_id":"b1","captured_at":1756425600000,"groups":[]}"#;
    std::fs::write(dir.join("b1/consumer-groups-snapshot.json"), snapshot).unwrap();

    let segment_path = dir.join(NONE_KBAK_RELATIVE);
    std::fs::create_dir_all(segment_path.parent().unwrap()).unwrap();
    std::fs::copy("../../e2e/fixtures/segments/none.kbak", &segment_path).unwrap();

    // A second backup set, a different topic, with NO consumer-groups
    // snapshot sibling — exercises list_backup_sets' count and describe()'s
    // "absent is normal, not an error" path.
    let manifest2 = r#"{
  "backup_id": "b2",
  "created_at": 1756425600100,
  "topics": [
    {"name": "payments", "partitions": [{"partition_id": 0, "segments": []}]}
  ]
}"#;
    let manifest2_path = dir.join("b2/manifest.json");
    std::fs::create_dir_all(manifest2_path.parent().unwrap()).unwrap();
    std::fs::write(&manifest2_path, manifest2).unwrap();
}

#[test]
fn the_archive_is_read_through_a_store_that_cannot_physically_put() {
    let dir = unique_dir("archive");
    seed_archive(&dir);
    let loc = StorageUrl::Filesystem { path: dir.clone() };
    let store = Store::read_only_from_url(&loc).unwrap();

    // Sanity: this handle really cannot put, proving list_backup_sets/
    // describe below go through a handle that is read-only by construction,
    // not merely unused-for-writes by coincidence.
    assert!(store.put_create_only("logweir/x.json", b"{}").is_err());

    let engine = engine_with("../../e2e/fixtures/fake-engine-clean.sh", store);

    let sets = engine.list_backup_sets(&loc).unwrap();
    let mut ids: Vec<&str> = sets.iter().map(|s| s.backup_id.as_str()).collect();
    ids.sort();
    assert_eq!(ids, vec!["b1", "b2"]);

    let b1 = sets.iter().find(|s| s.backup_id == "b1").unwrap();
    let facts = engine.describe(b1).unwrap();
    assert_eq!(facts.backup_id, "b1");
    assert_eq!(facts.source_cluster_id.as_deref(), Some("cluster-a"));
    assert!(facts.consumer_group_snapshot_present());
    assert_eq!(facts.topics.len(), 1);
    assert_eq!(facts.topics[0].name, "orders");
    assert_eq!(
        facts.topics[0].partitions[0].segments[0].key,
        NONE_KBAK_RELATIVE
    );

    let b2 = sets.iter().find(|s| s.backup_id == "b2").unwrap();
    let facts2 = engine.describe(b2).unwrap();
    assert!(!facts2.consumer_group_snapshot_present());

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn fingerprints_decodes_segments_in_the_window_and_sorts_by_offset() {
    let dir = unique_dir("fp");
    seed_archive(&dir);
    let loc = StorageUrl::Filesystem { path: dir.clone() };
    let store = Store::read_only_from_url(&loc).unwrap();
    let engine = engine_with("../../e2e/fixtures/fake-engine-clean.sh", store);

    // none.kbak carries 5 records at timestamps 1_756_425_600_000..=...004
    // (offsets 100..104); this window keeps only 101, 102, 103.
    let sel = SampleSelection {
        topic: "orders".into(),
        partition: 0,
        anchor: "head".into(),
        count: 10,
        window: (1_756_425_600_001, 1_756_425_600_003),
    };
    let fps = engine.fingerprints(&sel).unwrap();
    assert_eq!(
        fps.iter().map(|f| f.offset).collect::<Vec<_>>(),
        vec![101, 102, 103]
    );
    assert!(fps.windows(2).all(|w| w[0].offset < w[1].offset));
    for f in &fps {
        assert_eq!(f.topic, "orders");
        assert_eq!(f.partition, 0);
        assert_eq!(
            f.sha256.len(),
            64,
            "expected a raw hex sha256: {}",
            f.sha256
        );
    }

    let _ = std::fs::remove_dir_all(&dir);
}

/// Documents, with a real failing-in-spirit reproduction, the cross-set
/// merging risk called out in task-12-report.md: `SampleSelection` carries no
/// backup-set id, and `Store::segment_keys_for` (Task 12b) resolves
/// topic/partition against EVERY manifest under the store's prefix. Two
/// backup sets sharing a topic/partition with overlapping windows have their
/// segments merged with no way, at this trait boundary, to tell them apart.
#[test]
fn fingerprints_merges_segments_across_backup_sets_sharing_a_topic_and_partition() {
    let dir = unique_dir("fp-merge");
    // b1: orders/0, offsets 100..104 (none.kbak).
    let b1_manifest = format!(
        r#"{{"backup_id":"b1","created_at":0,"topics":[{{"name":"orders","partitions":[{{"partition_id":0,"segments":[
        {{"key":"{NONE_KBAK_RELATIVE}","start_offset":100,"end_offset":104,"start_timestamp":1756425600000,"end_timestamp":1756425600004,"record_count":5}}
        ]}}]}}]}}"#
    );
    std::fs::create_dir_all(dir.join("b1")).unwrap();
    std::fs::write(dir.join("b1/manifest.json"), b1_manifest).unwrap();
    let seg1 = dir.join(NONE_KBAK_RELATIVE);
    std::fs::create_dir_all(seg1.parent().unwrap()).unwrap();
    std::fs::copy("../../e2e/fixtures/segments/none.kbak", &seg1).unwrap();

    // b3: SAME topic/partition, overlapping window, a DIFFERENT segment file
    // (zstd.kbak, which decodes to the identical 5 offsets/timestamps —
    // different compression, same logical records).
    const ZSTD_RELATIVE: &str = "b3/topics/orders/partition=0/segment-00000000000000000100.bin";
    let b3_manifest = format!(
        r#"{{"backup_id":"b3","created_at":0,"topics":[{{"name":"orders","partitions":[{{"partition_id":0,"segments":[
        {{"key":"{ZSTD_RELATIVE}","start_offset":100,"end_offset":104,"start_timestamp":1756425600000,"end_timestamp":1756425600004,"record_count":5}}
        ]}}]}}]}}"#
    );
    std::fs::create_dir_all(dir.join("b3")).unwrap();
    std::fs::write(dir.join("b3/manifest.json"), b3_manifest).unwrap();
    let seg3 = dir.join(ZSTD_RELATIVE);
    std::fs::create_dir_all(seg3.parent().unwrap()).unwrap();
    std::fs::copy("../../e2e/fixtures/segments/zstd.kbak", &seg3).unwrap();

    let loc = StorageUrl::Filesystem { path: dir.clone() };
    let store = Store::read_only_from_url(&loc).unwrap();
    let engine = engine_with("../../e2e/fixtures/fake-engine-clean.sh", store);

    let sel = SampleSelection {
        topic: "orders".into(),
        partition: 0,
        anchor: "head".into(),
        count: 10,
        window: (1_756_425_600_000, 1_756_425_600_004),
    };
    let fps = engine.fingerprints(&sel).unwrap();
    // 10, not 5: b1's and b3's segments both matched "orders"/0 with an
    // overlapping window and were merged into one result set, exactly the
    // ambiguity flagged in the report — a caller cannot tell from `fps` alone
    // which backup set each fingerprint came from.
    assert_eq!(fps.len(), 10);

    let _ = std::fs::remove_dir_all(&dir);
}
