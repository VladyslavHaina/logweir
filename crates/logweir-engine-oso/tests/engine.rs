//! Tests `impl DataEngine for OsoCliEngine` against the fixture stubs under
//! `e2e/fixtures/fake-engine*.sh` — the real, Docker-extracted `kafka-backup`
//! binary (Task 13) does not exist on this machine (broken daemon proxy), so
//! these are what stand in for it. See task-12-report.md's "what remains
//! unproven" section for exactly what these tests cannot cover.
use logweir_core::engine::{
    BackupSetRef, CoverageState, DataEngine, EngineError, PhaseObserver, RestorePlan,
    SampleSelection, StorageUrl,
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

/// Fix 3 (post-review): `describe()`'s sibling-snapshot read used to
/// collapse EVERY `get` failure into `None` ("no snapshot"), so a permission
/// error, a timeout, or a truncated read was recorded identically to genuine
/// absence — `consumer_group_snapshot_sha256: None` then flows into the
/// signed scorecard as a positive claim the store never actually
/// established. This constructs a sibling file that EXISTS but cannot be
/// read (mode 000) and asserts `describe()` propagates the failure rather
/// than reporting `None`.
///
/// Assumes a non-root test runner: root bypasses Unix permission bits
/// entirely, which would make the constructed failure never actually occur
/// and this assertion vacuous. True for `cargo test` in this environment and
/// for ordinary (non-containerized-as-root) CI runners.
#[test]
#[cfg(unix)]
fn describe_reports_an_error_when_the_snapshot_sibling_is_unreadable_not_none() {
    use std::os::unix::fs::PermissionsExt;

    let dir = unique_dir("unreadable-snapshot");
    let manifest = r#"{"backup_id":"u1","created_at":0,"topics":[]}"#;
    std::fs::create_dir_all(dir.join("u1")).unwrap();
    std::fs::write(dir.join("u1/manifest.json"), manifest).unwrap();
    let sibling = dir.join("u1/consumer-groups-snapshot.json");
    std::fs::write(&sibling, b"{}").unwrap();
    std::fs::set_permissions(&sibling, std::fs::Permissions::from_mode(0o000)).unwrap();

    let loc = StorageUrl::Filesystem { path: dir.clone() };
    let store = Store::read_only_from_url(&loc).unwrap();
    let engine = engine_with("../../e2e/fixtures/fake-engine-clean.sh", store);

    let set = BackupSetRef {
        backup_id: "u1".into(),
        manifest_key: "u1/manifest.json".into(),
    };
    let err = engine.describe(&set).unwrap_err();
    // Confirms this propagated through StoreError::Io (Display: "storage:
    // {0}"), not a coincidental failure elsewhere in describe() (e.g. the
    // primary manifest read, which must have already succeeded for this
    // point to be reached at all).
    assert!(err.to_string().contains("storage:"), "{err}");

    // Restore permissions so the temp dir can be cleaned up.
    std::fs::set_permissions(&sibling, std::fs::Permissions::from_mode(0o644)).unwrap();
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
    // (offsets 100..104); this window keeps only 101, 102, 103. count (10)
    // exceeds what is available, so the anchor choice does not matter here —
    // see the dedicated head/tail/random tests below for that.
    let sel = SampleSelection {
        set: b1_ref(),
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

fn b1_ref() -> BackupSetRef {
    BackupSetRef {
        backup_id: "b1".into(),
        manifest_key: "b1/manifest.json".into(),
    }
}

/// Fix (post-review): the ambiguity `task-12-report.md` documented — two
/// backup sets sharing a topic/partition with an overlapping window used to
/// merge their segments in `fingerprints()`, with no way at the trait
/// boundary to tell them apart. That made the archive side a strict
/// SUPERSET of what a real restore populated: a healthy restore reads as a
/// mismatch (extra fingerprints), which is a false FAIL on a signed
/// attestation — the worst direction for this product. `SampleSelection` now
/// carries `set: BackupSetRef`, and `fingerprints()` resolves against exactly
/// that one manifest (`Store::segment_keys_for_set`). This test is the
/// SAME two-backup-set setup as before the fix, with the SAME assertion
/// inverted: 5, not 10 — b3's segments, though they match topic/partition/
/// window, are no longer visible when the request names b1.
#[test]
fn fingerprints_are_scoped_to_the_requested_set_not_merged_across_sets() {
    let dir = unique_dir("fp-scoped");
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
    // different compression, same logical records). Still present in the
    // archive; the point of this test is that it must NOT be visible when
    // the request names b1.
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
        set: b1_ref(),
        topic: "orders".into(),
        partition: 0,
        anchor: "head".into(),
        count: 10,
        window: (1_756_425_600_000, 1_756_425_600_004),
    };
    let fps = engine.fingerprints(&sel).unwrap();
    // 5, not 10: b3's segments matched topic/partition/window too, under the
    // pre-fix behaviour, but the request named b1's manifest specifically.
    assert_eq!(fps.len(), 5);
    assert_eq!(
        fps.iter().map(|f| f.offset).collect::<Vec<_>>(),
        vec![100, 101, 102, 103, 104]
    );

    let _ = std::fs::remove_dir_all(&dir);
}

/// Hand-rolls a minimal, valid KBAK v1 segment (compression=none, no
/// headers) matching the exact byte layout `kbak.rs` decodes — see its own
/// SOURCE comment for the format. Local to this test file because proving
/// `fingerprints()` reads from the CORRECT backup set (not merely returns the
/// right COUNT) needs two segments that share offsets/timestamps but differ
/// in content, which none of the checked-in `.kbak` fixtures do (they exist
/// to prove decoding, not cross-set identity).
fn make_kbak_segment(records: &[(i64, i64, &[u8], &[u8])]) -> Vec<u8> {
    let mut body = Vec::new();
    for &(offset, timestamp, key, value) in records {
        let mut rec = Vec::new();
        rec.extend_from_slice(&timestamp.to_le_bytes());
        rec.extend_from_slice(&offset.to_le_bytes());
        rec.extend_from_slice(&(key.len() as i32).to_le_bytes());
        rec.extend_from_slice(key);
        rec.extend_from_slice(&(value.len() as i32).to_le_bytes());
        rec.extend_from_slice(value);
        rec.extend_from_slice(&0u16.to_le_bytes()); // header_count = 0
        let total_len = rec.len() as u32;
        body.extend_from_slice(&total_len.to_le_bytes());
        body.extend_from_slice(&rec);
    }
    let start_offset = records.first().map(|r| r.0).unwrap_or(0);
    let end_offset = records.last().map(|r| r.0).unwrap_or(0);
    let mut out = Vec::new();
    out.extend_from_slice(b"KBAK");
    out.push(1u8); // version
    out.push(0u8); // compression: none
    out.extend_from_slice(&[0u8, 0u8]); // reserved
    out.extend_from_slice(&(records.len() as u64).to_le_bytes());
    out.extend_from_slice(&start_offset.to_le_bytes());
    out.extend_from_slice(&end_offset.to_le_bytes());
    assert_eq!(out.len(), 32, "HEADER_SIZE is 32");
    out.extend_from_slice(&body);
    let crc = crc32fast::hash(&out);
    out.extend_from_slice(&crc.to_le_bytes());
    out.extend_from_slice(b"BKAE");
    out
}

/// The test the coordinator asked for directly: two backup sets carrying the
/// SAME offsets (200, 201) with DIFFERENT bytes. Proves `fingerprints()`
/// returns the requested set's OWN content, not merely a same-sized result —
/// scoping by manifest key, not by a coincidental record count.
#[test]
fn fingerprints_come_from_the_requested_set_when_two_sets_share_offsets_with_different_bytes() {
    let dir = unique_dir("fp-identity");
    const SEG_A: &str = "ba/topics/orders/partition=0/segment-00000000000000000200.bin";
    const SEG_B: &str = "bb/topics/orders/partition=0/segment-00000000000000000200.bin";
    let manifest_for = |backup_id: &str, key: &str| {
        format!(
            r#"{{"backup_id":"{backup_id}","created_at":0,"topics":[{{"name":"orders","partitions":[{{"partition_id":0,"segments":[
        {{"key":"{key}","start_offset":200,"end_offset":201,"start_timestamp":200,"end_timestamp":201,"record_count":2}}
        ]}}]}}]}}"#
        )
    };

    std::fs::create_dir_all(dir.join("ba")).unwrap();
    std::fs::write(dir.join("ba/manifest.json"), manifest_for("ba", SEG_A)).unwrap();
    let seg_a_path = dir.join(SEG_A);
    std::fs::create_dir_all(seg_a_path.parent().unwrap()).unwrap();
    std::fs::write(
        &seg_a_path,
        make_kbak_segment(&[(200, 200, b"kA0", b"vA0"), (201, 201, b"kA1", b"vA1")]),
    )
    .unwrap();

    std::fs::create_dir_all(dir.join("bb")).unwrap();
    std::fs::write(dir.join("bb/manifest.json"), manifest_for("bb", SEG_B)).unwrap();
    let seg_b_path = dir.join(SEG_B);
    std::fs::create_dir_all(seg_b_path.parent().unwrap()).unwrap();
    std::fs::write(
        &seg_b_path,
        make_kbak_segment(&[(200, 200, b"kB0", b"vB0"), (201, 201, b"kB1", b"vB1")]),
    )
    .unwrap();

    let loc = StorageUrl::Filesystem { path: dir.clone() };
    let store = Store::read_only_from_url(&loc).unwrap();
    let engine = engine_with("../../e2e/fixtures/fake-engine-clean.sh", store);

    let sel_a = SampleSelection {
        set: BackupSetRef {
            backup_id: "ba".into(),
            manifest_key: "ba/manifest.json".into(),
        },
        topic: "orders".into(),
        partition: 0,
        anchor: "head".into(),
        count: 10,
        window: (200, 201),
    };
    let sel_b = SampleSelection {
        set: BackupSetRef {
            backup_id: "bb".into(),
            manifest_key: "bb/manifest.json".into(),
        },
        ..sel_a.clone()
    };

    let fps_a = engine.fingerprints(&sel_a).unwrap();
    let fps_b = engine.fingerprints(&sel_b).unwrap();
    assert_eq!(
        fps_a.iter().map(|f| f.offset).collect::<Vec<_>>(),
        vec![200, 201]
    );
    assert_eq!(
        fps_b.iter().map(|f| f.offset).collect::<Vec<_>>(),
        vec![200, 201]
    );

    let expected_a0 =
        logweir_kafka::fingerprint::record_fingerprint(Some(b"kA0"), Some(b"vA0"), &[], 200);
    let expected_b0 =
        logweir_kafka::fingerprint::record_fingerprint(Some(b"kB0"), Some(b"vB0"), &[], 200);
    assert_eq!(fps_a[0].sha256, expected_a0);
    assert_eq!(fps_b[0].sha256, expected_b0);
    assert_ne!(
        fps_a[0].sha256, fps_b[0].sha256,
        "same offset, different bytes, must fingerprint differently"
    );

    let _ = std::fs::remove_dir_all(&dir);
}

// --- anchor / count semantics (Fix 2: previously read by nothing) ---

/// Builds a small, self-contained archive: topic "orders", partition 0, ONE
/// backup set ("hc" — "head/count"), 6 records at offsets 300..305 with
/// distinguishable content, so head/tail/random selections are each
/// unambiguous by offset.
fn seed_anchor_archive(dir: &std::path::Path) -> BackupSetRef {
    const SEG: &str = "hc/topics/orders/partition=0/segment-00000000000000000300.bin";
    let manifest = format!(
        r#"{{"backup_id":"hc","created_at":0,"topics":[{{"name":"orders","partitions":[{{"partition_id":0,"segments":[
        {{"key":"{SEG}","start_offset":300,"end_offset":305,"start_timestamp":300,"end_timestamp":305,"record_count":6}}
        ]}}]}}]}}"#
    );
    std::fs::create_dir_all(dir.join("hc")).unwrap();
    std::fs::write(dir.join("hc/manifest.json"), manifest).unwrap();
    let seg_path = dir.join(SEG);
    std::fs::create_dir_all(seg_path.parent().unwrap()).unwrap();
    let records: Vec<(i64, i64, &[u8], &[u8])> = (0..6)
        .map(|i| {
            let n = 300 + i;
            (
                n,
                n,
                KEYS[i as usize].as_bytes(),
                VALUES[i as usize].as_bytes(),
            )
        })
        .collect();
    std::fs::write(&seg_path, make_kbak_segment(&records)).unwrap();
    BackupSetRef {
        backup_id: "hc".into(),
        manifest_key: "hc/manifest.json".into(),
    }
}
const KEYS: [&str; 6] = ["k0", "k1", "k2", "k3", "k4", "k5"];
const VALUES: [&str; 6] = ["v0", "v1", "v2", "v3", "v4", "v5"];

fn anchor_sel(set: BackupSetRef, anchor: &str, count: usize) -> SampleSelection {
    SampleSelection {
        set,
        topic: "orders".into(),
        partition: 0,
        anchor: anchor.into(),
        count,
        window: (300, 305),
    }
}

#[test]
fn head_anchor_returns_the_earliest_count_records() {
    let dir = unique_dir("anchor-head");
    let set = seed_anchor_archive(&dir);
    let loc = StorageUrl::Filesystem { path: dir.clone() };
    let engine = engine_with(
        "../../e2e/fixtures/fake-engine-clean.sh",
        Store::read_only_from_url(&loc).unwrap(),
    );
    let fps = engine.fingerprints(&anchor_sel(set, "head", 3)).unwrap();
    assert_eq!(
        fps.iter().map(|f| f.offset).collect::<Vec<_>>(),
        vec![300, 301, 302]
    );
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn tail_anchor_returns_the_latest_count_records() {
    let dir = unique_dir("anchor-tail");
    let set = seed_anchor_archive(&dir);
    let loc = StorageUrl::Filesystem { path: dir.clone() };
    let engine = engine_with(
        "../../e2e/fixtures/fake-engine-clean.sh",
        Store::read_only_from_url(&loc).unwrap(),
    );
    let fps = engine.fingerprints(&anchor_sel(set, "tail", 3)).unwrap();
    assert_eq!(
        fps.iter().map(|f| f.offset).collect::<Vec<_>>(),
        vec![303, 304, 305]
    );
    let _ = std::fs::remove_dir_all(&dir);
}

/// "random" is a deterministic, evenly-spaced sample across the full sorted
/// set (see `select_sample`'s doc comment in engine.rs for why): the first
/// and last picks are always the earliest and latest available record
/// (`idx(0) == 0` and `idx(count-1) == n-1` by construction), which is the
/// property that matters — the sample spans the whole window rather than
/// clustering at one end — not that it lands on any particular offset in
/// between. 6 records, count 3: idx = i*5/2 for i in 0,1,2 -> 0, 2, 5.
#[test]
fn random_anchor_returns_a_bounded_evenly_spaced_sample() {
    let dir = unique_dir("anchor-random");
    let set = seed_anchor_archive(&dir);
    let loc = StorageUrl::Filesystem { path: dir.clone() };
    let engine = engine_with(
        "../../e2e/fixtures/fake-engine-clean.sh",
        Store::read_only_from_url(&loc).unwrap(),
    );
    let fps = engine.fingerprints(&anchor_sel(set, "random", 3)).unwrap();
    let offsets: Vec<i64> = fps.iter().map(|f| f.offset).collect();
    assert_eq!(offsets, vec![300, 302, 305]);
    assert_eq!(
        *offsets.first().unwrap(),
        300,
        "must include the earliest record"
    );
    assert_eq!(
        *offsets.last().unwrap(),
        305,
        "must include the latest record"
    );
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn count_zero_returns_no_fingerprints() {
    let dir = unique_dir("anchor-zero");
    let set = seed_anchor_archive(&dir);
    let loc = StorageUrl::Filesystem { path: dir.clone() };
    let engine = engine_with(
        "../../e2e/fixtures/fake-engine-clean.sh",
        Store::read_only_from_url(&loc).unwrap(),
    );
    let fps = engine.fingerprints(&anchor_sel(set, "head", 0)).unwrap();
    assert!(fps.is_empty());
    let _ = std::fs::remove_dir_all(&dir);
}

/// `phase4_sample::run` (crates/logweir) cannot populate `manifest_key` — it
/// arrives empty until a caller patches `sel.set` from the original
/// `BackupSetRef`. This is the guard that catches a forgotten patch step: a
/// clear, specific `EngineError::Operational`, not an incidental storage
/// error a reader has to decode.
#[test]
fn an_empty_manifest_key_is_refused_with_a_clear_operational_error() {
    let dir = unique_dir("anchor-empty-manifest-key");
    let mut set = seed_anchor_archive(&dir);
    set.manifest_key = String::new();
    let loc = StorageUrl::Filesystem { path: dir.clone() };
    let engine = engine_with(
        "../../e2e/fixtures/fake-engine-clean.sh",
        Store::read_only_from_url(&loc).unwrap(),
    );
    let err = engine
        .fingerprints(&anchor_sel(set, "head", 3))
        .unwrap_err();
    match err {
        EngineError::Operational(msg) => assert!(msg.contains("manifest_key"), "{msg}"),
        other => panic!("expected EngineError::Operational, got {other:?}"),
    }
    let _ = std::fs::remove_dir_all(&dir);
}

/// `count` bigger than what is available is not an error — every matching
/// record comes back, regardless of anchor.
#[test]
fn count_larger_than_available_returns_everything() {
    let dir = unique_dir("anchor-large-count");
    let set = seed_anchor_archive(&dir);
    let loc = StorageUrl::Filesystem { path: dir.clone() };
    let engine = engine_with(
        "../../e2e/fixtures/fake-engine-clean.sh",
        Store::read_only_from_url(&loc).unwrap(),
    );
    let fps = engine.fingerprints(&anchor_sel(set, "tail", 1000)).unwrap();
    assert_eq!(fps.len(), 6);
    let _ = std::fs::remove_dir_all(&dir);
}

/// `anchor` is entirely Logweir-internal (never engine-reported), so an
/// unrecognised value is a bug in the caller, not degraded data — and a
/// scorecard cannot honestly claim an anchor that was never applied.
#[test]
fn an_unknown_anchor_is_an_operational_error() {
    let dir = unique_dir("anchor-unknown");
    let set = seed_anchor_archive(&dir);
    let loc = StorageUrl::Filesystem { path: dir.clone() };
    let engine = engine_with(
        "../../e2e/fixtures/fake-engine-clean.sh",
        Store::read_only_from_url(&loc).unwrap(),
    );
    let err = engine
        .fingerprints(&anchor_sel(set, "bogus", 3))
        .unwrap_err();
    assert!(err.to_string().contains("bogus"));
    let _ = std::fs::remove_dir_all(&dir);
}

/// Builds a backup set with TWO segments in the manifest, both matching the
/// query window: `hs/.../segment-...400.bin` (offsets 400-402, WRITTEN to
/// disk) and `hs/.../segment-...500.bin` (offsets 500-502, deliberately
/// **never written** — no file exists at that path at all). Segment keys are
/// zero-padded by start offset, so the 400 segment sorts before the 500 one
/// in `segment_keys_for_set`'s (lexicographically sorted) output — exactly
/// the ordering `OsoCliEngine::fingerprints`'s `head` short-circuit relies on.
fn seed_two_segment_archive_with_a_missing_later_segment(dir: &std::path::Path) -> BackupSetRef {
    const SEG_A: &str = "hs/topics/orders/partition=0/segment-00000000000000000400.bin";
    const SEG_B: &str = "hs/topics/orders/partition=0/segment-00000000000000000500.bin";
    let manifest = format!(
        r#"{{"backup_id":"hs","created_at":0,"topics":[{{"name":"orders","partitions":[{{"partition_id":0,"segments":[
        {{"key":"{SEG_A}","start_offset":400,"end_offset":402,"start_timestamp":400,"end_timestamp":402,"record_count":3}},
        {{"key":"{SEG_B}","start_offset":500,"end_offset":502,"start_timestamp":500,"end_timestamp":502,"record_count":3}}
        ]}}]}}]}}"#
    );
    std::fs::create_dir_all(dir.join("hs")).unwrap();
    std::fs::write(dir.join("hs/manifest.json"), manifest).unwrap();

    let seg_a_path = dir.join(SEG_A);
    std::fs::create_dir_all(seg_a_path.parent().unwrap()).unwrap();
    let records: Vec<(i64, i64, &[u8], &[u8])> = vec![
        (400, 400, b"ka0", b"va0"),
        (401, 401, b"ka1", b"va1"),
        (402, 402, b"ka2", b"va2"),
    ];
    std::fs::write(&seg_a_path, make_kbak_segment(&records)).unwrap();
    // SEG_B is intentionally never written: its directory is not even
    // created. A traversal that reaches it fails with StoreError::NotFound.

    BackupSetRef {
        backup_id: "hs".into(),
        manifest_key: "hs/manifest.json".into(),
    }
}

/// The "honest form" the coordinator asked for: not a re-check of output
/// length (which a full traversal that happens to produce the right count
/// would also pass), but a fixture where reading the segment `head` is
/// supposed to skip would ITSELF fail — proving the skip actually happened,
/// not merely that the answer came out right.
///
/// `count: 3` is satisfied entirely by the first (400-402) segment, so
/// `head` must never attempt the second (500-502) segment, which does not
/// exist on disk. `tail`, run against the IDENTICAL fixture, must traverse
/// the whole window to find the latest records and therefore DOES reach the
/// missing segment — its `Err` is the control that proves the missing
/// segment was genuinely reachable (matched topic/partition/window, was
/// really in `head`'s path), not irrelevant for some unrelated reason.
#[test]
fn head_short_circuits_before_reading_a_later_unneeded_segment() {
    let dir = unique_dir("head-short-circuit");
    let set = seed_two_segment_archive_with_a_missing_later_segment(&dir);
    let loc = StorageUrl::Filesystem { path: dir.clone() };
    let engine = engine_with(
        "../../e2e/fixtures/fake-engine-clean.sh",
        Store::read_only_from_url(&loc).unwrap(),
    );
    let window_sel = |anchor: &str| SampleSelection {
        set: set.clone(),
        topic: "orders".into(),
        partition: 0,
        anchor: anchor.into(),
        count: 3,
        window: (400, 502),
    };

    // head: satisfied by the first segment alone; must not touch the second.
    let fps = engine.fingerprints(&window_sel("head")).unwrap();
    assert_eq!(
        fps.iter().map(|f| f.offset).collect::<Vec<_>>(),
        vec![400, 401, 402]
    );

    // Control: tail, same fixture, same window — must traverse both segments
    // to know which are the LATEST 3 records, reaches the missing one, fails.
    // This is what proves segment B was genuinely in `head`'s path too, not
    // excluded by topic/partition/window for some other reason.
    let err = engine.fingerprints(&window_sel("tail")).unwrap_err();
    assert!(
        err.to_string().contains("segment-00000000000000000500.bin"),
        "{err}"
    );

    let _ = std::fs::remove_dir_all(&dir);
}
