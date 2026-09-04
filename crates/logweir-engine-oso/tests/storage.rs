use logweir_engine_oso::storage::{Store, StoreError};

/// object_store's in-process InMemory backend, so the create-only semantics are
/// provable with no MinIO and no network.
fn mem() -> Store {
    Store::in_memory("logweir")
}

#[test]
fn a_create_only_put_onto_an_existing_key_is_already_exists() {
    let s = mem();
    s.put_create_only("logweir/drills/a.json", b"{}").unwrap();
    match s.put_create_only("logweir/drills/a.json", b"{\"x\":1}") {
        Err(StoreError::AlreadyExists(k)) => assert!(k.contains("a.json")),
        other => panic!("a second create-only put must be AlreadyExists, got {other:?}"),
    }
    // and the first bytes are still there — never clobbered
    assert_eq!(s.get("logweir/drills/a.json").unwrap().0, b"{}");
}

// A1: keys seeded under `logweir/` so they satisfy put_create_only's own
// LOGWEIR_ROOT assertion; the brief's original fixture used `drills/...` and
// panicked on its own guard.
#[test]
fn list_manifests_returns_only_manifest_json_keys() {
    let s = mem();
    s.put_create_only("logweir/drills/b1/manifest.json", b"{}")
        .unwrap();
    s.put_create_only("logweir/drills/b1/segments/000.kbak", b"x")
        .unwrap();
    s.put_create_only("logweir/drills/b2/manifest.json", b"{}")
        .unwrap();
    let mut got: Vec<String> = s.list_manifest_keys("logweir/drills").unwrap();
    got.sort();
    assert_eq!(
        got,
        vec![
            "logweir/drills/b1/manifest.json".to_string(),
            "logweir/drills/b2/manifest.json".to_string(),
        ]
    );
}

/// spec §11 / §6 C3: a backend that cannot do a conditional put must be
/// RECORDED as such, never silently treated as if it had.
#[test]
fn a_backend_without_conditional_put_reports_create_only_enforced_false() {
    let s = Store::in_memory_without_conditional_put("logweir");
    let out = s.put_create_only("logweir/drills/a.json", b"{}").unwrap();
    assert!(!out.create_only_enforced);
}

// A2: renamed from `segment_keys_for_filters_by_the_time_window` — this test
// covers the pure helper `segment_keys_from`, not the manifest-resolving
// `segment_keys_for` the Interfaces block promises to Task 12.
#[test]
fn segment_keys_from_filters_by_the_time_window() {
    let s = mem();
    // keys are taken from the manifest, so this asserts the window filter only
    let all = s.segment_keys_from(&[("k1".into(), 0, 10), ("k2".into(), 100, 200)], (50, 300));
    assert_eq!(all, vec!["k2".to_string()]);
}

#[test]
#[should_panic(expected = "Global Constraint 6")]
fn a_write_outside_the_logweir_root_panics_even_when_the_prefix_allows_it() {
    // Constructed with the OSO ARCHIVE prefix — the disjunct that used to make
    // the guard vacuous. The fixed root must still refuse the write.
    let s = Store::in_memory("kafka-backups/");
    let _ = s.put_create_only("kafka-backups/daily/manifest.json", b"{}");
}

#[test]
fn fifty_sequential_puts_share_one_runtime() {
    // InMemory cannot observe connection reuse, so this asserts the property
    // that IS observable here: the store survives many calls with no runtime
    // churn. The connection-reuse assertion runs against MinIO in Task 20 step 0.
    let s = Store::in_memory("logweir/");
    for i in 0..50 {
        s.put_create_only(&format!("logweir/k{i}"), b"x").unwrap();
    }
    assert_eq!(s.get("logweir/k49").unwrap().0, b"x");
}

// A2: the Interfaces-block method Task 12's `fingerprints()` actually calls —
// resolves topic/partition against a manifest and delegates the overlap test
// to `segment_keys_from`.
#[test]
fn segment_keys_for_resolves_topic_partition_against_the_manifest() {
    let s = Store::in_memory("logweir/");
    let manifest = br#"{"topics":[{"name":"orders","partitions":[{"partition_id":0,"segments":[
        {"key":"logweir/sets/b1/orders-0/000.kbak","start_timestamp":0,"end_timestamp":10},
        {"key":"logweir/sets/b1/orders-0/001.kbak","start_timestamp":100,"end_timestamp":200}]}]}]}"#;
    s.put_create_only("logweir/sets/b1/manifest.json", manifest)
        .unwrap();
    assert_eq!(
        s.segment_keys_for("orders", 0, (50, 300)).unwrap(),
        vec!["logweir/sets/b1/orders-0/001.kbak".to_string()]
    );
    assert!(s
        .segment_keys_for("payments", 0, (50, 300))
        .unwrap()
        .is_empty());
}

// Controller amendment: the second, read-only constructor must physically be
// unable to put — its handle skips the LOGWEIR_ROOT guard (so it CAN be built
// over an archive prefix that from_url would refuse) but every put method on
// it refuses before ever touching the backend or the guard.
#[test]
fn a_read_only_store_rejects_a_put_even_over_the_archive_prefix() {
    let dir = std::env::temp_dir().join(format!(
        "logweir-storage-test-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    let archive = logweir_core::engine::StorageUrl::Filesystem { path: dir.clone() };
    let s = Store::read_only_from_url(&archive).unwrap();
    match s.put_create_only("kafka-backups/daily/manifest.json", b"{}") {
        Err(StoreError::ReadOnly(_)) => {}
        other => panic!("a read-only store must refuse every put, got {other:?}"),
    }
    let _ = std::fs::remove_dir_all(&dir);
}
