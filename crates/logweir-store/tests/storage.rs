use logweir_store::{Store, StoreError};

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

// Review FIX 4: the conditional_put=true path's AlreadyExists case
// (`a_create_only_put_onto_an_existing_key_is_already_exists` above) is
// produced by object_store's own `put_opts`, which never runs when
// `conditional_put` is false — so it left the HEAD-then-PUT fallback's OWN
// AlreadyExists branch (src/storage.rs, inside `put_create_only`, right after
// the conditional_put block) completely uncovered. This proves that branch:
// a second put to the same key in fallback mode is refused, not silently
// overwritten. The gap this does NOT close — a TOCTOU window between the
// HEAD and the PUT on a genuinely concurrent writer — is inherent to
// HEAD-then-PUT and is exactly what `create_only_enforced: false` on the
// first outcome exists to flag to anything building a scorecard from it;
// see the fix report for why no further field was added.
#[test]
fn a_second_put_in_fallback_mode_is_refused_not_overwritten() {
    let s = Store::in_memory_without_conditional_put("logweir");
    let first = s.put_create_only("logweir/drills/a.json", b"{}").unwrap();
    assert!(!first.create_only_enforced);
    match s.put_create_only("logweir/drills/a.json", b"{\"x\":1}") {
        Err(StoreError::AlreadyExists(k)) => assert!(k.contains("a.json")),
        other => panic!("a second fallback put must be AlreadyExists, got {other:?}"),
    }
    // and the first bytes are still there — never clobbered
    assert_eq!(s.get("logweir/drills/a.json").unwrap().0, b"{}");
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

/// A DEEPER prefix under `logweir/` used to be accepted at construction and
/// then panic mid-drill.
///
/// `logweir/prod/` reads as legal against the rule as `examples/drill.yaml`
/// and `docs/quickstart.md` state it, and `from_url`'s `starts_with` accepted
/// it — but every key builder in this crate is hard-coded to
/// `logweir/drills/…`, so `put_create_only`'s
/// `assert!(key.starts_with(&self.prefix))` fired AFTER the restore had run:
/// exit 101, outside the five-code exit contract, on the one failure path
/// where the drill had already written to the operator's cluster. The refusal
/// belongs at construction, which is what `from_url`'s own doc comment
/// promised all along.
#[test]
fn a_prefix_deeper_than_the_sanctioned_root_is_refused_at_construction() {
    let deeper = logweir_core::engine::StorageUrl::S3 {
        bucket: "logweir-evidence".into(),
        prefix: "logweir/prod/".into(),
        region: Some("us-east-1".into()),
        endpoint: None,
        path_style: true,
        allow_http: false,
    };
    let msg = match Store::from_url(&deeper) {
        Err(e) => e.to_string(),
        Ok(_) => panic!("a prefix no key builder can honour must be refused before anything runs"),
    };
    assert!(
        msg.contains("must be exactly `logweir/`"),
        "the refusal must say what is wrong with it: {msg}"
    );
    assert!(
        msg.contains("logweir/prod/"),
        "the refusal must name the prefix it refused: {msg}"
    );
}

/// The counterpart: the sanctioned root itself still builds. A refusal that
/// rejected everything would satisfy the test above and break every drill.
#[test]
fn the_sanctioned_root_still_builds() {
    let ok = logweir_core::engine::StorageUrl::S3 {
        bucket: "logweir-evidence".into(),
        prefix: "logweir/".into(),
        region: Some("us-east-1".into()),
        endpoint: None,
        path_style: true,
        allow_http: false,
    };
    assert!(
        Store::from_url(&ok).is_ok(),
        "`logweir/` is the prefix every example pins"
    );
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

// A2, corrected after review FIX 1: the Interfaces-block method Task 12's
// `fingerprints()` actually calls — resolves topic/partition against a
// manifest and delegates the overlap test to `segment_keys_from`.
//
// The manifest body below uses upstream's REAL prefix-relative key form —
// `{backup_id}/topics/{topic}/partition={n}/segment-{offset:020}.bin{ext}`
// [VERIFIED U/kafka-backup/crates/kafka-backup-core/src/backup/engine.rs:
// 1436-1442] — never the fully-qualified form this test used to seed, which
// let `segment_keys_for` return the manifest's relative key unqualified and
// still pass. `s.get(&got[0])` below is the regression guard: it proves the
// returned key is one `get` can actually resolve, not merely a string that
// looks right.
#[test]
fn segment_keys_for_resolves_topic_partition_against_the_manifest() {
    let s = Store::in_memory("logweir");
    let manifest = br#"{"topics":[{"name":"orders","partitions":[{"partition_id":0,"segments":[
        {"key":"b1/topics/orders/partition=0/segment-00000000000000000000.bin.zst","start_timestamp":0,"end_timestamp":10},
        {"key":"b1/topics/orders/partition=0/segment-00000000000000000100.bin.zst","start_timestamp":100,"end_timestamp":200}]}]}]}"#;
    s.put_create_only("logweir/b1/manifest.json", manifest)
        .unwrap();
    // The bytes actually live at the FULLY QUALIFIED key — `prefix` ("logweir")
    // plus the manifest's relative key — exactly as `S3Backend::full_path`
    // would place them on a real backend.
    s.put_create_only(
        "logweir/b1/topics/orders/partition=0/segment-00000000000000000100.bin.zst",
        b"segment-bytes",
    )
    .unwrap();

    let got = s.segment_keys_for("orders", 0, (50, 300)).unwrap();
    assert_eq!(
        got,
        vec![
            "logweir/b1/topics/orders/partition=0/segment-00000000000000000100.bin.zst".to_string()
        ]
    );
    // Proves the returned key resolves, not just that it string-matches.
    assert_eq!(s.get(&got[0]).unwrap().0, b"segment-bytes");

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

// MINOR-2 (Task 20 review): `list_keys` returns every key under the prefix, in
// ascending order, and Task 20's phase 8 depends on that when it picks the
// engine-validation report. Caveat, stated rather than glossed: this test
// pins the OBSERVABLE contract (all keys, ascending) but cannot pin the
// `out.sort()` line itself — `object_store`'s `InMemory` is a `BTreeMap` and
// returns keys ordered regardless, so removing the sort leaves this green
// (mutant Z4). `storage.rs`'s doc comment says the same. Proving the sort
// would need a deliberately-unordered `ObjectStore` double, which `Store` has
// no constructor for and which is not worth widening its public API to allow.
#[test]
fn list_keys_returns_every_key_under_the_prefix_sorted() {
    let s = Store::in_memory("logweir");
    for k in [
        "logweir/run/c.json",
        "logweir/run/a.json",
        "logweir/run/b.json",
    ] {
        s.put_create_only(k, b"{}").unwrap();
    }
    let keys = s.list_keys("logweir/run/").unwrap();
    assert_eq!(
        keys,
        vec![
            "logweir/run/a.json".to_string(),
            "logweir/run/b.json".to_string(),
            "logweir/run/c.json".to_string(),
        ],
        "list_keys must sort; phase 8 takes keys[0] and would otherwise retain \
         whichever object the backend happened to stream first"
    );
}

// The sibling half of the same guarantee: `list_manifest_keys` is now expressed
// as `list_keys` plus a filter, so it must apply the filter and nothing else —
// and it inherits the sort.
#[test]
fn list_manifest_keys_filters_list_keys_and_keeps_the_ordering() {
    let s = Store::in_memory("logweir");
    for k in [
        "logweir/z/manifest.json",
        "logweir/a/manifest.json",
        "logweir/a/notes.txt",
    ] {
        s.put_create_only(k, b"{}").unwrap();
    }
    assert_eq!(
        s.list_manifest_keys("logweir/").unwrap(),
        vec![
            "logweir/a/manifest.json".to_string(),
            "logweir/z/manifest.json".to_string(),
        ]
    );
}

// Spec §6 C3: `evidence.immutable` may be `true` ONLY after a provider
// readback. `object_store` 0.14 models no Object Lock / WORM API on any
// backend it can build, so the honest answer is "no proof", unconditionally.
// Pinned directly here because it is the SOURCE of that claim: phase 8's
// `sc.evidence.immutable = lock.map(..).unwrap_or(false)` is a provable no-op
// while this returns `None`, so a mutant deleting that assignment cannot be
// caught downstream — but a mutant fabricating a `Some` here can, and is
// (Task 20 mutant M15).
#[test]
fn object_lock_readback_reports_no_proof_rather_than_guessing() {
    let s = Store::in_memory("logweir");
    s.put_create_only("logweir/drills/RUN.json", b"{}").unwrap();
    assert!(
        s.object_lock_readback("logweir/drills/RUN.json").is_none(),
        "no backend object_store 0.14 can build exposes Object Lock; claiming \
         otherwise would put an unverified WORM assertion into a signed document"
    );
    assert!(
        s.object_lock_readback("logweir/drills/does-not-exist.json")
            .is_none(),
        "and absence of the object is not evidence of retention either"
    );
}

// Interface I12, new with the `logweir-store` extraction. The retention
// reconciler (spec §5, guard G-RET) runs in `weirkeeper` and therefore cannot
// call `OsoCliEngine::describe` — the only other thing in this workspace that
// carries a backup set's timestamps. `list_manifests` returns `BackupSetRef`,
// which is `{ backup_id, manifest_key }` derived from the KEY STRING alone,
// with no object read: there is no timestamp anywhere in it.
//
// Both arms matter. The first pins that the window comes out of the manifest
// BODY — the key below carries no timestamp at all, so anything derived from
// the key reports 0 and fails here. The second pins the refusal: a manifest
// with no segment bounds no window, and an empty min/max would otherwise be
// published as if it were a real one.
#[test]
fn manifest_facts_reads_the_window_from_the_body() {
    let s = Store::in_memory("logweir");
    let manifest = br#"{"topics":[{"name":"orders","partitions":[{"partition_id":0,"segments":[
        {"key":"b1/topics/orders/partition=0/segment-00000000000000000000.bin.zst","start_timestamp":1700000000000,"end_timestamp":1700000300000},
        {"key":"b1/topics/orders/partition=0/segment-00000000000000000100.bin.zst","start_timestamp":1700000300000,"end_timestamp":1700000600000}]}]},
        {"name":"payments","partitions":[{"partition_id":3,"segments":[
        {"key":"b1/topics/payments/partition=3/segment-00000000000000000000.bin.zst","start_timestamp":1700000100000,"end_timestamp":1700000500000}]}]}]}"#;
    s.put_create_only("logweir/b1/manifest.json", manifest)
        .unwrap();

    let facts = s.manifest_facts("logweir/b1/manifest.json").unwrap();
    assert_eq!(
        facts.backup_id, "b1",
        "backup_id is the manifest key's parent directory, derived exactly as \
         list_manifests derives it"
    );
    assert_eq!(
        facts.oldest_record_ms, 1_700_000_000_000,
        "oldest_record_ms is the MINIMUM start_timestamp over every segment of \
         every partition of every topic, read from the body — the key carries no \
         timestamp, so a key-derived value would be 0"
    );
    assert_eq!(
        facts.newest_record_ms, 1_700_000_600_000,
        "newest_record_ms is the MAXIMUM end_timestamp over every segment of \
         every partition of every topic, read from the body — the key carries no \
         timestamp, so a key-derived value would be 0"
    );

    // Second arm: a manifest that declares no segment bounds no window.
    s.put_create_only("logweir/b2/manifest.json", br#"{"topics":[]}"#)
        .unwrap();
    match s.manifest_facts("logweir/b2/manifest.json") {
        Err(StoreError::Backend(m)) => {
            assert!(
                m.contains("logweir/b2/manifest.json"),
                "the refusal must name the key it refused: {m}"
            );
            assert!(
                m.contains("manifest declares no segment, so it bounds no window"),
                "the refusal must say why, not just that: {m}"
            );
        }
        other => panic!("a segment-less manifest bounds no window, got {other:?}"),
    }
}
