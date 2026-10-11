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
    assert_eq!(
        s.get_capped(
            "logweir/drills/a.json",
            logweir_store::caps::SIGNED_DOCUMENT
        )
        .unwrap()
        .0,
        b"{}"
    );
}

// Keys are seeded under `logweir/` so they satisfy put_create_only's own
// LOGWEIR_ROOT assertion.
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

/// A backend that cannot do a conditional put must be
/// RECORDED as such, never silently treated as if it had.
#[test]
fn a_backend_without_conditional_put_reports_create_only_enforced_false() {
    let s = Store::in_memory_without_conditional_put("logweir");
    let out = s.put_create_only("logweir/drills/a.json", b"{}").unwrap();
    assert!(!out.create_only_enforced);
}

// The HEAD-then-PUT fallback's OWN AlreadyExists branch (`put_create_only`
// with `conditional_put` false): a second put to the same key is refused, not
// silently overwritten. The conditional-put path's AlreadyExists comes from
// object_store's own `put_opts`, which never runs in this mode. Not closed
// here: the TOCTOU window between the HEAD and the PUT on a concurrent writer
// is inherent to HEAD-then-PUT, and is what `create_only_enforced: false` on
// the first outcome flags to anything building a scorecard from it.
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
    assert_eq!(
        s.get_capped(
            "logweir/drills/a.json",
            logweir_store::caps::SIGNED_DOCUMENT
        )
        .unwrap()
        .0,
        b"{}"
    );
}

// This covers the pure helper `segment_keys_from`, not the manifest-resolving
// `segment_keys_for`.
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
    // Constructed with the OSO ARCHIVE prefix: the fixed root must still refuse
    // the write.
    let s = Store::in_memory("kafka-backups/");
    let _ = s.put_create_only("kafka-backups/daily/manifest.json", b"{}");
}

/// A DEEPER prefix under `logweir/` is refused at construction, not mid-drill.
///
/// Every key builder in this crate is hard-coded to `logweir/drills/…`, so a
/// store over `logweir/prod/` would fail `put_create_only`'s
/// `assert!(key.starts_with(&self.prefix))` AFTER the restore had run: exit
/// 101, outside the five-code exit contract, on the one failure path where the
/// drill had already written to the operator's cluster.
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
    // churn.
    let s = Store::in_memory("logweir/");
    for i in 0..50 {
        s.put_create_only(&format!("logweir/k{i}"), b"x").unwrap();
    }
    assert_eq!(
        s.get_capped("logweir/k49", logweir_store::caps::SIGNED_DOCUMENT)
            .unwrap()
            .0,
        b"x"
    );
}

// The method `fingerprints()` calls: resolves topic/partition against a
// manifest and delegates the overlap test to `segment_keys_from`.
//
// The manifest body below uses upstream's REAL prefix-relative key form —
// `{backup_id}/topics/{topic}/partition={n}/segment-{offset:020}.bin{ext}`
// [VERIFIED U/kafka-backup/crates/kafka-backup-core/src/backup/engine.rs:
// 1436-1442]. `s.get(&got[0])` below proves the returned key is one `get` can
// actually resolve, not merely a string that looks right.
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
    assert_eq!(
        s.get_capped(&got[0], logweir_store::caps::SIGNED_DOCUMENT)
            .unwrap()
            .0,
        b"segment-bytes"
    );

    assert!(s
        .segment_keys_for("payments", 0, (50, 300))
        .unwrap()
        .is_empty());
}

// The read-only constructor must physically be unable to put — its handle skips
// the LOGWEIR_ROOT guard (so it CAN be built over an archive prefix that
// from_url would refuse) but every put method on it refuses before ever
// touching the backend or the guard.
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

// `list_keys` returns every key under the prefix, in ascending order; phase 8
// depends on that when it picks the engine-validation report. This pins the
// OBSERVABLE contract but not the `out.sort()` line itself: `object_store`'s
// `InMemory` is a `BTreeMap` and returns keys ordered regardless, so removing
// the sort leaves this green. Proving the sort would need a
// deliberately-unordered `ObjectStore` double, which `Store` has no
// constructor for.
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

// The sibling half of the same guarantee: `list_manifest_keys` is expressed
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

// `evidence.immutable` may be `true` ONLY after a provider
// readback. `object_store` 0.14 models no Object Lock / WORM API on any
// backend it can build, so the honest answer is "no proof", unconditionally.
// Pinned directly here because it is the SOURCE of that claim: phase 8's
// `sc.evidence.immutable = lock.map(..).unwrap_or(false)` is a provable no-op
// while this returns `None`, so a mutant deleting that assignment cannot be
// caught downstream — but a mutant fabricating a `Some` here can.
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

// Interface I12. The retention reconciler (guard G-RET) runs in `weirkeeper`
// and therefore cannot call `OsoCliEngine::describe` — the only other thing in
// this workspace that carries a backup set's timestamps. `list_manifests`
// returns `BackupSetRef`, which is `{ backup_id, manifest_key }` derived from
// the KEY STRING alone, with no object read: there is no timestamp anywhere in
// it.
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

    let facts = s
        .manifest_facts(
            "logweir/b1/manifest.json",
            logweir_store::caps::CONTROLLER_MANIFEST,
        )
        .unwrap();
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
    match s.manifest_facts(
        "logweir/b2/manifest.json",
        logweir_store::caps::CONTROLLER_MANIFEST,
    ) {
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

/// An honest error taxonomy for `manifest_facts`.
///
/// `{"hello":"world"}` and `{"topics":5}` must not answer
/// `Backend("… declares no segment, so it bounds no window")`, the error
/// a real manifest describing an empty backup set gets. "The prefix points at
/// objects that are not manifests" and "this backup set declares no segment"
/// are different facts, and a retention report that cannot tell them apart
/// reports a misconfigured archive as an empty one.
///
/// THE THIRD ARM: `{"topics":[]}` must STILL be `Backend`, so the variant is a
/// taxonomy, not a rename.
#[test]
fn a_body_that_is_not_a_manifest_is_not_a_segment_less_manifest() {
    let s = mem();

    s.put_create_only("logweir/n1/manifest.json", br#"{"hello":"world"}"#)
        .unwrap();
    match s.manifest_facts(
        "logweir/n1/manifest.json",
        logweir_store::caps::CONTROLLER_MANIFEST,
    ) {
        Err(StoreError::NotAManifest(key, why)) => {
            assert_eq!(key, "logweir/n1/manifest.json");
            assert!(
                why.contains("declares no `topics` key"),
                "the refusal must say what was missing: {why}"
            );
        }
        other => panic!("a body with no `topics` key is not a manifest, got {other:?}"),
    }

    s.put_create_only("logweir/n2/manifest.json", br#"{"topics":5}"#)
        .unwrap();
    match s.manifest_facts(
        "logweir/n2/manifest.json",
        logweir_store::caps::CONTROLLER_MANIFEST,
    ) {
        Err(StoreError::NotAManifest(key, why)) => {
            assert_eq!(key, "logweir/n2/manifest.json");
            assert!(
                why.contains("`topics` is a number, not an array"),
                "the refusal must name the type it found, and never quote the value: {why}"
            );
        }
        other => panic!("a body whose `topics` is not an array is not a manifest, got {other:?}"),
    }

    // AND THE OLD ERROR STILL MEANS THE OLD THING.
    s.put_create_only("logweir/n3/manifest.json", br#"{"topics":[]}"#)
        .unwrap();
    match s.manifest_facts(
        "logweir/n3/manifest.json",
        logweir_store::caps::CONTROLLER_MANIFEST,
    ) {
        Err(StoreError::Backend(m)) => assert!(
            m.contains("manifest declares no segment, so it bounds no window"),
            "a manifest-shaped body with no segment keeps the Backend error: {m}"
        ),
        other => panic!("a segment-less manifest is still Backend, got {other:?}"),
    }
}

/// The `backup_id` derivation is ONE function, which `list_manifests` and
/// `manifest_facts` both call. Asserted over every shape that reaches either: a
/// plain key, a deeply prefixed key, a key with no directory at all, and — the
/// one that makes the two implementations distinguishable if they ever diverge
/// — a `backup_id` that itself contains the substring `manifest`.
#[test]
fn the_backup_id_derivation_is_one_function() {
    use logweir_store::backup_id_from_manifest_key;

    for (key, want) in [
        ("logweir/b1/manifest.json", "b1"),
        (
            "kafka-backups/daily/2026-09-09T00-00Z/manifest.json",
            "2026-09-09T00-00Z",
        ),
        // A manifest at the very root has no parent directory, and the shared
        // function answers with the file name itself:
        // `trim_end_matches("/manifest.json")` needs the leading slash to bite.
        // No archive Logweir writes or reads puts a manifest at the root —
        // every key is `<backup_id>/manifest.json`.
        ("manifest.json", "manifest.json"),
        ("archive/manifest-archive/manifest.json", "manifest-archive"),
    ] {
        assert_eq!(
            backup_id_from_manifest_key(key),
            want,
            "backup_id_from_manifest_key({key})"
        );
    }

    // BOTH CALLERS, OVER THE SAME KEY, THROUGH A REAL STORE. `list_manifests`
    // derives from the key string; `manifest_facts` reads the body. They must
    // return the identical `backup_id`, and both must equal the shared
    // function's answer.
    let s = mem();
    let key = "logweir/manifest-archive/manifest.json";
    s.put_create_only(
        key,
        br#"{"topics":[{"name":"t","partitions":[{"partition_id":0,"segments":[
            {"key":"s.bin","start_timestamp":1,"end_timestamp":2}]}]}]}"#,
    )
    .unwrap();
    let facts = s
        .manifest_facts(key, logweir_store::caps::CONTROLLER_MANIFEST)
        .unwrap();
    let listed = s
        .list_manifests(&logweir_core::engine::StorageUrl::S3 {
            bucket: "irrelevant".to_string(),
            prefix: "logweir".to_string(),
            region: None,
            endpoint: None,
            path_style: false,
            allow_http: false,
        })
        .unwrap();
    let listed = listed
        .iter()
        .find(|r| r.manifest_key == key)
        .expect("the manifest just written is listed");
    assert_eq!(facts.backup_id, listed.backup_id);
    assert_eq!(facts.backup_id, backup_id_from_manifest_key(key));
    assert_eq!(facts.backup_id, "manifest-archive");
}

/// **The key the engine reads a set's manifest at.** For a
/// store over the plan's location, `engine_manifest_key(id)` is the key
/// kafka-backup's restore loads (`<prefix>/<id>/manifest.json` through
/// `S3Backend::full_path`), spelled as the listing spells keys — so a set the
/// engine wrote under any prefix spelling is found under exactly that key, and
/// a same-id manifest elsewhere under the prefix is not.
///
/// Negative controls: the nested same-id manifest the listing also returns is
/// never the engine's key; a trailing or doubled slash in the prefix does not
/// change the key.
#[test]
fn the_engine_manifest_key_is_the_one_the_engine_reads() {
    let s = Store::in_memory("logweir/archive");
    s.put_create_only("logweir/archive/nightly-7/manifest.json", b"{}")
        .unwrap();
    s.put_create_only("logweir/archive/a/nightly-7/manifest.json", b"{}")
        .unwrap();
    let listed = s.list_manifest_keys("logweir/archive").unwrap();
    assert!(
        listed.contains(&"logweir/archive/a/nightly-7/manifest.json".to_string()),
        "the nested same-id set is listed too: {listed:?}"
    );
    for prefix in ["logweir/archive", "logweir/archive/", "logweir//archive/"] {
        let key = Store::in_memory(prefix).engine_manifest_key("nightly-7");
        assert_eq!(key, "logweir/archive/nightly-7/manifest.json", "{prefix:?}");
        assert!(listed.contains(&key), "{prefix:?}: {listed:?}");
    }
    assert_eq!(
        Store::in_memory("").engine_manifest_key("nightly-7"),
        "nightly-7/manifest.json",
        "an empty prefix is the bucket root, as the engine's `prefix: None`"
    );
}
