use logweir_core::engine::{BackupSetRef, RestorePlan, StorageUrl};
use logweir_engine_oso::{render_restore, render_validation, FORBIDDEN_KEYS};

fn plan() -> RestorePlan {
    RestorePlan {
        set: BackupSetRef {
            backup_id: "backup-2026-08-30T02:00:00Z".into(),
            manifest_key: "drills/backup-2026-08-30T02:00:00Z/manifest.json".into(),
        },
        // Addendum A1: StorageUrl is the tagged enum from Task 8a, not a flat
        // struct with a `backend` discriminator field.
        storage: StorageUrl::S3 {
            bucket: "kafka-backups".into(),
            prefix: "basic-demo".into(),
            region: Some("us-east-1".into()),
            endpoint: Some("http://minio:9000".into()),
            path_style: true,
            allow_http: true,
        },
        target_bootstrap: vec!["kafka-broker-1:9092".into()],
        topic_mapping: [
            ("orders".to_string(), "drill-20260903-orders".to_string()),
            (
                "payments".to_string(),
                "drill-20260903-payments".to_string(),
            ),
        ]
        .into_iter()
        .collect(),
        time_window: (
            "2026-08-29T00:00:00Z".parse().unwrap(),
            "2026-08-30T02:00:00Z".parse().unwrap(),
        ),
        default_replication_factor: 1,
        checkpoint_state: "/var/lib/logweir/01J9X/checkpoint.json".into(),
        checkpoint_interval_secs: 30,
    }
}

#[test]
fn restore_yaml_matches_the_golden() {
    insta::assert_snapshot!("restore_yaml", render_restore::render(&plan()));
}

#[test]
fn validation_yaml_matches_the_golden() {
    insta::assert_snapshot!(
        "validation_yaml",
        render_validation::render(&plan(), "01J9X2QK7C4V0R8YB3ZP6MTS5A", Some("KPMG Q3"))
    );
}

/// The invariant that outranks the golden: C4 NEVER emits these three keys, at
/// any value, in either document (Global Constraint 4). Compared on the whole
/// YAML KEY, never as a substring: `dry_run_check_segments` is a DIFFERENT key
/// from `dry_run` and is deliberately rendered (see `render_restore.rs`), so a
/// substring scan would make this test and
/// `restore_yaml_sets_both_levers_and_create_topics` mutually unsatisfiable.
/// `logweir_core::guard` is not reachable from here — it is created in Task 14 —
/// so the key split is done inline, with no new dependency.
#[test]
fn neither_document_ever_contains_a_forbidden_key() {
    for doc in [
        render_restore::render(&plan()),
        render_validation::render(&plan(), "r", None),
    ] {
        for line in doc.lines() {
            let key = line
                .trim_start()
                .split(':')
                .next()
                .unwrap_or("")
                .trim()
                .trim_start_matches("- ");
            for k in FORBIDDEN_KEYS {
                // Brief writes `*k`; `k: &str` already (FORBIDDEN_KEYS: [&str; 3]
                // is Copy), so `*k` derefs to the unsized `str` and
                // `assert_ne!` cannot compare `&str` with `str`
                // (E0277). Compare `key` against `k` directly — same
                // whole-key semantics, no substring behavior change.
                assert_ne!(
                    key, k,
                    "rendered document emits forbidden key `{k}`:\n{doc}"
                );
            }
        }
    }
}

/// Strengthens the brief's single-example forbidden-key scan into a property
/// of the renderer: every `StorageUrl` variant `render_storage_block` matches
/// on (S3, Azure, Gcs, Filesystem), crossed with an empty and a populated
/// `topic_mapping` and boundary values for the numeric fields, is rendered
/// through BOTH `render_restore::render` and `render_validation::render` and
/// scanned line-by-line, whole-key, for the three forbidden keys. Nothing in
/// either renderer branches on these fields to decide whether to print
/// `purge_topics`, `dry_run` or `header_preflight_external` — no `format!` or
/// `push_str` in either source file ever names them — so this is a property
/// over renderer structure, not a fact about any one input; the point of
/// varying inputs here is to walk every branch (all four storage arms, zero
/// vs many topics) rather than to hunt for a data-dependent counterexample.
#[test]
fn forbidden_keys_are_unreachable_across_every_storage_variant_and_plan_shape() {
    let storages: Vec<StorageUrl> = vec![
        StorageUrl::S3 {
            bucket: "b".into(),
            prefix: "p".into(),
            region: None,
            endpoint: None,
            path_style: false,
            allow_http: false,
        },
        StorageUrl::Azure {
            account_name: "acct".into(),
            container_name: "container".into(),
            prefix: "p".into(),
        },
        StorageUrl::Gcs {
            bucket: "b".into(),
            prefix: "p".into(),
        },
        StorageUrl::Filesystem {
            path: "/var/backups".into(),
        },
    ];

    let topic_mappings: Vec<std::collections::BTreeMap<String, String>> =
        vec![std::collections::BTreeMap::new(), plan().topic_mapping];

    for storage in storages {
        for topic_mapping in &topic_mappings {
            for default_replication_factor in [0i16, 1, -1, 5] {
                for checkpoint_interval_secs in [0u64, 30] {
                    let mut p = plan();
                    p.storage = storage.clone();
                    p.topic_mapping = topic_mapping.clone();
                    p.default_replication_factor = default_replication_factor;
                    p.checkpoint_interval_secs = checkpoint_interval_secs;

                    for doc in [
                        render_restore::render(&p),
                        render_validation::render(&p, "run-id", None),
                        render_validation::render(&p, "run-id", Some("someone")),
                    ] {
                        for line in doc.lines() {
                            let key = line
                                .trim_start()
                                .split(':')
                                .next()
                                .unwrap_or("")
                                .trim()
                                .trim_start_matches("- ");
                            for k in FORBIDDEN_KEYS {
                                assert_ne!(
                                    key, k,
                                    "rendered document emits forbidden key `{k}` for storage \
                                     variant {storage:?}:\n{doc}"
                                );
                            }
                        }
                    }
                }
            }
        }
    }
}

#[test]
fn restore_yaml_sets_both_levers_and_create_topics() {
    let doc = render_restore::render(&plan());
    assert!(doc.contains("header_preflight: full"));
    assert!(doc.contains("dry_run_check_segments: true"));
    assert!(doc.contains("create_topics: true"));
    assert!(doc.contains("default_replication_factor: 1"));
    assert!(doc.contains("checkpoint_interval_secs: 30"));
}

#[test]
fn the_time_window_is_rendered_as_epoch_millis_not_rfc3339() {
    let doc = render_restore::render(&plan());
    // The brief's literals here (1756425600000 / 1756519200000) decode to
    // 2025-08-29 / 2025-08-30, not the 2026-08-29T00:00:00Z /
    // 2026-08-30T02:00:00Z the same brief's `plan()` fixture specifies
    // (matching backup_id "backup-2026-08-30T02:00:00Z" a few lines above).
    // Corrected to the millis that `plan()`'s own RFC3339 strings actually
    // convert to; the property under test — epoch millis, not an RFC3339
    // string — is unchanged.
    assert!(doc.contains("time_window_start: 1787961600000"));
    assert!(doc.contains("time_window_end: 1788055200000"));
    assert!(
        !doc.contains("time_window_start: \""),
        "config.rs:751-758 types these Option<i64> epoch millis; a string is a serde TYPE error"
    );
}

#[test]
fn topic_mapping_is_one_explicit_entry_per_selected_topic() {
    let doc = render_restore::render(&plan());
    assert!(doc.contains("orders: drill-20260903-orders"));
    assert!(doc.contains("payments: drill-20260903-payments"));
}

#[test]
fn validation_yaml_points_evidence_storage_at_the_per_run_logweir_prefix() {
    let doc = render_validation::render(&plan(), "01J9X2QK7C4V0R8YB3ZP6MTS5A", None);
    assert!(doc.contains("prefix: logweir/01J9X2QK7C4V0R8YB3ZP6MTS5A/engine-validation"));
}
