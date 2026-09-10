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
        target_auth: logweir_core::engine::AuthRender::Plaintext,
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
    insta::assert_snapshot!(
        "restore_yaml",
        render_restore::render(&plan()).expect("G-GLOB: this fixture holds no glob metacharacter")
    );
}

#[test]
fn validation_yaml_matches_the_golden() {
    insta::assert_snapshot!(
        "validation_yaml",
        render_validation::render(&plan(), "01J9X2QK7C4V0R8YB3ZP6MTS5A", Some("KPMG Q3"))
            .expect("G-EXP/G-GLOB: this fixture holds no glob metacharacter and no `${`")
    );
}

/// Review fix (round 2, "FIX 2"): `render_storage_block`'s own doc comment
/// promises "one golden per backend"; only S3 had one. A typo in the Azure,
/// Gcs or Filesystem arm — a wrong field name, a dropped `prefix:`, bad
/// indentation (this has already happened once: the addendum had to
/// hand-repair the Azure arm) — would pass every existing test (none of them
/// pin the RENDERED SHAPE of these three arms) and then be silently ignored
/// by the engine at drill time. These three pin them.
#[test]
fn restore_yaml_matches_the_golden_for_azure_storage() {
    let mut p = plan();
    p.storage = StorageUrl::Azure {
        account_name: "logweirdemo".into(),
        container_name: "kafka-backups".into(),
        prefix: "basic-demo".into(),
    };
    insta::assert_snapshot!(
        "restore_yaml_azure",
        render_restore::render(&p).expect("G-GLOB: this fixture holds no glob metacharacter")
    );
}

#[test]
fn restore_yaml_matches_the_golden_for_gcs_storage() {
    let mut p = plan();
    p.storage = StorageUrl::Gcs {
        bucket: "kafka-backups".into(),
        prefix: "basic-demo".into(),
    };
    insta::assert_snapshot!(
        "restore_yaml_gcs",
        render_restore::render(&p).expect("G-GLOB: this fixture holds no glob metacharacter")
    );
}

#[test]
fn restore_yaml_matches_the_golden_for_filesystem_storage() {
    let mut p = plan();
    p.storage = StorageUrl::Filesystem {
        path: "/var/backups/basic-demo".into(),
    };
    insta::assert_snapshot!(
        "restore_yaml_filesystem",
        render_restore::render(&p).expect("G-GLOB: this fixture holds no glob metacharacter")
    );
}

/// Review fix, "FIX 2" continued: the `triggered_by: None` branch of
/// `render_validation` (an operator-less, e.g. cron-triggered, drill) was
/// previously covered only by a `contains` check on a DIFFERENT assertion
/// (`validation_yaml_points_evidence_storage_at_the_per_run_logweir_prefix`),
/// never by a golden of the branch itself.
#[test]
fn validation_yaml_matches_the_golden_with_no_triggered_by() {
    insta::assert_snapshot!(
        "validation_yaml_no_triggered_by",
        render_validation::render(&plan(), "01J9X2QK7C4V0R8YB3ZP6MTS5A", None)
            .expect("G-EXP/G-GLOB: this fixture holds no glob metacharacter and no `${`")
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
        render_restore::render(&plan()).expect("G-GLOB: this fixture holds no glob metacharacter"),
        render_validation::render(&plan(), "r", None)
            .expect("G-EXP/G-GLOB: this fixture holds no glob metacharacter and no `${`"),
    ] {
        assert_no_forbidden_key_line(&doc);
    }
}

/// Shared whole-key, per-physical-line scan used by every forbidden-key test
/// in this file. Never a substring match: a quoted value that happens to
/// CONTAIN the text "dry_run" mid-scalar (see the adversarial tests below)
/// must not trip this, only a genuine standalone `dry_run:` key-line may.
fn assert_no_forbidden_key_line(doc: &str) {
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
            // is Copy), so `*k` derefs to the unsized `str` and `assert_ne!`
            // cannot compare `&str` with `str` (E0277). Compare `key` against
            // `k` directly — same whole-key semantics, no substring change.
            assert_ne!(
                key, k,
                "rendered document emits forbidden key `{k}`:\n{doc}"
            );
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
///
/// This test varies SHAPE (which storage variant, how many topics, boundary
/// integers) but every string value stays a short benign literal. It does
/// NOT, by itself, prove a forbidden key can't ride in on the CONTENT of a
/// string value — that is what
/// `forbidden_keys_survive_adversarial_string_content` below is for.
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
                        render_restore::render(&p)
                            .expect("G-GLOB: this fixture holds no glob metacharacter"),
                        render_validation::render(&p, "run-id", None).expect(
                            "G-EXP/G-GLOB: this fixture holds no glob metacharacter and no `${`",
                        ),
                        render_validation::render(&p, "run-id", Some("someone")).expect(
                            "G-EXP/G-GLOB: this fixture holds no glob metacharacter and no `${`",
                        ),
                    ] {
                        assert_no_forbidden_key_line(&doc);
                    }
                }
            }
        }
    }
}

/// Review fix (round 2, "FIX 1"): every value in the plan reaches the
/// document through a bare `format!`, and until this fix none of it was
/// escaped — `render_validation::render(&plan, "r", Some("x\ndry_run:
/// true"))` emitted a physical line whose pre-colon token WAS `dry_run`. The
/// structural property above cannot catch that: it varies shape only and
/// holds every string at a benign literal.
///
/// This test mutates ONE interpolation site at a time — `backup_id`, a
/// bootstrap server, both sides of `topic_mapping`, every storage field
/// across all four backends, `checkpoint_state`, `run_id`, and
/// `triggered_by` — to the adversarial payload `"a\ndry_run: true"` (a raw
/// newline followed by text that reads as a `dry_run: true` key-line at
/// column 0), holding every other field at `plan()`'s benign default, and
/// re-runs the whole-key scan. One field at a time, rather than all at once,
/// so a regression in any single interpolation site is individually
/// attributable instead of buried in a combined failure.
///
/// Each case also asserts the ESCAPED form of the payload is present
/// verbatim in the document — `"a\\ndry_run: true"` as a Rust string
/// literal denotes the text a-backslash-n-d-r-y…, i.e. the newline survived
/// as the two-character YAML escape `\n`, not as a real line break — so a
/// regression that "fixed" this by silently dropping or truncating the
/// value instead of escaping it would also be caught.
#[test]
fn forbidden_keys_survive_adversarial_string_content() {
    const PAYLOAD: &str = "a\ndry_run: true";
    const ESCAPED: &str = "a\\ndry_run: true"; // literal backslash + 'n', not a newline

    let mut cases: Vec<(&str, RestorePlan, &str, Option<&str>)> = Vec::new();

    // backup_id
    let mut p = plan();
    p.set.backup_id = PAYLOAD.into();
    cases.push(("backup_id", p, "run-id", None));

    // a bootstrap server entry (shared by both documents)
    let mut p = plan();
    p.target_bootstrap = vec![PAYLOAD.into()];
    cases.push(("bootstrap_servers", p, "run-id", None));

    // topic_mapping key side (restore.yaml only, but rendered via `plan`)
    let mut p = plan();
    p.topic_mapping = [(PAYLOAD.to_string(), "target".to_string())]
        .into_iter()
        .collect();
    cases.push(("topic_mapping key", p, "run-id", None));

    // topic_mapping value side
    let mut p = plan();
    p.topic_mapping = [("orders".to_string(), PAYLOAD.to_string())]
        .into_iter()
        .collect();
    cases.push(("topic_mapping value", p, "run-id", None));

    // checkpoint_state path
    let mut p = plan();
    p.checkpoint_state = PAYLOAD.into();
    cases.push(("checkpoint_state", p, "run-id", None));

    // every S3 field
    for field in ["bucket", "prefix", "region", "endpoint"] {
        let mut p = plan();
        p.storage = StorageUrl::S3 {
            bucket: if field == "bucket" { PAYLOAD } else { "b" }.into(),
            prefix: if field == "prefix" { PAYLOAD } else { "p" }.into(),
            region: Some(if field == "region" { PAYLOAD } else { "r" }.into()),
            endpoint: Some(if field == "endpoint" { PAYLOAD } else { "e" }.into()),
            path_style: false,
            allow_http: false,
        };
        cases.push(("s3", p, "run-id", None));
    }

    // every Azure field
    for field in ["account_name", "container_name", "prefix"] {
        let mut p = plan();
        p.storage = StorageUrl::Azure {
            account_name: if field == "account_name" {
                PAYLOAD
            } else {
                "a"
            }
            .into(),
            container_name: if field == "container_name" {
                PAYLOAD
            } else {
                "c"
            }
            .into(),
            prefix: if field == "prefix" { PAYLOAD } else { "p" }.into(),
        };
        cases.push(("azure", p, "run-id", None));
    }

    // every Gcs field
    for field in ["bucket", "prefix"] {
        let mut p = plan();
        p.storage = StorageUrl::Gcs {
            bucket: if field == "bucket" { PAYLOAD } else { "b" }.into(),
            prefix: if field == "prefix" { PAYLOAD } else { "p" }.into(),
        };
        cases.push(("gcs", p, "run-id", None));
    }

    // Filesystem path
    let mut p = plan();
    p.storage = StorageUrl::Filesystem {
        path: PAYLOAD.into(),
    };
    cases.push(("filesystem", p, "run-id", None));

    // run_id (validation.yaml only)
    cases.push(("run_id", plan(), PAYLOAD, None));

    // triggered_by (validation.yaml only) — the review's explicit example
    cases.push(("triggered_by", plan(), "run-id", Some(PAYLOAD)));

    for (label, p, run_id, triggered_by) in cases {
        let restore_doc =
            render_restore::render(&p).expect("G-GLOB: this fixture holds no glob metacharacter");
        let validation_doc = render_validation::render(&p, run_id, triggered_by)
            .expect("G-EXP/G-GLOB: this fixture holds no glob metacharacter and no `${`");

        assert_no_forbidden_key_line(&restore_doc);
        assert_no_forbidden_key_line(&validation_doc);

        assert!(
            restore_doc.contains(ESCAPED) || validation_doc.contains(ESCAPED),
            "case `{label}`: expected the escaped payload `{ESCAPED}` to survive \
             verbatim in at least one document (restore or validation) — a \
             regression that dropped/truncated the value instead of escaping \
             it would also slip past the forbidden-key scan; got:\n\
             --- restore.yaml ---\n{restore_doc}\n--- validation.yaml ---\n{validation_doc}"
        );
    }
}

/// The name is kept from before Task 8: this test still asserts that
/// `create_topics` is SET explicitly rather than left absent. Its VALUE
/// changed, to `false` — guard **G-TS**: Logweir creates the target topics
/// itself, with `TARGET_TOPIC_CONFIGS`, because the engine's own creation path
/// carries no configuration at all
/// [U:crates/kafka-backup-core/src/restore/engine.rs:1447-1455].
#[test]
fn restore_yaml_sets_both_levers_and_create_topics() {
    let doc =
        render_restore::render(&plan()).expect("G-GLOB: this fixture holds no glob metacharacter");
    assert!(doc.contains("header_preflight: full"));
    assert!(doc.contains("dry_run_check_segments: true"));
    assert!(doc.contains("create_topics: false"));
    assert!(doc.contains("default_replication_factor: 1"));
    assert!(doc.contains("checkpoint_interval_secs: 30"));
}

/// **Guard G-TS**, the render half. The rendered restore document creates NO
/// topics, and the assertion is on the EXACT LINE — two leading spaces, the
/// key, the value — in the rendered bytes AND in every checked-in `restore*`
/// golden, so a change made only in the source or only in a snapshot cannot
/// pass.
///
/// The goldens are read as files rather than through `insta` on purpose: this
/// test's job is that no `restore.yaml` anywhere in the tree still says
/// `create_topics: true`, which is a property of the SET of goldens and not of
/// any one render call.
#[test]
fn the_rendered_restore_document_creates_no_topics() {
    const LINE: &str = "  create_topics: false\n";
    let doc =
        render_restore::render(&plan()).expect("G-GLOB: this fixture holds no glob metacharacter");
    assert!(
        doc.contains(LINE),
        "the rendered restore document does not carry the exact line \
         `  create_topics: false`:\n{doc}"
    );
    assert!(
        !doc.contains("create_topics: true"),
        "the rendered restore document still creates topics:\n{doc}"
    );

    let snapshots = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/snapshots");
    let mut checked = 0usize;
    for e in std::fs::read_dir(&snapshots)
        .expect("tests/snapshots")
        .flatten()
    {
        let name = e.file_name().to_string_lossy().to_string();
        // Every golden of a RESTORE document, under either of the two test
        // binaries that write one (`render__restore_*`, `render_scram__restore_*`).
        if !name.contains("restore") || !name.ends_with(".snap") {
            continue;
        }
        let body = std::fs::read_to_string(e.path()).expect(&name);
        assert!(
            body.contains(LINE),
            "golden {name} does not carry the exact line `  create_topics: false`"
        );
        assert!(
            !body.contains("create_topics: true"),
            "golden {name} still creates topics"
        );
        checked += 1;
    }
    assert!(
        checked >= 6,
        "expected at least the six checked-in restore goldens, walked {checked}"
    );
}

#[test]
fn the_time_window_is_rendered_as_epoch_millis_not_rfc3339() {
    let doc =
        render_restore::render(&plan()).expect("G-GLOB: this fixture holds no glob metacharacter");
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
    let doc =
        render_restore::render(&plan()).expect("G-GLOB: this fixture holds no glob metacharacter");
    // Review fix ("FIX 1"): both sides of the mapping now go through
    // `yaml_scalar`, so a benign topic name is rendered double-quoted.
    assert!(doc.contains("\"orders\": \"drill-20260903-orders\""));
    assert!(doc.contains("\"payments\": \"drill-20260903-payments\""));
}

#[test]
fn validation_yaml_points_evidence_storage_at_the_per_run_logweir_prefix() {
    let doc = render_validation::render(&plan(), "01J9X2QK7C4V0R8YB3ZP6MTS5A", None)
        .expect("G-EXP/G-GLOB: this fixture holds no glob metacharacter and no `${`");
    // Review fix ("FIX 1"): the whole composed value is one `yaml_scalar`
    // call, so it is rendered as a single double-quoted scalar.
    assert!(doc.contains("prefix: \"logweir/01J9X2QK7C4V0R8YB3ZP6MTS5A/engine-validation\""));
}

/// **G-GLOB, restore side** (GC18(c) rail 1). The same table as
/// `tests/render_backup.rs::topic_include_entries_reject_glob_metacharacters`,
/// run once through the mapping's KEYS and once through its VALUES, because
/// both reach an include-style position in the rendered document: the keys
/// become `target.topics.include` entries and the values become
/// `restore.topic_mapping` targets, which the engine's selector reads the same
/// way. `yaml_scalar` quotes both and neutralises neither — quoting is a YAML
/// concern, globbing is the engine's — so a topic legitimately named `orders*`
/// would widen one named entry into a set and the "no wildcard" rail would
/// hold only because nobody had typed one.
///
/// This arm is SEPARABLE from the backup arm on purpose: deleting the call in
/// `render_restore` alone must fail this test and leave the backup-side test
/// passing, which is the mutant the plan lists.
#[test]
fn topic_include_entries_reject_glob_metacharacters_on_the_restore_side() {
    use logweir_engine_oso::render_backup::RenderError;

    for bad in ["orders*", "orders?", "events[1]", "a]b", "x{1}", "y}z"] {
        // ... on the KEY side (the source topic name).
        let mut p = plan();
        p.topic_mapping = [(bad.to_string(), "drill-target".to_string())]
            .into_iter()
            .collect();
        assert_eq!(
            render_restore::render(&p).unwrap_err(),
            RenderError::GlobMetacharacter(bad.to_string()),
            "source topic `{bad}` must be refused as a glob pattern"
        );

        // ... and on the VALUE side (the mapped target name).
        let mut p = plan();
        p.topic_mapping = [("orders".to_string(), bad.to_string())]
            .into_iter()
            .collect();
        assert_eq!(
            render_restore::render(&p).unwrap_err(),
            RenderError::GlobMetacharacter(bad.to_string()),
            "mapped target `{bad}` must be refused as a glob pattern"
        );
    }

    for good in ["orders", "payments", "orders.v2", "a-b_c"] {
        let mut p = plan();
        p.topic_mapping = [(good.to_string(), format!("drill-{good}"))]
            .into_iter()
            .collect();
        assert!(
            render_restore::render(&p).is_ok(),
            "`{good}` is a plain topic name and must render"
        );
    }
}

/// Spec §6.1 M5/N1: `strip_offset_headers: false` is rendered EXPLICITLY in
/// both modes. Rendering `true` here would strip `x-original-offset` on the
/// way into the scratch topic, and that header is the only key phase 7
/// reconciles on (`phase7_verify.rs:243-265`) — a windowed restore would exit
/// 0 and be unverifiable. The exact line, indentation included, so a key
/// emitted at the wrong nesting level is a failure too.
#[test]
fn restore_document_renders_strip_offset_headers_false() {
    let doc =
        render_restore::render(&plan()).expect("G-GLOB: this fixture holds no glob metacharacter");
    assert!(
        doc.lines().any(|l| l == "  strip_offset_headers: false"),
        "expected the exact line `  strip_offset_headers: false`; got:\n{doc}"
    );
    assert!(
        !doc.contains("strip_offset_headers: true"),
        "the only permitted value is false:\n{doc}"
    );
}
