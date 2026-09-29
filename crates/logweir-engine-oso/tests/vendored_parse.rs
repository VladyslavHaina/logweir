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

// ---------------------------------------------------------------------------
// FX-1: the consumer-groups snapshot, against bytes the engine really wrote.
// ---------------------------------------------------------------------------

mod snapshot {
    use logweir_engine_oso::vendored::consumer_groups::{parse, Position};
    use std::collections::{BTreeMap, BTreeSet};

    /// Written by the pinned engine (`backup.consumer_group_snapshot: true`)
    /// on the compose stack; provenance in `e2e/fixtures/README.md`.
    const FIXTURE: &str = "../../e2e/fixtures/consumer-groups-snapshot.json";
    const EMPTY_FIXTURE: &str = "../../e2e/fixtures/consumer-groups-snapshot-empty.json";
    /// The broker's own account of the same groups at the same moment.
    const COMMITTED: &str = "../../e2e/fixtures/consumer-groups-snapshot.committed.json";

    fn bytes(path: &str) -> Vec<u8> {
        std::fs::read(path).unwrap_or_else(|e| panic!("{path}: {e}"))
    }

    /// group -> positions, as the broker reported them, restricted to the
    /// topics the backup archived (the only ones the engine keeps), plus the
    /// positions it reported on topics the backup did NOT archive.
    type ByGroup = BTreeMap<String, Vec<Position>>;
    type LeftOut = BTreeSet<(String, String)>;

    fn committed() -> (ByGroup, LeftOut) {
        let doc: serde_json::Value = serde_json::from_slice(&bytes(COMMITTED)).unwrap();
        let archived: BTreeSet<&str> = doc["archived_topics"]
            .as_array()
            .unwrap()
            .iter()
            .map(|t| t.as_str().unwrap())
            .collect();
        let (mut kept, mut left_out) = (ByGroup::new(), LeftOut::new());
        for row in doc["committed"].as_array().unwrap() {
            let group = row["group_id"].as_str().unwrap().to_string();
            let topic = row["topic"].as_str().unwrap().to_string();
            if archived.contains(topic.as_str()) {
                kept.entry(group).or_default().push(Position {
                    topic,
                    partition: row["partition"].as_i64().unwrap() as i32,
                    offset: row["offset"].as_i64().unwrap(), // engine-token-ok: a JSON field name of the committed-offsets oracle, not an engine subcommand
                });
            } else {
                left_out.insert((group, topic));
            }
        }
        for v in kept.values_mut() {
            v.sort();
        }
        (kept, left_out)
    }

    /// **The FX-1 regression row.** The engine's real bytes parse, and every
    /// position equals what the broker says the group committed. Under the
    /// invented shape (`offsets` a list) the parse itself failed: "invalid
    /// type: map, expected a sequence".
    #[test]
    fn the_engine_written_snapshot_parses_to_exactly_what_the_groups_committed() {
        let s = parse(&bytes(FIXTURE)).expect("the engine's own bytes parse");
        // The writer's `chrono::Utc::now().timestamp_millis()`, read from the
        // bytes (2026-09-29T07:34:57.677Z). Reverting the field to the old
        // name `captured_at` leaves this `None`.
        assert_eq!(s.snapshot_time, Some(1_790_667_297_677));
        assert!(
            s.extra.is_empty(),
            "the engine writes no other top-level field"
        );

        let got: ByGroup = s
            .groups
            .iter()
            .map(|g| {
                assert!(
                    g.extra.is_empty(),
                    "{}: no per-group field but offsets",
                    g.group_id
                );
                (g.group_id.clone(), g.positions().expect("positions"))
            })
            .collect();
        let (want, left_out) = committed();
        assert_eq!(
            got, want,
            "snapshot positions != the broker's committed positions"
        );
        assert_eq!(got.values().map(Vec::len).sum::<usize>(), 10);

        // What the writer LEAVES OUT (backup/engine.rs:883-903): a group whose
        // only position is on an unarchived topic, and that position of a
        // group that also has an archived one.
        assert!(!got.contains_key("fx1-unarchived-only"));
        assert_eq!(
            left_out,
            BTreeSet::from([
                ("fx1-mixed".to_string(), "test-topic".to_string()),
                ("fx1-unarchived-only".to_string(), "test-topic".to_string()),
            ])
        );
    }

    /// **The negative control: the old parser fails on these bytes.** The
    /// structs that stood in `vendored/consumer_groups.rs` before FX-1,
    /// verbatim (at `632ea345`), refuse the committed fixture with the exact
    /// error the drill reported on the compose stack. If the fixture ever
    /// stopped discriminating — replaced by an empty snapshot, say, which the
    /// old parser read happily — this fails, and the regression row above
    /// would no longer be a regression row.
    #[test]
    fn the_parser_that_stood_here_before_fx1_refuses_the_engines_bytes() {
        use serde::Deserialize;
        use serde_json::Value;
        use std::collections::HashMap;

        #[allow(dead_code)]
        #[derive(Debug, Deserialize)]
        struct ConsumerGroupsSnapshot {
            #[serde(default)]
            backup_id: String,
            #[serde(default)]
            captured_at: i64,
            #[serde(default)]
            groups: Vec<ConsumerGroupEntry>,
            #[serde(flatten)]
            extra: HashMap<String, Value>,
        }
        #[allow(dead_code)]
        #[derive(Debug, Deserialize)]
        struct ConsumerGroupEntry {
            #[serde(default)]
            group_id: String,
            #[serde(default)]
            state: String,
            #[serde(default)]
            offsets: Vec<Value>,
            #[serde(flatten)]
            extra: HashMap<String, Value>,
        }

        let e = serde_json::from_slice::<ConsumerGroupsSnapshot>(&bytes(FIXTURE))
            .expect_err("the invented shape cannot read what the engine writes");
        assert!(
            e.to_string()
                .starts_with("invalid type: map, expected a sequence"),
            "{e}"
        );
        // …while the EMPTY snapshot never exposed it: it has no `offsets`.
        serde_json::from_slice::<ConsumerGroupsSnapshot>(&bytes(EMPTY_FIXTURE))
            .expect("the old parser read an empty snapshot");
    }

    /// Absent-or-empty behaves as it always did: zero groups, never an error.
    /// The first file is the engine's own empty snapshot (no group had
    /// committed yet); `{}` is the minimal object.
    #[test]
    fn an_empty_snapshot_parses_to_zero_groups_not_an_error() {
        let s = parse(&bytes(EMPTY_FIXTURE)).expect("the engine's empty snapshot parses");
        assert!(s.groups.is_empty());
        assert_eq!(s.snapshot_time, Some(1_790_667_238_804));
        let s = parse(b"{}").expect("`{}` parses");
        assert!(s.groups.is_empty());
        assert_eq!(s.snapshot_time, None, "absent, never an invented epoch");
    }

    /// The documented choice for an unknown field: TOLERATED and KEPT, at both
    /// levels, and the positions are unchanged by it.
    #[test]
    fn an_unknown_field_is_kept_at_both_levels_not_refused() {
        let mut v: serde_json::Value = serde_json::from_slice(&bytes(FIXTURE)).unwrap();
        let before = parse(&bytes(FIXTURE)).unwrap();
        v["a_future_field"] = serde_json::json!({"x": 1});
        v["groups"][0]["state"] = "Stable".into();
        let s = parse(&serde_json::to_vec(&v).unwrap()).expect("unknown fields are tolerated");
        assert_eq!(s.extra["a_future_field"], serde_json::json!({"x": 1}));
        assert_eq!(s.groups[0].extra["state"], "Stable");
        assert_eq!(
            s.groups[0].positions().unwrap(),
            before.groups[0].positions().unwrap()
        );
    }

    /// The shape that stood in the vendored file before FX-1: `offsets` as a
    /// LIST is a type the engine never writes, and it is refused, not read as
    /// "no positions".
    #[test]
    fn offsets_as_a_flat_list_is_refused() {
        let old = br#"{"backup_id":"b","captured_at":1,"groups":[
            {"group_id":"g","state":"Stable","offsets":[]}]}"#;
        let e = parse(old).expect_err("a list is not the engine's shape");
        assert!(e.to_string().contains("expected a map"), "{e}");
    }

    /// What the writer never emits is refused rather than skipped: an import
    /// that dropped a position would read as "no committed offset".
    #[test]
    fn values_the_writer_never_emits_are_refused() {
        for (doc, needle) in [
            (
                r#"{"groups":[{"group_id":"g","offsets":{"t":{"x":1}}}]}"#,
                "is not a partition id",
            ),
            (
                r#"{"groups":[{"group_id":"g","offsets":{"t":{"01":1}}}]}"#,
                "is not a partition id",
            ),
            (
                r#"{"groups":[{"group_id":"g","offsets":{"t":{"-1":1}}}]}"#,
                "is not a partition id",
            ),
            (
                r#"{"groups":[{"group_id":"g","offsets":{"t":{"0":-1}}}]}"#,
                "is negative",
            ),
            (
                r#"{"groups":[{"group_id":"g"},{"group_id":"g"}]}"#,
                "appears twice",
            ),
            (r#"{"groups":[{"offsets":{}}]}"#, "group_id"),
            (
                r#"{"groups":[{"group_id":"g","offsets":{"t":{"0":"5"}}}]}"#,
                "invalid type",
            ),
        ] {
            let e = parse(doc.as_bytes()).expect_err(doc);
            assert!(e.to_string().contains(needle), "{doc}: {e}");
        }
    }

    /// Positions come out in partition order as a NUMBER, whatever order the
    /// writer's HashMap put the keys in (`"10"` sorts before `"2"` as text).
    #[test]
    fn positions_are_ordered_by_partition_number() {
        let s = parse(br#"{"groups":[{"group_id":"g","offsets":{"t":{"10":1,"2":3}}}]}"#).unwrap();
        let p: Vec<i32> = s.groups[0]
            .positions()
            .unwrap()
            .iter()
            .map(|p| p.partition)
            .collect();
        assert_eq!(p, vec![2, 10]);
    }
}
