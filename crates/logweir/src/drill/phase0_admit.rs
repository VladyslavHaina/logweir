use crate::drill::DrillError;
use logweir_core::guard::{check_topic_mapping_coverage, scan_forbidden_keys, GuardRefusal};
use logweir_core::spec::{AllowedClusters, Anchor, DrillSpec};
use logweir_kafka::reader::ClusterReader;
use std::collections::BTreeMap;

#[derive(Debug)]
pub struct Admitted {
    pub target_cluster_id: String,
    pub topic_mapping: BTreeMap<String, String>,
}

/// Every check here is OBSERVED. The `spec_text` argument is the raw file
/// bytes, not the parsed struct, so a forbidden key survives round-tripping.
///
/// Returns `DrillError`, not a bare `GuardRefusal`: a reader that cannot be
/// reached or cannot answer (`KafkaError`, via `DrillError::Kafka`) is an
/// OPERATIONAL failure — the plan itself may be fine and the correct action
/// is to retry — and must map to exit 1, never to exit 3's "the plan is
/// refused." Only `GuardRefusal` (via `DrillError::Guard`) means the guard
/// looked at something it could read and refused to proceed.
pub fn run(
    spec: &DrillSpec,
    spec_text: &str,
    allowed: &AllowedClusters,
    reader: &dyn ClusterReader,
) -> Result<Admitted, DrillError> {
    let bad = scan_forbidden_keys(spec_text);
    if !bad.is_empty() {
        return Err(GuardRefusal(format!(
            "forbidden key(s) present in the drill spec, at any value: {}. \
             purge_topics is irreversible, absent from the engine's dry run, has no \
             confirmation gate and truncates EVERY partition of each target topic \
             regardless of partition or time-window filters; dry_run would make the \
             restore a no-op and the measured RTO meaningless; header_preflight_external \
             would silently disable the header scan the drill depends on.",
            bad.join(", ")
        ))
        .into());
    }

    // The two PURELY LOCAL checks run first, before any network round trip. A
    // local refusal should not need a reachable broker, and putting them first
    // is what lets `guard_cli.rs` distinguish "refused by the mapping guard"
    // from "refused because the broker was down".
    let topic_mapping: BTreeMap<String, String> = spec
        .source
        .topics
        .iter()
        .map(|t| {
            (
                t.clone(),
                format!("{}{t}", spec.target.topic_mapping_prefix),
            )
        })
        .collect();
    // `check_topic_mapping_coverage` returns `Result<(), GuardRefusal>`; `?`
    // converts it into `DrillError::Guard` via the `#[from]` impl.
    check_topic_mapping_coverage(&spec.source.topics, &topic_mapping)?;

    // v0.1 implements `head` and refuses the other two, HERE, before anything
    // runs. `logweir_core::spec::Anchor`'s doc comment carries the full
    // reasoning and the measurement; the short version is that phase 4 honours
    // the anchor when choosing which ARCHIVE records to fingerprint while
    // `phase7_verify::verdict_for_selection` reads the TARGET's first
    // `records_per_partition` records, so `tail` and `random` reconcile two
    // different sets of records and report a healthy backup as broken.
    //
    // A REFUSAL, not a downgrade to `head`: silently running a different
    // sample than the approved plan named is the same class of dishonesty as
    // silently restoring a different backup set (`pick_backup_set` refuses
    // that too), and the scorecard would then record an anchor the drill did
    // not apply.
    if spec.sample.anchor != Anchor::Head {
        return Err(GuardRefusal(format!(
            "sample.anchor `{}` is not supported in v0.1; only `head` is. Phase 7 reconciles              the restored topic by reading its FIRST sample.records_per_partition records,              while `{}` selects archive records from elsewhere in the window — the two would              compare different records and report a healthy backup as a failure. Set              `sample.anchor: head`, or omit the field (that is now its default). Refusing              rather than silently sampling `head` under a plan that asked for `{}`.",
            spec.sample.anchor, spec.sample.anchor, spec.sample.anchor
        ))
        .into());
    }

    // FROM HERE ON every failure reaches the network. A `KafkaError` here
    // means the guard could not observe the fact it needed — it is NOT a
    // refusal, and `?` converts it into `DrillError::Kafka` (exit 1), not
    // `DrillError::Guard` (exit 3).
    let target_cluster_id = reader.cluster_id()?;
    if !allowed.allowed_cluster_ids.contains(&target_cluster_id) {
        return Err(GuardRefusal(format!(
            "target cluster id {target_cluster_id} is not in allowedClusterIds"
        ))
        .into());
    }
    if allowed.source_cluster_id.as_deref() == Some(target_cluster_id.as_str()) {
        return Err(GuardRefusal(format!(
            "target cluster id {target_cluster_id} equals the source cluster id"
        ))
        .into());
    }

    let topics = reader.list_topics()?;
    // The marker topic must be CONFIRMED HEALTHY, not merely named in the
    // list: `logweir_kafka::reader::TopicMeta`'s own doc comment assigns this
    // caller the job of checking `error.is_none()` rather than trusting bare
    // presence — a topic mid-leader-election or one this principal cannot
    // describe still appears in `list_topics`'s output (by that same
    // contract) carrying `partitions: 0` and an `error`, and admitting on
    // name alone would let that meaningless metadata reach a later phase.
    match topics.iter().find(|t| t.name == spec.target.marker_topic) {
        Some(t) if t.error.is_none() => {}
        Some(t) => {
            return Err(GuardRefusal(format!(
                "marker topic `{}` exists on cluster {target_cluster_id} but its metadata \
                 carried an error, so its presence cannot be confirmed healthy: {}. \
                 Its existence is the v0.1 segregation proof — recreate it healthy on the \
                 SCRATCH cluster only.",
                spec.target.marker_topic,
                t.error.as_deref().unwrap_or("<no detail>")
            ))
            .into());
        }
        None => {
            return Err(GuardRefusal(format!(
                "marker topic `{}` does not exist on cluster {target_cluster_id}. \
                 Create it on the SCRATCH cluster only — its existence is the v0.1 \
                 segregation proof.",
                spec.target.marker_topic
            ))
            .into());
        }
    }

    Ok(Admitted {
        target_cluster_id,
        topic_mapping,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use logweir_core::engine::StorageUrl;
    use logweir_core::spec::{Notifications, ObjectivesSpec, SampleSpec, SourceSpec, TargetSpec};
    use logweir_kafka::reader::{ConsumedRecord, KafkaError, TopicMeta};

    fn ts(s: &str) -> chrono::DateTime<chrono::Utc> {
        chrono::DateTime::parse_from_rfc3339(s)
            .unwrap()
            .with_timezone(&chrono::Utc)
    }

    fn spec_with(topics: &[&str], prefix: &str) -> DrillSpec {
        DrillSpec {
            source: SourceSpec {
                storage: StorageUrl::Filesystem {
                    path: "/tmp/src".into(),
                },
                backup: "latestCompleted".into(),
                topics: topics.iter().map(|t| t.to_string()).collect(),
            },
            target: TargetSpec {
                bootstrap_servers: vec!["localhost:9092".into()],
                marker_topic: "logweir.scratch".into(),
                topic_mapping_prefix: prefix.into(),
                default_replication_factor: 1,
                teardown: "delete".into(),
            },
            sample: SampleSpec {
                window_start: ts("2026-08-29T00:00:00Z"),
                window_end: ts("2026-08-30T00:00:00Z"),
                records_per_partition: 25,
                anchor: Anchor::Head,
                max_partitions: None,
            },
            objectives: ObjectivesSpec {
                rto_seconds: None,
                rpo_seconds: None,
                pass_rate: None,
            },
            evidence: StorageUrl::Filesystem {
                path: "/tmp/evidence".into(),
            },
            engine_overrides: Default::default(),
            notifications: Notifications::default(),
        }
    }

    fn allowed(ids: &[&str], source: Option<&str>) -> AllowedClusters {
        AllowedClusters {
            allowed_cluster_ids: ids.iter().map(|s| s.to_string()).collect(),
            source_cluster_id: source.map(str::to_string),
        }
    }

    /// A `ClusterReader` double whose every response is configured directly
    /// by the test, scoped to this file only. The SHARED `FakeReader` double
    /// in `crates/logweir/tests/fixtures/mod.rs` is deferred to Task 16
    /// (behind `TargetState`, per addendum A4); this stub carries no such
    /// dependency, so it can prove phase 0's cluster-identity checks now
    /// rather than waiting six tasks for a shared double to exist.
    struct StubReader {
        cluster_id: Result<String, KafkaError>,
        topics: Result<Vec<TopicMeta>, KafkaError>,
    }

    impl ClusterReader for StubReader {
        fn cluster_id(&self) -> Result<String, KafkaError> {
            self.cluster_id.clone()
        }
        fn list_topics(&self) -> Result<Vec<TopicMeta>, KafkaError> {
            self.topics.clone()
        }
        fn end_offsets(&self, _topic: &str) -> Result<Vec<(i32, i64)>, KafkaError> {
            Ok(vec![])
        }
        fn topic_configs(&self, _topic: &str) -> Result<BTreeMap<String, String>, KafkaError> {
            Ok(BTreeMap::new())
        }
        fn consume_range(
            &self,
            _topic: &str,
            _partition: i32,
            _from: i64,
            _max: usize,
        ) -> Result<Vec<ConsumedRecord>, KafkaError> {
            Ok(vec![])
        }
    }

    fn healthy_reader(cluster_id: &str, marker_topic: &str) -> StubReader {
        StubReader {
            cluster_id: Ok(cluster_id.to_string()),
            topics: Ok(vec![TopicMeta::new(marker_topic, 1)]),
        }
    }

    #[test]
    fn a_healthy_target_is_admitted() {
        let spec = spec_with(&["orders"], "drill-");
        let reader = healthy_reader("ALLOWED0000000000000000", &spec.target.marker_topic);
        let admitted = run(
            &spec,
            "restore: {}\n",
            &allowed(&["ALLOWED0000000000000000"], None),
            &reader,
        )
        .unwrap();
        assert_eq!(admitted.target_cluster_id, "ALLOWED0000000000000000");
        assert_eq!(
            admitted.topic_mapping.get("orders"),
            Some(&"drill-orders".to_string())
        );
    }

    #[test]
    fn a_target_cluster_not_in_allowed_cluster_ids_is_a_guard_refusal() {
        let spec = spec_with(&["orders"], "drill-");
        let reader = healthy_reader("WRONG0000000000000000000", &spec.target.marker_topic);
        let err = run(
            &spec,
            "restore: {}\n",
            &allowed(&["ALLOWED0000000000000000"], None),
            &reader,
        )
        .unwrap_err();
        match err {
            DrillError::Guard(GuardRefusal(msg)) => {
                assert!(msg.contains("not in allowedClusterIds"), "{msg}")
            }
            other => panic!("expected a guard refusal (exit 3), got {other:?}"),
        }
    }

    #[test]
    fn a_target_cluster_equal_to_the_source_cluster_is_a_guard_refusal() {
        let spec = spec_with(&["orders"], "drill-");
        let reader = healthy_reader("SAME0000000000000000000A", &spec.target.marker_topic);
        let err = run(
            &spec,
            "restore: {}\n",
            &allowed(
                &["SAME0000000000000000000A"],
                Some("SAME0000000000000000000A"),
            ),
            &reader,
        )
        .unwrap_err();
        match err {
            DrillError::Guard(GuardRefusal(msg)) => {
                assert!(msg.contains("equals the source cluster id"), "{msg}")
            }
            other => panic!("expected a guard refusal (exit 3), got {other:?}"),
        }
    }

    #[test]
    fn a_missing_marker_topic_is_a_guard_refusal_naming_it() {
        let spec = spec_with(&["orders"], "drill-");
        let reader = StubReader {
            cluster_id: Ok("ALLOWED0000000000000000".into()),
            topics: Ok(vec![]),
        };
        let err = run(
            &spec,
            "restore: {}\n",
            &allowed(&["ALLOWED0000000000000000"], None),
            &reader,
        )
        .unwrap_err();
        match err {
            DrillError::Guard(GuardRefusal(msg)) => {
                assert!(msg.contains(&spec.target.marker_topic), "{msg}");
                assert!(msg.contains("does not exist"), "{msg}");
            }
            other => panic!("expected a guard refusal (exit 3), got {other:?}"),
        }
    }

    /// The marker topic's NAME is present but its metadata carried an error
    /// (leader election, an authorization gap, etc.) — `error.is_none()`
    /// must be checked, not just the name, and the error text must be named
    /// in the refusal.
    #[test]
    fn a_marker_topic_present_but_errored_is_a_guard_refusal_naming_the_error() {
        let spec = spec_with(&["orders"], "drill-");
        let reader = StubReader {
            cluster_id: Ok("ALLOWED0000000000000000".into()),
            topics: Ok(vec![TopicMeta::errored(
                spec.target.marker_topic.clone(),
                "leader election in progress",
            )]),
        };
        let err = run(
            &spec,
            "restore: {}\n",
            &allowed(&["ALLOWED0000000000000000"], None),
            &reader,
        )
        .unwrap_err();
        match err {
            DrillError::Guard(GuardRefusal(msg)) => {
                assert!(msg.contains("leader election in progress"), "{msg}");
            }
            other => panic!("expected a guard refusal (exit 3), got {other:?}"),
        }
    }

    /// `tail` and `random` select archive records phase 7's leading-range
    /// read cannot reach, so they are REFUSED at phase 0 rather than
    /// downgraded. Refusing is exit 3 — the plan is not one this version can
    /// honour, and nothing has run.
    #[test]
    fn a_sample_anchor_other_than_head_is_a_guard_refusal_naming_the_limitation() {
        for anchor in [Anchor::Tail, Anchor::Random] {
            let mut spec = spec_with(&["orders"], "drill-");
            spec.sample.anchor = anchor;
            let reader = healthy_reader("ALLOWED0000000000000000", &spec.target.marker_topic);
            let err = run(
                &spec,
                "restore: {}\n",
                &allowed(&["ALLOWED0000000000000000"], None),
                &reader,
            )
            .unwrap_err();
            match err {
                DrillError::Guard(GuardRefusal(msg)) => {
                    assert!(msg.contains("sample.anchor"), "{msg}");
                    assert!(msg.contains(anchor.as_str()), "{msg}");
                    assert!(msg.contains("head"), "{msg}");
                }
                other => panic!("expected a guard refusal (exit 3) for {anchor}, got {other:?}"),
            }
        }
    }

    /// The refusal is LOCAL: it must not need a reachable broker, or a plan
    /// this version cannot honour would report exit 1 ("retry me") on a host
    /// whose target is down. Same property the forbidden-key and mapping
    /// guards have.
    #[test]
    fn the_anchor_refusal_does_not_need_a_reachable_broker() {
        let mut spec = spec_with(&["orders"], "drill-");
        spec.sample.anchor = Anchor::Random;
        let reader = StubReader {
            cluster_id: Err(KafkaError::Unreachable("no broker answered".into())),
            topics: Err(KafkaError::Unreachable("no broker answered".into())),
        };
        match run(
            &spec,
            "restore: {}\n",
            &allowed(&["ALLOWED0000000000000000"], None),
            &reader,
        )
        .unwrap_err()
        {
            DrillError::Guard(GuardRefusal(msg)) => assert!(msg.contains("sample.anchor"), "{msg}"),
            other => panic!("expected a guard refusal (exit 3), got {other:?}"),
        }
    }

    /// A broker that cannot answer `cluster_id` is an OPERATIONAL failure
    /// (exit 1) — the plan may be perfectly fine — never a guard refusal
    /// (exit 3).
    #[test]
    fn a_cluster_id_read_failure_is_operational_not_a_guard_refusal() {
        let spec = spec_with(&["orders"], "drill-");
        let reader = StubReader {
            cluster_id: Err(KafkaError::Unreachable("no broker answered".into())),
            topics: Ok(vec![]),
        };
        let err = run(
            &spec,
            "restore: {}\n",
            &allowed(&["ALLOWED0000000000000000"], None),
            &reader,
        )
        .unwrap_err();
        match err {
            DrillError::Kafka(_) => {}
            other => panic!("expected DrillError::Kafka (exit 1), got {other:?}"),
        }
    }

    /// Same as above for `list_topics`: unreachable is operational, not a
    /// refusal.
    #[test]
    fn a_list_topics_read_failure_is_operational_not_a_guard_refusal() {
        let spec = spec_with(&["orders"], "drill-");
        let reader = StubReader {
            cluster_id: Ok("ALLOWED0000000000000000".into()),
            topics: Err(KafkaError::Unreachable("no broker answered".into())),
        };
        let err = run(
            &spec,
            "restore: {}\n",
            &allowed(&["ALLOWED0000000000000000"], None),
            &reader,
        )
        .unwrap_err();
        match err {
            DrillError::Kafka(_) => {}
            other => panic!("expected DrillError::Kafka (exit 1), got {other:?}"),
        }
    }
}
