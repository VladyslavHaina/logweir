//! **Guard G-TS** — the target topics Logweir creates itself.
//!
//! Spec §10's G-TS row is `[EDIT-DERIVED]` because the obvious instrument is
//! the wrong one: `ClusterReader::topic_configs` describes a topic that does
//! not exist until Logweir creates it (the mapped target topics are absent at
//! phase 0 by construction), so a fixture-driven test over it would guard
//! nothing. What this file proves instead is that phase 0 reads the BROKER's
//! defaults, refuses the two configurations that destroy a restore silently,
//! and that every mapped target topic is created with exactly
//! `TARGET_TOPIC_CONFIGS` in order.
//!
//! **Every test here runs IN PROCESS over doubles.** None runs the binary and
//! none dials a socket: a binary-level arm for the `LogAppendTime` case would
//! need both a broker and a broker configured `LogAppendTime`, so the only
//! binary assertion of that contract is
//! `logweir/e2e/tests/guards.rs`'s residual-3 probe, under
//! `#![cfg(feature = "e2e")]`.
mod fixtures;

use logweir::drill::phase0_admit::{self, TopicPreflight};
use logweir::drill::DrillError;
use logweir::exit::ExitCode;
use logweir_core::guard::GuardRefusal;
use logweir_core::spec::{
    AllowedClusters, Anchor, DrillSpec, Notifications, ObjectivesSpec, SampleSpec, SourceSpec,
    TargetSpec,
};
use logweir_kafka::reader::{
    ClusterReader, ConsumedRecord, KafkaError, NewTopicSpec, TopicCreator, TopicDeleter, TopicMeta,
    TARGET_TOPIC_CONFIGS,
};
use std::collections::BTreeMap;
use std::sync::Mutex;

const CLUSTER: &str = "SCRATCH00000000000000AA";
const MARKER: &str = "logweir.scratch";
const PREFIX: &str = "drill-";

fn pinned() -> Vec<(String, String)> {
    TARGET_TOPIC_CONFIGS
        .iter()
        .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
        .collect()
}

/// A drill spec whose `bootstrap_servers` is the literal `localhost:9092`.
/// That string is why this file is in `no_network_in_unit_tests.rs`'s `ALLOWED`
/// (chain N, STANDING RULE 18): it is DATA handed to doubles and no client is
/// ever constructed from it.
fn spec_with_window_end(window_end: chrono::DateTime<chrono::Utc>) -> DrillSpec {
    DrillSpec {
        name: None,
        source: SourceSpec {
            storage: logweir_core::engine::StorageUrl::Filesystem {
                path: "/tmp/src".into(),
            },
            backup: "latestCompleted".into(),
            topics: vec!["orders".into(), "payments".into()],
            point: None,
        },
        target: TargetSpec {
            bootstrap_servers: vec!["localhost:9092".into()],
            auth: logweir_core::spec::AuthSpec::Plaintext,
            // Task 9b: `scratch` is the default and is what every row in this
            // file is about — the marker and allowlist checks below ARE the
            // scratch segregation proof. `newTopic`'s skips are asserted in
            // `crates/logweir/tests/restore_mode.rs`.
            mode: logweir_core::spec::TargetMode::Scratch,
            topic_naming: None,
            marker_topic: MARKER.into(),
            topic_mapping_prefix: PREFIX.into(),
            default_replication_factor: 1,
            teardown: "delete".into(),
        },
        sample: SampleSpec {
            window_start: window_end - chrono::Duration::hours(1),
            window_end,
            records_per_partition: 25,
            anchor: Anchor::Head,
            max_partitions: None,
            coverage: logweir_core::spec::Coverage::Sampled,
            complete_max_records: None,
        },
        restore: logweir_core::spec::RestoreSpecBlock::default(),
        objectives: ObjectivesSpec {
            rto_seconds: None,
            rpo_seconds: None,
            pass_rate: None,
        },
        evidence: logweir_core::engine::StorageUrl::Filesystem {
            path: "/tmp/evidence".into(),
        },
        engine_overrides: Default::default(),
        notifications: Notifications::default(),
    }
}

fn a_recent_spec() -> DrillSpec {
    spec_with_window_end(chrono::Utc::now() - chrono::Duration::minutes(5))
}

fn allowed() -> AllowedClusters {
    AllowedClusters {
        allowed_cluster_ids: vec![CLUSTER.to_string()],
        source_cluster_id: None,
    }
}

/// The `ClusterReader` half of the doubles.
///
/// `topic_configs` **panics** unless the named topic is one this double was
/// told Logweir had just created. That is the assertion in
/// `phase0_calls_broker_configs_and_never_topic_configs_before_creation`, and
/// it is what kills the "use `topic_configs` in place of `broker_configs`"
/// mutant: the mapped target topics do not exist at phase 0, so a preflight
/// that described one would be describing nothing.
struct BrokerDouble {
    broker: BTreeMap<String, String>,
    /// Topic -> its configuration, for topics that EXIST because Logweir
    /// created them. Any other name panics.
    readable_after_creation: Mutex<BTreeMap<String, BTreeMap<String, String>>>,
    /// Topics the target ALREADY HAS, beyond the marker, as `list_topics`
    /// reports them. Empty for every test but the one that proves spec §6.1's
    /// "a `Restore` refuses if any mapped target topic already exists".
    already_there: Vec<TopicMeta>,
    /// FX-4 / T13: the principal lacks DescribeConfigs on the cluster, so the
    /// broker configuration read is REFUSED — what `RdKafkaReader::
    /// broker_configs` returns now instead of the empty map rdkafka 0.36.2
    /// used to hand back.
    broker_refused: bool,
    /// FX-4 / T13: the probe topic's configuration read is REFUSED.
    readback_refused: bool,
    /// FX-18: how many configuration reads of a just-created topic answer
    /// `TopicNotFound` before it is readable — a creation still propagating.
    readback_lag: std::sync::atomic::AtomicUsize,
    /// FX-18: every `await_served(topic, partitions)` call, in order.
    served: Mutex<Vec<(String, i32)>>,
    /// FX-18: `await_served` answers this instead of `Ok`.
    never_served: Option<KafkaError>,
}

impl BrokerDouble {
    fn new(broker: &[(&str, &str)]) -> Self {
        Self {
            broker: broker
                .iter()
                .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
                .collect(),
            readable_after_creation: Mutex::new(BTreeMap::new()),
            already_there: Vec::new(),
            broker_refused: false,
            readback_refused: false,
            readback_lag: 0.into(),
            served: Mutex::new(Vec::new()),
            never_served: None,
        }
    }

    /// FX-18: the first `n` reads of a just-created topic's configuration
    /// answer `TopicNotFound`, as a broker whose metadata does not hold the
    /// topic yet does.
    fn lagging_readback(self, n: usize) -> Self {
        self.readback_lag
            .store(n, std::sync::atomic::Ordering::SeqCst);
        self
    }

    /// FX-18: a created topic the cluster never serves.
    fn never_serving(mut self, e: KafkaError) -> Self {
        self.never_served = Some(e);
        self
    }

    fn refusing_broker_configs(mut self) -> Self {
        self.broker_refused = true;
        self
    }

    fn refusing_readback(mut self) -> Self {
        self.readback_refused = true;
        self
    }

    /// The target already has this topic — healthy metadata, so its presence
    /// is not in doubt and the refusal cannot be confused with the marker
    /// check's "present but errored" arm.
    fn already_has(mut self, topic: &str, partitions: i32) -> Self {
        self.already_there.push(TopicMeta::new(topic, partitions));
        self
    }

    /// Pre-arm the readback the `LogAppendTime` probe performs: the topic is
    /// only readable because Logweir created it a moment earlier, which is the
    /// whole point of the panic in `topic_configs`.
    fn readable(self, topic: &str, configs: &[(&str, &str)]) -> Self {
        self.readable_after_creation.lock().unwrap().insert(
            topic.to_string(),
            configs
                .iter()
                .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
                .collect(),
        );
        self
    }
}

impl ClusterReader for BrokerDouble {
    fn cluster_id(&self) -> Result<String, KafkaError> {
        Ok(CLUSTER.to_string())
    }
    fn list_topics(&self) -> Result<Vec<TopicMeta>, KafkaError> {
        let mut out = vec![TopicMeta::new(MARKER, 1)];
        out.extend(self.already_there.iter().cloned());
        Ok(out)
    }
    fn end_offsets(&self, _topic: &str) -> Result<Vec<(i32, i64)>, KafkaError> {
        Ok(vec![])
    }
    fn topic_configs(&self, topic: &str) -> Result<BTreeMap<String, String>, KafkaError> {
        if self.readback_refused {
            return Err(KafkaError::NotAuthorized(format!(
                "{topic} (DescribeConfigs answered this visible topic with no configuration)"
            )));
        }
        match self.readable_after_creation.lock().unwrap().get(topic) {
            Some(_)
                if self
                    .readback_lag
                    .fetch_update(
                        std::sync::atomic::Ordering::SeqCst,
                        std::sync::atomic::Ordering::SeqCst,
                        |n| n.checked_sub(1),
                    )
                    .is_ok() =>
            {
                Err(KafkaError::TopicNotFound(topic.to_string()))
            }
            Some(c) => Ok(c.clone()),
            None => panic!(
                "phase 0 read topic_configs({topic:?}), a topic Logweir has not created. The \
                 mapped target topics do not exist at phase 0 by construction, so a TOPIC-resource \
                 DescribeConfigs describes nothing — the broker's own defaults are what phase 0 \
                 has to read, through broker_configs()"
            ),
        }
    }
    fn broker_configs(&self) -> Result<BTreeMap<String, String>, KafkaError> {
        if self.broker_refused {
            return Err(logweir_kafka::reader::empty_broker_config_answer(1001));
        }
        Ok(self.broker.clone())
    }
    fn await_served(
        &self,
        topic: &str,
        partitions: i32,
        _within: std::time::Duration,
    ) -> Result<(), KafkaError> {
        self.served
            .lock()
            .unwrap()
            .push((topic.to_string(), partitions));
        match &self.never_served {
            Some(e) => Err(e.clone()),
            None => Ok(()),
        }
    }
    fn consume_range(
        &self,
        _t: &str,
        _p: i32,
        _from: i64,
        _max: usize,
    ) -> Result<Vec<ConsumedRecord>, KafkaError> {
        Ok(vec![])
    }
}

/// Records every `NewTopicSpec` it is handed, in order.
#[derive(Default)]
struct RecordingCreator {
    calls: Mutex<Vec<NewTopicSpec>>,
}

impl TopicCreator for RecordingCreator {
    fn create_topics(
        &self,
        topics: &[NewTopicSpec],
    ) -> Result<Vec<(String, Result<(), String>)>, KafkaError> {
        self.calls.lock().unwrap().extend_from_slice(topics);
        Ok(topics.iter().map(|t| (t.name.clone(), Ok(()))).collect())
    }
}

#[derive(Default)]
struct RecordingDeleter {
    calls: Mutex<Vec<String>>,
}

impl TopicDeleter for RecordingDeleter {
    fn delete_topics(
        &self,
        names: &[String],
    ) -> Result<Vec<(String, Result<(), String>)>, KafkaError> {
        self.calls.lock().unwrap().extend_from_slice(names);
        Ok(names.iter().map(|n| (n.clone(), Ok(()))).collect())
    }
}

/// One archive manifest's worth of facts: `orders` with three partitions,
/// `payments` with one, which is what makes "the source topic's partition
/// count" an assertable value rather than a constant.
fn facts_for(topics: &[(&str, i32)]) -> logweir_core::engine::BackupSetFacts {
    logweir_core::engine::BackupSetFacts {
        backup_id: "backup-1".into(),
        created_at: chrono::Utc::now(),
        source_cluster_id: Some("SOURCE000000000000000AA".into()),
        manifest_sha256: "sha256:00".into(),
        manifest_version_id: None,
        consumer_group_snapshot_sha256: None,
        topics: topics
            .iter()
            .map(|(n, p)| logweir_core::engine::TopicFacts {
                name: (*n).to_string(),
                original_partition_count: Some(*p),
                source_replication_factor: Some(1),
                configurations: BTreeMap::new(),
                partitions: Vec::new(),
            })
            .collect(),
    }
}

fn guard_message(e: DrillError) -> String {
    let code = e.exit_code();
    match e {
        DrillError::Guard(GuardRefusal(m)) => {
            assert_eq!(
                code,
                ExitCode::GuardRefused,
                "a guard refusal is exit 3 and nothing else"
            );
            m
        }
        other => panic!("expected a guard refusal (exit 3), got {other:?}"),
    }
}

// ---------------------------------------------------------------------------
// G-TS, four arms
// ---------------------------------------------------------------------------

/// **G-TS.** The whole property, in the four arms spec §10's row names. Each is
/// a `#[test]` of its own below, and this one is the roll-up an auditor reads:
/// the run reaches creation, every mapped target topic is created with the
/// pinned config set, a `LogAppendTime` broker that refuses the override is a
/// refusal, and a timestamp bound that excludes the window's end is a refusal.
#[test]
fn restore_creates_target_topics_with_createtime_and_infinite_retention() {
    phase0_calls_broker_configs_and_never_topic_configs_before_creation();
    every_mapped_target_topic_is_created_with_the_pinned_config_set();
    a_logappendtime_broker_that_refuses_the_override_is_a_guard_refusal();
    a_timestamp_bound_that_excludes_the_window_end_is_a_guard_refusal();
}

/// **G-TS arm 1.** The reader double panics from `topic_configs` for any topic
/// Logweir has not created, and the run reaches creation regardless — because
/// phase 0 reads `broker_configs()`, never a topic resource.
///
/// This is the arm that kills the mutant "use `topic_configs` in place of
/// `broker_configs` in the preflight": that version panics inside the double.
#[test]
fn phase0_calls_broker_configs_and_never_topic_configs_before_creation() {
    let spec = a_recent_spec();
    let reader = BrokerDouble::new(&[
        ("log.message.timestamp.type", "CreateTime"),
        ("log.retention.ms", "604800000"),
    ]);
    let creator = RecordingCreator::default();
    let deleter = RecordingDeleter::default();

    let admitted = phase0_admit::run(
        &spec,
        "restore: {}\n",
        &allowed(),
        &reader,
        &creator,
        &deleter,
    )
    .expect("a CreateTime broker admits the plan");
    assert_eq!(admitted.topic_preflight.timestamp_type, "CreateTime");
    assert_eq!(admitted.topic_preflight.retention_ms, "604800000");
    assert_eq!(admitted.topic_preflight.timestamp_bound_ms, None);
    // Phase 0 creates NOTHING on this path: the probe is only for a
    // `LogAppendTime` broker, and the creation step runs later.
    assert!(
        creator.calls.lock().unwrap().is_empty(),
        "phase 0 must not create a topic on a CreateTime broker"
    );

    // …and the run reaches creation.
    let mut preflight = admitted.topic_preflight.clone();
    phase0_admit::create_target_topics(
        &creator,
        &reader,
        &admitted.topic_mapping,
        &facts_for(&[("orders", 3), ("payments", 1)]),
        spec.target.default_replication_factor,
        &mut preflight,
    )
    .expect("creation succeeds");
    assert_eq!(
        preflight.topics_created,
        vec!["drill-orders".to_string(), "drill-payments".to_string()],
        "the run reached creation and created every mapped target topic"
    );
}

/// **G-TS arm 2.** The recorded `Vec<NewTopicSpec>` is asserted equal to an
/// expected value INCLUDING the exact ordered `configs` vector.
///
/// Two mutants die here. "Create a target topic with an empty `configs`" fails
/// on the recorded value; "reorder `TARGET_TOPIC_CONFIGS`" fails on it too,
/// because the assertion is on the ordered vector and order is what
/// `NewTopic::set` is called in.
#[test]
fn every_mapped_target_topic_is_created_with_the_pinned_config_set() {
    let spec = a_recent_spec();
    let reader = BrokerDouble::new(&[("log.message.timestamp.type", "CreateTime")]);
    let creator = RecordingCreator::default();
    let deleter = RecordingDeleter::default();
    let admitted = phase0_admit::run(
        &spec,
        "restore: {}\n",
        &allowed(),
        &reader,
        &creator,
        &deleter,
    )
    .expect("admitted");
    let mut preflight = admitted.topic_preflight.clone();
    phase0_admit::create_target_topics(
        &creator,
        &reader,
        &admitted.topic_mapping,
        &facts_for(&[("orders", 3), ("payments", 1)]),
        spec.target.default_replication_factor,
        &mut preflight,
    )
    .expect("creation succeeds");

    let expected = vec![
        NewTopicSpec {
            name: "drill-orders".into(),
            num_partitions: 3,
            replication_factor: 1,
            configs: vec![
                (
                    "message.timestamp.type".to_string(),
                    "CreateTime".to_string(),
                ),
                ("retention.ms".to_string(), "-1".to_string()),
            ],
        },
        NewTopicSpec {
            name: "drill-payments".into(),
            num_partitions: 1,
            replication_factor: 1,
            configs: vec![
                (
                    "message.timestamp.type".to_string(),
                    "CreateTime".to_string(),
                ),
                ("retention.ms".to_string(), "-1".to_string()),
            ],
        },
    ];
    assert_eq!(*creator.calls.lock().unwrap(), expected);
    // The same set, restated from the shipped constant, so a reorder of
    // `TARGET_TOPIC_CONFIGS` fails here as well as above.
    assert_eq!(
        expected[0].configs,
        pinned(),
        "TARGET_TOPIC_CONFIGS is [message.timestamp.type=CreateTime, retention.ms=-1], in that \
         order"
    );
    assert_eq!(preflight.configs_set, pinned());
}

/// **G-TS arm 3.** A broker reporting `log.message.timestamp.type=LogAppendTime`
/// whose per-topic override reads back `LogAppendTime` is a guard refusal, and
/// the refusal message opens `TargetTopicConfigRefused: `.
///
/// This is the arm that kills "make any of the preflight checks a `warn!`":
/// that version returns `Ok`, and the assertion below expects
/// `ExitCode::GuardRefused`.
#[test]
fn a_logappendtime_broker_that_refuses_the_override_is_a_guard_refusal() {
    let spec = a_recent_spec();
    let reader = BrokerDouble::new(&[
        ("log.message.timestamp.type", "LogAppendTime"),
        ("log.retention.ms", "604800000"),
    ])
    // The readback: the broker did NOT honour the override.
    .readable(
        "drill-orders",
        &[("message.timestamp.type", "LogAppendTime")],
    );
    let creator = RecordingCreator::default();
    let deleter = RecordingDeleter::default();

    let msg = guard_message(
        phase0_admit::run(
            &spec,
            "restore: {}\n",
            &allowed(),
            &reader,
            &creator,
            &deleter,
        )
        .expect_err("a LogAppendTime broker that refuses the override is refused"),
    );
    assert!(
        msg.starts_with("TargetTopicConfigRefused: "),
        "the refusal message must OPEN with the terminal state and its `: ` separator, or \
         `logweir_core::guard::terminal_state` (which matches a prefix) classifies the run as a \
         plain GuardRefused:\n{msg}"
    );
    assert!(msg.contains("LogAppendTime"), "{msg}");
    assert!(msg.contains("message.timestamp.type"), "{msg}");

    // The probe created exactly one topic — the first mapped target — with the
    // pinned config set, and deleted it again, so phase 0 left the target as it
    // found it.
    let created = creator.calls.lock().unwrap().clone();
    assert_eq!(created.len(), 1, "one probe topic, not the whole mapping");
    assert_eq!(created[0].name, "drill-orders");
    assert_eq!(created[0].configs, pinned());
    assert_eq!(
        *deleter.calls.lock().unwrap(),
        vec!["drill-orders".to_string()],
        "the probe topic is deleted through the already-scoped TopicDeleter, so a refusal leaves \
         nothing behind"
    );
}

/// **G-TS arm 3b**, the other side of the probe: a broker on `LogAppendTime`
/// that HONOURS the per-topic override admits the plan.
///
/// Without this the refusal above could be satisfied by refusing every
/// `LogAppendTime` broker unconditionally, which is a different (and weaker)
/// guard: the spec's question is whether the override sticks.
#[test]
fn a_logappendtime_broker_that_honours_the_override_is_admitted() {
    let spec = a_recent_spec();
    let reader = BrokerDouble::new(&[("log.message.timestamp.type", "LogAppendTime")])
        .readable("drill-orders", &[("message.timestamp.type", "CreateTime")]);
    let creator = RecordingCreator::default();
    let deleter = RecordingDeleter::default();
    let admitted = phase0_admit::run(
        &spec,
        "restore: {}\n",
        &allowed(),
        &reader,
        &creator,
        &deleter,
    )
    .expect("an honoured override admits the plan");
    // The observation is recorded verbatim — the BROKER is on LogAppendTime,
    // and that is what `Restore.status.topicPreflight` will say.
    assert_eq!(admitted.topic_preflight.timestamp_type, "LogAppendTime");
    assert_eq!(
        *deleter.calls.lock().unwrap(),
        vec!["drill-orders".to_string()],
        "the probe topic is deleted on this branch too: at phase 0 the manifest's partition count \
         is not known, so a one-partition probe topic left behind would be the wrong target"
    );
}

/// **FX-18.** The `LogAppendTime` probe reads its topic back only once the
/// cluster serves it.
///
/// A broker whose metadata does not hold a just-created topic yet answers its
/// configuration read `TopicNotFound` (or, through T13, an empty answer named
/// "not authorized"). PROD-00.3f's matrix row hit it once in two runs.
///
/// NEGATIVE CONTROL: put `reader.topic_configs(&probe)` back in
/// `target_topic_preflight` and this plan, which the broker honours, exits 1
/// with `TopicNotFound` instead of being admitted.
#[test]
fn the_probe_readback_waits_for_the_topic_phase_0_just_created() {
    let spec = a_recent_spec();
    let reader = BrokerDouble::new(&[("log.message.timestamp.type", "LogAppendTime")])
        .readable("drill-orders", &[("message.timestamp.type", "CreateTime")])
        .lagging_readback(2);
    let creator = RecordingCreator::default();
    let deleter = RecordingDeleter::default();
    let admitted = phase0_admit::run(
        &spec,
        "restore: {}\n",
        &allowed(),
        &reader,
        &creator,
        &deleter,
    )
    .expect("the override is honoured once the topic is readable");
    assert_eq!(admitted.topic_preflight.timestamp_type, "LogAppendTime");
    assert_eq!(
        *reader.served.lock().unwrap(),
        vec![("drill-orders".to_string(), 1)],
        "the one-partition probe is awaited before it is read"
    );
    assert_eq!(
        *deleter.calls.lock().unwrap(),
        vec!["drill-orders".to_string()]
    );
}

/// **FX-18.** Every target topic Logweir creates is SERVED before the engine
/// is handed it, each with the partition count it was created with.
///
/// The engine retries `NOT_LEADER_FOR_PARTITION` on produce but not a
/// partition its first metadata read lists without a leader.
///
/// NEGATIVE CONTROL: delete the `await_served` loop from
/// `create_target_topics` and `served` is empty.
#[test]
fn every_created_target_topic_is_served_before_the_restore_runs() {
    let spec = a_recent_spec();
    let reader = BrokerDouble::new(&[]);
    let creator = RecordingCreator::default();
    let deleter = RecordingDeleter::default();
    let admitted = phase0_admit::run(
        &spec,
        "restore: {}\n",
        &allowed(),
        &reader,
        &creator,
        &deleter,
    )
    .expect("admitted");
    assert!(
        reader.served.lock().unwrap().is_empty(),
        "nothing created yet"
    );
    let mut preflight = admitted.topic_preflight.clone();
    phase0_admit::create_target_topics(
        &creator,
        &reader,
        &admitted.topic_mapping,
        &facts_for(&[("orders", 3), ("payments", 1)]),
        spec.target.default_replication_factor,
        &mut preflight,
    )
    .expect("created and served");
    let created: Vec<String> = creator
        .calls
        .lock()
        .unwrap()
        .iter()
        .map(|t| t.name.clone())
        .collect();
    let served = reader.served.lock().unwrap().clone();
    assert_eq!(
        served,
        vec![
            ("drill-orders".to_string(), 3),
            ("drill-payments".to_string(), 1)
        ]
    );
    assert_eq!(
        served.iter().map(|(t, _)| t.clone()).collect::<Vec<_>>(),
        created,
        "exactly the created topics, no more"
    );
}

/// **FX-18.** A created target topic the cluster never serves is an
/// operational failure (exit 1) naming the topic, and `topics_created` still
/// names every topic this run created. Nothing tears them down on this error:
/// the run returns before phase 9, as it does for a failed create in the same
/// batch, so the record is for whoever removes them by hand.
#[test]
fn a_created_target_topic_that_is_never_served_is_operational() {
    let spec = a_recent_spec();
    let reader = BrokerDouble::new(&[]).never_serving(KafkaError::Client(
        "drill-orders/0: ListOffsets: NotLeaderForPartition".into(),
    ));
    let creator = RecordingCreator::default();
    let deleter = RecordingDeleter::default();
    let admitted = phase0_admit::run(
        &spec,
        "restore: {}\n",
        &allowed(),
        &reader,
        &creator,
        &deleter,
    )
    .expect("admitted");
    let mut preflight = admitted.topic_preflight.clone();
    let e = phase0_admit::create_target_topics(
        &creator,
        &reader,
        &admitted.topic_mapping,
        &facts_for(&[("orders", 3), ("payments", 1)]),
        spec.target.default_replication_factor,
        &mut preflight,
    )
    .expect_err("a topic that is never served is not handed to the engine");
    assert_eq!(e.exit_code(), ExitCode::Operational, "{e}");
    let text = e.to_string();
    assert!(text.contains("drill-orders"), "{text}");
    assert!(text.contains("NotLeaderForPartition"), "{text}");
    assert_eq!(
        preflight.topics_created,
        vec!["drill-orders".to_string(), "drill-payments".to_string()]
    );
}

/// **G-TS arm 4.** A timestamp bound that excludes the window's end is a guard
/// refusal naming `TargetTopicConfigRefused`.
///
/// `before.max.ms` = 3600000 (one hour) against a window end 48 h old.
#[test]
fn a_timestamp_bound_that_excludes_the_window_end_is_a_guard_refusal() {
    let spec = spec_with_window_end(chrono::Utc::now() - chrono::Duration::hours(48));
    let reader = BrokerDouble::new(&[
        ("log.message.timestamp.type", "CreateTime"),
        ("log.message.timestamp.before.max.ms", "3600000"),
    ]);
    let creator = RecordingCreator::default();
    let deleter = RecordingDeleter::default();
    let msg = guard_message(
        phase0_admit::run(
            &spec,
            "restore: {}\n",
            &allowed(),
            &reader,
            &creator,
            &deleter,
        )
        .expect_err("a window end outside the broker's bound is refused"),
    );
    assert!(msg.starts_with("TargetTopicConfigRefused: "), "{msg}");
    assert!(msg.contains("3600000"), "{msg}");
    assert!(
        creator.calls.lock().unwrap().is_empty(),
        "a refusal knowable by comparison must not write to the target first"
    );
}

/// **The bound is checked against the window end THIS restore asks for.**
///
/// Since Task 9 `RestorePlan.time_window.1` is `restore.point_in_time` when
/// the spec states one and `sample.window_end` otherwise, so phase 0 makes the
/// same choice: reading `sample.window_end` unconditionally would compare the
/// broker's bound against a timestamp this restore never requests — passing a
/// plan the broker will reject record by record, or refusing one it would have
/// accepted.
///
/// Both directions are asserted, because the repoint was correct and entirely
/// unasserted: "read `sample.window_end` even when `point_in_time` is present"
/// survived `cargo test --workspace` at 814 passed / 0 failed (task-9 review,
/// MED-3). Arm 1 dies on the missing refusal, arm 2 on a refusal that should
/// not have happened, and arm 1 also pins the `restore.point_in_time` label
/// the message carries.
#[test]
fn the_timestamp_bound_is_checked_against_the_recovery_point_when_the_spec_states_one() {
    let reader_configs: &[(&str, &str)] = &[
        ("log.message.timestamp.type", "CreateTime"),
        ("log.message.timestamp.before.max.ms", "3600000"),
    ];

    // ARM 1 — the sample window's end is INSIDE the one-hour bound and the
    // requested recovery point is 48 h outside it. The plan asks the engine
    // for the recovery point, so this is a refusal.
    let mut spec = spec_with_window_end(chrono::Utc::now() - chrono::Duration::minutes(5));
    let recovery_point = chrono::Utc::now() - chrono::Duration::hours(48);
    spec.restore.point_in_time = Some(recovery_point);
    let creator = RecordingCreator::default();
    let deleter = RecordingDeleter::default();
    let msg = guard_message(
        phase0_admit::run(
            &spec,
            "restore: {}\n",
            &allowed(),
            &BrokerDouble::new(reader_configs),
            &creator,
            &deleter,
        )
        .expect_err("a RECOVERY POINT outside the broker's bound is refused"),
    );
    assert!(msg.starts_with("TargetTopicConfigRefused: "), "{msg}");
    assert!(
        msg.contains("restore.point_in_time"),
        "the refusal names the field it read, not `sample.window_end`: {msg}"
    );
    assert!(
        msg.contains(&recovery_point.timestamp_millis().to_string()),
        "and the integer it compared: {msg}"
    );
    assert!(
        creator.calls.lock().unwrap().is_empty(),
        "a refusal knowable by comparison must not write to the target first"
    );

    // ARM 2 — the mirror. The SAMPLE window's end is 48 h old, outside the
    // bound, while the requested recovery point is five minutes old and inside
    // it. Nothing may be refused: the engine is never asked for the sample
    // window's end.
    let mut spec = spec_with_window_end(chrono::Utc::now() - chrono::Duration::hours(48));
    spec.restore.point_in_time = Some(chrono::Utc::now() - chrono::Duration::minutes(5));
    let creator = RecordingCreator::default();
    let deleter = RecordingDeleter::default();
    let admitted = phase0_admit::run(
        &spec,
        "restore: {}\n",
        &allowed(),
        &BrokerDouble::new(reader_configs),
        &creator,
        &deleter,
    )
    .expect("a recovery point INSIDE the bound is admitted, whatever the sample window says");
    assert_eq!(
        admitted.topic_preflight.timestamp_bound_ms,
        Some(3_600_000),
        "the bound was read — arm 2 passes because the comparison used the \
         recovery point, not because the check was skipped"
    );
}

/// The deprecated spelling still counts: before Kafka 3.6 the key is
/// `log.message.timestamp.difference.max.ms`, and an adopter on 3.5 gets the
/// same refusal.
#[test]
fn the_pre_3_6_timestamp_bound_spelling_is_read_too() {
    let spec = spec_with_window_end(chrono::Utc::now() - chrono::Duration::hours(48));
    let reader = BrokerDouble::new(&[("log.message.timestamp.difference.max.ms", "3600000")]);
    let creator = RecordingCreator::default();
    let deleter = RecordingDeleter::default();
    let msg = guard_message(
        phase0_admit::run(
            &spec,
            "restore: {}\n",
            &allowed(),
            &reader,
            &creator,
            &deleter,
        )
        .expect_err("refused"),
    );
    assert!(msg.starts_with("TargetTopicConfigRefused: "), "{msg}");
}

/// The Apache default for both bound keys is `9223372036854775807`, i.e.
/// unbounded. A plain `now - bound` overflows: it panics in a debug build and
/// wraps to a floor in the FUTURE in a release build, which would refuse every
/// window on every default broker. `saturating_sub` is the fix and this is the
/// test that would have caught its absence.
#[test]
fn the_apache_default_timestamp_bound_refuses_nothing() {
    let spec = spec_with_window_end(chrono::Utc::now() - chrono::Duration::days(365));
    let reader = BrokerDouble::new(&[
        ("log.message.timestamp.type", "CreateTime"),
        ("log.message.timestamp.before.max.ms", "9223372036854775807"),
        (
            "log.message.timestamp.difference.max.ms",
            "9223372036854775807",
        ),
    ]);
    let creator = RecordingCreator::default();
    let deleter = RecordingDeleter::default();
    let admitted = phase0_admit::run(
        &spec,
        "restore: {}\n",
        &allowed(),
        &reader,
        &creator,
        &deleter,
    )
    .expect("an unbounded broker refuses nothing, even for a year-old window");
    assert_eq!(
        admitted.topic_preflight.timestamp_bound_ms,
        Some(9_223_372_036_854_775_807),
        "the value is still RECORDED verbatim; it just refuses nothing"
    );
}

/// A broker that cannot answer DescribeConfigs is OPERATIONAL (exit 1), never a
/// guard refusal (exit 3): nothing about the plan was observed, and the correct
/// action is to retry. Same contract `cluster_id` and `list_topics` already
/// have in this phase.
#[test]
fn a_broker_configs_read_failure_is_operational_not_a_guard_refusal() {
    struct Unreachable;
    impl ClusterReader for Unreachable {
        fn cluster_id(&self) -> Result<String, KafkaError> {
            Ok(CLUSTER.to_string())
        }
        fn list_topics(&self) -> Result<Vec<TopicMeta>, KafkaError> {
            Ok(vec![TopicMeta::new(MARKER, 1)])
        }
        fn end_offsets(&self, _t: &str) -> Result<Vec<(i32, i64)>, KafkaError> {
            Ok(vec![])
        }
        fn topic_configs(&self, _t: &str) -> Result<BTreeMap<String, String>, KafkaError> {
            unimplemented!()
        }
        fn broker_configs(&self) -> Result<BTreeMap<String, String>, KafkaError> {
            Err(KafkaError::Unreachable(
                "no broker returned its configuration within 20s".into(),
            ))
        }
        fn consume_range(
            &self,
            _t: &str,
            _p: i32,
            _f: i64,
            _m: usize,
        ) -> Result<Vec<ConsumedRecord>, KafkaError> {
            Ok(vec![])
        }
    }
    let creator = RecordingCreator::default();
    let deleter = RecordingDeleter::default();
    let err = phase0_admit::run(
        &a_recent_spec(),
        "restore: {}\n",
        &allowed(),
        &Unreachable,
        &creator,
        &deleter,
    )
    .expect_err("an unreachable broker is an error");
    assert_eq!(err.exit_code(), ExitCode::Operational, "{err:?}");
}

/// **FX-4 / T13, consumer 2.** A principal without DescribeConfigs on the
/// cluster used to get an EMPTY broker configuration back, and G-TS read the
/// missing `log.message.timestamp.type` as the Apache default `CreateTime` —
/// so on a `LogAppendTime` target the override probe never ran and the
/// timestamp bound was never checked. The read now REFUSES, and phase 0 stops
/// on it (exit 1, a `KafkaError`: the plan may be fine; the principal is not),
/// before writing anything.
///
/// Negative control: make phase 0 swallow the refusal as an empty map
/// (`reader.broker_configs().unwrap_or_default()`) and this test fails: the
/// plan is admitted as `CreateTime` with the probe skipped.
#[test]
fn a_refused_broker_configuration_read_is_never_read_as_the_apache_default() {
    // The broker IS on LogAppendTime — the principal simply may not read it.
    let reader = BrokerDouble::new(&[("log.message.timestamp.type", "LogAppendTime")])
        .refusing_broker_configs();
    let creator = RecordingCreator::default();
    let deleter = RecordingDeleter::default();
    let err = phase0_admit::run(
        &a_recent_spec(),
        "restore: {}\n",
        &allowed(),
        &reader,
        &creator,
        &deleter,
    )
    .expect_err("a refused broker configuration read must not admit the plan as CreateTime");
    assert_eq!(err.exit_code(), ExitCode::Operational, "{err:?}");
    assert!(err.to_string().contains("not authorized"), "{err}");
    assert!(
        creator.calls.lock().unwrap().is_empty() && deleter.calls.lock().unwrap().is_empty(),
        "nothing is written on the target when its configuration cannot be read"
    );
}

/// **FX-4 / T13, consumer 3.** On a `LogAppendTime` broker the override probe
/// creates a topic and reads its configuration back. A REFUSED readback used
/// to be an empty map, which G-TS then read as the BROKER's type and refused
/// with exit 3 claiming the broker "REFUSED a per-topic override" — a finding
/// about the broker that nothing observed. It is now operational (exit 1),
/// naming the refusal, and the probe topic is still deleted.
///
/// Negative control: `readback.unwrap_or_default()` in phase 0 turns this back
/// into the exit-3 override verdict, and this test fails.
#[test]
fn a_refused_probe_readback_is_operational_never_an_override_verdict() {
    let reader =
        BrokerDouble::new(&[("log.message.timestamp.type", "LogAppendTime")]).refusing_readback();
    let creator = RecordingCreator::default();
    let deleter = RecordingDeleter::default();
    let err = phase0_admit::run(
        &a_recent_spec(),
        "restore: {}\n",
        &allowed(),
        &reader,
        &creator,
        &deleter,
    )
    .expect_err("a refused readback proves nothing about the override");
    assert_eq!(
        err.exit_code(),
        ExitCode::Operational,
        "the override was never read, so no guard verdict about it may be issued: {err:?}"
    );
    assert!(err.to_string().contains("not authorized"), "{err}");
    assert_eq!(
        *deleter.calls.lock().unwrap(),
        vec!["drill-orders".to_string()],
        "the probe topic is deleted on this path too"
    );
}

// ---------------------------------------------------------------------------
// I9 — the refusal-reason line, in process
// ---------------------------------------------------------------------------

/// **Interface I9**, second producer half. The refusal built by the
/// `a_logappendtime_broker_that_refuses_the_override_is_a_guard_refusal`
/// fixture is handed to `logweir_core::guard::refusal_reason_line` and the
/// result is byte-equal to `refusal-reason=TargetTopicConfigRefused`; then
/// `logweir::exit::print_refusal_to`, the writer `print_refusal` prints stdout
/// through, is asserted to END its output with exactly that line and its
/// newline, after the one `refusal-detail=` line FX-34 put before it. (The
/// printer of the state line alone is gone: FX-34's review, L5.)
///
/// **In process, never the binary** — a binary-level arm would need both a
/// broker and a `LogAppendTime` broker, so the only binary assertion of this
/// contract lives in `logweir/e2e/tests/guards.rs` beside the residual-3 probe,
/// under `#![cfg(feature = "e2e")]`.
///
/// This is the arm that kills the mutant "open the refusal message with
/// `target topic config refused: ` (lower case, no colon-prefixed state)":
/// `terminal_state` matches a PREFIX, so the line would read
/// `refusal-reason=GuardRefused`.
#[test]
fn a_target_topic_refusal_prints_its_terminal_state() {
    let spec = a_recent_spec();
    let reader = BrokerDouble::new(&[("log.message.timestamp.type", "LogAppendTime")]).readable(
        "drill-orders",
        &[("message.timestamp.type", "LogAppendTime")],
    );
    let creator = RecordingCreator::default();
    let deleter = RecordingDeleter::default();
    let msg = guard_message(
        phase0_admit::run(
            &spec,
            "restore: {}\n",
            &allowed(),
            &reader,
            &creator,
            &deleter,
        )
        .expect_err("refused"),
    );

    assert_eq!(
        logweir_core::guard::refusal_reason_line(&msg),
        "refusal-reason=TargetTopicConfigRefused"
    );

    // …and the runner actually WRITES it, to stdout, through this seam: the
    // detail line, then the state line LAST.
    let mut captured: Vec<u8> = Vec::new();
    logweir::exit::print_refusal_to(
        &mut captured,
        logweir_core::refusal_detail::RefusingRun::Restore,
        None,
        &msg,
    )
    .expect("write");
    let written = String::from_utf8(captured).expect("utf8");
    let lines: Vec<&str> = written.split_inclusive('\n').collect();
    assert_eq!(lines.len(), 2, "two lines, nothing after them: {written}");
    assert!(
        lines[0].starts_with("refusal-detail={\"code\":\"TargetTopicConfigRefused\","),
        "{written}"
    );
    assert_eq!(
        lines[1], "refusal-reason=TargetTopicConfigRefused\n",
        "the line, and its newline — a controller tailing pods/log reads the final line, and the \
         pod log API has no stream selector, so stderr would not be distinguishable at all"
    );
}

/// `TargetTopicConfigRefused` is one of the three declared terminal states, so
/// a controller's exit-3 map is closed over it. Task 3 declared the constant for
/// this task; this is the assertion that it is now actually produced.
#[test]
fn the_terminal_state_this_task_emits_is_one_of_the_declared_three() {
    assert!(
        logweir_core::guard::TERMINAL_STATES
            .contains(&logweir_core::guard::TERMINAL_STATE_TARGET_TOPIC_CONFIG_REFUSED),
        "TargetTopicConfigRefused must be in TERMINAL_STATES"
    );
    assert_eq!(
        logweir_core::guard::TERMINAL_STATE_TARGET_TOPIC_CONFIG_REFUSED,
        "TargetTopicConfigRefused"
    );
}

// ---------------------------------------------------------------------------
// The preflight leaves the run in `RestoreOutcome`
// ---------------------------------------------------------------------------

/// The preflight leaves a WHOLE RUN, not just the phase: a full drill over the
/// orchestrator fixture's doubles, through `execute_with_outcome`.
///
/// Deliberately **not** `topic_preflight_lands_in_the_scorecard`: it is not a
/// scorecard field. Global Constraint 12 as amended freezes the document at 21
/// top-level properties and 17 required ones and permits nested optional fields
/// only, so a `topic_preflight` block would make the count 22 and break Task
/// 5's `the_scorecard_top_level_shape_is_unchanged`. Spec §10's G-TS row puts it
/// in `Restore.status.topicPreflight`, which the operator writes from this
/// outcome.
#[test]
fn topic_preflight_lands_in_the_run_outcome() {
    let f = fixtures::orchestrator_args_against_fixture_engine();
    let outcome = logweir::drill::execute_with_outcome(&f.args, &f.run_id, &f.ctx)
        .expect("the fixture drill passes");
    assert_eq!(outcome.topic_preflight.configs_set, pinned());
    assert_eq!(
        outcome.topic_preflight.topics_created,
        vec!["drill-orders".to_string()],
        "the mapped target names, and nothing else"
    );
    // The scorecard is still the scorecard: this block is not in it.
    let json = serde_json::to_value(&outcome.scorecard).expect("the scorecard serialises");
    assert!(
        json.get("topic_preflight").is_none(),
        "topic_preflight is NOT a scorecard field (Global Constraint 12 as amended)"
    );
}

/// The scorecard-returning half is unchanged, so the ~40 existing call sites in
/// `orchestrator.rs` and `teardown.rs` keep meaning what they meant.
#[test]
fn execute_with_still_returns_the_scorecard_alone() {
    let f = fixtures::orchestrator_args_against_fixture_engine();
    let sc = logweir::drill::execute_with(&f.args, &f.run_id, &f.ctx).expect("passes");
    assert_eq!(sc.outcome, logweir_core::outcome::Outcome::Pass);
}

/// The `TopicPreflight` a run reports is the one phase 0 built: same broker
/// observations, with `topics_created` filled in by the creation step. Nothing
/// re-derives it.
#[test]
fn the_outcome_preflight_is_the_one_phase_0_built() {
    let spec = a_recent_spec();
    let reader = BrokerDouble::new(&[
        ("log.message.timestamp.type", "CreateTime"),
        ("log.retention.ms", "604800000"),
        ("log.message.timestamp.before.max.ms", "9223372036854775807"),
    ]);
    let creator = RecordingCreator::default();
    let deleter = RecordingDeleter::default();
    let admitted = phase0_admit::run(
        &spec,
        "restore: {}\n",
        &allowed(),
        &reader,
        &creator,
        &deleter,
    )
    .expect("admitted");
    let mut preflight = admitted.topic_preflight.clone();
    phase0_admit::create_target_topics(
        &creator,
        &reader,
        &admitted.topic_mapping,
        &facts_for(&[("orders", 3), ("payments", 1)]),
        spec.target.default_replication_factor,
        &mut preflight,
    )
    .expect("created");
    assert_eq!(
        preflight,
        TopicPreflight {
            timestamp_type: "CreateTime".into(),
            retention_ms: "604800000".into(),
            timestamp_bound_ms: Some(9_223_372_036_854_775_807),
            configs_set: pinned(),
            topics_created: vec!["drill-orders".into(), "drill-payments".into()],
        }
    );
}

// ---------------------------------------------------------------------------
// Spec §6.1 — a `Restore` refuses if any mapped target topic already exists.
// Fix round 1, review finding 1.
// ---------------------------------------------------------------------------

/// A `TopicCreator` that records what it was asked for and answers
/// `TopicAlreadyExists` for one named topic — the broker's own error string,
/// which is what `TopicCreator`'s `Result<(), String>` contract carries
/// (Global Constraint 1: `crates/logweir` links no rdkafka, so a double must
/// be able to produce the same value the real client does).
#[derive(Default)]
struct CreatorThatFindsOneTopicPresent {
    calls: Mutex<Vec<NewTopicSpec>>,
    present: String,
}

impl TopicCreator for CreatorThatFindsOneTopicPresent {
    fn create_topics(
        &self,
        topics: &[NewTopicSpec],
    ) -> Result<Vec<(String, Result<(), String>)>, KafkaError> {
        self.calls.lock().unwrap().extend_from_slice(topics);
        Ok(topics
            .iter()
            .map(|t| {
                if t.name == self.present {
                    (
                        t.name.clone(),
                        Err("Broker: Topic already exists".to_string()),
                    )
                } else {
                    (t.name.clone(), Ok(()))
                }
            })
            .collect())
    }
}

/// **Spec §6.1, review finding 1.** A mapped target topic that already exists
/// is REFUSED at phase 0 — exit 3, `refusal-reason=GuardRefused`, the topic
/// named, nothing created and nothing deleted.
///
/// This is the arm that kills the mutant "reuse the existing topic (warn and
/// continue) instead of refusing": that version returns `Ok`, and every
/// assertion below expects a `GuardRefusal`. It also pins the two facts that
/// made the reuse a false claim — `TopicPreflight.configs_set` reports the
/// pinned pair "as applied" and `topics_created` named a topic Logweir did not
/// create, both of which reach `Restore.status.topicPreflight`.
#[test]
fn a_mapped_target_topic_that_already_exists_is_a_guard_refusal_naming_it() {
    let spec = a_recent_spec();
    let reader = BrokerDouble::new(&[
        ("log.message.timestamp.type", "CreateTime"),
        ("log.retention.ms", "604800000"),
    ])
    .already_has("drill-payments", 3);
    let creator = RecordingCreator::default();
    let deleter = RecordingDeleter::default();

    let e = phase0_admit::run(
        &spec,
        "restore: {}\n",
        &allowed(),
        &reader,
        &creator,
        &deleter,
    )
    .expect_err("a mapped target topic that already exists is refused");
    // `guard_message` asserts exit 3 on the way through.
    let m = guard_message(e);
    assert!(
        m.contains("`drill-payments`"),
        "the refusal must NAME the topic an operator has to deal with: {m:?}"
    );
    assert!(
        m.contains("already exist"),
        "and say what is wrong with it: {m:?}"
    );
    // NOT the declared terminal state for this task: that one is for a target
    // CONFIGURATION this build refuses. A target that merely exists is exit 3
    // under the general state.
    assert_eq!(
        logweir_core::guard::refusal_reason_line(&m),
        "refusal-reason=GuardRefused",
        "TargetTopicConfigRefused is for a config finding, not for existence"
    );
    // Nothing was written. Phase 0's ONLY write is the `LogAppendTime` probe,
    // and this broker is `CreateTime`; more to the point the existence check
    // runs BEFORE the probe, so even a `LogAppendTime` broker would not have
    // created a topic under a name that is already taken.
    assert!(
        creator.calls.lock().unwrap().is_empty(),
        "a refused plan creates nothing: {:?}",
        creator.calls.lock().unwrap()
    );
    assert!(
        deleter.calls.lock().unwrap().is_empty(),
        "and deletes nothing: {:?}",
        deleter.calls.lock().unwrap()
    );
}

/// The same refusal on a `LogAppendTime` broker, which is the case that would
/// otherwise WRITE: the probe creates the first mapped target name, and it may
/// only do that to a name phase 0 has just proved absent. Ordering, pinned.
#[test]
fn the_existence_check_runs_before_the_logappendtime_probe_writes() {
    let spec = a_recent_spec();
    let reader = BrokerDouble::new(&[("log.message.timestamp.type", "LogAppendTime")])
        // `drill-orders` is the FIRST mapped target, i.e. exactly the name the
        // probe would create.
        .already_has("drill-orders", 3);
    let creator = RecordingCreator::default();
    let deleter = RecordingDeleter::default();

    let m = guard_message(
        phase0_admit::run(
            &spec,
            "restore: {}\n",
            &allowed(),
            &reader,
            &creator,
            &deleter,
        )
        .expect_err("refused"),
    );
    assert!(m.contains("`drill-orders`"), "{m:?}");
    assert!(
        creator.calls.lock().unwrap().is_empty(),
        "the probe must not create a topic that already exists — it would then delete somebody \
         else's topic on the way out: {:?}",
        creator.calls.lock().unwrap()
    );
    assert!(
        deleter.calls.lock().unwrap().is_empty(),
        "and above all it must not DELETE it: {:?}",
        deleter.calls.lock().unwrap()
    );
}

/// **Review finding 1(b).** `topics_created` names only topics THIS RUN
/// created, so `configs_set`'s "as applied" is true of every one of them.
///
/// Two halves, and the second is the one that used to be false: a broker that
/// answers `TopicAlreadyExists` for a target is `DrillError::Operational`
/// (exit 1) and that name does NOT enter `topics_created`. The old code
/// warned, continued, and pushed the name — so
/// `Restore.status.topicPreflight` claimed `retention.ms=-1` over a topic that
/// kept whatever retention it had.
#[test]
fn topics_created_names_only_the_topics_this_run_created() {
    let spec = a_recent_spec();
    let facts = facts_for(&[("orders", 3), ("payments", 1)]);

    // The passing path: every name in `topics_created` is a name the creator
    // was asked for and confirmed, and `configs_set` is the pinned pair.
    let reader = BrokerDouble::new(&[("log.message.timestamp.type", "CreateTime")]);
    let creator = RecordingCreator::default();
    let deleter = RecordingDeleter::default();
    let admitted = phase0_admit::run(
        &spec,
        "restore: {}\n",
        &allowed(),
        &reader,
        &creator,
        &deleter,
    )
    .expect("admitted");
    let mut preflight = admitted.topic_preflight.clone();
    phase0_admit::create_target_topics(
        &creator,
        &reader,
        &admitted.topic_mapping,
        &facts,
        spec.target.default_replication_factor,
        &mut preflight,
    )
    .expect("created");
    let asked_for: Vec<String> = creator
        .calls
        .lock()
        .unwrap()
        .iter()
        .map(|s| s.name.clone())
        .collect();
    assert_eq!(preflight.topics_created, asked_for);
    for name in &preflight.topics_created {
        let spec_for_name = creator
            .calls
            .lock()
            .unwrap()
            .iter()
            .find(|s| &s.name == name)
            .cloned()
            .unwrap_or_else(|| panic!("{name} is in topics_created but was never created"));
        assert_eq!(
            spec_for_name.configs,
            pinned(),
            "configs_set claims this pair was applied to {name}"
        );
    }
    assert_eq!(preflight.configs_set, pinned());

    // The racing path: admitted, then the target gains one of the names before
    // the creation step reaches it.
    let racing = CreatorThatFindsOneTopicPresent {
        calls: Mutex::new(Vec::new()),
        present: "drill-payments".to_string(),
    };
    let mut preflight = admitted.topic_preflight.clone();
    let e = phase0_admit::create_target_topics(
        &racing,
        &reader,
        &admitted.topic_mapping,
        &facts,
        spec.target.default_replication_factor,
        &mut preflight,
    )
    .expect_err("a target topic that appeared after admission is not silently reused");
    assert_eq!(
        e.exit_code(),
        ExitCode::Operational,
        "by here phases 0-5 have run, so Global Constraint 11 does not allow exit 3: {e:?}"
    );
    // The stop is NAMED (exit 1, `CreatedTopicsLeft`): the topic this run
    // created before the broker refused the other is left in place and named,
    // never deleted (no code path deletes a restored-into name).
    let DrillError::CreationStopped(stop) = e else {
        panic!("expected the named creation stop")
    };
    let m = &stop.message;
    assert!(m.contains("drill-payments"), "{m:?}");
    assert_eq!(stop.reason, "CreatedTopicsLeft");
    assert!(stop.appeared.is_empty(), "{:?}", stop.appeared);
    assert_eq!(stop.left, preflight.topics_created);
    assert!(!stop.left.is_empty());
    assert!(
        !preflight
            .topics_created
            .contains(&"drill-payments".to_string()),
        "a topic this run did not create must never reach topics_created: {:?}",
        preflight.topics_created
    );
}

// ---------------------------------------------------------------------------
// Review finding 2 — the phase order, in process.
// ---------------------------------------------------------------------------

/// **Review finding 2.** The creation step runs AFTER phase 5's verdict, and
/// that ordering was pinned only behind Docker
/// (`e2e/tests/full_drill.rs::a_corrupted_segment_yields_exit_2_and_a_signed_preflight_failed_scorecard`,
/// which asserts `!topic_exists("drill-orders")`). The whole default suite
/// stayed green with the call moved before the `Verdict::Block` branch.
///
/// This is that assertion in process: a drill whose phase-5 preflight BLOCKS
/// created nothing on the target, and a drill that passes created the mapped
/// target — so the test fails both for a creation step moved earlier and for
/// one deleted altogether.
#[test]
fn a_blocked_preflight_creates_no_target_topic() {
    let blocked = fixtures::orchestrator_fixture(fixtures::Drill::BlocksAtPreflight);
    let e = logweir::drill::execute_with(&blocked.args, &blocked.run_id, &blocked.ctx)
        .expect_err("a blocked preflight is not a pass");
    assert_eq!(
        e.exit_code(),
        ExitCode::DrillNotPass,
        "phase 5 blocked, so the run ends in a signed preflight-failed scorecard: {e:?}"
    );
    assert!(
        fixtures::created_topics(&blocked).is_empty(),
        "a blocked preflight must not have written to the cluster — the creation step belongs \
         AFTER phase 5's verdict, because the Verdict::Block branch returns with NO teardown on \
         the ground that this drill created nothing: {:?}",
        fixtures::created_topics(&blocked)
    );

    // The control, so the assertion above cannot pass by the creation step
    // having been removed: the same fixture, passing, DOES create.
    let passing = fixtures::orchestrator_fixture(fixtures::Drill::Passes);
    logweir::drill::execute_with(&passing.args, &passing.run_id, &passing.ctx).expect("passes");
    let created: Vec<String> = fixtures::created_topics(&passing)
        .into_iter()
        .map(|s| s.name)
        .collect();
    assert_eq!(created, vec!["drill-orders".to_string()]);
}

// ---------------------------------------------------------------------------
// The PRODUCER half of erratum E10(c) — Task 24 fix round 1
// ---------------------------------------------------------------------------

/// A source file from the workspace, read from this crate's manifest
/// directory.
fn workspace_source(relative: &str) -> String {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .join(relative);
    std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("{}: {e}", path.display()))
}

/// The body of the first `fn <name>` in `src`, from its opening brace to the
/// matching close, braces counted.
///
/// Text, and deliberately so: the assertion below is about a `println!` whose
/// only observable effect is on the process's real stdout, reached only on
/// exit 0 of a full drill against a real broker and a real archive. See
/// `the_topic_preflight_line_is_printed_by_name_on_a_passing_run` for why that
/// makes this the strongest instrument available in process.
fn fn_body<'a>(src: &'a str, signature: &str) -> &'a str {
    let at = src
        .find(signature)
        .unwrap_or_else(|| panic!("`{signature}` is in the source"));
    let rest = &src[at..];
    let open = rest.find('{').expect("the function has a body");
    let mut depth = 0usize;
    for (i, c) in rest[open..].char_indices() {
        match c {
            '{' => depth += 1,
            '}' => {
                depth -= 1;
                if depth == 0 {
                    return &rest[open..=open + i];
                }
            }
            _ => {}
        }
    }
    panic!("`{signature}`'s body is unbalanced");
}

/// **THE RUNNER PRINTS `topic-preflight=` BY NAME ON A PASSING RUN** — the
/// PRODUCER half of plan erratum **E10(c)**, which the Task 24 review found
/// pinned by nothing at all: deleting the line survived the entire workspace
/// suite.
///
/// # Why this row is in process and what it can and cannot reach
///
/// `drill::exiting` is a private function that writes with `println!` to the
/// process's real stdout, and the branch that prints this line is reached only
/// at `ExitCode::Ok` — a full drill that dialled a broker, restored into it and
/// signed a scorecard. **There is no writer seam** (`exit::print_refusal_to`
/// is the precedent for one, and it covers interface I9's line, not this one),
/// and libtest gives a test no way to read back its own captured stdout. So
/// the print itself is not observable in process without a broker, and this row
/// says so rather than pretending otherwise. What it does instead is pin the
/// two halves the print is made of, and pin that the print is still there:
///
/// 1. **The VALUE, off a real passing run.** The `TopicPreflight` comes from
///    `execute_with_outcome` over the orchestrator fixture — the same object
///    `exiting` is handed — never from a literal, so the line's shape is the
///    one a passing drill actually produces.
/// 2. **The SHAPE the controller scans (erratum E4).** ONE line, the key by
///    NAME at its head, a single-line JSON object after it, and the three keys
///    spelled as `Restore.status.topicPreflight`'s own camelCase fields. A pod
///    log is stdout and stderr merged in nondeterministic order, so the
///    controller reads a BOUNDED TAIL and matches by key name; a value carrying
///    a newline would cost two of those eight lines and could be split across
///    them.
/// 3. **The PRINT, in `exiting`'s body.** Text, over the source — the mutant
///    this row exists for is "delete the `println!`", and the body is where
///    that is decidable without a cluster.
/// 4. **The two crates agree on the key.** `weirkeeper`'s scanner constant is
///    read out of its own source and compared, because the producer and the
///    consumer cannot share a type: `weirkeeper` must not depend on `logweir`
///    at all (`scripts/check-one-signer.sh`'s check 1), so the two halves meet
///    only in a pod log and only on this string.
///
/// KILLS: **M18** — dropping the `topic-preflight=` producer line, which
/// survived the whole workspace suite before this row; renaming the key on
/// either side; and emitting a multi-line or non-JSON value.
#[test]
fn the_topic_preflight_line_is_printed_by_name_on_a_passing_run() {
    // ---- 1. The value, off a real passing run ---------------------------
    let f = fixtures::orchestrator_args_against_fixture_engine();
    let outcome = logweir::drill::execute_with_outcome(&f.args, &f.run_id, &f.ctx)
        .expect("the fixture drill passes, which is the only exit code that prints this line");
    let line = format!(
        "{}{}",
        phase0_admit::TOPIC_PREFLIGHT_KEY_PREFIX,
        outcome.topic_preflight.status_line_value()
    );

    // ---- 2. The shape the controller's bounded tail scan needs ----------
    assert!(
        line.starts_with("topic-preflight="),
        "the key is at the HEAD of the line, because the controller matches by key name over a \
         bounded tail and never by position: {line}"
    );
    assert_eq!(
        line.lines().count(),
        1,
        "ONE LINE. Three separate `topic-preflight-*=` lines would spend three of the \
         controller's eight tail lines on one fact and crowd out interface I8's own keys: {line}"
    );
    let value = line
        .strip_prefix(phase0_admit::TOPIC_PREFLIGHT_KEY_PREFIX)
        .expect("the prefix is the prefix");
    let parsed: serde_json::Value =
        serde_json::from_str(value).unwrap_or_else(|e| panic!("the value is JSON: {e}\n{line}"));
    let object = parsed
        .as_object()
        .unwrap_or_else(|| panic!("…and it is an OBJECT: {line}"));
    assert_eq!(
        object
            .get("timestampType")
            .and_then(serde_json::Value::as_str),
        Some("CreateTime"),
        "the three keys are `Restore.status.topicPreflight`'s own camelCase fields, byte for \
         byte — the controller copies them across without renaming anything, so a fourth or a \
         differently-spelled field is DROPPED rather than misfiled: {line}"
    );
    for k in object.keys() {
        assert!(
            ["timestampType", "retentionMs", "timestampBound"].contains(&k.as_str()),
            "`{k}` is not one of the CRD's three fields: {line}"
        );
    }

    // ---- 3. THE PRINT ITSELF, in `exiting`'s body ----------------------
    let src = workspace_source("crates/logweir/src/drill/mod.rs");
    let body = fn_body(&src, "fn exiting(");
    assert!(
        body.contains("TOPIC_PREFLIGHT_KEY_PREFIX"),
        "`drill::exiting` must still PRINT the line. Deleting it is mutant M18, and before this \
         row it survived the entire workspace suite: the value above would still be computed, \
         the controller would still scan for the key, and nothing would ever emit it. Body:\n\
         {body}"
    );
    assert!(
        body.contains("status_line_value()"),
        "…and it prints the value this function built, not a second rendering of the same fact"
    );
    let printed = body
        .find("TOPIC_PREFLIGHT_KEY_PREFIX")
        .expect("just asserted");
    let println_at = body[..printed]
        .rfind("println!")
        .expect("the key reaches stdout through `println!`, which is the pod's log");
    assert!(
        body[println_at..printed].len() < 80,
        "the `println!` is the one that carries the key, not an unrelated one above it"
    );
    assert!(
        body[..println_at].contains("ExitCode::Ok"),
        "it is printed only on a PASSING run: a refused or crashed drill has no completed phase \
         0 to report, and a line naming a preflight nobody performed is worse than the absence \
         an operator already renders. Body:\n{body}"
    );

    // ---- 4. Producer and consumer spell the key the same ---------------
    //
    // Read out of `weirkeeper`'s source, not imported: `weirkeeper` must not
    // be a dependency of this crate in any direction —
    // `scripts/check-one-signer.sh`'s check 1 counts normal, build and dev
    // edges and pins the set of crates from which `logweir-evidence` is
    // reachable at exactly `{logweir, e2e}`.
    let scanner = workspace_source("crates/weirkeeper/src/controllers/restore.rs");
    assert!(
        scanner.contains(&format!(
            "TOPIC_PREFLIGHT_KEY_PREFIX: &str = \"{}\"",
            phase0_admit::TOPIC_PREFLIGHT_KEY_PREFIX
        )),
        "the controller scans for `{}` and this crate prints it; the two halves meet nowhere but \
         in a pod log, so the only thing keeping them in step is this comparison",
        phase0_admit::TOPIC_PREFLIGHT_KEY_PREFIX
    );
}

/// **FX-20 fix round (review F1), phase 0's own arm.** The restore runner
/// refuses an injected S3 region before the archive is read
/// (`guard_cli.rs::fx20_an_injected_region_exits_3_before_the_archive_is_read`);
/// phase 0 holds the same rule for every caller that reaches it another way,
/// on the source archive and on the evidence store, before any broker call.
/// CONTROL: the same plan with real regions is admitted.
#[test]
fn fx20_phase0_refuses_an_injected_region_on_either_store() {
    let s3 = |region: &str| logweir_core::engine::StorageUrl::S3 {
        bucket: "victim-backups".into(),
        prefix: "logweir/".into(),
        region: Some(region.into()),
        endpoint: None,
        path_style: false,
        allow_http: false,
    };
    let reader = || BrokerDouble::new(&[("log.message.timestamp.type", "CreateTime")]);
    for field in ["source.storage", "evidence"] {
        let mut spec = a_recent_spec();
        if field == "evidence" {
            spec.evidence = s3("x@127.0.0.1:9/");
        } else {
            spec.source.storage = s3("x@127.0.0.1:9/");
        }
        let creator = RecordingCreator::default();
        let deleter = RecordingDeleter::default();
        let refusal = phase0_admit::run(
            &spec,
            "restore: {}\n",
            &allowed(),
            &reader(),
            &creator,
            &deleter,
        )
        .expect_err("an injected region is refused at phase 0");
        let DrillError::Guard(GuardRefusal(message)) = refusal else {
            panic!("{field}: a guard refusal, not {refusal:?}");
        };
        assert!(
            message.starts_with(&format!("StorageRegionInvalid: {field}.region")),
            "{field}: {message}"
        );
        assert!(creator.calls.lock().unwrap().is_empty());
    }
    let mut spec = a_recent_spec();
    spec.source.storage = s3("eu-west-1");
    spec.evidence = s3("eu-west-1");
    phase0_admit::run(
        &spec,
        "restore: {}\n",
        &allowed(),
        &reader(),
        &RecordingCreator::default(),
        &RecordingDeleter::default(),
    )
    .expect("real regions are admitted");
}
