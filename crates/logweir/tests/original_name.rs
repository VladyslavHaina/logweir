//! **PROD-15.1 — restore under the original topic name into an absent topic:
//! the runner's conditions, in process, over doubles.**
//!
//! Every row drives `phase0_admit::run_with_original_name` (or the creation
//! step, or phase 9) over a broker double that answers exactly what the row
//! needs, and every refusal row asserts BOTH the exit code and that nothing
//! was written: the creator and the deleter are recording doubles, and a
//! refusal that created or deleted anything fails its row. The live rows are
//! `e2e/tests/original_name.rs`.
//!
//! The bootstrap literals below are data handed to doubles; no client is ever
//! constructed (`no_network_in_unit_tests.rs`).

use logweir::drill::{phase0_admit, phase9_teardown, DrillError};
use logweir::exit::ExitCode;
use logweir_core::backup_receipt::TopicOwner;
use logweir_core::original_name::ReceiptOwners;
use logweir_core::spec::{
    AllowedClusters, Anchor, DrillSpec, Notifications, ObjectivesSpec, OriginalNameSpec,
    RestoreSpecBlock, SampleSpec, SourceSpec, TargetMode, TargetSpec, TopicNaming,
};
use logweir_core::topic_configuration::DeclaredOwner;
use logweir_kafka::reader::{
    ClusterReader, ConsumedRecord, KafkaError, NewTopicSpec, TopicCreator, TopicDeleter, TopicMeta,
};
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

/// The target cluster the doubles report.
const TARGET: &str = "TARGET0000000000000000";
/// Another cluster's id: the source when the restore goes to a second cluster.
const OTHER: &str = "SOURCE0000000000000000";
const SCRATCH_PREFIX: &str = "drill-";

fn ts(s: &str) -> chrono::DateTime<chrono::Utc> {
    chrono::DateTime::parse_from_rfc3339(s)
        .expect("an RFC 3339 instant")
        .with_timezone(&chrono::Utc)
}

/// An original-name plan for `orders` and `payments`: `newTopic`, `prefix:
/// ""`, and the block. `owners` is the plan's owner statement.
fn plan(owners: Option<Vec<DeclaredOwner>>, owner_path: bool) -> DrillSpec {
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
            bootstrap_servers: vec!["target.example.invalid:9092".into()],
            auth: logweir_core::spec::AuthSpec::Plaintext,
            mode: TargetMode::NewTopic,
            topic_naming: Some(TopicNaming {
                prefix: String::new(),
                original_name: Some(OriginalNameSpec { owners, owner_path }),
            }),
            marker_topic: "logweir.scratch".into(),
            topic_mapping_prefix: SCRATCH_PREFIX.into(),
            default_replication_factor: 1,
            teardown: "delete".into(),
        },
        sample: SampleSpec {
            window_start: ts("2026-09-07T12:00:00Z"),
            window_end: ts("2026-09-07T15:00:00Z"),
            records_per_partition: 25,
            anchor: Anchor::Head,
            max_partitions: None,
            coverage: logweir_core::spec::Coverage::Sampled,
            complete_max_records: None,
        },
        restore: RestoreSpecBlock {
            point_in_time: Some(ts("2026-09-07T14:05:00Z")),
            time_basis: None,
            window_start: None,
            partitions: BTreeMap::new(),
        },
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

/// The plan with the approver's statement that no owner manages any name.
fn plan_no_owner() -> DrillSpec {
    plan(Some(Vec::new()), false)
}

fn allowed(source: Option<&str>) -> AllowedClusters {
    AllowedClusters {
        allowed_cluster_ids: vec![],
        source_cluster_id: source.map(str::to_string),
    }
}

/// A broker double: the topics it has, and what EACH broker answers for
/// `auto.create.topics.enable` (`None`: the key is absent from that broker's
/// answer). `log_append_time` arms G-TS's probe.
struct Broker {
    topics: Vec<TopicMeta>,
    auto_create: Vec<(i32, Option<String>)>,
    log_append_time: bool,
    reads_of_auto_create: Mutex<usize>,
    /// Topics a `Creator` sharing this list created: listed from then on, as
    /// a real cluster would (review M4's cleanup reads them back).
    created: Arc<Mutex<Vec<TopicMeta>>>,
    /// A created topic's configuration is not the one Logweir set (someone
    /// deleted and recreated it, or it was auto-created).
    foreign_configs: bool,
}

impl Broker {
    fn with_auto_create(values: &[Option<&str>]) -> Self {
        Self {
            topics: Vec::new(),
            auto_create: values
                .iter()
                .enumerate()
                .map(|(i, v)| (i32::try_from(i).unwrap() + 1, v.map(str::to_string)))
                .collect(),
            log_append_time: false,
            reads_of_auto_create: Mutex::new(0),
            created: Arc::new(Mutex::new(Vec::new())),
            foreign_configs: false,
        }
    }
    fn disabled() -> Self {
        Self::with_auto_create(&[Some("false"), Some("false")])
    }
    fn having(mut self, topic: &str) -> Self {
        self.topics.push(TopicMeta::new(topic, 3));
        self
    }
    fn on_log_append_time(mut self) -> Self {
        self.log_append_time = true;
        self
    }
}

impl ClusterReader for Broker {
    fn cluster_id(&self) -> Result<String, KafkaError> {
        Ok(TARGET.to_string())
    }
    fn list_topics(&self) -> Result<Vec<TopicMeta>, KafkaError> {
        let mut topics = self.topics.clone();
        topics.extend(self.created.lock().unwrap().iter().cloned());
        Ok(topics)
    }
    fn end_offsets(&self, _topic: &str) -> Result<Vec<(i32, i64)>, KafkaError> {
        Ok(vec![])
    }
    fn topic_configs(&self, _topic: &str) -> Result<BTreeMap<String, String>, KafkaError> {
        // The probe's read-back (the broker honours the per-topic override),
        // and what a topic Logweir created carries: both pinned entries.
        let retention = if self.foreign_configs {
            "604800000"
        } else {
            "-1"
        };
        Ok(BTreeMap::from([
            (
                "message.timestamp.type".to_string(),
                "CreateTime".to_string(),
            ),
            ("retention.ms".to_string(), retention.to_string()),
        ]))
    }
    fn broker_configs(&self) -> Result<BTreeMap<String, String>, KafkaError> {
        let ts_type = if self.log_append_time {
            "LogAppendTime"
        } else {
            "CreateTime"
        };
        Ok(BTreeMap::from([
            (
                "log.message.timestamp.type".to_string(),
                ts_type.to_string(),
            ),
            ("log.retention.ms".to_string(), "-1".to_string()),
        ]))
    }
    fn broker_config_value_all(&self, key: &str) -> Result<Vec<(i32, Option<String>)>, KafkaError> {
        assert_eq!(key, "auto.create.topics.enable");
        *self.reads_of_auto_create.lock().unwrap() += 1;
        Ok(self.auto_create.clone())
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

/// A creator that records every spec, and answers `TOPIC_ALREADY_EXISTS` for
/// the names it is told someone else created.
#[derive(Default)]
struct Creator {
    calls: Mutex<Vec<NewTopicSpec>>,
    taken: Vec<String>,
    /// Where created topics are listed (the `Broker`'s `created`).
    lists_into: Option<Arc<Mutex<Vec<TopicMeta>>>>,
    /// Created with this many partitions instead of the asked count (a
    /// topic someone recreated differently).
    partitions_override: Option<i32>,
    /// Answer only for these names (review L1's short answer).
    answers_only: Option<Vec<String>>,
}

impl TopicCreator for Creator {
    fn create_topics(
        &self,
        topics: &[NewTopicSpec],
    ) -> Result<Vec<(String, Result<(), String>)>, KafkaError> {
        self.calls.lock().unwrap().extend_from_slice(topics);
        Ok(topics
            .iter()
            .filter(|t| {
                self.answers_only
                    .as_ref()
                    .is_none_or(|only| only.contains(&t.name))
            })
            .map(|t| {
                if self.taken.contains(&t.name) {
                    (t.name.clone(), Err(rdkafka_already_exists()))
                } else {
                    if let Some(list) = &self.lists_into {
                        list.lock().unwrap().push(TopicMeta::new(
                            &t.name,
                            self.partitions_override.unwrap_or(t.num_partitions),
                        ));
                    }
                    (t.name.clone(), Ok(()))
                }
            })
            .collect())
    }
}

/// The text `RdKafkaReader::create_topics` reports for `TOPIC_ALREADY_EXISTS`
/// — read from the helper the creation step itself classifies by, so this
/// double and the real creator cannot disagree.
fn rdkafka_already_exists() -> String {
    // `RDKafkaErrorCode`'s `Display`: `{:?} ({rd_kafka_err2str})`.
    let text = "TopicAlreadyExists (Broker: Topic already exists)";
    assert!(
        logweir_kafka::rdkafka_reader::is_topic_already_exists(text),
        "the creation step no longer classifies rdkafka's rendering of TOPIC_ALREADY_EXISTS"
    );
    assert!(!logweir_kafka::rdkafka_reader::is_topic_already_exists(
        "InvalidReplicationFactor (Broker: Invalid replication factor)"
    ));
    text.to_string()
}

#[derive(Default)]
struct Deleter {
    calls: Mutex<Vec<String>>,
    /// Every name handed to the review-M4 cleanup.
    unwritten_calls: Mutex<Vec<String>>,
    /// Records a topic holds when the cleanup re-reads its end offsets.
    records: BTreeMap<String, i64>,
}

impl TopicDeleter for Deleter {
    fn delete_topics(
        &self,
        names: &[String],
    ) -> Result<Vec<(String, Result<(), String>)>, KafkaError> {
        self.calls.lock().unwrap().extend_from_slice(names);
        Ok(names.iter().map(|n| (n.clone(), Ok(()))).collect())
    }
    fn delete_unwritten_created(
        &self,
        names: &[String],
    ) -> Result<Vec<(String, Result<(), String>)>, KafkaError> {
        self.unwritten_calls
            .lock()
            .unwrap()
            .extend_from_slice(names);
        Ok(names
            .iter()
            .map(|n| match self.records.get(n) {
                Some(r) if *r > 0 => (
                    n.clone(),
                    Err(format!(
                        "left: it holds {r} record(s) this run did not write"
                    )),
                ),
                _ => (n.clone(), Ok(())),
            })
            .collect())
    }
}

struct Run {
    result: Result<phase0_admit::Admitted, DrillError>,
    created: Vec<NewTopicSpec>,
    deleted: Vec<String>,
}

/// Phase 0 over the doubles. `source` is the source cluster id the bound
/// point's VERIFIED receipt measured (review L3: the only source id that
/// counts); `inputs` may name its own, which wins.
fn admit(
    spec: &DrillSpec,
    source: Option<&str>,
    broker: &Broker,
    inputs: &phase0_admit::OriginalNameInputs,
) -> Run {
    let mut inputs = inputs.clone();
    if inputs.receipt_source_cluster_id.is_none() {
        inputs.receipt_source_cluster_id = source.map(str::to_string);
    }
    admit_with_allowlist(spec, None, broker, &inputs)
}

/// Phase 0 with the allowlist file naming `allowlist_source` as its
/// `source_cluster_id` — unsigned runner input.
fn admit_with_allowlist(
    spec: &DrillSpec,
    allowlist_source: Option<&str>,
    broker: &Broker,
    inputs: &phase0_admit::OriginalNameInputs,
) -> Run {
    let creator = Creator::default();
    let deleter = Deleter::default();
    let result = phase0_admit::run_with_original_name(
        spec,
        &serde_yaml::to_string(spec).expect("the spec serialises"),
        &allowed(allowlist_source),
        broker,
        &creator,
        &deleter,
        inputs,
    );
    Run {
        result,
        created: creator.calls.into_inner().unwrap(),
        deleted: deleter.calls.into_inner().unwrap(),
    }
}

fn no_inputs() -> phase0_admit::OriginalNameInputs {
    phase0_admit::OriginalNameInputs::default()
}

/// Exit 3, the message opens with `token`, and nothing was created or deleted.
fn refused(run: Run, token: &str) -> String {
    let message = match run.result {
        Ok(a) => panic!("expected `{token}`, got admitted: {a:?}"),
        Err(e) => {
            assert_eq!(e.exit_code(), ExitCode::GuardRefused, "{e}");
            e.to_string()
        }
    };
    assert!(
        message.contains(token),
        "expected `{token}` in the refusal, got: {message}"
    );
    assert!(
        run.created.is_empty(),
        "a refusal created {:?}",
        run.created
    );
    assert!(
        run.deleted.is_empty(),
        "a refusal deleted {:?}",
        run.deleted
    );
    message
}

fn admitted(run: Run) -> (phase0_admit::Admitted, Vec<NewTopicSpec>, Vec<String>) {
    match run.result {
        Ok(a) => (a, run.created, run.deleted),
        Err(e) => panic!("expected the plan admitted, got: {e}"),
    }
}

// ---------------------------------------------------------------------------
// Condition 1: the shape, and the identity ban that stays
// ---------------------------------------------------------------------------

/// The identity ban stays in scratch mode. KILLS: dropping `refuse_shape`'s
/// scratch arm (the mapping guard would then refuse nothing, because scratch
/// maps through `drill-`).
#[test]
fn the_block_in_scratch_mode_is_refused() {
    let mut spec = plan_no_owner();
    spec.target.mode = TargetMode::Scratch;
    refused(
        admit(&spec, Some(OTHER), &Broker::disabled(), &no_inputs()),
        "OriginalNameNotNewTopic",
    );
}

/// A plan that names two names is refused. KILLS: dropping the prefix arm.
#[test]
fn the_block_beside_a_prefix_is_refused() {
    let mut spec = plan_no_owner();
    spec.target.topic_naming.as_mut().unwrap().prefix = "restore-".into();
    refused(
        admit(&spec, Some(OTHER), &Broker::disabled(), &no_inputs()),
        "OriginalNamePrefixNotEmpty",
    );
}

/// An empty prefix NOBODY opted into keeps the old refusal. KILLS: allowing
/// the identity mapping for any empty prefix.
#[test]
fn an_empty_prefix_without_the_block_is_still_refused_as_onto_itself() {
    let mut spec = plan_no_owner();
    spec.target.topic_naming.as_mut().unwrap().original_name = None;
    refused(
        admit(&spec, Some(OTHER), &Broker::disabled(), &no_inputs()),
        "onto itself",
    );
}

/// Condition 2, the existing refusal, holds for original names. KILLS: an
/// original-name path that skips the absence check.
#[test]
fn an_original_name_that_exists_is_refused() {
    refused(
        admit(
            &plan_no_owner(),
            Some(OTHER),
            &Broker::disabled().having("payments"),
            &no_inputs(),
        ),
        "mapped target topic `payments` already exists",
    );
}

// ---------------------------------------------------------------------------
// Condition 3: which cluster, and auto-creation
// ---------------------------------------------------------------------------

/// A second cluster: admitted, `targetIsNotSource`, the identity mapping, and
/// auto-creation NOT read (a proven other cluster does not need it). KILLS:
/// reading auto-creation on another cluster and refusing an enabled one there.
#[test]
fn a_second_cluster_is_admitted_whatever_its_auto_creation() {
    let broker = Broker::with_auto_create(&[Some("true")]);
    let (a, created, deleted) =
        admitted(admit(&plan_no_owner(), Some(OTHER), &broker, &no_inputs()));
    assert_eq!(*broker.reads_of_auto_create.lock().unwrap(), 0);
    assert_eq!(a.topic_mapping_prefix, "");
    assert_eq!(
        a.topic_mapping,
        BTreeMap::from([
            ("orders".to_string(), "orders".to_string()),
            ("payments".to_string(), "payments".to_string())
        ])
    );
    let on = a.original_name.expect("the original-name admission");
    assert_eq!(
        logweir_core::original_name::cluster_condition(&on.relation),
        "targetIsNotSource"
    );
    assert_eq!(on.relation.source(), Some(OTHER));
    assert_eq!(on.owners.owner_detection, vec!["plan".to_string()]);
    assert!(created.is_empty() && deleted.is_empty());
}

/// The verified receipt's measured source counts as known. KILLS: ignoring
/// the receipt (the run would then read auto-creation and refuse it here).
#[test]
fn the_verified_receipts_source_makes_the_target_another_cluster() {
    let inputs = phase0_admit::OriginalNameInputs {
        receipt_source_cluster_id: Some(OTHER.into()),
        ..no_inputs()
    };
    let broker = Broker::with_auto_create(&[Some("true")]);
    let (a, _, _) = admitted(admit(&plan_no_owner(), None, &broker, &inputs));
    assert_eq!(
        logweir_core::original_name::cluster_condition(&a.original_name.unwrap().relation),
        "targetIsNotSource"
    );
}

/// The verified receipt naming the target as its source makes it the same
/// cluster, and the allowlist naming another cluster changes nothing (review
/// L3). KILLS: reading the allowlist's id beside the receipt's (`any` for
/// `all` over the known ids is the core row
/// `the_target_is_not_the_source_only_when_a_known_id_differs_and_none_equals`).
#[test]
fn a_known_source_equal_to_the_target_is_the_same_cluster() {
    let inputs = phase0_admit::OriginalNameInputs {
        receipt_source_cluster_id: Some(TARGET.into()),
        ..no_inputs()
    };
    refused(
        admit_with_allowlist(
            &plan_no_owner(),
            Some(OTHER),
            &Broker::with_auto_create(&[Some("true")]),
            &inputs,
        ),
        "OriginalNameAutoCreateEnabled",
    );
}

/// Review L3: the allowlist file's `source_cluster_id` is unsigned runner
/// input and never makes the target "another cluster" — without a verified
/// receipt the source is unknown, and auto-creation must be proven disabled.
/// KILLS: counting the allowlist's id again (the auto-creation read would be
/// skipped).
#[test]
fn the_allowlist_source_id_never_makes_the_target_another_cluster() {
    refused(
        admit_with_allowlist(
            &plan_no_owner(),
            Some(OTHER),
            &Broker::with_auto_create(&[Some("true")]),
            &no_inputs(),
        ),
        "OriginalNameAutoCreateEnabled",
    );
    let (a, _, _) = admitted(admit_with_allowlist(
        &plan_no_owner(),
        Some(OTHER),
        &Broker::disabled(),
        &no_inputs(),
    ));
    let proved = a
        .original_name
        .expect("admitted as an original-name restore");
    assert!(matches!(
        proved.relation,
        logweir_core::original_name::SourceRelation::SourceUnknown
    ));
}

/// The same cluster with auto-creation disabled on every broker: admitted,
/// `autoCreateDisabled`. KILLS: refusing the same cluster outright.
#[test]
fn the_same_cluster_with_auto_creation_disabled_is_admitted() {
    let broker = Broker::disabled();
    let (a, _, _) = admitted(admit(&plan_no_owner(), Some(TARGET), &broker, &no_inputs()));
    assert_eq!(*broker.reads_of_auto_create.lock().unwrap(), 1);
    let on = a.original_name.unwrap();
    assert_eq!(
        logweir_core::original_name::cluster_condition(&on.relation),
        "autoCreateDisabled"
    );
    assert_eq!(on.relation.source(), Some(TARGET));
}

/// The same cluster with ONE broker auto-creating: refused, nothing written.
/// KILLS: deleting the read; reading only the first broker.
#[test]
fn the_same_cluster_with_auto_creation_enabled_on_one_broker_is_refused() {
    let message = refused(
        admit(
            &plan_no_owner(),
            Some(TARGET),
            &Broker::with_auto_create(&[Some("false"), Some("true")]),
            &no_inputs(),
        ),
        "OriginalNameAutoCreateEnabled",
    );
    assert!(message.contains("broker(s) 2 report"), "{message}");
}

/// A setting nobody reported refuses: refuse when unsure. KILLS: reading an
/// absent value as disabled.
#[test]
fn an_unreported_auto_creation_setting_is_refused() {
    refused(
        admit(
            &plan_no_owner(),
            Some(TARGET),
            &Broker::with_auto_create(&[Some("false"), None]),
            &no_inputs(),
        ),
        "OriginalNameAutoCreateUnknown",
    );
}

/// No source known: the target MAY be the source, so auto-creation decides.
/// KILLS: reading "unknown" as "another cluster".
#[test]
fn an_unknown_source_needs_auto_creation_disabled() {
    refused(
        admit(
            &plan_no_owner(),
            None,
            &Broker::with_auto_create(&[Some("true")]),
            &no_inputs(),
        ),
        "OriginalNameAutoCreateEnabled",
    );
    let (a, _, _) = admitted(admit(
        &plan_no_owner(),
        None,
        &Broker::disabled(),
        &no_inputs(),
    ));
    assert_eq!(a.original_name.unwrap().relation.source(), None);
}

// ---------------------------------------------------------------------------
// Condition 4: declarative owners
// ---------------------------------------------------------------------------

fn strimzi(topic: &str) -> DeclaredOwner {
    DeclaredOwner {
        topic: topic.into(),
        kind: "strimzi".into(),
        reference: format!("kafka/{topic}"),
    }
}

/// Nowhere looked: refused. KILLS: reading "not checked" as "no owner".
#[test]
fn an_owner_nobody_looked_for_refuses() {
    refused(
        admit(
            &plan(None, false),
            Some(OTHER),
            &Broker::disabled(),
            &no_inputs(),
        ),
        "OriginalNameOwnerNotChecked",
    );
}

/// A declared owner refuses unless the owner path is chosen. KILLS: ignoring
/// the declaration; ignoring `owner_path`.
#[test]
fn a_declared_owner_refuses_unless_the_owner_path_is_chosen() {
    refused(
        admit(
            &plan(Some(vec![strimzi("orders")]), false),
            Some(OTHER),
            &Broker::disabled(),
            &no_inputs(),
        ),
        "OriginalNameOwnerPresent",
    );
    let (a, _, _) = admitted(admit(
        &plan(Some(vec![strimzi("orders")]), true),
        Some(OTHER),
        &Broker::disabled(),
        &no_inputs(),
    ));
    let owners = a.original_name.unwrap().owners;
    assert!(owners.owner_path);
    assert_eq!(owners.owners.len(), 1);
    assert_eq!(owners.owners[0].topic, "orders");
}

/// **An existing `KafkaTopic` owner, simulated** (no Strimzi in the lab): the
/// resources the runner was given name `payments`. Refused; with the owner
/// path, admitted. The plan states nothing, so the resources are the only
/// place looked. KILLS: not counting the resources as a place looked, or
/// their owners as owners.
#[test]
fn a_kafka_topic_resource_owner_refuses_unless_the_owner_path_is_chosen() {
    let docs = logweir_core::topic_configuration::parse_resource_documents(
        "apiVersion: kafka.strimzi.io/v1beta2\nkind: KafkaTopic\nmetadata:\n  name: payments\n  \
         namespace: kafka\n  labels:\n    strimzi.io/cluster: prod\nspec:\n  partitions: 3\n",
    )
    .unwrap();
    let found = logweir_core::topic_configuration::strimzi_owners(
        &docs,
        &["orders".to_string(), "payments".to_string()],
        None,
    );
    assert_eq!(found.len(), 1, "{found:?}");
    let inputs = phase0_admit::OriginalNameInputs {
        kafka_topic_owners: Some(found),
        ..no_inputs()
    };
    let message = refused(
        admit(
            &plan(None, false),
            Some(OTHER),
            &Broker::disabled(),
            &inputs,
        ),
        "OriginalNameOwnerPresent",
    );
    assert!(
        message.contains("`payments` (strimzi kafka/payments, from kafkaTopicResources)"),
        "{message}"
    );
    let (a, _, _) = admitted(admit(
        &plan(None, true),
        Some(OTHER),
        &Broker::disabled(),
        &inputs,
    ));
    assert_eq!(
        a.original_name.unwrap().owners.owner_detection,
        vec!["kafkaTopicResources".to_string()]
    );
}

/// The bound point's receipt recorded a Strimzi owner for the SOURCE topic:
/// it blocks a restore into the source cluster, and not one into another
/// cluster. KILLS: ignoring the receipt; applying the source's owners to a
/// proven other cluster.
#[test]
fn a_receipt_recorded_owner_blocks_only_on_the_cluster_it_describes() {
    let receipt = ReceiptOwners {
        owner_detection: vec!["kafkaTopicResources".into()],
        owners: BTreeMap::from([(
            "orders".to_string(),
            TopicOwner {
                kind: "strimzi".into(),
                basis: "kafkaTopicResource".into(),
                reference: "kafka/orders".into(),
            },
        )]),
    };
    let same = phase0_admit::OriginalNameInputs {
        receipt_source_cluster_id: Some(TARGET.into()),
        receipt_owners: Some(receipt.clone()),
        ..no_inputs()
    };
    refused(
        admit(&plan(None, false), None, &Broker::disabled(), &same),
        "OriginalNameOwnerPresent",
    );
    let other = phase0_admit::OriginalNameInputs {
        receipt_source_cluster_id: Some(OTHER.into()),
        receipt_owners: Some(receipt),
        ..no_inputs()
    };
    // Another cluster: the source's owners do not describe it, so the plan
    // must say where it looked.
    refused(
        admit(&plan(None, false), None, &Broker::disabled(), &other),
        "OriginalNameOwnerNotChecked",
    );
    admitted(admit(&plan_no_owner(), None, &Broker::disabled(), &other));

    // The fix round's sweep of review M2: the receipt ADDS owners and never
    // stands in for looking. A receipt that looked and recorded NO owner (a
    // backup records a KafkaTopic whose reference it cannot record as none)
    // is not "none found" on the cluster it describes: the plan must state
    // it, or the runner be given the resources. KILLS: reading the receipt's
    // look as the restore's own.
    let looked_and_found_none = phase0_admit::OriginalNameInputs {
        receipt_source_cluster_id: Some(TARGET.into()),
        receipt_owners: Some(ReceiptOwners {
            owner_detection: vec!["kafkaTopicResources".into()],
            owners: BTreeMap::new(),
        }),
        ..no_inputs()
    };
    let message = refused(
        admit(
            &plan(None, false),
            None,
            &Broker::disabled(),
            &looked_and_found_none,
        ),
        "OriginalNameOwnerNotChecked",
    );
    assert!(message.contains("receipt alone is not a look"), "{message}");
    // With the approver's statement beside it, the same receipt is consulted
    // and the restore is admitted, naming both places.
    let (a, _, _) = admitted(admit(
        &plan_no_owner(),
        None,
        &Broker::disabled(),
        &looked_and_found_none,
    ));
    assert_eq!(
        a.original_name
            .expect("original-name")
            .owners
            .owner_detection,
        vec!["plan".to_string(), "pointReceipt".to_string()]
    );
}

/// A declaration naming a topic the restore does not restore. KILLS: not
/// validating the plan's statement.
#[test]
fn a_declared_owner_of_another_topic_is_refused() {
    refused(
        admit(
            &plan(Some(vec![strimzi("ledger")]), true),
            Some(OTHER),
            &Broker::disabled(),
            &no_inputs(),
        ),
        "OriginalNameOwnersInvalid",
    );
}

// ---------------------------------------------------------------------------
// Condition 7: the probe never borrows an original name
// ---------------------------------------------------------------------------

/// On a `LogAppendTime` broker the probe is created under the scratch prefix
/// and deleted again; no original name is ever created or deleted. KILLS:
/// probing under the first mapped (original) name.
#[test]
fn the_log_append_time_probe_never_uses_an_original_name() {
    let broker = Broker::disabled().on_log_append_time();
    let (_, created, deleted) =
        admitted(admit(&plan_no_owner(), Some(OTHER), &broker, &no_inputs()));
    assert_eq!(created.len(), 1, "{created:?}");
    let probe = &created[0].name;
    assert!(
        probe.starts_with("drill-logweir-probe-"),
        "the probe is `{probe}`"
    );
    assert_eq!(deleted, vec![probe.clone()]);
    for original in ["orders", "payments"] {
        assert!(created.iter().all(|c| c.name != original));
        assert!(deleted.iter().all(|d| d != original));
    }
}

/// A probe name that is taken is refused, never reused. KILLS: probing into a
/// topic someone else owns.
#[test]
fn a_taken_probe_name_is_refused() {
    let spec = plan_no_owner();
    let probe = logweir_core::original_name::probe_topic_name(
        SCRATCH_PREFIX,
        &logweir_core::ids::sha256_prefixed(serde_yaml::to_string(&spec).unwrap().as_bytes()),
    );
    refused(
        admit(
            &spec,
            Some(OTHER),
            &Broker::disabled().on_log_append_time().having(&probe),
            &no_inputs(),
        ),
        "OriginalNameProbeUnusable",
    );
}

// ---------------------------------------------------------------------------
// Condition 6: creation is exclusive, and a race loses by name
// ---------------------------------------------------------------------------

fn mapping() -> BTreeMap<String, String> {
    BTreeMap::from([
        ("orders".to_string(), "orders".to_string()),
        ("payments".to_string(), "payments".to_string()),
    ])
}

fn create(
    broker: &Broker,
    creator: &Creator,
    deleter: &Deleter,
) -> (Result<(), DrillError>, phase0_admit::TopicPreflight) {
    let facts = logweir_core::engine::BackupSetFacts {
        backup_id: "b".into(),
        created_at: ts("2026-09-07T12:00:00Z"),
        source_cluster_id: None,
        manifest_sha256: "sha256:0".into(),
        manifest_version_id: None,
        consumer_group_snapshot_sha256: None,
        topics: ["orders", "payments"]
            .into_iter()
            .map(|t| logweir_core::engine::TopicFacts {
                name: t.into(),
                original_partition_count: Some(3),
                source_replication_factor: None,
                configurations: BTreeMap::new(),
                partitions: Vec::new(),
            })
            .collect(),
    };
    let mut preflight = phase0_admit::TopicPreflight {
        timestamp_type: "CreateTime".into(),
        retention_ms: "-1".into(),
        timestamp_bound_ms: None,
        configs_set: Vec::new(),
        topics_created: Vec::new(),
    };
    let r = phase0_admit::create_target_topics(
        creator,
        broker,
        deleter,
        &mapping(),
        &facts,
        1,
        &mut preflight,
    );
    (r, preflight)
}

fn race(e: DrillError) -> phase0_admit::TargetTopicRace {
    assert_eq!(e.exit_code(), ExitCode::Operational, "{e}");
    match e {
        DrillError::TargetTopicAppeared(race) => *race,
        other => panic!("expected the named race, got: {other}"),
    }
}

/// A name that appeared after phase 0 refuses BEFORE any create, by name, and
/// nothing is created or deleted. KILLS: deleting the pre-create look (the
/// creator would then be called); routing the race through a plain exit 1
/// with no names.
#[test]
fn a_name_that_appeared_since_phase_0_loses_before_anything_is_created() {
    let broker = Broker::disabled().having("payments");
    let creator = Creator::default();
    let deleter = Deleter::default();
    let (r, preflight) = create(&broker, &creator, &deleter);
    let race = race(r.expect_err("the race is lost"));
    assert!(
        race.message.starts_with("TargetTopicAppeared: "),
        "{}",
        race.message
    );
    assert!(race.message.contains("`payments`"), "{}", race.message);
    assert!(
        race.message.contains("This run created no topic."),
        "{}",
        race.message
    );
    assert_eq!(race.appeared, vec!["payments".to_string()]);
    assert!(race.removed.is_empty() && race.left.is_empty());
    assert!(creator.calls.lock().unwrap().is_empty());
    assert!(deleter.unwritten_calls.lock().unwrap().is_empty());
    assert!(preflight.topics_created.is_empty());
    assert_eq!(
        race.status_line_value(),
        r#"{"appeared":["payments"],"removed":[],"left":[]}"#
    );
}

/// Review M4. `CreateTopics` answering "already exists" for one name is the
/// same named loss; the topic THIS run created in the same request is removed
/// — its own answer, its partition count and its pinned configuration prove
/// it this run's, and it holds no record — and the name that appeared is
/// NEVER handed to any deleter. KILLS: leaving an empty production-named topic
/// behind; deleting the topic someone else created; skipping the proof.
#[test]
fn a_lost_race_removes_only_what_this_run_created_and_proved_empty() {
    let broker = Broker::disabled();
    let creator = Creator {
        taken: vec!["payments".into()],
        lists_into: Some(Arc::clone(&broker.created)),
        ..Creator::default()
    };
    let deleter = Deleter::default();
    let (r, preflight) = create(&broker, &creator, &deleter);
    let race = race(r.expect_err("the race is lost"));
    assert_eq!(race.appeared, vec!["payments".to_string()]);
    assert_eq!(race.removed, vec!["orders".to_string()]);
    assert!(race.left.is_empty(), "{:?}", race.left);
    assert_eq!(
        *deleter.unwritten_calls.lock().unwrap(),
        vec!["orders".to_string()]
    );
    assert!(
        deleter.calls.lock().unwrap().is_empty(),
        "the scratch deleter is never used"
    );
    assert!(
        race.message.contains("removed them again"),
        "{}",
        race.message
    );
    assert!(
        preflight.topics_created.is_empty(),
        "a removed topic is not reported created"
    );
    assert_eq!(
        race.status_line_value(),
        r#"{"appeared":["payments"],"removed":["orders"],"left":[]}"#
    );
}

/// Review M4's controls: a created topic is LEFT, and named with the reason,
/// whenever ownership or emptiness cannot be proved — it holds a record, its
/// configuration is not the one this run set, or its partition count is not.
/// KILLS: deleting a topic a producer already wrote into; deleting one that
/// was recreated by someone else; dropping either proof.
#[test]
fn a_created_topic_that_cannot_be_proved_its_own_and_empty_is_left_and_named() {
    // A record arrived before the cleanup.
    let broker = Broker::disabled();
    let creator = Creator {
        taken: vec!["payments".into()],
        lists_into: Some(Arc::clone(&broker.created)),
        ..Creator::default()
    };
    let deleter = Deleter {
        records: BTreeMap::from([("orders".to_string(), 1)]),
        ..Deleter::default()
    };
    let race_1 = race(create(&broker, &creator, &deleter).0.expect_err("lost"));
    assert!(race_1.removed.is_empty());
    assert_eq!(race_1.left.len(), 1);
    assert!(
        race_1.left[0].1.contains("holds 1 record"),
        "{:?}",
        race_1.left
    );
    assert!(
        race_1.message.contains("LEFT `orders`"),
        "{}",
        race_1.message
    );

    // Another configuration: never handed to the deleter.
    let mut broker = Broker::disabled();
    broker.foreign_configs = true;
    let creator = Creator {
        taken: vec!["payments".into()],
        lists_into: Some(Arc::clone(&broker.created)),
        ..Creator::default()
    };
    let deleter = Deleter::default();
    let race_2 = race(create(&broker, &creator, &deleter).0.expect_err("lost"));
    assert!(race_2.removed.is_empty());
    assert!(
        race_2.left[0].1.contains("configuration is not the one"),
        "{:?}",
        race_2.left
    );
    assert!(deleter.unwritten_calls.lock().unwrap().is_empty());

    // Another partition count: never handed to the deleter.
    let broker = Broker::disabled();
    let creator = Creator {
        taken: vec!["payments".into()],
        lists_into: Some(Arc::clone(&broker.created)),
        partitions_override: Some(1),
        ..Creator::default()
    };
    let deleter = Deleter::default();
    let race_3 = race(create(&broker, &creator, &deleter).0.expect_err("lost"));
    assert!(
        race_3.left[0].1.contains("1 partition(s), not the 3"),
        "{:?}",
        race_3.left
    );
    assert!(deleter.unwritten_calls.lock().unwrap().is_empty());
    assert_eq!(
        race_3.status_line_value(),
        r#"{"appeared":["payments"],"removed":[],"left":["orders"]}"#
    );
}

/// Review L1. A `CreateTopics` answer that names fewer topics than asked is
/// refused by name before the engine starts — a name without an answer was
/// neither created nor refused. KILLS: accepting a short answer (the engine
/// would be handed `payments`).
#[test]
fn a_create_answer_without_one_result_per_name_is_refused() {
    let broker = Broker::disabled();
    let creator = Creator {
        answers_only: Some(vec!["orders".into()]),
        ..Creator::default()
    };
    let deleter = Deleter::default();
    let (r, _) = create(&broker, &creator, &deleter);
    let e = r.expect_err("a short answer");
    assert_eq!(e.exit_code(), ExitCode::Operational);
    let message = e.to_string();
    assert!(
        message.contains(
            "CreateTopics answered for [orders] when this run asked for [orders, payments]"
        ),
        "{message}"
    );
    assert!(message.contains("left in place: [`orders`]"), "{message}");
}

// ---------------------------------------------------------------------------
// Teardown never deletes an original name
// ---------------------------------------------------------------------------

/// Phase 9's rail: a scratch teardown handed an identity entry never passes
/// it to the deleter. KILLS: deleting the partition step.
#[test]
fn teardown_never_hands_an_original_name_to_the_deleter() {
    let deleter = Deleter::default();
    let mapping = BTreeMap::from([
        ("orders".to_string(), "orders".to_string()),
        ("payments".to_string(), "drill-payments".to_string()),
    ]);
    let attestation = phase9_teardown::run(
        &deleter,
        &mapping,
        "delete",
        TargetMode::Scratch,
        "run",
        "sha256:0",
    );
    assert_eq!(
        *deleter.calls.lock().unwrap(),
        vec!["drill-payments".to_string()]
    );
    assert!(attestation
        .topics_failed
        .iter()
        .any(|(t, why)| t == "orders" && why.contains("original")));
    assert!(!attestation.topics_deleted.contains(&"orders".to_string()));
}

/// A `newTopic` (and so every original-name) restore tears nothing down.
#[test]
fn an_original_name_restore_tears_nothing_down() {
    let deleter = Deleter::default();
    phase9_teardown::run(
        &deleter,
        &mapping(),
        "delete",
        TargetMode::NewTopic,
        "run",
        "sha256:0",
    );
    assert!(deleter.calls.lock().unwrap().is_empty());
}
