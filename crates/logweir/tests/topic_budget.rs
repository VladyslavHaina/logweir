//! FX-33: the topic budget, measured, and the refusals that keep a backup
//! inside it.
//!
//! `logweir_core::topic_budget` states how many topics one backup may name
//! and what its receipt and catalog record may weigh. Every reader's cap is
//! derived from those numbers, so they have to be TRUE of the documents this
//! build writes, and stay true when a document gains a block. These rows:
//!
//! - measure one topic's cost in a receipt and in a record through the real
//!   types, the real encoder and the real catalog writer, and fail when it is
//!   over its budget;
//! - fail when the receipt gains a field the before-the-engine projection
//!   does not give a longest value to;
//! - hold the projection above real runs' documents;
//! - run the two refusals through the backup seam: the count, with no broker
//!   contacted, and the bytes, before the engine;
//! - hold the documentation to the numbers.
//!
//! Every row runs in process against doubles (Global Constraint 22): the
//! bootstrap list is `kafka-source:9092`, handed to a `ClusterReader` double
//! and never to a client.

use logweir::backup::config_coverage;
use logweir::backup::document_budget::{self, Known};
use logweir::backup::{execute_with, BackupError, BackupOutcome, BackupRunArgs};
use logweir_core::backup_receipt::BackupReceipt;
use logweir_core::engine::*;
use logweir_core::topic_budget::{
    self, reference_receipt, reference_topic_name, ReferenceShape, MAX_BACKUP_TOPICS,
    MAX_RECEIPT_BYTES, MAX_RECORD_BYTES, RECEIPT_BASE_BUDGET_BYTES, RECEIPT_TOPIC_BUDGET_BYTES,
    RECORD_BASE_BUDGET_BYTES, RECORD_TOPIC_BUDGET_BYTES,
};
use logweir_engine_oso::storage::{caps, Store};
use logweir_evidence::keys::SigningKey;
use logweir_kafka::reader::{
    ClusterReader, ConfigEntryObservation, ConfigSourceKind, ConsumedRecord, KafkaError,
    TopicConfigRead, TopicMeta,
};
use std::collections::BTreeMap;
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Mutex;

// ---------------------------------------------------------------------------
// Measuring
// ---------------------------------------------------------------------------

fn det(receipt: &BackupReceipt) -> Vec<u8> {
    logweir_core::det_json::to_deterministic_json(receipt).expect("a receipt serialises")
}

/// The catalog record `backup run` writes for `receipt`, through the real
/// writer, as the bytes it would put.
fn record_bytes(receipt: &BackupReceipt, bytes: &[u8]) -> Vec<u8> {
    let keys = logweir::backup::phase_run::receipt_keys(&receipt.backup_id, &receipt.run_id);
    let key_id = "0123456789abcdef".repeat(4);
    let inputs = logweir::catalog::writer::RecordInputs {
        receipt_key: keys.receipt_key,
        sidecar_key: keys.sidecar_key,
        location_id: "s3://lw-archive/kafka-backups".to_string(),
        recorded_at: "2026-10-09T03:05:00Z".parse().expect("an instant"),
        signing: logweir::catalog::RecordSigning {
            key_id: key_id.clone(),
            algorithm: "ecdsa-p256-sha256".to_string(),
        },
        installation: Some(logweir::catalog::RecordInstallation { key_id }),
        execution: None,
    };
    logweir::catalog::writer::from_receipt(receipt, bytes, &inputs)
        .expect("the record derives from a valid receipt")
        .canonical_bytes()
        .expect("the record serialises")
}

/// What one shape costs: bytes a topic and the fixed part, for the receipt
/// and for the record. Measured as a difference between two sizes, so the
/// fixed part is not spread over the topics.
#[derive(Debug, Clone, Copy)]
struct Cost {
    receipt_topic: u64,
    receipt_base: u64,
    record_topic: u64,
    record_base: u64,
}

fn cost_of(shape: &ReferenceShape) -> Cost {
    let size = |topics: usize| -> (u64, u64) {
        let receipt = reference_receipt(topics, shape);
        assert_eq!(receipt.validate_invariants(), Ok(()), "{topics} {shape:?}");
        let bytes = det(&receipt);
        let record = record_bytes(&receipt, &bytes);
        (bytes.len() as u64, record.len() as u64)
    };
    let (small_receipt, small_record) = size(20);
    let (large_receipt, large_record) = size(120);
    // Rounded UP: a budget is a ceiling.
    let receipt_topic = (large_receipt - small_receipt).div_ceil(100);
    let record_topic = (large_record - small_record).div_ceil(100);
    Cost {
        receipt_topic,
        receipt_base: small_receipt.saturating_sub(20 * receipt_topic),
        record_topic,
        record_base: small_record.saturating_sub(20 * record_topic),
    }
}

/// What one topic of `shape` costs AT MOST: the projection `backup run` makes
/// before its engine starts (`document_budget::project`), over a source that
/// answers as the reference topic does, with every field the engine has yet
/// to decide at its longest. Measured as a difference, like [`cost_of`].
fn at_most(shape: &ReferenceShape) -> Cost {
    let size = |topics: usize| -> (u64, u64) {
        let names = names(topics, shape.name_bytes);
        let groups: Vec<String> = (0..shape.consumer_groups)
            .map(|g| format!("{g:03}{}", "g".repeat(252)))
            .collect();
        let seam = seam(&names, "", groups);
        let reader = ShapedReader::uniform(
            &names,
            Answer::Read {
                overrides: shape.overrides,
                value_bytes: 0,
            },
        );
        let store = Store::in_memory("logweir/");
        let engine = ManifestEngine {
            archive: &store,
            topics: names.clone(),
            runs: Mutex::new(0),
        };
        let projected = projection(&seam, &reader, &engine);
        (projected.receipt_bytes, projected.record_bytes)
    };
    let (small_receipt, small_record) = size(20);
    let (large_receipt, large_record) = size(120);
    let receipt_topic = (large_receipt - small_receipt).div_ceil(100);
    let record_topic = (large_record - small_record).div_ceil(100);
    Cost {
        receipt_topic,
        receipt_base: small_receipt.saturating_sub(20 * receipt_topic),
        record_topic,
        record_base: small_record.saturating_sub(20 * record_topic),
    }
}

/// The shapes the budget is stated for, with the name each has in the
/// documentation, and whether the budget promises the full
/// [`MAX_BACKUP_TOPICS`] of them.
fn budgeted_shapes() -> Vec<(&'static str, ReferenceShape, bool)> {
    vec![
        (
            "broker defaults, 50-byte name",
            ReferenceShape::DEFAULTS,
            true,
        ),
        (
            "five overrides, 50-byte name",
            ReferenceShape {
                overrides: 5,
                ..ReferenceShape::DEFAULTS
            },
            true,
        ),
        (
            "broker defaults, 249-byte name",
            ReferenceShape {
                name_bytes: 249,
                ..ReferenceShape::DEFAULTS
            },
            false,
        ),
        (
            "all 22 non-semantic keys overridden, 50-byte name",
            ReferenceShape {
                overrides: 22,
                ..ReferenceShape::DEFAULTS
            },
            false,
        ),
    ]
}

// ---------------------------------------------------------------------------
// The budget
// ---------------------------------------------------------------------------

/// **One topic costs no more than its budget, in the receipt and in the
/// record; and everything that is not per topic fits the base.**
///
/// Two numbers a shape: what a topic TYPICALLY costs (the reference receipt,
/// through the real types, encoder and catalog writer) and what it costs AT
/// MOST (the projection `backup run` refuses on: the same topic with every
/// field the engine decides at its longest). The budget is held against the
/// second, because that is the number a run is admitted or refused by: a
/// shape the budget promises [`MAX_BACKUP_TOPICS`] of must cost at most the
/// budget, or a selection inside the count would be refused for its bytes.
///
/// This is the row that fails when a format grows past the budget. A block
/// added to the receipt's per-topic data raises both numbers (the reference
/// and the projection are built from the receipt's own types, and
/// `the_projection_carries_every_field_the_receipt_defines` fails until a new
/// field is given to both), and the assertion names the shape and the
/// numbers. The supported topic count cannot shrink silently.
///
/// The table it prints is the one `docs/kubernetes.md` quotes.
///
/// KILLS: the per-topic budget lowered under a promised shape's cost; a
/// per-topic block added to the receipt or the record without the budget
/// being re-derived; the base budget lowered under the largest consumer
/// summary.
#[test]
fn one_topic_costs_no_more_than_its_budget() {
    // Everything that is not per topic, at its largest: PROD-04.1's 100
    // groups of 255-byte ids, the longest keys and version id.
    let full = at_most(&ReferenceShape::FULL);
    assert!(
        full.receipt_base <= RECEIPT_BASE_BUDGET_BYTES,
        "the receipt's fixed part can be {} bytes, over the {RECEIPT_BASE_BUDGET_BYTES}-byte base",
        full.receipt_base
    );
    assert!(
        full.record_base <= RECORD_BASE_BUDGET_BYTES,
        "the record's fixed part can be {} bytes, over the {RECORD_BASE_BUDGET_BYTES}-byte base",
        full.record_base
    );
    println!(
        "[topic-budget] fixed part at most: receipt {} B, record {} B (100 consumer groups)",
        full.receipt_base, full.record_base
    );
    println!(
        "[topic-budget] shape | receipt B/topic typical | at most | record B/topic typical | at \
         most | topics that fit"
    );
    for (name, shape, promised) in budgeted_shapes() {
        let typical = cost_of(&shape);
        let most = at_most(&shape);
        let fit = ((MAX_RECEIPT_BYTES - full.receipt_base) / most.receipt_topic)
            .min((MAX_RECORD_BYTES - full.record_base) / most.record_topic)
            .min(MAX_BACKUP_TOPICS as u64);
        println!(
            "[topic-budget] {name} | {} | {} | {} | {} | {fit}",
            typical.receipt_topic, most.receipt_topic, typical.record_topic, most.record_topic
        );
        assert!(
            typical.receipt_topic <= most.receipt_topic
                && typical.record_topic <= most.record_topic,
            "{name}: a typical topic costs more than the projection's most ({typical:?} vs \
             {most:?}); the projection is not an upper bound"
        );
        if !promised {
            continue;
        }
        assert!(
            most.receipt_topic <= RECEIPT_TOPIC_BUDGET_BYTES,
            "{name}: one topic can cost {} bytes in a receipt, over the \
             {RECEIPT_TOPIC_BUDGET_BYTES}-byte budget. A backup of {MAX_BACKUP_TOPICS} such \
             topics would be refused for its bytes although it is inside the topic count. \
             Every reader's cap is derived from this budget: re-derive it (and the supported \
             topic count) in logweir_core::topic_budget; do not raise one cap",
            most.receipt_topic
        );
        assert!(
            most.record_topic <= RECORD_TOPIC_BUDGET_BYTES,
            "{name}: one topic can cost {} bytes in a catalog record, over the \
             {RECORD_TOPIC_BUDGET_BYTES}-byte budget",
            most.record_topic
        );
        assert_eq!(
            fit, MAX_BACKUP_TOPICS as u64,
            "{name}: the budget promises {MAX_BACKUP_TOPICS} of these and {fit} fit"
        );
        // The record's budget is the looser one, so the receipt's bound is
        // the one a selection of this shape meets first.
        assert!(
            most.record_topic * RECEIPT_TOPIC_BUDGET_BYTES
                <= most.receipt_topic * RECORD_TOPIC_BUDGET_BYTES,
            "{name}: the record grows faster against its budget than the receipt against its \
             own ({} vs {})",
            most.record_topic,
            most.receipt_topic
        );
    }
    // NEGATIVE CONTROLS: the budget is not so loose that it holds anything.
    // The two shapes it does not promise cost more than it at their most,
    // and fewer than the maximum of them fit — but still most of it.
    for (name, shape, promised) in budgeted_shapes() {
        if promised {
            continue;
        }
        let most = at_most(&shape);
        assert!(
            most.receipt_topic > RECEIPT_TOPIC_BUDGET_BYTES,
            "{name} can cost {} bytes a topic: the budget row can fail",
            most.receipt_topic
        );
        let fit = (MAX_RECEIPT_BYTES - full.receipt_base) / most.receipt_topic;
        assert!(
            (500..MAX_BACKUP_TOPICS as u64).contains(&fit),
            "{name}: {fit} such topics fit; the acceptance's 500 must, and fewer than the \
             maximum do"
        );
    }
}

/// **The acceptance's sizes fit, and the first one over the maximum does
/// not.** A receipt and record of 70, 105, 113, 300, 500 and 1,000 reference
/// topics with full recorded configuration and PROD-04.1's largest consumer
/// summary are each inside the bounds every reader caps at; 5,000 — what an
/// older build admitted — is over both, and inside the 64 MiB the runner and
/// the CLI read, which is why it stays restorable from the command line.
#[test]
fn the_acceptance_sizes_fit_their_bounds_and_five_thousand_does_not() {
    println!("[topic-budget] topics | receipt bytes | record bytes");
    for topics in [70usize, 105, 113, 300, 500, 1_000] {
        let receipt = reference_receipt(topics, &ReferenceShape::FULL);
        let bytes = det(&receipt);
        let record = record_bytes(&receipt, &bytes);
        println!(
            "[topic-budget] {topics} | {} | {}",
            bytes.len(),
            record.len()
        );
        assert!(
            bytes.len() as u64 <= MAX_RECEIPT_BYTES && record.len() as u64 <= MAX_RECORD_BYTES,
            "{topics} topics: receipt {} of {MAX_RECEIPT_BYTES}, record {} of {MAX_RECORD_BYTES}",
            bytes.len(),
            record.len()
        );
        assert!(topics <= MAX_BACKUP_TOPICS);
    }
    let receipt = reference_receipt(5_000, &ReferenceShape::FULL);
    let bytes = det(&receipt);
    let record = record_bytes(&receipt, &bytes);
    println!("[topic-budget] 5000 | {} | {}", bytes.len(), record.len());
    assert!(bytes.len() as u64 > MAX_RECEIPT_BYTES, "{}", bytes.len());
    assert!(record.len() as u64 > MAX_RECORD_BYTES, "{}", record.len());
    assert!(
        (bytes.len() as u64) < caps::SIGNED_DOCUMENT,
        "the runner's point binding still reads a 5,000-topic receipt"
    );
    // The three caps are the budget's sums, and a receipt has one of them.
    assert_eq!(caps::CATALOG_RECEIPT, MAX_RECEIPT_BYTES);
    assert_eq!(caps::CONTROLLER_RECEIPT, MAX_RECEIPT_BYTES);
    assert_eq!(caps::CATALOG_RECORD, MAX_RECORD_BYTES);
    assert_eq!(
        logweir_core::check_contract::MAX_EVIDENCE_PAYLOAD_BYTES,
        MAX_RECEIPT_BYTES
    );
}

// ---------------------------------------------------------------------------
// The seam: doubles
// ---------------------------------------------------------------------------

const ARCHIVE_PREFIX: &str = "logweir/archive-fixture/";
const BACKUP_ID: &str = "many-topics";

/// How one topic's configuration read answers.
#[derive(Clone)]
enum Answer {
    /// The 13 semantic entries at broker defaults, plus `overrides` explicit
    /// overrides whose values are `value_bytes` long.
    Read {
        overrides: usize,
        value_bytes: usize,
    },
    Denied,
    Failed,
}

/// A source whose every topic answers as `answers` says, and which counts
/// how often it was asked anything.
struct ShapedReader {
    answers: BTreeMap<String, Answer>,
    calls: AtomicUsize,
}

impl ShapedReader {
    fn uniform(topics: &[String], answer: Answer) -> Self {
        Self {
            answers: topics.iter().map(|t| (t.clone(), answer.clone())).collect(),
            calls: AtomicUsize::new(0),
        }
    }

    fn calls(&self) -> usize {
        self.calls.load(Ordering::SeqCst)
    }

    fn entries(overrides: usize, value_bytes: usize) -> Vec<ConfigEntryObservation> {
        use logweir_core::topic_configuration::{defined_on, KAFKA_LINES, TABLE};
        let mut out: Vec<ConfigEntryObservation> = TABLE
            .iter()
            .filter(|rule| rule.semantic && defined_on(rule.key, KAFKA_LINES[1]))
            .map(|rule| ConfigEntryObservation {
                name: rule.key.to_string(),
                value: Some(rule.sample.to_string()),
                source: ConfigSourceKind::DefaultConfig,
                read_only: false,
                sensitive: false,
            })
            .collect();
        out.extend(
            TABLE
                .iter()
                .filter(|rule| !rule.semantic)
                .take(overrides)
                .map(|rule| ConfigEntryObservation {
                    name: rule.key.to_string(),
                    value: Some(if value_bytes == 0 {
                        rule.sample.to_string()
                    } else {
                        "9".repeat(value_bytes)
                    }),
                    source: ConfigSourceKind::DynamicTopicConfig,
                    read_only: false,
                    sensitive: false,
                }),
        );
        out
    }
}

impl ClusterReader for ShapedReader {
    fn cluster_id(&self) -> Result<String, KafkaError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok("SOURCE-CLUSTER-00000001".into())
    }
    fn list_topics(&self) -> Result<Vec<TopicMeta>, KafkaError> {
        Ok(vec![])
    }
    fn end_offsets(&self, _topic: &str) -> Result<Vec<(i32, i64)>, KafkaError> {
        Ok(vec![])
    }
    fn topic_configs(&self, _topic: &str) -> Result<BTreeMap<String, String>, KafkaError> {
        Ok(BTreeMap::new())
    }
    fn broker_configs(&self) -> Result<BTreeMap<String, String>, KafkaError> {
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
    fn describe_topic_configs(
        &self,
        topics: &[String],
    ) -> Result<Vec<(String, TopicConfigRead)>, KafkaError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(topics
            .iter()
            .map(|t| {
                let answer = match self.answers.get(t) {
                    Some(Answer::Read {
                        overrides,
                        value_bytes,
                    }) => Ok(Self::entries(*overrides, *value_bytes)),
                    Some(Answer::Denied) => Err(KafkaError::NotAuthorized(format!("{t}: denied"))),
                    Some(Answer::Failed) | None => {
                        Err(KafkaError::Unreachable(format!("{t}: no broker answered")))
                    }
                };
                (t.clone(), answer)
            })
            .collect())
    }
    fn replication_factors(&self, topics: &[String]) -> Result<BTreeMap<String, u32>, KafkaError> {
        Ok(topics.iter().map(|t| (t.clone(), 3)).collect())
    }
    fn topic_ids(
        &self,
        topics: &[String],
    ) -> Result<Vec<(String, logweir_kafka::topic_ids::TopicIdRead)>, KafkaError> {
        Ok(topics
            .iter()
            .enumerate()
            .map(|(i, t)| {
                let id = logweir_core::topic_identity::topic_id_text(
                    0x0123_4567_89ab_cdef,
                    i64::try_from(i).unwrap() + 2,
                )
                .expect("a canonical id");
                (t.clone(), logweir_kafka::topic_ids::TopicIdRead::Id(id))
            })
            .collect())
    }
}

fn ts(s: &str) -> chrono::DateTime<chrono::Utc> {
    s.parse().expect("an instant")
}

/// An engine that writes its manifest into the archive when it runs, answers
/// `describe` with one segment per named topic, and counts its runs.
struct ManifestEngine<'a> {
    archive: &'a Store,
    topics: Vec<String>,
    runs: Mutex<usize>,
}

impl ManifestEngine<'_> {
    fn runs(&self) -> usize {
        *self.runs.lock().unwrap()
    }
}

impl DataEngine for ManifestEngine<'_> {
    fn id(&self) -> EngineId {
        EngineId {
            id: "double".into(),
            version: "0.23.3+logweir.2".into(),
            digest: format!("sha256:{}", "d".repeat(64)),
        }
    }
    fn list_backup_sets(&self, _: &StorageUrl) -> Result<Vec<BackupSetRef>, EngineError> {
        unimplemented!("the backup path lists through the Store handle")
    }
    fn describe(&self, set: &BackupSetRef) -> Result<BackupSetFacts, EngineError> {
        Ok(BackupSetFacts {
            backup_id: set.backup_id.clone(),
            created_at: ts("2026-10-09T03:04:00Z"),
            source_cluster_id: None,
            manifest_sha256: "sha256:from-the-engines-own-handle".into(),
            manifest_version_id: None,
            consumer_group_snapshot_sha256: None,
            topics: self
                .topics
                .iter()
                .map(|name| TopicFacts {
                    name: name.clone(),
                    original_partition_count: Some(12),
                    source_replication_factor: Some(3),
                    configurations: BTreeMap::new(),
                    partitions: vec![PartitionFacts {
                        partition_id: 0,
                        segments: vec![SegmentFacts {
                            key: format!(
                                "{ARCHIVE_PREFIX}{}/topics/{name}/0/seg-0.kbak",
                                set.backup_id
                            ),
                            start_offset: 0,
                            end_offset: 9,
                            start_timestamp: 1_791_514_000_000,
                            end_timestamp: 1_791_514_060_000,
                            record_count: 10,
                            sha256: String::new(),
                            uploaded_at: 0,
                        }],
                        gaps: vec![],
                        pruned: vec![],
                    }],
                })
                .collect(),
        })
    }
    fn preflight(&self, _: &RestorePlan) -> Result<PreflightReport, EngineError> {
        unimplemented!()
    }
    fn restore(
        &self,
        _: &RestorePlan,
        _: &mut dyn PhaseObserver,
    ) -> Result<RestoreFacts, EngineError> {
        unimplemented!()
    }
    fn fingerprints(&self, _: &SampleSelection) -> Result<Vec<RecordFingerprint>, EngineError> {
        unimplemented!()
    }
    fn backup(
        &self,
        plan: &BackupPlan,
        _obs: &mut dyn PhaseObserver,
    ) -> Result<BackupFacts, EngineError> {
        *self.runs.lock().unwrap() += 1;
        self.archive
            .put_create_only(
                &format!("{ARCHIVE_PREFIX}{}/manifest.json", plan.backup_id),
                format!("{{\"backup_id\":\"{}\",\"topics\":[]}}", plan.backup_id).as_bytes(),
            )
            .map_err(|e| EngineError::Operational(format!("the double's manifest write: {e}")))?;
        Ok(BackupFacts {
            started_at: ts("2026-10-09T03:00:00Z"),
            finished_at: ts("2026-10-09T03:04:00Z"),
            exit_code: 0,
            unknown_key_warnings: vec![],
        })
    }
}

struct Seam {
    _dir: tempfile::TempDir,
    args: BackupRunArgs,
    key: SigningKey,
    spec_text: String,
}

fn spec_yaml(topics: &[String], extra: &str) -> String {
    format!(
        "backup_id: {BACKUP_ID}\n\
         source:\n\
        \x20 bootstrap_servers: [kafka-source:9092]\n\
        \x20 topics: [{}]\n\
        {extra}\
         storage:\n\
        \x20 backend: s3\n\
        \x20 bucket: kafka-backups\n\
        \x20 prefix: {ARCHIVE_PREFIX}\n\
        \x20 region: us-east-1\n\
         backup:\n\
        \x20 compression: zstd\n\
        \x20 segment_max_records: 1000\n\
        \x20 segment_max_bytes: 10485760\n\
        \x20 max_concurrent_partitions: 3\n",
        topics.join(", ")
    )
}

fn seam(topics: &[String], extra: &str, consumer_groups: Vec<String>) -> Seam {
    let dir = tempfile::tempdir().expect("a scratch directory");
    let spec_text = spec_yaml(topics, extra);
    let spec = dir.path().join("backup.yaml");
    let allowed = dir.path().join("allowed-clusters.json");
    std::fs::write(&spec, &spec_text).unwrap();
    std::fs::write(
        &allowed,
        "{\"allowed_cluster_ids\": [\"SCRATCH-CLUSTER-0000001\"]}",
    )
    .unwrap();
    // An EPHEMERAL key, generated here and never committed.
    let key = SigningKey::generate_p256();
    let key_path = dir.path().join("signer.pem");
    std::fs::write(&key_path, key.to_pkcs8_pem().unwrap()).unwrap();
    Seam {
        args: BackupRunArgs {
            store_contract_version: None,
            spec,
            allowed_clusters: allowed,
            signing_key: key_path,
            triggered_by: Some("schedule".into()),
            out: None,
            receipt_out: None,
            backup_id_override: None,
            kafka_topic_resources: None,
            strimzi_cluster: None,
            consumer_groups,
        },
        _dir: dir,
        key,
        spec_text,
    }
}

fn run_seam(
    seam: &Seam,
    reader: &ShapedReader,
    engine: &ManifestEngine<'_>,
    store: &Store,
) -> Result<BackupOutcome, BackupError> {
    execute_with(
        &seam.args,
        "01JABCDEFGHJKMNPQRSTVWXYZ0",
        reader,
        engine,
        store,
        store,
    )
}

fn names(count: usize, name_bytes: usize) -> Vec<String> {
    let shape = ReferenceShape {
        name_bytes,
        ..ReferenceShape::DEFAULTS
    };
    (0..count)
        .map(|i| reference_topic_name(i, &shape))
        .collect()
}

/// What `backup run` projects for this seam, computed here from the same
/// inputs it has before the engine.
fn projection(
    seam: &Seam,
    reader: &ShapedReader,
    engine: &ManifestEngine<'_>,
) -> document_budget::Projected {
    let spec: logweir_core::spec::BackupSpec =
        serde_yaml::from_str(&seam.spec_text).expect("the spec parses");
    let observed = config_coverage::observe(reader, &spec.source.topics);
    let owners = logweir_core::topic_configuration::merge_owners(
        BTreeMap::new(),
        spec.source.topic_owners.as_deref().unwrap_or(&[]),
    );
    let owner_detection = logweir_core::topic_configuration::owner_detection(
        spec.source.topic_owners.is_some(),
        false,
    );
    let selected = logweir::backup::selected_groups(&seam.args, &spec);
    document_budget::project(&Known {
        backup_id: BACKUP_ID,
        run_id: "01JABCDEFGHJKMNPQRSTVWXYZ0",
        triggered_by: "schedule",
        source_cluster_id: "SOURCE-CLUSTER-00000001",
        bootstrap_servers: &spec.source.bootstrap_servers,
        topics: &spec.source.topics,
        engine: &engine.id(),
        storage: &spec.storage,
        source_auth: &logweir::backup::source_auth_render(&spec.source.auth),
        observed: &observed,
        owners: &owners,
        owner_detection: &owner_detection,
        selected_groups: &selected,
        signing: logweir::catalog::signing_of(&seam.key.verifying_key()),
    })
    .expect("the projection measures")
}

fn stored(store: &Store, key: &str) -> Vec<u8> {
    store
        .get_capped(key, caps::SIGNED_DOCUMENT)
        .unwrap_or_else(|e| panic!("read {key}: {e}"))
        .0
}

// ---------------------------------------------------------------------------
// The projection
// ---------------------------------------------------------------------------

/// **No real receipt or record is larger than what the run projected for it
/// before its engine started** — over runs whose topics answer every way a
/// configuration read can: defaults, every key overridden with long values,
/// denied, failed, owned, under the longest names, and with consumer groups
/// selected.
///
/// And the projection is not so loose that it measures nothing: it is within
/// a stated slack of the real document, a topic.
///
/// KILLS: an after-the-engine field projected shorter than a real run writes
/// it; a block added to the receipt and left out of the projection; the
/// projection made vacuous by a placeholder far too large.
#[test]
fn no_real_receipt_is_larger_than_its_projection() {
    const TOPICS: usize = 24;
    let owners = format!(
        "  topic_owners:\n    - topic: {}\n      kind: external\n      reference: {}\n",
        reference_topic_name(0, &ReferenceShape::DEFAULTS),
        "r".repeat(256)
    );
    let groups: Vec<String> = (0..100)
        .map(|g| format!("{g:03}{}", "g".repeat(252)))
        .collect();
    let cases: Vec<(&str, usize, Answer, &str, Vec<String>)> = vec![
        (
            "broker defaults",
            50,
            Answer::Read {
                overrides: 0,
                value_bytes: 0,
            },
            "",
            vec![],
        ),
        (
            "every key overridden, 300-byte values",
            50,
            Answer::Read {
                overrides: 22,
                value_bytes: 300,
            },
            "",
            vec![],
        ),
        ("every read denied", 50, Answer::Denied, "", vec![]),
        ("every read failed", 50, Answer::Failed, "", vec![]),
        (
            "the longest names",
            249,
            Answer::Read {
                overrides: 5,
                value_bytes: 0,
            },
            "",
            vec![],
        ),
        (
            "a declared owner with the longest reference",
            50,
            Answer::Read {
                overrides: 0,
                value_bytes: 0,
            },
            owners.as_str(),
            vec![],
        ),
        (
            "100 consumer groups selected",
            50,
            Answer::Read {
                overrides: 0,
                value_bytes: 0,
            },
            "",
            groups,
        ),
    ];
    for (what, name_bytes, answer, extra, groups) in cases {
        let topics = names(TOPICS, name_bytes);
        let seam = seam(&topics, extra, groups);
        let reader = ShapedReader::uniform(&topics, answer);
        let store = Store::in_memory("logweir/");
        let engine = ManifestEngine {
            archive: &store,
            topics: topics.clone(),
            runs: Mutex::new(0),
        };
        let projected = projection(&seam, &reader, &engine);
        let outcome = run_seam(&seam, &reader, &engine, &store)
            .unwrap_or_else(|e| panic!("{what}: the run failed: {e}"));
        let receipt = stored(&store, &outcome.receipt_key);
        let record = stored(
            &store,
            outcome
                .catalog_key
                .as_deref()
                .expect("the record was written"),
        );
        println!(
            "[projection] {what}: receipt {} of {} projected, record {} of {}",
            receipt.len(),
            projected.receipt_bytes,
            record.len(),
            projected.record_bytes
        );
        assert!(
            receipt.len() as u64 <= projected.receipt_bytes,
            "{what}: the receipt is {} bytes and the run projected at most {}",
            receipt.len(),
            projected.receipt_bytes
        );
        assert!(
            record.len() as u64 <= projected.record_bytes,
            "{what}: the record is {} bytes and the run projected at most {}",
            record.len(),
            projected.record_bytes
        );
        // NOT VACUOUS: the slack is what the after-the-engine fields can add
        // (a schema dependency entry of two full sides is about 1.9 KB in the
        // record, counts and layout about 100 bytes), the manifest key and
        // version id (2 KiB), and for a selection the summary of groups that
        // were all captured.
        let slack =
            TOPICS as u64 * 3_000 + 4_096 + if groups_selected(&seam) { 80 * 1024 } else { 0 };
        assert!(
            projected.receipt_bytes <= receipt.len() as u64 + slack
                && projected.record_bytes <= record.len() as u64 + slack,
            "{what}: the projection is looser than its stated slack ({slack}): receipt {} vs \
             {}, record {} vs {}",
            projected.receipt_bytes,
            receipt.len(),
            projected.record_bytes,
            record.len()
        );
    }
}

fn groups_selected(seam: &Seam) -> bool {
    !seam.args.consumer_groups.is_empty()
}

/// Every key any struct of the receipt's schema defines, by name.
fn schema_property_names() -> std::collections::BTreeSet<String> {
    let schema: serde_json::Value =
        serde_json::from_str(&logweir_core::schema::backup_receipt_schema()).expect("a schema");
    let mut out = std::collections::BTreeSet::new();
    let mut take = |node: &serde_json::Value| {
        if let Some(properties) = node.get("properties").and_then(|p| p.as_object()) {
            out.extend(properties.keys().cloned());
        }
    };
    take(&schema);
    for definition in schema
        .get("definitions")
        .and_then(|d| d.as_object())
        .expect("the receipt schema has definitions")
        .values()
    {
        take(definition);
    }
    out
}

/// Every object key that appears anywhere in `value`.
fn keys_of(value: &serde_json::Value, out: &mut std::collections::BTreeSet<String>) {
    match value {
        serde_json::Value::Object(map) => {
            for (key, inner) in map {
                out.insert(key.clone());
                keys_of(inner, out);
            }
        }
        serde_json::Value::Array(items) => items.iter().for_each(|i| keys_of(i, out)),
        _ => {}
    }
}

/// **The projection gives a longest value to every field the receipt
/// defines.** The receipt's own schema lists every property of every struct
/// in it; each one appears in the receipt the projection measures.
///
/// This is what makes "a block added to the receipt cannot silently shrink
/// the supported topic count" true of the BYTE bound as well: a new optional
/// field is `None` in `document_budget::longest_outcome` until someone gives
/// it its longest value there, and this row names it.
///
/// KILLS: a receipt field added without its longest value in the projection.
#[test]
fn the_projection_carries_every_field_the_receipt_defines() {
    let topics = names(3, 50);
    let owners = format!(
        "  topic_owners:\n    - topic: {}\n      kind: external\n      reference: elsewhere\n",
        topics[0]
    );
    let seam = seam(&topics, &owners, vec!["billing".to_string()]);
    let reader = ShapedReader::uniform(
        &topics,
        Answer::Read {
            overrides: 1,
            value_bytes: 0,
        },
    );
    let spec: logweir_core::spec::BackupSpec =
        serde_yaml::from_str(&seam.spec_text).expect("the spec parses");
    let observed = config_coverage::observe(&reader, &spec.source.topics);
    let owners = logweir_core::topic_configuration::merge_owners(
        BTreeMap::new(),
        spec.source.topic_owners.as_deref().unwrap_or(&[]),
    );
    let selected = logweir::backup::selected_groups(&seam.args, &spec);
    let engine_id = EngineId {
        id: "double".into(),
        version: "0.23.3+logweir.2".into(),
        digest: format!("sha256:{}", "d".repeat(64)),
    };
    let outcome = document_budget::longest_outcome(&Known {
        backup_id: BACKUP_ID,
        run_id: "01JABCDEFGHJKMNPQRSTVWXYZ0",
        triggered_by: "schedule",
        source_cluster_id: "SOURCE-CLUSTER-00000001",
        bootstrap_servers: &spec.source.bootstrap_servers,
        topics: &spec.source.topics,
        engine: &engine_id,
        storage: &spec.storage,
        source_auth: &logweir::backup::source_auth_render(&spec.source.auth),
        observed: &observed,
        owners: &owners,
        owner_detection: &["declared".to_string()],
        selected_groups: &selected,
        signing: logweir::catalog::signing_of(&seam.key.verifying_key()),
    });
    let receipt = logweir::backup::phase_run::build_receipt(&outcome);
    let value = serde_json::to_value(&receipt).expect("JSON");
    let mut present = std::collections::BTreeSet::new();
    keys_of(&value, &mut present);
    let defined = schema_property_names();
    assert!(
        defined.len() >= 45,
        "the scan reads the receipt's schema: {} properties",
        defined.len()
    );
    let missing: Vec<&String> = defined.difference(&present).collect();
    assert!(
        missing.is_empty(),
        "the receipt defines {missing:?}, and the receipt `backup run` measures before the \
         engine does not carry it. Give it its LONGEST value in \
         logweir::backup::document_budget::longest_outcome, and add it to \
         logweir_core::topic_budget::reference_receipt if it is per topic: a field the \
         projection does not measure can push a signed receipt over the bound every reader \
         caps at"
    );
    // NEGATIVE CONTROL: the check can fail — a receipt before the projection's
    // blocks lacks them.
    let mut bare = serde_json::to_value(reference_receipt(1, &ReferenceShape::DEFAULTS)).unwrap();
    bare.as_object_mut().unwrap().remove("generations");
    let mut bare_keys = std::collections::BTreeSet::new();
    keys_of(&bare, &mut bare_keys);
    assert!(
        defined.difference(&bare_keys).any(|k| k == "generations"),
        "a receipt without `generations` is reported as missing it"
    );
}

// ---------------------------------------------------------------------------
// The refusals, through the seam
// ---------------------------------------------------------------------------

fn guard_message(result: Result<BackupOutcome, BackupError>) -> String {
    match result {
        Err(BackupError::Guard(refusal)) => refusal.0,
        Err(other) => panic!("expected a guard refusal (exit 3), got {other:?}"),
        Ok(_) => panic!("expected a guard refusal (exit 3), and the backup ran"),
    }
}

/// **A selection over the maximum is refused by name before any client is
/// used, and the maximum itself runs.** 1,001 named topics: exit 3,
/// `BackupSelectionTooLarge`, naming both numbers; the source is never asked
/// anything, the engine never runs and nothing is written. 1,000 run.
///
/// KILLS: the count refusal removed from phase −1, or moved after the first
/// broker read; the bound read as `>=`.
#[test]
fn a_selection_over_the_maximum_is_refused_before_any_client_is_used() {
    let over = names(MAX_BACKUP_TOPICS + 1, 20);
    let seam_over = seam(&over, "", vec![]);
    let reader = ShapedReader::uniform(
        &over,
        Answer::Read {
            overrides: 0,
            value_bytes: 0,
        },
    );
    let store = Store::in_memory("logweir/");
    let engine = ManifestEngine {
        archive: &store,
        topics: over.clone(),
        runs: Mutex::new(0),
    };
    let result = run_seam(&seam_over, &reader, &engine, &store);
    assert_eq!(
        result.as_ref().err().map(BackupError::exit_code),
        Some(logweir::exit::ExitCode::GuardRefused)
    );
    let message = guard_message(result);
    assert!(
        message
            .starts_with("BackupSelectionTooLarge: the backup names 1001 topics and at most 1000"),
        "{message}"
    );
    assert!(message.contains("source.topics"), "{message}");
    assert_eq!(reader.calls(), 0, "a local refusal asks the source nothing");
    assert_eq!(engine.runs(), 0, "the engine never ran");
    assert!(
        store.list_keys("logweir/").unwrap_or_default().is_empty(),
        "nothing was written: no claim, no archive, no evidence"
    );

    // NEGATIVE CONTROL: the maximum itself is a backup.
    let at = names(MAX_BACKUP_TOPICS, 20);
    let seam_at = seam(&at, "", vec![]);
    let reader = ShapedReader::uniform(
        &at,
        Answer::Read {
            overrides: 0,
            value_bytes: 0,
        },
    );
    let store = Store::in_memory("logweir/");
    let engine = ManifestEngine {
        archive: &store,
        topics: at.clone(),
        runs: Mutex::new(0),
    };
    let outcome = run_seam(&seam_at, &reader, &engine, &store).expect("1,000 topics run");
    assert_eq!(engine.runs(), 1);
    let receipt = stored(&store, &outcome.receipt_key);
    assert!(
        receipt.len() as u64 <= MAX_RECEIPT_BYTES,
        "{}",
        receipt.len()
    );
    let parsed: BackupReceipt = serde_json::from_slice(&receipt).expect("a receipt");
    assert_eq!(parsed.source.topics.len(), MAX_BACKUP_TOPICS);
}

/// **A selection inside the count whose receipt would be over the bound is
/// refused before the engine runs.** 600 topics, each with every
/// non-semantic key overridden by a 300-byte value: about 9 KB a topic, 5.6
/// MB of receipt. Exit 3, `BackupSelectionTooLarge`, naming the receipt, its
/// projected size and the bound; the engine never runs and the archive holds
/// nothing. The same 600 topics at their defaults run, and sign a receipt
/// inside the bound.
///
/// KILLS: the byte refusal removed, or placed after the engine; the
/// projection comparing against a larger bound.
#[test]
fn a_receipt_that_would_be_over_the_bound_is_refused_before_the_engine() {
    let topics = names(600, 50);
    let seam = seam(&topics, "", vec![]);
    let heavy = ShapedReader::uniform(
        &topics,
        Answer::Read {
            overrides: 22,
            value_bytes: 300,
        },
    );
    let store = Store::in_memory("logweir/");
    let engine = ManifestEngine {
        archive: &store,
        topics: topics.clone(),
        runs: Mutex::new(0),
    };
    let projected = projection(&seam, &heavy, &engine);
    assert!(projected.receipt_bytes > MAX_RECEIPT_BYTES, "{projected:?}");
    let result = run_seam(&seam, &heavy, &engine, &store);
    assert_eq!(
        result.as_ref().err().map(BackupError::exit_code),
        Some(logweir::exit::ExitCode::GuardRefused)
    );
    let message = guard_message(result);
    assert!(
        message.starts_with("BackupSelectionTooLarge: the receipt of these 600 topics could be"),
        "{message}"
    );
    assert!(
        message.contains(&projected.receipt_bytes.to_string())
            && message.contains(&format!("{MAX_RECEIPT_BYTES}-byte bound")),
        "the refusal states the size against the bound: {message}"
    );
    assert!(message.contains("NO backup was taken"), "{message}");
    assert_eq!(engine.runs(), 0, "the engine never ran");
    let keys = store.list_keys("logweir/").unwrap_or_default();
    assert!(
        !keys.iter().any(|k| k.starts_with(ARCHIVE_PREFIX)),
        "the archive holds nothing: {keys:?}"
    );
    assert!(
        !keys.iter().any(|k| k.ends_with(".receipt.json")),
        "no receipt was signed: {keys:?}"
    );

    // NEGATIVE CONTROL: the same selection, at broker defaults, runs.
    let light = ShapedReader::uniform(
        &topics,
        Answer::Read {
            overrides: 0,
            value_bytes: 0,
        },
    );
    let store = Store::in_memory("logweir/");
    let engine = ManifestEngine {
        archive: &store,
        topics: topics.clone(),
        runs: Mutex::new(0),
    };
    let outcome = run_seam(&seam, &light, &engine, &store).expect("600 default topics run");
    assert_eq!(engine.runs(), 1);
    assert!(stored(&store, &outcome.receipt_key).len() as u64 <= MAX_RECEIPT_BYTES);
}

/// **The two refusals change no byte of what a backup signs.** A small backup
/// through the seam signs exactly the receipt the builders make from that
/// run's own outcome, and the source is read as often as before: the size
/// check measures the configuration read the run already made, reads nothing
/// more and writes nothing.
///
/// (That the bytes equal the PREVIOUS build's is held by the rows that pin
/// them: the signed fixtures under `e2e/fixtures/signed/`, the receipt and
/// record schema drift rows, `the_catalog_sync_body_is_pinned_for_the_
/// controllers_parser`, and `weirkeeper`'s run-policy digest rows. None of
/// them is edited by this change.)
#[test]
fn the_refusals_change_no_byte_of_what_a_small_backup_signs() {
    let topics = names(2, 11);
    let seam = seam(&topics, "", vec![]);
    let reader = ShapedReader::uniform(
        &topics,
        Answer::Read {
            overrides: 1,
            value_bytes: 0,
        },
    );
    let store = Store::in_memory("logweir/");
    let engine = ManifestEngine {
        archive: &store,
        topics: topics.clone(),
        runs: Mutex::new(0),
    };
    let outcome = run_seam(&seam, &reader, &engine, &store).expect("the backup runs");
    let signed = stored(&store, &outcome.receipt_key);
    let rebuilt = det(&logweir::backup::phase_run::signable_schema_dependency(
        logweir::backup::phase_run::build_receipt(&outcome),
    ));
    assert_eq!(
        signed, rebuilt,
        "the signed receipt is the builders' projection of the run's outcome, and nothing the \
         size check touched"
    );
    assert_eq!(
        outcome.receipt_sha256,
        logweir_core::ids::sha256_prefixed(&signed)
    );
    // The configuration read happened once: the projection re-reads nothing.
    assert_eq!(
        reader.calls(),
        2,
        "one cluster-id read and ONE configuration read; the size check adds none"
    );
}

// ---------------------------------------------------------------------------
// The signatures the controller's tests verify
// ---------------------------------------------------------------------------

/// Where the controller's tests keep the sidecars they verify.
const SIDECAR_FIXTURES: &str = "crates/weirkeeper/tests/fixtures/topic-budget";
/// Set to `1` to write the sidecars instead of comparing them.
const WRITE_FIXTURES_ENV: &str = "LOGWEIR_FX33_WRITE_FIXTURES";

/// **The sidecars the controller's tests verify are what the signer writes
/// today**, over the reference receipts' bytes, under the checked-in
/// throwaway key (`e2e/fixtures/signed/signing.pem`; ECDSA P-256 with RFC
/// 6979 nonces, so a signature is a function of the key and the bytes).
///
/// `weirkeeper` cannot link the signer, so `crates/weirkeeper/tests/
/// topic_budget.rs` verifies COMMITTED sidecars over receipts it builds from
/// `logweir_core::topic_budget::REFERENCE_SET`. This row is the other half:
/// a receipt format change, a reference change or a signer change that moves
/// one byte fails here, with the command that regenerates the files —
/// `LOGWEIR_FX33_WRITE_FIXTURES=1 cargo test -p logweir --test topic_budget
/// the_controllers_reference_sidecars`.
///
/// It also holds the receipt at the bound to within one percent UNDER
/// `MAX_RECEIPT_BYTES`, so the controller's memory rows measure a document at
/// its cap and not a small one.
#[test]
fn the_controllers_reference_sidecars_are_what_the_signer_writes() {
    let key = SigningKey::from_pem_file(&root().join("e2e/fixtures/signed/signing.pem"))
        .expect("the checked-in throwaway fixture signing key");
    let write = std::env::var(WRITE_FIXTURES_ENV).is_ok_and(|v| v == "1");
    let dir = root().join(SIDECAR_FIXTURES);
    for (label, topics, shape) in topic_budget::REFERENCE_SET {
        let receipt = reference_receipt(topics, &shape);
        assert_eq!(receipt.validate_invariants(), Ok(()), "{label}");
        let bytes = det(&receipt);
        let sign = || {
            let sidecar = logweir_evidence::sign::sign_detached(
                &key,
                logweir_evidence::PAYLOAD_TYPE_BACKUP_RECEIPT,
                &bytes,
            )
            .expect("the sidecar signs");
            let mut out = serde_json::to_vec(&sidecar).expect("a sidecar serialises");
            out.push(b'\n');
            out
        };
        let sidecar = sign();
        assert_eq!(sidecar, sign(), "{label}: the signature is deterministic");
        eprintln!(
            "[fx-33] reference {label}: {topics} topics, receipt {} bytes, sidecar {} bytes",
            bytes.len(),
            sidecar.len()
        );
        if label == "at-the-bound" {
            let len = bytes.len() as u64;
            assert!(
                len <= MAX_RECEIPT_BYTES && len * 100 >= MAX_RECEIPT_BYTES * 99,
                "the receipt at the bound is {len} bytes; the bound is {MAX_RECEIPT_BYTES}"
            );
        }
        let path = dir.join(format!("reference-{label}.sig"));
        if write {
            std::fs::create_dir_all(&dir).expect("the fixture directory");
            std::fs::write(&path, &sidecar).expect("the fixture is written");
            continue;
        }
        let committed = std::fs::read(&path).unwrap_or_else(|e| {
            panic!(
                "{} is missing ({e}); write it with {WRITE_FIXTURES_ENV}=1",
                path.display()
            )
        });
        assert_eq!(
            committed,
            sidecar,
            "{} is not what the signer writes over the reference receipt today. If the receipt \
             or the reference changed on purpose, regenerate with {WRITE_FIXTURES_ENV}=1",
            path.display()
        );
    }
}

// ---------------------------------------------------------------------------
// The documentation quotes the test
// ---------------------------------------------------------------------------

fn root() -> std::path::PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .expect("crates/logweir has a grandparent")
        .to_path_buf()
}

fn grouped(n: u64) -> String {
    let digits = n.to_string();
    let mut out = String::new();
    for (i, c) in digits.chars().enumerate() {
        if i > 0 && (digits.len() - i) % 3 == 0 {
            out.push(',');
        }
        out.push(c);
    }
    out
}

/// **The numbers in the documentation are this module's.** The operator
/// documentation states the maximum, the per-topic budget and the two bounds;
/// each is formatted here from the constant and must appear, so a constant
/// that moves without its sentence fails the build.
#[test]
fn the_documentation_states_the_budget_in_the_constants_own_numbers() {
    let wanted = [
        format!("{} topics", grouped(MAX_BACKUP_TOPICS as u64)),
        format!("{} bytes a topic", grouped(RECEIPT_TOPIC_BUDGET_BYTES)),
        format!("{} bytes", grouped(MAX_RECEIPT_BYTES)),
        format!("{} bytes", grouped(MAX_RECORD_BYTES)),
        topic_budget::SELECTION_TOO_LARGE.to_string(),
    ];
    for doc in [
        "docs/kubernetes.md",
        "docs/stability.md",
        "docs/release-notes.md",
    ] {
        let text = std::fs::read_to_string(root().join(doc))
            .unwrap_or_else(|e| panic!("{doc} is checked in: {e}"));
        for needle in &wanted {
            assert!(
                text.contains(needle.as_str()),
                "{doc} does not state `{needle}`, which logweir_core::topic_budget defines. \
                 The documentation quotes the budget's numbers; update the sentence with the \
                 constant"
            );
        }
    }
}
