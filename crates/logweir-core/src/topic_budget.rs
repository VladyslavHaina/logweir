//! **FX-33 — how many topics one backup may name, and what its signed
//! documents may weigh.** One place: every reader's cap and every refusal is
//! derived from the numbers here.
//!
//! # Why a backup has a topic limit at all
//!
//! A backup receipt signs several blocks per named topic (the record count,
//! the configuration capture coverage, the configuration model, the schema
//! dependency, the topic IDs), and the catalog point record copies them. So
//! both documents grow with the topic count, about 3 KB a topic. Every reader
//! holds a document under a cap, and before this module the caps were chosen
//! one by one: the catalog walk read 256 KiB and the shared controller 1 MiB,
//! while a dynamic selection admitted 5,000 topics. A backup of about 80
//! topics was listed by no catalog entry, and one of about 300 was never
//! verified by the controller.
//!
//! # What sets the limit
//!
//! The reader that can hold the least: **the evidence relay**. A destination
//! whose `evidenceRead` grant only a pod may hold is verified from bytes a
//! check Job prints to its pod log, and the controller reads at most 8 MiB of
//! that log (`weirkeeper::check::relay::RELAY_LIMIT_BYTES`, below the
//! kubelet's default 10 MiB log rotation). Base64 in 3,000-character frames
//! turns a payload of [`MAX_RECEIPT_BYTES`] into about 6.6 MiB of log; a
//! 6 MiB payload is 8.1 MiB and does not fit. A receipt no relay can carry is
//! a receipt half of all installs can never verify, so the SAME number caps
//! the controller's own handle and the catalog walk
//! (`logweir_store::caps::{CONTROLLER_RECEIPT, CATALOG_RECEIPT}`): a point the
//! catalog lists `Available` is one the controller can verify, whichever way
//! its evidence is read.
//!
//! # The budget
//!
//! | constant | value | what it is |
//! |---|---|---|
//! | [`MAX_BACKUP_TOPICS`] | 1,000 | the most topics one backup may name |
//! | [`RECEIPT_TOPIC_BUDGET_BYTES`] | 5,000 | what ONE topic may cost in a receipt |
//! | [`RECEIPT_BASE_BUDGET_BYTES`] | 131,072 | everything in a receipt that is not per topic, PROD-04.1's 80 KiB consumer summary included |
//! | [`MAX_RECEIPT_BYTES`] | 5,131,072 | base + maximum × per topic |
//! | [`RECORD_TOPIC_BUDGET_BYTES`] | 6,000 | what one topic may cost in the catalog point record, which re-indents the receipt's blocks |
//! | [`MAX_RECORD_BYTES`] | 6,131,072 | the same sum for the record |
//!
//! **A topic's budget is what it may cost AT MOST**: with its recorded
//! configuration as the source reports it, and every field the engine decides
//! at its longest (a schema-dependent topic listing 16 ids a side, counts of
//! `u64::MAX`). That is the number a run is admitted or refused by
//! (`logweir::backup::document_budget`), so it is the number the budget is
//! held against. Measured by `crates/logweir/tests/topic_budget.rs`, bytes a
//! topic:
//!
//! | shape of every topic | receipt, typical | receipt, at most | record, at most | topics that fit |
//! |---|---|---|---|---|
//! | broker defaults, 50-byte name | 3,090 | 4,003 | 4,145 | 1,000 |
//! | five overrides, 50-byte name | 3,833 | 4,746 | 4,938 | 1,000 |
//! | broker defaults, 249-byte name | 4,284 | 5,197 | 4,344 | 972 |
//! | all 22 non-semantic keys overridden, 50-byte name | 6,485 | 7,398 | 7,760 | 683 |
//!
//! "Broker defaults" is the 13 semantic configuration entries a 4.x broker
//! reports, the capture coverage, the schema dependency and both topic IDs.
//! The fixed part of a receipt is at most 77,683 bytes with PROD-04.1's
//! largest consumer summary. The first two shapes are the ones the budget
//! PROMISES the full count of; a selection of the other two is refused by its
//! bytes above the count shown.
//!
//! That test fails when a promised shape costs more than its budget, and when
//! the receipt type gains a field the reference and the projection do not
//! carry — so the next block added to the receipt cannot silently shrink the
//! supported topic count. The numbers the documentation quotes are the ones
//! it prints.
//!
//! # A count and a byte bound, both before the engine
//!
//! The COUNT is refused wherever a selection is first known
//! ([`refuse_topic_count`]): the CLI's phase −1, the controller's static and
//! dynamic selections, the schedule. The BYTES are refused by the runner
//! after its own configuration read and before the engine
//! ([`refuse_document_bytes`]): a topic may carry more overrides than its
//! budget assumes, and the rule is that Logweir never writes a backup it
//! cannot later list and verify.
//!
//! Pure: no I/O, no clock (Global Constraint 1).

/// **The most topics one backup may name.**
///
/// At [`RECEIPT_TOPIC_BUDGET_BYTES`] a topic this is the largest round count
/// whose receipt the evidence relay can carry (module doc). It is also the
/// count a backup readiness check already stops at
/// (`check_contract::MAX_READINESS_TOPICS`).
pub const MAX_BACKUP_TOPICS: usize = 1_000;

/// **What one topic may cost in a signed receipt AT MOST**, in bytes of the
/// receipt's own encoding (two-space deterministic JSON), averaged over a
/// backup's topics. The module doc's table is what it is measured against: a
/// topic at broker defaults under a 50-byte name can cost 4,003 bytes, and
/// with five overrides 4,746.
pub const RECEIPT_TOPIC_BUDGET_BYTES: u64 = 5_000;

/// **What a receipt may spend on everything that is not per topic**: its
/// header, source, engine and archive blocks (about 1 KB; keys and a version
/// id can add 3 KB), and PROD-04.1's consumer position summary, which
/// `consumer_positions::MAX_BLOCK_BYTES` holds to 80 KiB.
pub const RECEIPT_BASE_BUDGET_BYTES: u64 = 128 * 1024;

/// **The largest receipt Logweir writes, and the cap of every reader that
/// must agree about it**: [`RECEIPT_BASE_BUDGET_BYTES`] +
/// [`MAX_BACKUP_TOPICS`] × [`RECEIPT_TOPIC_BUDGET_BYTES`].
pub const MAX_RECEIPT_BYTES: u64 =
    RECEIPT_BASE_BUDGET_BYTES + (MAX_BACKUP_TOPICS as u64) * RECEIPT_TOPIC_BUDGET_BYTES;

/// **What one topic may cost in the catalog point record AT MOST.** The
/// record copies the receipt's per-topic blocks under `topics[]`, two levels
/// deeper, so the same topic costs 2 to 4 percent more there (measured), and
/// less under a long name, which the record writes once. A fifth above the
/// receipt's budget, so the receipt's bound is the one a selection meets
/// first.
pub const RECORD_TOPIC_BUDGET_BYTES: u64 = 6_000;

/// **What a record may spend on everything that is not per topic**: its
/// identity, receipt reference, archive, source, execution and signing
/// blocks, and its copy of the consumer position summary.
pub const RECORD_BASE_BUDGET_BYTES: u64 = 128 * 1024;

/// **The largest catalog point record Logweir writes, and the catalog walk's
/// cap for one.**
pub const MAX_RECORD_BYTES: u64 =
    RECORD_BASE_BUDGET_BYTES + (MAX_BACKUP_TOPICS as u64) * RECORD_TOPIC_BUDGET_BYTES;

// The record's budget is above the receipt's, so a selection inside the
// receipt's bound is never refused for the record alone in the reference
// shapes; and each total is its own sum.
const _: () = assert!(RECORD_TOPIC_BUDGET_BYTES > RECEIPT_TOPIC_BUDGET_BYTES);
const _: () = assert!(MAX_RECORD_BYTES > MAX_RECEIPT_BYTES);
// The consumer position summary fits the base beside the receipt's own header.
const _: () = assert!(
    RECEIPT_BASE_BUDGET_BYTES >= crate::consumer_positions::MAX_BLOCK_BYTES as u64 + 32 * 1024
);

/// The named refusal of a backup whose selection, or whose signed documents,
/// are over the bounds above. It names the message; the runner's
/// `refusal-reason=` line stays `GuardRefused`, as
/// `consumer_positions::SELECTION_TOO_LARGE` does.
pub const SELECTION_TOO_LARGE: &str = "BackupSelectionTooLarge";

/// Why a selection of `topics` names is refused before anything runs, or
/// `None`: more than [`MAX_BACKUP_TOPICS`]. The message starts with
/// [`SELECTION_TOO_LARGE`] and names both numbers and the remedy.
#[must_use]
pub fn refuse_topic_count(topics: usize) -> Option<String> {
    (topics > MAX_BACKUP_TOPICS).then(|| {
        format!(
            "{SELECTION_TOO_LARGE}: the backup names {topics} topics and at most \
             {MAX_BACKUP_TOPICS} may be named. A backup signs a receipt of up to \
             {RECEIPT_TOPIC_BUDGET_BYTES} bytes a topic, and one over {MAX_RECEIPT_BYTES} bytes \
             could not be listed by the recovery catalog or verified by the controller. Split \
             the topics across backups"
        )
    })
}

/// Why a backup whose signed documents could reach these sizes is refused
/// before the engine runs, or `None`. `receipt_bytes` and `record_bytes` are
/// the largest the run could sign (`logweir::backup::projected_documents`).
/// The message starts with [`SELECTION_TOO_LARGE`], names the document over
/// its bound, both sizes, and the remedy.
#[must_use]
pub fn refuse_document_bytes(
    topics: usize,
    receipt_bytes: u64,
    record_bytes: u64,
) -> Option<String> {
    let over = if receipt_bytes > MAX_RECEIPT_BYTES {
        Some(("receipt", receipt_bytes, MAX_RECEIPT_BYTES))
    } else if record_bytes > MAX_RECORD_BYTES {
        Some(("catalog point record", record_bytes, MAX_RECORD_BYTES))
    } else {
        None
    };
    over.map(|(document, bytes, bound)| {
        let per_topic = bytes / (topics.max(1) as u64);
        format!(
            "{SELECTION_TOO_LARGE}: the {document} of these {topics} topics could be {bytes} \
             bytes (about {per_topic} a topic, with the configuration the source reports now), \
             over the {bound}-byte bound the recovery catalog and the controller read. The \
             budget is {RECEIPT_TOPIC_BUDGET_BYTES} bytes a topic for {MAX_BACKUP_TOPICS} \
             topics: these topics carry more recorded configuration, or longer names, than \
             that. Split the topics across backups"
        )
    })
}

// ---------------------------------------------------------------------------
// The reference topic: what the budget is stated FOR
// ---------------------------------------------------------------------------

/// The shape of the topics a [`reference_receipt`] names.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReferenceShape {
    /// The length of every topic name, in bytes; at least 11, at most 249
    /// (Kafka's limit). A receipt writes a name six times.
    pub name_bytes: usize,
    /// How many non-semantic keys every topic overrides, each recorded with
    /// its value, on top of the 13 semantic entries; at most 22.
    pub overrides: usize,
    /// How many consumer groups the backup selected, each with a 255-byte id
    /// and captured: 100 is PROD-04.1's largest summary. 0 selects none, and
    /// the receipt then carries no `consumer_positions`.
    pub consumer_groups: usize,
}

impl ReferenceShape {
    /// A topic as a 4.x broker reports it at its defaults, under a
    /// 50-character name: the shape the per-topic budget is quoted for.
    pub const DEFAULTS: Self = Self {
        name_bytes: 50,
        overrides: 0,
        consumer_groups: 0,
    };

    /// [`Self::DEFAULTS`] with PROD-04.1's largest consumer position summary:
    /// "a topic with full recorded configuration" in FX-33's acceptance.
    pub const FULL: Self = Self {
        name_bytes: 50,
        overrides: 0,
        consumer_groups: crate::consumer_positions::MAX_SELECTED_GROUPS,
    };
}

impl ReferenceShape {
    /// Kafka's longest topic names (249 bytes), five recorded overrides and
    /// the largest consumer position summary: with [`REFERENCE_AT_THE_BOUND`]
    /// topics, a valid receipt within one percent of [`MAX_RECEIPT_BYTES`].
    /// It is what a reader's memory is measured over.
    pub const LONGEST_NAMES: Self = Self {
        name_bytes: 249,
        overrides: 5,
        consumer_groups: crate::consumer_positions::MAX_SELECTED_GROUPS,
    };
}

/// How many [`ReferenceShape::LONGEST_NAMES`] topics the receipt at the bound
/// names.
pub const REFERENCE_AT_THE_BOUND: usize = 1_000;

/// **The reference receipts other crates hold signatures over**: a label, a
/// topic count and a shape each. FX-33's acceptance sizes in the
/// [`ReferenceShape::FULL`] shape, and the receipt at the bound.
///
/// `weirkeeper` cannot link the signer, so its tests verify committed
/// sidecars over these receipts (`crates/weirkeeper/tests/fixtures/
/// topic-budget/reference-<label>.sig`), and `crates/logweir/tests/
/// topic_budget.rs` holds each sidecar to what the signer writes over the
/// same bytes today.
pub const REFERENCE_SET: [(&str, usize, ReferenceShape); 7] = [
    ("70", 70, ReferenceShape::FULL),
    ("105", 105, ReferenceShape::FULL),
    ("113", 113, ReferenceShape::FULL),
    ("300", 300, ReferenceShape::FULL),
    ("500", 500, ReferenceShape::FULL),
    ("1000", 1_000, ReferenceShape::FULL),
    (
        "at-the-bound",
        REFERENCE_AT_THE_BOUND,
        ReferenceShape::LONGEST_NAMES,
    ),
];

/// The name of reference topic `index` under `shape`: `topic-<5 digits>`,
/// padded with `x` to the shape's length.
#[must_use]
pub fn reference_topic_name(index: usize, shape: &ReferenceShape) -> String {
    let base = format!("topic-{index:05}");
    let pad = shape.name_bytes.saturating_sub(base.len());
    format!("{base}{}", "x".repeat(pad))
}

/// **The receipt the budget is stated for**: a VALID backup receipt of
/// `topics` topics, each with full recorded configuration.
///
/// Every topic carries what a 4.x broker's topic has at its defaults and a
/// run records of it:
///
/// - a `records` count;
/// - a `config_coverage` entry, `captured`, with the effective timestamp type
///   and its source;
/// - a `topic_configuration` entry: the partition count, the replication
///   factor, and the 13 semantic entries Kafka 4.x defines
///   (`topic_configuration::TABLE`), each at the table's sample value from
///   the broker's default (`inherited`), plus `shape.overrides` explicit
///   overrides of non-semantic keys;
/// - a `schema_dependency` entry (`notDetected`, sampled, both sides judged);
/// - a `generations` entry with the topic ID before and after the engine.
///
/// With `shape.consumer_groups` over 0 it also carries a `consumer_positions`
/// summary of that many captured groups with 255-byte ids.
///
/// It is deterministic — the same arguments give the same bytes through
/// `det_json` — so a test in one crate can hold a signature another crate's
/// test made over it. It is built from the receipt's own types, so a block
/// added to the receipt is absent here until it is added here, and
/// `tests/topic_budget.rs` fails on that. Nothing in a product path calls it:
/// it is the definition the budget's numbers are measurements OF.
#[must_use]
pub fn reference_receipt(
    topics: usize,
    shape: &ReferenceShape,
) -> crate::backup_receipt::BackupReceipt {
    use crate::backup_receipt::{
        BackupReceipt, ConfigEntry, EffectiveConfigValue, ReceiptArchive, ReceiptAuth,
        ReceiptCovered, ReceiptEngine, ReceiptSource, SideFraming, TopicConfigCoverage,
        TopicConfiguration, TopicIdentity, TopicSchemaDependency,
    };
    use crate::topic_configuration::{defined_on, INHERITED, KAFKA_LINES, TABLE};
    use std::collections::BTreeMap;

    let names: Vec<String> = (0..topics)
        .map(|i| reference_topic_name(i, shape))
        .collect();
    // 2026-10-09T03:00:00Z, and four minutes of engine.
    let started_at =
        chrono::DateTime::<chrono::Utc>::from_timestamp(1_791_514_800, 0).unwrap_or_default();
    let finished_at = started_at + chrono::Duration::minutes(4);
    let backup_id = "reference-set";
    let run_id = "01JABCDEFGHJKMNPQRSTVWXYZ0";

    let mut entries: BTreeMap<String, ConfigEntry> = TABLE
        .iter()
        .filter(|rule| rule.semantic && defined_on(rule.key, KAFKA_LINES[1]))
        .map(|rule| {
            (
                rule.key.to_string(),
                ConfigEntry {
                    value: Some(rule.sample.to_string()),
                    source: "defaultConfig".to_string(),
                    portability: INHERITED.to_string(),
                },
            )
        })
        .collect();
    for rule in TABLE
        .iter()
        .filter(|rule| !rule.semantic)
        .take(shape.overrides)
    {
        entries.insert(
            rule.key.to_string(),
            ConfigEntry {
                value: Some(rule.sample.to_string()),
                source: crate::topic_configuration::TOPIC_OVERRIDE_SOURCE.to_string(),
                portability: rule.class.wire_name().to_string(),
            },
        );
    }
    let side = || SideFraming {
        dependent: false,
        framed: 0,
        unframed: 40,
        nulls: 0,
        schema_ids: Vec::new(),
        schema_id_count: 0,
    };
    let consumer_positions = (shape.consumer_groups > 0).then(|| {
        use crate::consumer_positions::{
            ConsumerPositions, DocumentRef, GroupSnapshot, PositionCounts,
        };
        ConsumerPositions {
            observed_from: started_at - chrono::Duration::seconds(2),
            observed_to: started_at - chrono::Duration::seconds(1),
            listing: "complete".to_string(),
            document: DocumentRef {
                key: crate::consumer_positions::document_key(backup_id, run_id),
                sha256: format!("sha256:{}", "c".repeat(64)),
                bytes: 1,
            },
            groups: (0..shape.consumer_groups)
                .map(|g| {
                    (
                        format!(
                            "{g:03}{}",
                            "g".repeat(crate::consumer_positions::MAX_GROUP_ID_BYTES - 3)
                        ),
                        GroupSnapshot {
                            outcome: "captured".to_string(),
                            reason: None,
                            group_type: Some("consumer".to_string()),
                            state: Some("Stable".to_string()),
                            listed_state: Some("Stable".to_string()),
                            members: Some(3),
                            active: Some(true),
                            counts: Some(PositionCounts {
                                related: u32::try_from(topics).unwrap_or(u32::MAX),
                                not_related: 0,
                                never_committed: 0,
                                beyond_end: 0,
                                failed: 0,
                                not_observed: 0,
                            }),
                        },
                    )
                })
                .collect(),
        }
    });

    BackupReceipt {
        format_version: if consumer_positions.is_some() {
            crate::backup_receipt::FORMAT_VERSION_WITH_CONSUMER_POSITIONS
        } else {
            crate::backup_receipt::FORMAT_VERSION_WITH_GENERATIONS
        }
        .to_string(),
        run_id: run_id.to_string(),
        backup_id: backup_id.to_string(),
        requested_at: started_at - chrono::Duration::seconds(5),
        started_at,
        finished_at,
        exit_code: 0,
        triggered_by: "schedule".to_string(),
        source: ReceiptSource {
            cluster_id: "REFERENCE-SOURCE-CLUSTER0".to_string(),
            bootstrap_servers: vec!["kafka-source:9092".to_string()],
            auth: ReceiptAuth {
                mode: "scramSha512".to_string(),
                username: Some("logweir".to_string()),
            },
            topics: names.clone(),
        },
        engine: ReceiptEngine {
            id: "oso".to_string(),
            version: "0.23.3+logweir.2".to_string(),
            digest: format!("sha256:{}", "d".repeat(64)),
        },
        archive: ReceiptArchive {
            manifest_key: format!("kafka-backups/{backup_id}/manifest.json"),
            manifest_sha256: format!("sha256:{}", "a".repeat(64)),
            manifest_version_id: None,
            prefix: "kafka-backups".to_string(),
        },
        records: names.iter().map(|t| (t.clone(), 123_456)).collect(),
        covered: ReceiptCovered {
            from_ms: started_at.timestamp_millis() - 3_600_000,
            to_ms: started_at.timestamp_millis(),
        },
        config_coverage: Some(
            names
                .iter()
                .map(|t| {
                    (
                        t.clone(),
                        TopicConfigCoverage {
                            coverage: "captured".to_string(),
                            reason: None,
                            timestamp_type: Some(EffectiveConfigValue {
                                value: "CreateTime".to_string(),
                                source: "defaultConfig".to_string(),
                            }),
                        },
                    )
                })
                .collect(),
        ),
        topic_configuration: Some(
            names
                .iter()
                .map(|t| {
                    (
                        t.clone(),
                        TopicConfiguration {
                            partitions: Some(12),
                            replication_factor: Some(3),
                            entries: Some(entries.clone()),
                            owner: None,
                        },
                    )
                })
                .collect(),
        ),
        owner_detection: Some(Vec::new()),
        schema_dependency: Some(
            names
                .iter()
                .map(|t| {
                    (
                        t.clone(),
                        TopicSchemaDependency {
                            verdict: crate::schema_dependency::NOT_DETECTED.to_string(),
                            reason: None,
                            basis: Some(crate::schema_dependency::BASIS_SAMPLED.to_string()),
                            key: Some(side()),
                            value: Some(side()),
                        },
                    )
                })
                .collect(),
        ),
        generations: Some(
            names
                .iter()
                .enumerate()
                .map(|(i, t)| {
                    // A distinct, canonical ID per topic: the index in the low
                    // half, a fixed non-zero high half.
                    let id = crate::topic_identity::topic_id_text(
                        0x0123_4567_89ab_cdef,
                        i64::try_from(i).unwrap_or(0) + 2,
                    );
                    (
                        t.clone(),
                        TopicIdentity {
                            topic_id: id.clone(),
                            topic_id_after: id,
                            topic_id_source: Some(
                                crate::topic_identity::DESCRIBE_TOPICS.to_string(),
                            ),
                            topic_id_reason: None,
                            topic_id_after_reason: None,
                        },
                    )
                })
                .collect(),
        ),
        consumer_positions,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The sums are the sums, and the count refusal is at the boundary: the
    /// maximum is admitted, one more is refused by name with both numbers.
    #[test]
    fn the_bounds_are_their_own_sums_and_the_count_is_refused_one_past_the_maximum() {
        assert_eq!(MAX_RECEIPT_BYTES, 131_072 + 1_000 * 5_000);
        assert_eq!(MAX_RECORD_BYTES, 131_072 + 1_000 * 6_000);
        assert_eq!(refuse_topic_count(0), None);
        assert_eq!(refuse_topic_count(MAX_BACKUP_TOPICS), None);
        let why = refuse_topic_count(MAX_BACKUP_TOPICS + 1).expect("one past the maximum");
        assert!(why.starts_with("BackupSelectionTooLarge: "), "{why}");
        assert!(
            why.contains("1001 topics") && why.contains("at most 1000"),
            "{why}"
        );
        assert!(why.contains("5131072"), "{why}");
    }

    /// **The budget is held by rows that exist.** The numbers in this module
    /// are true only because rows in two other crates measure them: one
    /// topic's cost against its budget, the projection against the receipt's
    /// schema, the two refusals, the controller's cap. A row deleted there
    /// would leave every constant here unguarded and nothing else failing, so
    /// each is named here and must be a `#[test]`.
    ///
    /// KILLS: the per-topic budget test removed.
    #[test]
    fn the_budget_is_held_by_rows_that_exist() {
        let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .and_then(std::path::Path::parent)
            .expect("crates/logweir-core has a grandparent");
        for (file, rows) in [
            (
                "crates/logweir/tests/topic_budget.rs",
                &[
                    "one_topic_costs_no_more_than_its_budget",
                    "the_acceptance_sizes_fit_their_bounds_and_five_thousand_does_not",
                    "no_real_receipt_is_larger_than_its_projection",
                    "the_projection_carries_every_field_the_receipt_defines",
                    "a_selection_over_the_maximum_is_refused_before_any_client_is_used",
                    "a_receipt_that_would_be_over_the_bound_is_refused_before_the_engine",
                    "the_controllers_reference_sidecars_are_what_the_signer_writes",
                    "the_documentation_states_the_budget_in_the_constants_own_numbers",
                ][..],
            ),
            (
                "crates/weirkeeper/tests/topic_budget.rs",
                &[
                    "the_controllers_document_cap_is_one_of_its_two_rows",
                    "a_backup_of_many_topics_is_verified_by_the_controller",
                    "the_largest_receipt_fits_the_relay",
                    "the_controller_builds_no_tree_of_a_receipt_on_any_path",
                    "a_receipt_read_and_a_receipt_relay_stay_inside_what_they_reserve",
                    "simultaneous_large_receipts_stay_inside_the_read_budget",
                ][..],
            ),
        ] {
            let text = std::fs::read_to_string(root.join(file))
                .unwrap_or_else(|e| panic!("{file} is checked in: {e}"));
            for row in rows {
                let as_a_test = format!("#[test]\nfn {row}()");
                assert!(
                    text.contains(&as_a_test),
                    "{file} no longer holds the row `{row}` as a test. The topic budget's \
                     constants are measurements that row makes; put it back, or change the \
                     budget and this list together"
                );
            }
        }
    }

    /// The reference receipt is a receipt: it holds every invariant the
    /// reader checks, at each shape and size the budget's rows use, and it is
    /// deterministic.
    #[test]
    fn the_reference_receipt_holds_the_receipts_own_invariants() {
        let longest = ReferenceShape {
            name_bytes: 249,
            overrides: 22,
            consumer_groups: crate::consumer_positions::MAX_SELECTED_GROUPS,
        };
        for shape in [ReferenceShape::DEFAULTS, ReferenceShape::FULL, longest] {
            for topics in [1, 2, 70, 500] {
                let receipt = reference_receipt(topics, &shape);
                assert_eq!(receipt.validate_invariants(), Ok(()), "{topics} {shape:?}");
                assert_eq!(receipt.source.topics.len(), topics);
                let entries = receipt.topic_configuration.as_ref().unwrap()
                    [&reference_topic_name(0, &shape)]
                    .entries
                    .as_ref()
                    .unwrap()
                    .len();
                assert_eq!(entries, 13 + shape.overrides, "{shape:?}");
                assert_eq!(
                    crate::det_json::to_deterministic_json(&receipt).unwrap(),
                    crate::det_json::to_deterministic_json(&reference_receipt(topics, &shape))
                        .unwrap(),
                    "the reference is deterministic"
                );
            }
        }
        assert_eq!(reference_topic_name(7, &ReferenceShape::DEFAULTS).len(), 50);
        assert_eq!(reference_topic_name(7, &longest).len(), 249);
        // Distinct names and distinct IDs: a reference of N topics is N topics.
        let r = reference_receipt(300, &ReferenceShape::DEFAULTS);
        let ids: std::collections::BTreeSet<_> = r
            .generations
            .as_ref()
            .unwrap()
            .values()
            .map(|g| g.topic_id.clone())
            .collect();
        assert_eq!(ids.len(), 300);
        assert!(ids.iter().all(Option::is_some));
    }

    /// The byte refusal names the document that is over, its size and its
    /// bound; a receipt at its bound and a record at its bound are admitted.
    #[test]
    fn the_byte_refusal_names_the_document_its_size_and_its_bound() {
        assert_eq!(
            refuse_document_bytes(1_000, MAX_RECEIPT_BYTES, MAX_RECORD_BYTES),
            None,
            "NEGATIVE CONTROL: at the bounds nothing is refused"
        );
        let receipt = refuse_document_bytes(900, MAX_RECEIPT_BYTES + 1, 0).expect("over");
        assert!(receipt.starts_with("BackupSelectionTooLarge: the receipt of these 900 topics"));
        assert!(
            receipt.contains("5131073") && receipt.contains("5131072-byte bound"),
            "{receipt}"
        );
        let record =
            refuse_document_bytes(900, MAX_RECEIPT_BYTES, MAX_RECORD_BYTES + 1).expect("over");
        assert!(
            record.starts_with("BackupSelectionTooLarge: the catalog point record of these 900"),
            "{record}"
        );
        assert!(
            record.contains("6131073") && record.contains("6131072-byte bound"),
            "{record}"
        );
    }
}
