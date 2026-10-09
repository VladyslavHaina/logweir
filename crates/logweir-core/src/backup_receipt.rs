//! The backup receipt: what an auditor can check about a backup that
//! Logweir took, signed on its own.
//!
//! # Why a NEW DOCUMENT and not new scorecard fields
//!
//! Global Constraint 12 freezes the drill scorecard at `format_version 1.0.0`
//! with **21 top-level properties and 17 required ones**, and permits tag 1 to
//! add NESTED OPTIONAL fields only. The backup's evidence is not a nested
//! detail of a restore drill: the source cluster id read from the broker, the
//! rendered auth mode, the named topic set, the engine id/version/digest, the
//! `backup_id`, the manifest key and its sha256, the per-topic record counts
//! and the covered time range are top-level facts about a DIFFERENT
//! operation, on a different cluster, at a different time. Bolting them onto
//! the scorecard would have cost eight new top-level properties in the one
//! document GC12 exists to keep still — and would have left every scorecard
//! ever written claiming, by the shape of its own schema, to say something
//! about a backup it never observed.
//!
//! So this is its own media type
//! (`logweir_verify::PAYLOAD_TYPE_BACKUP_RECEIPT`), its own schema
//! (`schemas/logweir-backup-receipt-<format_version>.json`, one file per MINOR,
//! the older ones frozen), its own `format_version` and its own arms — four
//! self-contradiction invariants and one closed value set in 1.0.0, and six
//! more (arms 6-11) that read only the 1.1.0 `config_coverage` block. Spec §7:
//! "new payload types, not new scorecard fields."
//!
//! # 1.1.0: topic-configuration capture coverage (FX-4)
//!
//! The engine captures topic configuration non-fatally (`backup/engine.rs:
//! 382-393` in the pinned source): a denied DescribeConfigs leaves the
//! manifest's `configurations` EMPTY, which reads exactly like "this topic has
//! no overrides", and one denied topic empties every topic of the run
//! (`kafka/admin.rs:476-487` fails the whole call on the first per-resource
//! error). `config_coverage` records, per named topic, what Logweir's OWN
//! DescribeConfigs read established about that manifest record — see
//! [`TopicConfigCoverage`]. ABSENT (every 1.0.0 receipt) means UNKNOWN for
//! every topic, never `captured`; [`SourceConfigCoverage`] is the one reader
//! of the block and cannot answer anything stronger for an absent entry.
//!
//! # 1.2.0: the manifest's version id, pinned (FX-7)
//!
//! On a bucket with versioning enabled, `archive.manifest_version_id` names
//! WHICH VERSION of the manifest key the run read back; a receipt that carries
//! it is written as [`FORMAT_VERSION_WITH_MANIFEST_VERSION`], every other one
//! as [`RECEIPT_FORMAT_VERSION`] ([`format_version_for`] decides). No arm reads
//! the pin: a reader compares it with the bucket it reads (`logweir`'s
//! `catalog::pin`), and a 1.1.0 reader, which ignores unknown fields inside
//! major 1, reads a 1.2.0 receipt as the 1.1.0 document under it.
//!
//! # 1.3.0: the topic configuration model (PROD-05.1)
//!
//! `topic_configuration` records, per named topic, the source's partition
//! count as the archive's manifest records it, its replication factor as
//! Logweir's own metadata read found it (the manifest's only where that read
//! named none), the topic's
//! configuration entries as Logweir's own read returned them — each with its
//! source and a portability class from
//! `crate::topic_configuration::TABLE` — and the topic's declarative owner
//! ([`TopicConfiguration`]), with `owner_detection` saying where the run
//! looked for owners. Arms 12-21 read them, and only when they are present.
//! Every receipt this build signs carries it, so every one is written as
//! [`FORMAT_VERSION_WITH_TOPIC_CONFIGURATION`] ([`format_version_for`]).
//!
//! # 1.5.0: schema dependency (PROD-03.0)
//!
//! `schema_dependency` records, per named topic, whether the archived keys and
//! values carry Confluent wire-format framing (magic byte 0 and a schema id),
//! judged from the segment bytes the run just wrote — never from a registry,
//! which Logweir never contacts — with the schema ids seen
//! ([`TopicSchemaDependency`]; the detection contract is
//! `crate::schema_dependency`). Arms 22-29 read it, and only when it is
//! present. ABSENT means NOT ASSESSED for every topic, never "not
//! schema-dependent" ([`SchemaDependency::of`]). Every receipt this build signs
//! carries it, so every one is written as
//! [`FORMAT_VERSION_WITH_SCHEMA_DEPENDENCY`] or later.
//!
//! # 1.6.0: the topic's ID before and after the engine (PROD-01.4a)
//!
//! `generations` records, per named topic, the topic ID (KIP-516) Logweir's
//! own DescribeTopics read returned immediately before the engine
//! (`topic_id`) and immediately after it (`topic_id_after`), in Kafka's text
//! form, or `null` with the reason ([`TopicIdentity`]). It is what tells a
//! topic deleted and recreated under the same name — a NEW generation, whose
//! offsets mean other records — from the same topic
//! (`crate::topic_identity`). ABSENT (every receipt before 1.6.0) means the
//! generation is UNKNOWN for every topic, never "the same". Arms 36-40 read
//! it, and only when it is present. Every receipt this build signs carries
//! it, so every one is written as [`FORMAT_VERSION_WITH_GENERATIONS`] or,
//! when its backup selected consumer groups,
//! [`FORMAT_VERSION_WITH_CONSUMER_POSITIONS`].
//! PROD-02.1 extends the same block with the offset observation and the
//! lineage of decision §3.2.
//!
//! # A backup that produces no verifiable evidence is a backup an auditor has
//! # to take Logweir's word for
//!
//! That is the whole reason this file exists. `logweir backup run` measures
//! all of the above and, before this document, could only print it. A printed
//! line is not evidence: nothing binds it to the archive it describes and
//! nothing stops it being retyped. A signed receipt is checkable by a third
//! party who has the public key and neither the cluster nor the bucket.
//!
//! # Global Constraint 1
//!
//! No I/O, no clock, no network. `validate_invariants` is a pure function of
//! the document; every timestamp here is a value the caller measured and
//! handed in.

use chrono::{DateTime, Utc};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// The signed record of one `logweir backup run`.
///
/// Field order is the document's own serialisation order (`serde_json` is
/// built with `preserve_order`, so declaration order IS byte order through
/// `crate::det_json::to_deterministic_json`). Do not reorder without
/// regenerating the current receipt schema (`just schema`; the 1.0.0 and 1.1.0
/// files are frozen) and re-minting `e2e/fixtures/signed/backup-receipt.json`.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct BackupReceipt {
    /// Semver of THIS format — [`RECEIPT_FORMAT_VERSION`] (`1.1.0`) since
    /// FX-4, or [`FORMAT_VERSION_WITH_MANIFEST_VERSION`] (`1.2.0`) for a
    /// receipt that pins `archive.manifest_version_id` (FX-7); `1.0.0` before
    /// FX-4. Independent of the scorecard's.
    ///
    /// The schema PINS the major with a pattern rather than leaving the field
    /// an unconstrained string, for the reason
    /// `crate::scorecard::Scorecard::format_version` records: without it a
    /// document reading `"9.9.9"` validated cleanly against the file named
    /// `logweir-backup-receipt-1.0.0.json`, so a schema-only validator — the
    /// one route that does not go through `validate_invariants` — accepted
    /// exactly the document GC12 exists to refuse. Any `1.x.y` is allowed,
    /// because a MINOR bump adds optional fields only and a 1.0.0 reader must
    /// still read it.
    #[schemars(regex(pattern = r"^1\.[0-9]+\.[0-9]+$"))]
    pub format_version: String,
    /// ULID of the run that produced this receipt. Also the object key stem
    /// in the evidence bucket, exactly as `Scorecard::run_id` is.
    pub run_id: String,
    /// The engine's own identifier for the archive this run wrote. Distinct
    /// from `run_id`: two runs can be asked to append to one backup set, and
    /// `archive.manifest_key` is keyed on THIS.
    pub backup_id: String,
    /// When the run was requested — the same clock reading
    /// `Scorecard::requested_at` carries.
    pub requested_at: DateTime<Utc>,
    /// When the engine subprocess started. Logweir-measured, never
    /// engine-reported (`crate::engine::BackupFacts`): `backup` has no
    /// `--format` and writes no report file.
    pub started_at: DateTime<Utc>,
    /// When the engine subprocess finished.
    pub finished_at: DateTime<Utc>,
    /// The engine's exit status, as `crate::engine::BackupFacts` measured it —
    /// NOT the `logweir` process's own exit code, which maps through Global
    /// Constraint 11. Invariant 2 below is the biconditional that makes this
    /// field mean something: a receipt for a failed backup names no manifest.
    pub exit_code: i32,
    /// Free text from `--triggered-by`. Deliberately **not** a metric label:
    /// unbounded cardinality, for the reason `Scorecard::triggered_by`
    /// records.
    pub triggered_by: String,
    pub source: ReceiptSource,
    pub engine: ReceiptEngine,
    pub archive: ReceiptArchive,
    /// Records captured, per topic. Invariant 3 requires exactly one entry
    /// per `source.topics` entry and no others: a receipt that counts a topic
    /// the run was never asked to back up, or omits one it was, is describing
    /// some other run.
    ///
    /// A `BTreeMap`, so the key order is the topic names' own order and two
    /// runs over the same topic set produce byte-identical bytes here.
    pub records: BTreeMap<String, u64>,
    pub covered: ReceiptCovered,
    /// **Format 1.1.0 (FX-4).** Per named topic, whether the archive's record
    /// of the topic's configuration was captured, and the topic's EFFECTIVE
    /// `message.timestamp.type` with where that value came from. One entry per
    /// `source.topics` entry and no others (arm 7).
    ///
    /// ABSENT means UNKNOWN for every topic — the state of every receipt
    /// written before 1.1.0 — and is never read as `captured`: a reader goes
    /// through [`SourceConfigCoverage`], whose answer for an absent block is
    /// [`ConfigCoverage::Unknown`]. Appended LAST (declaration order is byte
    /// order) and skipped when absent, so a 1.0.0 document round-trips
    /// byte-for-byte.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub config_coverage: Option<BTreeMap<String, TopicConfigCoverage>>,
    /// **Format 1.3.0 (PROD-05.1).** Per named topic, the configuration model
    /// a restore rebuilds the topic from: partition count, replication factor,
    /// the recorded configuration entries with their portability, and the
    /// declarative owner. One entry per `source.topics` entry and no others
    /// (arm 14); requires `config_coverage` (arm 13), whose read it shares.
    ///
    /// ABSENT means NOT RECORDED for every topic — every receipt before 1.3.0
    /// — and never "no configuration": a reader that needs a topic's settings
    /// then has none, and says so. Appended LAST and skipped when absent, so
    /// an older document round-trips byte for byte.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub topic_configuration: Option<BTreeMap<String, TopicConfiguration>>,
    /// **Format 1.3.0 (PROD-05.1).** WHERE the run looked for declarative
    /// owners: `declared` (the plan carried `source.topic_owners`) and/or
    /// `kafkaTopicResources` (it was given Strimzi `KafkaTopic` resources),
    /// from the closed set `crate::topic_configuration::OWNER_DETECTION_SOURCES`
    /// (arm 20), each at most once. EMPTY means the run looked nowhere: a
    /// topic without an `owner` then has an owner NOT CHECKED, and a reader
    /// never says it is applied through the admin API. Every owner's basis
    /// names a source listed here (arm 21). Present only beside
    /// `topic_configuration`; ABSENT reads as empty.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub owner_detection: Option<Vec<String>>,
    /// **Format 1.5.0 (PROD-03.0).** Per named topic, whether the archived
    /// records carry Confluent wire-format framing — magic byte 0 and a schema
    /// id — so that an application needs the schema registry that issued those
    /// ids to read them after a restore. Judged by Logweir from the archived
    /// segment bytes this run wrote, never from a registry (Logweir contacts
    /// none), with keys and values judged separately
    /// (`crate::schema_dependency` is the detection contract). One entry per
    /// `source.topics` entry and no others (arm 23).
    ///
    /// No registry is ever captured (`docs/stability.md` Never #2), so a
    /// `schemaDependent` topic reads "schema-dependent, registry not captured"
    /// on every surface.
    ///
    /// ABSENT means NOT ASSESSED for every topic — every receipt before 1.5.0
    /// — and never "not schema-dependent". [`SchemaDependency::of`] states
    /// that rule in one place (it answers [`SchemaDependency::NotAssessed`] for
    /// an absent block or entry); each surface — the two verifiers, the
    /// catalog rule-3 check, the API and the console — applies the same rule
    /// itself and holds it with its own row. Appended LAST and skipped when
    /// absent, so an older document round-trips byte for byte.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub schema_dependency: Option<BTreeMap<String, TopicSchemaDependency>>,
    /// **Format 1.6.0 (PROD-01.4a).** Per named topic, the topic ID Logweir's
    /// own DescribeTopics read returned before the engine and after it, or
    /// why there is none ([`TopicIdentity`]). One entry per `source.topics`
    /// entry and no others (arm 37).
    ///
    /// ABSENT means the generation of every topic is UNKNOWN — every receipt
    /// before 1.6.0 — and never "the same as the previous point":
    /// `crate::topic_identity::between` reads an absent block as
    /// `NotEstablished`. Appended LAST and skipped when absent, so an older
    /// document round-trips byte for byte.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub generations: Option<BTreeMap<String, TopicIdentity>>,
    /// **Format 1.7.0 (PROD-04.1).** The consumer position evidence of the
    /// groups the backup SELECTED: one outcome per group with its position
    /// counts, the observation window, and the positions DOCUMENT beside this
    /// receipt that holds the positions, bound by its SHA-256 and length
    /// ([`crate::consumer_positions`]). Its size depends on the selection
    /// only, never on partitions. Arms 30-35 read it, and only when it is
    /// present; a reader given the document checks it with
    /// [`BackupReceipt::validate_consumer_positions_document`].
    ///
    /// ABSENT means the backup selected no group — every receipt before 1.7.0,
    /// and every later one whose plan names none — and never "the groups had
    /// no positions". Appended LAST and skipped when absent, so a backup that
    /// selects no group writes the document it wrote before, byte for byte.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub consumer_positions: Option<crate::consumer_positions::ConsumerPositions>,
}

/// **PROD-01.4a, receipt 1.6.0.** What one run observed about one topic's
/// identity: the topic ID (KIP-516) before the engine and after it.
///
/// # Where each part comes from
///
/// Logweir's OWN DescribeTopics read of the source (`logweir-rdkafka-ffi`,
/// OD-6 (a2)), through the engine's principal: once immediately before the
/// engine starts, once immediately after it exits. The text is Kafka's, the
/// one `kafka-topics.sh --describe` prints, derived from the ID's two halves
/// (`crate::topic_identity::topic_id_text`). Kafka's two reserved IDs, which
/// no topic is ever given, are never written: the all-zero ID — Kafka's "no
/// ID", a cluster below inter-broker protocol 2.8 — becomes `null` with
/// `noTopicId`, and `AAAAAAAAAAAAAAAAAAAAAQ` (`Uuid.ONE_UUID`,
/// `METADATA_TOPIC_ID`) `null` with `reservedTopicId`.
///
/// | field | present | meaning |
/// |---|---|---|
/// | `topic_id` | always, possibly `null` | the ID before the engine |
/// | `topic_id_after` | always, possibly `null` | the ID after it; a different non-null value means the topic was recreated WHILE the engine ran |
/// | `topic_id_source` | exactly when an ID is recorded (arm 40) | `describeTopics` (this build), or `engineManifest` (decision §6.3, not written by this build) |
/// | `topic_id_reason` | exactly when `topic_id` is `null` (arm 39) | why: `noTopicId`, `notAuthorized`, `topicNotFound`, `readFailed`, `notRead` or `reservedTopicId` |
/// | `topic_id_after_reason` | exactly when `topic_id_after` is `null` (arm 39) | the same, for the read after the engine |
///
/// `null` means UNKNOWN, never "the same" (decision §3.1): a reason says why,
/// so a refused read (`notAuthorized`) is never mistaken for a broker that has
/// no IDs (`noTopicId`). A reader reads an absent `topic_id` or
/// `topic_id_after` as `null`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct TopicIdentity {
    /// The topic ID before the engine, or `null`. Kafka's text: 22 URL-safe
    /// base64 characters, the last of which carries two bits (arm 38 also
    /// refuses Kafka's reserved IDs, which the pattern cannot).
    #[serde(default)]
    #[schemars(regex(pattern = r"^[A-Za-z0-9_-]{21}[AQgw]$"))]
    pub topic_id: Option<String>,
    /// The topic ID after the engine, or `null`; the same form.
    #[serde(default)]
    #[schemars(regex(pattern = r"^[A-Za-z0-9_-]{21}[AQgw]$"))]
    pub topic_id_after: Option<String>,
    /// Where the recorded IDs came from: `describeTopics` or
    /// `engineManifest` (`crate::topic_identity::TOPIC_ID_SOURCES`). Present
    /// exactly when at least one ID is recorded.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub topic_id_source: Option<String>,
    /// Why `topic_id` is `null` (`crate::topic_identity::TOPIC_ID_REASONS`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub topic_id_reason: Option<String>,
    /// Why `topic_id_after` is `null`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub topic_id_after_reason: Option<String>,
}

/// **PROD-03.0, receipt 1.5.0.** What the archived bytes of ONE topic say
/// about its dependence on a schema registry.
///
/// | `verdict` | `reason` | `basis` | `key`/`value` | meaning |
/// |---|---|---|---|---|
/// | `schemaDependent` | — | `sampled`/`complete` | both | at least one side is dependent ([`SideFraming::dependent`]): its records carry Confluent framing, so a reader needs the registry, which was not captured |
/// | `notDetected` | — | `sampled`/`complete` | both | neither side is dependent over the records judged |
/// | `notAssessed` | `noRecords` | — | neither | the archive holds no record of the topic, so there is nothing to judge |
/// | `notAssessed` | `segmentUnreadable` | — | neither | a segment the sample needed could not be read or decoded, or decoded to a count its manifest does not record, or the detector failed |
/// | `notAssessed` | `segmentTooLargeForDetection` | — | neither | a segment the sample needed is stored, or decompresses, larger than the detector reads at backup time |
/// | `notAssessed` | `detectionTimeBudgetExceeded` | — | neither | the backup's detection time budget ran out before the topic was judged |
///
/// `basis` is `complete` exactly when every record the receipt counts for
/// the topic was judged, and `sampled` otherwise (arm 26).
///
/// Strings on the wire, not enums, for the reason `ReceiptAuth::mode` gives;
/// the closed sets are enforced by arm 24 in both readers.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct TopicSchemaDependency {
    /// `schemaDependent`, `notDetected` or `notAssessed` —
    /// [`crate::schema_dependency::VERDICTS`].
    pub verdict: String,
    /// `noRecords`, `segmentUnreadable`, `segmentTooLargeForDetection` or
    /// `detectionTimeBudgetExceeded` —
    /// [`crate::schema_dependency::NOT_ASSESSED_REASONS`] — present exactly
    /// when `verdict` is `notAssessed` (arm 24).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    /// `sampled` or `complete` — [`crate::schema_dependency::BASES`] —
    /// present exactly when `verdict` is not `notAssessed` (arm 24).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub basis: Option<String>,
    /// The record KEYS judged, present exactly when `verdict` is not
    /// `notAssessed` (arm 25).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub key: Option<SideFraming>,
    /// The record VALUES judged, beside `key` and over the same records.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub value: Option<SideFraming>,
}

/// One side (the keys, or the values) of one topic's judged records.
///
/// Every judged record counts once on each side: as `framed`, `unframed` or
/// `nulls` (a null key, or a null value — a tombstone). `nulls` never count
/// toward the share: a side is `dependent` exactly when at least one record
/// is framed and at least one in
/// [`crate::schema_dependency::DEPENDENT_SHARE_DENOMINATOR`] of its NON-NULL
/// records is (arm 28).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct SideFraming {
    /// Whether this side depends on the registry, by the threshold above.
    pub dependent: bool,
    /// Records whose bytes on this side are framed
    /// ([`crate::schema_dependency::framed_schema_id`]).
    pub framed: u64,
    /// Non-null records whose bytes on this side are not framed.
    pub unframed: u64,
    /// Records with no bytes on this side: a null key, or a null value.
    pub nulls: u64,
    /// The distinct schema ids the framed records name, ascending, at most
    /// [`crate::schema_dependency::SCHEMA_IDS_LISTED`] — the smallest ones
    /// when more were seen (arm 27).
    pub schema_ids: Vec<u32>,
    /// How many distinct schema ids the framed records name, listed or not.
    pub schema_id_count: u64,
}

/// A topic's schema dependency as a READER uses it.
///
/// `NotAssessed` is what an absent `schema_dependency` block, an absent
/// entry, a `notAssessed` entry, or a verdict this build does not recognise
/// means. It is never `NotDetected`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SchemaDependency {
    SchemaDependent,
    NotDetected,
    NotAssessed,
}

impl SchemaDependency {
    /// The reader's side of the wire: anything outside the closed set is
    /// `NotAssessed`, never `NotDetected`.
    #[must_use]
    pub fn from_wire(verdict: &str) -> Self {
        match verdict {
            "schemaDependent" => Self::SchemaDependent,
            "notDetected" => Self::NotDetected,
            _ => Self::NotAssessed,
        }
    }

    /// `topic`'s verdict in `receipt`: `NotAssessed` for a receipt without
    /// the 1.5.0 block or without an entry for the topic.
    #[must_use]
    pub fn of(receipt: &BackupReceipt, topic: &str) -> Self {
        receipt
            .schema_dependency
            .as_ref()
            .and_then(|block| block.get(topic))
            .map_or(Self::NotAssessed, |e| Self::from_wire(&e.verdict))
    }
}

/// **PROD-03.0.** The `format_version` of a receipt that carries
/// `schema_dependency` — the MINOR after PROD-01.3's 1.4.0. Every receipt this
/// build signs carries the block, so every one is 1.5.0 or later
/// ([`format_version_for`]); 1.5.0 defines every earlier minor's fields and
/// values. An older reader ignores the field inside major 1 and reads the
/// document under it.
pub const FORMAT_VERSION_WITH_SCHEMA_DEPENDENCY: &str = "1.5.0";

/// The first minor of format 1 that defines `schema_dependency` (arm 22). A
/// renumber moves this, [`FORMAT_VERSION_WITH_SCHEMA_DEPENDENCY`] and
/// `docs/verify_scorecard.py`'s `RECEIPT_SCHEMA_DEPENDENCY_SINCE_MINOR`
/// together; `tests/backup_receipt.rs::the_written_version_defines_schema_dependency`
/// keeps them coherent.
pub const SCHEMA_DEPENDENCY_SINCE_MINOR: u64 = 5;

/// **PROD-04.1.** The `format_version` of a receipt that carries
/// `consumer_positions` — a MINOR bump over [`FORMAT_VERSION_WITH_GENERATIONS`]
/// (PROD-01.4a's 1.6.0) under OD-7 (a): arms 30-35 read only the new optional
/// block, so every document without it is decided exactly as before, and an
/// older reader ignores the field inside major 1. Written only for a backup
/// that selects consumer groups; it includes every earlier minor.
pub const FORMAT_VERSION_WITH_CONSUMER_POSITIONS: &str = "1.7.0";

/// The first minor of format 1 that defines `consumer_positions` (arm 30). A
/// renumber moves this, [`FORMAT_VERSION_WITH_CONSUMER_POSITIONS`] and
/// `docs/verify_scorecard.py`'s `RECEIPT_CONSUMER_POSITIONS_SINCE_MINOR`
/// together; `tests/backup_receipt.rs::the_written_version_defines_consumer_positions`
/// keeps them coherent.
pub const CONSUMER_POSITIONS_SINCE_MINOR: u64 = 7;

/// The `format_version` this build WRITES for a receipt that pins no manifest
/// version (a pinned one is [`FORMAT_VERSION_WITH_MANIFEST_VERSION`]; see
/// [`format_version_for`]). A reader accepts any `1.x.y` (arm 1); a document
/// that carries `config_coverage` must declare a minor of at least
/// [`CONFIG_COVERAGE_SINCE_MINOR`] (arm 6).
pub const RECEIPT_FORMAT_VERSION: &str = "1.1.0";

/// The first minor of format 1 that defines `config_coverage`. **A renumber
/// changes this and [`RECEIPT_FORMAT_VERSION`] together** (FX-4 kept 1.1.0;
/// FX-7's pin, which merged after it, took 1.2.0); arm 6's message and
/// `docs/verify_scorecard.py`'s `RECEIPT_CONFIG_COVERAGE_SINCE_MINOR` follow
/// it, and `tests/backup_receipt.rs::the_written_version_defines_config_coverage`
/// keeps the pair coherent.
pub const CONFIG_COVERAGE_SINCE_MINOR: u64 = 1;

/// `TopicConfigCoverage::coverage`'s closed set (arm 8), in the order the
/// refusal names them.
pub const COVERAGE_VALUES: [&str; 3] = ["captured", "notCaptured", "captureDenied"];

/// `TopicConfigCoverage::reason`'s closed set (arm 9). Present exactly when
/// `coverage` is `notCaptured`.
pub const NOT_CAPTURED_REASONS: [&str; 2] = ["describeFailed", "manifestDiffers"];

/// `EffectiveConfigValue::value`'s closed set for `message.timestamp.type`
/// (arm 11) — Kafka's own two values.
pub const TIMESTAMP_TYPES: [&str; 2] = ["CreateTime", "LogAppendTime"];

/// `EffectiveConfigValue::source`'s closed set (arm 11): Kafka's
/// `DescribeConfigsResponse` `ConfigSource`, camel-cased, and `unknown` for a
/// source the broker did not report (Kafka before 1.1, or a code this client
/// does not map). `dynamicTopicConfig` is the TOPIC OVERRIDE; the other four
/// named sources are the broker's (a per-broker or cluster-wide dynamic
/// default, the broker's static `server.properties`, or the built-in default).
pub const CONFIG_SOURCES: [&str; 6] = [
    "dynamicTopicConfig",
    "dynamicBrokerConfig",
    "dynamicDefaultBrokerConfig",
    "staticBrokerConfig",
    "defaultConfig",
    "unknown",
];

/// **PROD-05.1.** The `format_version` of a receipt that carries
/// `topic_configuration` — the MINOR after FX-7's 1.2.0. Every receipt this
/// build signs carries the block, pinned or not; until PROD-01.4a that made
/// every one 1.3.0, and since then every one is
/// [`FORMAT_VERSION_WITH_GENERATIONS`] ([`format_version_for`]). A 1.0.0,
/// 1.1.0 or 1.2.0 reader ignores the field inside major 1 and reads the
/// document under it.
pub const FORMAT_VERSION_WITH_TOPIC_CONFIGURATION: &str = "1.3.0";

/// The first minor of format 1 that defines `topic_configuration` (arm 12). A
/// renumber moves this, [`FORMAT_VERSION_WITH_TOPIC_CONFIGURATION`] and
/// `docs/verify_scorecard.py`'s `RECEIPT_TOPIC_CONFIGURATION_SINCE_MINOR`
/// together; `tests/backup_receipt.rs::the_written_version_defines_topic_configuration`
/// keeps them coherent.
pub const TOPIC_CONFIGURATION_SINCE_MINOR: u64 = 3;

/// What Logweir established about ONE topic's configuration at capture
/// (receipt 1.1.0, FX-4).
///
/// # Where the answer comes from
///
/// Logweir's OWN DescribeConfigs read of the topic, taken by `logweir backup
/// run` immediately before the engine starts, through the same principal the
/// engine uses — NOT the engine's capture, which is non-fatal, all-or-nothing
/// across the run's topics and invisible in the manifest. That read is then
/// compared with the manifest the engine wrote:
///
/// | `coverage` | `reason` | meaning |
/// |---|---|---|
/// | `captured` | — | the read succeeded AND the manifest's `configurations` for this topic equal the explicit overrides the engine captures (its own filter: topic-override source, not read-only, not sensitive, on its allowlist). The manifest is a complete record of them, so a configuration parity check may compare against it. |
/// | `captureDenied` | — | the broker's authorizer refused the read (`TOPIC_AUTHORIZATION_FAILED`). The engine runs as the same principal, so an empty manifest record says nothing. |
/// | `notCaptured` | `describeFailed` | the read failed for any other reason (a timeout, an unknown topic, a broker error). |
/// | `notCaptured` | `manifestDiffers` | the read succeeded but the manifest does not record the same overrides — the engine's own capture failed (one denied topic empties them all), or the configuration changed between the two reads. |
///
/// Overrides outside the engine's allowlist are never part of the claim:
/// `captured` says the ARCHIVE's configuration record is complete, not that
/// every setting of the topic was archived (that is PROD-05.1's portability
/// table).
///
/// Strings on the wire, not enums, for the reason `ReceiptAuth::mode` gives:
/// a reader must be able to REPORT a value it refuses. The closed sets are
/// enforced by arms 8, 9 and 11 in both readers.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct TopicConfigCoverage {
    /// `captured`, `notCaptured` or `captureDenied` — [`COVERAGE_VALUES`].
    pub coverage: String,
    /// `describeFailed` or `manifestDiffers` — [`NOT_CAPTURED_REASONS`] —
    /// present exactly when `coverage` is `notCaptured` (arm 9).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    /// The topic's EFFECTIVE `message.timestamp.type` as Logweir's read
    /// returned it, and where that value came from — the broker-default arm
    /// of FX-8, which the manifest cannot carry (the engine keeps explicit
    /// overrides only). ABSENT when the read did not succeed (arm 10) or the
    /// broker returned no such entry: the timestamp type is then NOT
    /// RECORDED, never assumed `CreateTime`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timestamp_type: Option<EffectiveConfigValue>,
}

/// One configuration value as the broker reported it: the value in force and
/// where it came from.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct EffectiveConfigValue {
    /// For `timestamp_type`: `CreateTime` or `LogAppendTime`
    /// ([`TIMESTAMP_TYPES`]).
    pub value: String,
    /// One of [`CONFIG_SOURCES`]. `dynamicTopicConfig` is a topic override;
    /// every other named source is the broker's.
    pub source: String,
}

/// **PROD-05.1, receipt 1.3.0.** One topic's configuration model: what a
/// restore needs to rebuild the topic rather than leave it to a target
/// broker's defaults.
///
/// # Where each part comes from
///
/// - `partitions`: the ARCHIVE's record, the manifest's
///   `original_partition_count` — the manifest this run read back and
///   digested, so the count a restore creates the topic with. ABSENT when the
///   manifest does not record one, never `0`.
/// - `replication_factor`: Logweir's OWN metadata read of the source, through
///   the engine's principal, immediately before the engine — the SMALLEST
///   replica count of the topic's partitions (a partition mid-reassignment
///   also lists the replicas being added). Only where that read named no
///   factor for the topic does the manifest's `source_replication_factor`
///   stand, and engine 0.23.3 records that for the first topic it saves only
///   (`merge_manifests` drops it from every later save) unless it is
///   Logweir's build `0.23.3+logweir.2` or later, whose patch 0002 records
///   every topic's (FX-21). When the
///   metadata read is unavailable — it failed, or the reader cannot answer —
///   the run logs a warning and records the factor only where the manifest
///   has one: for every other topic it is ABSENT, NOT RECORDED, never `0` and
///   never the target's.
/// - `entries`: Logweir's OWN DescribeConfigs read before the engine — the
///   same read as `config_coverage` — filtered by
///   `crate::topic_configuration::records`: every explicit override, and the
///   effective value of every semantic key. Present exactly when that read
///   succeeded (arm 15); a `captureDenied` or `describeFailed` topic records
///   none, which reads as NOT RECORDED, never as "no overrides".
/// - `owner`: the plan's declaration (`source.topic_owners`), or a Strimzi
///   `KafkaTopic` resource the run was given. A topic with an owner is
///   restored by exporting desired state for that owner, never through the
///   admin API around it. A topic WITHOUT one has "no owner found" only where
///   the receipt's `owner_detection` says the run looked; where it is empty,
///   the owner was NOT CHECKED.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct TopicConfiguration {
    /// The source's partition count, as the archive's manifest records it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub partitions: Option<u32>,
    /// The source's replication factor: Logweir's own metadata read before
    /// the engine (the smallest replica count of the topic's partitions), else
    /// the manifest's where that read named none. When the read is unavailable
    /// the run warns and the manifest's factor stands where it has one. ABSENT
    /// when neither names one — NOT RECORDED, never `0`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub replication_factor: Option<u32>,
    /// The recorded configuration entries, by key. ABSENT when the read did
    /// not succeed; present and possibly empty when it did.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub entries: Option<BTreeMap<String, ConfigEntry>>,
    /// The topic's declarative owner, when one manages it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub owner: Option<TopicOwner>,
}

/// One recorded configuration entry: the value in force, where it came from,
/// and its portability class.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct ConfigEntry {
    /// The value. ABSENT exactly when `portability` is `secret` (arm 17): a
    /// sensitive entry is recorded by key, never by value.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub value: Option<String>,
    /// One of [`CONFIG_SOURCES`]; `dynamicTopicConfig` is the topic's own
    /// explicit override.
    pub source: String,
    /// One of `crate::topic_configuration::PORTABILITY_CLASSES`: `inherited`
    /// exactly when the source is not the topic's override (arm 17), else the
    /// table's class for the key.
    pub portability: String,
}

/// A declarative owner of a topic's configuration.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct TopicOwner {
    /// `strimzi` or `external`.
    pub kind: String,
    /// `kafkaTopicResource` (a Strimzi `KafkaTopic` named it; `strimzi` only)
    /// or `declared` (the plan said so).
    pub basis: String,
    /// Where the desired state lives: `<namespace>/<name>` of the
    /// `KafkaTopic`, or the plan's own words. 1 to 256 characters.
    pub reference: String,
}

/// A topic's configuration capture coverage as a READER uses it.
///
/// `Unknown` is not a wire value: it is what an absent `config_coverage`
/// block, an absent entry, or a value this build does not recognise means.
/// It is never `Captured`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConfigCoverage {
    Captured,
    NotCaptured,
    CaptureDenied,
    Unknown,
}

impl ConfigCoverage {
    /// The receipt's spelling, and `unknown` for [`ConfigCoverage::Unknown`]
    /// (which no receipt carries; a report uses it).
    #[must_use]
    pub const fn wire_name(self) -> &'static str {
        match self {
            Self::Captured => "captured",
            Self::NotCaptured => "notCaptured",
            Self::CaptureDenied => "captureDenied",
            Self::Unknown => "unknown",
        }
    }

    /// The reader's side of the wire: anything outside the closed set is
    /// `Unknown`, never `Captured`.
    #[must_use]
    pub fn from_wire(value: &str) -> Self {
        match value {
            "captured" => Self::Captured,
            "notCaptured" => Self::NotCaptured,
            "captureDenied" => Self::CaptureDenied,
            _ => Self::Unknown,
        }
    }
}

/// What a restore knows about each SOURCE topic's configuration capture —
/// read from a VERIFIED receipt, or from nothing.
///
/// The one way a parity check asks "may I compare against the manifest's
/// configuration?". [`SourceConfigCoverage::unknown`] (no receipt: a plan not
/// bound to a recovery point) and a receipt without the 1.1.0 block both
/// answer [`ConfigCoverage::Unknown`] for every topic.
///
/// **FX-21.** It also lends the receipt's own record of each source topic's
/// replication factor ([`SourceConfigCoverage::replication_factor`], from
/// `topic_configuration`, format 1.3.0), which a parity check reads where the
/// archive's manifest records none: engine 0.23.3 records the factor in the
/// manifest for the first topic it saves only.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SourceConfigCoverage {
    recorded: Option<BTreeMap<String, TopicConfigCoverage>>,
    /// `topic_configuration[<topic>].replication_factor`, for every topic the
    /// receipt records one for; empty for no receipt and for a receipt before
    /// 1.3.0.
    factors: BTreeMap<String, u32>,
}

impl SourceConfigCoverage {
    /// Nothing recorded: every topic is [`ConfigCoverage::Unknown`].
    #[must_use]
    pub fn unknown() -> Self {
        Self::default()
    }

    /// The receipt's own blocks, copied: `config_coverage` and the
    /// replication factors `topic_configuration` records. The caller must have
    /// VERIFIED the receipt's signature: this type trusts what it is handed.
    #[must_use]
    pub fn from_receipt(receipt: &BackupReceipt) -> Self {
        Self {
            recorded: receipt.config_coverage.clone(),
            factors: receipt
                .topic_configuration
                .iter()
                .flatten()
                .filter_map(|(topic, c)| {
                    c.replication_factor
                        .filter(|f| *f >= 1)
                        .map(|f| (topic.clone(), f))
                })
                .collect(),
        }
    }

    /// **FX-21.** The source `topic`'s replication factor as the receipt
    /// records it (`topic_configuration`, format 1.3.0), or `None` when it
    /// records none — NOT RECORDED, never a default.
    #[must_use]
    pub fn replication_factor(&self, topic: &str) -> Option<u32> {
        self.factors.get(topic).copied()
    }

    /// `topic`'s coverage; `Unknown` for anything not recorded.
    #[must_use]
    pub fn of(&self, topic: &str) -> ConfigCoverage {
        self.entry(topic).map_or(ConfigCoverage::Unknown, |e| {
            ConfigCoverage::from_wire(&e.coverage)
        })
    }

    /// `topic`'s recorded entry, when there is one — how FX-8 reads the
    /// effective `timestamp_type`.
    #[must_use]
    pub fn entry(&self, topic: &str) -> Option<&TopicConfigCoverage> {
        self.recorded.as_ref().and_then(|m| m.get(topic))
    }
}

/// The SOURCE cluster, as measured — never as a spec claimed it.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct ReceiptSource {
    /// Read from the broker at phase −1, never from the spec. GC18(c)'s
    /// fourth rail records the source `cluster_id` and re-asserts it is not
    /// the restore target; this is where the recorded value is attested.
    pub cluster_id: String,
    pub bootstrap_servers: Vec<String>,
    pub auth: ReceiptAuth,
    /// The named topic allowlist the run was given. GC18(c) rail 1: a named
    /// set with no glob metacharacter (**G-GLOB**), so this list is the exact
    /// set of topics, not a pattern that a reader would have to re-expand
    /// against a cluster it cannot see.
    pub topics: Vec<String>,
}

/// How the source client was told to authenticate. **Never a password, and
/// no field that could hold one** — the render-side twin of
/// `crate::engine::AuthRender`, whose doc comment states the same rule for
/// the same reason: the secret reaches the engine through its own `${VAR}`
/// environment expansion and is never interpolated by us, so it can never be
/// interpolated into a document we then sign and publish.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct ReceiptAuth {
    /// **A CLOSED SET, VERSIONED: `"plaintext"` or `"scramSha512"` in every
    /// format, and from 1.4.0 also `"scramSha256"`, `"plain"` or `"mtls"`
    /// (PROD-01.3).** Any other value — or a new value under a format that
    /// predates it — is refused by BOTH readers (arm 5 below, and
    /// `docs/verify_scorecard.py::check_backup_receipt_invariants`'s mirror),
    /// so a receipt naming an undefined spelling is never signed and never
    /// verifies.
    ///
    /// These are `logweir_core::spec::AuthSpec`'s serde tag values, which is
    /// what makes ONE spelling possible at all: they are the strings an
    /// adopter writes in a spec, `KafkaCluster.spec.auth.mode`'s CRD enum
    /// byte for byte, the only values `AuthSpec::mode_str()` can return,
    /// and — since Task 17 copies this field — what
    /// `Backup.status.auth.mode`'s CRD description promises. A `String` and
    /// not an enum on the wire because a reader must be able to REPORT a
    /// value it refuses; the closed set is enforced by the arm, where both
    /// readers can state it in the same words.
    pub mode: String,
    /// The SASL username, when there is one. `null` under `plaintext` —
    /// which is not the same as an empty username — and under `mtls`, whose
    /// identity is the client certificate.
    #[serde(default)]
    pub username: Option<String>,
}

/// The pinned engine that took the backup. `digest` is why this block is
/// worth signing: GC7 pins by digest and never by tag, and a receipt that
/// named only a version would be satisfied by any binary claiming it.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct ReceiptEngine {
    /// `"oso-cli"`.
    pub id: String,
    /// `"v0.21.0"` — at or above the GC8 floor.
    pub version: String,
    /// `"sha256:…"`, from `third_party/kafka-backup-binary.digest`.
    pub digest: String,
}

/// What was written, and where. The two fields an auditor needs in order to
/// go and look: the manifest's key, and a digest over the exact manifest
/// bytes THIS RUN READ BACK (not over bytes Logweir remembers writing) — and,
/// on a versioned bucket, WHICH VERSION of that key those bytes were.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct ReceiptArchive {
    /// Empty **if and only if** the backup did not exit 0 — invariant 2.
    pub manifest_key: String,
    /// `"sha256:<hex>"` over the manifest bytes read back after the run.
    pub manifest_sha256: String,
    /// **FX-7, format `1.2.0`.** The object store's VERSION id for the
    /// manifest bytes this run read back — present only when the store
    /// answered that read with one, i.e. on a bucket with versioning enabled.
    ///
    /// **ABSENT means "no version was pinned"**: an unversioned bucket (whose
    /// objects carry no version id, or S3's literal `null`, which names an
    /// object an overwrite replaces in place), or a receipt written before
    /// this field existed. Never read as "version zero" and never inferred.
    ///
    /// A reader that finds it compares the key's CURRENT version with it: a
    /// different current version means the backup set was written again after
    /// this receipt was signed, which the manifest digest alone cannot see —
    /// engine 0.21.0 rewrites a set's segments in place and can leave the
    /// manifest bytes identical. And it can read THIS version by id, which a
    /// versioned bucket retains whatever the current one is.
    ///
    /// Absent when `None`, and that is the compatibility argument: declaration
    /// order is byte order, and an absent field writes nothing, so a receipt
    /// without a pin is byte-for-byte the [`RECEIPT_FORMAT_VERSION`] document
    /// it would be without FX-7.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub manifest_version_id: Option<String>,
    /// The object-store prefix everything this run wrote lives under. GC6:
    /// Logweir writes only under its own `logweir/` prefix.
    pub prefix: String,
}

/// **FX-7.** The `format_version` a receipt carries when it pins
/// `archive.manifest_version_id`: a MINOR bump over [`RECEIPT_FORMAT_VERSION`]
/// (FX-4's 1.1.0, which merged first), because the field is optional and a
/// 1.1.0 or 1.0.0 reader, which ignores unknown fields inside major 1, still
/// reads it (reading rule 1). Written only when the pin is present, so every
/// receipt on an unversioned bucket is exactly the [`RECEIPT_FORMAT_VERSION`]
/// document.
pub const FORMAT_VERSION_WITH_MANIFEST_VERSION: &str = "1.2.0";

/// **PROD-01.3.** The `format_version` of a receipt whose `source.auth.mode`
/// is one of the modes PROD-01.3 added (`scramSha256`, `plain`, `mtls`) — a
/// MINOR bump over [`FORMAT_VERSION_WITH_TOPIC_CONFIGURATION`] (PROD-05.1's
/// 1.3.0, which merged first), because the change is new content in an
/// existing field that an older reader can only refuse (arm 5a), never accept
/// as something stronger (OD-7, third case). Written only for those modes, so
/// a receipt for a `plaintext` or `scramSha512` backup is byte-for-byte the
/// document it was before. It includes every earlier minor: a 1.4.0 receipt
/// carries `topic_configuration` and may carry `archive.manifest_version_id`.
pub const FORMAT_VERSION_WITH_AUTH_MODES: &str = "1.4.0";

/// The first minor of format 1 whose `source.auth.mode` may be one of
/// `crate::connection::PROD_01_3_AUTH_MODES` (arm 5b). **A renumber changes
/// this and [`FORMAT_VERSION_WITH_AUTH_MODES`] together**, and
/// `docs/verify_scorecard.py`'s `RECEIPT_AUTH_MODES_SINCE_MINOR` follows it.
pub const AUTH_MODES_SINCE_MINOR: u64 = 4;

/// **PROD-01.4a.** The `format_version` of a receipt that carries
/// `generations` — a MINOR bump over [`FORMAT_VERSION_WITH_SCHEMA_DEPENDENCY`]
/// (PROD-03.0's 1.5.0) under OD-7 (a): arms 36-40 read only the new optional
/// block, so every document without it is decided exactly as before, and an
/// older reader ignores the field inside major 1. Every receipt this build
/// signs carries the block, so every one is at least 1.6.0; it includes every
/// earlier minor.
pub const FORMAT_VERSION_WITH_GENERATIONS: &str = "1.6.0";

/// The first minor of format 1 that defines `generations` (arm 36). A
/// renumber moves this, [`FORMAT_VERSION_WITH_GENERATIONS`] and
/// `docs/verify_scorecard.py`'s `RECEIPT_GENERATIONS_SINCE_MINOR` together;
/// `tests/backup_receipt.rs::the_written_version_defines_generations` keeps
/// them coherent.
pub const GENERATIONS_SINCE_MINOR: u64 = 6;

/// The version id a reader may PIN, out of what a store answered.
///
/// `None` for no answer, for a blank one, and for S3's literal `"null"` — the
/// id of an object written while versioning was never enabled or suspended,
/// which the next write REPLACES in place, so it identifies no retained bytes.
#[must_use]
pub fn pinnable_version_id(answered: Option<&str>) -> Option<String> {
    match answered.map(str::trim) {
        None | Some("") | Some("null") => None,
        Some(id) => Some(id.to_string()),
    }
}

/// The `format_version` a receipt is written with — the NEWEST minor whose
/// fields it uses, so a receipt carrying both features names the version that
/// defines both: [`FORMAT_VERSION_WITH_SCHEMA_DEPENDENCY`] when it carries
/// `schema_dependency` (PROD-03.0 — every receipt this build signs; 1.5.0
/// defines every earlier field and value), else
/// [`FORMAT_VERSION_WITH_AUTH_MODES`] when the auth mode is one
/// PROD-01.3 added (it also defines PROD-05.1's `topic_configuration` and
/// FX-7's pin), else [`FORMAT_VERSION_WITH_TOPIC_CONFIGURATION`] when it
/// carries `topic_configuration` (PROD-05.1),
/// else
/// [`FORMAT_VERSION_WITH_MANIFEST_VERSION`] when it pins the manifest's
/// version, else [`RECEIPT_FORMAT_VERSION`] — FX-4's 1.1.0. The ONE place a
/// writer decides it.
///
/// [`FORMAT_VERSION_WITH_CONSUMER_POSITIONS`] (PROD-04.1) comes first: a
/// receipt carrying `consumer_positions` is 1.7.0, which defines every earlier
/// minor's fields. [`FORMAT_VERSION_WITH_GENERATIONS`] (PROD-01.4a) comes
/// next: a receipt carrying `generations` is 1.6.0, which defines every
/// earlier minor's fields. Every receipt this build signs carries
/// `generations`, so every one is at least 1.6.0.
#[must_use]
pub fn format_version_for(
    archive: &ReceiptArchive,
    topic_configuration: bool,
    schema_dependency: bool,
    auth: &ReceiptAuth,
    generations: bool,
    consumer_positions: bool,
) -> &'static str {
    if consumer_positions {
        FORMAT_VERSION_WITH_CONSUMER_POSITIONS
    } else if generations {
        FORMAT_VERSION_WITH_GENERATIONS
    } else if schema_dependency {
        FORMAT_VERSION_WITH_SCHEMA_DEPENDENCY
    } else if crate::connection::is_prod_01_3_auth_mode(&auth.mode) {
        FORMAT_VERSION_WITH_AUTH_MODES
    } else if topic_configuration {
        FORMAT_VERSION_WITH_TOPIC_CONFIGURATION
    } else if archive.manifest_version_id.is_some() {
        FORMAT_VERSION_WITH_MANIFEST_VERSION
    } else {
        RECEIPT_FORMAT_VERSION
    }
}

/// The time range the archive covers, in **EPOCH MILLISECONDS**, as a
/// HALF-OPEN interval: `from_ms` is inclusive, `to_ms` is EXCLUSIVE.
///
/// # The end is exclusive, and both documents now say so (I22)
///
/// `config/crd/backups.yaml` documents `Backup.status.windowCovered.toMs` as
/// the exclusive end, and Task 17 copies these two numbers into it. Task 5
/// shipped invariant 4 as `<=` — accepting `from_ms == to_ms` as an
/// "instantaneous window" — which made the receipt and the CRD two
/// descriptions of one range under rules that disagreed. Invariant 4 is now
/// strict, and `crates/logweir/src/backup/phase_run.rs` converts the
/// manifest's inclusive newest `end_timestamp` to an exclusive bound in the
/// one place that measures it.
///
/// # Not RFC 3339, and this is the interface, not a preference (I22)
///
/// `Backup.status.windowCovered{fromMs,toMs}` mirrors this shape as two
/// `int64`s, and a Kubernetes status subresource has no date-time type to
/// mirror a string into. Two representations of one window — a string here
/// and an integer there — would need a conversion nobody owns, and the first
/// disagreement between them would be invisible: both would still be
/// well-formed. So the receipt speaks the operator's units, and the operator
/// copies the numbers.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct ReceiptCovered {
    pub from_ms: i64,
    pub to_ms: i64,
}

/// Three dot-separated non-negative integers, or `None`.
///
/// Hand-parsed on purpose: Global Constraint 38 closes the workspace graph,
/// so no `semver` crate is added for eleven lines. Stricter than
/// `crate::scorecard`'s `major_version`, which reads the leading component
/// alone — `"1"` and `"1.2.3.4"` are semver-shaped enough for that reader and
/// are refused here, because invariant 1 claims the whole string parses.
fn parse_semver(v: &str) -> Option<(u64, u64, u64)> {
    let mut parts = v.split('.');
    let major = parts.next()?.parse().ok()?;
    let minor = parts.next()?.parse().ok()?;
    let patch = parts.next()?.parse().ok()?;
    if parts.next().is_some() {
        return None;
    }
    Some((major, minor, patch))
}

impl BackupReceipt {
    /// The five arms of a signed receipt: the four claims it cannot
    /// contradict, and the one field whose value set is closed.
    ///
    /// Called before signing and by `logweir drill verify --payload-type
    /// backup-receipt`, so no signed receipt can carry a self-contradicting
    /// claim. `Err` is the exact message, and the messages are **not to be
    /// reworded**: `docs/verify_scorecard.py`'s mirrored block compares
    /// byte-for-byte against them, and `crates/logweir-core/tests/
    /// backup_receipt.rs` asserts each one in full —
    /// `backup_receipt_refuses_each_self_contradiction_arm_with_its_exact_message` over the four
    /// self-contradiction arms, `arm_5_refuses_an_auth_mode_outside_the_closed_two`
    /// over arm 5, and
    /// `validate_invariants_has_exactly_five_return_err_statements` over the
    /// total, so neither the four nor the five can go stale on its own.
    ///
    /// `Result<(), String>` rather than a `thiserror` newtype, because the
    /// interface this task publishes is the STRING: the comparison the two
    /// readers make is on the text, and a wrapper type would put a prefix in
    /// front of it that the Python half has no way to reproduce.
    ///
    /// # The arms, in order
    ///
    /// 1. `format_version` parses as semver and its major is `1`. Checked
    ///    FIRST, like `Scorecard::refuse_unreadable_major`, so a document
    ///    from a future major is refused before any other arm is evaluated
    ///    against fields that build may have redefined.
    /// 2. `exit_code == 0` **iff** `archive.manifest_key` is non-empty.
    /// 3. `records` covers exactly `source.topics`.
    /// 4. `covered.from_ms < covered.to_ms` STRICTLY — the end is EXCLUSIVE
    ///    (I22, and Task 5's review finding F3).
    /// 5. `source.auth.mode` is one of the values this format defines.
    ///    The first arm that is not a self-contradiction check: the document
    ///    does not disagree with itself, it names a mechanism the format has
    ///    no spelling for. LAST of the 1.0.0 arms on purpose — the four
    ///    consistency arms are what an auditor reads first, and a receipt that
    ///    contradicts itself should say so before it is told its auth mode is
    ///    unknown. Since 1.4.0 (PROD-01.3) it is three statements: 5a, the
    ///    closed two below 1.4.0 (unchanged); 5b, a PROD-01.3 mode under a
    ///    version that predates it; 5c, the closed five from 1.4.0.
    ///
    /// Arms 6-11 (format 1.1.0, FX-4) read `config_coverage` and NOTHING ELSE,
    /// and run only when it is present — so every document without it, which
    /// is every receipt written before 1.1.0, is accepted or refused exactly
    /// as before. Within the block, topics are visited in name order (the
    /// map's own order) and arms 8-11 run per topic, in order.
    ///
    /// 6. `config_coverage` is present only under a `format_version` whose
    ///    minor is at least 1 — a 1.0.x document cannot carry a 1.1 field.
    /// 7. `config_coverage` covers exactly `source.topics`, as arm 3 does for
    ///    `records`.
    /// 8. every `coverage` is one of [`COVERAGE_VALUES`].
    /// 9. `reason` is present exactly when `coverage` is `notCaptured`, and
    ///    is one of [`NOT_CAPTURED_REASONS`].
    /// 10. a `timestamp_type` is recorded only where the read succeeded: a
    ///     `captureDenied` topic, or a `notCaptured` one whose reason is
    ///     `describeFailed`, cannot have observed one.
    /// 11. a `timestamp_type`'s `value` is one of [`TIMESTAMP_TYPES`] and its
    ///     `source` one of [`CONFIG_SOURCES`].
    ///
    /// Arms 12-19 (format 1.3.0, PROD-05.1) read `topic_configuration` — with
    /// `config_coverage` and `source.topics` as the context it is judged in,
    /// the way arm 7 reads `source.topics` — and run only when it is present,
    /// so every document without it is decided exactly as before:
    ///
    /// 12. `topic_configuration` is present only under a minor of at least 3.
    /// 13. it is present only beside `config_coverage`.
    /// 14. it covers exactly `source.topics`.
    /// 15. a topic's `entries` are present exactly when its configuration read
    ///     succeeded (`captured`, or `notCaptured`/`manifestDiffers`).
    /// 16. every entry's `source` is one of [`CONFIG_SOURCES`] and its
    ///     `portability` one of `PORTABILITY_CLASSES`.
    /// 17. an entry is `secret` exactly when it carries no value, and
    ///     otherwise `inherited` exactly when its source is not the topic's own
    ///     override.
    /// 18. an `owner`'s kind and basis are from the closed sets (a
    ///     `kafkaTopicResource` owner is `strimzi`) and its reference is 1 to
    ///     256 characters with no control character.
    /// 19. a recorded partition count or replication factor is at least 1.
    /// 20. `owner_detection` is present only beside `topic_configuration`, and
    ///     lists `declared` and `kafkaTopicResources` each at most once.
    /// 21. every recorded owner's basis names a source `owner_detection` lists
    ///     (absent reads as empty).
    ///
    /// Arms 22-29 (format 1.5.0, PROD-03.0) read `schema_dependency` — with
    /// `source.topics` and `records` as the context it is judged in — and run
    /// only when it is present, so every document without it is decided
    /// exactly as before:
    ///
    /// 22. `schema_dependency` is present only under a minor of at least 5.
    /// 23. it covers exactly `source.topics`.
    /// 24. a topic's `verdict` is one of `crate::schema_dependency::VERDICTS`;
    ///     a `notAssessed` one has a `reason` from
    ///     `NOT_ASSESSED_REASONS` and no `basis`, any other a `basis` from
    ///     `BASES` and no `reason`.
    /// 25. `key` and `value` are both present exactly when the topic was
    ///     judged, and count the same records, at least one.
    /// 26. a `complete` basis judged exactly the records `records` counts for
    ///     the topic, a `sampled` one at most that many, and `noRecords` is
    ///     said only of a topic `records` counts none of.
    /// 27. a side's `schema_ids` are distinct, ascending and plausible
    ///     (`1..=MAX_SCHEMA_ID`), `min(schema_id_count, SCHEMA_IDS_LISTED)` of
    ///     them, and `schema_id_count` is at least 1 exactly when `framed` is,
    ///     and at most `framed`.
    /// 28. a side's `dependent` is
    ///     `crate::schema_dependency::dependent_by_share(framed, unframed)`.
    /// 29. a judged topic is `schemaDependent` exactly when a side is
    ///     dependent.
    ///
    /// Arms 30-35 (format 1.7.0, PROD-04.1) read `consumer_positions` and run
    /// only when it is present ([`crate::consumer_positions`]); the positions
    /// document it binds is checked by
    /// [`BackupReceipt::validate_consumer_positions_document`] (CP-1 to
    /// CP-14) when a reader is given it:
    ///
    /// 30. `consumer_positions` is present only under a minor of at least 7.
    /// 31. the capture ends at or after it starts, `listing` is from its
    ///     closed set, and at least one group is recorded.
    /// 32. `document` names this run's positions document, by a well-formed
    ///     digest over at least one byte.
    /// 33. each group's outcome is from the closed set, with a reason exactly
    ///     when it is not `captured`, from that outcome's set.
    /// 34. a captured group records its type, both states, its members,
    ///     `active` and counts over at least one partition, and is never
    ///     `Dead` with no member; a `GroupTypeNotCaptured` group only
    ///     `group_type: other`; any other group none of them.
    /// 35. `active` is what the two states say.
    ///
    /// Arms 36-40 (format 1.6.0, PROD-01.4a) read `generations` — with
    /// `source.topics` as its context — and run only when it is present, so
    /// every document without it is decided exactly as before. Topics in name
    /// order; per topic, arms 38, 39 and 40, each over `topic_id` then
    /// `topic_id_after`:
    ///
    /// 36. `generations` is present only under a minor of at least 6.
    /// 37. it covers exactly `source.topics`.
    /// 38. every recorded ID is canonical (`crate::topic_identity::is_canonical`):
    ///     22 URL-safe base64 characters over 16 bytes, never one of Kafka's
    ///     reserved IDs (zero, `AAAAAAAAAAAAAAAAAAAAAQ`).
    /// 39. a reason is present exactly when its ID is `null`, from the closed
    ///     set.
    /// 40. `topic_id_source` is present exactly when an ID is recorded, from
    ///     the closed set.
    ///
    /// Two different recorded IDs are NOT refused: they are a fact the run
    /// observed (the topic was recreated while the engine ran), which a reader
    /// reports (`crate::topic_identity::within_capture`).
    pub fn validate_invariants(&self) -> Result<(), String> {
        // ARM 1. GC12 for this document: a reader refuses a major it has
        // never seen rather than guessing at a shape.
        if parse_semver(&self.format_version).map(|(major, _, _)| major) != Some(1) {
            return Err(format!(
                "format_version {:?} is not a 1.x version this reader understands",
                self.format_version
            ));
        }
        // ARM 2. A biconditional, both directions, one message. A receipt for
        // a failed backup names no manifest, and a receipt naming a manifest
        // did not fail.
        //
        // TRIMMED-EMPTY COUNTS AS ABSENT (ruling R-A). `.is_empty()` alone
        // accepted `"   "` as "a manifest was named", which is the exact
        // class of defect R-A was raised over on the scorecard's
        // `partial_reason`: a whitespace-only string that one reader treats
        // as present and the other as blank. Naming no manifest and naming a
        // manifest made of spaces are the same claim, and this is the strict
        // side of it — the accepted set narrows and no receipt Logweir writes
        // is affected, because `BackupOutcome::manifest_key` is either the
        // engine's key or `""`.
        let named = !self.archive.manifest_key.trim().is_empty();
        if (self.exit_code == 0) != named {
            let rendered = if named {
                format!("{:?}", self.archive.manifest_key)
            } else {
                "absent".to_string()
            };
            return Err(format!(
                "exit_code {} and manifest_key {} disagree: a receipt names a manifest \
                 if and only if the backup exited 0",
                self.exit_code, rendered
            ));
        }
        // ARM 3. The counted set and the named set are the same set. A
        // receipt that counts a topic the run was never asked to back up, or
        // omits one it was, is describing some other run — and either way the
        // per-topic figures cannot be read against the topic list beside
        // them.
        //
        // Both sides are rendered as SORTED, DEDUPLICATED lists so the
        // message is deterministic: `records` is a BTreeMap (already sorted)
        // and `source.topics` is a Vec whose order is the spec's.
        let counted: std::collections::BTreeSet<&str> =
            self.records.keys().map(String::as_str).collect();
        let named_topics: std::collections::BTreeSet<&str> =
            self.source.topics.iter().map(String::as_str).collect();
        if counted != named_topics {
            return Err(format!(
                "records covers {} but the named topic set is {}",
                render_set(&counted),
                render_set(&named_topics)
            ));
        }
        // ARM 4. THE END IS EXCLUSIVE, so the window is HALF-OPEN and
        // `from_ms == to_ms` is not an "instantaneous window" — it is an empty
        // one, and an archive that captured a record cannot cover an empty
        // range.
        //
        // STRICT `<` SINCE TASK 5b (Task 5's review, F3). Task 5 shipped `<=`
        // and a test asserting `from_ms == to_ms` was legal, while
        // `config/crd/backups.yaml` already documented
        // `status.windowCovered.toMs` as the *exclusive* end of the same
        // window (I22) — two documents describing one range by rules that
        // disagree, with Task 17 about to copy the numbers from one into the
        // other. One of the two had to move; the CRD's is the shape an
        // operator reads and the half-open convention every range API in this
        // tree uses, so the receipt's is the one that moved.
        //
        // The measurement side moved with it: `crates/logweir/src/backup/
        // phase_run.rs` derives `to_ms` from the newest segment's INCLUSIVE
        // `end_timestamp` and adds one millisecond, so a single-record topic
        // yields `[t, t+1)` — a window that contains exactly that record —
        // rather than the empty `[t, t]` this arm now refuses. That
        // conversion is stated once, there, and nothing else in the tree
        // converts between the two conventions.
        if self.covered.from_ms >= self.covered.to_ms {
            return Err(format!(
                "covered.from_ms {} is not before covered.to_ms {}: the covered window's end is EXCLUSIVE, so an empty range covers no record",
                self.covered.from_ms, self.covered.to_ms
            ));
        }
        // ARM 5. THE AUTH MODE'S VALUE SET IS CLOSED, and this is where it is
        // closed (controller ruling, Task 5b fix round 1; Task 6's review
        // Ruling 3).
        //
        // Before this arm the field was an unconstrained `String` that no
        // reader looked at: a receipt carrying
        // `"auth": {"mode": "totally-made-up"}` verified 0/0 at BOTH readers,
        // which was proved by execution in Task 5b's review. That is worse
        // than a documentation defect, because `Backup.status.auth.mode` is
        // copied FROM here by Task 17 and its CRD description promises the
        // two values below — so an unrefused third spelling propagates into
        // the control plane as an attested claim.
        //
        // The two values are `logweir_core::spec::AuthSpec`'s serde tags, the
        // only strings `AuthSpec::mode_str()` returns, and the
        // `KafkaCluster` CRD's `auth.mode` enum byte for byte. There is
        // therefore no legitimate writer of a third value anywhere in this
        // tree, and `crates/logweir/src/backup/phase_run.rs::receipt_auth` —
        // the ONE site that fills this field on a real run — cannot drift
        // back to `scram-sha-512` without `logweir backup run` refusing its
        // own receipt at `persist_receipt`'s step 1, before it signs or puts
        // anything.
        //
        // The mode IS interpolated, unlike the scorecard's two `target.auth`
        // arms: every arm of THIS document already echoes an adopter-supplied
        // string (`format_version` in arm 1, `manifest_key` in arm 2), a
        // reader that refuses a value without naming it makes the refusal
        // unactionable, and `{:?}` is the same rendering
        // `docs/verify_scorecard.py::_rust_debug_str` reproduces.
        //
        // PROD-01.3 (format 1.4.0) SPLITS THE ARM BY VERSION, and leaves every
        // document below 1.4.0 judged exactly as before. The three new modes
        // (`crate::connection::PROD_01_3_AUTH_MODES`) are values of 1.4.0 and
        // later only: a document declaring an older minor that names one is
        // refused (arm 5b) — no writer of that version could have produced
        // it — and a 1.4.0 document is held to the closed set of five (arm
        // 5c). An older reader refuses a 1.4.0 receipt that names a new mode
        // through arm 5a, which is the SAFER verdict (OD-7, third case): so
        // the change is MINOR, and a receipt for a `plaintext` or
        // `scramSha512` backup is still written as the 1.1.0/1.2.0/1.3.0 document
        // it always was (`format_version_for`).
        let mode = self.source.auth.mode.as_str();
        let five_defined = parse_semver(&self.format_version)
            .is_some_and(|(_, minor, _)| minor >= AUTH_MODES_SINCE_MINOR);
        if crate::connection::is_prod_01_3_auth_mode(mode) {
            if !five_defined {
                // ARM 5b.
                return Err(format!(
                    "source.auth.mode {:?} is defined from 1.{AUTH_MODES_SINCE_MINOR}.0 and \
                     format_version {:?} predates it",
                    self.source.auth.mode, self.format_version
                ));
            }
        } else if !crate::connection::ORIGINAL_AUTH_MODES.contains(&mode) {
            if five_defined {
                // ARM 5c.
                return Err(format!(
                    "source.auth.mode {:?} is not one of the five values this format defines: \
                     \"plaintext\", \"scramSha512\", \"scramSha256\", \"plain\" or \"mtls\"",
                    self.source.auth.mode
                ));
            }
            // ARM 5a, unchanged since 1.0.0.
            return Err(format!(
                "source.auth.mode {:?} is not one of the two values this format defines: \
                 \"plaintext\" or \"scramSha512\"",
                self.source.auth.mode
            ));
        }
        // ARMS 6-11 (format 1.1.0, FX-4): the `config_coverage` block, and
        // only when it is present. Nothing below reads a field a 1.0.0
        // document has, so every earlier receipt is decided exactly as before.
        if let Some(coverage) = &self.config_coverage {
            // ARM 6. A document that declares 1.0.x cannot carry a 1.1 field:
            // either the version or the block is not what the writer produced.
            // Arm 1 has already established that the version parses and that
            // its major is 1, so the minor is read without a fallback path
            // that could matter.
            let minor = parse_semver(&self.format_version).map_or(0, |(_, minor, _)| minor);
            if minor < CONFIG_COVERAGE_SINCE_MINOR {
                return Err(format!(
                    "config_coverage is present but format_version {:?} predates it: the field \
                     is defined from 1.{CONFIG_COVERAGE_SINCE_MINOR}.0",
                    self.format_version
                ));
            }
            // ARM 7. The covered set and the named set are the same set — the
            // twin of arm 3, rendered the same way.
            let covered: std::collections::BTreeSet<&str> =
                coverage.keys().map(String::as_str).collect();
            if covered != named_topics {
                return Err(format!(
                    "config_coverage covers {} but the named topic set is {}",
                    render_set(&covered),
                    render_set(&named_topics)
                ));
            }
            for (topic, entry) in coverage {
                // ARM 8. The coverage vocabulary is closed.
                if !COVERAGE_VALUES.contains(&entry.coverage.as_str()) {
                    return Err(format!(
                        "config_coverage[{topic:?}].coverage {:?} is not one of the three values \
                         this format defines: \"captured\", \"notCaptured\" or \"captureDenied\"",
                        entry.coverage
                    ));
                }
                // ARM 9. A reason exactly when the coverage is `notCaptured`,
                // from a closed set. An absent reason is spelled `absent`, the
                // way arm 2 spells an absent manifest key.
                let reason_fits = match entry.reason.as_deref() {
                    Some(reason) => {
                        entry.coverage == "notCaptured" && NOT_CAPTURED_REASONS.contains(&reason)
                    }
                    None => entry.coverage != "notCaptured",
                };
                if !reason_fits {
                    let rendered = match &entry.reason {
                        Some(reason) => format!("{reason:?}"),
                        None => "absent".to_string(),
                    };
                    return Err(format!(
                        "config_coverage[{topic:?}].reason {rendered} does not fit coverage {:?}: \
                         a reason is present exactly when coverage is \"notCaptured\", and is \
                         \"describeFailed\" or \"manifestDiffers\"",
                        entry.coverage
                    ));
                }
                // ARM 10. A timestamp type is an OBSERVATION, so it exists only
                // where the read succeeded.
                let read_failed = entry.coverage == "captureDenied"
                    || entry.reason.as_deref() == Some("describeFailed");
                if read_failed && entry.timestamp_type.is_some() {
                    return Err(format!(
                        "config_coverage[{topic:?}] records a timestamp_type, but a topic whose \
                         configuration read was denied or failed cannot have observed one"
                    ));
                }
                // ARM 11. The observed value and its source, from closed sets.
                if let Some(ts) = &entry.timestamp_type {
                    if !TIMESTAMP_TYPES.contains(&ts.value.as_str())
                        || !CONFIG_SOURCES.contains(&ts.source.as_str())
                    {
                        return Err(format!(
                            "config_coverage[{topic:?}].timestamp_type {:?} from {:?} is not a \
                             value and source this format defines: the value is \"CreateTime\" \
                             or \"LogAppendTime\", and the source is \"dynamicTopicConfig\", \
                             \"dynamicBrokerConfig\", \"dynamicDefaultBrokerConfig\", \
                             \"staticBrokerConfig\", \"defaultConfig\" or \"unknown\"",
                            ts.value, ts.source
                        ));
                    }
                }
            }
        }
        // ARMS 12-19 (format 1.3.0, PROD-05.1): the `topic_configuration`
        // block, and only when it is present. Every earlier receipt is decided
        // exactly as before. Within the block, topics in name order (the map's
        // own), and per topic arm 15, then arms 16 and 17 per entry in key
        // order, then 18 and 19.
        if let Some(model) = &self.topic_configuration {
            // ARM 12. A document that declares a minor before 3 cannot carry
            // a 1.3 field.
            let minor = parse_semver(&self.format_version).map_or(0, |(_, minor, _)| minor);
            if minor < TOPIC_CONFIGURATION_SINCE_MINOR {
                return Err(format!(
                    "topic_configuration is present but format_version {:?} predates it: the \
                     field is defined from 1.{TOPIC_CONFIGURATION_SINCE_MINOR}.0",
                    self.format_version
                ));
            }
            // ARM 13. The entries are judged against the read that produced
            // them, which `config_coverage` records.
            let Some(coverage) = &self.config_coverage else {
                return Err(format!(
                    "topic_configuration is present under format_version {:?} but \
                     config_coverage is not: a topic's configuration entries cannot be judged \
                     without the read that produced them",
                    self.format_version
                ));
            };
            // ARM 14. The modelled set and the named set are the same set —
            // the twin of arms 3 and 7.
            let modelled: std::collections::BTreeSet<&str> =
                model.keys().map(String::as_str).collect();
            if modelled != named_topics {
                return Err(format!(
                    "topic_configuration covers {} but the named topic set is {}",
                    render_set(&modelled),
                    render_set(&named_topics)
                ));
            }
            for (topic, entry) in model {
                // ARM 15. Entries exactly where the read succeeded. Arms 7 and
                // 14 make the coverage entry exist; `None` is still rendered,
                // as `absent`, rather than assumed.
                let read = coverage.get(topic);
                let succeeded = read.is_some_and(|c| {
                    c.coverage == "captured"
                        || (c.coverage == "notCaptured"
                            && c.reason.as_deref() == Some("manifestDiffers"))
                });
                if entry.entries.is_some() != succeeded {
                    let rendered = match read {
                        Some(c) => {
                            let said = match &c.reason {
                                Some(reason) => format!("{}/{reason}", c.coverage),
                                None => c.coverage.clone(),
                            };
                            format!("{said:?}")
                        }
                        None => "absent".to_string(),
                    };
                    return Err(format!(
                        "topic_configuration[{topic:?}].entries {} does not fit its \
                         config_coverage {rendered}: entries are recorded exactly when the \
                         configuration read succeeded (\"captured\", or \"notCaptured\" with \
                         reason \"manifestDiffers\")",
                        if entry.entries.is_some() {
                            "present"
                        } else {
                            "absent"
                        }
                    ));
                }
                for (key, config) in entry.entries.iter().flatten() {
                    // ARM 16. The source and the class, from closed sets.
                    if !CONFIG_SOURCES.contains(&config.source.as_str())
                        || !crate::topic_configuration::PORTABILITY_CLASSES
                            .contains(&config.portability.as_str())
                    {
                        return Err(format!(
                            "topic_configuration[{topic:?}].entries[{key:?}] source {:?} and \
                             portability {:?} are not a source and class this format defines: \
                             the source is \"dynamicTopicConfig\", \"dynamicBrokerConfig\", \
                             \"dynamicDefaultBrokerConfig\", \"staticBrokerConfig\", \
                             \"defaultConfig\" or \"unknown\", and the class is \"portable\", \
                             \"inherited\", \"removedInKafka4\", \"clusterBound\", \
                             \"requiresTieredStorage\", \"providerOnly\" or \"secret\"",
                            config.source, config.portability
                        ));
                    }
                    // ARM 17. A secret carries no value and nothing else lacks
                    // one; otherwise `inherited` is exactly a value the topic
                    // did not set itself.
                    let secret = config.portability == crate::topic_configuration::SECRET;
                    let fits = if secret || config.value.is_none() {
                        secret && config.value.is_none()
                    } else {
                        (config.portability == crate::topic_configuration::INHERITED)
                            == (config.source != crate::topic_configuration::TOPIC_OVERRIDE_SOURCE)
                    };
                    if !fits {
                        return Err(format!(
                            "topic_configuration[{topic:?}].entries[{key:?}] is {:?} from {:?} \
                             with {}: an entry is \"secret\" exactly when it carries no value, \
                             and otherwise \"inherited\" exactly when its source is not \
                             \"dynamicTopicConfig\"",
                            config.portability,
                            config.source,
                            if config.value.is_some() {
                                "a value"
                            } else {
                                "no value"
                            }
                        ));
                    }
                }
                // ARM 18. The owner, from closed sets, and a usable reference.
                if let Some(owner) = &entry.owner {
                    let kind_ok =
                        crate::topic_configuration::OWNER_KINDS.contains(&owner.kind.as_str());
                    let basis_ok = owner.basis == "declared"
                        || (owner.basis == "kafkaTopicResource" && owner.kind == "strimzi");
                    if !kind_ok
                        || !basis_ok
                        || !crate::topic_configuration::reference_fits(&owner.reference)
                    {
                        return Err(format!(
                            "topic_configuration[{topic:?}].owner {:?} by {:?} is not an owner \
                             this format defines: the kind is \"strimzi\" or \"external\", the \
                             basis is \"kafkaTopicResource\" (for \"strimzi\" only) or \
                             \"declared\", and the reference is 1 to 256 characters with no \
                             control character",
                            owner.kind, owner.basis
                        ));
                    }
                }
                // ARM 19. A recorded count is a count.
                if entry.partitions == Some(0) || entry.replication_factor == Some(0) {
                    let shown = |n: Option<u32>| n.map_or("absent".to_string(), |n| n.to_string());
                    return Err(format!(
                        "topic_configuration[{topic:?}] records partitions {} and \
                         replication_factor {}: a recorded count is at least 1",
                        shown(entry.partitions),
                        shown(entry.replication_factor)
                    ));
                }
            }
        }
        // ARM 20 (format 1.3.0, PROD-05.1). Where the run looked for owners:
        // only beside the model it qualifies, from the closed set, each source
        // at most once.
        if let Some(detection) = &self.owner_detection {
            let mut seen = std::collections::BTreeSet::new();
            let fits = self.topic_configuration.is_some()
                && detection.iter().all(|d| {
                    crate::topic_configuration::OWNER_DETECTION_SOURCES.contains(&d.as_str())
                        && seen.insert(d.as_str())
                });
            if !fits {
                return Err(format!(
                    "owner_detection {detection:?} is not a detection this format defines: it is \
                     present only beside topic_configuration, and lists \"declared\" and \
                     \"kafkaTopicResources\" each at most once"
                ));
            }
        }
        // ARM 21. An owner is recorded only from a source the run looked in: a
        // `declared` owner needs `declared`, a `kafkaTopicResource` owner needs
        // `kafkaTopicResources`. An absent detection is an empty one.
        if let Some(model) = &self.topic_configuration {
            let detection: &[String] = self.owner_detection.as_deref().unwrap_or(&[]);
            for (topic, entry) in model {
                let Some(owner) = &entry.owner else {
                    continue;
                };
                let source = crate::topic_configuration::detection_for_basis(&owner.basis);
                if !source.is_some_and(|s| detection.iter().any(|d| d == s)) {
                    return Err(format!(
                        "topic_configuration[{topic:?}].owner by {:?} names no source \
                         owner_detection {detection:?} lists: a \"declared\" owner needs \
                         \"declared\", a \"kafkaTopicResource\" owner \"kafkaTopicResources\"",
                        owner.basis
                    ));
                }
            }
        }
        // ARMS 22-29 (format 1.5.0, PROD-03.0): the `schema_dependency` block,
        // and only when it is present — every earlier receipt is decided
        // exactly as before. Topics in name order (the map's own), and per
        // topic arms 24, 25 and 26, then 27 and 28 for the key side and then
        // the value side, then 29.
        if let Some(dependency) = &self.schema_dependency {
            // ARM 22. A document that declares a minor before 5 cannot carry
            // a 1.5 field.
            let minor = parse_semver(&self.format_version).map_or(0, |(_, minor, _)| minor);
            if minor < SCHEMA_DEPENDENCY_SINCE_MINOR {
                return Err(format!(
                    "schema_dependency is present but format_version {:?} predates it: the \
                     field is defined from 1.{SCHEMA_DEPENDENCY_SINCE_MINOR}.0",
                    self.format_version
                ));
            }
            // ARM 23. The judged set and the named set are the same set — the
            // twin of arms 3, 7 and 14.
            let judged: std::collections::BTreeSet<&str> =
                dependency.keys().map(String::as_str).collect();
            if judged != named_topics {
                return Err(format!(
                    "schema_dependency covers {} but the named topic set is {}",
                    render_set(&judged),
                    render_set(&named_topics)
                ));
            }
            for (topic, entry) in dependency {
                use crate::schema_dependency as sd;
                let shown =
                    |s: Option<&String>| s.map_or("absent".to_string(), |s| format!("{s:?}"));
                // ARM 24. The verdict, and a reason or a basis as it requires,
                // from closed sets.
                let assessed = entry.verdict != sd::NOT_ASSESSED;
                let fits = sd::VERDICTS.contains(&entry.verdict.as_str())
                    && if assessed {
                        entry.reason.is_none()
                            && entry
                                .basis
                                .as_deref()
                                .is_some_and(|b| sd::BASES.contains(&b))
                    } else {
                        entry.basis.is_none()
                            && entry
                                .reason
                                .as_deref()
                                .is_some_and(|r| sd::NOT_ASSESSED_REASONS.contains(&r))
                    };
                if !fits {
                    return Err(format!(
                        "schema_dependency[{topic:?}] verdict {:?} with reason {} and basis {} \
                         is not a verdict this format defines: the verdict is \
                         \"schemaDependent\", \"notDetected\" or \"notAssessed\"; a \
                         \"notAssessed\" topic has a reason, \"noRecords\", \
                         \"segmentUnreadable\", \"segmentTooLargeForDetection\" or \
                         \"detectionTimeBudgetExceeded\", and no basis, and any other topic has \
                         a basis, \"sampled\" or \"complete\", and no reason",
                        entry.verdict,
                        shown(entry.reason.as_ref()),
                        shown(entry.basis.as_ref())
                    ));
                }
                // ARM 25. Both sides exactly when the topic was judged, over
                // the same records, at least one.
                let side_shown = |s: Option<&SideFraming>| {
                    s.map_or("absent".to_string(), |s| {
                        format!("{} records", sd::judged_records(s))
                    })
                };
                let fits = match (&entry.key, &entry.value) {
                    (Some(k), Some(v)) => {
                        assessed
                            && sd::judged_records(k) == sd::judged_records(v)
                            && sd::judged_records(k) >= 1
                    }
                    (None, None) => !assessed,
                    _ => false,
                };
                if !fits {
                    return Err(format!(
                        "schema_dependency[{topic:?}] verdict {:?} records key {} and value {}: \
                         a judged topic records a key side and a value side over the same \
                         records, at least one, and a \"notAssessed\" topic records neither",
                        entry.verdict,
                        side_shown(entry.key.as_ref()),
                        side_shown(entry.value.as_ref())
                    ));
                }
                // ARM 26. What was judged, against what the receipt counts.
                // Arms 3 and 23 make the count exist; `0` is not assumed.
                let counted = u128::from(self.records.get(topic).copied().unwrap_or(0));
                let judged_n = entry.key.as_ref().map_or(0, sd::judged_records);
                let fits = match (entry.basis.as_deref(), entry.reason.as_deref()) {
                    (Some(sd::BASIS_COMPLETE), _) => judged_n == counted,
                    (Some(_), _) => judged_n <= counted,
                    (None, Some(sd::REASON_NO_RECORDS)) => counted == 0,
                    (None, _) => true,
                };
                if !fits {
                    let under = entry
                        .basis
                        .as_ref()
                        .or(entry.reason.as_ref())
                        .map_or("absent".to_string(), |s| format!("{s:?}"));
                    return Err(format!(
                        "schema_dependency[{topic:?}] judges {judged_n} records under {under} \
                         and records counts {counted}: a \"complete\" basis judges every record \
                         the receipt counts, a \"sampled\" one at most that many, and \
                         \"noRecords\" is said only of a topic that counts none"
                    ));
                }
                for (name, side) in [("key", &entry.key), ("value", &entry.value)] {
                    let Some(side) = side else {
                        continue;
                    };
                    // ARM 27. The ids: distinct, ascending, plausible, as many
                    // as the count allows, and a count that fits the framing.
                    let listed = side.schema_ids.len() as u64;
                    let fits = side.schema_ids.windows(2).all(|w| w[0] < w[1])
                        && side
                            .schema_ids
                            .iter()
                            .all(|id| (1..=sd::MAX_SCHEMA_ID).contains(id))
                        && listed == side.schema_id_count.min(sd::SCHEMA_IDS_LISTED as u64)
                        && (side.schema_id_count >= 1) == (side.framed >= 1)
                        && side.schema_id_count <= side.framed;
                    if !fits {
                        return Err(format!(
                            "schema_dependency[{topic:?}].{name} lists schema_ids {:?} with \
                             schema_id_count {} and framed {}: the ids are distinct, ascending \
                             and from 1 to 16777215, all of them when the count is 16 or fewer \
                             and 16 otherwise, and the count is at least 1 exactly when a \
                             record is framed and never above the framed count",
                            side.schema_ids, side.schema_id_count, side.framed
                        ));
                    }
                    // ARM 28. The threshold: `dependent` is what the counts
                    // say, never a claim beside them.
                    if side.dependent != sd::dependent_by_share(side.framed, side.unframed) {
                        return Err(format!(
                            "schema_dependency[{topic:?}].{name} is {} with framed {} and \
                             unframed {}: a side is dependent exactly when at least one record \
                             and at least one in ten of its non-null records are framed",
                            if side.dependent {
                                "dependent"
                            } else {
                                "not dependent"
                            },
                            side.framed,
                            side.unframed
                        ));
                    }
                }
                // ARM 29. The verdict is what the sides say.
                if let (Some(k), Some(v)) = (&entry.key, &entry.value) {
                    let dependent = k.dependent || v.dependent;
                    if (entry.verdict == sd::SCHEMA_DEPENDENT) != dependent {
                        return Err(format!(
                            "schema_dependency[{topic:?}] verdict {:?} does not fit its sides: a \
                             judged topic is \"schemaDependent\" exactly when its key side or \
                             its value side is dependent",
                            entry.verdict
                        ));
                    }
                }
            }
        }
        // ARMS 30-35 (format 1.7.0, PROD-04.1): the `consumer_positions`
        // block, and only when it is present. Every earlier receipt, and every
        // later one whose backup selected no group, is decided exactly as
        // before. Groups in id order. The positions themselves are in the
        // document the block binds, checked by
        // `validate_consumer_positions_document` (arms CP-1 to CP-14).
        if let Some(cp) = &self.consumer_positions {
            use crate::consumer_positions as model;
            // ARM 30. A document that declares a minor before 7 cannot carry
            // a 1.7 field.
            let minor = parse_semver(&self.format_version).map_or(0, |(_, minor, _)| minor);
            if minor < CONSUMER_POSITIONS_SINCE_MINOR {
                return Err(format!(
                    "consumer_positions is present but format_version {:?} predates it: the \
                     field is defined from 1.{CONSUMER_POSITIONS_SINCE_MINOR}.0",
                    self.format_version
                ));
            }
            // ARM 31. The capture ends at or after it starts, the listing word
            // is closed, and a block records at least one group: a backup that
            // selects none carries no block.
            let window_fits = cp.observed_to >= cp.observed_from;
            if !window_fits
                || !model::LISTING_VALUES.contains(&cp.listing.as_str())
                || cp.groups.is_empty()
            {
                return Err(format!(
                    "consumer_positions records listing {:?}, {} group(s) and a capture that {}: \
                     the capture ends at or after it starts, the listing is \"complete\" or \
                     \"notComplete\", and at least one group is recorded",
                    cp.listing,
                    cp.groups.len(),
                    if window_fits {
                        "ends at or after it starts"
                    } else {
                        "ends before it starts"
                    }
                ));
            }
            // ARM 32. The positions document is the one beside this receipt,
            // named by a well-formed digest over at least one byte.
            let key = model::document_key(&self.backup_id, &self.run_id);
            let digest_fits = cp
                .document
                .sha256
                .strip_prefix("sha256:")
                .is_some_and(|hex| {
                    hex.len() == 64 && hex.bytes().all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f'))
                });
            if cp.document.key != key || !digest_fits || cp.document.bytes == 0 {
                return Err(format!(
                    "consumer_positions.document is {:?} with sha256 {:?} over {} bytes: the \
                     positions document is {key:?}, its digest \"sha256:\" and 64 lowercase hex \
                     digits, over at least one byte",
                    cp.document.key, cp.document.sha256, cp.document.bytes
                ));
            }
            let shown = |s: Option<&String>| s.map_or("absent".to_string(), |s| format!("{s:?}"));
            for (id, group) in &cp.groups {
                // ARM 33. The outcome, and a reason exactly when it is not
                // `captured`, from that outcome's set.
                let reason_fits = match (group.outcome.as_str(), group.reason.as_deref()) {
                    ("captured", None) => true,
                    ("excluded", Some(r)) => model::EXCLUDED_REASONS.contains(&r),
                    ("failed", Some(r)) => model::FAILED_REASONS.contains(&r),
                    _ => false,
                };
                if !reason_fits {
                    return Err(format!(
                        "consumer_positions.groups[{id:?}] has outcome {:?} and reason {}: the \
                         outcome is \"captured\", \"excluded\" or \"failed\", a reason is \
                         present exactly when it is not \"captured\", and it is one this format \
                         defines for that outcome",
                        group.outcome,
                        shown(group.reason.as_ref())
                    ));
                }
                // ARM 34. What a group records follows from its outcome; a
                // captured group's counts cover at least one partition, and a
                // group described `Dead` with no member is never captured.
                let captured = group.outcome == "captured";
                let other = group.reason.as_deref() == Some(model::GROUP_TYPE_NOT_CAPTURED);
                let state_ok = |s: Option<&String>| {
                    s.is_some_and(|s| model::GROUP_STATES.contains(&s.as_str()))
                };
                let fields_fit = if captured {
                    group
                        .group_type
                        .as_deref()
                        .is_some_and(|t| model::CAPTURED_TYPES.contains(&t))
                        && state_ok(group.state.as_ref())
                        && state_ok(group.listed_state.as_ref())
                        && group.members.is_some()
                        && group.active.is_some()
                        && group.counts.is_some_and(|c| c.total() >= 1)
                        && !matches!(
                            (group.state.as_deref(), group.members),
                            (Some(state), Some(members)) if model::vanished(state, members)
                        )
                } else {
                    let only_type = group.state.is_none()
                        && group.listed_state.is_none()
                        && group.members.is_none()
                        && group.active.is_none()
                        && group.counts.is_none();
                    let ty = if other {
                        group.group_type.as_deref() == Some(model::OTHER_TYPE)
                    } else {
                        group.group_type.is_none()
                    };
                    only_type && ty
                };
                if !fields_fit {
                    return Err(format!(
                        "consumer_positions.groups[{id:?}] is {:?} with group_type {}, state {}, \
                         listed_state {}, members {}, active {} and counts {}: a captured group \
                         records a type of \"classic\" or \"consumer\", both states from the \
                         closed set, its members, active and counts over at least one \
                         partition, and is never \"Dead\" with no member; a \
                         GroupTypeNotCaptured group records group_type \"other\" and nothing \
                         else; any other group records none of them",
                        group.outcome,
                        shown(group.group_type.as_ref()),
                        shown(group.state.as_ref()),
                        shown(group.listed_state.as_ref()),
                        group
                            .members
                            .map_or("absent".to_string(), |n| n.to_string()),
                        group.active.map_or("absent".to_string(), |a| a.to_string()),
                        group.counts.map_or("absent".to_string(), |c| format!(
                            "over {} partition(s)",
                            c.total()
                        ))
                    ));
                }
                if let (Some(state), Some(listed), Some(active)) =
                    (&group.state, &group.listed_state, group.active)
                {
                    // ARM 35. `active` is what the two states say.
                    let derived = model::active(state, listed);
                    if active != derived {
                        return Err(format!(
                            "consumer_positions.groups[{id:?}].active is {active} but its states \
                             {state:?} and {listed:?} say {derived}: a group is active unless \
                             both its states are \"Empty\" or \"Dead\""
                        ));
                    }
                }
            }
        }
        // ARMS 36-40 (format 1.6.0, PROD-01.4a): the `generations` block, and
        // only when it is present. Every earlier receipt is decided exactly as
        // before. Topics in name order (the map's own).
        if let Some(generations) = &self.generations {
            // ARM 36. A document that declares a minor before 6 cannot carry
            // a 1.6 field.
            let minor = parse_semver(&self.format_version).map_or(0, |(_, minor, _)| minor);
            if minor < GENERATIONS_SINCE_MINOR {
                return Err(format!(
                    "generations is present but format_version {:?} predates it: the field \
                     is defined from 1.{GENERATIONS_SINCE_MINOR}.0",
                    self.format_version
                ));
            }
            // ARM 37. The observed set and the named set are the same set —
            // the twin of arms 3, 7 and 14.
            let observed: std::collections::BTreeSet<&str> =
                generations.keys().map(String::as_str).collect();
            if observed != named_topics {
                return Err(format!(
                    "generations covers {} but the named topic set is {}",
                    render_set(&observed),
                    render_set(&named_topics)
                ));
            }
            for (topic, entry) in generations {
                let reads = [
                    ("topic_id", &entry.topic_id, &entry.topic_id_reason),
                    (
                        "topic_id_after",
                        &entry.topic_id_after,
                        &entry.topic_id_after_reason,
                    ),
                ];
                // ARM 38. A recorded ID is Kafka's text form of a real ID.
                for (field, id, _) in reads {
                    if let Some(id) = id {
                        if !crate::topic_identity::is_canonical(id) {
                            return Err(format!(
                                "generations[{topic:?}].{field} {id:?} is not a topic ID this \
                                 format defines: 22 characters of URL-safe base64 without \
                                 padding over the ID's 16 bytes, and never one of Kafka's \
                                 reserved IDs (AAAAAAAAAAAAAAAAAAAAAA, AAAAAAAAAAAAAAAAAAAAAQ)"
                            ));
                        }
                    }
                }
                // ARM 39. A reason exactly when the ID is null, from the
                // closed set: null is UNKNOWN, and the reason says why.
                for (field, id, reason) in reads {
                    let fits = match (id, reason) {
                        (Some(_), None) => true,
                        (None, Some(reason)) => {
                            crate::topic_identity::TOPIC_ID_REASONS.contains(&reason.as_str())
                        }
                        _ => false,
                    };
                    if !fits {
                        let rendered = match reason {
                            Some(reason) => format!("{reason:?}"),
                            None => "absent".to_string(),
                        };
                        let state = if id.is_some() { "recorded" } else { "null" };
                        return Err(format!(
                            "generations[{topic:?}].{field}_reason {rendered} does not fit a \
                             {state} {field}: a reason is present exactly when the ID is null, \
                             and is \"noTopicId\", \"notAuthorized\", \"topicNotFound\", \
                             \"readFailed\", \"notRead\" or \"reservedTopicId\""
                        ));
                    }
                }
                // ARM 40. A source exactly when an ID is recorded, from the
                // closed set.
                let recorded = entry.topic_id.is_some() || entry.topic_id_after.is_some();
                let fits = match &entry.topic_id_source {
                    Some(source) => {
                        recorded
                            && crate::topic_identity::TOPIC_ID_SOURCES.contains(&source.as_str())
                    }
                    None => !recorded,
                };
                if !fits {
                    let rendered = match &entry.topic_id_source {
                        Some(source) => format!("{source:?}"),
                        None => "absent".to_string(),
                    };
                    return Err(format!(
                        "generations[{topic:?}].topic_id_source {rendered} does not fit its \
                         IDs: a source is present exactly when an ID is recorded, and is \
                         \"describeTopics\" or \"engineManifest\""
                    ));
                }
            }
        }
        Ok(())
    }

    /// **PROD-04.1: the positions document, checked against THIS receipt**
    /// — arms CP-1 to CP-14, run by a reader that was given the document
    /// (`logweir drill verify --consumer-positions`, `verify_scorecard.py
    /// --consumer-positions`) after `validate_invariants` accepted the
    /// receipt. `bytes` are the document's exact bytes and `doc` their parse.
    /// `docs/verify_scorecard.py::check_consumer_positions_document` mirrors
    /// every arm, in order, with the same text.
    ///
    /// CP-1. the receipt carries a `consumer_positions` block.
    /// CP-2. the bytes' SHA-256 and length are the ones the block binds.
    /// CP-3. the document is format 1 for the receipt's backup and run.
    /// CP-4. its topics are exactly `source.topics`.
    /// CP-5. each topic lists its partitions from 0, once each, in order.
    /// CP-6. every mark pair and the archived range are whole, non-negative
    ///       and ordered, and an unobserved partition has no group-capture
    ///       marks.
    /// CP-7. `changed_during_capture` is what the marks say.
    /// CP-8. it records positions for exactly the receipt's captured groups.
    /// CP-9. no group holds a kept position on a topic that changed during
    ///       the capture, and none fails `GenerationChangedDuringCapture` when
    ///       no topic changed.
    /// CP-10. a group is captured only when every named topic's partitions
    ///        were read, and none fails `PartitionsNotRead` when all were.
    /// CP-11. a captured group's entries name partitions of the named
    ///        topics in order, once each, every unobserved one among them,
    ///        and `no_committed_position` counts every other partition:
    ///        absence is never offset 0.
    /// CP-12. each entry's status, value and reason fit one another, and
    ///        `notObserved` is exactly an unobserved partition.
    /// CP-13. a coverage word exactly on a captured position, and every kept
    ///        position's verdict is what its partition's facts derive.
    /// CP-14. the receipt's counts are what the document's positions say.
    ///
    /// # Errors
    ///
    /// The first arm the document breaks, as its message.
    pub fn validate_consumer_positions_document(
        &self,
        bytes: &[u8],
        doc: &crate::consumer_positions::PositionsDocument,
    ) -> Result<(), String> {
        use crate::consumer_positions as model;
        // ARM CP-1. Only a receipt that selected groups binds a document.
        let Some(cp) = &self.consumer_positions else {
            return Err(format!(
                "the receipt of run {:?} records no consumer_positions block, so it binds no \
                 positions document: only a backup that selected consumer groups writes one",
                self.run_id
            ));
        };
        // ARM CP-2. The exact bytes the receipt's signature covers.
        let digest = crate::ids::sha256_prefixed(bytes);
        if digest != cp.document.sha256 || bytes.len() as u64 != cp.document.bytes {
            return Err(format!(
                "the positions document is {digest} over {} bytes but the receipt binds {} over \
                 {} bytes: it is not the document this receipt signed",
                bytes.len(),
                cp.document.sha256,
                cp.document.bytes
            ));
        }
        // ARM CP-3. Format 1, for this receipt's own backup and run.
        let major = parse_semver(&doc.format_version).map(|(major, _, _)| major);
        if major != Some(1) || doc.backup_id != self.backup_id || doc.run_id != self.run_id {
            return Err(format!(
                "the positions document is format {:?} for backup {:?} run {:?} but the \
                 receipt is backup {:?} run {:?}: a format-1 positions document names its \
                 receipt's own backup and run",
                doc.format_version, doc.backup_id, doc.run_id, self.backup_id, self.run_id
            ));
        }
        // ARM CP-4. The observed topics are the named topics — the twin of
        // arms 3, 7 and 14.
        let named_topics: std::collections::BTreeSet<&str> =
            self.source.topics.iter().map(String::as_str).collect();
        let observed: std::collections::BTreeSet<&str> =
            doc.topics.keys().map(String::as_str).collect();
        if observed != named_topics {
            return Err(format!(
                "the positions document's topics cover {} but the named topic set is {}",
                render_set(&observed),
                render_set(&named_topics)
            ));
        }
        let mut changed: std::collections::BTreeSet<&str> = std::collections::BTreeSet::new();
        let mut unread: std::collections::BTreeSet<&str> = std::collections::BTreeSet::new();
        for (topic, entry) in &doc.topics {
            for (i, p) in entry.partitions.iter().enumerate() {
                // ARM CP-5. Every partition once, from 0, in order.
                if u64::from(p.partition) != i as u64 {
                    return Err(format!(
                        "the positions document's topics[{topic:?}].partitions[{i}] is \
                         partition {}: each topic lists its partitions from 0, one entry each, \
                         in order",
                        p.partition
                    ));
                }
                // ARM CP-6. Marks and ranges are well formed: a pair is
                // recorded whole, never negative and never inverted, and a
                // partition the capture did not observe has no group-capture
                // marks.
                let pair = |a: Option<i64>, b: Option<i64>| match (a, b) {
                    (None, None) => true,
                    (Some(a), Some(b)) => 0 <= a && a <= b,
                    _ => false,
                };
                let fits = pair(p.log_start, p.high_watermark)
                    && pair(p.log_start_after, p.high_watermark_after)
                    && pair(p.archived_first, p.archived_last)
                    && (p.observed || (p.log_start.is_none() && p.high_watermark.is_none()));
                if !fits {
                    return Err(format!(
                        "the positions document's topics[{topic:?}].partitions[{i}] records \
                         marks that are not well formed: a log start and its high watermark \
                         are recorded together with 0 <= log start <= high watermark, the \
                         archived range is recorded whole with 0 <= first <= last, and a \
                         partition the capture did not observe has no group-capture marks"
                    ));
                }
            }
            // ARM CP-7. `changed_during_capture` is what the marks say.
            let derived = model::changed_during_capture(&entry.partitions);
            if entry.changed_during_capture != derived {
                return Err(format!(
                    "the positions document's topics[{topic:?}].changed_during_capture is {} \
                     but its marks say {}: a topic changed during the capture exactly when a \
                     mark read after the engine is below the one read at group capture",
                    entry.changed_during_capture, derived
                ));
            }
            if derived {
                changed.insert(topic.as_str());
            }
            if entry.partitions.is_empty() {
                unread.insert(topic.as_str());
            }
        }
        // ARM CP-8. Positions for exactly the captured groups.
        let captured: std::collections::BTreeSet<&str> =
            cp.captured().map(|(id, _)| id.as_str()).collect();
        let positioned: std::collections::BTreeSet<&str> =
            doc.groups.keys().map(String::as_str).collect();
        if positioned != captured {
            return Err(format!(
                "the positions document records positions for the groups {} but the receipt's \
                 captured groups are {}: it records exactly the captured groups",
                render_set(&positioned),
                render_set(&captured)
            ));
        }
        // The (topic, partition) of every named partition, in order, with its
        // facts.
        let facts_at: std::collections::BTreeMap<(&str, u32), &model::PartitionFacts> = doc
            .topics
            .iter()
            .flat_map(|(t, e)| {
                e.partitions
                    .iter()
                    .map(move |p| ((t.as_str(), p.partition), p))
            })
            .collect();
        let shown = |s: Option<&String>| s.map_or("absent".to_string(), |s| format!("{s:?}"));
        for (id, group) in &cp.groups {
            let positions = doc.groups.get(id);
            // ARM CP-9. No group claims a position on a topic that changed
            // during the capture, and no group fails
            // GenerationChangedDuringCapture when none changed.
            let on_changed = positions
                .iter()
                .flat_map(|g| g.positions.iter())
                .any(|e| e.position.is_some() && changed.contains(e.topic.as_str()));
            let blamed = group.reason.as_deref() == Some(model::GENERATION_CHANGED);
            if on_changed || (blamed && changed.is_empty()) {
                return Err(format!(
                    "consumer_positions.groups[{id:?}] is {:?} with reason {} while the topics \
                     that changed during the capture are {}: a group holding a position on such \
                     a topic fails GenerationChangedDuringCapture, and no group fails so when \
                     none changed",
                    group.outcome,
                    shown(group.reason.as_ref()),
                    render_set(&changed)
                ));
            }
            // ARM CP-10. A group is captured only over topics whose
            // partitions were read.
            let not_read = group.reason.as_deref() == Some(model::PARTITIONS_NOT_READ);
            if (group.outcome == "captured" && !unread.is_empty())
                || (not_read && unread.is_empty())
            {
                return Err(format!(
                    "consumer_positions.groups[{id:?}] is {:?} with reason {} while the topics \
                     whose partitions were never read are {}: a group is captured only when \
                     every named topic's partitions were read, and fails PartitionsNotRead only \
                     when one was not",
                    group.outcome,
                    shown(group.reason.as_ref()),
                    render_set(&unread)
                ));
            }
            let Some(positions) = positions else {
                continue;
            };
            // ARM CP-11. The entries name partitions of the named topics, in
            // order, once each, every unobserved one among them; every other
            // partition is counted as having no committed position. Absence
            // is never offset 0, and a partition is never silently missing.
            let entries = &positions.positions;
            let out_of_place = (0..entries.len()).find(|&i| {
                let here = (entries[i].topic.as_str(), entries[i].partition);
                !facts_at.contains_key(&here)
                    || (i > 0 && (entries[i - 1].topic.as_str(), entries[i - 1].partition) >= here)
            });
            let listed: std::collections::BTreeSet<(&str, u32)> = entries
                .iter()
                .map(|e| (e.topic.as_str(), e.partition))
                .collect();
            let unobserved_missing = facts_at
                .iter()
                .filter(|(at, f)| !f.observed && !listed.contains(at))
                .count();
            let total = facts_at.len() as u64;
            if out_of_place.is_some()
                || unobserved_missing > 0
                || entries.len() as u64 + u64::from(positions.no_committed_position) != total
            {
                return Err(format!(
                    "the positions document's groups[{id:?}] lists {} position(s) (first out of \
                     place: {}), leaves {unobserved_missing} unobserved partition(s) out and \
                     counts {} without a committed position over {total} partition(s): a \
                     captured group lists, topics in name order and partitions in order, each \
                     partition of a named topic at most once and every one the capture did not \
                     observe, and counts every other partition as without a committed position",
                    entries.len(),
                    out_of_place.map_or("none".to_string(), |i| format!(
                        "{i}, {:?}:{}",
                        entries[i].topic, entries[i].partition
                    )),
                    positions.no_committed_position
                ));
            }
            for (i, entry) in entries.iter().enumerate() {
                let facts = facts_at[&(entry.topic.as_str(), entry.partition)];
                // ARM CP-12. The status, a position exactly where one was
                // committed and kept, a reason from the status's own set, and
                // `notObserved` exactly where the capture did not look.
                let status = entry.status.as_str();
                let reason = entry.reason.as_deref();
                let fits = model::POSITION_STATUSES.contains(&status)
                    && entry.position.is_some() == matches!(status, "captured" | "excluded")
                    && entry.position.is_none_or(|p| p >= 0)
                    && match status {
                        "excluded" => reason == Some(model::POSITION_BEYOND_END),
                        "failed" => {
                            reason.is_some_and(|r| model::POSITION_FAILED_REASONS.contains(&r))
                        }
                        "notObserved" => {
                            reason.is_some_and(|r| model::NOT_OBSERVED_REASONS.contains(&r))
                        }
                        _ => reason.is_none(),
                    }
                    && (status == "notObserved") != facts.observed;
                if !fits {
                    return Err(format!(
                        "the positions document's groups[{id:?}].positions[{i}] has status {:?}, \
                         position {} and reason {}: the status is \"captured\", \"excluded\", \
                         \"failed\" or \"notObserved\", a position is present exactly when it is \
                         \"captured\" or \"excluded\" and is never negative, a reason exactly \
                         when it is not \"captured\" and from that status's set, and \
                         \"notObserved\" is exactly a partition the capture did not observe",
                        entry.status,
                        entry
                            .position
                            .map_or("absent".to_string(), |p| p.to_string()),
                        shown(entry.reason.as_ref())
                    ));
                }
                // ARM CP-13. A coverage word exactly on a captured position,
                // and what a kept position records follows from its
                // partition's facts: the coverage word of a captured one,
                // PositionBeyondEnd of an excluded one.
                let derived = entry.position.map(|p| match model::relation(p, facts) {
                    model::Relation::MarksNotRead => model::MARKS_NOT_READ,
                    model::Relation::BeyondEnd => model::POSITION_BEYOND_END,
                    model::Relation::Coverage(word) => word,
                });
                let recorded = match status {
                    "excluded" => Some(model::POSITION_BEYOND_END),
                    "captured" => entry.coverage.as_deref(),
                    _ => None,
                };
                let coverage_fits = entry.coverage.is_some() == (status == "captured");
                if !coverage_fits || recorded != derived {
                    return Err(format!(
                        "the positions document's groups[{id:?}].positions[{i}] is {:?} with \
                         coverage {} at position {}, but its partition's facts make it {}: a \
                         coverage word is recorded exactly on a captured position, and a kept \
                         position's coverage, or its PositionBeyondEnd, follows from the marks \
                         and the archived range",
                        entry.status,
                        shown(entry.coverage.as_ref()),
                        entry
                            .position
                            .map_or("absent".to_string(), |p| p.to_string()),
                        derived.unwrap_or("unjudged")
                    ));
                }
            }
            // ARM CP-14. The receipt's counts are the document's.
            let derived = model::PositionCounts::of(positions);
            if group.counts != Some(derived) {
                return Err(format!(
                    "consumer_positions.groups[{id:?}].counts are {} but its positions count {}: \
                     the receipt counts what the positions document records",
                    group.counts.map_or("absent".to_string(), |c| c.render()),
                    derived.render()
                ));
            }
        }
        Ok(())
    }
}

/// `{a, b}` — a set rendered the way arm 3's message spells it.
fn render_set(set: &std::collections::BTreeSet<&str>) -> String {
    let inner: Vec<String> = set.iter().map(|t| format!("{t:?}")).collect();
    format!("{{{}}}", inner.join(", "))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_semver_refuses_anything_that_is_not_three_integers() {
        assert_eq!(parse_semver("1.0.0"), Some((1, 0, 0)));
        assert_eq!(parse_semver("1.12.3"), Some((1, 12, 3)));
        assert_eq!(parse_semver("1"), None);
        assert_eq!(parse_semver("1.0"), None);
        assert_eq!(parse_semver("1.0.0.0"), None);
        assert_eq!(parse_semver("1.0.0-rc1"), None);
        assert_eq!(parse_semver("v1.0.0"), None);
        assert_eq!(parse_semver(""), None);
    }

    #[test]
    fn render_set_is_sorted_and_quoted() {
        let set: std::collections::BTreeSet<&str> = ["b", "a"].into_iter().collect();
        assert_eq!(render_set(&set), "{\"a\", \"b\"}");
        let empty: std::collections::BTreeSet<&str> = Default::default();
        assert_eq!(render_set(&empty), "{}");
    }
}
