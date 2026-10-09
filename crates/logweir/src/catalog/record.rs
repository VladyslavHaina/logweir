//! The catalog point record: the document, its identity, and its keys.
//!
//! Decision D3 §5.1 and §5.2. Everything here is a PURE projection of a
//! signed backup receipt plus the location it was read from — no clock except
//! the one `recorded_at` the caller measures and hands in, no I/O, and no
//! field whose value this module invents.

use chrono::{DateTime, Datelike, Utc};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// The format this module writes for a POINT RECORD that pins no manifest
/// version, and the only MAJOR it reads (D3 §5.2 reading rule 1). A record that
/// carries `archive.manifest_version_id` is
/// [`FORMAT_VERSION_WITH_MANIFEST_VERSION`] instead.
///
/// `1.1.0` since FX-4, which added the optional `topics[].config_coverage`. A
/// minor bump adds optional fields only (reading rule 2), so a 1.0.0 reader
/// reads a 1.1.0 record and ignores the field; the 1.0.0 schema is frozen
/// beside the 1.1.0 one.
pub const FORMAT_VERSION: &str = "1.1.0";

/// The format of the day-sharded INDEX entry, which FX-4 did not change: its
/// fields are copies of the record's and none of them is the new one.
pub const LOG_ENTRY_FORMAT_VERSION: &str = "1.0.0";

/// **FX-7.** The format of a record that carries `archive.manifest_version_id`
/// — a MINOR bump over [`FORMAT_VERSION`] (FX-4's 1.1.0, which merged first;
/// reading rule 2: a 1.1.0 or 1.0.0 reader ignores the field and reads the
/// rest). Written only when the pin is present, so a record for a point on an
/// unversioned bucket is exactly the [`FORMAT_VERSION`] document.
pub const FORMAT_VERSION_WITH_MANIFEST_VERSION: &str = "1.2.0";

/// **PROD-05.1.** The format of a record whose topics carry the receipt's
/// `topic_configuration` (`topics[].configuration`, and `topics[].partitions`
/// from it) — the MINOR after FX-7's 1.2.0. Written exactly when the receipt
/// is a 1.3.0 one that carries the block, which every receipt this build
/// signs is; a record backfilled from an older receipt keeps the format it
/// would have had (reading rule 2: an older reader ignores the fields).
pub const FORMAT_VERSION_WITH_TOPIC_CONFIGURATION: &str = "1.3.0";

/// **PROD-01.3.** The format of a record whose `source.auth_mode` is one of the
/// modes PROD-01.3 added (`scramSha256`, `plain`, `mtls`) — copied from a
/// receipt that is itself 1.4.0 (`logweir_core::backup_receipt::
/// FORMAT_VERSION_WITH_AUTH_MODES`). A MINOR bump over
/// [`FORMAT_VERSION_WITH_TOPIC_CONFIGURATION`]: the field's set of values
/// grows and nothing else changes, and a 1.4.0 record carries PROD-05.1's
/// topic configuration and may pin a manifest version (1.4.0 includes every
/// earlier minor). Written only for those modes, so every other record is the
/// document it was.
pub const FORMAT_VERSION_WITH_AUTH_MODES: &str = "1.4.0";

/// **PROD-04.1.** The format of a record that carries `consumer_positions` —
/// copied from a receipt that is itself 1.5.0
/// (`logweir_core::backup_receipt::FORMAT_VERSION_WITH_CONSUMER_POSITIONS`).
/// A MINOR bump over [`FORMAT_VERSION_WITH_AUTH_MODES`]: one optional field,
/// which an older reader ignores, and 1.5.0 includes every earlier minor.
/// Written only for a backup that selected consumer groups.
pub const FORMAT_VERSION_WITH_CONSUMER_POSITIONS: &str = "1.5.0";
/// **PROD-03.0.** The format of a record whose topics carry the receipt's
/// `schema_dependency` (`topics[].schema_dependency`) — copied from a receipt
/// that is itself 1.5.0 (`logweir_core::backup_receipt::
/// FORMAT_VERSION_WITH_SCHEMA_DEPENDENCY`), which every receipt this build
/// signs is. A MINOR bump over [`FORMAT_VERSION_WITH_AUTH_MODES`]: one
/// optional field, and 1.5.0 defines every earlier minor's fields and values
/// (reading rule 2: an older reader ignores the field). A record backfilled
/// from an older receipt keeps the format it would have had.
pub const FORMAT_VERSION_WITH_SCHEMA_DEPENDENCY: &str = "1.5.0";

/// `lwp1-`: the identity scheme's own version, inside the identifier.
///
/// It is part of the id and not metadata beside it, so a future scheme cannot
/// produce a string that a `lwp1` reader would mistake for one of its own.
pub const POINT_ID_PREFIX: &str = "lwp1-";

/// The `logweir/`-rooted prefix every catalog object lives under. The layout
/// is versioned IN THE KEY PATH (D3 §5.5): a future `v2` writes under
/// `logweir/catalog/v2/` and nothing under `v1/` is ever rewritten.
pub const CATALOG_PREFIX: &str = "logweir/catalog/v1/";

/// `logweir/catalog/v1/points/`
pub const POINTS_PREFIX: &str = "logweir/catalog/v1/points/";

/// `logweir/catalog/v1/log/`
pub const LOG_PREFIX: &str = "logweir/catalog/v1/log/";

/// The prefix `logweir backup run` writes its receipts under, which is what
/// `logweir catalog sync`'s backfill walks. Derived in ONE place from
/// `crate::backup::phase_run::receipt_keys`'s own shape — see the test
/// `the_receipt_prefix_is_the_one_the_backup_runner_writes_under`.
pub const RECEIPTS_PREFIX: &str = "logweir/backups/";

/// The point identity, D3 §5.1: `"lwp1-" + lowercase_hex(sha256(receipt
/// bytes))[0..32]`, over the EXACT stored bytes of the signed backup receipt.
///
/// # Why the receipt and not the manifest
///
/// Tracker defect **RECEIPT-DUP**: a Backup Job re-created from its frozen
/// inputs writes a SECOND run-id receipt under the same execution id while
/// overwriting the manifest at the same key. Run identity is idempotent;
/// signed evidence is not. An identity derived from the manifest would
/// therefore collapse those two runs into one point and silently drop the
/// older receipt's window — which is exactly the history an auditor came for.
/// Derived from the receipt, the same two runs yield TWO points sharing one
/// `backup_id`, which is the distinction "recovery point" needs (§5.1's
/// closing paragraph).
///
/// # What the short id is, and what the digest is
///
/// 128 bits is a display and lookup key. The full `receipt.sha256` travels
/// beside it in the record and IS the binding; nothing in this crate decides
/// anything from the short id alone.
#[must_use]
pub fn point_id(receipt_bytes: &[u8]) -> String {
    let hex = logweir_core::ids::sha256_hex(receipt_bytes);
    // 32 hex characters = 128 bits. `sha256_hex` returns 64 lowercase hex
    // characters for every input, so this slice is total.
    format!("{POINT_ID_PREFIX}{}", &hex[..32])
}

/// `logweir/catalog/v1/points/<pointId>/record.json`
#[must_use]
pub fn record_key(point_id: &str) -> String {
    format!("{POINTS_PREFIX}{point_id}/record.json")
}

/// `logweir/catalog/v1/points/<pointId>/record.sig`
#[must_use]
pub fn record_sidecar_key(point_id: &str) -> String {
    format!("{POINTS_PREFIX}{point_id}/record.sig")
}

/// `logweir/catalog/v1/log/<yyyy>/<mm>/<dd>/<recoveryPointAtMs:013>-<pointId>.json`
///
/// The day shard and the zero-padded millisecond are what make "newest first"
/// and "only what changed since the cursor" a BOUNDED listing (D3 §5.2):
/// `Store::list_page` walks one day rather than one bucket, and keys inside a
/// shard sort by time because the number is fixed-width.
///
/// **Thirteen digits** covers every millisecond from the epoch to the year
/// 2286 (`9_999_999_999_999` ms = 2286-11-20). A negative instant — a capture
/// start before 1970, which no Kafka archive has and which a corrupt receipt
/// could still claim — is clamped to 0 rather than rendered with a `-`, which
/// would sort before every real key and break the ordering the shard exists
/// for. The clamp is recorded here because it is a lossy step: the record's
/// own `capture.started_at` keeps the value the receipt actually carried.
#[must_use]
pub fn log_key(recovery_point_at: DateTime<Utc>, point_id: &str) -> String {
    let ms = recovery_point_at.timestamp_millis().max(0);
    format!(
        "{LOG_PREFIX}{:04}/{:02}/{:02}/{:013}-{point_id}.json",
        recovery_point_at.year(),
        recovery_point_at.month(),
        recovery_point_at.day(),
        ms
    )
}

/// The signed durable record of one recovery point.
///
/// Field order IS byte order: `serde_json` is built with `preserve_order` and
/// `logweir_core::det_json::to_deterministic_json` walks the value, so
/// declaration order here is the order in the bytes that get signed. Do not
/// reorder without regenerating the current catalog point schema
/// (`just schema`; the 1.0.0 and 1.1.0 files are frozen).
///
/// **Nothing here may hold a credential.** `source.bootstrap_servers` is
/// addressing and `source.auth_mode` is a mechanism name — the same two
/// values `BackupReceipt` publishes, and for the same reason
/// (`logweir_core::backup_receipt::ReceiptAuth`: "never a password, and no
/// field that could hold one"). There is deliberately no `username` here even
/// though the receipt has one: a catalog is the surface an operator lists in
/// bulk, and a principal name is not a fact a recovery point needs.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct CatalogPoint {
    /// Semver of THIS format: [`FORMAT_VERSION`] (`1.1.0` since FX-4),
    /// [`FORMAT_VERSION_WITH_MANIFEST_VERSION`] (`1.2.0`) for a record that
    /// carries `archive.manifest_version_id` (FX-7), or
    /// [`FORMAT_VERSION_WITH_TOPIC_CONFIGURATION`] (`1.3.0`) for one whose
    /// topics carry the receipt's configuration model (PROD-05.1), or
    /// [`FORMAT_VERSION_WITH_AUTH_MODES`] (`1.4.0`) for one whose
    /// `source.auth_mode` is a mode PROD-01.3 added, or
    /// [`FORMAT_VERSION_WITH_CONSUMER_POSITIONS`] (`1.5.0`) for one that carries
    /// `consumer_positions` (PROD-04.1). Major `1`; a higher major is
    /// [`crate::catalog::reader::PointState::UnsupportedFormat`] per entry,
    /// never fatal for the sync (D3 §5.2 rule 1).
    #[schemars(regex(pattern = r"^1\.[0-9]+\.[0-9]+$"))]
    pub format_version: String,
    /// `lwp1-<32 hex>` — [`point_id`] over the receipt bytes.
    pub point_id: String,
    /// When this RECORD was written. Not a fact about the backup: the backup's
    /// own instants are `capture` below.
    pub recorded_at: DateTime<Utc>,
    pub receipt: RecordReceipt,
    /// Receipt-derived (rule 3). The archive SET identifier; two runs
    /// appending to one set share it and are still two points.
    pub backup_id: String,
    /// Receipt-derived (rule 3). The run that produced the receipt.
    pub run_id: String,
    pub archive: RecordArchive,
    /// Receipt-derived (rule 3). Half-open, epoch milliseconds, end
    /// EXCLUSIVE — interface I22, the receipt's own convention, copied and
    /// never reinterpreted.
    pub covered: RecordCovered,
    /// Receipt-derived (rule 3). `started_at` is the RECOVERY POINT (D3
    /// §3.2): freshness is measured from the capture start, never from the
    /// newest record instant, because an idle topic would otherwise look
    /// stale forever.
    pub capture: RecordCapture,
    /// One entry per topic the receipt names, in the receipt's own order.
    pub topics: Vec<RecordTopic>,
    /// **Format 1.3.0 (PROD-05.1).** The receipt's `owner_detection`, copied
    /// and never recomputed: where the run looked for declarative owners
    /// (`declared`, `kafkaTopicResources`). It is what lets a reader tell a
    /// topic with no owner found from one whose owner was never looked for:
    /// a topic without an owner beside an EMPTY list reads "owner not
    /// checked", never "applied through the admin API".
    ///
    /// Receipt-derived under rule 3 — `reader::cross_check` refuses a record
    /// whose copy the receipt does not back. ABSENT means NOT RECORDED (rule
    /// 2): every record before 1.3.0. Never read as "looked everywhere".
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub owner_detection: Option<Vec<String>>,
    pub source: RecordSource,
    /// ABSENT means the provenance is UNKNOWN — an archive imported from
    /// another installation, or a run this build could not identify. Never
    /// read as "no execution" and never defaulted (rule 2).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub execution: Option<RecordExecution>,
    /// The key that signed THIS record.
    pub signing: RecordSigning,
    /// The installation whose key signed the RECEIPT — i.e. who produced the
    /// backup, which is a different question from who wrote this record. On a
    /// backfill the two differ. ABSENT means unknown (rule 2).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub installation: Option<RecordInstallation>,
    /// **Format 1.5.0 (PROD-04.1).** The receipt's consumer position evidence,
    /// summarised ([`RecordConsumerPositions::of`]) and BOUND by the digest of
    /// the receipt's block: when the positions were observed, and per selected
    /// group its outcome and how many positions relate to archived data. The
    /// positions themselves are in the positions document the receipt's block
    /// binds by its own digest, so this digest binds them too.
    ///
    /// Receipt-derived under rule 3 — `reader::cross_check` refuses a record
    /// whose summary or digest the verified receipt does not back. ABSENT
    /// means the backup selected no group, or the record predates 1.5.0 —
    /// never "no positions".
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub consumer_positions: Option<RecordConsumerPositions>,
}

/// **PROD-04.1.** A receipt's `consumer_positions`, as the catalog carries it.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct RecordConsumerPositions {
    /// `sha256:<hex>` over the deterministic JSON of the receipt's
    /// `consumer_positions` block (`ConsumerPositions::digest`): the binding a
    /// reader recomputes from the verified receipt. The block carries the
    /// positions document's digest, so one position moved is a different
    /// block and a different digest here.
    pub sha256: String,
    /// When the group capture started (the receipt's `observed_from`).
    pub observed_from: DateTime<Utc>,
    /// When it ended, before the engine: the snapshot's freshness is measured
    /// from here to the recovery point (`capture.started_at`).
    pub observed_to: DateTime<Utc>,
    /// `complete` or `notComplete`: whether the group listings were complete.
    pub listing: String,
    /// One per selected group, in id order.
    pub groups: Vec<RecordGroup>,
}

/// One selected group in the catalog.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct RecordGroup {
    pub group_id: String,
    /// `captured`, `excluded` or `failed`.
    pub outcome: String,
    /// The receipt's reason, when not captured.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    /// `classic`, `consumer` or `other`, when the receipt records one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub group_type: Option<String>,
    /// Whether the group had members at capture (captured only).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub active: Option<bool>,
    /// Its positions, counted by what they say about archived data (captured
    /// only).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub positions: Option<logweir_core::consumer_positions::PositionCounts>,
}

impl RecordConsumerPositions {
    /// The summary of a receipt's block: the ONE projection, which the writer
    /// writes and `reader::cross_check` recomputes.
    ///
    /// # Errors
    ///
    /// The block does not serialise (it always does).
    pub fn of(block: &logweir_core::consumer_positions::ConsumerPositions) -> Result<Self, String> {
        Ok(Self {
            sha256: block.digest()?,
            observed_from: block.observed_from,
            observed_to: block.observed_to,
            listing: block.listing.clone(),
            groups: block
                .groups
                .iter()
                .map(|(id, g)| RecordGroup {
                    group_id: id.clone(),
                    outcome: g.outcome.clone(),
                    reason: g.reason.clone(),
                    group_type: g.group_type.clone(),
                    active: g.active,
                    positions: g.counts,
                })
                .collect(),
        })
    }
}

/// Where the signed receipt this point is derived from lives, and what it
/// hashes to. **The verification root**: every other field here is either
/// recomputed from these bytes or informational.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct RecordReceipt {
    /// `logweir/backups/<backup_id>/<run_id>.receipt.json`
    pub key: String,
    /// `sha256:<hex>` over the receipt bytes EXACTLY as stored. The binding;
    /// `point_id` is its 128-bit display form.
    pub sha256: String,
    /// `logweir/backups/<backup_id>/<run_id>.receipt.sig`
    pub sidecar_key: String,
    /// The receipt's DSSE media type, so a reader knows which verifier to run
    /// without guessing from the bytes.
    pub payload_type: String,
}

/// Where the archive itself is, as the writer saw it.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct RecordArchive {
    /// `s3://<bucket>/<prefix>`, `gs://…`, `az://<account>/<container>/…` or
    /// `file://<path>` — bucket and prefix only, never an endpoint and never
    /// a credential.
    ///
    /// It is the LOCATION half of D3 §5.1's "the same archive copied to a
    /// second bucket yields the same point with two `locations[]`": identity
    /// is content-derived and excludes this field entirely, so two records
    /// for one point id that differ only here describe one point in two
    /// places.
    pub location_id: String,
    /// Receipt-derived (rule 3).
    pub manifest_key: String,
    /// Receipt-derived (rule 3). `sha256:<hex>`.
    pub manifest_sha256: String,
    /// **FX-7, format `1.2.0`.** Receipt-derived: the version id of the
    /// manifest bytes the receipt attests, copied from the receipt's own
    /// `archive.manifest_version_id` and present exactly when it is.
    ///
    /// ABSENT means UNKNOWN here (rule 2) — an unversioned bucket, a receipt
    /// from before the field, or a record an older writer produced — and a
    /// reader then takes the pin from the verified RECEIPT, which is the
    /// authority. A record that carries a pin the receipt does not, or a
    /// different one, contradicts its receipt (`reader::cross_check`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub manifest_version_id: Option<String>,
    /// The archive's own key prefix, as the receipt records it.
    pub prefix: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct RecordCovered {
    /// INCLUSIVE start, epoch milliseconds.
    pub from_ms: i64,
    /// **EXCLUSIVE** end, epoch milliseconds (I22).
    pub to_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct RecordCapture {
    /// The engine subprocess's start — **the recovery point** (D3 §3.2).
    pub started_at: DateTime<Utc>,
    pub finished_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct RecordTopic {
    pub name: String,
    /// The source's partition count, receipt-derived (rule 3) from the
    /// receipt's `topic_configuration[<name>].partitions` (format 1.3.0,
    /// PROD-05.1) — the count the archive's manifest records.
    ///
    /// ABSENT means UNKNOWN (rule 2): every record derived from a receipt
    /// before 1.3.0, which records no partition count at all, and a 1.3.0 one
    /// whose manifest recorded none. A `0` here would read as "this topic has
    /// no partitions" and would let D3 §4.2's `maxPartitions` filter accept a
    /// point it has no size information about.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub partitions: Option<u32>,
    /// Records this run captured for the topic, from the receipt.
    pub records: u64,
    /// **Format 1.1.0 (FX-4).** The receipt's `config_coverage` entry for this
    /// topic, copied and never recomputed: whether the archive's record of
    /// the topic's configuration was captured, and the effective
    /// `message.timestamp.type` with its source.
    ///
    /// Receipt-derived under rule 3 — `reader::cross_check` refuses a record
    /// whose copy the receipt does not back. ABSENT means UNKNOWN (rule 2):
    /// every 1.0.0 record, and every record derived from a receipt that
    /// predates 1.1.0. Never read as `captured`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub config_coverage: Option<logweir_core::backup_receipt::TopicConfigCoverage>,
    /// **Format 1.3.0 (PROD-05.1).** The receipt's `topic_configuration`
    /// entry for this topic, copied and never recomputed: the source's
    /// partition count and replication factor, the recorded configuration
    /// entries with their portability classes, and the declarative owner —
    /// the portable desired-state model a restore rebuilds the topic from.
    ///
    /// Receipt-derived under rule 3 — `reader::cross_check` refuses a record
    /// whose copy the receipt does not back. ABSENT means NOT RECORDED (rule
    /// 2): every record before 1.3.0, and every record derived from a receipt
    /// that predates 1.3.0. Never read as "no configuration".
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub configuration: Option<logweir_core::backup_receipt::TopicConfiguration>,
    /// **Format 1.5.0 (PROD-03.0).** The receipt's `schema_dependency` entry
    /// for this topic, copied and never recomputed: whether the archived keys
    /// or values carry Confluent wire-format framing, with the schema ids
    /// seen and the basis of the judgement. A `schemaDependent` topic reads
    /// "schema-dependent, registry not captured".
    ///
    /// Receipt-derived under rule 3 — `reader::cross_check` refuses a record
    /// whose copy the receipt does not back. ABSENT means NOT ASSESSED (rule
    /// 2): every record before 1.5.0, and every record derived from a receipt
    /// that predates 1.5.0. Never read as "not schema-dependent".
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub schema_dependency: Option<logweir_core::backup_receipt::TopicSchemaDependency>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct RecordSource {
    /// Read from the broker at phase −1 and carried by the receipt — never
    /// from a spec.
    pub cluster_id: String,
    pub bootstrap_servers: Vec<String>,
    /// The receipt's `source.auth.mode`, copied: `plaintext` or `scramSha512`
    /// in every format, and from 1.4.0 also `scramSha256`, `plain` or `mtls`
    /// (PROD-01.3) — the receipt's versioned closed set, one spelling in this
    /// product.
    pub auth_mode: String,
}

/// The Kubernetes execution that produced the backup.
///
/// **Every field is optional and nothing here is invented.** D-SEAMS S4:
/// `inputs_sha256` belongs to PLAT-06.1's `execution-inputs.json` grammar and
/// this record may CITE it and may never redefine it.
///
/// # What the runner can and cannot establish (review finding F4)
///
/// PLAT-06.1 HAS landed: the controller records `Backup.status.execution` and
/// freezes `execution-inputs.json`. What it does not do is hand any of that to
/// the runner — `weirkeeper::backup_execution::runner_argv` passes
/// `--backup-id-override <execution_id>` and no namespace, name, UID or
/// `inputsSha256` — so `logweir backup run` genuinely cannot fill those fields
/// and writes them ABSENT, which rule 2 makes mean UNKNOWN.
///
/// `triggered_by` is the exception and IS filled: `BackupReceipt::triggered_by`
/// is always present in the receipt, so dropping it would have discarded
/// provenance the runner had in hand.
///
/// `execution_id` stays absent even on a controller-driven run although
/// `backup_id` happens to equal it there: the runner cannot tell an execution
/// id from a schedule slot (`--backup-id-override` carries both), and a field
/// that is right on one path and a fabrication on the other is worse than an
/// absent one.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct RecordExecution {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub kind: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub namespace: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub uid: Option<String>,
    /// `Backup.status.execution.id` (PLAT-06.1), cited and never redefined.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub execution_id: Option<String>,
    /// `Backup.status.execution.inputsSha256` (PLAT-06.1, D-SEAMS S4), cited
    /// and never redefined: this record does not describe what went into that
    /// digest and does not recompute it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub inputs_sha256: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub schedule: Option<RecordSchedule>,
    /// The receipt's `triggered_by`, verbatim. Free text; never a metric
    /// label.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub triggered_by: Option<String>,
}

impl RecordExecution {
    /// True when the block establishes nothing at all.
    ///
    /// A block of eight `None`s is not "provenance unknown" written down, it is
    /// an empty object pretending to be a fact. The writer drops it, so absent
    /// stays the one spelling of unknown (rule 2).
    #[must_use]
    pub fn is_empty(&self) -> bool {
        *self == Self::default()
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct RecordSchedule {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub uid: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub slot: Option<String>,
}

/// The two values [`RecordSigning::algorithm`] may take. One spelling in this
/// product: these are the strings
/// `crates/logweir/src/identity.rs::public_material` publishes for the SAME key
/// in the installation's public identity ConfigMap.
///
/// Declared as data rather than as prose so the PUBLISHED SCHEMA can carry the
/// vocabulary — see [`algorithm_schema`].
pub const SIGNING_ALGORITHMS: [&str; 2] = ["ecdsa-p256-sha256", "ed25519"];

/// The JSON Schema for [`RecordSigning::algorithm`]: a string with an explicit
/// `enum`, generated from [`SIGNING_ALGORITHMS`].
///
/// **Review finding F2.** Without it the checked-in schema carried only a free
/// `"type": "string"` and a doc comment naming `p256` — the very spelling this
/// record rejects — so a consumer generating its type from the one artifact
/// that exists to publish the vocabulary would have matched on a value no
/// record ever carries, and nothing mechanical would have noticed. An `enum`
/// rather than a `pattern` because the set really is closed and a reader
/// should be able to READ it out of the schema, not infer it from a regex.
fn algorithm_schema(_: &mut schemars::gen::SchemaGenerator) -> schemars::schema::Schema {
    schemars::schema::SchemaObject {
        instance_type: Some(schemars::schema::InstanceType::String.into()),
        enum_values: Some(
            SIGNING_ALGORITHMS
                .iter()
                .map(|a| serde_json::Value::String((*a).to_string()))
                .collect(),
        ),
        metadata: Some(Box::new(schemars::schema::Metadata {
            description: Some(
                "How the key that signed this record signs. A CLOSED SET OF TWO, and the \
                 same two strings the installation's public identity ConfigMap publishes for \
                 the same key (`logweir identity bootstrap`): one spelling in this product. \
                 Decision D3 §5.2's illustrative JSON writes `p256`; that is not a value any \
                 record carries."
                    .to_string(),
            ),
            ..Default::default()
        })),
        ..Default::default()
    }
    .into()
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct RecordSigning {
    /// The lowercase-hex sha256 of the key's DER SPKI — the same number
    /// `openssl` prints (`docs/keys.md`).
    pub key_id: String,
    /// `ecdsa-p256-sha256` or `ed25519` — [`SIGNING_ALGORITHMS`], and the same
    /// two strings `logweir identity bootstrap` publishes for the same key.
    /// **Never `p256`**, which is D3 §5.2's illustrative spelling and is not a
    /// value this product writes anywhere.
    #[schemars(schema_with = "algorithm_schema")]
    pub algorithm: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct RecordInstallation {
    pub key_id: String,
}

impl CatalogPoint {
    /// The EXACT bytes that get signed, stored and hashed — deterministic
    /// JSON with a trailing newline, the same encoder every other Logweir
    /// document uses.
    ///
    /// Callers serialise ONCE and reuse the buffer. A document re-rendered
    /// after signing does not verify, which is the rule
    /// `phase_run::persist_receipt`'s step 2 states for the receipt.
    pub fn canonical_bytes(&self) -> Result<Vec<u8>, String> {
        logweir_core::det_json::to_deterministic_json(self)
            .map_err(|e| format!("the catalog point record could not be serialised: {e}"))
    }

    /// The recovery point (D3 §3.2): the capture START.
    ///
    /// One accessor, so the log key, any freshness reader and the printed row
    /// cannot come apart on which of the four instants in this document means
    /// "how old is my newest recoverable backup".
    #[must_use]
    pub fn recovery_point_at(&self) -> DateTime<Utc> {
        self.capture.started_at
    }

    /// This record's own log-index key.
    #[must_use]
    pub fn log_key(&self) -> String {
        log_key(self.recovery_point_at(), &self.point_id)
    }
}

/// The tiny day-sharded index entry beside the record (D3 §5.2).
///
/// **It is an INDEX, not evidence.** It carries no signature and no sidecar
/// key of its own — D3 §5.2's key list gives `.sig` companions to records,
/// tombstones and snapshots and to nothing else. Every field here is a copy
/// of a field in the signed record, present so that listing a page costs one
/// listing instead of one `get` per point; a reader that needs to TRUST a
/// value fetches `record_key` and verifies it. `logweir catalog list` prints
/// that sentence rather than leaving it to be inferred.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct CatalogLogEntry {
    #[schemars(regex(pattern = r"^1\.[0-9]+\.[0-9]+$"))]
    pub format_version: String,
    pub point_id: String,
    pub backup_id: String,
    pub run_id: String,
    /// `capture.started_at` in epoch milliseconds — the number in the key.
    pub recovery_point_at_ms: i64,
    pub covered: RecordCovered,
    /// Where the signed record is. The only field a reader needs in order to
    /// stop trusting this entry and go and check.
    pub record_key: String,
    pub receipt_key: String,
    pub receipt_sha256: String,
}

impl CatalogLogEntry {
    /// The index entry for `point`, derived in ONE place so an entry can
    /// never describe a record that says something else.
    #[must_use]
    pub fn of(point: &CatalogPoint) -> Self {
        Self {
            format_version: LOG_ENTRY_FORMAT_VERSION.to_string(),
            point_id: point.point_id.clone(),
            backup_id: point.backup_id.clone(),
            run_id: point.run_id.clone(),
            recovery_point_at_ms: point.recovery_point_at().timestamp_millis(),
            covered: point.covered.clone(),
            record_key: record_key(&point.point_id),
            receipt_key: point.receipt.key.clone(),
            receipt_sha256: point.receipt.sha256.clone(),
        }
    }

    pub fn canonical_bytes(&self) -> Result<Vec<u8>, String> {
        logweir_core::det_json::to_deterministic_json(self)
            .map_err(|e| format!("the catalog log entry could not be serialised: {e}"))
    }
}

/// `StorageUrl` -> `archive.location_id`: **bucket and prefix, nothing else.**
///
/// No endpoint, no region, no addressing style and — the rule this function
/// exists to make unbreakable — no userinfo, because a `StorageUrl` carries
/// none and this function reads only the two fields it names. A location id
/// that quoted an endpoint would also make the same archive reached through a
/// gateway look like a different place.
#[must_use]
pub fn location_id(u: &logweir_core::engine::StorageUrl) -> String {
    use logweir_core::engine::StorageUrl as U;
    let join = |scheme: &str, root: &str, prefix: &str| {
        let prefix = prefix.trim_matches('/');
        if prefix.is_empty() {
            format!("{scheme}://{root}")
        } else {
            format!("{scheme}://{root}/{prefix}")
        }
    };
    match u {
        U::S3 { bucket, prefix, .. } => join("s3", bucket, prefix),
        U::Gcs { bucket, prefix, .. } => join("gs", bucket, prefix),
        U::Azure {
            account_name,
            container_name,
            prefix,
        } => join("az", &format!("{account_name}/{container_name}"), prefix),
        // `Filesystem` carries only `path` (`StorageUrl::prefix` returns "" for
        // it), so the path IS the location.
        U::Filesystem { path } => format!("file://{}", path.display()),
    }
}
