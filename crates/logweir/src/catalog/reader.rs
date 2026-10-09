//! Reading a catalog point record — **all four of D3 §5.2's rules, in one
//! place**, so the Rust reader and any later consumer cannot drift apart on
//! what a record is allowed to mean.
//!
//! | rule | where |
//! |---|---|
//! | 1. major must be `1`; a higher major is `UnsupportedFormat`, never fatal | [`read_record`] |
//! | 2. unknown fields ignored inside major 1; absent optional fields mean UNKNOWN, never zero | [`read_record`] + the `Option` fields on [`CatalogPoint`] |
//! | 3. the receipt-derived facts are recomputed from the VERIFIED receipt; a disagreeing copy is `RecordMismatch` | [`cross_check`] |
//! | 4. `logweir/` objects are never rewritten; a correction is a new record | the writer's create-only puts, and [`reconcile`] for what two copies mean |
//!
//! Nothing here does I/O: every function takes bytes or values the caller
//! already fetched, so the rules are testable against a fixture with no store
//! at all.

use super::record::*;
use logweir_core::backup_receipt::BackupReceipt;

/// What reading ONE record's bytes established.
#[derive(Debug, Clone, PartialEq)]
pub enum RecordVerdict {
    /// A record of major 1 that parsed. Says nothing about whether its facts
    /// agree with the receipt — that is [`cross_check`], which needs the
    /// receipt.
    Point(Box<CatalogPoint>),
    /// **Rule 1.** The document declares a `format_version` whose major this
    /// build does not implement. Per-ENTRY and never fatal for the walk: a
    /// catalog written by a newer Logweir still lists, with this entry marked
    /// and the rest readable. D3 §5.4 maps it to availability
    /// `UnsupportedFormat`.
    UnsupportedFormat { format_version: String },
    /// The bytes are not a major-1 record at all: not JSON, no
    /// `format_version`, a `format_version` that is not semver, or a shape
    /// major 1 cannot hold. Distinct from `UnsupportedFormat` because the two
    /// have different remedies — "upgrade this reader" against "this object is
    /// corrupt or is not a record".
    Unreadable(String),
}

/// **Rules 1 and 2.**
///
/// `format_version` is read out of an untyped `Value` FIRST, before the typed
/// deserialisation: a major-2 record may have any shape at all, and reaching
/// for a typed field in it would report "not a record" for a document this
/// build merely does not implement yet.
///
/// Unknown fields inside major 1 are IGNORED — there is deliberately no
/// `deny_unknown_fields` on [`CatalogPoint`] — because a minor bump adds
/// optional fields and a 1.0.0 reader must still read a 1.1.0 record. Absent
/// optional fields deserialise to `None`, which every consumer must read as
/// UNKNOWN; nothing in this module substitutes a zero for one.
pub fn read_record(bytes: &[u8]) -> RecordVerdict {
    let value: serde_json::Value = match serde_json::from_slice(bytes) {
        Ok(v) => v,
        Err(e) => return RecordVerdict::Unreadable(format!("not valid JSON: {e}")),
    };
    let Some(version) = value
        .get("format_version")
        .and_then(|v| v.as_str())
        .map(str::to_string)
    else {
        return RecordVerdict::Unreadable(
            "no `format_version` string, so this object declares no format at all".to_string(),
        );
    };
    let Some(major) = major_of(&version) else {
        return RecordVerdict::Unreadable(format!(
            "`format_version` {version:?} is not a semver major.minor.patch"
        ));
    };
    if major != 1 {
        return RecordVerdict::UnsupportedFormat {
            format_version: version,
        };
    }
    match serde_json::from_value::<CatalogPoint>(value) {
        Ok(p) => RecordVerdict::Point(Box::new(p)),
        Err(e) => RecordVerdict::Unreadable(format!(
            "declares format_version {version:?} but is not a major-1 catalog point record: {e}"
        )),
    }
}

/// The leading integer of a `major.minor.patch` string, or `None`.
///
/// Three dot-separated components, each a non-empty run of ASCII digits —
/// the same shape `BackupReceipt`'s schema pattern pins. A value like `"1"`
/// or `"1.0"` is refused rather than read as major 1: a document that does
/// not spell its own version the way the format defines is not one this
/// reader should start guessing about.
fn major_of(version: &str) -> Option<u64> {
    let mut parts = version.split('.');
    let major = parts.next()?;
    let minor = parts.next()?;
    let patch = parts.next()?;
    if parts.next().is_some() {
        return None;
    }
    for part in [major, minor, patch] {
        if part.is_empty() || !part.bytes().all(|b| b.is_ascii_digit()) {
            return None;
        }
    }
    major.parse().ok()
}

/// The facts a record does NOT get to assert on its own authority.
///
/// D3 §5.2 rule 3: everything except these is informational, because the
/// receipt's signature is the verification root. They are pulled into one
/// value so that [`cross_check`] (record against receipt) and [`reconcile`]
/// (record against another copy of itself) compare the SAME set — two
/// comparisons written separately would eventually stop agreeing about which
/// fields are load-bearing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReceiptFacts {
    pub backup_id: String,
    pub run_id: String,
    pub covered_from_ms: i64,
    pub covered_to_ms: i64,
    pub capture_started_at: String,
    pub capture_finished_at: String,
    pub manifest_key: String,
    pub manifest_sha256: String,
}

impl ReceiptFacts {
    /// As the RECORD claims them.
    #[must_use]
    pub fn of_record(p: &CatalogPoint) -> Self {
        Self {
            backup_id: p.backup_id.clone(),
            run_id: p.run_id.clone(),
            covered_from_ms: p.covered.from_ms,
            covered_to_ms: p.covered.to_ms,
            capture_started_at: p.capture.started_at.to_rfc3339(),
            capture_finished_at: p.capture.finished_at.to_rfc3339(),
            manifest_key: p.archive.manifest_key.clone(),
            manifest_sha256: p.archive.manifest_sha256.clone(),
        }
    }

    /// As the VERIFIED RECEIPT establishes them. The authority.
    #[must_use]
    pub fn of_receipt(r: &BackupReceipt) -> Self {
        Self {
            backup_id: r.backup_id.clone(),
            run_id: r.run_id.clone(),
            covered_from_ms: r.covered.from_ms,
            covered_to_ms: r.covered.to_ms,
            capture_started_at: r.started_at.to_rfc3339(),
            capture_finished_at: r.finished_at.to_rfc3339(),
            manifest_key: r.archive.manifest_key.clone(),
            manifest_sha256: r.archive.manifest_sha256.clone(),
        }
    }

    /// Every field where `self` and `other` disagree, named, so a report says
    /// WHICH fact is in dispute rather than only that something is.
    #[must_use]
    pub fn disagreements(&self, other: &Self) -> Vec<String> {
        let mut out = Vec::new();
        let mut cmp = |field: &str, a: &str, b: &str| {
            if a != b {
                out.push(format!("{field}: {a:?} vs {b:?}"));
            }
        };
        cmp("backup_id", &self.backup_id, &other.backup_id);
        cmp("run_id", &self.run_id, &other.run_id);
        cmp(
            "covered.from_ms",
            &self.covered_from_ms.to_string(),
            &other.covered_from_ms.to_string(),
        );
        cmp(
            "covered.to_ms",
            &self.covered_to_ms.to_string(),
            &other.covered_to_ms.to_string(),
        );
        cmp(
            "capture.started_at",
            &self.capture_started_at,
            &other.capture_started_at,
        );
        cmp(
            "capture.finished_at",
            &self.capture_finished_at,
            &other.capture_finished_at,
        );
        cmp(
            "archive.manifest_key",
            &self.manifest_key,
            &other.manifest_key,
        );
        cmp(
            "archive.manifest_sha256",
            &self.manifest_sha256,
            &other.manifest_sha256,
        );
        out
    }
}

/// What a record's facts turned out to be worth once the receipt was read.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CrossCheck {
    /// Every receipt-derived fact in the record is the receipt's own.
    Agrees,
    /// D3 §5.4 availability `Conflict`: the record contradicts the receipt it
    /// names. The record is NOT corrected — nothing under `logweir/` is
    /// rewritten (rule 4) — it is reported, and a correction is a new record
    /// under a new point id.
    RecordMismatch(Vec<String>),
    /// The record names a receipt whose digest is not the one it claims, so
    /// there is nothing to compare: the two documents are about different
    /// bytes. Reported separately from `RecordMismatch` because the remedy
    /// differs — the record's `point_id`/`receipt.sha256` pair is wrong, not
    /// one of its copied facts.
    WrongReceipt { claimed: String, actual: String },
}

/// **Rule 3.** Recompute the receipt-derived facts from the verified receipt
/// and compare.
///
/// `receipt_bytes` are the EXACT bytes whose DSSE signature the caller has
/// already checked; this function does no cryptography and takes the caller's
/// word for the verification, which is why its name says "cross check" and not
/// "verify". The digest comparison is here rather than at the caller so that
/// "the record is about these bytes" and "the record agrees with these bytes"
/// are answered by one function and cannot be run in the wrong order.
#[must_use]
pub fn cross_check(
    point: &CatalogPoint,
    receipt: &BackupReceipt,
    receipt_bytes: &[u8],
) -> CrossCheck {
    let actual = logweir_core::ids::sha256_prefixed(receipt_bytes);
    if point.receipt.sha256 != actual {
        return CrossCheck::WrongReceipt {
            claimed: point.receipt.sha256.clone(),
            actual,
        };
    }
    let mut disagreements =
        ReceiptFacts::of_record(point).disagreements(&ReceiptFacts::of_receipt(receipt));
    // **FX-7 — the pin, one way round.** A record that carries
    // `archive.manifest_version_id` must carry the RECEIPT's: a different pin,
    // or one the receipt does not have, would send a reader to the wrong
    // object version. A record WITHOUT one says "unknown" (rule 2) — an older
    // writer, or an unversioned bucket — and is not a contradiction: readers
    // take the pin from the verified receipt, never from the record. That is
    // why this is not a field of `ReceiptFacts`, whose comparison `reconcile`
    // also uses between two RECORDS, where one written by an older build
    // legitimately lacks the field.
    if let Some(recorded) = point.archive.manifest_version_id.as_deref() {
        let attested = receipt.archive.manifest_version_id.as_deref();
        if attested != Some(recorded) {
            disagreements.push(format!(
                "archive.manifest_version_id: {recorded:?} vs {:?}",
                attested.unwrap_or("")
            ));
        }
    }
    disagreements.extend(unbacked_coverage(point, receipt));
    disagreements.extend(unbacked_configuration(point, receipt));
    disagreements.extend(unbacked_owner_detection(point, receipt));
    disagreements.extend(unbacked_consumer_positions(point, receipt));
    disagreements.extend(unbacked_schema_dependency(point, receipt));
    disagreements.extend(unbacked_identity(point, receipt));
    if disagreements.is_empty() {
        CrossCheck::Agrees
    } else {
        CrossCheck::RecordMismatch(disagreements)
    }
}

/// **FX-4, rule 3 for `topics[].config_coverage`.** Every coverage entry the
/// RECORD claims must be the receipt's own, byte for byte — a record may know
/// LESS than its receipt (an older writer copies nothing: absent is UNKNOWN,
/// rule 2), never more, and never something else. So a record that says
/// `captured` beside a receipt that says `captureDenied`, or that carries a
/// block its 1.0.0 receipt has no way to hold, is a `RecordMismatch`: the one
/// way a catalog could present old evidence as a stronger claim.
///
/// Deliberately NOT part of [`ReceiptFacts`]: `reconcile` compares two
/// RECORDS, where one legitimately knowing less than the other is not a
/// conflict ([`coverage_conflicts`]).
fn unbacked_coverage(point: &CatalogPoint, receipt: &BackupReceipt) -> Vec<String> {
    point
        .topics
        .iter()
        .filter_map(|t| {
            let claimed = t.config_coverage.as_ref()?;
            let backed = receipt
                .config_coverage
                .as_ref()
                .and_then(|block| block.get(&t.name));
            (backed != Some(claimed)).then(|| {
                format!(
                    "topics[{:?}].config_coverage: {:?} vs {}",
                    t.name,
                    claimed.coverage,
                    backed.map_or_else(
                        || "none in the receipt".to_string(),
                        |b| format!("{:?}", b.coverage)
                    )
                )
            })
        })
        .collect()
}

/// **PROD-05.1, rule 3 for `topics[].configuration` and
/// `topics[].partitions`.** The same one-way rule as [`unbacked_coverage`]: a
/// record may carry LESS than its receipt (an older writer copies nothing:
/// absent is NOT RECORDED or UNKNOWN, rule 2), never more and never something
/// else. A configuration model, or a partition count, the receipt does not
/// back is a `RecordMismatch` — a catalog that could add an override, drop a
/// declarative owner or change a class would hand a restore a desired state
/// nobody signed.
fn unbacked_configuration(point: &CatalogPoint, receipt: &BackupReceipt) -> Vec<String> {
    let mut out = Vec::new();
    for t in &point.topics {
        let backed = receipt
            .topic_configuration
            .as_ref()
            .and_then(|block| block.get(&t.name));
        if let Some(claimed) = t.configuration.as_ref() {
            if backed != Some(claimed) {
                out.push(format!(
                    "topics[{:?}].configuration: {} vs {}",
                    t.name,
                    configuration_summary(Some(claimed)),
                    backed.map_or_else(
                        || "none in the receipt".to_string(),
                        |b| configuration_summary(Some(b))
                    )
                ));
            }
        }
        if let Some(count) = t.partitions {
            let attested = backed.and_then(|b| b.partitions);
            if attested != Some(count) {
                out.push(format!(
                    "topics[{:?}].partitions: {count} vs {}",
                    t.name,
                    attested.map_or_else(|| "none in the receipt".to_string(), |n| n.to_string())
                ));
            }
        }
    }
    out
}

/// **PROD-05.1, rule 3 for `owner_detection`.** The same one-way rule: a
/// record may carry none (an older writer), never a list its receipt does
/// not. A record that claims the run looked for owners it never looked for
/// would turn "owner not checked" into "applied through the admin API".
fn unbacked_owner_detection(point: &CatalogPoint, receipt: &BackupReceipt) -> Option<String> {
    let claimed = point.owner_detection.as_ref()?;
    (receipt.owner_detection.as_ref() != Some(claimed)).then(|| {
        format!(
            "owner_detection: {claimed:?} vs {}",
            receipt
                .owner_detection
                .as_ref()
                .map_or_else(|| "none in the receipt".to_string(), |d| format!("{d:?}"))
        )
    })
}

/// **PROD-04.1, rule 3 for `consumer_positions`.** The same one-way rule: a
/// record may carry none (an older writer, or a backup that selected no
/// group), never a summary or digest its verified receipt does not back. A
/// record that claimed a group captured, or its positions related to archived
/// data, where the receipt says otherwise would hand a cutover positions
/// nobody signed — so the whole summary is recomputed from the receipt's block
/// and compared, its digest first.
fn unbacked_consumer_positions(point: &CatalogPoint, receipt: &BackupReceipt) -> Option<String> {
    let claimed = point.consumer_positions.as_ref()?;
    let backed = receipt
        .consumer_positions
        .as_ref()
        .map(crate::catalog::record::RecordConsumerPositions::of);
    match backed {
        None => Some(format!(
            "consumer_positions: {} vs none in the receipt",
            claimed.sha256
        )),
        Some(Err(e)) => Some(format!("consumer_positions: {} vs {e}", claimed.sha256)),
        Some(Ok(b)) if b.sha256 != claimed.sha256 => Some(format!(
            "consumer_positions.sha256: {} vs {}",
            claimed.sha256, b.sha256
        )),
        Some(Ok(b)) if &b != claimed => Some(format!(
            "consumer_positions: the summary of {} is not the receipt block's",
            claimed.sha256
        )),
        Some(Ok(_)) => None,
    }
}

/// **PROD-04.1, rule 4.** Two records of one point must carry the same
/// consumer position summary wherever BOTH carry one.
fn consumer_positions_conflicts(a: &CatalogPoint, b: &CatalogPoint) -> Option<String> {
    match (a.consumer_positions.as_ref(), b.consumer_positions.as_ref()) {
        (Some(ca), Some(cb)) if ca != cb => Some(format!(
            "consumer_positions: {} vs {}",
            ca.sha256, cb.sha256
        )),
        _ => None,
    }
}

/// **PROD-03.0, rule 3 for `topics[].schema_dependency`.** The same one-way
/// rule as [`unbacked_coverage`]: a record may carry LESS than its receipt (an
/// older writer copies nothing: absent is NOT ASSESSED, rule 2), never more
/// and never something else. A record that says `notDetected` beside a
/// receipt that says `schemaDependent` — or that names schema ids the receipt
/// does not — would hide from a restore that its records need a registry.
fn unbacked_schema_dependency(point: &CatalogPoint, receipt: &BackupReceipt) -> Vec<String> {
    point
        .topics
        .iter()
        .filter_map(|t| {
            let claimed = t.schema_dependency.as_ref()?;
            let backed = receipt
                .schema_dependency
                .as_ref()
                .and_then(|block| block.get(&t.name));
            (backed != Some(claimed)).then(|| {
                format!(
                    "topics[{:?}].schema_dependency: {} vs {}",
                    t.name,
                    dependency_summary(claimed),
                    backed.map_or_else(|| "none in the receipt".to_string(), dependency_summary)
                )
            })
        })
        .collect()
}

/// A schema dependency entry in one short phrase, for a disagreement line:
/// the verdict and the ids its dependent sides name.
fn dependency_summary(e: &logweir_core::backup_receipt::TopicSchemaDependency) -> String {
    let (ids, _) = logweir_core::schema_dependency::dependent_ids(e);
    format!("{:?} ids {ids:?}", e.verdict)
}

/// **PROD-03.0, rule 4.** Two records of one point must agree on a topic's
/// schema dependency wherever BOTH carry one; one carrying none (an older
/// writer) is not a conflict.
fn schema_dependency_conflicts(a: &CatalogPoint, b: &CatalogPoint) -> Vec<String> {
    a.topics
        .iter()
        .filter_map(|ta| {
            let da = ta.schema_dependency.as_ref()?;
            let db = b
                .topics
                .iter()
                .find(|tb| tb.name == ta.name)
                .and_then(|tb| tb.schema_dependency.as_ref())?;
            (da != db).then(|| {
                format!(
                    "topics[{:?}].schema_dependency: {} vs {}",
                    ta.name,
                    dependency_summary(da),
                    dependency_summary(db)
                )
            })
        })
        .collect()
}

/// **PROD-01.4a, rule 3 for `topics[].identity`.** The same one-way rule as
/// [`unbacked_coverage`]: a record may carry no topic IDs (an older writer:
/// absent is UNKNOWN, rule 2), never IDs its receipt does not back. A record
/// that could swap an ID would turn a recreated topic into the same
/// generation — offsets of one incarnation read as the other's.
fn unbacked_identity(point: &CatalogPoint, receipt: &BackupReceipt) -> Vec<String> {
    point
        .topics
        .iter()
        .filter_map(|t| {
            let claimed = t.identity.as_ref()?;
            let backed = receipt
                .generations
                .as_ref()
                .and_then(|block| block.get(&t.name));
            (backed != Some(claimed)).then(|| {
                format!(
                    "topics[{:?}].identity: {} vs {}",
                    t.name,
                    identity_summary(claimed),
                    backed.map_or_else(|| "none in the receipt".to_string(), identity_summary)
                )
            })
        })
        .collect()
}

/// A topic's IDs in one phrase, for a disagreement line.
fn identity_summary(i: &logweir_core::backup_receipt::TopicIdentity) -> String {
    let side = |id: &Option<String>, reason: &Option<String>| match (id, reason) {
        (Some(id), _) => id.clone(),
        (None, Some(reason)) => format!("null ({reason})"),
        (None, None) => "null".to_string(),
    };
    format!(
        "{} before, {} after",
        side(&i.topic_id, &i.topic_id_reason),
        side(&i.topic_id_after, &i.topic_id_after_reason)
    )
}

/// **PROD-01.4a, rule 4 for `topics[].identity`.** Two records of one point
/// must agree on a topic's IDs wherever BOTH carry them.
fn identity_conflicts(a: &CatalogPoint, b: &CatalogPoint) -> Vec<String> {
    a.topics
        .iter()
        .filter_map(|ta| {
            let ia = ta.identity.as_ref()?;
            let ib = b
                .topics
                .iter()
                .find(|tb| tb.name == ta.name)
                .and_then(|tb| tb.identity.as_ref())?;
            (ia != ib).then(|| {
                format!(
                    "topics[{:?}].identity: {} vs {}",
                    ta.name,
                    identity_summary(ia),
                    identity_summary(ib)
                )
            })
        })
        .collect()
}

/// A model entry in one short phrase, for a disagreement line: counts, the
/// number of entries, the owner. Never a configuration VALUE: a mismatch line
/// is logged, and a value is the adopter's data.
fn configuration_summary(c: Option<&logweir_core::backup_receipt::TopicConfiguration>) -> String {
    match c {
        None => "none".to_string(),
        Some(c) => format!(
            "partitions {:?}, replication factor {:?}, {} entries, owner {:?}",
            c.partitions,
            c.replication_factor,
            c.entries
                .as_ref()
                .map_or_else(|| "no".to_string(), |e| e.len().to_string()),
            c.owner.as_ref().map(|o| o.kind.as_str())
        ),
    }
}

/// **PROD-05.1, rule 4.** Two records of one point must agree on a topic's
/// configuration model and partition count wherever BOTH carry one.
fn configuration_conflicts(a: &CatalogPoint, b: &CatalogPoint) -> Vec<String> {
    let mut out = Vec::new();
    for ta in &a.topics {
        let Some(tb) = b.topics.iter().find(|tb| tb.name == ta.name) else {
            continue;
        };
        if let (Some(ca), Some(cb)) = (ta.configuration.as_ref(), tb.configuration.as_ref()) {
            if ca != cb {
                out.push(format!(
                    "topics[{:?}].configuration: {} vs {}",
                    ta.name,
                    configuration_summary(Some(ca)),
                    configuration_summary(Some(cb))
                ));
            }
        }
        if let (Some(pa), Some(pb)) = (ta.partitions, tb.partitions) {
            if pa != pb {
                out.push(format!("topics[{:?}].partitions: {pa} vs {pb}", ta.name));
            }
        }
    }
    if let (Some(da), Some(db)) = (a.owner_detection.as_ref(), b.owner_detection.as_ref()) {
        if da != db {
            out.push(format!("owner_detection: {da:?} vs {db:?}"));
        }
    }
    out
}

/// **FX-4, rule 4 for `topics[].config_coverage`.** Two records of one point
/// must agree wherever BOTH carry an entry; one carrying none (an older
/// writer) is not a conflict.
fn coverage_conflicts(a: &CatalogPoint, b: &CatalogPoint) -> Vec<String> {
    a.topics
        .iter()
        .filter_map(|ta| {
            let ca = ta.config_coverage.as_ref()?;
            let cb = b
                .topics
                .iter()
                .find(|tb| tb.name == ta.name)
                .and_then(|tb| tb.config_coverage.as_ref())?;
            (ca != cb).then(|| {
                format!(
                    "topics[{:?}].config_coverage: {:?} vs {:?}",
                    ta.name, ca.coverage, cb.coverage
                )
            })
        })
        .collect()
}

/// What two records carrying ONE point id mean.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Duplicate {
    /// D3 §5.1: "the same archive copied to a second bucket yields the same
    /// point with two `locations[]` rather than a duplicate". One point, seen
    /// in more than one place, sorted and deduplicated.
    SameIdentity { locations: Vec<String> },
    /// D3 §5.4 availability `Conflict`, **on both copies**: two records
    /// disagree for one identity. Neither is selectable, and neither is
    /// deleted or rewritten.
    Conflict(Vec<String>),
    /// Two records that are not the same point at all. A caller that reached
    /// this compared records it should not have.
    DifferentPoints,
}

/// **Rule 4's consumer half.** Reconcile two copies of one point id.
///
/// The comparison is over [`ReceiptFacts`] — the same set rule 3 recomputes —
/// and NOT over the whole document: `recorded_at`, `signing` and
/// `archive.location_id` legitimately differ between two writers of one point,
/// and calling that a conflict would make every multi-region archive
/// unreadable.
#[must_use]
pub fn reconcile(a: &CatalogPoint, b: &CatalogPoint) -> Duplicate {
    if a.point_id != b.point_id {
        return Duplicate::DifferentPoints;
    }
    let mut disagreements = ReceiptFacts::of_record(a).disagreements(&ReceiptFacts::of_record(b));
    disagreements.extend(coverage_conflicts(a, b));
    disagreements.extend(configuration_conflicts(a, b));
    disagreements.extend(consumer_positions_conflicts(a, b));
    disagreements.extend(schema_dependency_conflicts(a, b));
    disagreements.extend(identity_conflicts(a, b));
    if !disagreements.is_empty() {
        return Duplicate::Conflict(disagreements);
    }
    // The receipt digest is part of the identity, so two records sharing an id
    // and disagreeing about it is a third kind of contradiction — folded into
    // `Conflict` because the consequence is the same: neither copy may be
    // trusted about this point.
    if a.receipt.sha256 != b.receipt.sha256 {
        return Duplicate::Conflict(vec![format!(
            "receipt.sha256: {:?} vs {:?}",
            a.receipt.sha256, b.receipt.sha256
        )]);
    }
    let mut locations = vec![a.archive.location_id.clone(), b.archive.location_id.clone()];
    locations.sort();
    locations.dedup();
    Duplicate::SameIdentity { locations }
}

// ---------------------------------------------------------------------------
// The INDEX half of the layout (review finding F3)
// ---------------------------------------------------------------------------

/// What reading ONE day-sharded index entry established.
///
/// **Rule 1 applies here too, and it did not before.** `docs/formats/
/// catalog-point.md` promises "refusal is per entry, not per catalog"; the
/// first version of this module implemented that for `record.json` and let a
/// single unreadable object under `logweir/catalog/v1/log/` abort a whole
/// listing. A `v2` Logweir that renames a field in the index, a truncated
/// object or one transient `get` would have taken `logweir catalog list` down
/// wholesale instead of listing what it could.
#[derive(Debug, Clone, PartialEq)]
pub enum LogEntryVerdict {
    Entry(Box<CatalogLogEntry>),
    /// Rule 1: a `format_version` major this build does not implement.
    UnsupportedFormat {
        format_version: String,
    },
    /// Not an index entry at all: not JSON, no `format_version`, not semver,
    /// or a shape major 1 cannot hold.
    Unreadable(String),
    /// **Review finding F6.** The entry contradicts itself: its `record_key`
    /// is not the one its `point_id` implies, or its `point_id` is not a
    /// well-formed `lwp1-<32 hex>`.
    ///
    /// The log prefix is create-only but not append-restricted, so anyone who
    /// can write a NEW key under it can publish a row attributing an arbitrary
    /// `record_key` to a chosen `point_id`. The index is not evidence and
    /// `logweir catalog list` says so on every run — but a row whose own two
    /// halves disagree is free to drop, and dropping it costs nothing and
    /// touches no trust decision.
    Inconsistent(String),
}

/// **Rule 1 and the self-consistency check, for one index entry.**
///
/// `format_version` is read from an untyped `Value` first, for the reason
/// [`read_record`] gives: a major-2 entry may have any shape, and reaching for
/// a typed field in it would report "not an entry" for something this build
/// merely does not implement yet.
pub fn read_log_entry(bytes: &[u8]) -> LogEntryVerdict {
    let value: serde_json::Value = match serde_json::from_slice(bytes) {
        Ok(v) => v,
        Err(e) => return LogEntryVerdict::Unreadable(format!("not valid JSON: {e}")),
    };
    let Some(version) = value
        .get("format_version")
        .and_then(|v| v.as_str())
        .map(str::to_string)
    else {
        return LogEntryVerdict::Unreadable(
            "no `format_version` string, so this object declares no format at all".to_string(),
        );
    };
    let Some(major) = major_of(&version) else {
        return LogEntryVerdict::Unreadable(format!(
            "`format_version` {version:?} is not a semver major.minor.patch"
        ));
    };
    if major != 1 {
        return LogEntryVerdict::UnsupportedFormat {
            format_version: version,
        };
    }
    let entry: CatalogLogEntry = match serde_json::from_value(value) {
        Ok(e) => e,
        Err(e) => {
            return LogEntryVerdict::Unreadable(format!(
                "declares format_version {version:?} but is not a major-1 catalog index entry: {e}"
            ))
        }
    };
    if !is_point_id(&entry.point_id) {
        return LogEntryVerdict::Inconsistent(format!(
            "`point_id` {:?} is not a `lwp1-` identifier followed by 32 lowercase hex characters",
            entry.point_id
        ));
    }
    let implied = record_key(&entry.point_id);
    if entry.record_key != implied {
        return LogEntryVerdict::Inconsistent(format!(
            "`record_key` {:?} is not the key `point_id` {:?} implies ({implied:?}); the entry \
             attributes one point's identity to another object",
            entry.record_key, entry.point_id
        ));
    }
    LogEntryVerdict::Entry(Box::new(entry))
}

/// `lwp1-` followed by exactly 32 LOWERCASE hex characters.
///
/// Lowercase, because [`point_id`] emits lowercase and two spellings of one
/// identity is the defect the id's own doc comment argues against.
#[must_use]
pub fn is_point_id(id: &str) -> bool {
    let Some(hex) = id.strip_prefix(POINT_ID_PREFIX) else {
        return false;
    };
    hex.len() == 32
        && hex
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}
