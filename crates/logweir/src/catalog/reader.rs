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
    let disagreements =
        ReceiptFacts::of_record(point).disagreements(&ReceiptFacts::of_receipt(receipt));
    if disagreements.is_empty() {
        CrossCheck::Agrees
    } else {
        CrossCheck::RecordMismatch(disagreements)
    }
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
    let disagreements = ReceiptFacts::of_record(a).disagreements(&ReceiptFacts::of_record(b));
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
