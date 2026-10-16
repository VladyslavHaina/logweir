//! **Schema Registry: capture at backup, import keeping IDs at restore**
//! (PROD-03.1, PROD-03.2; `docs/to-do/decisions/OD-23-core.md` §1).
//!
//! A record a Confluent serializer wrote is `0x00`, a 4-byte schema ID and a
//! payload only that schema decodes. Logweir never rewrites a record byte, so a
//! restored record is readable exactly when the target registry answers the
//! record's ID with the source's schema. This module is the pure half of that:
//!
//! - the **snapshot** a backup takes of the source registry through its REST
//!   API ([`capture`]): every subject (soft-deleted included), every version
//!   with its ID, type, schema text, references and a `deleted` flag;
//! - the **receipt block** that names the snapshot ([`ReceiptSchemaRegistry`],
//!   arms 46 to 49 of `BackupReceipt::validate_invariants`);
//! - the restore's **closure** ([`closure`]): the subjects holding an ID the
//!   restored records carry, and transitively the subjects their references
//!   name, each whole, references first;
//! - the **classifier** ([`classify`]) that compares the closure with the
//!   target registry before any topic is created, refusing `SchemaConflict`;
//! - the **import** ([`import`]) in Confluent's `IMPORT` mode, refusing
//!   `SchemaRegistryCannotKeepIds` when the target cannot keep an ID and
//!   `SchemaRegistryReadOnly` for a read-only subject, and putting each
//!   subject's mode back exactly as it was;
//! - the **scorecard block** ([`ScorecardSchemas`], arms SC-1 to SC-3 in
//!   [`check_scorecard_block`]).
//!
//! Every registry call goes through [`Rest`], which the runner implements over
//! HTTP; nothing here does I/O (Global Constraint 1).
//!
//! # What is never done
//!
//! Soft deletes are not replayed (an imported version soft-deleted while it
//! was its subject's only version stops resolving by ID on Confluent, so
//! imported versions stay live); compatibility settings and modes are not
//! captured or imported; IDs are never mapped and record bytes never
//! rewritten; imported schemas are never removed.

use crate::backup_receipt::TopicSchemaDependency;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::{BTreeMap, BTreeSet};

/// The snapshot document's `format`.
pub const SNAPSHOT_FORMAT: u32 = 1;
/// The largest snapshot a backup writes, as JSON bytes.
pub const MAX_SNAPSHOT_BYTES: usize = 32 * 1024 * 1024;
/// The most subject-versions a snapshot holds.
pub const MAX_SNAPSHOT_VERSIONS: usize = 100_000;
/// The most entries `unresolved_references` and `ids_not_in_snapshot` list.
pub const LISTED: usize = 32;
/// The most entries the scorecard's `imported` lists (`imported_count` counts
/// them all).
pub const IMPORTED_LISTED: usize = 256;

/// Backup coverage reason: the registry did not answer (transport error, or a
/// 5xx).
pub const REASON_UNREACHABLE: &str = "registryUnreachable";
/// Backup coverage reason: the registry refused the credential (401, 403).
pub const REASON_UNAUTHORIZED: &str = "registryUnauthorized";
/// Backup coverage reason: over [`MAX_SNAPSHOT_BYTES`] or
/// [`MAX_SNAPSHOT_VERSIONS`].
pub const REASON_TOO_LARGE: &str = "snapshotTooLarge";
/// Backup coverage reason: a listed version could not be read, an answer did
/// not parse, or the time budget ran out.
pub const REASON_INCOMPLETE: &str = "snapshotIncomplete";
/// `schema_registry.reason`'s closed set (arm SR-R2).
pub const CAPTURE_REASONS: [&str; 4] = [
    REASON_UNREACHABLE,
    REASON_UNAUTHORIZED,
    REASON_TOO_LARGE,
    REASON_INCOMPLETE,
];

/// Scorecard: the archive has a snapshot and the target connection names no
/// registry, so nothing was imported.
pub const NOT_IMPORTED_NO_TARGET_REGISTRY: &str = "noTargetRegistry";
/// Scorecard: the target connection names a registry and the archive has no
/// captured snapshot.
pub const NOT_IMPORTED_NO_SNAPSHOT: &str = "noSnapshot";
/// `target.schemas.not_imported`'s closed set (arm SC-2).
pub const NOT_IMPORTED_REASONS: [&str; 2] =
    [NOT_IMPORTED_NO_TARGET_REGISTRY, NOT_IMPORTED_NO_SNAPSHOT];

/// Restore refusal: an ID or subject-version the target holds has other
/// content.
pub const SCHEMA_CONFLICT: &str = "SchemaConflict";
/// Restore refusal: the target registry cannot register a schema under its
/// source ID (no `IMPORT` mode, or it answered another ID).
pub const CANNOT_KEEP_IDS: &str = "SchemaRegistryCannotKeepIds";
/// Restore refusal: the snapshot's bytes are not the ones the receipt names,
/// or do not parse.
pub const SNAPSHOT_INVALID: &str = "SchemaSnapshotInvalid";
/// Restore refusal: the target registry did not answer.
pub const UNREACHABLE: &str = "SchemaRegistryUnreachable";
/// Restore refusal: a subject to import is, or inherits, `READONLY` or
/// `READONLY_OVERRIDE`; refused before any write.
pub const READ_ONLY: &str = "SchemaRegistryReadOnly";
/// Restore refusal: a subject's mode could not be put back after the import
/// set it to `IMPORT`.
pub const MODE_NOT_RESTORED: &str = "SchemaRegistryModeNotRestored";

/// The snapshot object's key: beside the receipt, create-only.
#[must_use]
pub fn snapshot_key(backup_id: &str, run_id: &str) -> String {
    format!("logweir/backups/{backup_id}/{run_id}.schema-registry.json")
}

/// One schema reference, as the registry answers it.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct SchemaReference {
    /// The name the schema imports it by.
    pub name: String,
    /// The referenced subject.
    pub subject: String,
    /// The referenced version.
    pub version: u32,
}

/// One subject-version of the snapshot.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SnapshotVersion {
    /// The subject.
    pub subject: String,
    /// The version number.
    pub version: u32,
    /// The schema ID, which restored records carry.
    pub id: u32,
    /// `AVRO`, `PROTOBUF` or `JSON`.
    #[serde(rename = "schemaType")]
    pub schema_type: String,
    /// The schema text, as the registry answered it.
    pub schema: String,
    /// The references, in the registry's order.
    #[serde(default)]
    pub references: Vec<SchemaReference>,
    /// Whether the version was soft-deleted at capture. Recorded, never
    /// replayed.
    pub deleted: bool,
}

/// The snapshot object (`<run_id>.schema-registry.json`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Snapshot {
    /// [`SNAPSHOT_FORMAT`].
    pub format: u32,
    /// When the capture started.
    pub captured_at: chrono::DateTime<chrono::Utc>,
    /// The registry it was read from.
    pub registry_url: String,
    /// Every subject-version, by subject then version.
    pub versions: Vec<SnapshotVersion>,
}

impl Snapshot {
    /// The snapshot's IDs.
    #[must_use]
    pub fn ids(&self) -> BTreeSet<u32> {
        self.versions.iter().map(|v| v.id).collect()
    }

    /// The number of distinct subjects.
    #[must_use]
    pub fn subject_count(&self) -> u64 {
        self.versions
            .iter()
            .map(|v| v.subject.as_str())
            .collect::<BTreeSet<_>>()
            .len() as u64
    }

    /// Every reference to a subject-version the snapshot lacks, as
    /// `<subject>@<version>`, sorted and distinct.
    #[must_use]
    pub fn unresolved_references(&self) -> Vec<String> {
        let held: BTreeSet<(&str, u32)> = self
            .versions
            .iter()
            .map(|v| (v.subject.as_str(), v.version))
            .collect();
        self.versions
            .iter()
            .flat_map(|v| v.references.iter())
            .filter(|r| !held.contains(&(r.subject.as_str(), r.version)))
            .map(|r| listed_text(&format!("{}@{}", r.subject, r.version)))
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect()
    }

    /// The snapshot's bytes, deterministic, refused over
    /// [`MAX_SNAPSHOT_BYTES`].
    ///
    /// # Errors
    ///
    /// [`CaptureFailure::TooLarge`].
    pub fn to_bytes(&self) -> Result<Vec<u8>, CaptureFailure> {
        let bytes = crate::det_json::to_deterministic_json(self).map_err(|e| {
            CaptureFailure::Incomplete(format!("the snapshot did not serialise: {e}"))
        })?;
        if bytes.len() > MAX_SNAPSHOT_BYTES {
            return Err(CaptureFailure::TooLarge);
        }
        Ok(bytes)
    }

    /// Parse the bytes a receipt names by `sha256`.
    ///
    /// # Errors
    ///
    /// A [`SNAPSHOT_INVALID`] message when the digest differs, the bytes do
    /// not parse, the format is not [`SNAPSHOT_FORMAT`], or a subject-version
    /// appears twice.
    pub fn parse(bytes: &[u8], sha256: &str) -> Result<Snapshot, String> {
        let digest = crate::ids::sha256_prefixed(bytes);
        if digest != sha256 {
            return Err(format!(
                "{SNAPSHOT_INVALID}: the schema snapshot is {digest} but the receipt names \
                 {sha256}; it is not the snapshot the backup signed"
            ));
        }
        let snapshot: Snapshot = serde_json::from_slice(bytes)
            .map_err(|e| format!("{SNAPSHOT_INVALID}: the schema snapshot does not parse: {e}"))?;
        if snapshot.format != SNAPSHOT_FORMAT {
            return Err(format!(
                "{SNAPSHOT_INVALID}: the schema snapshot is format {}; this build reads \
                 format {SNAPSHOT_FORMAT}",
                snapshot.format
            ));
        }
        let mut seen = BTreeSet::new();
        for v in &snapshot.versions {
            if !seen.insert((v.subject.as_str(), v.version)) {
                return Err(format!(
                    "{SNAPSHOT_INVALID}: the schema snapshot lists {}@{} twice",
                    v.subject, v.version
                ));
            }
        }
        Ok(snapshot)
    }
}

/// The longest text one listed entry keeps, in characters: a subject name is
/// the registry's to choose, and a receipt's size is bounded.
pub const LISTED_TEXT_CHARS: usize = 256;

fn listed_text(text: &str) -> String {
    text.chars().take(LISTED_TEXT_CHARS).collect()
}

/// The registry's REST API, as the runner reaches it. `path` starts with `/`
/// and is already percent-encoded ([`encode`]); the answer is the status and
/// the body parsed as JSON (`Value::Null` for an empty or unparsable body).
/// `Err` is a transport failure.
pub trait Rest {
    /// One request.
    ///
    /// # Errors
    ///
    /// A transport failure, as text naming no credential.
    fn call(
        &mut self,
        method: &str,
        path: &str,
        body: Option<&Value>,
    ) -> Result<(u16, Value), String>;
}

/// Percent-encode a subject for a path segment (every byte outside the
/// unreserved set).
#[must_use]
pub fn encode(subject: &str) -> String {
    let mut out = String::with_capacity(subject.len());
    for b in subject.bytes() {
        if b.is_ascii_alphanumeric() || matches!(b, b'-' | b'.' | b'_' | b'~') {
            out.push(char::from(b));
        } else {
            out.push_str(&format!("%{b:02X}"));
        }
    }
    out
}

/// Why a capture did not complete. Never a failed backup.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CaptureFailure {
    /// [`REASON_UNREACHABLE`].
    Unreachable(String),
    /// [`REASON_UNAUTHORIZED`].
    Unauthorized,
    /// [`REASON_TOO_LARGE`].
    TooLarge,
    /// [`REASON_INCOMPLETE`].
    Incomplete(String),
}

impl CaptureFailure {
    /// The receipt's reason.
    #[must_use]
    pub fn reason(&self) -> &'static str {
        match self {
            Self::Unreachable(_) => REASON_UNREACHABLE,
            Self::Unauthorized => REASON_UNAUTHORIZED,
            Self::TooLarge => REASON_TOO_LARGE,
            Self::Incomplete(_) => REASON_INCOMPLETE,
        }
    }
}

impl std::fmt::Display for CaptureFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Unreachable(why) => write!(f, "{REASON_UNREACHABLE}: {why}"),
            Self::Unauthorized => write!(
                f,
                "{REASON_UNAUTHORIZED}: the registry refused the connection's registry credential"
            ),
            Self::TooLarge => write!(
                f,
                "{REASON_TOO_LARGE}: the registry holds more than {MAX_SNAPSHOT_VERSIONS} \
                 versions or {MAX_SNAPSHOT_BYTES} bytes of schemas"
            ),
            Self::Incomplete(why) => write!(f, "{REASON_INCOMPLETE}: {why}"),
        }
    }
}

/// A GET whose answer must be 200; the classes of [`CaptureFailure`] for
/// everything else.
fn get_ok(rest: &mut dyn Rest, path: &str) -> Result<Value, CaptureFailure> {
    match rest.call("GET", path, None) {
        Err(e) => Err(CaptureFailure::Unreachable(e)),
        Ok((200, body)) => Ok(body),
        Ok((401 | 403, _)) => Err(CaptureFailure::Unauthorized),
        Ok((status, _)) if status >= 500 => Err(CaptureFailure::Unreachable(format!(
            "GET {path} answered {status}"
        ))),
        Ok((status, _)) => Err(CaptureFailure::Incomplete(format!(
            "GET {path} answered {status}"
        ))),
    }
}

fn strings(v: &Value, path: &str) -> Result<Vec<String>, CaptureFailure> {
    serde_json::from_value(v.clone())
        .map_err(|_| CaptureFailure::Incomplete(format!("GET {path} is not a list of subjects")))
}

fn numbers(v: &Value, path: &str) -> Result<Vec<u32>, CaptureFailure> {
    serde_json::from_value(v.clone())
        .map_err(|_| CaptureFailure::Incomplete(format!("GET {path} is not a list of versions")))
}

/// Read the whole registry through the portable calls
/// (`/subjects?deleted=true`, `/subjects`, `/subjects/{s}/versions[?deleted=true]`,
/// `/subjects/{s}/versions/{v}?deleted=true`), by subject then version.
/// `out_of_time` is asked before every request.
///
/// # Errors
///
/// [`CaptureFailure`]; never a failed backup.
pub fn capture(
    rest: &mut dyn Rest,
    out_of_time: &dyn Fn() -> bool,
) -> Result<Vec<SnapshotVersion>, CaptureFailure> {
    let late = || CaptureFailure::Incomplete("the capture's time budget ran out".to_string());
    let mut all = strings(&get_ok(rest, "/subjects?deleted=true")?, "/subjects")?;
    all.sort();
    all.dedup();
    let live_subjects: BTreeSet<String> = strings(&get_ok(rest, "/subjects")?, "/subjects")?
        .into_iter()
        .collect();
    let mut out = Vec::new();
    for subject in &all {
        let s = encode(subject);
        if out_of_time() {
            return Err(late());
        }
        let path = format!("/subjects/{s}/versions?deleted=true");
        let mut versions = numbers(&get_ok(rest, &path)?, &path)?;
        versions.sort_unstable();
        versions.dedup();
        let live: BTreeSet<u32> = if live_subjects.contains(subject) {
            let path = format!("/subjects/{s}/versions");
            numbers(&get_ok(rest, &path)?, &path)?.into_iter().collect()
        } else {
            BTreeSet::new()
        };
        for version in versions {
            if out.len() >= MAX_SNAPSHOT_VERSIONS {
                return Err(CaptureFailure::TooLarge);
            }
            if out_of_time() {
                return Err(late());
            }
            let path = format!("/subjects/{s}/versions/{version}?deleted=true");
            let body = get_ok(rest, &path)?;
            let unreadable =
                || CaptureFailure::Incomplete(format!("GET {path} is not a schema version"));
            let id = body
                .get("id")
                .and_then(Value::as_u64)
                .and_then(|id| u32::try_from(id).ok())
                .ok_or_else(unreadable)?;
            let schema = body
                .get("schema")
                .and_then(Value::as_str)
                .ok_or_else(unreadable)?
                .to_string();
            let schema_type = body
                .get("schemaType")
                .and_then(Value::as_str)
                .unwrap_or("AVRO")
                .to_string();
            let references: Vec<SchemaReference> = match body.get("references") {
                None | Some(Value::Null) => Vec::new(),
                Some(r) => serde_json::from_value(r.clone()).map_err(|_| unreadable())?,
            };
            out.push(SnapshotVersion {
                subject: subject.clone(),
                version,
                id,
                schema_type,
                schema,
                references,
                deleted: !live.contains(&version),
            });
        }
    }
    Ok(out)
}

/// The receipt's `schema_registry` block (receipt format 1.9.0).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct ReceiptSchemaRegistry {
    /// Whether the snapshot was taken and written.
    pub captured: bool,
    /// Why not, from [`CAPTURE_REASONS`]: present exactly when `captured` is
    /// false.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    /// The snapshot object's key ([`snapshot_key`]): present exactly when
    /// captured.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub snapshot_key: Option<String>,
    /// The snapshot's `sha256:` digest: present exactly when captured.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub snapshot_sha256: Option<String>,
    /// Distinct subjects in the snapshot.
    pub subjects: u64,
    /// Subject-versions in the snapshot.
    pub versions: u64,
    /// References to subject-versions the snapshot lacks, `<subject>@<version>`,
    /// sorted, at most [`LISTED`].
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub unresolved_references: Vec<String>,
    /// Schema IDs the records carry (the receipt's `schema_dependency`) that
    /// the snapshot lacks, sorted, at most [`LISTED`].
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub ids_not_in_snapshot: Vec<u32>,
}

impl ReceiptSchemaRegistry {
    /// A capture that did not complete.
    #[must_use]
    pub fn not_captured(failure: &CaptureFailure) -> Self {
        Self {
            captured: false,
            reason: Some(failure.reason().to_string()),
            snapshot_key: None,
            snapshot_sha256: None,
            subjects: 0,
            versions: 0,
            unresolved_references: Vec::new(),
            ids_not_in_snapshot: Vec::new(),
        }
    }

    /// A written snapshot, summarised: its key and digest, its counts, its
    /// unresolved references, and the IDs `dependency` lists that it lacks.
    #[must_use]
    pub fn captured(
        snapshot: &Snapshot,
        key: String,
        sha256: String,
        dependency: &BTreeMap<String, TopicSchemaDependency>,
    ) -> Self {
        let held = snapshot.ids();
        let seen: BTreeSet<u32> = dependency
            .values()
            .flat_map(|e| crate::schema_dependency::dependent_ids(e).0)
            .collect();
        Self {
            captured: true,
            reason: None,
            snapshot_key: Some(key),
            snapshot_sha256: Some(sha256),
            subjects: snapshot.subject_count(),
            versions: snapshot.versions.len() as u64,
            unresolved_references: snapshot
                .unresolved_references()
                .into_iter()
                .take(LISTED)
                .collect(),
            ids_not_in_snapshot: seen.difference(&held).copied().take(LISTED).collect(),
        }
    }

    /// Whether the capture is complete: captured, with no unresolved
    /// reference and no record ID the snapshot lacks. Anything else is
    /// incomplete coverage on the backup and its catalog point.
    #[must_use]
    pub fn complete(&self) -> bool {
        self.captured
            && self.unresolved_references.is_empty()
            && self.ids_not_in_snapshot.is_empty()
    }

    /// The one-line reason a point's registry coverage is incomplete, or
    /// `None` when it is complete.
    #[must_use]
    pub fn incomplete_reason(&self) -> Option<String> {
        if let Some(reason) = &self.reason {
            return Some(reason.clone());
        }
        if !self.unresolved_references.is_empty() {
            return Some("unresolvedReferences".to_string());
        }
        if !self.ids_not_in_snapshot.is_empty() {
            return Some("idsNotInSnapshot".to_string());
        }
        None
    }
}

/// The tail every "not captured" receipt line ends with.
const NOT_CAPTURED_TAIL: &str =
    "this receipt holds no schema snapshot, so a restore cannot put the schemas its records \
     reference in a target registry";

/// The `schema_registry` line both readers print for a receipt: what was
/// captured and what is missing, or that the registry was not captured
/// (every receipt before 1.9.0 too). `docs/verify_scorecard.py::
/// _schema_registry_lines` prints the same line.
#[must_use]
pub fn receipt_lines(block: Option<&ReceiptSchemaRegistry>) -> Vec<String> {
    let Some(block) = block else {
        return vec![format!(
            "schema_registry: registry not captured: {NOT_CAPTURED_TAIL}"
        )];
    };
    if !block.captured {
        return vec![format!(
            "schema_registry: registry not captured ({}): {NOT_CAPTURED_TAIL}",
            block.reason.as_deref().unwrap_or_default()
        )];
    }
    let mut line = format!(
        "schema_registry: captured {} subject(s), {} version(s) in {} ({})",
        block.subjects,
        block.versions,
        block.snapshot_key.as_deref().unwrap_or_default(),
        block.snapshot_sha256.as_deref().unwrap_or_default()
    );
    if !block.unresolved_references.is_empty() {
        line.push_str(&format!(
            "; INCOMPLETE: references to versions the snapshot lacks: {}",
            block.unresolved_references.join(", ")
        ));
    }
    if !block.ids_not_in_snapshot.is_empty() {
        line.push_str(&format!(
            "; INCOMPLETE: record schema IDs the snapshot lacks: {}",
            join_ids(&block.ids_not_in_snapshot)
        ));
    }
    vec![line]
}

fn join_ids(ids: &[u32]) -> String {
    ids.iter()
        .map(u32::to_string)
        .collect::<Vec<_>>()
        .join(", ")
}

/// The `schemas:` line both readers print for a scorecard carrying
/// `target.schemas`; nothing for one without it.
/// `docs/verify_scorecard.py::_schemas_lines` prints the same line.
#[must_use]
pub fn scorecard_lines(block: Option<&ScorecardSchemas>) -> Vec<String> {
    let Some(block) = block else {
        return Vec::new();
    };
    if let Some(reason) = &block.not_imported {
        return vec![format!(
            "schemas: not imported ({reason}): the restored records carry their source schema \
             IDs, which the target registry may not hold"
        )];
    }
    let mut line = format!(
        "schemas: imported {} subject-version(s) into {} keeping their source IDs, {} already \
         present; snapshot {}",
        block.imported_count,
        block.target_registry_url.as_deref().unwrap_or_default(),
        block.already_present,
        block.snapshot_sha256.as_deref().unwrap_or_default()
    );
    if !block.ids_not_in_snapshot.is_empty() {
        line.push_str(&format!(
            "; record schema IDs the snapshot lacks: {}",
            join_ids(&block.ids_not_in_snapshot)
        ));
    }
    vec![line]
}

fn sorted_distinct<T: Ord>(items: &[T]) -> bool {
    items.windows(2).all(|w| w[0] < w[1])
}

/// **A point's registry capture as the catalog carries it** (catalog point
/// 1.8.0): the receipt block's verdict and counts, never the snapshot.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct PointSchemaRegistry {
    /// The receipt's `captured`.
    pub captured: bool,
    /// The receipt's `subjects`.
    pub subjects: u64,
    /// The receipt's `versions`.
    pub versions: u64,
    /// Why the capture is incomplete: the receipt's `reason`,
    /// `unresolvedReferences` or `idsNotInSnapshot`. Absent when complete.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub incomplete_reason: Option<String>,
}

impl PointSchemaRegistry {
    /// The summary of a receipt's block.
    #[must_use]
    pub fn of(block: &ReceiptSchemaRegistry) -> Self {
        Self {
            captured: block.captured,
            subjects: block.subjects,
            versions: block.versions,
            incomplete_reason: block.incomplete_reason(),
        }
    }
}

/// Arm 48's predicate (`BackupReceipt::validate_invariants`): a captured
/// block names its snapshot beside this receipt by key and digest and counts
/// no more subjects than versions (both zero or neither); an uncaptured one
/// names, counts and lists nothing.
#[must_use]
pub fn receipt_block_shape_ok(
    block: &ReceiptSchemaRegistry,
    backup_id: &str,
    run_id: &str,
) -> bool {
    if block.captured {
        block.snapshot_key.as_deref() == Some(snapshot_key(backup_id, run_id).as_str())
            && block
                .snapshot_sha256
                .as_deref()
                .is_some_and(crate::check_contract::is_sha256_prefixed)
            && block.subjects <= block.versions
            && (block.subjects == 0) == (block.versions == 0)
    } else {
        block.snapshot_key.is_none()
            && block.snapshot_sha256.is_none()
            && block.subjects == 0
            && block.versions == 0
            && block.unresolved_references.is_empty()
            && block.ids_not_in_snapshot.is_empty()
    }
}

/// Arm 49's predicate: both lists at most [`LISTED`] entries, sorted and
/// distinct, each reference at most [`LISTED_TEXT_CHARS`] characters, each ID
/// a plausible schema ID.
#[must_use]
pub fn receipt_block_lists_ok(block: &ReceiptSchemaRegistry) -> bool {
    block.unresolved_references.len() <= LISTED
        && block.ids_not_in_snapshot.len() <= LISTED
        && sorted_distinct(&block.unresolved_references)
        && sorted_distinct(&block.ids_not_in_snapshot)
        && block
            .unresolved_references
            .iter()
            .all(|r| r.chars().count() <= LISTED_TEXT_CHARS)
        && block
            .ids_not_in_snapshot
            .iter()
            .all(|id| (1..=crate::schema_dependency::MAX_SCHEMA_ID).contains(id))
}

/// The subject-versions a restore imports: every subject holding an ID in
/// `needed`, and transitively every subject a reference of theirs names, each
/// WHOLE, ordered so a version follows every version it references and every
/// earlier version of its subject. A reference the snapshot lacks is skipped
/// here (the receipt names it).
#[must_use]
pub fn closure<'a>(snapshot: &'a Snapshot, needed: &BTreeSet<u32>) -> Vec<&'a SnapshotVersion> {
    let mut by_subject: BTreeMap<&str, Vec<&SnapshotVersion>> = BTreeMap::new();
    for v in &snapshot.versions {
        by_subject.entry(v.subject.as_str()).or_default().push(v);
    }
    for versions in by_subject.values_mut() {
        versions.sort_by_key(|v| v.version);
    }
    let mut subjects: BTreeSet<&str> = snapshot
        .versions
        .iter()
        .filter(|v| needed.contains(&v.id))
        .map(|v| v.subject.as_str())
        .collect();
    let mut frontier: Vec<&str> = subjects.iter().copied().collect();
    while let Some(subject) = frontier.pop() {
        for v in by_subject.get(subject).into_iter().flatten() {
            for r in &v.references {
                if by_subject.contains_key(r.subject.as_str()) && subjects.insert(&r.subject) {
                    frontier.push(&r.subject);
                }
            }
        }
    }
    let mut order: Vec<&SnapshotVersion> = Vec::new();
    let mut placed: BTreeSet<(&str, u32)> = BTreeSet::new();
    fn visit<'a>(
        v: &'a SnapshotVersion,
        by_subject: &BTreeMap<&'a str, Vec<&'a SnapshotVersion>>,
        placed: &mut BTreeSet<(&'a str, u32)>,
        visiting: &mut BTreeSet<(&'a str, u32)>,
        order: &mut Vec<&'a SnapshotVersion>,
    ) {
        let key = (v.subject.as_str(), v.version);
        if placed.contains(&key) || !visiting.insert(key) {
            return;
        }
        let earlier = by_subject
            .get(v.subject.as_str())
            .into_iter()
            .flatten()
            .filter(|e| e.version < v.version);
        let referenced = v.references.iter().filter_map(|r| {
            by_subject
                .get(r.subject.as_str())
                .and_then(|vs| vs.iter().find(|e| e.version == r.version))
        });
        for dep in earlier.chain(referenced).copied().collect::<Vec<_>>() {
            visit(dep, by_subject, placed, visiting, order);
        }
        visiting.remove(&key);
        placed.insert(key);
        order.push(v);
    }
    let mut visiting = BTreeSet::new();
    for subject in &subjects {
        for v in by_subject.get(subject).into_iter().flatten() {
            visit(v, &by_subject, &mut placed, &mut visiting, &mut order);
        }
    }
    order
}

/// What the target holds for one closure entry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Class {
    /// The target answers this ID with this schema under this subject and
    /// version: nothing to write.
    Present,
    /// The target holds neither the ID nor the subject-version: import it.
    Import,
}

/// Why a restore's schemas cannot be put in place. Every arm refuses before
/// any topic is created (exit 3).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ImportRefusal {
    /// [`SCHEMA_CONFLICT`]: the subject, version and ID, and what differs.
    Conflict {
        /// The subject.
        subject: String,
        /// The version.
        version: u32,
        /// The ID.
        id: u32,
        /// What the target answered.
        detail: String,
    },
    /// [`CANNOT_KEEP_IDS`]: the subject and what the target answered.
    CannotKeepIds {
        /// The subject.
        subject: String,
        /// What the target answered.
        detail: String,
    },
    /// [`UNREACHABLE`].
    Unreachable(String),
    /// [`READ_ONLY`]: a subject to import is, or inherits, a read-only mode;
    /// refused before any write.
    ReadOnly {
        /// The subject.
        subject: String,
        /// Its effective mode.
        mode: String,
    },
    /// [`MODE_NOT_RESTORED`]: a subject's mode could not be put back after
    /// the import touched it; `after` is the refusal that stopped the import
    /// first, if any.
    ModeNotRestored {
        /// Which subjects, and what the registry answered.
        detail: String,
        /// The refusal that came first.
        after: Option<Box<ImportRefusal>>,
    },
}

impl ImportRefusal {
    /// The refusal's reason.
    #[must_use]
    pub fn reason(&self) -> &'static str {
        match self {
            Self::Conflict { .. } => SCHEMA_CONFLICT,
            Self::CannotKeepIds { .. } => CANNOT_KEEP_IDS,
            Self::Unreachable(_) => UNREACHABLE,
            Self::ReadOnly { .. } => READ_ONLY,
            Self::ModeNotRestored { .. } => MODE_NOT_RESTORED,
        }
    }
}

impl std::fmt::Display for ImportRefusal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Conflict {
                subject,
                version,
                id,
                detail,
            } => write!(
                f,
                "{SCHEMA_CONFLICT}: the target registry holds subject {subject:?} version \
                 {version} or schema ID {id} with other content ({detail}); restored records \
                 would decode against the wrong schema, so nothing was written and no topic \
                 was created. Restore into a registry that holds none of these schemas, or the \
                 source's own"
            ),
            Self::CannotKeepIds { subject, detail } => write!(
                f,
                "{CANNOT_KEEP_IDS}: the target registry cannot register subject {subject:?}'s \
                 schemas under their source IDs ({detail}); Logweir never rewrites record \
                 bytes, so the restore is refused before any topic is created. Use a registry \
                 with IMPORT mode (Confluent Schema Registry, Redpanda)"
            ),
            Self::Unreachable(why) => write!(
                f,
                "{UNREACHABLE}: the target registry did not answer ({why}); no topic was created"
            ),
            Self::ReadOnly { subject, mode } => write!(
                f,
                "{READ_ONLY}: the target registry's subject {subject:?} is {mode}, so no schema \
                 was written and no topic was created; Logweir does not override a registry's \
                 read-only mode. Lift it, or restore into another registry"
            ),
            Self::ModeNotRestored { detail, after } => {
                write!(
                    f,
                    "{MODE_NOT_RESTORED}: {detail}; set it back by hand. No topic was created"
                )?;
                if let Some(first) = after {
                    write!(f, ". The import had stopped first: {first}")?;
                }
                Ok(())
            }
        }
    }
}

fn register_body(v: &SnapshotVersion, keep: bool) -> Value {
    let mut body = json!({
        "schema": v.schema,
        "schemaType": v.schema_type,
        "references": v.references,
    });
    if keep {
        body["id"] = json!(v.id);
        body["version"] = json!(v.version);
    }
    body
}

fn transport(
    method: &str,
    path: &str,
    answer: Result<(u16, Value), String>,
) -> Result<(u16, Value), ImportRefusal> {
    match answer {
        Err(e) => Err(ImportRefusal::Unreachable(e)),
        Ok((status @ (401 | 403), _)) => Err(ImportRefusal::Unreachable(format!(
            "{method} {path} answered {status}: the registry refused the connection's registry \
             credential"
        ))),
        Ok((status, _)) if status >= 500 => Err(ImportRefusal::Unreachable(format!(
            "{method} {path} answered {status}"
        ))),
        Ok(ok) => Ok(ok),
    }
}

fn call(
    rest: &mut dyn Rest,
    method: &str,
    path: &str,
    body: Option<&Value>,
) -> Result<(u16, Value), ImportRefusal> {
    transport(method, path, rest.call(method, path, body))
}

/// Classify every closure entry against the target, writing nothing. An ID
/// the target holds is [`Class::Present`] only when the target's own lookup
/// of the snapshot schema under that subject answers the same ID and version
/// (so text normalisation does not matter); a subject the target holds may
/// hold only versions the snapshot has. Anything else is [`SCHEMA_CONFLICT`].
///
/// # Errors
///
/// [`ImportRefusal::Conflict`] or [`ImportRefusal::Unreachable`].
pub fn classify(
    rest: &mut dyn Rest,
    entries: &[&SnapshotVersion],
) -> Result<Vec<Class>, ImportRefusal> {
    let conflict = |v: &SnapshotVersion, detail: String| ImportRefusal::Conflict {
        subject: v.subject.clone(),
        version: v.version,
        id: v.id,
        detail,
    };
    let mut classes = Vec::with_capacity(entries.len());
    for v in entries {
        let s = encode(&v.subject);
        let path = format!("/schemas/ids/{}", v.id);
        let (status, _) = call(rest, "GET", &path, None)?;
        if status == 200 {
            let path = format!("/subjects/{s}?deleted=true");
            let (status, look) = call(rest, "POST", &path, Some(&register_body(v, false)))?;
            let same = status == 200
                && look.get("id").and_then(Value::as_u64) == Some(u64::from(v.id))
                && look.get("version").and_then(Value::as_u64) == Some(u64::from(v.version));
            if !same {
                return Err(conflict(
                    v,
                    format!(
                        "the target holds ID {} for another schema or subject-version",
                        v.id
                    ),
                ));
            }
            classes.push(Class::Present);
        } else if status == 404 {
            let path = format!("/subjects/{s}/versions/{}?deleted=true", v.version);
            let (status, _) = call(rest, "GET", &path, None)?;
            if status != 404 {
                return Err(conflict(
                    v,
                    format!(
                        "the target holds version {} of the subject under another ID",
                        v.version
                    ),
                ));
            }
            classes.push(Class::Import);
        } else {
            return Err(conflict(v, format!("GET {path} answered {status}")));
        }
    }
    // A subject the target holds holds only the snapshot's versions.
    let mut by_subject: BTreeMap<&str, BTreeSet<u32>> = BTreeMap::new();
    for v in entries {
        by_subject.entry(&v.subject).or_default().insert(v.version);
    }
    for (subject, versions) in by_subject {
        let path = format!("/subjects/{}/versions?deleted=true", encode(subject));
        let (status, held) = call(rest, "GET", &path, None)?;
        if status != 200 {
            continue;
        }
        let held: Vec<u32> = serde_json::from_value(held).unwrap_or_default();
        if let Some(extra) = held.iter().find(|h| !versions.contains(h)) {
            let first = entries
                .iter()
                .find(|v| v.subject == subject)
                .expect("the subject came from the entries");
            return Err(conflict(
                first,
                format!("the target's subject holds version {extra}, which the snapshot does not"),
            ));
        }
    }
    Ok(classes)
}

/// What an import wrote.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ImportReport {
    /// The subject-versions registered, in order.
    pub imported: Vec<ImportedVersion>,
    /// The entries the target already held.
    pub already_present: u64,
}

/// One registered subject-version.
#[derive(
    Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, schemars::JsonSchema,
)]
pub struct ImportedVersion {
    /// The subject.
    pub subject: String,
    /// The version.
    pub version: u32,
    /// The ID, the source's.
    pub id: u32,
}

/// The mode a subject had before the import touched it: its own
/// (subject-level) mode, or `None` when it inherits the registry's.
fn own_mode(rest: &mut dyn Rest, s: &str) -> Result<Option<String>, ImportRefusal> {
    let (status, mode) = call(rest, "GET", &format!("/mode/{s}"), None)?;
    Ok((status == 200)
        .then(|| mode.get("mode").and_then(Value::as_str).map(str::to_string))
        .flatten())
}

/// Put a subject's mode back to what it was: its own value, or, when it
/// inherited, no subject-level mode at all (`DELETE /mode/{s}`).
fn restore_mode(rest: &mut dyn Rest, subject: &str, before: Option<&str>) -> Result<(), String> {
    let s = encode(subject);
    let answer = match before {
        Some(mode) => rest.call(
            "PUT",
            &format!("/mode/{s}?force=true"),
            Some(&json!({ "mode": mode })),
        ),
        None => rest.call("DELETE", &format!("/mode/{s}"), None),
    };
    match answer {
        Ok((status, _)) if (200..300).contains(&status) => Ok(()),
        Ok((status, body)) => Err(format!(
            "subject {subject:?}'s mode could not be put back to {} (answered {status} {body})",
            before.unwrap_or("the registry's default")
        )),
        Err(e) => Err(format!(
            "subject {subject:?}'s mode could not be put back to {} ({e})",
            before.unwrap_or("the registry's default")
        )),
    }
}

/// Whether a mode forbids writes (`READONLY`, `READONLY_OVERRIDE`).
fn read_only(mode: &str) -> bool {
    matches!(mode, "READONLY" | "READONLY_OVERRIDE")
}

/// Register every [`Class::Import`] entry keeping its ID and version.
///
/// First, before any write, each subject to import is read for its mode: one
/// that is (or inherits) `READONLY` or `READONLY_OVERRIDE` is refused
/// [`READ_ONLY`]. Then per subject: `PUT /mode/{s}?force=true` `IMPORT` (and
/// `GET /mode/{s}` must answer `IMPORT`), each version with its `id` and
/// `version` (the answer's ID must be it), and after the subject's last entry
/// its mode is put back exactly: its own value, or no subject-level mode when
/// it inherited. A failure part-way puts every touched subject's mode back
/// too and leaves identical versions only, so a retry continues; a mode that
/// cannot be put back is said in the refusal ([`MODE_NOT_RESTORED`]).
///
/// # Errors
///
/// [`ImportRefusal::ReadOnly`], [`ImportRefusal::CannotKeepIds`],
/// [`ImportRefusal::ModeNotRestored`] or [`ImportRefusal::Unreachable`].
pub fn import(
    rest: &mut dyn Rest,
    entries: &[&SnapshotVersion],
    classes: &[Class],
) -> Result<ImportReport, ImportRefusal> {
    let mut report = ImportReport::default();
    let mut last: BTreeMap<&str, usize> = BTreeMap::new();
    for (i, (v, c)) in entries.iter().zip(classes).enumerate() {
        if *c == Class::Import {
            last.insert(&v.subject, i);
        } else {
            report.already_present += 1;
        }
    }
    if last.is_empty() {
        return Ok(report);
    }
    // Every subject's mode, before any write.
    let (status, global) = call(rest, "GET", "/mode", None)?;
    let global = (status == 200)
        .then(|| {
            global
                .get("mode")
                .and_then(Value::as_str)
                .map(str::to_string)
        })
        .flatten();
    let mut before: BTreeMap<&str, Option<String>> = BTreeMap::new();
    for subject in last.keys() {
        let own = own_mode(rest, &encode(subject))?;
        if let Some(mode) = own.as_deref().or(global.as_deref()) {
            if read_only(mode) {
                return Err(ImportRefusal::ReadOnly {
                    subject: (*subject).to_string(),
                    mode: mode.to_string(),
                });
            }
        }
        before.insert(subject, own);
    }
    let mut importing: BTreeSet<&str> = BTreeSet::new();
    let mut not_restored: Vec<String> = Vec::new();
    let result = (|| {
        for (i, (v, c)) in entries.iter().zip(classes).enumerate() {
            if *c != Class::Import {
                continue;
            }
            let s = encode(&v.subject);
            let cannot = |detail: String| ImportRefusal::CannotKeepIds {
                subject: v.subject.clone(),
                detail,
            };
            if !importing.contains(v.subject.as_str()) {
                let path = format!("/mode/{s}?force=true");
                let (status, _) = call(rest, "PUT", &path, Some(&json!({"mode": "IMPORT"})))?;
                if !(200..300).contains(&status) {
                    return Err(cannot(format!("PUT /mode answered {status}")));
                }
                importing.insert(&v.subject);
                let path = format!("/mode/{s}");
                let (status, mode) = call(rest, "GET", &path, None)?;
                if status != 200 || mode.get("mode").and_then(Value::as_str) != Some("IMPORT") {
                    return Err(cannot(format!(
                        "GET /mode answered {status} {mode}, not IMPORT"
                    )));
                }
            }
            let path = format!("/subjects/{s}/versions");
            let (status, answer) = call(rest, "POST", &path, Some(&register_body(v, true)))?;
            let kept = answer.get("id").and_then(Value::as_u64) == Some(u64::from(v.id));
            if status != 200 || !kept {
                return Err(cannot(format!(
                    "registering version {} with ID {} answered {status} {answer}",
                    v.version, v.id
                )));
            }
            report.imported.push(ImportedVersion {
                subject: v.subject.clone(),
                version: v.version,
                id: v.id,
            });
            if last.get(v.subject.as_str()) == Some(&i) {
                importing.remove(v.subject.as_str());
                if let Err(why) =
                    restore_mode(rest, &v.subject, before[v.subject.as_str()].as_deref())
                {
                    not_restored.push(why);
                }
            }
        }
        Ok(())
    })();
    // Every subject still in IMPORT after a failure gets its mode back too.
    for subject in importing {
        if let Err(why) = restore_mode(rest, subject, before[subject].as_deref()) {
            not_restored.push(why);
        }
    }
    match (result, not_restored.is_empty()) {
        (Ok(()), true) => Ok(report),
        (Ok(()), false) => Err(ImportRefusal::ModeNotRestored {
            detail: not_restored.join("; "),
            after: None,
        }),
        (Err(e), true) => Err(e),
        (Err(e), false) => Err(ImportRefusal::ModeNotRestored {
            detail: not_restored.join("; "),
            after: Some(Box::new(e)),
        }),
    }
}

/// The IDs a restore needs, collected from the restored records' first
/// bytes: every framed ID the snapshot holds, and, per topic side where at
/// least one in ten non-null records resolves, the framed IDs it does not
/// hold. An ID the snapshot lacks is never NEEDED, so a key that only looks
/// framed (a `LongSerializer` key) cannot flood the set.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct NeededIds {
    held: BTreeSet<u32>,
    found: BTreeSet<u32>,
    sides: BTreeMap<(String, bool), SideScan>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct SideScan {
    resolved: u64,
    non_null: u64,
    missing: BTreeSet<u32>,
}

impl NeededIds {
    /// A scan against the snapshot's IDs.
    #[must_use]
    pub fn new(held: BTreeSet<u32>) -> Self {
        Self {
            held,
            ..Self::default()
        }
    }

    /// One record side's first bytes (`key` true for the key); `None` is a
    /// null key or value.
    pub fn observe(&mut self, topic: &str, key: bool, payload: Option<&[u8]>) {
        let Some(bytes) = payload else { return };
        let side = self.sides.entry((topic.to_string(), key)).or_default();
        side.non_null = side.non_null.saturating_add(1);
        if let Some(id) = crate::schema_dependency::framed_schema_id(bytes) {
            if self.held.contains(&id) {
                side.resolved = side.resolved.saturating_add(1);
                self.found.insert(id);
            } else if side.missing.len() < LISTED {
                side.missing.insert(id);
            }
        }
    }

    /// Every framed ID found that the snapshot holds.
    #[must_use]
    pub fn needed(&self) -> &BTreeSet<u32> {
        &self.found
    }

    /// The framed IDs the snapshot lacks, on sides where at least one in ten
    /// non-null records resolves, sorted, at most [`LISTED`].
    #[must_use]
    pub fn not_in_snapshot(&self) -> Vec<u32> {
        let mut out = BTreeSet::new();
        for side in self.sides.values() {
            if side.resolved >= 1 && u128::from(side.resolved) * 10 >= u128::from(side.non_null) {
                out.extend(side.missing.iter().copied());
            }
        }
        out.into_iter().take(LISTED).collect()
    }
}

/// The scorecard's `target.schemas` block (scorecard 1.13.0, 2.5.0).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct ScorecardSchemas {
    /// Why nothing was imported, from [`NOT_IMPORTED_REASONS`]; present exactly
    /// when the run did not import (and then nothing below is).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub not_imported: Option<String>,
    /// The snapshot's digest, as the receipt names it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub snapshot_sha256: Option<String>,
    /// The target registry, as the plan names it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target_registry_url: Option<String>,
    /// The subject-versions registered, sorted, at most [`IMPORTED_LISTED`].
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub imported: Vec<ImportedVersion>,
    /// How many were registered.
    #[serde(default)]
    pub imported_count: u64,
    /// How many the target already held.
    #[serde(default)]
    pub already_present: u64,
    /// Framed IDs the restored records carry that the snapshot lacks (on
    /// sides where at least one in ten resolves), sorted, at most [`LISTED`].
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub ids_not_in_snapshot: Vec<u32>,
}

impl ScorecardSchemas {
    /// Nothing imported, for `reason`.
    #[must_use]
    pub fn not_imported(reason: &str) -> Self {
        Self {
            not_imported: Some(reason.to_string()),
            snapshot_sha256: None,
            target_registry_url: None,
            imported: Vec::new(),
            imported_count: 0,
            already_present: 0,
            ids_not_in_snapshot: Vec::new(),
        }
    }

    /// An import's evidence.
    #[must_use]
    pub fn imported(
        snapshot_sha256: &str,
        target_registry_url: &str,
        report: &ImportReport,
        ids_not_in_snapshot: Vec<u32>,
    ) -> Self {
        let mut imported = report.imported.clone();
        imported.sort();
        let count = imported.len() as u64;
        imported.truncate(IMPORTED_LISTED);
        Self {
            not_imported: None,
            snapshot_sha256: Some(snapshot_sha256.to_string()),
            target_registry_url: Some(target_registry_url.to_string()),
            imported,
            imported_count: count,
            already_present: report.already_present,
            ids_not_in_snapshot,
        }
    }
}

/// Arms SC-1 to SC-3 of the scorecard's `target.schemas`. `defined` is
/// whether the document's version defines it (1.13.0, 2.5.0 on).
/// `docs/verify_scorecard.py::_schemas_fault` mirrors each arm, in order, with
/// the same text.
///
/// # Errors
///
/// The first arm the block breaks.
pub fn check_scorecard_block(block: &ScorecardSchemas, defined: bool) -> Result<(), String> {
    // SC-1.
    if !defined {
        return Err(
            "target.schemas is present but format_version does not define it: the block is \
             defined from 1.13.0 of format 1 and from 2.5.0 of format 2"
                .to_string(),
        );
    }
    // SC-2. Not imported, with a reason from the closed set and nothing else;
    // or imported, naming the snapshot and the target registry.
    let shape_ok = match &block.not_imported {
        Some(reason) => {
            NOT_IMPORTED_REASONS.contains(&reason.as_str())
                && block.snapshot_sha256.is_none()
                && block.target_registry_url.is_none()
                && block.imported.is_empty()
                && block.imported_count == 0
                && block.already_present == 0
                && block.ids_not_in_snapshot.is_empty()
        }
        None => {
            block
                .snapshot_sha256
                .as_deref()
                .is_some_and(crate::check_contract::is_sha256_prefixed)
                && block
                    .target_registry_url
                    .as_deref()
                    .is_some_and(|u| !u.trim().is_empty())
        }
    };
    if !shape_ok {
        return Err(
            "target.schemas carries not_imported (noTargetRegistry or noSnapshot) and nothing \
             else, or names the snapshot's sha256 and the target registry"
                .to_string(),
        );
    }
    // SC-3. The lists are bounded, sorted and distinct, and `imported` is the
    // first entries of `imported_count`.
    let listed = usize::try_from(block.imported_count)
        .unwrap_or(usize::MAX)
        .min(IMPORTED_LISTED);
    if block.imported.len() != listed
        || !sorted_distinct(&block.imported)
        || block.ids_not_in_snapshot.len() > LISTED
        || !sorted_distinct(&block.ids_not_in_snapshot)
        || block
            .ids_not_in_snapshot
            .iter()
            .any(|id| *id == 0 || *id > crate::schema_dependency::MAX_SCHEMA_ID)
    {
        return Err(format!(
            "target.schemas.imported lists the first {IMPORTED_LISTED} of imported_count, \
             sorted and distinct, and ids_not_in_snapshot at most {LISTED} IDs from 1 to \
             16777215, sorted and distinct"
        ));
    }
    Ok(())
}
