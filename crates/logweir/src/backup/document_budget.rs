//! **FX-33 — the largest receipt and catalog record this run could sign,
//! measured BEFORE the engine runs.**
//!
//! # Why a count is not enough
//!
//! Phase −1 refuses a selection of more than
//! `topic_budget::MAX_BACKUP_TOPICS` names with no I/O. That bounds the
//! receipt only while each topic stays inside its per-topic budget, and a
//! topic's cost is the SOURCE's: every explicit override is recorded, with
//! its value, and a name is written six times. A selection inside the count
//! whose topics carry twenty overrides each would sign a receipt no catalog
//! walk lists and no controller verifies — and it would do so after the
//! archive exists. So the bytes are measured here, from Logweir's own
//! configuration read (the same read the receipt records), and the run is
//! refused by name before the engine writes anything.
//!
//! # How it is an upper bound
//!
//! The measure is [`phase_run::build_receipt`] and
//! [`crate::catalog::writer::project`] themselves, over a
//! [`BackupOutcome`] in which everything already known is the run's own and
//! everything the engine has yet to decide is at its LONGEST:
//!
//! | field | known before the engine | projected as |
//! |---|---|---|
//! | topics, source, engine, storage, owners, the detection sources | yes | themselves |
//! | each topic's recorded configuration entries and timestamp type | yes (`config_coverage::observe`) | themselves |
//! | `config_coverage` | the read's outcome | `classify` against an EMPTY manifest: a read topic is `notCaptured`/`manifestDiffers`, the longest pair |
//! | partition count, replication factor | no (the manifest) | `u32::MAX` |
//! | `records` | no | `u64::MAX` |
//! | `schema_dependency` | no | the longest entry the receipt's arms admit: `schemaDependent`, both sides with `u64::MAX` counts and 16 ids of the largest id |
//! | `generations` | the read before the engine is taken after this | the longest entry the arms admit: one ID with its source, the other `null` with the longest reason |
//! | manifest key and version id | no | 1,024 bytes each (an S3 key's and a version id's limit) |
//! | instants, exit code, covered window | no | nanosecond instants, `i32::MIN`, `i64::MIN` |
//! | `consumer_positions` | the selection | `consumer_positions::worst_case_block` of the selection |
//!
//! `tests/topic_budget.rs::no_real_receipt_is_larger_than_its_projection`
//! holds the bound: real outcomes of every shape are never larger than what
//! this projects for them. Because the measure IS the builders, a block added
//! to the receipt is measured the day it is added — and must be given its
//! longest value here, or that row fails.
//!
//! The projection is a SIZE and never a document: it is not signed, not put
//! and not validated (its schema block would not pass arm 26).

use std::collections::BTreeMap;

use chrono::{DateTime, Utc};
use logweir_core::backup_receipt::{SideFraming, TopicIdentity, TopicOwner, TopicSchemaDependency};
use logweir_core::engine::{AuthRender, BackupFacts, EngineId, StorageUrl};

use super::config_coverage::{self, Observation};
use super::{phase_run, BackupOutcome};

/// What one run's signed documents could weigh at most.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Projected {
    /// The receipt, in bytes of the encoding it is signed in.
    pub receipt_bytes: u64,
    /// The catalog point record, likewise.
    pub record_bytes: u64,
}

/// Everything about a run that is known before its engine starts.
pub struct Known<'a> {
    pub backup_id: &'a str,
    pub run_id: &'a str,
    pub triggered_by: &'a str,
    pub source_cluster_id: &'a str,
    pub bootstrap_servers: &'a [String],
    /// The named topics, as the plan lists them.
    pub topics: &'a [String],
    pub engine: &'a EngineId,
    pub storage: &'a StorageUrl,
    pub source_auth: &'a AuthRender,
    /// Logweir's own configuration read, one entry per named topic.
    pub observed: &'a BTreeMap<String, Observation>,
    pub owners: &'a BTreeMap<String, TopicOwner>,
    pub owner_detection: &'a [String],
    /// The consumer groups the run selected; empty selects none.
    pub selected_groups: &'a [String],
    /// The key that will sign the record.
    pub signing: crate::catalog::RecordSigning,
}

/// The longest object key, and the longest version id, an S3 store carries.
const LONGEST_KEY_BYTES: usize = 1024;

/// An instant as long as any a run records: nine fraction digits.
fn longest_instant() -> DateTime<Utc> {
    DateTime::<Utc>::from_timestamp(i64::from(i32::MAX), 999_999_999).unwrap_or_default()
}

/// One side of a schema dependency entry at its longest: every count at
/// `u64::MAX` and as many ids as the receipt lists, each the largest id the
/// receipt's arm 27 admits.
fn longest_side() -> SideFraming {
    SideFraming {
        dependent: false,
        framed: u64::MAX,
        unframed: u64::MAX,
        nulls: u64::MAX,
        schema_ids: vec![
            logweir_core::schema_dependency::MAX_SCHEMA_ID;
            logweir_core::schema_dependency::SCHEMA_IDS_LISTED
        ],
        schema_id_count: u64::MAX,
    }
}

/// The longest of `candidates` as the receipt encodes them.
fn longest<T: serde::Serialize + Clone>(candidates: &[T]) -> T {
    candidates
        .iter()
        .max_by_key(|c| serde_json::to_string_pretty(c).map_or(0, |text| text.len()))
        .cloned()
        .expect("every caller passes at least one candidate")
}

/// The longest schema dependency entry the receipt's arms admit (24 and 25):
/// a judged entry carries a basis and both sides and no reason; a
/// `notAssessed` one a reason and nothing else. Both shapes are built and the
/// longer taken, so a vocabulary that grows is measured, not assumed.
fn longest_schema_dependency() -> TopicSchemaDependency {
    use logweir_core::schema_dependency as sd;
    longest(&[
        TopicSchemaDependency {
            verdict: sd::SCHEMA_DEPENDENT.to_string(),
            reason: None,
            basis: Some(longest_of(&sd::BASES).to_string()),
            key: Some(longest_side()),
            value: Some(longest_side()),
        },
        TopicSchemaDependency {
            verdict: sd::NOT_ASSESSED.to_string(),
            reason: Some(longest_of(&sd::NOT_ASSESSED_REASONS).to_string()),
            basis: None,
            key: None,
            value: None,
        },
    ])
}

/// The longest `generations` entry the receipt's arms admit (39 and 40): an
/// ID carries no reason and a `null` one carries exactly one, and the source
/// is present exactly when an ID is. All four shapes are built and the
/// longest taken.
fn longest_identity() -> TopicIdentity {
    use logweir_core::topic_identity as ti;
    let id = || Some("A".repeat(ti::TOPIC_ID_TEXT_LEN));
    let reason = || Some(longest_of(&ti::TOPIC_ID_REASONS).to_string());
    let source = || Some(longest_of(&ti::TOPIC_ID_SOURCES).to_string());
    longest(&[
        TopicIdentity {
            topic_id: id(),
            topic_id_after: id(),
            topic_id_source: source(),
            topic_id_reason: None,
            topic_id_after_reason: None,
        },
        TopicIdentity {
            topic_id: id(),
            topic_id_after: None,
            topic_id_source: source(),
            topic_id_reason: None,
            topic_id_after_reason: reason(),
        },
        TopicIdentity {
            topic_id: None,
            topic_id_after: id(),
            topic_id_source: source(),
            topic_id_reason: reason(),
            topic_id_after_reason: None,
        },
        TopicIdentity {
            topic_id: None,
            topic_id_after: None,
            topic_id_source: None,
            topic_id_reason: reason(),
            topic_id_after_reason: reason(),
        },
    ])
}

/// The longest word of a closed set.
fn longest_of<'a>(words: &[&'a str]) -> &'a str {
    words.iter().copied().max_by_key(|w| w.len()).unwrap_or("")
}

/// One entry per named topic, each `value`.
fn every_topic<T: Clone>(topics: &[String], value: T) -> BTreeMap<String, T> {
    topics.iter().map(|t| (t.clone(), value.clone())).collect()
}

/// The outcome of this run with every undecided field at its longest — what
/// [`project`] measures. `pub` for the row that holds it above real outcomes.
#[must_use]
pub fn longest_outcome(known: &Known<'_>) -> BackupOutcome {
    let at = longest_instant();
    let layouts: BTreeMap<String, config_coverage::Layout> = known
        .topics
        .iter()
        .map(|t| (t.clone(), (Some(i32::MAX), Some(i16::MAX))))
        .collect();
    let keys = phase_run::receipt_keys(known.backup_id, known.run_id);
    let (consumer_positions, consumer_positions_document) = if known.selected_groups.is_empty() {
        (None, None)
    } else {
        (
            Some(logweir_core::consumer_positions::worst_case_block(
                known.selected_groups,
            )),
            None,
        )
    };
    BackupOutcome {
        backup_id: known.backup_id.to_string(),
        run_id: known.run_id.to_string(),
        requested_at: at,
        triggered_by: known.triggered_by.to_string(),
        source_cluster_id: known.source_cluster_id.to_string(),
        bootstrap_servers: known.bootstrap_servers.to_vec(),
        topics: known.topics.to_vec(),
        engine: known.engine.clone(),
        archive_prefix: known.storage.prefix().to_string(),
        storage: known.storage.clone(),
        source_auth: known.source_auth.clone(),
        manifest_key: "k".repeat(LONGEST_KEY_BYTES),
        manifest_sha256: format!("sha256:{}", "f".repeat(64)),
        manifest_version_id: Some("v".repeat(LONGEST_KEY_BYTES)),
        records_per_topic: every_topic(known.topics, u64::MAX),
        covered_from_ms: i64::MIN,
        covered_to_ms: i64::MIN,
        // Against an EMPTY manifest every read topic is `notCaptured` with
        // `manifestDiffers`, the longest pair the block can hold for it; a
        // denied or failed read is what it already is.
        config_coverage: config_coverage::classify(known.observed, &BTreeMap::new()),
        topic_configuration: config_coverage::model(
            known.observed,
            &layouts,
            &every_topic(known.topics, u32::MAX),
            known.owners,
        ),
        owner_detection: known.owner_detection.to_vec(),
        schema_dependency: every_topic(known.topics, longest_schema_dependency()),
        generations: every_topic(known.topics, longest_identity()),
        consumer_positions,
        consumer_positions_document,
        facts: BackupFacts {
            started_at: at,
            finished_at: at,
            exit_code: i32::MIN,
            unknown_key_warnings: Vec::new(),
        },
        receipt_key: keys.receipt_key,
        sidecar_key: keys.sidecar_key,
        receipt_sha256: format!("sha256:{}", "f".repeat(64)),
        catalog_key: None,
    }
}

/// The largest receipt and catalog record this run could sign.
///
/// # Errors
///
/// A document that does not serialise (it always does).
pub fn project(known: &Known<'_>) -> Result<Projected, String> {
    let outcome = longest_outcome(known);
    let receipt = phase_run::build_receipt(&outcome);
    let receipt_bytes = logweir_core::det_json::to_deterministic_json(&receipt)
        .map_err(|e| format!("the projected receipt could not be serialised: {e}"))?;
    let inputs = crate::catalog::writer::RecordInputs {
        receipt_key: outcome.receipt_key.clone(),
        sidecar_key: outcome.sidecar_key.clone(),
        location_id: crate::catalog::record::location_id(known.storage),
        recorded_at: longest_instant(),
        signing: known.signing.clone(),
        installation: Some(crate::catalog::RecordInstallation {
            key_id: known.signing.key_id.clone(),
        }),
        execution: None,
    };
    let record =
        crate::catalog::writer::project(&receipt, &receipt_bytes, &inputs)?.canonical_bytes()?;
    Ok(Projected {
        receipt_bytes: receipt_bytes.len() as u64,
        record_bytes: record.len() as u64,
    })
}

/// Why this run is refused before its engine starts, or `None`: the receipt
/// or the record it could sign is over its bound
/// (`topic_budget::refuse_document_bytes`). A projection that could not be
/// computed refuses too: an unmeasured run is not an admitted one.
#[must_use]
pub fn refusal(known: &Known<'_>) -> Option<String> {
    match project(known) {
        Ok(p) => logweir_core::topic_budget::refuse_document_bytes(
            known.topics.len(),
            p.receipt_bytes,
            p.record_bytes,
        ),
        Err(e) => Some(format!(
            "{}: the size of this run's receipt could not be measured before the engine ({e}), \
             so the run is not started",
            logweir_core::topic_budget::SELECTION_TOO_LARGE
        )),
    }
}
