//! Building a point record from a signed receipt, and putting it — create
//! only, never rewritten (D3 §5.2 rule 4).

use super::record::*;
use logweir_core::backup_receipt::BackupReceipt;
use logweir_engine_oso::storage::{Store, StoreError};

/// Everything `from_receipt` needs that is NOT in the receipt.
///
/// A struct rather than eight positional arguments, because six of them are
/// `String` and a transposition would compile.
#[derive(Debug, Clone)]
pub struct RecordInputs {
    /// `logweir/backups/<backup_id>/<run_id>.receipt.json`
    pub receipt_key: String,
    /// `logweir/backups/<backup_id>/<run_id>.receipt.sig`
    pub sidecar_key: String,
    /// [`location_id`] of the archive this backup wrote to.
    pub location_id: String,
    /// When the RECORD is written. Measured by the caller: this module reads
    /// no clock.
    pub recorded_at: chrono::DateTime<chrono::Utc>,
    /// The key that will sign the record.
    pub signing: RecordSigning,
    /// The key under which the writer VERIFIED the receipt — the installation
    /// that produced the backup. `None` when the writer has no verified
    /// signer identity to record, which the reader reports as unknown.
    pub installation: Option<RecordInstallation>,
    /// `None` on this build: a Backup Job carries no execution-contract
    /// environment, so provenance is unknown rather than defaulted. See
    /// [`RecordExecution`].
    pub execution: Option<RecordExecution>,
}

/// Receipt + inputs -> the record. **A pure projection**: every field is a
/// value one of the two arguments already carries.
///
/// `receipt_bytes` must be the EXACT stored bytes — they are what the identity
/// and the digest are taken over, and a re-serialisation of `receipt` would
/// produce a different point for the same backup.
///
/// The receipt's own invariants are checked FIRST, for the reason
/// `phase_run::persist_receipt`'s step 1 gives about the receipt: no signed
/// document may carry a self-contradicting claim, and arm 3 (one `records`
/// entry per named topic and no others) is what makes the `topics` projection
/// below total rather than lossy.
pub fn from_receipt(
    receipt: &BackupReceipt,
    receipt_bytes: &[u8],
    inputs: &RecordInputs,
) -> Result<CatalogPoint, String> {
    receipt.validate_invariants().map_err(|e| {
        format!(
            "the backup receipt at {} violates its own invariants, so no catalog point was \
             derived from it: {e}",
            inputs.receipt_key
        )
    })?;
    // The topic rows come from `records`, which arm 3 has just established is
    // exactly the named topic set — so this projection needs no fallback and
    // cannot invent a count. `BTreeMap` order is the topic names' own order,
    // which makes two writers over one receipt produce identical bytes.
    let topics = receipt
        .records
        .iter()
        .map(|(name, records)| RecordTopic {
            name: name.clone(),
            // UNKNOWN, and that is the honest value: a backup receipt records
            // no partition count at all. See `RecordTopic::partitions`.
            partitions: None,
            records: *records,
        })
        .collect();
    Ok(CatalogPoint {
        format_version: FORMAT_VERSION.to_string(),
        point_id: point_id(receipt_bytes),
        recorded_at: inputs.recorded_at,
        receipt: RecordReceipt {
            key: inputs.receipt_key.clone(),
            sha256: logweir_core::ids::sha256_prefixed(receipt_bytes),
            sidecar_key: inputs.sidecar_key.clone(),
            payload_type: logweir_evidence::PAYLOAD_TYPE_BACKUP_RECEIPT.to_string(),
        },
        backup_id: receipt.backup_id.clone(),
        run_id: receipt.run_id.clone(),
        archive: RecordArchive {
            location_id: inputs.location_id.clone(),
            manifest_key: receipt.archive.manifest_key.clone(),
            manifest_sha256: receipt.archive.manifest_sha256.clone(),
            prefix: receipt.archive.prefix.clone(),
        },
        covered: RecordCovered {
            from_ms: receipt.covered.from_ms,
            to_ms: receipt.covered.to_ms,
        },
        capture: RecordCapture {
            started_at: receipt.started_at,
            finished_at: receipt.finished_at,
        },
        topics,
        source: RecordSource {
            cluster_id: receipt.source.cluster_id.clone(),
            bootstrap_servers: receipt.source.bootstrap_servers.clone(),
            auth_mode: receipt.source.auth.mode.clone(),
        },
        execution: inputs.execution.clone(),
        signing: inputs.signing.clone(),
        installation: inputs.installation.clone(),
    })
}

/// The three keys one point occupies.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Written {
    pub point_id: String,
    pub record_key: String,
    pub sidecar_key: String,
    pub log_key: String,
}

/// What a put attempt established, per object.
///
/// `Created` and `AlreadyPresent` are BOTH successes and are deliberately
/// distinguishable: repeated import is idempotent (D3 §5.5 step 2) because
/// identities are content-derived, and a second sync of one archive must
/// report "already there" rather than either failing or claiming to have
/// written something.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PutState {
    Created,
    AlreadyPresent,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WriteOutcome {
    pub keys: Written,
    pub record: PutState,
    pub sidecar: PutState,
    pub log: PutState,
}

impl WriteOutcome {
    /// True when this call actually added the record object. Used for the
    /// `catalog-key=` line and for the sync summary's `written` count.
    #[must_use]
    pub fn created_record(&self) -> bool {
        self.record == PutState::Created
    }
}

/// Validate, serialise, sign, and put the three objects — **create-only, in
/// that order**, under `logweir/catalog/v1/` and nowhere else.
///
/// # The order, and why an existing object is not an error
///
/// 1. **Self-consistency**, before anything is signed: `point_id` must be the
///    one `receipt.sha256` implies. It is the one claim in this document a
///    reader cannot recompute without fetching the receipt, and a record whose
///    id and digest disagree would make every lookup by id return a document
///    about some other backup.
/// 2. **Serialise once** — the exact bytes that are signed, stored and
///    digested. Never a re-serialisation afterwards.
/// 3. **Sign** with the run's already-validated signer. One signer per run:
///    this is a call into `crates/logweir/src/signer.rs`'s handle, exactly as
///    the receipt's signing is, and no signing primitive lives here
///    (`scripts/check-one-signer.sh`).
/// 4. **Put** record, sidecar, log entry — each `PutMode::Create`. A
///    `StoreError::AlreadyExists` is reported as [`PutState::AlreadyPresent`]
///    and is NOT an error: `logweir/` objects are never rewritten (rule 4), so
///    "it is already there" is the correct outcome of re-running a backfill
///    over an archive that already has its records. A record that DISAGREES
///    with this one is not silently accepted by that path either — it is the
///    reader's `RecordMismatch`, which is where the comparison belongs,
///    because only a reader has both documents.
///
/// The store handle must be the writable evidence handle over `logweir/`;
/// `Store::put_create_only` asserts the root itself, so a mutant that pointed
/// this at the archive aborts inside the store rather than writing there.
pub fn put_point(
    point: &CatalogPoint,
    entry: &CatalogLogEntry,
    signer: &crate::signer::ValidatedSigner,
    evidence: &Store,
) -> Result<WriteOutcome, String> {
    let expected = point
        .receipt
        .sha256
        .strip_prefix("sha256:")
        .filter(|hex| hex.len() >= 32)
        .map(|hex| format!("{POINT_ID_PREFIX}{}", &hex[..32]));
    if expected.as_deref() != Some(point.point_id.as_str()) {
        return Err(format!(
            "refusing to sign a catalog point whose id `{}` is not the one its receipt digest \
             `{}` implies ({}). The id is the digest's display form (D3 §5.1); a record where \
             they disagree would answer a lookup with a different backup's window.",
            point.point_id,
            point.receipt.sha256,
            expected.as_deref().unwrap_or("<not a sha256: digest>")
        ));
    }
    let bytes = point.canonical_bytes()?;
    let entry_bytes = entry.canonical_bytes()?;
    let sidecar = signer.sign(super::PAYLOAD_TYPE_CATALOG_POINT, &bytes)?;
    let sidecar_bytes = serde_json::to_vec(&sidecar).map_err(|e| format!("DSSE sidecar: {e}"))?;

    let keys = Written {
        point_id: point.point_id.clone(),
        record_key: record_key(&point.point_id),
        sidecar_key: record_sidecar_key(&point.point_id),
        log_key: point.log_key(),
    };
    let record = create_only(evidence, &keys.record_key, &bytes)?;
    let sidecar = create_only(evidence, &keys.sidecar_key, &sidecar_bytes)?;
    let log = create_only(evidence, &keys.log_key, &entry_bytes)?;
    Ok(WriteOutcome {
        keys,
        record,
        sidecar,
        log,
    })
}

/// One create-only put, with `AlreadyExists` folded into a SUCCESS state and
/// every other failure kept as an error.
///
/// Folding the two would be the defect this whole layer is about: "could not
/// tell" (a 403, a timeout) reported as "already there" would make a sync
/// silently skip points it never managed to look at.
fn create_only(store: &Store, key: &str, bytes: &[u8]) -> Result<PutState, String> {
    match store.put_create_only(key, bytes) {
        Ok(_) => Ok(PutState::Created),
        Err(StoreError::AlreadyExists(_)) => Ok(PutState::AlreadyPresent),
        Err(e) => Err(format!("{key}: {e}")),
    }
}
