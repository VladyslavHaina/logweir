//! Phase 7's half of position translation (PROD-04.2, design §2): the bound
//! point's captured positions, probed in the complete verification's own
//! in-order scan of each restored partition, and mapped by
//! `logweir_core::positions_mapping` once the run is scored.
//!
//! Nothing is read twice: [`Probes`] tells the complete lane which source
//! partitions carry a captured position, the lane feeds each restored record's
//! lineage to a [`Prober`] while it compares it, and collects the archived
//! offsets its own selection excluded while it reads the segments. The
//! mapping is written only over a passing complete verification
//! (`crate::drill`, after phase 8 decides the outcome).

use logweir_core::consumer_positions::{ConsumerPositions, PositionsDocument};
use logweir_core::positions_mapping::{self, MappingDocument, PartitionScan, Prober};
use std::collections::{BTreeMap, BTreeSet};

/// Per SOURCE partition (topic, partition), the captured positions to probe.
pub type Probes = BTreeMap<(String, u32), BTreeSet<i64>>;

/// Per RESTORED source partition, what the scan established.
pub type Scans = BTreeMap<(String, u32), PartitionScan>;

/// The bound point's verified consumer positions: the receipt's block and the
/// document it binds, checked against it (arms CP-1 to CP-14) by
/// `binding::fetch_positions`.
#[derive(Debug, Clone)]
pub struct BoundPositions {
    pub point_id: String,
    pub receipt: ConsumerPositions,
    pub document: PositionsDocument,
    /// The document's digest, as the receipt binds it.
    pub sha256: String,
}

impl BoundPositions {
    /// What the complete lane must probe.
    #[must_use]
    pub fn probes(&self) -> Probes {
        positions_mapping::probes(&self.document)
    }

    /// The mapping document of this run.
    #[must_use]
    pub fn map(
        &self,
        run_id: &str,
        topic_mapping: &BTreeMap<String, String>,
        scans: &Scans,
        transactional: bool,
    ) -> MappingDocument {
        positions_mapping::build(&positions_mapping::MapInputs {
            run_id,
            point_id: &self.point_id,
            positions_sha256: &self.sha256,
            receipt: &self.receipt,
            document: &self.document,
            topic_mapping,
            scans,
            transactional,
        })
    }
}

/// One partition's probe while the complete lane reads it: the excluded
/// archived offsets, and the prober the target records are fed to.
pub(super) struct PartitionProbe {
    pub(super) prober: Prober,
    pub(super) excluded: Vec<i64>,
}

impl PartitionProbe {
    pub(super) fn new(positions: &BTreeSet<i64>) -> Self {
        Self {
            prober: Prober::new(positions.iter().copied()),
            excluded: Vec::new(),
        }
    }

    /// The scan, once the partition was compared.
    pub(super) fn finish(
        mut self,
        target_topic: &str,
        high_watermark: i64,
        gaps: Vec<(i64, i64)>,
    ) -> PartitionScan {
        self.excluded.sort_unstable();
        PartitionScan {
            target_topic: target_topic.to_string(),
            high_watermark,
            found: self.prober.finish(),
            excluded: self.excluded,
            gaps,
        }
    }
}

/// **The bound point's consumer positions**, read back from the archive:
/// the receipt at the plan's `source.point.receipt_key`, whose bytes must hash
/// to the approved `receipt_sha256` (the same bytes `binding::
/// verify_point_binding` verified the signature of before any phase ran), and
/// the positions document its `consumer_positions.document` names, checked by
/// arms CP-1 to CP-14. Read under the runner's signed-document cap
/// (`caps::SIGNED_DOCUMENT`, 64 MiB): the receipt's signature binds the
/// document's length, and the largest document PROD-04.1 measured is 2 MB.
///
/// `None` for a plan bound to no point, and for a point that captured no
/// group. A document that cannot be read or does not verify is `None` with a
/// warning: the restore is unaffected, no mapping is written, and a
/// `Switchover` over it is refused `NoPositionsMapping`.
///
/// **Both keys are confined before they are read (FX-36).** The receipt key
/// by the binding's own rule (`binding::confine_receipt_key`: a receipt of the
/// plan's own set), and the document key to the one beside that receipt
/// (`consumer_positions::document_key` of the receipt's set and run, arm 32's
/// rule, held here before the read rather than after it).
#[must_use]
pub fn fetch(
    plan: &logweir_core::spec::DrillSpec,
    archive: &logweir_engine_oso::storage::Store,
) -> Option<BoundPositions> {
    use logweir_engine_oso::storage::caps;
    let point = plan.source.point.as_ref()?;
    let warn = |why: String| {
        tracing::warn!(point_id = %point.point_id, "consumer positions not mapped: {why}");
    };
    if let Err(e) = crate::drill::binding::confine_receipt_key(plan, point) {
        warn(format!("the receipt key is not read: {e}"));
        return None;
    }
    let receipt_bytes = match archive.get_capped(&point.receipt_key, caps::SIGNED_DOCUMENT) {
        Ok((bytes, _)) => bytes,
        Err(e) => {
            warn(format!("the receipt could not be read again: {e}"));
            return None;
        }
    };
    if logweir_core::ids::sha256_prefixed(&receipt_bytes) != point.receipt_sha256 {
        warn("the receipt read again is not the bound receipt".to_string());
        return None;
    }
    let receipt: logweir_core::backup_receipt::BackupReceipt =
        match serde_json::from_slice(&receipt_bytes) {
            Ok(r) => r,
            Err(e) => {
                warn(format!("the receipt does not parse: {e}"));
                return None;
            }
        };
    let cp = receipt.consumer_positions.clone()?;
    let beside =
        logweir_core::consumer_positions::document_key(&receipt.backup_id, &receipt.run_id);
    if cp.document.key != beside {
        warn(format!(
            "the receipt names the positions document {:?}, which is not the one beside it \
             ({beside}); it is not read",
            cp.document.key
        ));
        return None;
    }
    let doc_bytes = match archive.get_capped(&cp.document.key, caps::SIGNED_DOCUMENT) {
        Ok((bytes, _)) => bytes,
        Err(e) => {
            warn(format!(
                "the positions document {} could not be read: {e}",
                cp.document.key
            ));
            return None;
        }
    };
    let document: PositionsDocument = match serde_json::from_slice(&doc_bytes) {
        Ok(d) => d,
        Err(e) => {
            warn(format!("the positions document does not parse: {e}"));
            return None;
        }
    };
    if let Err(e) = receipt.validate_consumer_positions_document(&doc_bytes, &document) {
        warn(format!("the positions document does not verify: {e}"));
        return None;
    }
    Some(BoundPositions {
        point_id: point.point_id.clone(),
        sha256: cp.document.sha256.clone(),
        receipt: cp,
        document,
    })
}

#[cfg(test)]
mod tests {
    //! FX-36's rows for the second read of the bound receipt and the read of
    //! the positions document it names: nothing outside the plan's own set,
    //! and nothing but the document beside the receipt, is read.
    use super::*;
    use crate::drill::binding::tests::{plan_with, receipt};
    use logweir_core::consumer_positions::{ConsumerPositions, DocumentRef};
    use logweir_core::execution_contract::PointBinding;
    use logweir_engine_oso::storage::Store;

    const RECEIPT_KEY: &str = "logweir/backups/nightly-7/run-1.receipt.json";
    const BESIDE: &str = "logweir/backups/nightly-7/run-1.consumer-positions.json";

    /// A receipt of `nightly-7` run `run-1` whose consumer-positions block
    /// names `document_key`, stored at [`RECEIPT_KEY`], with an object at
    /// `document_key` and at [`BESIDE`] (bytes that are not a document: the
    /// rows are about which keys are READ, and a read document that does not
    /// verify is `None` too), and the plan's binding to it.
    fn archive(document_key: &str) -> (Store, PointBinding) {
        let store = Store::in_memory("logweir");
        let mut r = receipt("logweir/nightly-7/manifest.json", "sha256:00");
        let at = chrono::DateTime::<chrono::Utc>::from_timestamp(1_800_000_000, 0).expect("t");
        r.consumer_positions = Some(ConsumerPositions {
            observed_from: at,
            observed_to: at,
            listing: "complete".into(),
            document: DocumentRef {
                key: document_key.into(),
                sha256: format!("sha256:{}", "a".repeat(64)),
                bytes: 2,
            },
            groups: Default::default(),
        });
        let bytes = serde_json::to_vec(&r).expect("serialises");
        store.put_create_only(RECEIPT_KEY, &bytes).expect("written");
        for key in [document_key, BESIDE] {
            let _ = store.put_create_only(key, b"{}");
        }
        let binding = PointBinding {
            point_id: crate::catalog::record::point_id(&bytes),
            receipt_key: RECEIPT_KEY.into(),
            receipt_sha256: logweir_core::ids::sha256_prefixed(&bytes),
            manifest_sha256: "sha256:00".into(),
        };
        (store, binding)
    }

    /// **The receipt key is confined before the second read, as the binding
    /// confines it before the first.** A plan whose key is not a receipt of
    /// its own set reads nothing here; the CONTROL, the writer's key, reads
    /// the receipt and then the document beside it.
    ///
    /// KILLS: the `confine_receipt_key` call removed from `fetch` (the
    /// planted copy is read).
    #[test]
    fn a_receipt_key_outside_the_plans_own_set_is_not_read_again() {
        let (store, binding) = archive(BESIDE);
        let elsewhere = "logweir/elsewhere/run-1.receipt.json";
        let bytes = store
            .get_capped(
                RECEIPT_KEY,
                logweir_engine_oso::storage::caps::SIGNED_DOCUMENT,
            )
            .expect("the receipt")
            .0;
        store.put_create_only(elsewhere, &bytes).expect("a copy");
        let (logged, log) = store.logging_reads();
        let mut outside = binding.clone();
        outside.receipt_key = elsewhere.into();
        assert!(fetch(&plan_with(Some(outside)), &logged).is_none());
        assert!(log.reads().is_empty(), "{:?}", log.reads());

        // CONTROL: the writer's key is read, and so is the document beside it.
        assert!(fetch(&plan_with(Some(binding)), &logged).is_none());
        assert_eq!(
            log.reads(),
            [format!("get {RECEIPT_KEY}"), format!("get {BESIDE}")]
        );
    }

    /// **The positions document is read only at the key beside its
    /// receipt.** A receipt (whose bytes are the bound ones) naming any other
    /// key — another set's document, another prefix, a scorecard — has that
    /// key left unread, and no mapping is made.
    ///
    /// KILLS: the `document_key` comparison removed (the named key is read).
    #[test]
    fn a_positions_document_not_beside_its_receipt_is_not_read() {
        for named in [
            "logweir/backups/nightly-8/run-1.consumer-positions.json",
            "logweir/backups/nightly-7/run-2.consumer-positions.json",
            "logweir/tenant-b/backups/nightly-7/run-1.consumer-positions.json",
            "logweir/drills/run-1.json",
        ] {
            let (store, binding) = archive(named);
            let (logged, log) = store.logging_reads();
            assert!(
                fetch(&plan_with(Some(binding)), &logged).is_none(),
                "{named}"
            );
            assert_eq!(log.reads(), [format!("get {RECEIPT_KEY}")], "{named}");
        }
    }
}
