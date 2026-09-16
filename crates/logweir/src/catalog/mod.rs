//! **The durable recovery catalog (PLAT-15.1, decision D3 §5).**
//!
//! A recovery point is a thing an operator needs long after the `Backup`
//! object that produced it is gone: after a namespace is deleted, after a
//! cluster is rebuilt, on a fresh installation that never saw the CR. Today
//! the only durable record of one is the signed backup receipt, keyed by
//! `backup_id`, which carries no ordering and no index. This module is the
//! durable, append-only, create-only metadata that turns those receipts into
//! a catalog:
//!
//! ```text
//! logweir/catalog/v1/points/<pointId>/record.json   # the signed point record
//! logweir/catalog/v1/points/<pointId>/record.sig    # its detached DSSE sidecar
//! logweir/catalog/v1/log/<yyyy>/<mm>/<dd>/<recoveryPointAtMs:013>-<pointId>.json
//! ```
//!
//! # The four things that make this safe to build on
//!
//! 1. **Identity is content-derived** ([`record::point_id`]): `lwp1-` plus 32
//!    hex of `sha256(receipt bytes)`. Anyone holding the receipt computes it;
//!    nobody can forge one point's identity into another's without breaking
//!    the receipt signature; the same archive copied to a second bucket is ONE
//!    point in two places. It is also the answer to tracker defect
//!    **RECEIPT-DUP** — two receipts under one execution id become two points
//!    sharing a `backup_id`, instead of one overwriting the other.
//! 2. **The receipt's signature is the verification root** (D3 §5.2 rule 3).
//!    This record is a convenience index over evidence that already exists; a
//!    valid signature on a record proves who wrote the record and nothing at
//!    all about whether the archive is there. [`reader::cross_check`]
//!    recomputes every receipt-derived fact and reports a disagreeing record
//!    rather than believing it.
//! 3. **Nothing under `logweir/` is ever rewritten** (rule 4). Every put is
//!    `PutMode::Create`; a correction is a new record under a new point id; a
//!    removal is a tombstone (D3 §6, not this module's).
//! 4. **Availability and verification are separate axes** (D3 §5.4). Nothing
//!    here reports a point as available — this module writes and reads
//!    records; whether the archive behind one can still be fetched is a
//!    question only a fetch answers, and it belongs to the `RecoveryCatalog`
//!    sync (PLAT-15.2 / W8).
//!
//! # What this module does NOT do
//!
//! No Kubernetes view, no `RecoveryCatalog` object, no availability or
//! verification state machine, no disaster import, no tombstone and no
//! deletion of anything. Those are PLAT-15.2 and D3's wave-2 controller work.
//! `logweir catalog sync` here is the OPERATOR's command — it runs with the
//! operator's own credentials, creates no Job, and writes only under
//! `logweir/catalog/v1/`; D3 §5.3's controller-driven sync is a `catalogSync`
//! check plan under D2's one check runner and is a different thing with the
//! same name.

pub mod cli;
pub mod reader;
pub mod record;
pub mod schema;
pub mod writer;

pub use record::{
    CatalogLogEntry, CatalogPoint, RecordArchive, RecordCapture, RecordCovered, RecordExecution,
    RecordInstallation, RecordReceipt, RecordSchedule, RecordSigning, RecordSource, RecordTopic,
};

/// `application/vnd.logweir.catalog-point+json;version=1.0.0`.
///
/// Re-exported from `logweir-verify`, which DECLARES it beside the other four
/// media types Logweir signs: a controller that must CHECK a record links the
/// verify-only crate and never the signer
/// (`scripts/check-one-signer.sh`, ADR 0008 §E).
pub use logweir_evidence::PAYLOAD_TYPE_CATALOG_POINT;

/// The algorithm name a record publishes for the key that signed it.
///
/// **ONE SPELLING IN THIS PRODUCT.** These are the two strings
/// `crates/logweir/src/identity.rs::public_material` already publishes for the
/// SAME key in the installation's public identity ConfigMap, so an operator
/// comparing `signing.algorithm` in a catalog record with
/// `logweir-signing-trust`'s `algorithm` reads one value and not two spellings
/// of one fact. Decision D3 §5.2's illustrative JSON writes `"p256"`; that
/// would have been a second name for a key whose algorithm this product
/// already names, which is the defect the receipt's `scramSha512` comment
/// argues at length (`backup/phase_run.rs::receipt_auth`).
#[must_use]
pub fn algorithm_name(key: &logweir_evidence::keys::VerifyingKey) -> &'static str {
    match key {
        logweir_evidence::keys::VerifyingKey::P256(_) => "ecdsa-p256-sha256",
        logweir_evidence::keys::VerifyingKey::Ed25519(_) => "ed25519",
    }
}

/// [`RecordSigning`] for a key in hand. One derivation, so the record's
/// `key_id` and `algorithm` always describe the same key.
#[must_use]
pub fn signing_of(key: &logweir_evidence::keys::VerifyingKey) -> RecordSigning {
    RecordSigning {
        key_id: key.key_id(),
        algorithm: algorithm_name(key).to_string(),
    }
}
