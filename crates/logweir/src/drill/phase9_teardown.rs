//! Phase 9 — delete the scratch topics the drill created, and attest what
//! actually happened.
//!
//! The attestation has one job the rest of the drill cannot do for it: say
//! honestly whether teardown completed. A topic the broker refused to delete
//! is recorded in `topics_failed` and is NEVER listed in `topics_deleted`, so
//! the signed document can never assert a clean state the cluster is not in.
//! A teardown failure is a WARNING at the call site, never a change of
//! outcome: the drill result is already signed and uploaded by phase 8, and
//! leaving scratch topics behind is an operational annoyance rather than a
//! false claim.
use crate::drill::DrillError;
use logweir_engine_oso::storage::Store;
use logweir_kafka::reader::TopicDeleter;
use std::collections::BTreeMap;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct TeardownAttestation {
    pub run_id: String,
    /// `sha256:<hex>` of the SIGNED scorecard bytes — not the run id again.
    /// Binding the attestation to `run_id` twice would carry no independent
    /// information and could not identify WHICH signed document this teardown
    /// accompanies.
    pub scorecard_sha256: String,
    pub teardown_policy: String,
    pub topics_deleted: Vec<String>,
    pub topics_failed: Vec<(String, String)>,
    pub deleted_at: chrono::DateTime<chrono::Utc>,
}

/// Teardown goes through `logweir_kafka::reader::TopicDeleter`, NOT through
/// rdkafka directly: `crates/logweir` declares no rdkafka dependency and adding
/// one would break the layering rule that `logweir-kafka` is the only crate
/// that dials a broker. The deleter takes exact names from
/// `topic_mapping.values()` — never a pattern, never a prefix.
pub fn run(
    deleter: &dyn TopicDeleter,
    mapping: &BTreeMap<String, String>,
    policy: &str,
    run_id: &str,
    scorecard_sha256: &str,
) -> TeardownAttestation {
    let mut deleted = Vec::new();
    let mut failed = Vec::new();
    if policy == "delete" {
        let names: Vec<String> = mapping.values().cloned().collect();
        match deleter.delete_topics(&names) {
            Ok(results) => {
                for (name, r) in results {
                    match r {
                        Ok(()) => deleted.push(name),
                        Err(e) => failed.push((name, e)),
                    }
                }
            }
            // The whole call failed, so nothing is known to have been
            // deleted: every mapped topic is attested as failed rather than
            // the attestation falling silent about them.
            Err(e) => {
                for n in mapping.values() {
                    failed.push((n.clone(), e.to_string()));
                }
            }
        }
    }
    TeardownAttestation {
        run_id: run_id.into(),
        scorecard_sha256: scorecard_sha256.into(),
        teardown_policy: policy.into(),
        topics_deleted: deleted,
        topics_failed: failed,
        // Global Constraint 1: the clock is read HERE, in `crates/logweir`,
        // never in `logweir-core`.
        deleted_at: chrono::Utc::now(),
    }
}

fn sig<E: std::fmt::Display>(e: E) -> DrillError {
    DrillError::SigningOrLock(e.to_string())
}

/// Signed with `PAYLOAD_TYPE_TEARDOWN` and put create-only next to the
/// scorecard. Without this the payload type would be a media type nothing ever
/// emits, and the segregation evidence — which scratch topics were actually
/// deleted — would never be persisted.
///
/// Signing precedes both puts here for the same reason it does in phase 8, and
/// the same `SigningOrLock` variant carries the failure, so a teardown that
/// cannot be attested is exit 4 rather than a silent success. A failure here
/// is a WARNING at the call site, never an outcome: the drill result is
/// already signed and uploaded by phase 8.
pub fn persist(
    att: &TeardownAttestation,
    signing_key: &std::path::Path,
    store: &Store,
) -> Result<(), DrillError> {
    let bytes = logweir_core::det_json::to_deterministic_json(att).map_err(sig)?;
    let key = logweir_evidence::keys::SigningKey::from_pem_file(signing_key).map_err(sig)?;
    let sidecar = logweir_evidence::sign::sign_detached(
        &key,
        logweir_evidence::PAYLOAD_TYPE_TEARDOWN,
        &bytes,
    )
    .map_err(sig)?;
    let sidecar_bytes = serde_json::to_vec(&sidecar).map_err(sig)?;
    store
        .put_create_only(
            &format!("logweir/drills/{}.teardown.json", att.run_id),
            &bytes,
        )
        .map_err(sig)?;
    store
        .put_create_only(
            &format!("logweir/drills/{}.teardown.sig", att.run_id),
            &sidecar_bytes,
        )
        .map_err(sig)?;
    Ok(())
}
