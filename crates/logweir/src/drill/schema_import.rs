//! **PROD-03.2: the schemas a restore needs, imported keeping their IDs,
//! before any topic is created.**
//!
//! [`prepare`] decides, from the plan's `target.schema_registry` and the bound
//! point's receipt, whether this run imports (and reads and verifies the
//! snapshot the receipt names), says why it does not, or has nothing to say.
//! The archive pass collects the framed IDs the restored records carry
//! (`archive_scan`); [`import`] then takes their closure, classifies it against
//! the target registry and registers what is missing
//! (`logweir_core::schema_registry`). Every refusal is exit 3 before phase 2,
//! so no target topic exists when it is reported; its message opens with the
//! named reason (`SchemaConflict`, `SchemaRegistryCannotKeepIds`,
//! `SchemaRegistryReadOnly`, `SchemaRegistryModeNotRestored`,
//! `SchemaSnapshotInvalid`, `SchemaRegistryUnreachable`). Imported schemas are
//! never removed, by phase 9 or by cleanup.

use logweir_core::guard::GuardRefusal;
use logweir_core::schema_registry::{
    classify, closure, import as register, NeededIds, ScorecardSchemas, Snapshot,
    MAX_SNAPSHOT_BYTES, NOT_IMPORTED_NO_SNAPSHOT, NOT_IMPORTED_NO_TARGET_REGISTRY,
    SNAPSHOT_INVALID,
};
use logweir_core::spec::{DrillSpec, SchemaRegistrySpec};
use logweir_engine_oso::storage::{Store, StoreError};

use crate::drill::binding::BoundSet;
use crate::drill::DrillError;

/// What this run does about schemas.
#[derive(Debug)]
pub enum Prepared {
    /// Neither the plan nor the point names a registry: no block.
    Nothing,
    /// The scorecard says why nothing is imported (and the run goes on).
    NotImported(&'static str),
    /// Import: the target, the verified snapshot and its digest.
    Import(Box<Plan>),
}

/// An import to run.
#[derive(Debug)]
pub struct Plan {
    /// The target registry, from the approved plan.
    pub target: SchemaRegistrySpec,
    /// The snapshot, verified against the receipt's digest.
    pub snapshot: Snapshot,
    /// That digest.
    pub sha256: String,
}

fn refused(message: String) -> DrillError {
    DrillError::Guard(GuardRefusal(message))
}

/// Decide, and read the snapshot when this run imports. A target connection
/// without a registry is not refused (a drill into a scratch cluster needs
/// none): the scorecard says the schemas were not imported.
///
/// # Errors
///
/// `SchemaSnapshotInvalid` (exit 3) for a snapshot the archive does not hold
/// or whose bytes are not the receipt's; exit 1 for a store failure.
pub fn prepare(
    spec: &DrillSpec,
    bound: Option<&BoundSet>,
    archive: &Store,
) -> Result<Prepared, DrillError> {
    let captured = bound
        .and_then(|b| b.schema_registry.as_ref())
        .filter(|r| r.captured);
    let (target, block) = match (spec.target.schema_registry.as_ref(), captured) {
        (None, None) => return Ok(Prepared::Nothing),
        (None, Some(_)) => return Ok(Prepared::NotImported(NOT_IMPORTED_NO_TARGET_REGISTRY)),
        (Some(_), None) => return Ok(Prepared::NotImported(NOT_IMPORTED_NO_SNAPSHOT)),
        (Some(t), Some(b)) => (t, b),
    };
    let (Some(key), Some(sha256)) = (&block.snapshot_key, &block.snapshot_sha256) else {
        return Err(refused(format!(
            "{SNAPSHOT_INVALID}: the receipt says its registry was captured and names no \
             snapshot; no target topic was created"
        )));
    };
    let bytes = match archive.get_capped(key, MAX_SNAPSHOT_BYTES as u64) {
        Ok((bytes, _)) => bytes,
        Err(StoreError::NotFound(_) | StoreError::TooLarge { .. }) => {
            return Err(refused(format!(
                "{SNAPSHOT_INVALID}: the schema snapshot {key} the receipt names is not in the \
                 archive, or is larger than {MAX_SNAPSHOT_BYTES} bytes; no target topic was \
                 created"
            )))
        }
        Err(e) => {
            return Err(DrillError::Operational(format!(
                "the schema snapshot {key} could not be read: {e}; no target topic was created"
            )))
        }
    };
    let snapshot = Snapshot::parse(&bytes, sha256).map_err(refused)?;
    Ok(Prepared::Import(Box::new(Plan {
        target: target.clone(),
        snapshot,
        sha256: sha256.clone(),
    })))
}

/// Import the closure of `needed` into the target registry, keeping IDs.
///
/// # Errors
///
/// Every refusal of `logweir_core::schema_registry::ImportRefusal`, and a
/// registry credential whose Secret is bound elsewhere, as exit 3.
pub fn import(plan: &Plan, needed: &NeededIds) -> Result<ScorecardSchemas, DrillError> {
    let password = crate::credential_binding::registry_password(crate::tls_ca::Side::Target)
        .map_err(|e| refused(e.to_string()))?;
    let ca = crate::tls_ca::projected_ca_file(crate::tls_ca::TARGET_TLS_CA_FILE_VAR)
        .map_err(DrillError::Operational)?;
    let mut client = crate::backup::schema_registry::RegistryClient::new(
        &plan.target,
        password.as_deref(),
        ca.as_deref(),
    )
    .map_err(DrillError::Operational)?;
    let entries = closure(&plan.snapshot, needed.needed());
    let classes = classify(&mut client, &entries).map_err(|e| refused(e.to_string()))?;
    let report = register(&mut client, &entries, &classes).map_err(|e| refused(e.to_string()))?;
    tracing::info!(
        registry = %plan.target.url,
        imported = report.imported.len(),
        already_present = report.already_present,
        "the restored records' schemas are in the target registry with their source IDs"
    );
    Ok(ScorecardSchemas::imported(
        &plan.sha256,
        &plan.target.url,
        &report,
        needed.not_in_snapshot(),
    ))
}
