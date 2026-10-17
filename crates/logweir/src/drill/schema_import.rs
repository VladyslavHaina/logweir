//! **PROD-03.2: the schemas a restore needs, imported keeping their IDs,
//! before any topic is created.**
//!
//! [`prepare`] decides, from the plan's `target.schema_registry` and the bound
//! point's receipt (for a plan bound to none, its set's own:
//! [`receipt_of_set`]), whether this run imports (and reads and verifies the
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

/// The receipt of a set a plan names by id alone (no `source.point`): the
/// set's execution claim names the run that wrote it, and its receipt sits
/// beside the claim. `None` when there is no claim or receipt to read (an
/// older archive), or they do not name this set. Read, NOT verified: neither
/// the runner of such a plan nor a check Job holds an evidence keyring, so it
/// is used only to REFUSE an import ([`unverifiable`]), never to make one.
/// The runner and the restore preflight both read it.
///
/// # Errors
///
/// A store failure other than "absent" or "over the cap", as `read` words it.
pub fn receipt_of_set(
    backup_id: &str,
    mut read: impl FnMut(&str, u64) -> Result<Vec<u8>, StoreError>,
) -> Result<Option<logweir_core::backup_receipt::BackupReceipt>, StoreError> {
    let mut get = |key: &str, cap: u64| match read(key, cap) {
        Ok(bytes) => Ok(Some(bytes)),
        Err(StoreError::NotFound(_) | StoreError::TooLarge { .. }) => Ok(None),
        Err(e) => Err(e),
    };
    let claim_key = crate::backup::phase_run::claim_key(backup_id);
    let Some(claim) = get(&claim_key, logweir_engine_oso::storage::caps::SIDECAR)? else {
        return Ok(None);
    };
    let run_id = serde_json::from_slice::<serde_json::Value>(&claim)
        .ok()
        .and_then(|c| c.get("run_id")?.as_str().map(str::to_string));
    let Some(key) = run_id
        .map(|run| crate::backup::phase_run::receipt_keys(backup_id, &run).receipt_key)
        .filter(|key| crate::catalog::record::receipt_run_id(key, backup_id).is_some())
    else {
        return Ok(None);
    };
    let Some(bytes) = get(&key, logweir_engine_oso::storage::caps::SIGNED_DOCUMENT)? else {
        return Ok(None);
    };
    Ok(
        serde_json::from_slice::<logweir_core::backup_receipt::BackupReceipt>(&bytes)
            .ok()
            .filter(|r| r.backup_id == backup_id),
    )
}

/// PoC D3: why a plan bound to no point is refused when its set captured a
/// registry. Its receipt cannot be verified here (only a point-bound plan
/// carries the evidence keyring), and importing on an unverified receipt
/// would let whoever can write the bucket choose what is registered, under
/// which IDs, on the target. The named reason is `SchemaSnapshotInvalid`.
#[must_use]
pub fn unverifiable(backup_id: &str) -> String {
    format!(
        "{SNAPSHOT_INVALID}: backup set {backup_id} captured a schema registry, and a restore \
         bound to no recovery point cannot verify the receipt that names the snapshot, so \
         nothing is imported from it. Plan the restore from the set's catalog point (connect \
         a recovery catalog over its destination), or choose a target connection without a \
         registry; no target topic was created"
    )
}

/// Decide, and read the snapshot when this run imports. A target connection
/// without a registry is not refused (a drill into a scratch cluster needs
/// none): the scorecard says the schemas were not imported. A plan bound to
/// a point takes the registry block from the verified receipt; one bound to
/// none is refused when [`receipt_of_set`] of the set it restores
/// (`set_backup_id`) says the registry was captured ([`unverifiable`]).
///
/// # Errors
///
/// `SchemaSnapshotInvalid` (exit 3) for a snapshot the archive does not hold
/// or whose bytes are not the receipt's; exit 1 for a store failure.
pub fn prepare(
    spec: &DrillSpec,
    bound: Option<&BoundSet>,
    set_backup_id: &str,
    archive: &Store,
) -> Result<Prepared, DrillError> {
    if bound.is_none() && spec.target.schema_registry.is_some() {
        let receipt = receipt_of_set(set_backup_id, |key: &str, cap| {
            archive.get_capped(key, cap).map(|(bytes, _)| bytes)
        })
        .map_err(|e| {
            DrillError::Operational(format!(
                "the receipt of backup set {set_backup_id} could not be read: {e}; no target \
                 topic was created"
            ))
        })?;
        if receipt
            .and_then(|r| r.schema_registry)
            .is_some_and(|r| r.captured)
        {
            return Err(refused(unverifiable(set_backup_id)));
        }
    }
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
    let bytes = match archive.get_capped(key, logweir_engine_oso::storage::caps::SCHEMA_SNAPSHOT) {
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

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::drill::binding::tests::{plan_with, receipt};

    pub(crate) const SET: &str = "nightly-7";

    /// An archive holding set `nightly-7`'s claim, its receipt (whose
    /// `schema_registry` says captured) and the snapshot the receipt names,
    /// the keys `backup run` writes them at.
    pub(crate) fn archive(with_claim: bool) -> Store {
        let store = Store::in_memory("logweir");
        let snapshot = br#"{"format":1,"captured_at":"2026-10-16T00:00:00Z","registry_url":"http://source:8081","versions":[]}"#;
        let snapshot_key = format!("logweir/backups/{SET}/run-1.schema-registry.json");
        let mut r = receipt("logweir/nightly-7/manifest.json", "sha256:aa");
        r.schema_registry = Some(logweir_core::schema_registry::ReceiptSchemaRegistry {
            captured: true,
            reason: None,
            snapshot_key: Some(snapshot_key.clone()),
            snapshot_sha256: Some(logweir_core::ids::sha256_prefixed(snapshot)),
            subjects: 0,
            versions: 0,
            unresolved_references: vec![],
            ids_not_in_snapshot: vec![],
        });
        let keys = crate::backup::phase_run::receipt_keys(SET, "run-1");
        let put = |key: &str, bytes: &[u8]| store.put_create_only(key, bytes).expect("written");
        put(&keys.receipt_key, &serde_json::to_vec(&r).unwrap());
        put(&snapshot_key, snapshot);
        if with_claim {
            put(
                &crate::backup::phase_run::claim_key(SET),
                br#"{"backup_id":"nightly-7","run_id":"run-1"}"#,
            );
        }
        store
    }

    fn plan_into_a_registry() -> DrillSpec {
        let mut plan = plan_with(None);
        plan.target.schema_registry = Some(SchemaRegistrySpec {
            url: "http://target:8081".into(),
            username: None,
        });
        plan
    }

    /// PoC D3: a plan bound to no point (a restore planned from a Backup)
    /// whose set captured a registry is REFUSED, `SchemaSnapshotInvalid`,
    /// before anything is imported: its receipt cannot be verified, so it
    /// never decides what is registered. Controls: without the claim that
    /// names the run, and for another set id, the run goes on with
    /// `noSnapshot`, as every such plan did before.
    #[test]
    fn a_plan_bound_to_no_point_never_imports_from_an_unverified_receipt() {
        let plan = plan_into_a_registry();
        match prepare(&plan, None, SET, &archive(true)) {
            Err(DrillError::Guard(GuardRefusal(m))) => {
                assert!(m.starts_with(SNAPSHOT_INVALID), "{m}");
                assert!(m.contains("cannot verify the receipt"), "{m}");
                assert!(m.contains("no target topic was created"), "{m}");
            }
            other => panic!("expected the refusal, got {other:?}"),
        }
        // Control: the same set bound to its point, whose receipt the binding
        // verified, imports from the same snapshot.
        let store = archive(true);
        let keys = crate::backup::phase_run::receipt_keys(SET, "run-1");
        let (bytes, _) = store
            .get_capped(
                &keys.receipt_key,
                logweir_engine_oso::storage::caps::SIGNED_DOCUMENT,
            )
            .expect("the receipt");
        let verified: logweir_core::backup_receipt::BackupReceipt =
            serde_json::from_slice(&bytes).unwrap();
        let bound = BoundSet {
            point_id: "lwp1-00000000000000000000000000000000".into(),
            backup_id: SET.into(),
            manifest_key: verified.archive.manifest_key.clone(),
            manifest_sha256: verified.archive.manifest_sha256.clone(),
            manifest_version_id: None,
            schema_registry: verified.schema_registry,
        };
        match prepare(&plan, Some(&bound), SET, &store).expect("prepared") {
            Prepared::Import(p) => assert_eq!(p.target.url, "http://target:8081"),
            other => panic!("expected an import, got {other:?}"),
        }
        match prepare(&plan, None, SET, &archive(false)).expect("prepared") {
            Prepared::NotImported(reason) => assert_eq!(reason, NOT_IMPORTED_NO_SNAPSHOT),
            other => panic!("expected noSnapshot, got {other:?}"),
        }
        // Another set's id reads nothing of this one.
        match prepare(&plan, None, "nightly-8", &archive(true)).expect("prepared") {
            Prepared::NotImported(reason) => assert_eq!(reason, NOT_IMPORTED_NO_SNAPSHOT),
            other => panic!("expected noSnapshot, got {other:?}"),
        }
    }
}
