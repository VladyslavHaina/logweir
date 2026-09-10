//! The run itself, and the read-back that makes its result a measured fact
//! rather than an exit code.
//!
//! `backup` has no `--format` and writes no report file
//! [VERIFIED U:crates/kafka-backup-cli/src/main.rs:35-39,559-561], so its only
//! machine-readable signal is the exit code. Everything else this phase
//! reports is read back out of the ARCHIVE — the manifest key, the exact
//! bytes' digest, the per-topic record counts and the covered window — which
//! is what turns "the engine exited 0" into "these records are in this
//! archive". A backup of empty topics also exits 0, and a run that reports
//! success without reading anything back is this project's recurring defect
//! (`scripts/e2e-seed.sh`'s own header says so about its own steps).
//!
//! Global Constraint 6 is untouched: the archive handle is
//! `Store::read_only_from_url`'s, which physically cannot put.
use crate::backup::BackupError;
use logweir_core::engine::{BackupFacts, BackupPlan, DataEngine, PhaseObserver};
use logweir_engine_oso::storage::Store;
use std::collections::BTreeMap;

/// Everything the run and the read-back established.
#[derive(Debug)]
pub struct Ran {
    pub facts: BackupFacts,
    pub manifest_key: String,
    /// `"sha256:<hex>"` over the EXACT bytes read back, via
    /// `logweir_core::ids::sha256_prefixed` (`ids.rs:11-13`) — never over a
    /// re-serialisation of anything.
    pub manifest_sha256: String,
    pub records_per_topic: BTreeMap<String, u64>,
    pub covered_from_ms: i64,
    pub covered_to_ms: i64,
}

pub fn run(
    plan: &BackupPlan,
    engine: &dyn DataEngine,
    store: &Store,
    obs: &mut dyn PhaseObserver,
) -> Result<Ran, BackupError> {
    obs.phase_started(-1, "backup");
    let facts = engine.backup(plan, obs);
    obs.phase_finished(
        -1,
        &match &facts {
            Ok(_) => "ok".to_string(),
            Err(e) => format!("failed: {e}"),
        },
    );
    let facts = facts?;

    // The manifest this run's `backup_id` produced. Listed through the
    // read-only archive handle rather than reconstructed from the prefix and
    // the id: `Store::list_manifests` derives `backup_id` from the key's
    // parent directory, so matching on it is matching the archive's own
    // answer about which set is which, and a set that is not there is a fact
    // worth failing on rather than a path we hope exists.
    let sets = store
        .list_manifests(&plan.storage)
        .map_err(BackupError::Engine)?;
    let set = sets
        .into_iter()
        .find(|s| s.backup_id == plan.backup_id)
        .ok_or_else(|| {
            BackupError::Operational(format!(
                "the engine exited 0 but the archive holds no backup set `{}` at the configured \
                 storage location (prefix `{}`); nothing was read back, so this run establishes \
                 nothing about the source cluster",
                plan.backup_id,
                plan.storage.prefix()
            ))
        })?;

    // The digest is over the bytes THIS RUN READ, not over
    // `BackupSetFacts::manifest_sha256`. The two are the same value today
    // (`OsoCliEngine::describe` hashes the bytes it read the same way), and
    // they are read here anyway: `describe` reaches the archive through the
    // engine's OWN store handle, so a receipt quoting only the engine's
    // number would be attesting bytes this process never saw.
    let (manifest_bytes, _version_id) = store
        .get(&set.manifest_key)
        .map_err(|e| BackupError::Operational(e.to_string()))?;
    let manifest_sha256 = logweir_core::ids::sha256_prefixed(&manifest_bytes);

    let archive = engine.describe(&set)?;

    let mut records_per_topic: BTreeMap<String, u64> = BTreeMap::new();
    let mut oldest: Option<i64> = None;
    let mut newest: Option<i64> = None;
    for topic in &archive.topics {
        let entry = records_per_topic.entry(topic.name.clone()).or_insert(0);
        for partition in &topic.partitions {
            for segment in &partition.segments {
                // `record_count` is `i64` on the wire. A negative count is
                // not a smaller number, it is a manifest this build cannot
                // read as a count, so it saturates at 0 rather than wrapping
                // into a colossal `u64`.
                *entry += segment.record_count.max(0) as u64;
                oldest = Some(oldest.map_or(segment.start_timestamp, |o: i64| {
                    o.min(segment.start_timestamp)
                }));
                newest = Some(
                    newest.map_or(segment.end_timestamp, |n: i64| n.max(segment.end_timestamp)),
                );
            }
        }
    }
    // A set with no segment bounds no window, and saying so is the only honest
    // answer — the same refusal `Store::manifest_facts` makes for the same
    // reason. An empty min/max would be published as a real window, and a
    // covered range of `[0, 0]` reads as "this archive covers the epoch".
    let (Some(covered_from_ms), Some(covered_to_ms)) = (oldest, newest) else {
        return Err(BackupError::Operational(format!(
            "backup set `{}` declares no segment, so it bounds no window: the engine exited 0 \
             having captured nothing. Check that the named topics hold records.",
            plan.backup_id
        )));
    };

    Ok(Ran {
        facts,
        manifest_key: set.manifest_key,
        manifest_sha256,
        records_per_topic,
        covered_from_ms,
        covered_to_ms,
    })
}
