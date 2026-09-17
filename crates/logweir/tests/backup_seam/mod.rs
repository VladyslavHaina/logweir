//! The `backup run` seam, in miniature, for `crates/logweir/tests/catalog.rs`.
//!
//! A SECOND, SMALLER set of doubles rather than a shared one:
//! `crates/logweir/tests/backup_run.rs`'s `RecordingEngine` and `StubReader`
//! carry the knobs that file's twenty-five rows need (identity overrides,
//! failing engines, a signing file that rotates mid-run), and every one of
//! them is inert here. What the catalog rows need is one successful backup,
//! over a store the test chooses, so that is all this builds.
//!
//! Global Constraint 22: nothing here dials. The bootstrap list is
//! `kafka-source:9092` — a hostname handed to a `ClusterReader` DOUBLE, never
//! to a client — and deliberately not the loopback spelling the other backup
//! fixtures use, so this file names no token from
//! `crates/logweir/tests/no_network_in_unit_tests.rs`'s `DIAL_TOKENS` and
//! needs no entry on its allow-list.

// A shared test module is compiled into EVERY binary that declares it, and
// each one uses a different part of it: `catalog.rs` calls `execute`,
// `catalog_minio.rs` calls `execute_as`. Without this, whichever half a binary
// does not use is a `dead_code` warning — and `clippy -D warnings` turns a
// warning into a build failure.
#![allow(dead_code)]

use logweir::backup::{execute_with, BackupError, BackupOutcome, BackupRunArgs};
use logweir_core::engine::*;
use logweir_engine_oso::storage::Store;
use logweir_evidence::keys::{SigningKey, VerifyingKey};
use logweir_kafka::reader::{ClusterReader, ConsumedRecord, KafkaError, TopicMeta};
use std::collections::BTreeMap;

/// Where the fixture archive lives. Under `logweir/` for the mechanical reason
/// `backup_run.rs` records: `Store::in_memory` is the only writable store a
/// test can build without a backend, and `put_create_only` asserts that root.
/// Nothing in production writes an archive here.
pub const ARCHIVE_PREFIX: &str = "logweir/archive-fixture/";
pub const BACKUP_ID: &str = "nightly-20260915";

pub struct Fixture {
    _dir: tempfile::TempDir,
    args: BackupRunArgs,
    key: SigningKey,
}

impl Fixture {
    pub fn new() -> Self {
        let dir = tempfile::tempdir().unwrap();
        let spec = dir.path().join("backup.yaml");
        std::fs::write(
            &spec,
            format!(
                "backup_id: {BACKUP_ID}\n\
                 source:\n\
                \x20 bootstrap_servers: [kafka-source:9092]\n\
                \x20 topics: [orders]\n\
                 storage:\n\
                \x20 backend: s3\n\
                \x20 bucket: kafka-backups\n\
                \x20 prefix: {ARCHIVE_PREFIX}\n\
                \x20 region: us-east-1\n\
                 backup:\n\
                \x20 compression: zstd\n\
                \x20 segment_max_records: 1000\n\
                \x20 segment_max_bytes: 10485760\n\
                \x20 max_concurrent_partitions: 3\n"
            ),
        )
        .unwrap();
        let allowed = dir.path().join("allowed-clusters.json");
        std::fs::write(
            &allowed,
            "{\"allowed_cluster_ids\": [\"SCRATCH-CLUSTER-01\"]}",
        )
        .unwrap();
        // An EPHEMERAL key, generated here: the throwaway fixture key under
        // `e2e/fixtures/signed/` is reserved for the corpus walkers.
        let key = SigningKey::generate_p256();
        let key_path = dir.path().join("signer.pem");
        std::fs::write(&key_path, key.to_pkcs8_pem().unwrap()).unwrap();
        Self {
            args: BackupRunArgs {
                // D2 §3.5: a seam invocation is not under the store contract.
                store_contract_version: None,
                spec,
                allowed_clusters: allowed,
                signing_key: key_path,
                triggered_by: Some("schedule".into()),
                out: None,
                receipt_out: None,
                backup_id_override: None,
            },
            key,
            _dir: dir,
        }
    }

    pub fn public_key(&self) -> VerifyingKey {
        self.key.verifying_key()
    }

    /// One successful `backup run`, writing its evidence through `evidence`.
    ///
    /// The ARCHIVE handle is built here and is a different store: a backup
    /// never writes to the archive (Global Constraint 6), and the catalog rows
    /// count the objects in the evidence store, which only works if the two
    /// are not the same one.
    pub fn execute(&self, evidence: &Store) -> Result<BackupOutcome, BackupError> {
        self.execute_as(evidence, BACKUP_ID, "01J9X2QK7C4V0R8YB3ZP6MTS5A")
    }

    /// The same run under a caller-chosen `backup_id` and `run_id`.
    ///
    /// The MinIO leg needs it: every evidence key is create-only, and the
    /// compose bucket outlives one `cargo test`, so a fixed pair would make
    /// the second run of the suite fail at a put rather than at an assertion.
    pub fn execute_as(
        &self,
        evidence: &Store,
        backup_id: &str,
        run_id: &str,
    ) -> Result<BackupOutcome, BackupError> {
        let archive = Store::in_memory(ARCHIVE_PREFIX);
        archive
            .put_create_only(
                &format!("{ARCHIVE_PREFIX}{backup_id}/manifest.json"),
                format!("{{\"backup_id\":\"{backup_id}\",\"topics\":[]}}").as_bytes(),
            )
            .unwrap();
        // Rebuilt rather than cloned: `BackupRunArgs` derives no `Clone`, and
        // adding one to production code to serve a test fixture is the wrong
        // way round.
        let args = BackupRunArgs {
            // D2 §3.5: a seam invocation is not under the store contract.
            store_contract_version: None,
            spec: self.args.spec.clone(),
            allowed_clusters: self.args.allowed_clusters.clone(),
            signing_key: self.args.signing_key.clone(),
            triggered_by: self.args.triggered_by.clone(),
            out: None,
            receipt_out: None,
            backup_id_override: Some(backup_id.to_string()),
        };
        execute_with(
            &args,
            run_id,
            &StubReader,
            &OneTopicEngine,
            &archive,
            evidence,
        )
    }
}

/// A `ClusterReader` that answers one cluster id and nothing else. It
/// constructs no client; the bootstrap list above is data.
struct StubReader;

impl ClusterReader for StubReader {
    fn cluster_id(&self) -> Result<String, KafkaError> {
        Ok("SOURCE-CLUSTER-01".into())
    }
    fn list_topics(&self) -> Result<Vec<TopicMeta>, KafkaError> {
        Ok(vec![])
    }
    fn end_offsets(&self, _: &str) -> Result<Vec<(i32, i64)>, KafkaError> {
        Ok(vec![])
    }
    fn topic_configs(&self, _: &str) -> Result<BTreeMap<String, String>, KafkaError> {
        Ok(BTreeMap::new())
    }
    fn broker_configs(&self) -> Result<BTreeMap<String, String>, KafkaError> {
        Ok(BTreeMap::new())
    }
    fn consume_range(
        &self,
        _: &str,
        _: i32,
        _: i64,
        _: usize,
    ) -> Result<Vec<ConsumedRecord>, KafkaError> {
        Ok(vec![])
    }
}

/// A `DataEngine` that reports one topic with one segment and spawns nothing.
struct OneTopicEngine;

fn ts(s: &str) -> chrono::DateTime<chrono::Utc> {
    s.parse().unwrap()
}

impl DataEngine for OneTopicEngine {
    fn id(&self) -> EngineId {
        EngineId {
            id: "double".into(),
            version: "v0.21.0".into(),
            digest: "sha256:0".into(),
        }
    }
    fn list_backup_sets(&self, _: &StorageUrl) -> Result<Vec<BackupSetRef>, EngineError> {
        unimplemented!("the backup path lists through the Store handle, not the engine")
    }
    fn describe(&self, set: &BackupSetRef) -> Result<BackupSetFacts, EngineError> {
        Ok(BackupSetFacts {
            backup_id: set.backup_id.clone(),
            created_at: ts("2026-09-15T03:04:00Z"),
            source_cluster_id: None,
            manifest_sha256: "sha256:from-the-engines-own-handle".into(),
            manifest_version_id: None,
            consumer_group_snapshot_sha256: None,
            topics: vec![TopicFacts {
                name: "orders".into(),
                original_partition_count: Some(1),
                source_replication_factor: Some(1),
                configurations: BTreeMap::new(),
                partitions: vec![PartitionFacts {
                    partition_id: 0,
                    segments: vec![SegmentFacts {
                        key: "seg".into(),
                        start_offset: 0,
                        end_offset: 1233,
                        start_timestamp: 1_757_980_800_000,
                        end_timestamp: 1_757_984_399_999,
                        record_count: 1234,
                        sha256: String::new(),
                        uploaded_at: 0,
                    }],
                    gaps: vec![],
                    pruned: vec![],
                }],
            }],
        })
    }
    fn preflight(&self, _: &RestorePlan) -> Result<PreflightReport, EngineError> {
        unimplemented!()
    }
    fn restore(
        &self,
        _: &RestorePlan,
        _: &mut dyn PhaseObserver,
    ) -> Result<RestoreFacts, EngineError> {
        unimplemented!()
    }
    fn fingerprints(&self, _: &SampleSelection) -> Result<Vec<RecordFingerprint>, EngineError> {
        unimplemented!()
    }
    fn backup(
        &self,
        _plan: &BackupPlan,
        _obs: &mut dyn PhaseObserver,
    ) -> Result<BackupFacts, EngineError> {
        Ok(BackupFacts {
            started_at: ts("2026-09-15T03:00:00Z"),
            finished_at: ts("2026-09-15T03:04:00Z"),
            exit_code: 0,
            unknown_key_warnings: vec![],
        })
    }
}
