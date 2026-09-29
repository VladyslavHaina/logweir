//! **FX-7 — keep earlier points valid after a manifest rewrite: the PIN.**
//!
//! A backup taken on a VERSIONED bucket records, in its signed receipt and in
//! its catalog point record, the object version id of the manifest bytes it
//! attests (`archive.manifest_version_id`, receipt and record format `1.1.0`).
//! A reader then compares the key's CURRENT version with that pin, so a set
//! written again after the point was signed is seen even when the manifest
//! bytes came out identical — which engine 0.21.0 does: it rewrites a set's
//! segments in place and keeps the first run's manifest entries.
//!
//! These rows pin the WRITER half — the runner and the catalog — over the
//! in-process versioned double (`Store::in_memory_versioned`). The READER half
//! is `drill/binding.rs`'s unit tests and `check_cli.rs`'s catalogSync rows.
//! No endpoint, no network.
use logweir::backup::{execute_with, BackupOutcome, BackupRunArgs};
use logweir_core::backup_receipt::{pinnable_version_id, BackupReceipt};
use logweir_core::engine::*;
use logweir_engine_oso::storage::{Store, VersionedBucket};
use logweir_evidence::keys::SigningKey;
use logweir_kafka::reader::{ClusterReader, ConsumedRecord, KafkaError, TopicMeta};
use std::collections::BTreeMap;

/// The archive prefix: under `logweir/`, for the reason `backup_run.rs`
/// records — the in-memory store asserts that root on every put.
const ARCHIVE_PREFIX: &str = "logweir/archive-fixture/";
const BACKUP_ID: &str = "nightly-20260929";

fn manifest_key(backup_id: &str) -> String {
    format!("{ARCHIVE_PREFIX}{backup_id}/manifest.json")
}

struct Fixture {
    _dir: tempfile::TempDir,
    args: BackupRunArgs,
    key: SigningKey,
}

impl Fixture {
    fn new() -> Self {
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
        let key = SigningKey::generate_p256();
        let key_path = dir.path().join("signer.pem");
        std::fs::write(&key_path, key.to_pkcs8_pem().unwrap()).unwrap();
        Self {
            args: BackupRunArgs {
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

    /// One run over `store` (archive AND evidence, as the in-process seam
    /// does), with an engine that writes its manifest `writes` times.
    fn run(&self, store: &Store, engine: &ManifestWriter<'_>) -> BackupOutcome {
        execute_with(
            &self.args,
            "01K6FX7RUN00000000000000001",
            &StubReader,
            engine,
            store,
            store,
        )
        .expect("the run succeeds")
    }

    fn receipt(&self, store: &Store, outcome: &BackupOutcome) -> (Vec<u8>, BackupReceipt) {
        let (bytes, _) = store.get(&outcome.receipt_key).unwrap();
        let receipt = serde_json::from_slice(&bytes).unwrap();
        (bytes, receipt)
    }
}

/// How the engine double writes its manifest: through the create-only store
/// (an unversioned bucket, or the first write of a versioned one), then — on a
/// versioned bucket — `rewrites` more times through the bucket handle, as the
/// real engine re-puts `<backup_id>/manifest.json` several times in one run.
struct ManifestWriter<'a> {
    store: &'a Store,
    bucket: Option<&'a VersionedBucket>,
    rewrites: usize,
}

impl DataEngine for ManifestWriter<'_> {
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
            created_at: "2026-09-29T03:04:00Z".parse().unwrap(),
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
                        end_offset: 99,
                        start_timestamp: 1_790_000_000_000,
                        end_timestamp: 1_790_000_060_000,
                        record_count: 100,
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
        plan: &BackupPlan,
        _obs: &mut dyn PhaseObserver,
    ) -> Result<BackupFacts, EngineError> {
        let key = manifest_key(&plan.backup_id);
        self.store
            .put_create_only(&key, b"{\"topics\":[],\"snapshot\":0}")
            .map_err(|e| EngineError::Operational(e.to_string()))?;
        if let Some(bucket) = self.bucket {
            for n in 1..=self.rewrites {
                bucket.overwrite(
                    &key,
                    format!("{{\"topics\":[],\"snapshot\":{n}}}").as_bytes(),
                );
            }
        }
        Ok(BackupFacts {
            started_at: "2026-09-29T03:00:00Z".parse().unwrap(),
            finished_at: "2026-09-29T03:04:00Z".parse().unwrap(),
            exit_code: 0,
            unknown_key_warnings: vec![],
        })
    }
}

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

// ---------------------------------------------------------------------------
// The writer
// ---------------------------------------------------------------------------

/// **The pin.** On a versioned bucket the receipt names the version of the
/// manifest bytes it attests — the LAST one the engine wrote, which is the one
/// the run read back — at format `1.1.0`, and the catalog record carries the
/// same pin at its own `1.1.0`.
#[test]
fn a_run_on_a_versioned_bucket_pins_the_manifest_version_it_read_back() {
    let f = Fixture::new();
    let (store, bucket) = Store::in_memory_versioned("logweir/");
    let engine = ManifestWriter {
        store: &store,
        bucket: Some(&bucket),
        rewrites: 4,
    };
    let outcome = f.run(&store, &engine);
    let versions = bucket.versions(&manifest_key(BACKUP_ID));
    assert_eq!(
        versions.len(),
        5,
        "the engine wrote its manifest five times"
    );
    let last = versions.last().unwrap().clone();

    let (_, receipt) = f.receipt(&store, &outcome);
    assert_eq!(
        receipt.archive.manifest_version_id.as_deref(),
        Some(last.as_str()),
        "the pin is the version of the bytes the run READ BACK — the last write — not an \
         earlier one and not none"
    );
    assert_eq!(receipt.format_version, "1.1.0", "a pinned receipt is 1.1.0");
    let (pinned, _) = store
        .get_version(&receipt.archive.manifest_key, &last)
        .unwrap();
    assert_eq!(
        logweir_core::ids::sha256_prefixed(&pinned),
        receipt.archive.manifest_sha256,
        "the digest is over exactly the pinned version's bytes"
    );

    // The catalog record carries the receipt's pin, and says so in its own
    // minor version.
    let record_key = outcome
        .catalog_key
        .clone()
        .expect("the run wrote its catalog point");
    let (record, _) = store.get(&record_key).unwrap();
    let record: serde_json::Value = serde_json::from_slice(&record).unwrap();
    assert_eq!(record["format_version"], "1.1.0");
    assert_eq!(record["archive"]["manifest_version_id"], last.as_str());
}

/// **Old evidence keeps its shape.** On an unversioned bucket there is nothing
/// to pin: the receipt is the `1.0.0` document with NO `manifest_version_id`
/// key at all (absent, never `null`), and so is the catalog record.
#[test]
fn an_unversioned_run_writes_the_1_0_0_receipt_with_no_pin_field() {
    let f = Fixture::new();
    let store = Store::in_memory("logweir/");
    let engine = ManifestWriter {
        store: &store,
        bucket: None,
        rewrites: 0,
    };
    let outcome = f.run(&store, &engine);
    let (bytes, receipt) = f.receipt(&store, &outcome);
    assert_eq!(receipt.format_version, "1.0.0");
    assert_eq!(receipt.archive.manifest_version_id, None);
    let raw: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    assert!(
        raw["archive"].get("manifest_version_id").is_none(),
        "absent from the bytes, not null: {}",
        raw["archive"]
    );
    let (record, _) = store.get(outcome.catalog_key.as_ref().unwrap()).unwrap();
    let record: serde_json::Value = serde_json::from_slice(&record).unwrap();
    assert_eq!(record["format_version"], "1.0.0");
    assert!(record["archive"].get("manifest_version_id").is_none());
}

/// S3's literal `null` is the version id of an object written while
/// versioning was never enabled or is suspended — the next write REPLACES it
/// in place — so it pins nothing. Blank answers pin nothing either.
#[test]
fn a_null_or_blank_version_id_is_not_a_pin() {
    assert_eq!(pinnable_version_id(None), None);
    assert_eq!(pinnable_version_id(Some("null")), None);
    assert_eq!(pinnable_version_id(Some("")), None);
    assert_eq!(pinnable_version_id(Some("  ")), None);
    assert_eq!(
        pinnable_version_id(Some("3sL4kqtJlcpXroDTDmJ.rmSpXd3dIbrHY")),
        Some("3sL4kqtJlcpXroDTDmJ.rmSpXd3dIbrHY".to_string())
    );
}

// ---------------------------------------------------------------------------
// The catalog record: the receipt is the authority
// ---------------------------------------------------------------------------

fn pinned_receipt_and_bytes() -> (BackupReceipt, Vec<u8>) {
    let f = Fixture::new();
    let (store, bucket) = Store::in_memory_versioned("logweir/");
    let engine = ManifestWriter {
        store: &store,
        bucket: Some(&bucket),
        rewrites: 1,
    };
    let outcome = f.run(&store, &engine);
    let (bytes, receipt) = f.receipt(&store, &outcome);
    assert!(receipt.archive.manifest_version_id.is_some());
    (receipt, bytes)
}

fn record_of(receipt: &BackupReceipt, bytes: &[u8]) -> logweir::catalog::CatalogPoint {
    let keys = logweir::backup::phase_run::receipt_keys(&receipt.backup_id, &receipt.run_id);
    logweir::catalog::writer::from_receipt(
        receipt,
        bytes,
        &logweir::catalog::writer::RecordInputs {
            receipt_key: keys.receipt_key,
            sidecar_key: keys.sidecar_key,
            location_id: "s3://kafka-backups/logweir/archive-fixture".into(),
            recorded_at: "2026-09-29T04:00:00Z".parse().unwrap(),
            signing: logweir::catalog::RecordSigning {
                key_id: "a".repeat(64),
                algorithm: "ecdsa-p256-sha256".into(),
            },
            installation: None,
            execution: None,
        },
    )
    .unwrap()
}

/// A record whose pin is not its receipt's contradicts the receipt: a reader
/// sent to the wrong object version is the failure the pin exists to prevent.
#[test]
fn a_record_whose_pin_is_not_its_receipts_is_a_mismatch() {
    use logweir::catalog::reader::{cross_check, CrossCheck};
    let (receipt, bytes) = pinned_receipt_and_bytes();
    let mut record = record_of(&receipt, &bytes);
    assert_eq!(cross_check(&record, &receipt, &bytes), CrossCheck::Agrees);

    record.archive.manifest_version_id = Some("some-other-version".into());
    match cross_check(&record, &receipt, &bytes) {
        CrossCheck::RecordMismatch(fields) => assert!(
            fields
                .iter()
                .any(|f| f.starts_with("archive.manifest_version_id")),
            "{fields:?}"
        ),
        other => panic!("a different pin must be a record mismatch, got {other:?}"),
    }
}

/// A record that carries a pin its receipt DOES NOT have is a contradiction
/// too — a record may not assert a fact its verification root lacks.
#[test]
fn a_record_that_invents_a_pin_is_a_mismatch() {
    use logweir::catalog::reader::{cross_check, CrossCheck};
    let f = Fixture::new();
    let store = Store::in_memory("logweir/");
    let engine = ManifestWriter {
        store: &store,
        bucket: None,
        rewrites: 0,
    };
    let outcome = f.run(&store, &engine);
    let (bytes, receipt) = f.receipt(&store, &outcome);
    let mut record = record_of(&receipt, &bytes);
    assert_eq!(cross_check(&record, &receipt, &bytes), CrossCheck::Agrees);
    record.archive.manifest_version_id = Some("invented".into());
    assert!(matches!(
        cross_check(&record, &receipt, &bytes),
        CrossCheck::RecordMismatch(_)
    ));
}

/// A record WITHOUT the pin, for a receipt that has one, is what an OLDER
/// catalog writer produces: absent means unknown (rule 2), readers take the
/// pin from the receipt, and the record is NOT a contradiction.
#[test]
fn a_record_from_an_older_writer_without_the_pin_still_agrees() {
    use logweir::catalog::reader::{cross_check, CrossCheck};
    let (receipt, bytes) = pinned_receipt_and_bytes();
    let mut record = record_of(&receipt, &bytes);
    record.archive.manifest_version_id = None;
    record.format_version = "1.0.0".into();
    assert_eq!(
        cross_check(&record, &receipt, &bytes),
        CrossCheck::Agrees,
        "an absent copy is unknown, not a disagreement"
    );
}

/// **Never reinterpreted.** `catalog sync` backfilling a receipt that pins
/// nothing writes a record that pins nothing — even over a versioned bucket
/// whose manifest HAS a current version it could have looked up. A pin is a
/// fact the signed receipt states, never one a later reader infers.
#[test]
fn a_backfill_never_infers_a_pin_the_receipt_does_not_carry() {
    use logweir::catalog::cli::sync_with;
    let f = Fixture::new();
    // An unversioned run, whose receipt pins nothing…
    let plain = Store::in_memory("logweir/");
    let engine = ManifestWriter {
        store: &plain,
        bucket: None,
        rewrites: 0,
    };
    let outcome = f.run(&plain, &engine);
    let (bytes, _) = f.receipt(&plain, &outcome);
    let sig = plain.get(&outcome.sidecar_key).unwrap().0;
    // …copied, receipt and sidecar only, into a VERSIONED bucket that also
    // holds a manifest under the receipt's key.
    let (versioned, _bucket) = Store::in_memory_versioned("logweir/");
    versioned
        .put_create_only(&outcome.receipt_key, &bytes)
        .unwrap();
    versioned
        .put_create_only(&outcome.sidecar_key, &sig)
        .unwrap();
    versioned
        .put_create_only(&manifest_key(BACKUP_ID), b"{\"topics\":[]}")
        .unwrap();

    let signer = logweir::backup::phase_run::load_signer(&f.args.signing_key).unwrap();
    let report = sync_with(
        &logweir::catalog::cli::SyncArgs {
            location: logweir::catalog::cli::Location {
                url: "s3://kafka-backups".into(),
                region: None,
                endpoint: None,
                path_style: false,
                allow_http: false,
            },
            signing_key: std::path::PathBuf::from("unused-by-the-seam"),
            public_keys: Vec::new(),
            since: None,
            max: 100,
        },
        &versioned,
        &signer,
        &[f.key.verifying_key()],
        "2026-09-29T05:00:00Z".parse().unwrap(),
        "s3://kafka-backups",
    )
    .unwrap();
    assert_eq!(report.written, 1, "{report:?}");
    let record_key = logweir::catalog::record::record_key(&report.points[0].1);
    let (record, _) = versioned.get(&record_key).unwrap();
    let record: serde_json::Value = serde_json::from_slice(&record).unwrap();
    assert_eq!(record["format_version"], "1.0.0");
    assert!(
        record["archive"].get("manifest_version_id").is_none(),
        "the backfill invented a pin: {}",
        record["archive"]
    );
}
