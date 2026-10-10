//! FX-1 fix round, M1: an unreadable consumer-groups snapshot is TOLD, not
//! dropped.
//!
//! Since FX-1, `OsoCliEngine::describe` no longer refuses an archive over the
//! content of its consumer-groups snapshot — Logweir restores no consumer
//! offsets and signs nothing about the snapshot — and a snapshot that is not
//! in the engine's shape is an `ArchiveNotice` beside the facts
//! (`DataEngine::describe_with_notices`). This file pins that `drill run` and
//! `backup run` PRINT every notice: a `warning:` line on stderr and a WARN
//! event with the notice's fields on the structured log. It fails when either
//! call site stops asking for notices, or stops surfacing them, or when the
//! surfacing function stops writing the line or the event.
//!
//! Every row runs in process: the drill over `tests/fixtures`' orchestrator
//! doubles, the backup over a `ClusterReader` double and `Store::in_memory`.
//! Nothing here dials — the backup spec's bootstrap is the hostname
//! `kafka-source:9092`, handed to a double, never to a client.
//!
//! Signed evidence is deliberately NOT asserted to carry the notice: no
//! scorecard, receipt or catalog field records it, and carrying it into
//! signed evidence is PROD-04.1's. The rows assert the opposite, so that
//! adding it there becomes a visible, versioned decision.

mod fixtures;

use logweir_core::engine::*;
use logweir_engine_oso::storage::Store;
use logweir_evidence::keys::SigningKey;
use logweir_kafka::reader::{ClusterReader, ConsumedRecord, KafkaError, TopicMeta};
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

// ---------------------------------------------------------------------------
// The notice, and a capture of the structured log.
// ---------------------------------------------------------------------------

const KIND: &str = "consumer-groups-snapshot-unreadable";

fn notice(key: &str) -> ArchiveNotice {
    ArchiveNotice {
        kind: KIND.into(),
        key: key.into(),
        sha256: format!("sha256:{}", "ab".repeat(32)),
        message: "the consumer-groups snapshot is present but unreadable; Logweir reads no \
                  consumer offsets from it and signs nothing about it, so this run does not \
                  depend on it"
            .into(),
        reason: "not the consumer-groups snapshot shape kafka-backup writes: invalid type: \
                 sequence, expected a map at line 1 column 80"
            .into(),
    }
}

#[derive(Clone, Default)]
struct Buf(Arc<Mutex<Vec<u8>>>);

impl std::io::Write for Buf {
    fn write(&mut self, b: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(b);
        Ok(b.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// Runs `f` under a JSON subscriber that writes into memory, and returns its
/// result with every event it logged. The same `fmt().json()` layer both
/// commands install, at `warn` so only the events this file is about remain.
fn capture<T>(f: impl FnOnce() -> T) -> (T, Vec<serde_json::Value>) {
    let buf = Buf::default();
    let writer = buf.clone();
    let subscriber = tracing_subscriber::fmt()
        .json()
        .with_current_span(true)
        .with_env_filter(tracing_subscriber::EnvFilter::new("warn"))
        .with_writer(move || writer.clone())
        .finish();
    let out = tracing::subscriber::with_default(subscriber, f);
    let text = String::from_utf8(buf.0.lock().unwrap().clone()).unwrap();
    let events = text
        .lines()
        .map(|l| serde_json::from_str(l).unwrap_or_else(|e| panic!("{e}: {l}")))
        .collect();
    (out, events)
}

/// The WARN events that tell a notice of `KIND`.
fn notice_events(events: &[serde_json::Value]) -> Vec<&serde_json::Value> {
    events
        .iter()
        .filter(|e| e["level"] == "WARN" && e["fields"]["notice"] == KIND)
        .collect()
}

// ---------------------------------------------------------------------------
// An engine that tells a notice, around any other.
// ---------------------------------------------------------------------------

/// Delegates everything to `inner` and adds `notices` to `describe`'s answer,
/// the way `OsoCliEngine` does for an unreadable snapshot.
struct Tells {
    inner: Box<dyn DataEngine>,
    notices: Vec<ArchiveNotice>,
}

impl DataEngine for Tells {
    fn id(&self) -> EngineId {
        self.inner.id()
    }
    fn list_backup_sets(&self, l: &StorageUrl) -> Result<Vec<BackupSetRef>, EngineError> {
        self.inner.list_backup_sets(l)
    }
    fn describe(&self, s: &BackupSetRef) -> Result<BackupSetFacts, EngineError> {
        self.inner.describe(s)
    }
    fn preflight(&self, p: &RestorePlan) -> Result<PreflightReport, EngineError> {
        self.inner.preflight(p)
    }
    fn restore(
        &self,
        p: &RestorePlan,
        o: &mut dyn PhaseObserver,
    ) -> Result<RestoreFacts, EngineError> {
        self.inner.restore(p, o)
    }
    fn fingerprints(&self, s: &SampleSelection) -> Result<Vec<RecordFingerprint>, EngineError> {
        self.inner.fingerprints(s)
    }
    fn validation_run(&self, p: &RestorePlan) -> Result<EngineRun, EngineError> {
        self.inner.validation_run(p)
    }
    fn backup(
        &self,
        p: &BackupPlan,
        o: &mut dyn PhaseObserver,
    ) -> Result<BackupFacts, EngineError> {
        self.inner.backup(p, o)
    }
    fn describe_with_notices(
        &self,
        s: &BackupSetRef,
    ) -> Result<(BackupSetFacts, Vec<ArchiveNotice>), EngineError> {
        Ok((self.inner.describe(s)?, self.notices.clone()))
    }
}

/// Holds `Ctx::engine`'s place for the instant it is moved into `Tells`.
struct Placeholder;

impl DataEngine for Placeholder {
    fn id(&self) -> EngineId {
        unreachable!()
    }
    fn list_backup_sets(&self, _: &StorageUrl) -> Result<Vec<BackupSetRef>, EngineError> {
        unreachable!()
    }
    fn describe(&self, _: &BackupSetRef) -> Result<BackupSetFacts, EngineError> {
        unreachable!()
    }
    fn preflight(&self, _: &RestorePlan) -> Result<PreflightReport, EngineError> {
        unreachable!()
    }
    fn restore(
        &self,
        _: &RestorePlan,
        _: &mut dyn PhaseObserver,
    ) -> Result<RestoreFacts, EngineError> {
        unreachable!()
    }
    fn fingerprints(&self, _: &SampleSelection) -> Result<Vec<RecordFingerprint>, EngineError> {
        unreachable!()
    }
}

// ---------------------------------------------------------------------------
// The rows.
// ---------------------------------------------------------------------------

/// The surfacing function itself: one `warning:` line per notice on the writer
/// it is given, naming the backup set, the object, its digest, the reason and
/// the kind; and one WARN event per notice carrying the same fields.
#[test]
fn every_notice_is_one_warning_line_and_one_structured_event() {
    let n = notice("drill-demo/drill-demo/consumer-groups-snapshot.json");
    let mut stderr = Vec::new();
    let ((), events) = capture(|| {
        logweir::drill::surface_archive_notices(&mut stderr, "drill-demo", &[n.clone(), n.clone()])
    });
    let expected = format!(
        "warning: backup set drill-demo: {}: {} ({}): {} [{KIND}]\n",
        n.message, n.key, n.sha256, n.reason
    );
    assert_eq!(String::from_utf8(stderr).unwrap(), expected.repeat(2));
    assert_eq!(
        logweir::drill::archive_notice_line("drill-demo", &n) + "\n",
        expected
    );
    let told = notice_events(&events);
    assert_eq!(told.len(), 2, "{events:?}");
    for e in told {
        let f = &e["fields"];
        assert_eq!(f["backup_id"], "drill-demo", "{e}");
        assert_eq!(f["key"], n.key, "{e}");
        assert_eq!(f["sha256"], n.sha256, "{e}");
        assert_eq!(f["reason"], n.reason, "{e}");
        assert_eq!(f["message"], n.message, "{e}");
    }

    // No notice, nothing said.
    let mut quiet = Vec::new();
    let ((), events) =
        capture(|| logweir::drill::surface_archive_notices(&mut quiet, "drill-demo", &[]));
    assert!(quiet.is_empty());
    assert!(notice_events(&events).is_empty(), "{events:?}");
}

/// **`drill run` tells it.** The orchestrator's own `describe` call site asks
/// for notices and surfaces them — before any target is touched — and the
/// drill still passes: the notice changes nothing it does or signs.
#[test]
fn a_drill_tells_an_unreadable_snapshot_and_still_passes() {
    let mut f = fixtures::orchestrator_fixture(fixtures::Drill::Passes);
    let inner = std::mem::replace(&mut f.ctx.engine, Box::new(Placeholder));
    let key = "drills/fixture/consumer-groups-snapshot.json";
    f.ctx.engine = Box::new(Tells {
        inner,
        notices: vec![notice(key)],
    });
    let (sc, events) = capture(|| logweir::drill::execute_with(&f.args, &f.run_id, &f.ctx));
    let sc = sc.expect("the fixture drill passes with a notice as without one");
    assert_eq!(
        serde_json::to_value(&sc).unwrap()["outcome"],
        "pass",
        "the notice must not change the outcome"
    );
    let told = notice_events(&events);
    assert_eq!(told.len(), 1, "no WARN event told the notice: {events:?}");
    assert_eq!(told[0]["fields"]["key"], key);
    assert_eq!(told[0]["fields"]["backup_id"], sc.source.backup_id);
    // Nothing signed records it: carrying it into evidence is PROD-04.1's.
    let signed = serde_json::to_string(&sc).unwrap();
    assert!(!signed.contains("consumer-groups"), "{signed}");
}

// --- the backup ------------------------------------------------------------

const ARCHIVE_PREFIX: &str = "logweir/archive-fixture/";
const BACKUP_ID: &str = "nightly-20260929";

/// A `ClusterReader` that answers one cluster id; it constructs no client.
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

/// A `DataEngine` whose backup "ran" and whose archive holds one topic with
/// one segment. It spawns nothing, and — when given an archive — writes its
/// manifest there DURING the run, as the real engine does: a backup run
/// refuses a set whose manifest exists before its engine starts (FX-7).
struct OneTopic {
    archive: Option<Arc<Store>>,
}

fn ts(s: &str) -> chrono::DateTime<chrono::Utc> {
    s.parse().unwrap()
}

impl DataEngine for OneTopic {
    fn id(&self) -> EngineId {
        EngineId {
            id: "double".into(),
            version: "v0.21.0".into(),
            digest: "sha256:0".into(),
        }
    }
    fn list_backup_sets(&self, _: &StorageUrl) -> Result<Vec<BackupSetRef>, EngineError> {
        unreachable!("the backup path lists through the Store handle, not the engine")
    }
    fn describe(&self, set: &BackupSetRef) -> Result<BackupSetFacts, EngineError> {
        Ok(BackupSetFacts {
            backup_id: set.backup_id.clone(),
            created_at: ts("2026-09-29T03:04:00Z"),
            source_cluster_id: None,
            manifest_sha256: "sha256:from-the-engines-own-handle".into(),
            manifest_version_id: None,
            consumer_group_snapshot_sha256: Some(format!("sha256:{}", "ab".repeat(32))),
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
                        start_timestamp: 1_790_650_000_000,
                        end_timestamp: 1_790_650_099_999,
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
        unreachable!()
    }
    fn restore(
        &self,
        _: &RestorePlan,
        _: &mut dyn PhaseObserver,
    ) -> Result<RestoreFacts, EngineError> {
        unreachable!()
    }
    fn fingerprints(&self, _: &SampleSelection) -> Result<Vec<RecordFingerprint>, EngineError> {
        unreachable!()
    }
    fn backup(
        &self,
        plan: &BackupPlan,
        _obs: &mut dyn PhaseObserver,
    ) -> Result<BackupFacts, EngineError> {
        if let Some(archive) = &self.archive {
            let backup_id = &plan.backup_id;
            archive
                .put_create_only(
                    &format!("{ARCHIVE_PREFIX}{backup_id}/manifest.json"),
                    format!("{{\"backup_id\":\"{backup_id}\",\"topics\":[]}}").as_bytes(),
                )
                .map_err(|e| EngineError::Operational(e.to_string()))?;
        }
        Ok(BackupFacts {
            started_at: ts("2026-09-29T03:00:00Z"),
            finished_at: ts("2026-09-29T03:04:00Z"),
            exit_code: 0,
            unknown_key_warnings: vec![],
        })
    }
}

/// **`backup run` tells it.** The receipt path's own `describe` call site asks
/// for notices and surfaces them, and the receipt is still written, saying
/// nothing about the snapshot.
#[test]
fn a_backup_tells_an_unreadable_snapshot_and_still_writes_its_receipt() {
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
    let key_path = dir.path().join("signer.pem");
    std::fs::write(
        &key_path,
        SigningKey::generate_p256().to_pkcs8_pem().unwrap(),
    )
    .unwrap();
    let args = logweir::backup::BackupRunArgs {
        store_contract_version: None,
        spec,
        allowed_clusters: allowed,
        signing_key: key_path,
        triggered_by: Some("schedule".into()),
        out: None,
        receipt_out: None,
        backup_id_override: None,
        kafka_topic_resources: None,
        strimzi_cluster: None,
        consumer_groups: Vec::new(),
    };
    // EMPTY: the engine double writes the manifest when it runs (FX-7: a set
    // whose manifest exists before the engine starts is refused).
    let archive = Arc::new(Store::in_memory(ARCHIVE_PREFIX));
    let evidence = Store::in_memory("logweir/");
    let snapshot_key = format!("{ARCHIVE_PREFIX}{BACKUP_ID}/consumer-groups-snapshot.json");
    let engine = Tells {
        inner: Box::new(OneTopic {
            archive: Some(archive.clone()),
        }),
        notices: vec![notice(&snapshot_key)],
    };

    let (outcome, events) = capture(|| {
        logweir::backup::execute_with(
            &args,
            "01J9X2QK7C4V0R8YB3ZP6MTS5A",
            &StubReader,
            &engine,
            &archive,
            &evidence,
        )
    });
    let outcome = outcome.expect("the backup succeeds with a notice as without one");
    let told = notice_events(&events);
    assert_eq!(told.len(), 1, "no WARN event told the notice: {events:?}");
    assert_eq!(told[0]["fields"]["key"], snapshot_key);
    assert_eq!(told[0]["fields"]["backup_id"], BACKUP_ID);
    let (receipt, _) = evidence
        .get(&outcome.receipt_key)
        .expect("the receipt was written");
    let receipt = String::from_utf8(receipt).unwrap();
    assert!(receipt.contains(BACKUP_ID), "{receipt}");
    // Nothing signed records it: carrying it into evidence is PROD-04.1's.
    assert!(!receipt.contains("consumer-groups"), "{receipt}");
}
