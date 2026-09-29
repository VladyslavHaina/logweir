//! Live I/O for PROD-01.1's rows: produce a deterministic fixture (plainly or
//! inside Kafka transactions), read a topic back at a stated isolation level,
//! and read an archive's records through Logweir's OWN `.kbak` decoder.
//!
//! Every reader here returns [`Rec`]s, the oracle's one record type, so the
//! source, the archive and the restored topic are compared as three readings
//! of the same kind.
//!
//! Every call that can block has a deadline: a producer flush, a read loop, a
//! child process ([`output_within`]). A hung fixture must fail the row, not
//! hang the suite.
#![allow(dead_code)]

use super::oracle::{Headers, Rec, TsType};
use crate::harness::{bin, engine_bin, engine_digest, engine_mount, engine_version, root};
use crate::harness::{ARCHIVE_BUCKET, BOOTSTRAP};
use rdkafka::client::ClientContext;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::error::{KafkaError, RDKafkaErrorCode};
use rdkafka::message::{BorrowedMessage, DeliveryResult, Header, Headers as _, Message};
use rdkafka::message::{OwnedHeaders, Timestamp};
use rdkafka::producer::{BaseProducer, BaseRecord, Producer, ProducerContext};
use rdkafka::{Offset, TopicPartitionList};
use std::io::Read;
use std::path::{Path, PathBuf};
use std::process::{Command, Output, Stdio};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

/// One record to produce, with every field stated.
#[derive(Debug, Clone)]
pub struct Out {
    pub partition: i32,
    pub key: Option<Vec<u8>>,
    pub value: Option<Vec<u8>>,
    pub headers: Headers,
    /// `None` lets librdkafka stamp the send time.
    pub timestamp: Option<i64>,
}

impl Out {
    pub fn kv(partition: i32, timestamp: Option<i64>, key: &str, value: &str) -> Self {
        Out {
            partition,
            key: Some(key.as_bytes().to_vec()),
            value: Some(value.as_bytes().to_vec()),
            headers: Vec::new(),
            timestamp,
        }
    }
}

/// Counts delivery reports, so a fixture that the broker rejected fails the
/// row instead of silently producing a smaller window.
#[derive(Default)]
pub struct Counting {
    delivered: AtomicUsize,
    failed: AtomicUsize,
}

impl ClientContext for Counting {}

impl ProducerContext for Counting {
    type DeliveryOpaque = ();
    fn delivery(&self, r: &DeliveryResult<'_>, _: Self::DeliveryOpaque) {
        match r {
            Ok(_) => self.delivered.fetch_add(1, Ordering::SeqCst),
            Err(_) => self.failed.fetch_add(1, Ordering::SeqCst),
        };
    }
}

fn nonce() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("the clock is after 1970")
        .as_nanos()
}

fn owned_headers(h: &Headers) -> OwnedHeaders {
    let mut oh = OwnedHeaders::new_with_capacity(h.len().max(1));
    for (k, v) in h {
        oh = oh.insert(Header {
            key: k,
            value: v.as_deref(),
        });
    }
    oh
}

fn send(p: &BaseProducer<Counting>, topic: &str, o: &Out) -> Result<(), String> {
    for _ in 0..100 {
        let mut rec: BaseRecord<'_, [u8], [u8]> = BaseRecord::to(topic).partition(o.partition);
        if let Some(ts) = o.timestamp {
            rec = rec.timestamp(ts);
        }
        if let Some(k) = o.key.as_deref() {
            rec = rec.key(k);
        }
        if let Some(v) = o.value.as_deref() {
            rec = rec.payload(v);
        }
        if !o.headers.is_empty() {
            rec = rec.headers(owned_headers(&o.headers));
        }
        match p.send(rec) {
            Ok(()) => return Ok(()),
            Err((KafkaError::MessageProduction(RDKafkaErrorCode::QueueFull), _)) => {
                p.poll(Duration::from_millis(100));
            }
            Err((e, _)) => return Err(format!("send {topic}/{}: {e}", o.partition)),
        }
    }
    Err(format!("send {topic}/{}: queue full for 10 s", o.partition))
}

fn base_config() -> ClientConfig {
    let mut c = ClientConfig::new();
    c.set("bootstrap.servers", BOOTSTRAP)
        .set("acks", "all")
        .set("message.timeout.ms", "30000")
        .set("linger.ms", "0")
        .set("compression.type", "none");
    c
}

fn check_delivery(p: &BaseProducer<Counting>, want: usize, what: &str) -> Result<(), String> {
    p.flush(Duration::from_secs(30))
        .map_err(|e| format!("{what}: flush: {e}"))?;
    let ok = p.context().delivered.load(Ordering::SeqCst);
    let bad = p.context().failed.load(Ordering::SeqCst);
    if bad != 0 || ok != want {
        return Err(format!(
            "{what}: {ok} delivered, {bad} failed, {want} sent — the broker did not take the whole fixture"
        ));
    }
    Ok(())
}

/// Produce `recs` in order, non-transactionally, with idempotence on so a
/// retry cannot reorder a partition, and require a successful delivery report
/// for every one.
pub fn produce_plain(topic: &str, recs: &[Out]) -> Result<(), String> {
    let p: BaseProducer<Counting> = base_config()
        .set("enable.idempotence", "true")
        .create_with_context(Counting::default())
        .map_err(|e| format!("producer: {e}"))?;
    for o in recs {
        send(&p, topic, o)?;
    }
    check_delivery(&p, recs.len(), &format!("produce_plain({topic})"))
}

/// A transactional producer. Its records reach the log as soon as they are
/// flushed and become visible to a `read_committed` reader only at commit.
pub struct Txn {
    p: BaseProducer<Counting>,
    sent: usize,
    id: String,
}

impl Txn {
    pub fn new(transactional_id: &str) -> Result<Self, String> {
        let p: BaseProducer<Counting> = base_config()
            .set("transactional.id", transactional_id)
            // Long enough to hold a transaction open across a whole backup
            // run; below the broker's default transaction.max.timeout.ms.
            .set("transaction.timeout.ms", "600000")
            .create_with_context(Counting::default())
            .map_err(|e| format!("transactional producer: {e}"))?;
        p.init_transactions(Duration::from_secs(60))
            .map_err(|e| format!("init_transactions({transactional_id}): {e}"))?;
        Ok(Txn {
            p,
            sent: 0,
            id: transactional_id.to_string(),
        })
    }

    pub fn begin(&self) -> Result<(), String> {
        self.p
            .begin_transaction()
            .map_err(|e| format!("begin_transaction({}): {e}", self.id))
    }

    pub fn send(&mut self, topic: &str, o: &Out) -> Result<(), String> {
        send(&self.p, topic, o)?;
        self.sent += 1;
        Ok(())
    }

    /// Flush without ending the transaction: the records are in the log,
    /// uncommitted. Requires a delivery report for every record sent so far.
    pub fn flush(&self) -> Result<(), String> {
        check_delivery(&self.p, self.sent, &format!("txn {}", self.id))
    }

    pub fn commit(&self) -> Result<(), String> {
        self.p
            .commit_transaction(Duration::from_secs(60))
            .map_err(|e| format!("commit_transaction({}): {e}", self.id))
    }

    pub fn abort(&self) -> Result<(), String> {
        self.p
            .abort_transaction(Duration::from_secs(60))
            .map_err(|e| format!("abort_transaction({}): {e}", self.id))
    }
}

/// The consumer isolation level a reading is taken at.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Isolation {
    Committed,
    Uncommitted,
}

fn to_rec(m: &BorrowedMessage<'_>) -> Rec {
    let (timestamp, ts_type) = match m.timestamp() {
        Timestamp::CreateTime(t) => (t, TsType::CreateTime),
        Timestamp::LogAppendTime(t) => (t, TsType::LogAppendTime),
        Timestamp::NotAvailable => (-1, TsType::NotAvailable),
    };
    let headers: Headers = m
        .headers()
        .map(|hs| {
            hs.iter()
                .map(|h| (h.key.to_string(), h.value.map(<[u8]>::to_vec)))
                .collect()
        })
        .unwrap_or_default();
    Rec {
        partition: m.partition(),
        offset: m.offset(),
        timestamp,
        ts_type,
        key: m.key().map(<[u8]>::to_vec),
        value: m.payload().map(<[u8]>::to_vec),
        headers,
    }
}

/// Every record of one partition, from the beginning to the end the broker
/// reports, at `iso`. Ends on the partition-EOF event, never on a quiet poll,
/// so a slow broker cannot shorten a reading.
pub fn read_partition(topic: &str, partition: i32, iso: Isolation) -> Result<Vec<Rec>, String> {
    let c: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", BOOTSTRAP)
        .set("group.id", format!("recsem-read-{}", nonce()))
        .set("enable.auto.commit", "false")
        .set("enable.auto.offset.store", "false")
        .set("enable.partition.eof", "true")
        .set("auto.offset.reset", "earliest")
        .set(
            "isolation.level",
            match iso {
                Isolation::Committed => "read_committed",
                Isolation::Uncommitted => "read_uncommitted",
            },
        )
        .create()
        .map_err(|e| format!("consumer: {e}"))?;
    let (lo, hi) = c
        .fetch_watermarks(topic, partition, Duration::from_secs(20))
        .map_err(|e| format!("watermarks {topic}/{partition}: {e}"))?;
    if hi <= lo {
        return Ok(Vec::new());
    }
    let mut tpl = TopicPartitionList::new();
    tpl.add_partition_offset(topic, partition, Offset::Beginning)
        .map_err(|e| format!("assign {topic}/{partition}: {e}"))?;
    c.assign(&tpl)
        .map_err(|e| format!("assign {topic}/{partition}: {e}"))?;
    let deadline = Instant::now() + Duration::from_secs(60);
    let mut out = Vec::new();
    loop {
        if Instant::now() > deadline {
            return Err(format!(
                "read {topic}/{partition} ({iso:?}): no end-of-partition after 60 s and {} records",
                out.len()
            ));
        }
        match c.poll(Duration::from_millis(500)) {
            None => continue,
            Some(Err(KafkaError::PartitionEOF(p))) if p == partition => break,
            Some(Err(e)) => return Err(format!("read {topic}/{partition}: {e}")),
            Some(Ok(m)) => out.push(to_rec(&m)),
        }
    }
    Ok(out)
}

/// [`read_partition`] over partitions `0..partitions`.
pub fn read_topic(topic: &str, partitions: i32, iso: Isolation) -> Result<Vec<Rec>, String> {
    let mut all = Vec::new();
    for p in 0..partitions {
        all.extend(read_partition(topic, p, iso)?);
    }
    Ok(all)
}

/// `(partition, high watermark)` for every partition of `topic`, read at
/// `read_uncommitted`: librdkafka answers a watermark query at the consumer's
/// isolation level, and its default (`read_committed`) would return the last
/// stable offset instead whenever a transaction is open.
pub fn high_watermarks(topic: &str, partitions: i32) -> Result<Vec<(i32, i64)>, String> {
    Ok(watermarks_at(topic, partitions, Isolation::Uncommitted)?
        .into_iter()
        .map(|(p, _, hi)| (p, hi))
        .collect())
}

/// `(partition, low, high)` as `rd_kafka_query_watermark_offsets` answers a
/// consumer configured at `iso`. Measured, not assumed, by the TXN row: if
/// librdkafka sends its ListOffsets at the consumer's isolation level, the
/// `read_committed` high mark is the last stable offset, and a gap between
/// the two readings is an open transaction — a detection route that needs
/// neither DescribeProducers nor `unsafe`.
pub fn watermarks_at(
    topic: &str,
    partitions: i32,
    iso: Isolation,
) -> Result<Vec<(i32, i64, i64)>, String> {
    let c: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", BOOTSTRAP)
        .set("group.id", format!("recsem-wm-{}", nonce()))
        .set("enable.auto.commit", "false")
        .set(
            "isolation.level",
            match iso {
                Isolation::Committed => "read_committed",
                Isolation::Uncommitted => "read_uncommitted",
            },
        )
        .create()
        .map_err(|e| format!("consumer: {e}"))?;
    (0..partitions)
        .map(|p| {
            c.fetch_watermarks(topic, p, Duration::from_secs(5))
                .map(|(lo, hi)| (p, lo, hi))
                .map_err(|e| format!("watermarks {topic}/{p}: {e}"))
        })
        .collect()
}

/// The fetch position a `read_committed` consumer holds once it reports
/// end-of-partition: where an open transaction stops it, if one does. A
/// second, consumption-based reading of the last stable offset beside
/// [`watermarks_at`].
pub fn committed_position_at_eof(topic: &str, partition: i32) -> Result<i64, String> {
    let c: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", BOOTSTRAP)
        .set("group.id", format!("recsem-lso-{}", nonce()))
        .set("enable.auto.commit", "false")
        .set("enable.auto.offset.store", "false")
        .set("enable.partition.eof", "true")
        .set("isolation.level", "read_committed")
        .create()
        .map_err(|e| format!("consumer: {e}"))?;
    let mut tpl = TopicPartitionList::new();
    tpl.add_partition_offset(topic, partition, Offset::Beginning)
        .map_err(|e| format!("assign {topic}/{partition}: {e}"))?;
    c.assign(&tpl)
        .map_err(|e| format!("assign {topic}/{partition}: {e}"))?;
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        if Instant::now() > deadline {
            return Err(format!("{topic}/{partition}: no end-of-partition in 30 s"));
        }
        match c.poll(Duration::from_millis(500)) {
            Some(Err(KafkaError::PartitionEOF(p))) if p == partition => break,
            Some(Err(e)) => return Err(format!("{topic}/{partition}: {e}")),
            _ => {}
        }
    }
    let pos = c
        .position()
        .map_err(|e| format!("position {topic}/{partition}: {e}"))?;
    pos.find_partition(topic, partition)
        .and_then(|e| e.offset().to_raw())
        .ok_or_else(|| format!("{topic}/{partition}: no position"))
}

/// One manifest segment entry, as the engine wrote it.
#[derive(Debug, Clone)]
pub struct Segment {
    pub partition: i32,
    pub key: String,
    pub start_offset: i64,
    pub end_offset: i64,
    pub start_timestamp: i64,
    pub end_timestamp: i64,
    pub record_count: i64,
}

/// An archive, read without the engine: the manifest as JSON, its segment
/// entries, and every record decoded by `logweir_engine_oso::kbak`.
pub struct Archive {
    pub manifest_key: String,
    pub manifest: serde_json::Value,
    pub manifest_bytes: Vec<u8>,
    pub segments: Vec<Segment>,
    pub records: Vec<Rec>,
}

pub fn archive_location(backup_id: &str) -> logweir_core::engine::StorageUrl {
    logweir_core::engine::StorageUrl::S3 {
        bucket: ARCHIVE_BUCKET.to_string(),
        prefix: backup_id.to_string(),
        region: Some("us-east-1".to_string()),
        endpoint: Some("http://localhost:9000".to_string()),
        path_style: true,
        allow_http: true,
    }
}

/// The manifest key a `logweir backup run` with `storage.prefix = backup_id`
/// writes (the engine nests its own `{backup_id}/` under the prefix).
pub fn manifest_key(backup_id: &str) -> String {
    format!("{backup_id}/{backup_id}/manifest.json")
}

pub fn read_archive(backup_id: &str, topic: &str) -> Result<Archive, String> {
    let store =
        logweir_engine_oso::storage::Store::read_only_from_url(&archive_location(backup_id))
            .map_err(|e| format!("store for {backup_id}: {e:?}"))?;
    let mk = manifest_key(backup_id);
    let (manifest_bytes, _) = store.get(&mk).map_err(|e| format!("{mk}: {e:?}"))?;
    let manifest: serde_json::Value =
        serde_json::from_slice(&manifest_bytes).map_err(|e| format!("{mk}: {e}"))?;
    let mut segments = Vec::new();
    let mut records = Vec::new();
    let topics = manifest["topics"].as_array().cloned().unwrap_or_default();
    for t in topics.iter().filter(|t| t["name"].as_str() == Some(topic)) {
        for p in t["partitions"].as_array().cloned().unwrap_or_default() {
            let partition = p["partition_id"]
                .as_i64()
                .ok_or_else(|| format!("{mk}: partition without partition_id"))?
                as i32;
            for s in p["segments"].as_array().cloned().unwrap_or_default() {
                let num = |f: &str| {
                    s[f].as_i64()
                        .ok_or_else(|| format!("{mk}: segment without {f}"))
                };
                let key = s["key"]
                    .as_str()
                    .ok_or_else(|| format!("{mk}: segment without key"))?
                    .to_string();
                let seg = Segment {
                    partition,
                    key: key.clone(),
                    start_offset: num("start_offset")?,
                    end_offset: num("end_offset")?,
                    start_timestamp: num("start_timestamp")?,
                    end_timestamp: num("end_timestamp")?,
                    record_count: num("record_count")?,
                };
                let qualified = store.qualify(&key);
                let (bytes, _) = store
                    .get(&qualified)
                    .map_err(|e| format!("{qualified}: {e:?}"))?;
                let decoded = logweir_engine_oso::kbak::decode_segment(&bytes)
                    .map_err(|e| format!("{qualified}: {e:?}"))?;
                for r in decoded {
                    records.push(Rec {
                        partition,
                        offset: r.offset,
                        timestamp: r.timestamp,
                        ts_type: TsType::Unrecorded,
                        key: r.key,
                        value: r.value,
                        headers: r.headers,
                    });
                }
                segments.push(seg);
            }
        }
    }
    records.sort_by_key(|r| (r.partition, r.offset));
    Ok(Archive {
        manifest_key: mk,
        manifest,
        manifest_bytes,
        segments,
        records,
    })
}

/// The raw manifest bytes, or `None` when there is no manifest.
pub fn manifest_bytes(backup_id: &str) -> Option<Vec<u8>> {
    let store =
        logweir_engine_oso::storage::Store::read_only_from_url(&archive_location(backup_id))
            .ok()?;
    store.get(&manifest_key(backup_id)).ok().map(|(b, _)| b)
}

/// Run `cmd` to completion or kill it after `secs`, reading both pipes
/// concurrently so a chatty child cannot deadlock on a full pipe.
pub fn output_within(mut cmd: Command, secs: u64) -> Result<Output, String> {
    cmd.stdout(Stdio::piped()).stderr(Stdio::piped());
    let mut child = cmd.spawn().map_err(|e| format!("spawn {cmd:?}: {e}"))?;
    let mut so = child.stdout.take().expect("piped stdout");
    let mut se = child.stderr.take().expect("piped stderr");
    let t_out = std::thread::spawn(move || {
        let mut b = Vec::new();
        let _ = so.read_to_end(&mut b);
        b
    });
    let t_err = std::thread::spawn(move || {
        let mut b = Vec::new();
        let _ = se.read_to_end(&mut b);
        b
    });
    let deadline = Instant::now() + Duration::from_secs(secs);
    let status = loop {
        match child.try_wait() {
            Ok(Some(s)) => break s,
            Ok(None) if Instant::now() > deadline => {
                let _ = child.kill();
                let _ = child.wait();
                return Err(format!("{cmd:?}: killed after {secs} s"));
            }
            Ok(None) => std::thread::sleep(Duration::from_millis(200)),
            Err(e) => return Err(format!("{cmd:?}: {e}")),
        }
    };
    Ok(Output {
        status,
        stdout: t_out.join().unwrap_or_default(),
        stderr: t_err.join().unwrap_or_default(),
    })
}

/// An allowlist that does NOT name the live cluster: `--allowed-clusters` is
/// the restore-TARGET allowlist and GC18(c) rail 4 refuses a source listed in
/// it (the same file `e2e/tests/pitr_boundary.rs` writes, under its own name).
fn backup_allowlist() -> PathBuf {
    let p = crate::harness::demo_dir().join("recsem-backup-allowed-clusters.json");
    std::fs::write(
        &p,
        "{\"allowed_cluster_ids\": [\"SCRATCH-CLUSTER-NOT-THE-SOURCE\"]}\n",
    )
    .expect("the allowlist is writable");
    p
}

/// `logweir backup run` with the REAL, digest-pinned engine over `topics`,
/// into `storage.prefix = backup_id`. Returns the process output unjudged.
pub fn backup_run(backup_id: &str, topics: &[&str], segment_max_records: u64) -> Output {
    let spec = crate::harness::demo_dir().join(format!("{backup_id}-backup.yaml"));
    let list = topics.join(", ");
    std::fs::write(
        &spec,
        format!(
            "backup_id: {backup_id}\n\
             source:\n\
             \x20 bootstrap_servers: [{BOOTSTRAP}]\n\
             \x20 topics: [{list}]\n\
             storage:\n\
             \x20 backend: s3\n\
             \x20 bucket: {ARCHIVE_BUCKET}\n\
             \x20 prefix: {backup_id}\n\
             \x20 region: us-east-1\n\
             \x20 endpoint: http://localhost:9000\n\
             \x20 path_style: true\n\
             \x20 allow_http: true\n\
             backup:\n\
             \x20 compression: zstd\n\
             \x20 segment_max_records: {segment_max_records}\n\
             \x20 segment_max_bytes: 10485760\n\
             \x20 max_concurrent_partitions: 3\n"
        ),
    )
    .expect("the backup spec is writable");
    let mut c = Command::new(bin());
    c.args(["backup", "run", "--spec"])
        .arg(&spec)
        .arg("--allowed-clusters")
        .arg(backup_allowlist())
        .arg("--signing-key")
        .arg(root().join("e2e/fixtures/signed/signing.pem"))
        .env("AWS_ACCESS_KEY_ID", "minioadmin")
        .env("AWS_SECRET_ACCESS_KEY", "minioadmin")
        .env("AWS_REGION", "us-east-1")
        .env("LOGWEIR_ENGINE_BIN", engine_bin())
        .env("LOGWEIR_ENGINE_VERSION", engine_version())
        .env("LOGWEIR_ENGINE_DIGEST", engine_digest())
        .env("LOGWEIR_E2E_ENGINE_MOUNT", engine_mount())
        .env("TMPDIR", engine_mount());
    output_within(c, 900).unwrap_or_else(|e| panic!("logweir backup run {backup_id}: {e}"))
}

/// `docker compose … <verb> kafka-broker-1`, bounded.
pub fn compose_broker(verb: &str) -> Output {
    let mut c = Command::new("docker");
    c.args([
        "compose",
        "-f",
        "e2e/compose/docker-compose.yml",
        verb,
        "kafka-broker-1",
    ])
    .current_dir(root());
    output_within(c, 120).unwrap_or_else(|e| panic!("docker compose {verb}: {e}"))
}

pub fn write_json(path: &Path, v: &serde_json::Value) {
    if let Some(d) = path.parent() {
        std::fs::create_dir_all(d).expect("the outcome directory is creatable");
    }
    std::fs::write(
        path,
        serde_json::to_vec_pretty(v).expect("an outcome serialises"),
    )
    .expect("the outcome is writable");
}
