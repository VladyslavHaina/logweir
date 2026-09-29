#![cfg(feature = "e2e")]
//! **PROD-01.4 — topic identity and generations: the reusable oracle.**
//!
//! A Kafka topic that is deleted and created again under the same name is a
//! NEW topic. The broker gives it a new topic ID (KIP-516) and restarts every
//! partition's offsets at zero, so an offset that meant one record before the
//! recreation means a different record after it. Logweir cannot read topic IDs
//! today: rdkafka 0.36.2's safe API has no DescribeTopics, `logweir-kafka`
//! forbids `unsafe`, and the pinned engine asks for Metadata v9, which carries
//! none. `docs/to-do/decisions/PROD-01.4-topic-identity.md` therefore adopts a
//! heuristic until a real ID route lands, and this file measures it.
//!
//! * Every live row builds one situation on the compose broker: a recreated
//!   topic (same, fewer and more partitions), added partitions, DeleteRecords,
//!   compaction, retention expiry, an open transaction, non-monotonic
//!   timestamps, a byte-identical replay and an original-name restore.
//! * The GROUND TRUTH is the broker's own topic ID, read with
//!   `kafka-topics.sh --describe`. Each row asserts it before it asserts a
//!   verdict, so a fixture that did not do what it claims fails loudly instead
//!   of making its verdict vacuous.
//! * The capture is the PINNED ENGINE's, through `engine_bin()`, and the
//!   archived boundary fingerprints come from Logweir's own `.kbak` decoder
//!   (`logweir_engine_oso::kbak`), with the two headers the engine appends
//!   stripped (see [`archived_fingerprints`]).
//! * The verdict is [`classify`], the reference rule. The consuming rows
//!   (PROD-02.1 first) implement it in product code and must agree with it on
//!   every row here.
//!
//! Known false negatives are rows too (c13, c14): each asserts the miss, so a
//! future change that closes one fails here and has to update the decision
//! record deliberately.
//!
//! Set `LOGWEIR_TOPIC_IDENTITY_EVIDENCE=<file>` to append one JSON line per
//! live row. Row c10 waits for the broker's five-minute retention check and is
//! `#[ignore]`d; run it with `--ignored`. Run the file alone with
//! `cargo test -p e2e --features e2e --test topic_identity -- --test-threads=1`
//! against a stack from `just e2e-up`.
mod harness;
use harness::*;

use logweir_engine_oso::kbak::{decode_segment, ArchivedRecord};
use logweir_kafka::fingerprint::record_fingerprint;
use logweir_kafka::rdkafka_reader::RdKafkaReader;
use logweir_kafka::reader::{AuthConfig, ClusterReader, NewTopicSpec, TopicCreator, TopicDeleter};
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::error::{KafkaError, RDKafkaErrorCode};
use rdkafka::message::{BorrowedMessage, Header, Headers, OwnedHeaders};
use rdkafka::producer::{BaseProducer, BaseRecord, Producer};
use rdkafka::{Message, Offset, TopicPartitionList};
use serde_json::{json, Value};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::process::{Command, Output, Stdio};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

/// Every client call's own budget.
const T: Duration = Duration::from_secs(20);

/// The topic prefix every row uses. `RdKafkaReader::delete_topics` is scoped
/// to it, so this file can delete nothing else.
const PREFIX: &str = "ti-";

/// Tail candidates the probe tries, newest first. More than one because the
/// engine archives transaction markers as ordinary records (its fetch decoder
/// keeps every record of every batch), and no consumer ever returns a marker.
const TAIL: usize = 3;

// ===========================================================================
// The reference rule. Pure: no broker, no clock, no I/O.
// ===========================================================================

/// Log start offset and high watermark of one partition, READ_UNCOMMITTED
/// unless a row says otherwise (the engine captures READ_UNCOMMITTED, so the
/// marks it is compared with must be read the same way).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Marks {
    log_start: i64,
    high_watermark: i64,
}

/// One archived record near the end of a partition: its offset and the
/// fingerprint of the SOURCE record it was archived from.
#[derive(Debug, Clone, PartialEq, Eq)]
struct TailRecord {
    offset: i64,
    /// `record_fingerprint` over the archived record minus the two headers
    /// the engine appended.
    fingerprint: String,
    /// The same over the archived bytes verbatim, kept only to show why the
    /// strip is required.
    raw_fingerprint: String,
}

/// What one capture archived for one partition.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Archived {
    first_offset: i64,
    last_offset: i64,
    first_timestamp_ms: i64,
    last_timestamp_ms: i64,
    records: usize,
    /// Up to [`TAIL`] records, newest first.
    tail: Vec<TailRecord>,
}

/// One partition as one capture observed it.
#[derive(Debug, Clone, PartialEq, Eq)]
struct PartitionObs {
    partition: i32,
    before: Marks,
    after: Marks,
    /// `None` when the engine archived no record for the partition.
    archived: Option<Archived>,
}

/// One capture of one topic: the previous observation a later one is
/// compared with.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Capture {
    cluster_id: String,
    partition_count: i32,
    partitions: Vec<PartitionObs>,
}

/// The current state a new run reads before its engine starts.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Current {
    cluster_id: String,
    partition_count: i32,
    marks: BTreeMap<i32, Marks>,
}

/// What a read at exactly one offset returned.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Probe {
    /// A record at exactly the requested offset, with its fingerprint.
    At(String),
    /// No record at that offset: the next record's offset, or `None` at the
    /// end of the partition (compacted away, or a transaction marker).
    Absent(Option<i64>),
    /// Below the log start offset.
    OutOfRange,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum Signal {
    PartitionCountDecreased {
        previous: i32,
        current: i32,
    },
    PartitionCountIncreased {
        previous: i32,
        current: i32,
    },
    LogStartRegressed {
        partition: i32,
        previous: i64,
        current: i64,
    },
    EndRegressed {
        partition: i32,
        previous: i64,
        current: i64,
    },
    BoundaryRecordChanged {
        partition: i32,
        offset: i64,
    },
    BoundaryRecordVerified {
        partition: i32,
        offset: i64,
    },
    BoundaryRecordAbsent {
        partition: i32,
        offset: i64,
        returned: Option<i64>,
    },
    BoundaryDeleted {
        partition: i32,
        offset: i64,
        log_start: i64,
    },
    CaptureGap {
        partition: i32,
        from: i64,
        to: i64,
    },
    ChangedDuringCapture {
        partition: i32,
        reason: &'static str,
    },
}

impl Signal {
    /// A signal that cannot occur within one continuous offset history.
    fn is_break(&self) -> bool {
        matches!(
            self,
            Signal::PartitionCountDecreased { .. }
                | Signal::LogStartRegressed { .. }
                | Signal::EndRegressed { .. }
                | Signal::BoundaryRecordChanged { .. }
                | Signal::ChangedDuringCapture { .. }
        )
    }

    fn to_json(&self) -> Value {
        match self {
            Signal::PartitionCountDecreased { previous, current } => {
                json!({"signal": "PartitionCountDecreased", "previous": previous, "current": current})
            }
            Signal::PartitionCountIncreased { previous, current } => {
                json!({"signal": "PartitionCountIncreased", "previous": previous, "current": current})
            }
            Signal::LogStartRegressed {
                partition,
                previous,
                current,
            } => json!({"signal": "LogStartRegressed", "partition": partition,
                        "previous": previous, "current": current}),
            Signal::EndRegressed {
                partition,
                previous,
                current,
            } => json!({"signal": "EndRegressed", "partition": partition,
                        "previous": previous, "current": current}),
            Signal::BoundaryRecordChanged { partition, offset } => {
                json!({"signal": "BoundaryRecordChanged", "partition": partition, "offset": offset})
            }
            Signal::BoundaryRecordVerified { partition, offset } => {
                json!({"signal": "BoundaryRecordVerified", "partition": partition, "offset": offset})
            }
            Signal::BoundaryRecordAbsent {
                partition,
                offset,
                returned,
            } => json!({"signal": "BoundaryRecordAbsent", "partition": partition,
                        "offset": offset, "returned": returned}),
            Signal::BoundaryDeleted {
                partition,
                offset,
                log_start,
            } => json!({"signal": "BoundaryDeleted", "partition": partition,
                        "offset": offset, "log_start": log_start}),
            Signal::CaptureGap {
                partition,
                from,
                to,
            } => json!({"signal": "CaptureGap", "partition": partition, "from": from, "to": to}),
            Signal::ChangedDuringCapture { partition, reason } => {
                json!({"signal": "ChangedDuringCapture", "partition": partition, "reason": reason})
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Verdict {
    /// No break signal, and at least one boundary record verified by content.
    Continuous,
    /// No break signal, and nothing left to verify content against.
    Unverified,
    /// No break signal, but partitions were added and nothing was verified:
    /// CreatePartitions and a recreation with more partitions look alike.
    Suspected,
    /// A signal that cannot occur within one continuous offset history.
    Break,
    /// No comparable previous observation (another cluster).
    Unknown,
}

impl Verdict {
    fn as_str(self) -> &'static str {
        match self {
            Verdict::Continuous => "continuous",
            Verdict::Unverified => "unverified",
            Verdict::Suspected => "suspected",
            Verdict::Break => "break",
            Verdict::Unknown => "unknown",
        }
    }
}

/// The reference rule: compare the current state of a topic with the
/// previous capture of the same (cluster, topic).
///
/// `probe` is `None` for the offsets-only variant, which the evidence reports
/// beside the full rule to show what the boundary probe adds.
fn classify(
    prev: &Capture,
    cur: &Current,
    mut probe: Option<&mut dyn FnMut(i32, i64) -> Probe>,
) -> (Verdict, Vec<Signal>) {
    if prev.cluster_id != cur.cluster_id {
        return (Verdict::Unknown, Vec::new());
    }
    let mut s = Vec::new();
    if cur.partition_count < prev.partition_count {
        s.push(Signal::PartitionCountDecreased {
            previous: prev.partition_count,
            current: cur.partition_count,
        });
    } else if cur.partition_count > prev.partition_count {
        s.push(Signal::PartitionCountIncreased {
            previous: prev.partition_count,
            current: cur.partition_count,
        });
    }
    for p in &prev.partitions {
        // A partition that no longer exists is PartitionCountDecreased above.
        let Some(m) = cur.marks.get(&p.partition) else {
            continue;
        };
        let prev_start = p.before.log_start.max(p.after.log_start);
        let mut prev_end = p.before.high_watermark.max(p.after.high_watermark);
        if let Some(a) = &p.archived {
            prev_end = prev_end.max(a.last_offset + 1);
        }
        if m.log_start < prev_start {
            s.push(Signal::LogStartRegressed {
                partition: p.partition,
                previous: prev_start,
                current: m.log_start,
            });
        }
        if m.high_watermark < prev_end {
            s.push(Signal::EndRegressed {
                partition: p.partition,
                previous: prev_end,
                current: m.high_watermark,
            });
        }
        let Some(a) = &p.archived else {
            continue;
        };
        if m.log_start > a.last_offset + 1 {
            s.push(Signal::CaptureGap {
                partition: p.partition,
                from: a.last_offset + 1,
                to: m.log_start,
            });
        }
        let Some(probe) = probe.as_deref_mut() else {
            continue;
        };
        for t in &a.tail {
            if t.offset < m.log_start {
                s.push(Signal::BoundaryDeleted {
                    partition: p.partition,
                    offset: t.offset,
                    log_start: m.log_start,
                });
                break;
            }
            if t.offset >= m.high_watermark {
                // Beyond the current end: EndRegressed already says so.
                continue;
            }
            match probe(p.partition, t.offset) {
                Probe::At(fp) if fp == t.fingerprint => {
                    s.push(Signal::BoundaryRecordVerified {
                        partition: p.partition,
                        offset: t.offset,
                    });
                    break;
                }
                Probe::At(_) => {
                    s.push(Signal::BoundaryRecordChanged {
                        partition: p.partition,
                        offset: t.offset,
                    });
                    break;
                }
                Probe::Absent(returned) => {
                    // Compacted away, or a marker: try the next older one.
                    s.push(Signal::BoundaryRecordAbsent {
                        partition: p.partition,
                        offset: t.offset,
                        returned,
                    });
                }
                Probe::OutOfRange => {
                    s.push(Signal::BoundaryDeleted {
                        partition: p.partition,
                        offset: t.offset,
                        log_start: m.log_start,
                    });
                    break;
                }
            }
        }
    }
    let verified = s
        .iter()
        .any(|x| matches!(x, Signal::BoundaryRecordVerified { .. }));
    let added = s
        .iter()
        .any(|x| matches!(x, Signal::PartitionCountIncreased { .. }));
    let verdict = if s.iter().any(Signal::is_break) {
        Verdict::Break
    } else if added && !verified {
        Verdict::Suspected
    } else if verified {
        Verdict::Continuous
    } else {
        Verdict::Unverified
    };
    (verdict, s)
}

/// The within-run check: what one capture says about itself.
fn intra_run(c: &Capture) -> Vec<Signal> {
    let mut s = Vec::new();
    for p in &c.partitions {
        let mut flag = |reason| {
            s.push(Signal::ChangedDuringCapture {
                partition: p.partition,
                reason,
            })
        };
        if p.after.log_start < p.before.log_start {
            flag("the log start offset regressed during the capture");
        }
        if p.after.high_watermark < p.before.high_watermark {
            flag("the end offset regressed during the capture");
        }
        if let Some(a) = &p.archived {
            if a.first_offset < p.before.log_start {
                flag("the capture archived an offset below the pre-run log start");
            }
            if a.last_offset >= p.after.high_watermark {
                flag("the capture archived an offset at or beyond the post-run end");
            }
        }
    }
    s
}

// ===========================================================================
// Broker, engine and archive plumbing.
// ===========================================================================

/// `cmd` to completion, or killed after `secs`: every subprocess this file
/// starts has a deadline.
fn run_bounded(mut cmd: Command, secs: u64, what: &str) -> Output {
    use std::io::Read;
    let mut child = cmd
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap_or_else(|e| panic!("{what}: could not start: {e}"));
    let mut out = child.stdout.take().expect("piped stdout");
    let mut err = child.stderr.take().expect("piped stderr");
    let t_out = std::thread::spawn(move || {
        let mut b = Vec::new();
        let _ = out.read_to_end(&mut b);
        b
    });
    let t_err = std::thread::spawn(move || {
        let mut b = Vec::new();
        let _ = err.read_to_end(&mut b);
        b
    });
    let deadline = Instant::now() + Duration::from_secs(secs);
    let status = loop {
        if let Some(status) = child.try_wait().expect("try_wait") {
            break status;
        }
        if Instant::now() >= deadline {
            let _ = child.kill();
            let _ = child.wait();
            panic!("{what}: still running after {secs}s, killed");
        }
        std::thread::sleep(Duration::from_millis(100));
    };
    Output {
        status,
        stdout: t_out.join().expect("stdout reader"),
        stderr: t_err.join().expect("stderr reader"),
    }
}

/// A Kafka command-line tool inside the RUNNING broker container.
fn broker_cli(args: &[&str], what: &str) -> String {
    let mut c = Command::new("docker");
    c.args([
        "compose",
        "-f",
        "e2e/compose/docker-compose.yml",
        "exec",
        "-T",
        "kafka-broker-1",
    ]);
    c.args(args);
    c.current_dir(root());
    let o = run_bounded(c, 120, what);
    assert!(
        o.status.success(),
        "{what} failed (exit {:?}) — is the stack up? run `just e2e-up`\n{}\n{}",
        o.status.code(),
        o.stdout_utf8(),
        o.stderr_utf8()
    );
    o.stdout_utf8()
}

/// The broker's own topic ID: the ground truth every row is judged against.
fn topic_id(topic: &str) -> String {
    let out = broker_cli(
        &[
            "/opt/kafka/bin/kafka-topics.sh",
            "--bootstrap-server",
            "kafka-broker-1:9094",
            "--describe",
            "--topic",
            topic,
        ],
        "kafka-topics --describe",
    );
    let id = out
        .split_whitespace()
        .skip_while(|w| *w != "TopicId:")
        .nth(1)
        .unwrap_or_else(|| panic!("no TopicId in the describe output for {topic}:\n{out}"));
    assert_eq!(
        id.len(),
        22,
        "a Kafka topic ID prints as 22 base64url characters: {id:?}"
    );
    id.to_string()
}

fn kafka_version() -> String {
    static V: std::sync::OnceLock<String> = std::sync::OnceLock::new();
    V.get_or_init(|| {
        broker_cli(
            &["/opt/kafka/bin/kafka-topics.sh", "--version"],
            "kafka-topics --version",
        )
        .split_whitespace()
        .next()
        .unwrap_or("unknown")
        .to_string()
    })
    .clone()
}

fn delete_records(topic: &str, to: &[(i32, i64)]) {
    let parts: Vec<String> = to
        .iter()
        .map(|(p, o)| format!(r#"{{"topic":"{topic}","partition":{p},"offset":{o}}}"#))
        .collect();
    let json = format!(r#"{{"partitions":[{}],"version":1}}"#, parts.join(","));
    let file = format!("/tmp/{topic}-delete-records.json");
    let script = format!(
        "printf '%s' '{json}' > {file} && /opt/kafka/bin/kafka-delete-records.sh \
         --bootstrap-server kafka-broker-1:9094 --offset-json-file {file}"
    );
    broker_cli(&["sh", "-c", &script], "kafka-delete-records");
}

fn alter_partitions(topic: &str, partitions: i32) {
    broker_cli(
        &[
            "/opt/kafka/bin/kafka-topics.sh",
            "--bootstrap-server",
            "kafka-broker-1:9094",
            "--alter",
            "--topic",
            topic,
            "--partitions",
            &partitions.to_string(),
        ],
        "kafka-topics --alter --partitions",
    );
    wait_for(
        60,
        &format!("{topic} to report {partitions} partitions"),
        || partition_count(topic) == Some(partitions),
    );
}

fn alter_config(topic: &str, entry: &str) {
    broker_cli(
        &[
            "/opt/kafka/bin/kafka-configs.sh",
            "--bootstrap-server",
            "kafka-broker-1:9094",
            "--alter",
            "--entity-type",
            "topics",
            "--entity-name",
            topic,
            "--add-config",
            entry,
        ],
        "kafka-configs --alter",
    );
}

fn wait_for(secs: u64, what: &str, mut ready: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(secs);
    while Instant::now() < deadline {
        if ready() {
            return;
        }
        std::thread::sleep(Duration::from_millis(500));
    }
    panic!("timed out after {secs}s waiting for {what}");
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Isolation {
    ReadUncommitted,
    ReadCommitted,
}

impl Isolation {
    fn as_str(self) -> &'static str {
        match self {
            Isolation::ReadUncommitted => "read_uncommitted",
            Isolation::ReadCommitted => "read_committed",
        }
    }
}

fn consumer(isolation: Isolation) -> BaseConsumer {
    ClientConfig::new()
        .set("bootstrap.servers", BOOTSTRAP)
        .set("group.id", "logweir-topic-identity-oracle")
        .set("enable.auto.commit", "false")
        .set("enable.partition.eof", "true")
        .set("auto.offset.reset", "error")
        .set("allow.auto.create.topics", "false")
        .set("isolation.level", isolation.as_str())
        .create()
        .expect("a consumer for the compose broker")
}

/// Healthy partition count, or `None` while the topic is absent or electing.
fn partition_count(topic: &str) -> Option<i32> {
    let c = consumer(Isolation::ReadUncommitted);
    let md = c.fetch_metadata(Some(topic), T).ok()?;
    let t = md.topics().first()?;
    if t.error().is_some() || t.partitions().is_empty() {
        return None;
    }
    if t.partitions().iter().any(|p| p.leader() < 0) {
        return None;
    }
    Some(t.partitions().len() as i32)
}

fn marks(topic: &str, isolation: Isolation) -> BTreeMap<i32, Marks> {
    let c = consumer(isolation);
    let n = partition_count(topic).unwrap_or_else(|| panic!("{topic} has no healthy metadata"));
    (0..n)
        .map(|p| {
            let (lo, hi) = c
                .fetch_watermarks(topic, p, T)
                .unwrap_or_else(|e| panic!("watermarks {topic}/{p}: {e}"));
            (
                p,
                Marks {
                    log_start: lo,
                    high_watermark: hi,
                },
            )
        })
        .collect()
}

fn message_fingerprint(m: &BorrowedMessage<'_>) -> String {
    let headers: Vec<(String, Option<Vec<u8>>)> = m
        .headers()
        .map(|hs| {
            (0..hs.count())
                .map(|i| {
                    let h = hs.get(i);
                    (h.key.to_string(), h.value.map(|v| v.to_vec()))
                })
                .collect()
        })
        .unwrap_or_default();
    record_fingerprint(
        m.key(),
        m.payload(),
        &headers,
        m.timestamp().to_millis().unwrap_or(-1),
    )
}

/// A read at EXACTLY `offset`, READ_UNCOMMITTED. The first record returned
/// counts only if it carries the requested offset.
fn probe(topic: &str, partition: i32, offset: i64) -> Probe {
    let c = consumer(Isolation::ReadUncommitted);
    let mut tpl = TopicPartitionList::new();
    tpl.add_partition_offset(topic, partition, Offset::Offset(offset))
        .expect("add partition");
    c.assign(&tpl).expect("assign");
    let deadline = Instant::now() + T;
    while Instant::now() < deadline {
        match c.poll(Duration::from_millis(500)) {
            None => continue,
            Some(Err(KafkaError::PartitionEOF(_))) => return Probe::Absent(None),
            Some(Err(KafkaError::MessageConsumption(RDKafkaErrorCode::AutoOffsetReset))) => {
                return Probe::OutOfRange
            }
            Some(Err(e)) => panic!("probe {topic}/{partition}@{offset}: {e}"),
            Some(Ok(m)) if m.offset() == offset => return Probe::At(message_fingerprint(&m)),
            Some(Ok(m)) => return Probe::Absent(Some(m.offset())),
        }
    }
    panic!("probe {topic}/{partition}@{offset}: no answer within {T:?}");
}

/// The fingerprint of the SOURCE record an archived record came from, and of
/// the archived bytes verbatim.
///
/// Logweir renders `include_offset_headers: true`, so the engine appends
/// `x-original-offset` and `x-original-timestamp` (little-endian `i64`) AFTER
/// the record's own headers. Only those two TRAILING headers are removed, and
/// only when their values are this record's own offset and timestamp: a
/// record that already carried such headers at the source (a topic restored
/// earlier, whose records keep the pair from their first archive) keeps its
/// own inner pair.
fn archived_fingerprints(r: &ArchivedRecord) -> (String, String) {
    let raw = record_fingerprint(
        r.key.as_deref(),
        r.value.as_deref(),
        &r.headers,
        r.timestamp,
    );
    let n = r.headers.len();
    assert!(
        n >= 2,
        "archived record at {} carries {n} headers; the engine appends two",
        r.offset
    );
    let (own, appended) = r.headers.split_at(n - 2);
    assert_eq!(appended[0].0, "x-original-offset", "at {}", r.offset);
    assert_eq!(
        appended[0].1.as_deref(),
        Some(&r.offset.to_le_bytes()[..]),
        "x-original-offset at {} is not the record's own offset",
        r.offset
    );
    assert_eq!(appended[1].0, "x-original-timestamp", "at {}", r.offset);
    assert_eq!(
        appended[1].1.as_deref(),
        Some(&r.timestamp.to_le_bytes()[..]),
        "x-original-timestamp at {} is not the record's own timestamp",
        r.offset
    );
    let source = record_fingerprint(r.key.as_deref(), r.value.as_deref(), own, r.timestamp);
    (source, raw)
}

/// One capture's archive, read back.
struct Archive {
    partition_count: Option<i64>,
    partitions: BTreeMap<i32, (Archived, Vec<ArchivedRecord>)>,
    gaps: Value,
}

/// The pinned engine backs `topic` up into a filesystem archive under
/// `engine_mount()`, the one host directory its container route can write.
fn engine_backup(dir: &Path, backup_id: &str, topic: &str) -> Archive {
    let archive = dir.join("archive");
    std::fs::create_dir_all(&archive).expect("archive dir");
    let cfg = dir.join(format!("{backup_id}.yaml"));
    std::fs::write(
        &cfg,
        format!(
            "mode: backup\nbackup_id: \"{backup_id}\"\nsource:\n  bootstrap_servers:\n    - {BOOTSTRAP}\n  \
             topics:\n    include:\n      - \"{topic}\"\nstorage:\n  backend: filesystem\n  path: \"{}\"\n\
             backup:\n  compression: zstd\n  continuous: false\n  segment_max_records: 1000\n  \
             include_offset_headers: true\n",
            archive.display()
        ),
    )
    .expect("engine config");
    let mut c = Command::new(engine_bin());
    c.env("LOGWEIR_E2E_ENGINE_MOUNT", engine_mount())
        .env("RUST_LOG", "warn")
        .arg("backup")
        .arg("--config")
        .arg(&cfg)
        .current_dir(root());
    let o = run_bounded(c, 300, "engine backup");
    assert!(
        o.status.success(),
        "engine backup of {topic} exited {:?}\n{}\n{}",
        o.status.code(),
        o.stdout_utf8(),
        o.stderr_utf8()
    );
    let manifest_path = archive.join(backup_id).join("manifest.json");
    let manifest: Value = serde_json::from_slice(
        &std::fs::read(&manifest_path)
            .unwrap_or_else(|e| panic!("{}: {e}", manifest_path.display())),
    )
    .expect("manifest json");
    let entry = manifest["topics"]
        .as_array()
        .and_then(|ts| ts.iter().find(|t| t["name"] == topic))
        .unwrap_or_else(|| panic!("manifest names no topic {topic}: {manifest}"))
        .clone();
    let mut partitions = BTreeMap::new();
    let mut gaps = serde_json::Map::new();
    for p in entry["partitions"].as_array().cloned().unwrap_or_default() {
        let pid = p["partition_id"].as_i64().expect("partition_id") as i32;
        let segments = p["segments"].as_array().cloned().unwrap_or_default();
        if segments.is_empty() {
            continue;
        }
        if let Some(g) = p.get("gaps") {
            gaps.insert(pid.to_string(), g.clone());
        }
        let mut records = Vec::new();
        let (mut first_ts, mut last_ts) = (None, None);
        let (mut lo, mut hi) = (i64::MAX, i64::MIN);
        for s in &segments {
            let key = s["key"].as_str().expect("segment key");
            let bytes = std::fs::read(archive.join(key)).unwrap_or_else(|e| panic!("{key}: {e}"));
            records.extend(decode_segment(&bytes).unwrap_or_else(|e| panic!("{key}: {e}")));
            let start = s["start_offset"].as_i64().expect("start_offset");
            let end = s["end_offset"].as_i64().expect("end_offset");
            if start < lo {
                lo = start;
                first_ts = s["start_timestamp"].as_i64();
            }
            if end > hi {
                hi = end;
                last_ts = s["end_timestamp"].as_i64();
            }
        }
        records.sort_by_key(|r| r.offset);
        let first = records.first().expect("a segment decodes to records");
        let last = records.last().expect("a segment decodes to records");
        assert_eq!(
            (first.offset, last.offset),
            (lo, hi),
            "{topic}/{pid}: the manifest's offset bounds disagree with the decoded records"
        );
        let tail = records
            .iter()
            .rev()
            .take(TAIL)
            .map(|r| {
                let (fingerprint, raw_fingerprint) = archived_fingerprints(r);
                TailRecord {
                    offset: r.offset,
                    fingerprint,
                    raw_fingerprint,
                }
            })
            .collect();
        let archived = Archived {
            first_offset: lo,
            last_offset: hi,
            first_timestamp_ms: first_ts.expect("start_timestamp"),
            last_timestamp_ms: last_ts.expect("end_timestamp"),
            records: records.len(),
            tail,
        };
        partitions.insert(pid, (archived, records));
    }
    Archive {
        partition_count: entry["original_partition_count"].as_i64(),
        partitions,
        gaps: Value::Object(gaps),
    }
}

/// One capture: marks before (both isolation levels), the engine, marks after.
struct Run {
    capture: Capture,
    rc_before: BTreeMap<i32, Marks>,
    rc_after: BTreeMap<i32, Marks>,
    archive: Archive,
}

fn capture(topic: &str, dir: &Path, backup_id: &str) -> Run {
    let cluster = cluster_id();
    let count = partition_count(topic).expect("healthy topic before the capture");
    let before = marks(topic, Isolation::ReadUncommitted);
    let rc_before = marks(topic, Isolation::ReadCommitted);
    let archive = engine_backup(dir, backup_id, topic);
    let after = marks(topic, Isolation::ReadUncommitted);
    let rc_after = marks(topic, Isolation::ReadCommitted);
    let partitions = (0..count)
        .map(|p| PartitionObs {
            partition: p,
            before: before[&p],
            after: after[&p],
            archived: archive.partitions.get(&p).map(|(a, _)| a.clone()),
        })
        .collect();
    Run {
        capture: Capture {
            cluster_id: cluster,
            partition_count: count,
            partitions,
        },
        rc_before,
        rc_after,
        archive,
    }
}

fn current(topic: &str, isolation: Isolation) -> Current {
    Current {
        cluster_id: cluster_id(),
        partition_count: partition_count(topic).expect("healthy topic"),
        marks: marks(topic, isolation),
    }
}

/// One record to produce, fully specified.
#[derive(Debug, Clone)]
struct Rec {
    partition: i32,
    key: Option<Vec<u8>>,
    value: Option<Vec<u8>>,
    headers: Vec<(String, Option<Vec<u8>>)>,
    ts: i64,
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock after 1970")
        .as_millis() as i64
}

/// `per_partition` records on each partition, each with one header of its
/// own so the strip in [`archived_fingerprints`] has something to keep.
fn recs(partitions: i32, per_partition: usize, tag: &str, ts0: i64) -> Vec<Rec> {
    let mut out = Vec::new();
    for i in 0..per_partition {
        for p in 0..partitions {
            out.push(Rec {
                partition: p,
                key: Some(format!("{tag}-k{p}-{i}").into_bytes()),
                value: Some(format!("{tag}-v{p}-{i}").into_bytes()),
                headers: vec![("lw-fixture".to_string(), Some(tag.as_bytes().to_vec()))],
                ts: ts0 + (i as i64) * 10 + i64::from(p),
            });
        }
    }
    out
}

fn send(producer: &BaseProducer, topic: &str, r: &Rec) {
    let mut headers = OwnedHeaders::new_with_capacity(r.headers.len());
    for (k, v) in &r.headers {
        headers = headers.insert(Header {
            key: k.as_str(),
            value: v.as_deref(),
        });
    }
    let mut record: BaseRecord<'_, [u8], [u8]> = BaseRecord::to(topic)
        .partition(r.partition)
        .timestamp(r.ts)
        .headers(headers);
    if let Some(k) = &r.key {
        record = record.key(&k[..]);
    }
    if let Some(v) = &r.value {
        record = record.payload(&v[..]);
    }
    producer
        .send(record)
        .unwrap_or_else(|(e, _)| panic!("enqueue into {topic}/{}: {e}", r.partition));
}

fn producer(extra: &[(&str, &str)]) -> BaseProducer {
    let mut cfg = ClientConfig::new();
    cfg.set("bootstrap.servers", BOOTSTRAP)
        .set("acks", "all")
        .set("enable.idempotence", "true")
        .set("message.timeout.ms", "10000");
    for (k, v) in extra {
        cfg.set(*k, *v);
    }
    cfg.create().expect("a producer for the compose broker")
}

/// Produces `records` in order and returns once every partition's
/// READ_UNCOMMITTED end offset has advanced by what was sent to it.
fn produce(topic: &str, records: &[Rec]) {
    let before = marks(topic, Isolation::ReadUncommitted);
    let p = producer(&[]);
    for r in records {
        send(&p, topic, r);
    }
    p.flush(Duration::from_secs(15)).expect("flush");
    let mut want = BTreeMap::new();
    for r in records {
        *want.entry(r.partition).or_insert(0i64) += 1;
    }
    wait_for(20, &format!("{topic}'s end offsets to advance"), || {
        let now = marks(topic, Isolation::ReadUncommitted);
        want.iter().all(|(p, n)| {
            now.get(p).map(|m| m.high_watermark).unwrap_or(0)
                >= before.get(p).map(|m| m.high_watermark).unwrap_or(0) + n
        })
    });
}

fn create_topic_exact(topic: &str, partitions: i32, configs: &[(&str, &str)]) {
    let spec = NewTopicSpec {
        name: topic.to_string(),
        num_partitions: partitions,
        replication_factor: 1,
        configs: configs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect(),
    };
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        let res = TopicCreator::create_topics(&reader(), std::slice::from_ref(&spec))
            .expect("create_topics");
        match &res[0].1 {
            Ok(()) => break,
            // A name whose deletion the controller is still processing.
            Err(e) if Instant::now() < deadline => {
                eprintln!("[topic-identity] create {topic}: {e}; retrying");
                std::thread::sleep(Duration::from_millis(500));
            }
            Err(e) => panic!("create {topic}: {e}"),
        }
    }
    wait_for(
        60,
        &format!("{topic} to report {partitions} partitions"),
        || partition_count(topic) == Some(partitions),
    );
}

fn delete_topic(topic: &str) {
    assert!(
        topic.starts_with(PREFIX),
        "{topic} is not a topic this file owns"
    );
    let r = reader()
        .with_scratch_prefix(PREFIX)
        .expect("the prefix is long enough");
    let _ = TopicDeleter::delete_topics(&r, &[topic.to_string()]);
    wait_for(60, &format!("{topic} to disappear"), || {
        !ClusterReader::list_topics(&reader())
            .map(|ts| ts.iter().any(|t| t.name == topic))
            .unwrap_or(true)
    });
}

fn marks_json(m: &BTreeMap<i32, Marks>) -> Value {
    Value::Object(
        m.iter()
            .map(|(p, x)| {
                (
                    p.to_string(),
                    json!({"log_start": x.log_start, "high_watermark": x.high_watermark}),
                )
            })
            .collect(),
    )
}

fn run_json(r: &Run) -> Value {
    let parts: Vec<Value> = r
        .capture
        .partitions
        .iter()
        .map(|p| {
            json!({
                "partition": p.partition,
                "before": {"log_start": p.before.log_start, "high_watermark": p.before.high_watermark},
                "after": {"log_start": p.after.log_start, "high_watermark": p.after.high_watermark},
                "archived": p.archived.as_ref().map(|a| json!({
                    "first_offset": a.first_offset,
                    "last_offset": a.last_offset,
                    "first_timestamp_ms": a.first_timestamp_ms,
                    "last_timestamp_ms": a.last_timestamp_ms,
                    "records": a.records,
                    "tail": a.tail.iter().map(|t| json!({
                        "offset": t.offset,
                        "fingerprint": t.fingerprint,
                        "raw_fingerprint": t.raw_fingerprint,
                    })).collect::<Vec<_>>(),
                })),
            })
        })
        .collect();
    json!({
        "cluster_id": r.capture.cluster_id,
        "partition_count": r.capture.partition_count,
        "engine_partition_count": r.archive.partition_count,
        "engine_gaps": r.archive.gaps,
        "partitions": parts,
        "read_committed_before": marks_json(&r.rc_before),
        "read_committed_after": marks_json(&r.rc_after),
        "intra_run": intra_run(&r.capture).iter().map(Signal::to_json).collect::<Vec<_>>(),
    })
}

/// One live row: its topic, its scratch directory and its evidence.
struct Case {
    name: &'static str,
    topic: String,
    dir: PathBuf,
    runs: u32,
    evidence: serde_json::Map<String, Value>,
}

impl Case {
    fn start(name: &'static str) -> Case {
        let nonce = format!("{:x}", now_ms());
        let topic = format!("{PREFIX}{name}-{nonce}");
        let dir = engine_mount().join("topic-identity").join(&topic);
        std::fs::create_dir_all(&dir).expect("case dir");
        eprintln!("[topic-identity] {name}: topic {topic}");
        let mut evidence = serde_json::Map::new();
        evidence.insert("case".into(), json!(name));
        evidence.insert("topic".into(), json!(topic));
        evidence.insert("kafka_version".into(), json!(kafka_version()));
        evidence.insert("engine_digest".into(), json!(engine_digest()));
        evidence.insert(
            "engine_route".into(),
            json!(engine_bin().display().to_string()),
        );
        Case {
            name,
            topic,
            dir,
            runs: 0,
            evidence,
        }
    }

    fn create(&self, partitions: i32, configs: &[(&str, &str)]) {
        create_topic_exact(&self.topic, partitions, configs);
    }

    fn recreate(&self, partitions: i32, configs: &[(&str, &str)]) {
        delete_topic(&self.topic);
        create_topic_exact(&self.topic, partitions, configs);
    }

    fn capture(&mut self) -> Run {
        self.runs += 1;
        let id = format!("{}-r{}", self.topic, self.runs);
        let run = capture(&self.topic, &self.dir, &id);
        self.evidence
            .insert(format!("run{}", self.runs), run_json(&run));
        run
    }

    fn note(&mut self, key: &str, v: Value) {
        self.evidence.insert(key.to_string(), v);
    }

    /// Classifies the current state against `prev` with both rule variants,
    /// records everything, and returns the full rule's verdict and signals.
    fn classify(&mut self, prev: &Run, isolation: Isolation) -> (Verdict, Vec<Signal>) {
        let cur = current(&self.topic, isolation);
        let topic = self.topic.clone();
        let mut probes = Vec::new();
        let mut do_probe = |p: i32, o: i64| {
            let r = probe(&topic, p, o);
            probes.push(json!({"partition": p, "offset": o, "result": format!("{r:?}")}));
            r
        };
        let (verdict, signals) = classify(&prev.capture, &cur, Some(&mut do_probe));
        let (offsets_only, _) = classify(&prev.capture, &cur, None);
        let key = format!("classified_{}", isolation.as_str());
        self.evidence.insert(
            key,
            json!({
                "now": {
                    "partition_count": cur.partition_count,
                    "marks": marks_json(&cur.marks),
                },
                "probes": probes,
                "signals": signals.iter().map(Signal::to_json).collect::<Vec<_>>(),
                "verdict": verdict.as_str(),
                "verdict_offsets_only": if offsets_only == Verdict::Break { "break" } else { "no_break" },
            }),
        );
        (verdict, signals)
    }

    /// Writes the evidence line: the ground truth and the verdict beside it.
    fn finish(&mut self, same_generation: bool, verdict: Verdict, expected: Verdict) {
        let class = match (same_generation, verdict) {
            (false, Verdict::Break) | (false, Verdict::Suspected) => "true_positive",
            (false, _) => "false_negative",
            (true, Verdict::Break) | (true, Verdict::Suspected) => "false_positive",
            (true, _) => "true_negative",
        };
        self.evidence.insert(
            "ground_truth".into(),
            json!({"same_generation": same_generation}),
        );
        self.evidence
            .insert("verdict".into(), json!(verdict.as_str()));
        self.evidence
            .insert("expected".into(), json!(expected.as_str()));
        self.evidence.insert("classification".into(), json!(class));
        if let Ok(path) = std::env::var("LOGWEIR_TOPIC_IDENTITY_EVIDENCE") {
            use std::io::Write;
            let mut f = std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(&path)
                .unwrap_or_else(|e| panic!("{path}: {e}"));
            writeln!(f, "{}", Value::Object(self.evidence.clone())).expect("evidence line");
        }
        eprintln!(
            "[topic-identity] {}: verdict {} (expected {}), same generation {same_generation}: {class}",
            self.name,
            verdict.as_str(),
            expected.as_str()
        );
    }
}

impl Drop for Case {
    /// Best effort, also on a failed assertion: the row's topic and archive.
    /// Never panics, so a failing row cannot turn into a double panic.
    fn drop(&mut self) {
        let r = RdKafkaReader::connect(&[BOOTSTRAP.to_string()], AuthConfig::Plaintext)
            .and_then(|r| r.with_scratch_prefix(PREFIX));
        if let Ok(r) = r {
            let _ = TopicDeleter::delete_topics(&r, std::slice::from_ref(&self.topic));
        }
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

/// Topic settings every row starts from: no retention, record timestamps kept.
const PLAIN: &[(&str, &str)] = &[
    ("retention.ms", "-1"),
    ("message.timestamp.type", "CreateTime"),
];

// ===========================================================================
// Live rows. Ground truth first, then the verdict.
// ===========================================================================

/// c01: recreated with the same partition count and fewer records. The end
/// offsets regress: the offsets-only rule already sees it.
#[test]
fn c01_recreate_same_partition_count_shorter() {
    let mut c = Case::start("c01");
    c.create(3, PLAIN);
    produce(&c.topic, &recs(3, 10, "g1", now_ms() - 60_000));
    let id1 = topic_id(&c.topic);
    let run1 = c.capture();
    c.recreate(3, PLAIN);
    produce(&c.topic, &recs(3, 4, "g2", now_ms() - 30_000));
    let id2 = topic_id(&c.topic);
    assert_ne!(id1, id2, "the fixture must have made a new topic");
    let (v, s) = c.classify(&run1, Isolation::ReadUncommitted);
    assert!(
        s.iter().any(|x| matches!(x, Signal::EndRegressed { .. })),
        "{s:?}"
    );
    c.finish(false, v, Verdict::Break);
    assert_eq!(v, Verdict::Break, "{s:?}");
}

/// c02: recreated with the same partition count and refilled past the old
/// end. No offset regresses; only the boundary probe sees the new topic.
#[test]
fn c02_recreate_same_partition_count_refilled_past_the_old_end() {
    let mut c = Case::start("c02");
    c.create(3, PLAIN);
    produce(&c.topic, &recs(3, 10, "g1", now_ms() - 60_000));
    let id1 = topic_id(&c.topic);
    let run1 = c.capture();
    c.recreate(3, PLAIN);
    produce(&c.topic, &recs(3, 15, "g2", now_ms() - 30_000));
    let id2 = topic_id(&c.topic);
    assert_ne!(id1, id2, "the fixture must have made a new topic");
    let (v, s) = c.classify(&run1, Isolation::ReadUncommitted);
    let offsets_only = classify(
        &run1.capture,
        &current(&c.topic, Isolation::ReadUncommitted),
        None,
    )
    .0;
    assert_ne!(
        offsets_only,
        Verdict::Break,
        "the offsets-only rule misses this row by construction"
    );
    assert!(
        s.iter()
            .any(|x| matches!(x, Signal::BoundaryRecordChanged { .. })),
        "{s:?}"
    );
    c.finish(false, v, Verdict::Break);
    assert_eq!(v, Verdict::Break, "{s:?}");
}

/// c03: recreated with fewer partitions. Kafka never removes a partition
/// from a live topic, so the count alone is proof.
#[test]
fn c03_recreate_with_fewer_partitions() {
    let mut c = Case::start("c03");
    c.create(3, PLAIN);
    produce(&c.topic, &recs(3, 10, "g1", now_ms() - 60_000));
    let id1 = topic_id(&c.topic);
    let run1 = c.capture();
    c.recreate(1, PLAIN);
    produce(&c.topic, &recs(1, 30, "g2", now_ms() - 30_000));
    let id2 = topic_id(&c.topic);
    assert_ne!(id1, id2, "the fixture must have made a new topic");
    let (v, s) = c.classify(&run1, Isolation::ReadUncommitted);
    assert!(
        s.iter()
            .any(|x| matches!(x, Signal::PartitionCountDecreased { .. })),
        "{s:?}"
    );
    c.finish(false, v, Verdict::Break);
    assert_eq!(v, Verdict::Break, "{s:?}");
}

/// c04: recreated with more partitions and refilled. The probe on the old
/// partitions finds different records.
#[test]
fn c04_recreate_with_more_partitions() {
    let mut c = Case::start("c04");
    c.create(3, PLAIN);
    produce(&c.topic, &recs(3, 10, "g1", now_ms() - 60_000));
    let id1 = topic_id(&c.topic);
    let run1 = c.capture();
    c.recreate(5, PLAIN);
    produce(&c.topic, &recs(5, 15, "g2", now_ms() - 30_000));
    let id2 = topic_id(&c.topic);
    assert_ne!(id1, id2, "the fixture must have made a new topic");
    let (v, s) = c.classify(&run1, Isolation::ReadUncommitted);
    assert!(
        s.iter()
            .any(|x| matches!(x, Signal::BoundaryRecordChanged { .. })),
        "{s:?}"
    );
    c.finish(false, v, Verdict::Break);
    assert_eq!(v, Verdict::Break, "{s:?}");
}

/// c05 (negative control): partitions ADDED to the same topic. Not a break:
/// the old partitions verify by content.
#[test]
fn c05_create_partitions_keeps_the_generation() {
    let mut c = Case::start("c05");
    c.create(3, PLAIN);
    produce(&c.topic, &recs(3, 10, "g1", now_ms() - 60_000));
    let id1 = topic_id(&c.topic);
    let run1 = c.capture();
    alter_partitions(&c.topic, 5);
    produce(&c.topic, &recs(5, 5, "g1b", now_ms() - 30_000));
    let id2 = topic_id(&c.topic);
    assert_eq!(id1, id2, "CreatePartitions keeps the topic ID");
    let (v, s) = c.classify(&run1, Isolation::ReadUncommitted);
    assert!(
        s.iter()
            .any(|x| matches!(x, Signal::PartitionCountIncreased { .. })),
        "{s:?}"
    );
    // The strip is what makes verification possible at all: the archived
    // bytes verbatim never match the source record.
    for p in &run1.capture.partitions {
        let a = p.archived.as_ref().expect("archived");
        assert!(a.tail.iter().all(|t| t.raw_fingerprint != t.fingerprint));
    }
    c.finish(true, v, Verdict::Continuous);
    assert_eq!(v, Verdict::Continuous, "{s:?}");
}

/// c06 (negative control): DeleteRecords inside the archived range, and on
/// one partition exactly past it. The log start advances; nothing breaks.
#[test]
fn c06_delete_records_inside_the_archived_range() {
    let mut c = Case::start("c06");
    c.create(3, PLAIN);
    produce(&c.topic, &recs(3, 10, "g1", now_ms() - 60_000));
    let id1 = topic_id(&c.topic);
    let run1 = c.capture();
    delete_records(&c.topic, &[(0, 5), (1, 9), (2, 10)]);
    let id2 = topic_id(&c.topic);
    assert_eq!(id1, id2, "DeleteRecords keeps the topic ID");
    let m = marks(&c.topic, Isolation::ReadUncommitted);
    assert_eq!(
        (m[&0].log_start, m[&1].log_start, m[&2].log_start),
        (5, 9, 10),
        "the fixture must have advanced the log starts"
    );
    let (v, s) = c.classify(&run1, Isolation::ReadUncommitted);
    assert!(
        !s.iter().any(|x| matches!(x, Signal::CaptureGap { .. })),
        "{s:?}"
    );
    c.finish(true, v, Verdict::Continuous);
    assert_eq!(v, Verdict::Continuous, "{s:?}");
}

/// c07 (negative control): records produced after the capture and deleted
/// before the next one. Same topic, and a capture gap the next point must
/// report.
#[test]
fn c07_delete_records_past_the_archived_end_is_a_gap_not_a_break() {
    let mut c = Case::start("c07");
    c.create(3, PLAIN);
    produce(&c.topic, &recs(3, 10, "g1", now_ms() - 60_000));
    let id1 = topic_id(&c.topic);
    let run1 = c.capture();
    produce(&c.topic, &recs(3, 10, "g1b", now_ms() - 30_000));
    delete_records(&c.topic, &[(0, 15), (1, 15), (2, 15)]);
    let id2 = topic_id(&c.topic);
    assert_eq!(id1, id2, "DeleteRecords keeps the topic ID");
    let (v, s) = c.classify(&run1, Isolation::ReadUncommitted);
    for p in 0..3 {
        assert!(
            s.contains(&Signal::CaptureGap {
                partition: p,
                from: 10,
                to: 15
            }),
            "partition {p}: {s:?}"
        );
    }
    c.finish(true, v, Verdict::Unverified);
    assert_eq!(v, Verdict::Unverified, "{s:?}");
}

/// c08 (negative control): every record deleted (log start = end).
#[test]
fn c08_delete_all_records() {
    let mut c = Case::start("c08");
    c.create(3, PLAIN);
    produce(&c.topic, &recs(3, 10, "g1", now_ms() - 60_000));
    let id1 = topic_id(&c.topic);
    let run1 = c.capture();
    delete_records(&c.topic, &[(0, 10), (1, 10), (2, 10)]);
    let id2 = topic_id(&c.topic);
    assert_eq!(id1, id2, "DeleteRecords keeps the topic ID");
    let (v, s) = c.classify(&run1, Isolation::ReadUncommitted);
    c.finish(true, v, Verdict::Unverified);
    assert_eq!(v, Verdict::Unverified, "{s:?}");
}

/// c09 (negative control): compaction removes the archived tail. The log
/// start does not move and the end only grows; the tail records are absent
/// and the probe must not read the next surviving record as a mismatch.
#[test]
fn c09_compaction_removes_the_archived_tail() {
    let mut c = Case::start("c09");
    let compact: &[(&str, &str)] = &[
        ("cleanup.policy", "compact"),
        ("min.cleanable.dirty.ratio", "0.01"),
        ("min.compaction.lag.ms", "0"),
        ("segment.ms", "100"),
        ("message.timestamp.type", "CreateTime"),
    ];
    c.create(1, compact);
    let ts0 = now_ms() - 60_000;
    // Five keys, four rounds: offsets 0..19, the tail is round 3 of k2..k4.
    let round = |r: usize, tag: &str, ts: i64| -> Vec<Rec> {
        (0..5)
            .map(|k| Rec {
                partition: 0,
                key: Some(format!("k{k}").into_bytes()),
                value: Some(format!("{tag}-r{r}-k{k}").into_bytes()),
                headers: vec![("lw-fixture".into(), Some(tag.as_bytes().to_vec()))],
                ts: ts + (r as i64) * 10 + k,
            })
            .collect()
    };
    for r in 0..4 {
        produce(&c.topic, &round(r, "g1", ts0));
    }
    let id1 = topic_id(&c.topic);
    let run1 = c.capture();
    let tail: Vec<i64> = run1.capture.partitions[0]
        .archived
        .as_ref()
        .expect("archived")
        .tail
        .iter()
        .map(|t| t.offset)
        .collect();
    assert_eq!(tail, vec![19, 18, 17]);
    // Overwrite every key, then roll the segment holding the overwrites.
    produce(&c.topic, &round(4, "g1b", ts0 + 1_000));
    for i in 0..2 {
        std::thread::sleep(Duration::from_millis(300));
        produce(
            &c.topic,
            &[Rec {
                partition: 0,
                key: Some(format!("roll-{i}").into_bytes()),
                value: Some(b"roll".to_vec()),
                headers: vec![],
                ts: ts0 + 2_000 + i,
            }],
        );
    }
    let topic = c.topic.clone();
    wait_for(180, "the log cleaner to remove offsets 17..19", || {
        [17, 18, 19]
            .iter()
            .all(|o| matches!(probe(&topic, 0, *o), Probe::Absent(Some(_))))
    });
    let id2 = topic_id(&c.topic);
    assert_eq!(id1, id2, "compaction keeps the topic ID");
    // What an inexact probe would have compared: the first record at or
    // after the old tail is a different record, so "first record from L"
    // reads compaction as a new topic.
    let next = probe(&c.topic, 0, 19);
    c.note(
        "inexact_probe_first_record_after_19",
        json!(format!("{next:?}")),
    );
    let (v, s) = c.classify(&run1, Isolation::ReadUncommitted);
    assert!(
        s.iter()
            .filter(|x| matches!(x, Signal::BoundaryRecordAbsent { .. }))
            .count()
            == 3,
        "{s:?}"
    );
    c.finish(true, v, Verdict::Unverified);
    assert_eq!(v, Verdict::Unverified, "{s:?}");
}

/// c10 (negative control): retention expiry. Waits for the broker's
/// retention check, which runs every five minutes by default.
#[test]
#[ignore = "waits up to seven minutes for the broker's five-minute retention check; run with --ignored"]
fn c10_retention_expiry_advances_the_log_start() {
    let mut c = Case::start("c10");
    c.create(1, PLAIN);
    produce(&c.topic, &recs(1, 10, "g1", now_ms() - 60_000));
    let id1 = topic_id(&c.topic);
    let run1 = c.capture();
    produce(&c.topic, &recs(1, 5, "g1b", now_ms() - 30_000));
    alter_config(&c.topic, "retention.ms=1000");
    let started = Instant::now();
    let topic = c.topic.clone();
    wait_for(420, "retention to delete every segment", || {
        marks(&topic, Isolation::ReadUncommitted)[&0].log_start >= 15
    });
    c.note("retention_wait_seconds", json!(started.elapsed().as_secs()));
    let id2 = topic_id(&c.topic);
    assert_eq!(id1, id2, "retention keeps the topic ID");
    let (v, s) = c.classify(&run1, Isolation::ReadUncommitted);
    assert!(
        s.contains(&Signal::CaptureGap {
            partition: 0,
            from: 10,
            to: 15
        }),
        "{s:?}"
    );
    c.finish(true, v, Verdict::Unverified);
    assert_eq!(v, Verdict::Unverified, "{s:?}");
}

/// c11 (negative control): an open transaction during and after the
/// capture. READ_UNCOMMITTED marks agree with what the engine archived;
/// librdkafka's default READ_COMMITTED marks stop at the last stable offset
/// and read as a regression, which is why the rule requires the former.
#[test]
fn c11_an_open_transaction_needs_read_uncommitted_marks() {
    let mut c = Case::start("c11");
    c.create(1, PLAIN);
    let ts0 = now_ms() - 60_000;
    produce(&c.topic, &recs(1, 5, "g1", ts0));
    let txid = format!("{}-txn", c.topic);
    let txp = producer(&[("transactional.id", txid.as_str())]);
    txp.init_transactions(Duration::from_secs(30))
        .expect("init_transactions");
    txp.begin_transaction().expect("begin_transaction");
    for r in recs(1, 3, "txn", ts0 + 1_000) {
        send(&txp, &c.topic, &r);
    }
    txp.flush(Duration::from_secs(15)).expect("flush");
    let topic = c.topic.clone();
    wait_for(20, "the open transaction's records to be appended", || {
        marks(&topic, Isolation::ReadUncommitted)[&0].high_watermark == 8
    });
    let id1 = topic_id(&c.topic);
    let run1 = c.capture();
    let a = run1.capture.partitions[0]
        .archived
        .clone()
        .expect("archived");
    assert_eq!(
        a.last_offset, 7,
        "the engine archives the open transaction's records"
    );
    assert_eq!(
        run1.rc_after[&0].high_watermark, 5,
        "READ_COMMITTED stops at the LSO"
    );
    // Within the run: READ_COMMITTED marks make the capture contradict itself.
    let mut rc_capture = run1.capture.clone();
    rc_capture.partitions[0].before = run1.rc_before[&0];
    rc_capture.partitions[0].after = run1.rc_after[&0];
    let rc_intra = intra_run(&rc_capture);
    assert!(
        !rc_intra.is_empty(),
        "READ_COMMITTED marks flag the capture"
    );
    assert!(
        intra_run(&run1.capture).is_empty(),
        "READ_UNCOMMITTED marks do not"
    );
    c.note(
        "intra_run_read_committed",
        json!(rc_intra.iter().map(Signal::to_json).collect::<Vec<_>>()),
    );
    let id2 = topic_id(&c.topic);
    assert_eq!(id1, id2);
    let (v_rc, s_rc) = c.classify(&run1, Isolation::ReadCommitted);
    let (v, s) = c.classify(&run1, Isolation::ReadUncommitted);
    txp.abort_transaction(Duration::from_secs(30))
        .expect("abort_transaction");
    assert_eq!(
        v_rc,
        Verdict::Break,
        "READ_COMMITTED marks: a false break: {s_rc:?}"
    );
    c.finish(true, v, Verdict::Continuous);
    assert_eq!(v, Verdict::Continuous, "{s:?}");
}

/// c12 (negative control): records produced after the capture carry OLDER
/// timestamps than the archived tail. Timestamps are not a signal.
#[test]
fn c12_non_monotonic_timestamps_are_not_a_signal() {
    let mut c = Case::start("c12");
    c.create(1, PLAIN);
    let ts0 = now_ms() - 60_000;
    produce(&c.topic, &recs(1, 10, "g1", ts0));
    let id1 = topic_id(&c.topic);
    let run1 = c.capture();
    produce(&c.topic, &recs(1, 5, "g1b", ts0 - 3_600_000));
    let id2 = topic_id(&c.topic);
    assert_eq!(id1, id2);
    let last_ts = run1.capture.partitions[0]
        .archived
        .as_ref()
        .expect("archived")
        .last_timestamp_ms;
    assert!(
        ts0 - 3_600_000 < last_ts,
        "the new records are older than the tail"
    );
    c.note(
        "naive_timestamp_rule",
        json!("a rule that treats a record older than the archived tail as a new topic reports a false break here"),
    );
    let (v, s) = c.classify(&run1, Isolation::ReadUncommitted);
    c.finish(true, v, Verdict::Continuous);
    assert_eq!(v, Verdict::Continuous, "{s:?}");
}

/// c13 (KNOWN FALSE NEGATIVE): recreated, refilled past the old end, and the
/// old boundary offsets deleted before the next capture. Nothing is left to
/// compare and no offset regresses. Only a topic ID closes this.
#[test]
fn c13_known_miss_recreated_then_trimmed_past_the_boundary() {
    let mut c = Case::start("c13");
    c.create(1, PLAIN);
    produce(&c.topic, &recs(1, 10, "g1", now_ms() - 60_000));
    let id1 = topic_id(&c.topic);
    let run1 = c.capture();
    c.recreate(1, PLAIN);
    produce(&c.topic, &recs(1, 20, "g2", now_ms() - 30_000));
    delete_records(&c.topic, &[(0, 12)]);
    let id2 = topic_id(&c.topic);
    assert_ne!(id1, id2, "the fixture must have made a new topic");
    let (v, s) = c.classify(&run1, Isolation::ReadUncommitted);
    c.finish(false, v, Verdict::Unverified);
    assert_eq!(v, Verdict::Unverified, "the documented miss: {s:?}");
}

/// c14 (KNOWN FALSE NEGATIVE): recreated and refilled with byte-identical
/// records (same keys, values, headers and timestamps, same order). The
/// boundary probe matches. A restore that strips Logweir's offset headers
/// produces exactly this.
#[test]
fn c14_known_miss_byte_identical_replay() {
    let mut c = Case::start("c14");
    c.create(1, PLAIN);
    let gen1 = recs(1, 10, "g1", now_ms() - 60_000);
    produce(&c.topic, &gen1);
    let id1 = topic_id(&c.topic);
    let run1 = c.capture();
    c.recreate(1, PLAIN);
    produce(&c.topic, &gen1);
    produce(&c.topic, &recs(1, 5, "g2", now_ms() - 30_000));
    let id2 = topic_id(&c.topic);
    assert_ne!(id1, id2, "the fixture must have made a new topic");
    let (v, s) = c.classify(&run1, Isolation::ReadUncommitted);
    c.finish(false, v, Verdict::Continuous);
    assert_eq!(v, Verdict::Continuous, "the documented miss: {s:?}");
}

/// c15: the original-name restore PROD-15.1 will perform, emulated: the
/// archived records produced back verbatim, WITH the offset headers the
/// engine added (`strip_offset_headers: false`, which Logweir renders). The
/// probe sees the extra headers, so the next capture reports a break.
#[test]
fn c15_an_original_name_restore_is_a_new_generation() {
    let mut c = Case::start("c15");
    c.create(1, PLAIN);
    produce(&c.topic, &recs(1, 10, "g1", now_ms() - 60_000));
    let id1 = topic_id(&c.topic);
    let run1 = c.capture();
    let archived: Vec<Rec> = run1.archive.partitions[&0]
        .1
        .iter()
        .map(|r| Rec {
            partition: 0,
            key: r.key.clone(),
            value: r.value.clone(),
            headers: r.headers.clone(),
            ts: r.timestamp,
        })
        .collect();
    c.recreate(1, PLAIN);
    produce(&c.topic, &archived);
    let id2 = topic_id(&c.topic);
    assert_ne!(id1, id2, "the fixture must have made a new topic");
    let (v, s) = c.classify(&run1, Isolation::ReadUncommitted);
    assert!(
        s.iter()
            .any(|x| matches!(x, Signal::BoundaryRecordChanged { .. })),
        "{s:?}"
    );
    c.finish(false, v, Verdict::Break);
    assert_eq!(v, Verdict::Break, "{s:?}");
}

/// c16: recreated with more partitions, refilled, and the old boundary
/// offsets deleted. Nothing verifies and partitions were added: suspected,
/// not continuous.
#[test]
fn c16_more_partitions_with_nothing_to_verify_is_suspected() {
    let mut c = Case::start("c16");
    c.create(3, PLAIN);
    produce(&c.topic, &recs(3, 10, "g1", now_ms() - 60_000));
    let id1 = topic_id(&c.topic);
    let run1 = c.capture();
    c.recreate(5, PLAIN);
    produce(&c.topic, &recs(5, 20, "g2", now_ms() - 30_000));
    delete_records(&c.topic, &[(0, 12), (1, 12), (2, 12)]);
    let id2 = topic_id(&c.topic);
    assert_ne!(id1, id2, "the fixture must have made a new topic");
    let (v, s) = c.classify(&run1, Isolation::ReadUncommitted);
    c.finish(false, v, Verdict::Suspected);
    assert_eq!(v, Verdict::Suspected, "{s:?}");
}

// ===========================================================================
// The rule on its own: no broker. Each row kills one way the rule can drift.
// ===========================================================================

fn obs(partition: i32, ls: i64, hw: i64, tail: &[(i64, &str)]) -> PartitionObs {
    PartitionObs {
        partition,
        before: Marks {
            log_start: ls,
            high_watermark: hw,
        },
        after: Marks {
            log_start: ls,
            high_watermark: hw,
        },
        archived: (!tail.is_empty()).then(|| Archived {
            first_offset: ls,
            last_offset: tail[0].0,
            first_timestamp_ms: 0,
            last_timestamp_ms: 0,
            records: tail.len(),
            tail: tail
                .iter()
                .map(|(o, fp)| TailRecord {
                    offset: *o,
                    fingerprint: fp.to_string(),
                    raw_fingerprint: format!("raw-{fp}"),
                })
                .collect(),
        }),
    }
}

fn prev1(ls: i64, hw: i64, tail: &[(i64, &str)]) -> Capture {
    Capture {
        cluster_id: "c".into(),
        partition_count: 1,
        partitions: vec![obs(0, ls, hw, tail)],
    }
}

fn cur(count: i32, marks: &[(i64, i64)]) -> Current {
    Current {
        cluster_id: "c".into(),
        partition_count: count,
        marks: marks
            .iter()
            .enumerate()
            .map(|(p, (ls, hw))| {
                (
                    p as i32,
                    Marks {
                        log_start: *ls,
                        high_watermark: *hw,
                    },
                )
            })
            .collect(),
    }
}

fn run_rule(prev: &Capture, now: &Current, answers: &[(i64, Probe)]) -> (Verdict, Vec<Signal>) {
    let mut f = |_p: i32, o: i64| {
        answers
            .iter()
            .find(|(x, _)| *x == o)
            .map(|(_, r)| r.clone())
            .unwrap_or_else(|| panic!("unexpected probe at {o}"))
    };
    classify(prev, now, Some(&mut f))
}

#[test]
fn rule_a_matching_boundary_is_continuous() {
    let (v, _) = run_rule(
        &prev1(0, 10, &[(9, "a")]),
        &cur(1, &[(0, 12)]),
        &[(9, Probe::At("a".into()))],
    );
    assert_eq!(v, Verdict::Continuous);
}

#[test]
fn rule_a_different_record_at_the_boundary_is_a_break() {
    let (v, s) = run_rule(
        &prev1(0, 10, &[(9, "a")]),
        &cur(1, &[(0, 12)]),
        &[(9, Probe::At("b".into()))],
    );
    assert_eq!(v, Verdict::Break);
    assert_eq!(
        s,
        vec![Signal::BoundaryRecordChanged {
            partition: 0,
            offset: 9
        }]
    );
}

#[test]
fn rule_an_end_regression_is_a_break_without_probing() {
    let (v, s) = run_rule(&prev1(0, 10, &[(9, "a")]), &cur(1, &[(0, 4)]), &[]);
    assert_eq!(v, Verdict::Break);
    assert!(s.contains(&Signal::EndRegressed {
        partition: 0,
        previous: 10,
        current: 4
    }));
}

#[test]
fn rule_a_log_start_regression_is_a_break() {
    let (v, s) = run_rule(
        &prev1(5, 10, &[(9, "a")]),
        &cur(1, &[(0, 12)]),
        &[(9, Probe::At("a".into()))],
    );
    assert_eq!(v, Verdict::Break);
    assert!(s.contains(&Signal::LogStartRegressed {
        partition: 0,
        previous: 5,
        current: 0
    }));
}

#[test]
fn rule_a_partition_count_decrease_is_a_break() {
    let prev = Capture {
        cluster_id: "c".into(),
        partition_count: 2,
        partitions: vec![obs(0, 0, 10, &[(9, "a")]), obs(1, 0, 10, &[(9, "b")])],
    };
    let (v, _) = run_rule(&prev, &cur(1, &[(0, 12)]), &[(9, Probe::At("a".into()))]);
    assert_eq!(v, Verdict::Break);
}

#[test]
fn rule_an_absent_boundary_tries_the_next_candidate() {
    let (v, s) = run_rule(
        &prev1(0, 10, &[(9, "a"), (8, "b")]),
        &cur(1, &[(0, 12)]),
        &[(9, Probe::Absent(Some(10))), (8, Probe::At("b".into()))],
    );
    assert_eq!(v, Verdict::Continuous, "{s:?}");
}

#[test]
fn rule_an_absent_boundary_is_never_a_break() {
    let (v, _) = run_rule(
        &prev1(0, 10, &[(9, "a")]),
        &cur(1, &[(0, 12)]),
        &[(9, Probe::Absent(Some(10)))],
    );
    assert_eq!(v, Verdict::Unverified);
}

#[test]
fn rule_a_log_start_past_the_tail_is_a_gap_and_unverified() {
    let (v, s) = run_rule(&prev1(0, 10, &[(9, "a")]), &cur(1, &[(15, 15)]), &[]);
    assert_eq!(v, Verdict::Unverified);
    assert!(s.contains(&Signal::CaptureGap {
        partition: 0,
        from: 10,
        to: 15
    }));
}

#[test]
fn rule_added_partitions_without_verification_are_suspected() {
    let (v, _) = run_rule(
        &prev1(0, 10, &[(9, "a")]),
        &cur(2, &[(12, 20), (0, 3)]),
        &[],
    );
    assert_eq!(v, Verdict::Suspected);
}

#[test]
fn rule_added_partitions_with_verification_are_continuous() {
    let (v, _) = run_rule(
        &prev1(0, 10, &[(9, "a")]),
        &cur(2, &[(0, 20), (0, 3)]),
        &[(9, Probe::At("a".into()))],
    );
    assert_eq!(v, Verdict::Continuous);
}

#[test]
fn rule_another_cluster_is_unknown() {
    let mut now = cur(1, &[(0, 12)]);
    now.cluster_id = "other".into();
    let (v, _) = run_rule(&prev1(0, 10, &[(9, "a")]), &now, &[]);
    assert_eq!(v, Verdict::Unknown);
}

#[test]
fn rule_offsets_only_never_probes() {
    let (v, _) = classify(&prev1(0, 10, &[(9, "a")]), &cur(1, &[(0, 12)]), None);
    assert_eq!(v, Verdict::Unverified);
}

#[test]
fn intra_run_flags_an_archive_beyond_the_post_run_end() {
    let mut c = prev1(0, 10, &[(9, "a")]);
    c.partitions[0].after.high_watermark = 5;
    let s = intra_run(&c);
    assert!(s.iter().any(Signal::is_break), "{s:?}");
    assert!(intra_run(&prev1(0, 10, &[(9, "a")])).is_empty());
}

#[test]
fn intra_run_flags_an_archive_that_ends_at_the_post_run_end() {
    // Offset 9 archived, and the partition now ends at 9: offset 9 no longer
    // exists, which one continuous history cannot produce.
    let mut c = prev1(0, 10, &[(9, "a")]);
    c.partitions[0].before.high_watermark = 9;
    c.partitions[0].after.high_watermark = 9;
    let s = intra_run(&c);
    assert_eq!(s.len(), 1, "{s:?}");
}

#[test]
fn rule_no_new_records_is_not_a_regression() {
    let (v, s) = run_rule(
        &prev1(0, 10, &[(9, "a")]),
        &cur(1, &[(0, 10)]),
        &[(9, Probe::At("a".into()))],
    );
    assert_eq!(v, Verdict::Continuous, "{s:?}");
}

#[test]
fn rule_a_log_start_just_past_the_tail_is_no_gap() {
    let (v, s) = run_rule(&prev1(0, 10, &[(9, "a")]), &cur(1, &[(10, 10)]), &[]);
    assert_eq!(v, Verdict::Unverified);
    assert!(
        !s.iter().any(|x| matches!(x, Signal::CaptureGap { .. })),
        "{s:?}"
    );
}
