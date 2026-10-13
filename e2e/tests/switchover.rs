#![cfg(feature = "e2e")]
//! **PROD-04.2 — position translation and the reviewed switchover, live**
//! (design `docs/to-do/decisions/PROD-04.2-cutover.md` §8.1, rows C1–C17).
//!
//! Every row backs a topic up with `logweir backup run --consumer-group …`,
//! restores it COMPLETELY under a prefix bound to that point (so phase 7 maps
//! the captured positions and writes the positions-mapping document), builds a
//! plan with `logweir switchover plan`, has it approved with `logweir drill
//! approve --subject-kind Switchover`, and runs `logweir switchover run`. The
//! BROKER is the oracle: committed positions are read back with Kafka's own
//! consumer (`kafka-consumer-groups.sh`, or an rdkafka consumer that resumes
//! from them with `auto.offset.reset=error`), never off a document alone.
//!
//! | row | rows | proves |
//! |---|---|---|
//! | `the_headline_a_paused_consumer_resumes_at_its_record` | C1, C3, C8, C9, C17 | positions absent after the restore; a paused group resumes on the restored topic at `x-original-offset = P` with the source key and value (log start 5); a caught-up group at the end, a new record its first; an emptied partition exact at 0; the Restore's approval cannot run it; committing `P` unmapped lands 5 off |
//! | `live_groups_are_refused_and_a_new_run_recovers` | C2 (4.3), C4, C5 | a live classic and a live KIP-848 group refused `GroupActive`, readback unchanged; `PartiallyApplied`; the same plan again: `alreadyApplied` for the applied, the others applied once their members stopped; a moved group `TargetPositionsChanged`; a commit by a second member of the SAME live group is accepted and moves its readback |
//! | `a_rollback_restores_priors_and_is_refused_once_consumers_moved` | C15 | a rollback plan restores the priors and lists the absent ones; after a consumer read 10 records it is refused naming them |
//! | `a_recreated_target_topic_is_refused` | C10 | `TargetGenerationChanged`, nothing applied; unchanged: applies |
//! | `only_a_passing_complete_restore_maps_positions` | C11 | a sampled restore writes no mapping and is refused naming `coverage`; a complete restore with a record planted before verification is refused naming `unexpected` |
//! | `a_filtered_restore_maps_approximate_or_not_restored` | C6 | a window start past unread records: `approximate` with the exact `skipped`; a partition subset: `PartitionNotRestored` |
//! | `a_compaction_hole_maps_to_the_next_record` | C7 | `exact` at the next surviving record |
//! | `an_empty_streams_group_is_refused_by_type` | C12 | `TargetGroupTypeUnsupported`, no commit |
//! | `a_hidden_target_group_is_not_visible_and_never_created` | C13 | the classification answers `NotVisible` (never absent) |
//! | `a_transactional_archive_maps_nothing` | C16 | every position `TransactionalArchive` |
//! | `an_absent_group_is_created_on_3_9` | C14, C2 (3.9) | absent target group created; a live classic group refused |
//! | `the_review_reads_every_position_before_the_switchover` | §3.8 (K1's preview) | `logweir check run` over a `switchoverReadiness` plan: the restore's evidence and mapping verify, the target is the restore's, every group admissible, and the details list every partition `exact` at its mapped offset with no committed position; with a live member the groups row is `GroupActive`; over the scorecard without `target.creation` the target row is `TargetIdentityUnknown`; nothing is committed |
//!
//! # `target.creation`
//!
//! The switchover compares each target topic's ID with the one the restore
//! recorded at creation (`target.creation`, PROD-11.1a), which every run that
//! creates topics signs; [`own_scorecard`] asserts it is there. The review's
//! `TargetIdentityUnknown` control re-signs the scorecard without it
//! ([`without_creation`]).
//!
//! # Running it
//!
//! ```text
//! eval "$(e2e/compose/stack-env.sh --slot 2 --kafka 4.3 --profiles auth,acl,streams-protocol)"
//! just e2e-up
//! cargo build -p logweir
//! LOGWEIR_PYTHON=<python3 with cryptography> AWS_EC2_METADATA_DISABLED=true \
//!   cargo test -p e2e --features e2e --test switchover -- --ignored --test-threads=1 --nocapture
//! just e2e-down
//! ```
//!
//! Each row writes what it observed to `switchover/<row>.json` under
//! `harness::demo_dir()`.
mod harness;

use harness::{bin, demo_dir, engine_bin, engine_digest, engine_mount, engine_version, root};
use logweir_core::backup_receipt::BackupReceipt;
use logweir_core::positions_mapping::MappingDocument;
use logweir_core::switchover::{SwitchoverPlan, SwitchoverRecord};
use logweir_kafka::positions::{
    CommittedPosition, GroupListing, PartitionPosition, TopicPartition,
};
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::message::{Headers, Message};
use rdkafka::producer::{BaseProducer, BaseRecord, Producer};
use serde_json::{json, Value};
use std::collections::BTreeMap;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::process::{Command, Output, Stdio};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

// ============================================================ processes

fn output_within(mut cmd: Command, secs: u64) -> Output {
    cmd.stdout(Stdio::piped()).stderr(Stdio::piped());
    let mut child = cmd.spawn().unwrap_or_else(|e| panic!("spawn {cmd:?}: {e}"));
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
                panic!("{cmd:?}: killed after {secs} s");
            }
            Ok(None) => std::thread::sleep(Duration::from_millis(200)),
            Err(e) => panic!("{cmd:?}: {e}"),
        }
    };
    Output {
        status,
        stdout: t_out.join().unwrap_or_default(),
        stderr: t_err.join().unwrap_or_default(),
    }
}

fn text(o: &Output) -> String {
    format!(
        "{}\n{}",
        String::from_utf8_lossy(&o.stdout),
        String::from_utf8_lossy(&o.stderr)
    )
}

fn nonce() -> String {
    let n = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("the clock is after 1970")
        .as_nanos();
    format!("{:010}", n % 10_000_000_000)
}

fn line(o: &Output, key: &str) -> Option<String> {
    String::from_utf8_lossy(&o.stdout)
        .lines()
        .rev()
        .find_map(|l| l.strip_prefix(key))
        .map(str::to_string)
}

fn write_evidence(row: &str, v: &Value) {
    let dir = demo_dir().join("switchover");
    std::fs::create_dir_all(&dir).expect("the evidence directory");
    let p = dir.join(format!("{row}.json"));
    std::fs::write(&p, serde_json::to_vec_pretty(v).expect("serialises")).expect("written");
    eprintln!("[prod-04-2] evidence: {}", p.display());
}

// ============================================================ the broker

const IN_NETWORK: &str = "kafka-broker-1:9094";

fn broker_exec(args: &[&str], what: &str) -> Output {
    let o = harness::compose_exec_broker(args);
    assert!(o.status.success(), "{what} failed:\n{}", text(&o));
    o
}

fn reader() -> logweir_kafka::rdkafka_reader::RdKafkaReader {
    harness::reader()
}

fn create_topic(topic: &str, partitions: i32, configs: &[(&str, &str)]) {
    if configs.is_empty() {
        harness::create_topic(topic, partitions);
    } else {
        harness::create_topic_with_configs(topic, partitions, configs);
    }
}

fn producer() -> BaseProducer {
    rdkafka::config::ClientConfig::new()
        .set("bootstrap.servers", harness::bootstrap())
        .set("enable.idempotence", "false")
        .create()
        .expect("a producer")
}

/// `n` keyed records to `topic`/`partition`, numbered from `from`.
fn produce(topic: &str, partition: i32, from: usize, n: usize) {
    let p = producer();
    for i in from..from + n {
        let key = format!("k{partition}-{i}");
        let value = format!("v{partition}-{i}");
        loop {
            match p.send(
                BaseRecord::to(topic)
                    .partition(partition)
                    .key(&key)
                    .payload(&value),
            ) {
                Ok(()) => break,
                Err((e, _)) => {
                    eprintln!("[prod-04-2] produce retry: {e}");
                    p.poll(Duration::from_millis(100));
                }
            }
        }
        p.poll(Duration::from_millis(0));
    }
    p.flush(Duration::from_secs(60)).expect("flushed");
}

fn delete_records(topic: &str, partition: i32, before: i64) {
    let offsets = format!(
        "{{\"partitions\":[{{\"topic\":\"{topic}\",\"partition\":{partition},\"offset\":{before}}}],\"version\":1}}"
    );
    broker_exec(
        &[
            "bash",
            "-c",
            &format!(
                "printf '%s' '{offsets}' > /tmp/p042-delete-records.json && \
                 /opt/kafka/bin/kafka-delete-records.sh --bootstrap-server {IN_NETWORK} \
                 --offset-json-file /tmp/p042-delete-records.json"
            ),
        ],
        "kafka-delete-records",
    );
}

/// A non-member commit (PROD-04.0a): a paused consumer's positions.
fn commit(group: &str, positions: &[(&str, i32, i64)]) {
    let positions: Vec<(TopicPartition, CommittedPosition)> = positions
        .iter()
        .map(|(t, p, o)| {
            (
                TopicPartition::new(*t, *p),
                CommittedPosition {
                    offset: *o,
                    leader_epoch: None,
                    metadata: None,
                },
            )
        })
        .collect();
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        match reader().commit_positions(group, &positions) {
            Ok(()) => return,
            Err(e) if Instant::now() < deadline => {
                eprintln!("[prod-04-2] commit {group} retrying: {e}");
                std::thread::sleep(Duration::from_secs(2));
            }
            Err(e) => panic!("commit {group}: {e}"),
        }
    }
}

/// The group's committed positions on `topic`'s partitions, as the broker
/// answers them (`None`: no committed position).
fn committed(group: &str, topic: &str, partitions: i32) -> Vec<Option<i64>> {
    let tps: Vec<TopicPartition> = (0..partitions)
        .map(|p| TopicPartition::new(topic, p))
        .collect();
    let answer = reader()
        .committed_positions(group, &tps, GroupListing::NotListed)
        .unwrap_or_else(|e| panic!("read {group}'s positions: {e}"));
    tps.iter()
        .map(|tp| match answer.get(tp) {
            Some(PartitionPosition::Committed(c)) => Some(c.offset),
            _ => None,
        })
        .collect()
}

/// The source offset a record's last `x-original-offset` header names.
fn lineage(m: &rdkafka::message::BorrowedMessage<'_>) -> Option<i64> {
    let h = m.headers()?;
    (0..h.count())
        .rev()
        .map(|i| h.get(i))
        .find(|h| h.key == "x-original-offset")
        .and_then(|h| h.value)
        .and_then(|v| <[u8; 8]>::try_from(v).ok())
        .map(i64::from_le_bytes)
}

/// **The consumer resumes**: `group` subscribes to `topic` with
/// `auto.offset.reset=error` (librdkafka's `none`) and no auto-commit, and
/// the FIRST record each partition delivers is returned:
/// `(target offset, lineage, key, value)`. A partition with no committed
/// position would error instead of silently starting anywhere.
fn first_records(
    group: &str,
    topic: &str,
    partitions: i32,
) -> BTreeMap<i32, (i64, Option<i64>, String, String)> {
    let c: BaseConsumer = rdkafka::config::ClientConfig::new()
        .set("bootstrap.servers", harness::bootstrap())
        .set("group.id", group)
        .set("auto.offset.reset", "error")
        .set("enable.auto.commit", "false")
        .set("session.timeout.ms", "10000")
        .create()
        .expect("a consumer");
    c.subscribe(&[topic]).expect("subscribed");
    let mut out = BTreeMap::new();
    let deadline = Instant::now() + Duration::from_secs(60);
    while out.len() < partitions as usize && Instant::now() < deadline {
        match c.poll(Duration::from_millis(500)) {
            Some(Ok(m)) => {
                out.entry(m.partition()).or_insert_with(|| {
                    (
                        m.offset(),
                        lineage(&m),
                        String::from_utf8_lossy(m.key().unwrap_or_default()).into_owned(),
                        String::from_utf8_lossy(m.payload().unwrap_or_default()).into_owned(),
                    )
                });
            }
            Some(Err(e)) => eprintln!("[prod-04-2] consumer: {e}"),
            None => {}
        }
    }
    c.unsubscribe();
    drop(c);
    out
}

/// A live member of `group` on `topic`, inside the broker container, bounded
/// by `timeout`, until [`stop_member`]. It commits nothing itself
/// (`enable.auto.commit=false`), so the group's positions are whatever the
/// switchover left.
fn start_member(group: &str, topic: &str, kip848: bool) {
    let protocol = if kip848 {
        " --consumer-property group.protocol=consumer"
    } else {
        ""
    };
    broker_exec(
        &[
            "bash",
            "-c",
            &format!(
                "nohup timeout 900 /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server \
                 {IN_NETWORK} --topic {topic} --group {group}{protocol} --consumer-property \
                 enable.auto.commit=false > /dev/null 2>&1 &"
            ),
        ],
        "start a member",
    );
    // Until the group is Stable with a member.
    let deadline = Instant::now() + Duration::from_secs(90);
    loop {
        let o = harness::compose_exec_broker(&[
            "/opt/kafka/bin/kafka-consumer-groups.sh",
            "--bootstrap-server",
            IN_NETWORK,
            "--describe",
            "--group",
            group,
            "--state",
        ]);
        if String::from_utf8_lossy(&o.stdout).contains("Stable") {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "{group} never became Stable:\n{}",
            text(&o)
        );
        std::thread::sleep(Duration::from_secs(2));
    }
}

fn stop_member(group: &str) {
    let first = &group[..1];
    let rest = &group[1..];
    let _ = harness::compose_exec_broker(&[
        "bash",
        "-c",
        &format!("pkill -TERM -f -- '--group [{first}]{rest}( |$)' || true"),
    ]);
    let deadline = Instant::now() + Duration::from_secs(90);
    loop {
        let o = harness::compose_exec_broker(&[
            "/opt/kafka/bin/kafka-consumer-groups.sh",
            "--bootstrap-server",
            IN_NETWORK,
            "--describe",
            "--group",
            group,
            "--state",
        ]);
        let s = String::from_utf8_lossy(&o.stdout).into_owned();
        if s.contains("Empty") || s.contains("Dead") {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "{group} never became Empty:\n{s}"
        );
        std::thread::sleep(Duration::from_secs(2));
    }
}

// ============================================================ evidence

fn signing_pem() -> PathBuf {
    root().join("e2e/fixtures/signed/signing.pem")
}

fn s3_env(c: &mut Command) {
    let minio = ["minio", "admin"].concat();
    c.env("AWS_ACCESS_KEY_ID", &minio)
        .env("AWS_SECRET_ACCESS_KEY", &minio)
        .env("AWS_REGION", "us-east-1")
        .env("AWS_EC2_METADATA_DISABLED", "true");
}

fn engine_env(c: &mut Command) {
    s3_env(c);
    c.env("LOGWEIR_ENGINE_BIN", engine_bin())
        .env("LOGWEIR_ENGINE_VERSION", engine_version())
        .env("LOGWEIR_ENGINE_DIGEST", engine_digest())
        .env("LOGWEIR_E2E_ENGINE_MOUNT", engine_mount())
        .env("TMPDIR", engine_mount());
}

fn store_url(bucket: &str, prefix: &str) -> logweir_core::engine::StorageUrl {
    serde_yaml::from_str(&format!(
        "backend: s3\nbucket: {bucket}\nprefix: {prefix}\nregion: us-east-1\nendpoint: {}\n\
         path_style: true\nallow_http: true\n",
        harness::s3_endpoint()
    ))
    .expect("a storage url")
}

fn use_s3_env() {
    let minio = ["minio", "admin"].concat();
    std::env::set_var("AWS_ACCESS_KEY_ID", &minio);
    std::env::set_var("AWS_SECRET_ACCESS_KEY", &minio);
    std::env::set_var("AWS_REGION", "us-east-1");
}

/// The evidence bucket, read-write (the test's own shim writes there).
fn evidence_store() -> logweir_engine_oso::storage::Store {
    use_s3_env();
    logweir_engine_oso::storage::Store::from_url(&store_url(harness::EVIDENCE_BUCKET, "logweir/"))
        .expect("the evidence store")
}

fn fetch(key: &str) -> Vec<u8> {
    evidence_store()
        .get_capped(key, logweir_engine_oso::storage::caps::SIGNED_DOCUMENT)
        .unwrap_or_else(|e| panic!("read {key}: {e}"))
        .0
}

fn evidence_keyring() -> PathBuf {
    use logweir_core::execution_contract as wire;
    let sk = logweir_evidence::keys::SigningKey::from_pem_file(&signing_pem()).expect("the signer");
    let key_id = sk.verifying_key().key_id();
    let ring = wire::EvidenceKeyring {
        format_version: wire::EVIDENCE_KEYRING_FORMAT_VERSION.to_string(),
        keys: vec![wire::EvidenceKey {
            public_key_pem: sk.verifying_key().to_public_key_pem().expect("pem"),
            trust: logweir_core::trust::TrustedKey {
                key_id: key_id.clone(),
                principal_id: format!("install:{key_id}"),
                usages: vec![logweir_core::trust::KeyUsage::EvidenceSigning],
                not_before: chrono::Utc::now() - chrono::Duration::days(1),
                not_after: chrono::Utc::now() + chrono::Duration::days(1),
                state: logweir_core::trust::KeyState::Active,
                retired_at: None,
                revoked_at: None,
                revocation_reason: None,
                revocation_effective_from: None,
            },
        }],
    };
    let p = demo_dir().join("p042-evidence-keys.json");
    std::fs::write(&p, serde_json::to_vec(&ring).expect("serialises")).expect("written");
    p
}

fn approver() -> (PathBuf, PathBuf) {
    let pem = demo_dir().join("p042-approver.pem");
    if !pem.exists() {
        let sk = logweir_evidence::keys::SigningKey::generate_p256();
        std::fs::write(&pem, sk.to_pkcs8_pem().expect("pem")).expect("written");
    }
    let public = demo_dir().join("p042-approver.pub.pem");
    let sk = logweir_evidence::keys::SigningKey::from_pem_file(&pem).expect("the approver");
    std::fs::write(
        &public,
        sk.verifying_key().to_public_key_pem().expect("pem"),
    )
    .expect("written");
    (pem, public)
}

/// `logweir drill approve --spec FILE --subject-kind KIND`, as an approver
/// runs it.
fn approve(file: &Path, kind: &str, label: &str) -> PathBuf {
    let (pem, _) = approver();
    let out_path = demo_dir().join(format!("p042-{label}-approval.json"));
    let _ = std::fs::remove_file(&out_path);
    let mut c = Command::new(bin());
    c.args(["drill", "approve", "--spec"])
        .arg(file)
        .arg("--key")
        .arg(&pem)
        .args([
            "--approver",
            "p042-e2e@example.com",
            "--ticket",
            "PROD-04.2",
        ])
        .args(["--subject-kind", kind])
        .arg("--out")
        .arg(&out_path);
    let o = output_within(c, 60);
    assert!(o.status.success(), "drill approve:\n{}", text(&o));
    out_path
}

// ============================================================ backup, restore

struct Backup {
    backup_id: String,
    receipt_key: String,
    receipt_bytes: Vec<u8>,
    receipt: BackupReceipt,
}

fn backup(backup_id: &str, topic: &str, groups: &[&str]) -> Backup {
    let spec = demo_dir().join(format!("{backup_id}-backup.yaml"));
    let groups: Vec<String> = groups.iter().map(|g| format!("\"{g}\"")).collect();
    std::fs::write(
        &spec,
        format!(
            "backup_id: {backup_id}\n\
             source:\n\
             \x20 bootstrap_servers: [{bootstrap}]\n\
             \x20 topics: [{topic}]\n\
             \x20 consumer_groups: [{groups}]\n\
             storage:\n\
             \x20 backend: s3\n\
             \x20 bucket: {bucket}\n\
             \x20 prefix: {backup_id}\n\
             \x20 region: us-east-1\n\
             \x20 endpoint: {endpoint}\n\
             \x20 path_style: true\n\
             \x20 allow_http: true\n\
             backup:\n\
             \x20 compression: zstd\n\
             \x20 segment_max_records: 1000\n\
             \x20 segment_max_bytes: 10485760\n\
             \x20 max_concurrent_partitions: 3\n",
            bootstrap = harness::bootstrap(),
            groups = groups.join(", "),
            bucket = harness::ARCHIVE_BUCKET,
            endpoint = harness::s3_endpoint(),
        ),
    )
    .expect("the backup spec");
    let allow = demo_dir().join("p042-backup-allowed-clusters.json");
    std::fs::write(
        &allow,
        "{\"allowed_cluster_ids\": [\"SCRATCH-CLUSTER-NOT-THE-SOURCE\"]}\n",
    )
    .expect("written");
    let mut c = Command::new(bin());
    c.args(["backup", "run", "--spec"])
        .arg(&spec)
        .arg("--allowed-clusters")
        .arg(&allow)
        .arg("--signing-key")
        .arg(signing_pem());
    engine_env(&mut c);
    let out = output_within(c, 900);
    assert_eq!(
        out.status.code(),
        Some(0),
        "backup run {backup_id}:\n{}",
        text(&out)
    );
    let receipt_key = line(&out, "receipt-key=").expect("receipt-key=");
    use_s3_env();
    let store = logweir_engine_oso::storage::Store::read_only_from_url(&store_url(
        harness::ARCHIVE_BUCKET,
        backup_id,
    ))
    .expect("the archive");
    let (receipt_bytes, _) = store
        .get_capped(
            &receipt_key,
            logweir_engine_oso::storage::caps::SIGNED_DOCUMENT,
        )
        .expect("the receipt");
    let receipt: BackupReceipt = serde_json::from_slice(&receipt_bytes).expect("a receipt");
    assert!(
        receipt.consumer_positions.is_some(),
        "the receipt captured the groups"
    );
    Backup {
        backup_id: backup_id.to_string(),
        receipt_key,
        receipt_bytes,
        receipt,
    }
}

fn rfc3339(ms: i64) -> String {
    chrono::DateTime::from_timestamp_millis(ms)
        .expect("representable")
        .to_rfc3339_opts(chrono::SecondsFormat::Millis, true)
}

/// A restore plan bound to `b`'s point, into `prefix` on the same broker.
/// `extra` is YAML appended under `restore:` (a window start, a subset, the
/// transaction-marker override); `complete` asks for complete coverage.
fn restore_spec(
    b: &Backup,
    topic: &str,
    prefix: &str,
    complete: bool,
    restore_block: &str,
) -> String {
    format!(
        "source:\n\
         \x20 storage:\n\
         \x20   backend: s3\n\
         \x20   bucket: {bucket}\n\
         \x20   prefix: {id}\n\
         \x20   region: us-east-1\n\
         \x20   endpoint: {endpoint}\n\
         \x20   path_style: true\n\
         \x20   allow_http: true\n\
         \x20 backup: {id}\n\
         \x20 topics: [{topic}]\n\
         \x20 point:\n\
         \x20   point_id: {point_id}\n\
         \x20   receipt_key: {receipt_key}\n\
         \x20   receipt_sha256: \"{receipt_sha256}\"\n\
         \x20   manifest_sha256: \"{manifest_sha256}\"\n\
         target:\n\
         \x20 bootstrap_servers: [{bootstrap}]\n\
         \x20 mode: newTopic\n\
         \x20 topic_mapping_prefix: \"{prefix}\"\n\
         \x20 topic_naming:\n\
         \x20   prefix: \"{prefix}\"\n\
         \x20 default_replication_factor: 1\n\
         {restore_block}\
         sample:\n\
         \x20 window_start: \"{start}\"\n\
         \x20 window_end: \"{end}\"\n\
         \x20 records_per_partition: 10\n\
         \x20 anchor: head\n\
         {coverage}\
         objectives:\n\
         \x20 rto_seconds: 3600\n\
         \x20 rpo_seconds: 86400\n\
         \x20 pass_rate: 1.0\n\
         evidence:\n\
         \x20 backend: s3\n\
         \x20 bucket: {evidence}\n\
         \x20 prefix: logweir/\n\
         \x20 region: us-east-1\n\
         \x20 endpoint: {endpoint}\n\
         \x20 path_style: true\n\
         \x20 allow_http: true\n",
        bucket = harness::ARCHIVE_BUCKET,
        id = b.backup_id,
        endpoint = harness::s3_endpoint(),
        point_id = logweir::catalog::record::point_id(&b.receipt_bytes),
        receipt_key = b.receipt_key,
        receipt_sha256 = logweir_core::ids::sha256_prefixed(&b.receipt_bytes),
        manifest_sha256 = b.receipt.archive.manifest_sha256,
        bootstrap = harness::bootstrap(),
        start = rfc3339(b.receipt.covered.from_ms - 1000),
        end = rfc3339(b.receipt.covered.to_ms + 1000),
        coverage = if complete {
            "\x20 coverage: complete\n"
        } else {
            ""
        },
        evidence = harness::EVIDENCE_BUCKET,
    )
}

struct Restore {
    out: Output,
    spec: PathBuf,
    scorecard: Value,
    scorecard_bytes: Vec<u8>,
    scorecard_key: String,
    mapping: Option<(Vec<u8>, MappingDocument)>,
}

/// `logweir restore run` over `spec_text`, under an ordinary approval.
fn restore(spec_text: &str, label: &str) -> Restore {
    let spec = demo_dir().join(format!("p042-{label}-restore.yaml"));
    std::fs::write(&spec, spec_text).expect("written");
    let approval = approve(&spec, "Restore", &format!("{label}-restore"));
    let (_, approver_pub) = approver();
    let allow = demo_dir().join("p042-restore-allowed-clusters.json");
    std::fs::write(
        &allow,
        serde_json::to_vec(&json!({
            "allowed_cluster_ids": [harness::cluster_id()],
            "source_cluster_id": null
        }))
        .expect("serialises"),
    )
    .expect("written");
    let out_json = demo_dir().join(format!("p042-{label}-scorecard.json"));
    let _ = std::fs::remove_file(&out_json);
    let mut c = Command::new(bin());
    c.args(["restore", "run", "--spec"])
        .arg(&spec)
        .arg("--approval")
        .arg(&approval)
        .arg("--approver-key")
        .arg(&approver_pub)
        .arg("--allowed-clusters")
        .arg(&allow)
        .arg("--signing-key")
        .arg(signing_pem())
        .arg("--out")
        .arg(&out_json)
        .arg("--evidence-keys")
        .arg(evidence_keyring());
    engine_env(&mut c);
    let out = output_within(c, 900);
    let scorecard_bytes = std::fs::read(&out_json).unwrap_or_default();
    let scorecard: Value = serde_json::from_slice(&scorecard_bytes).unwrap_or(Value::Null);
    let run_id = scorecard["run_id"].as_str().unwrap_or_default().to_string();
    let mapping = line(&out, "positions-mapping-key=").map(|k| {
        let bytes = fetch(&k);
        let doc: MappingDocument = serde_json::from_slice(&bytes).expect("a mapping");
        (bytes, doc)
    });
    eprintln!(
        "[prod-04-2] restore {label}: exit={:?} outcome={} format={} mapping={}",
        out.status.code(),
        scorecard["outcome"],
        scorecard["format_version"],
        mapping.is_some()
    );
    Restore {
        out,
        spec,
        scorecard,
        scorecard_bytes,
        scorecard_key: format!("logweir/drills/{run_id}.json"),
        mapping,
    }
}

/// The restore's own signed scorecard. Every run that creates topics signs
/// PROD-11.1a's `target.creation` (each topic's ID at creation), which the
/// switchover compares with the target's.
fn own_scorecard(r: &Restore) -> (String, Vec<u8>) {
    assert!(
        r.scorecard.pointer("/target/creation/topics").is_some(),
        "the restore signs target.creation: {}",
        r.scorecard
    );
    (r.scorecard_key.clone(), r.scorecard_bytes.clone())
}

/// The review's control for `TargetIdentityUnknown`: the restore's scorecard
/// with `target.creation` removed, re-signed with the fixture key at
/// `<key stem>-no-creation.json`.
fn without_creation(r: &Restore) -> String {
    let mut sc = r.scorecard.clone();
    sc["target"]
        .as_object_mut()
        .expect("a target block")
        .remove("creation")
        .expect("the restore signs target.creation");
    let bytes = logweir_core::det_json::to_deterministic_json(&sc).expect("serialises");
    let key = r.scorecard_key.replace(".json", "-no-creation.json");
    let sk = logweir_evidence::keys::SigningKey::from_pem_file(&signing_pem()).expect("the signer");
    let side = logweir_evidence::sign::sign_detached(
        &sk,
        logweir_evidence::PAYLOAD_TYPE_SCORECARD,
        &bytes,
    )
    .expect("signed");
    let store = evidence_store();
    store.put_create_only(&key, &bytes).expect("put");
    store
        .put_create_only(
            &key.replace(".json", ".sig"),
            &serde_json::to_vec(&side).unwrap(),
        )
        .expect("put sig");
    key
}

struct Plan {
    path: PathBuf,
    plan: SwitchoverPlan,
}

/// `logweir switchover plan` over the restore's (shimmed) scorecard and its
/// mapping, for `groups` (`source` or `source=target`; empty: every mapped
/// group).
fn plan(r: &Restore, scorecard: &[u8], groups: &[&str], label: &str) -> Plan {
    let (mapping, _) = r.mapping.as_ref().expect("the restore wrote a mapping");
    let sc_path = demo_dir().join(format!("p042-{label}-plan-scorecard.json"));
    let map_path = demo_dir().join(format!("p042-{label}-plan-mapping.json"));
    std::fs::write(&sc_path, scorecard).unwrap();
    std::fs::write(&map_path, mapping).unwrap();
    let path = demo_dir().join(format!("p042-{label}-plan.yaml"));
    let mut c = Command::new(bin());
    c.args(["switchover", "plan", "--scorecard"])
        .arg(&sc_path)
        .arg("--positions-mapping")
        .arg(&map_path)
        .args(["--restore-name", "cli-restore", "--restore-uid", "cli-uid"])
        .arg("--out")
        .arg(&path);
    for g in groups {
        c.args(["--group", g]);
    }
    let o = output_within(c, 60);
    assert!(o.status.success(), "switchover plan:\n{}", text(&o));
    let plan = SwitchoverPlan::parse(&std::fs::read(&path).unwrap()).expect("a plan");
    Plan { path, plan }
}

fn write_plan(p: &SwitchoverPlan, label: &str) -> Plan {
    let path = demo_dir().join(format!("p042-{label}-plan.yaml"));
    std::fs::write(&path, serde_yaml::to_string(p).unwrap()).unwrap();
    Plan {
        path,
        plan: p.clone(),
    }
}

struct Run {
    out: Output,
    record: Option<SwitchoverRecord>,
    record_key: Option<String>,
}

/// `logweir switchover run`.
fn switchover(r: &Restore, scorecard_key: &str, p: &Plan, approval: &Path) -> Run {
    let (_, approver_pub) = approver();
    let mut c = Command::new(bin());
    c.args(["switchover", "run", "--plan"])
        .arg(&p.path)
        .arg("--restore-plan")
        .arg(&r.spec)
        .arg("--approval")
        .arg(approval)
        .arg("--approver-key")
        .arg(&approver_pub)
        .arg("--evidence-keys")
        .arg(evidence_keyring())
        .arg("--signing-key")
        .arg(signing_pem())
        .args(["--scorecard-key", scorecard_key])
        .args(["--namespace", "e2e", "--name", "sw", "--uid", "sw-uid"]);
    s3_env(&mut c);
    let out = output_within(c, 300);
    let record_key = line(&out, "switchover-record-key=");
    let record = record_key
        .as_ref()
        .map(|k| serde_json::from_slice(&fetch(k)).expect("a record"));
    eprintln!(
        "[prod-04-2] switchover: exit={:?} outcome={:?} counts={:?}",
        out.status.code(),
        line(&out, "switchover-outcome="),
        line(&out, "switchover-counts=")
    );
    Run {
        out,
        record,
        record_key,
    }
}

fn outcomes(run: &Run) -> Vec<(String, String, Option<String>)> {
    run.record
        .as_ref()
        .map(|r| {
            r.groups
                .iter()
                .map(|g| (g.target.clone(), g.outcome.clone(), g.reason.clone()))
                .collect()
        })
        .unwrap_or_default()
}

fn restored_topic(r: &Restore, source: &str) -> String {
    r.scorecard["integrity"]["verification"]["complete"]["partitions"]
        .as_array()
        .and_then(|ps| {
            ps.iter()
                .find(|p| p["topic"] == source)
                .and_then(|p| p["target_topic"].as_str())
        })
        .expect("the restored topic")
        .to_string()
}

// ============================================================ the rows

/// **C1 (headline), C3, C8, C9, C17.**
#[test]
fn the_headline_a_paused_consumer_resumes_at_its_record() {
    let n = nonce();
    let topic = format!("sw-orders-{n}");
    let (app, caught, drained) = (
        format!("app-{n}"),
        format!("caught-{n}"),
        format!("drained-{n}"),
    );
    create_topic(&topic, 3, &[]);
    produce(&topic, 0, 0, 100);
    produce(&topic, 1, 0, 100);
    produce(&topic, 2, 0, 5);
    // p0's log starts at 5; p2 is emptied (log start = high watermark = 5).
    delete_records(&topic, 0, 5);
    delete_records(&topic, 2, 5);
    commit(&app, &[(&topic, 0, 40), (&topic, 1, 40)]);
    commit(&caught, &[(&topic, 1, 100)]);
    commit(&drained, &[(&topic, 2, 5)]);
    let b = backup(&format!("p042-c1-{n}"), &topic, &[&app, &caught, &drained]);
    let r = restore(
        &restore_spec(&b, &topic, &format!("c1-{n}-"), true, ""),
        "c1",
    );
    assert_eq!(
        r.out.status.code(),
        Some(0),
        "the restore passes:\n{}",
        text(&r.out)
    );
    let target = restored_topic(&r, &topic);
    let (_, mapping) = r
        .mapping
        .as_ref()
        .expect("C1: a passing complete restore writes the mapping");

    // C3: restoring never resets — every captured group is absent on the target.
    for g in [&app, &caught, &drained] {
        assert_eq!(
            committed(g, &target, 3),
            vec![None, None, None],
            "C3: {g} after the restore"
        );
    }
    let entry = |g: &str, p: u32| {
        mapping.groups[g]
            .entries
            .iter()
            .find(|e| e.partition == p)
            .cloned()
            .expect("an entry")
    };
    let app0 = entry(&app, 0);
    assert_eq!(
        (app0.class.as_str(), app0.target_offset),
        ("exact", Some(35)),
        "C1: P=40 at log start 5"
    );
    let caught1 = entry(&caught, 1);
    assert_eq!(
        (caught1.class.as_str(), caught1.target_offset),
        ("exact", Some(100)),
        "C8: the end"
    );
    let drained2 = entry(&drained, 2);
    assert_eq!(
        (drained2.class.as_str(), drained2.target_offset),
        ("exact", Some(0)),
        "C9"
    );

    let (sc_key, sc_bytes) = own_scorecard(&r);
    let p = plan(&r, &sc_bytes, &[], "c1");
    // C17: an approval for the Restore cannot authorise the switchover.
    let restore_approval = approve(&p.path, "Restore", "c1-wrong-kind");
    let wrong = switchover(&r, &sc_key, &p, &restore_approval);
    assert_eq!(
        wrong.out.status.code(),
        Some(3),
        "C17:\n{}",
        text(&wrong.out)
    );
    assert_eq!(
        line(&wrong.out, "refusal-reason=").as_deref(),
        Some("SubjectKindMismatch")
    );
    assert_eq!(
        committed(&app, &target, 3),
        vec![None, None, None],
        "C17: nothing applied"
    );
    // The switchover's own approval does.
    let approval = approve(&p.path, "Switchover", "c1");
    let run = switchover(&r, &sc_key, &p, &approval);
    assert_eq!(
        run.out.status.code(),
        Some(0),
        "the switchover:\n{}",
        text(&run.out)
    );
    let rec = run.record.as_ref().expect("a signed record");
    assert_eq!(rec.outcome, "Succeeded");

    // C1: the paused consumer resumes on the restored topic.
    let first = first_records(&app, &target, 2);
    let source_first = |p: i32, offset: i64| (format!("k{p}-{offset}"), format!("v{p}-{offset}"));
    for (p, want) in [(0, 40), (1, 40)] {
        let (_, lin, key, value) = first.get(&p).cloned().expect("a first record");
        assert_eq!(
            lin,
            Some(want),
            "C1: partition {p}'s first record is source offset {want}"
        );
        assert_eq!(
            (key, value),
            source_first(p, want),
            "C1: the source record's key and value"
        );
    }
    // C8: the caught-up group's first record is a newly produced one.
    produce(&target, 1, 1000, 1);
    let caught_first = first_records(&caught, &target, 1);
    let (offset, lin, key, _) = caught_first.get(&1).cloned().expect("the new record");
    assert_eq!((offset, lin, key.as_str()), (100, None, "k1-1000"), "C8");
    // C9: the drained group sits at 0 on its empty partition.
    assert_eq!(committed(&drained, &target, 3)[2], Some(0), "C9");

    // C1's negative control: committing P unmapped lands 5 records off.
    let naive = format!("naive-{n}");
    commit(&naive, &[(&target, 0, 40)]);
    let naive_first = first_records(&naive, &target, 1);
    assert_eq!(
        naive_first.get(&0).and_then(|x| x.1),
        Some(45),
        "C1 control: P copied is 5 off"
    );

    // Both readers verify the scorecard with its mapping.
    let map_path = demo_dir().join("p042-c1-verify-mapping.json");
    std::fs::write(&map_path, &r.mapping.as_ref().unwrap().0).unwrap();
    let sig_path = demo_dir().join("p042-c1-scorecard.sig");
    let verify = output_within(
        {
            let mut c = Command::new(bin());
            c.args(["drill", "verify", "--scorecard"])
                .arg(demo_dir().join("p042-c1-scorecard.json"))
                .arg("--signature")
                .arg(&sig_path)
                .arg("--public-key")
                .arg(root().join("e2e/fixtures/signed/public.pem"))
                .arg("--positions-mapping")
                .arg(&map_path);
            c
        },
        60,
    );
    write_evidence(
        "c1_c3_c8_c9_c17",
        &json!({
            "topic": topic, "target": target,
            "mapping": mapping, "record": rec,
            "first_records": format!("{first:?}"),
            "caught_first": format!("{caught_first:?}"),
            "naive_first": format!("{naive_first:?}"),
            "wrong_kind": text(&wrong.out),
            "verify_exit": verify.status.code(), "verify": text(&verify),
        }),
    );
    assert_eq!(
        verify.status.code(),
        Some(0),
        "drill verify --positions-mapping:\n{}",
        text(&verify)
    );
}

/// **C2 (4.3), C4, C5.**
#[test]
fn live_groups_are_refused_and_a_new_run_recovers() {
    let n = nonce();
    let topic = format!("sw-live-{n}");
    let gs: Vec<String> = ["g1", "g2", "classic", "kip848", "moved"]
        .iter()
        .map(|g| format!("{g}-{n}"))
        .collect();
    create_topic(&topic, 2, &[]);
    produce(&topic, 0, 0, 30);
    produce(&topic, 1, 0, 30);
    for g in &gs {
        commit(g, &[(&topic, 0, 10), (&topic, 1, 20)]);
    }
    let refs: Vec<&str> = gs.iter().map(String::as_str).collect();
    let b = backup(&format!("p042-c2-{n}"), &topic, &refs);
    let r = restore(
        &restore_spec(&b, &topic, &format!("c2-{n}-"), true, ""),
        "c2",
    );
    assert_eq!(r.out.status.code(), Some(0), "{}", text(&r.out));
    let target = restored_topic(&r, &topic);
    let (sc_key, sc_bytes) = own_scorecard(&r);
    let p = plan(&r, &sc_bytes, &[], "c2");
    let approval = approve(&p.path, "Switchover", "c2");
    // Live members on the TARGET group ids, and a group that moved since the plan.
    start_member(&gs[2], &target, false);
    start_member(&gs[3], &target, true);
    commit(&gs[4], &[(&target, 0, 3)]);

    let first = switchover(&r, &sc_key, &p, &approval);
    assert_eq!(first.out.status.code(), Some(2), "{}", text(&first.out));
    let rec = first.record.as_ref().expect("the record");
    assert_eq!(rec.outcome, "PartiallyApplied", "C4");
    let o = outcomes(&first);
    let by = |g: &str| o.iter().find(|x| x.0 == g).cloned().expect("an outcome");
    assert_eq!(by(&gs[0]).1, "applied");
    assert_eq!(by(&gs[1]).1, "applied");
    assert_eq!(
        (by(&gs[2]).1.as_str(), by(&gs[2]).2.as_deref()),
        ("refused", Some("GroupActive")),
        "C2 classic"
    );
    assert_eq!(
        (by(&gs[3]).1.as_str(), by(&gs[3]).2.as_deref()),
        ("refused", Some("GroupActive")),
        "C2 KIP-848"
    );
    assert!(
        by(&gs[4])
            .2
            .unwrap_or_default()
            .starts_with("TargetPositionsChanged"),
        "C5"
    );
    // The lines, the record and the broker agree.
    assert_eq!(
        line(&first.out, "switchover-counts=").as_deref(),
        Some("applied=2 alreadyApplied=0 refused=3 failed=0 untouched=0")
    );
    assert_eq!(
        committed(&gs[2], &target, 2),
        vec![None, None],
        "C2: readback unchanged"
    );
    assert_eq!(
        committed(&gs[4], &target, 2),
        vec![Some(3), None],
        "C5: not overwritten"
    );
    let applied_commits = rec.groups.iter().filter(|g| g.committed).count();
    assert_eq!(
        applied_commits, 2,
        "C4: exactly the two applied groups were committed"
    );
    for g in rec.groups.iter().filter(|g| g.outcome == "applied") {
        assert!(g
            .partitions
            .iter()
            .all(|p| p.prior.is_none() && p.readback == Some(p.planned)));
    }

    // C2's negative control (review L2): a commit by a MEMBER OF THE SAME
    // LIVE GROUP is accepted by the broker and moves that group's readback,
    // so "readback unchanged" above would have caught a commit made as one.
    let (member_commit, member_partition) = {
        let c: BaseConsumer = rdkafka::config::ClientConfig::new()
            .set("bootstrap.servers", harness::bootstrap())
            .set("group.id", gs[2].as_str())
            .set("enable.auto.commit", "false")
            .set("auto.offset.reset", "earliest")
            .create()
            .expect("a consumer");
        c.subscribe(&[&target]).unwrap();
        let deadline = Instant::now() + Duration::from_secs(90);
        while c.assignment().map(|a| a.count()).unwrap_or(0) == 0 && Instant::now() < deadline {
            let _ = c.poll(Duration::from_millis(500));
        }
        let partition = c
            .assignment()
            .ok()
            .and_then(|a| a.elements().first().map(|e| e.partition()))
            .expect("the second member of the live group is assigned a partition");
        let mut tpl = rdkafka::TopicPartitionList::new();
        tpl.add_partition_offset(&target, partition, rdkafka::Offset::Offset(7))
            .unwrap();
        let res = c.commit(&tpl, rdkafka::consumer::CommitMode::Sync);
        c.unsubscribe();
        drop(c);
        (res.is_ok(), partition)
    };
    assert!(
        member_commit,
        "C2 control: the broker accepts a commit from a member of the live group"
    );
    let moved = committed(&gs[2], &target, 2);
    assert_eq!(
        moved[usize::try_from(member_partition).expect("a partition")],
        Some(7),
        "C2 control: a member's commit on the SAME group moves its readback: {moved:?}"
    );

    // The members stop; the control's own commit is removed once the group
    // is empty, so the SAME plan meets the group as the first run did.
    stop_member(&gs[2]);
    stop_member(&gs[3]);
    let deleted = harness::compose_exec_broker(&[
        "/opt/kafka/bin/kafka-consumer-groups.sh",
        "--bootstrap-server",
        IN_NETWORK,
        "--delete-offsets",
        "--group",
        &gs[2],
        "--topic",
        &target,
    ]);
    assert!(
        deleted.status.success(),
        "delete-offsets:\n{}",
        text(&deleted)
    );
    assert_eq!(committed(&gs[2], &target, 2), vec![None, None]);
    let second = switchover(&r, &sc_key, &p, &approval);
    let o = outcomes(&second);
    let by = |g: &str| o.iter().find(|x| x.0 == g).cloned().expect("an outcome");
    assert_eq!(by(&gs[0]).1, "alreadyApplied", "C4");
    assert_eq!(by(&gs[1]).1, "alreadyApplied", "C4");
    assert_eq!(by(&gs[2]).1, "applied", "C2: after the member stopped");
    assert_eq!(
        by(&gs[3]).1,
        "applied",
        "C2: KIP-848, after the member stopped"
    );
    let rec2 = second.record.as_ref().unwrap();
    assert!(
        !rec2
            .groups
            .iter()
            .find(|g| g.target == gs[0])
            .unwrap()
            .committed,
        "C4: no second commit"
    );
    write_evidence(
        "c2_c4_c5",
        &json!({"first": rec, "second": rec2, "first_lines": text(&first.out),
                "member_commit": member_commit, "member_partition": member_partition,
                "readback_after_member_commit": moved}),
    );
}

/// **C15.**
#[test]
fn a_rollback_restores_priors_and_is_refused_once_consumers_moved() {
    let n = nonce();
    let topic = format!("sw-rb-{n}");
    let g = format!("rb-{n}");
    create_topic(&topic, 1, &[]);
    produce(&topic, 0, 0, 60);
    commit(&g, &[(&topic, 0, 40)]);
    let b = backup(&format!("p042-c15-{n}"), &topic, &[&g]);
    let r = restore(
        &restore_spec(&b, &topic, &format!("c15-{n}-"), true, ""),
        "c15",
    );
    assert_eq!(r.out.status.code(), Some(0), "{}", text(&r.out));
    let target = restored_topic(&r, &topic);
    let (sc_key, sc_bytes) = own_scorecard(&r);
    // An earlier cutover attempt left the target group at 30: the plan states
    // it as the precondition the approver sees.
    commit(&g, &[(&target, 0, 30)]);
    let mut p = plan(&r, &sc_bytes, &[], "c15").plan;
    p.groups[0].topics.get_mut(&target).unwrap().expected =
        logweir_core::switchover::Expected::Positions([(0, 30)].into());
    let p = write_plan(&p, "c15-expecting");
    let approval = approve(&p.path, "Switchover", "c15");
    let run = switchover(&r, &sc_key, &p, &approval);
    assert_eq!(run.out.status.code(), Some(0), "{}", text(&run.out));
    let rec = run.record.clone().unwrap();
    assert_eq!(
        rec.groups[0].partitions[0].prior,
        Some(30),
        "the prior, read before the commit"
    );

    // The rollback plan, built from the record.
    let rec_path = demo_dir().join("p042-c15-record.json");
    std::fs::write(&rec_path, fetch(run.record_key.as_deref().unwrap())).unwrap();
    let rb_path = demo_dir().join("p042-c15-rollback.yaml");
    let o = output_within(
        {
            let mut c = Command::new(bin());
            c.args(["switchover", "rollback-plan", "--record"])
                .arg(&rec_path)
                .arg("--out")
                .arg(&rb_path);
            c
        },
        60,
    );
    assert!(o.status.success(), "{}", text(&o));
    let rb = Plan {
        plan: SwitchoverPlan::parse(&std::fs::read(&rb_path).unwrap()).unwrap(),
        path: rb_path.clone(),
    };
    assert_eq!(rb.plan.groups[0].topics[&target].offsets, [(0, 30)].into());

    // After the consumer read 10 records (40 -> 50), the rollback is refused naming the 10.
    commit(&g, &[(&target, 0, 50)]);
    let rb_approval = approve(&rb.path, "Switchover", "c15-rollback");
    let refused = switchover(&r, &sc_key, &rb, &rb_approval);
    let o1 = outcomes(&refused);
    assert_eq!(o1[0].1, "refused");
    assert!(
        o1[0]
            .2
            .as_deref()
            .unwrap_or_default()
            .contains("10 record(s) from the expected 40"),
        "C15: names the 10: {o1:?}"
    );
    assert_eq!(committed(&g, &target, 1), vec![Some(50)]);
    // With no consumption since, the rollback restores the prior.
    commit(&g, &[(&target, 0, 40)]);
    let restored = switchover(&r, &sc_key, &rb, &rb_approval);
    assert_eq!(outcomes(&restored)[0].1, "applied");
    assert_eq!(
        committed(&g, &target, 1),
        vec![Some(30)],
        "C15: the prior is back"
    );

    // A forward run from absent positions has no restorable prior: listed, not planned.
    let g2 = format!("rb2-{n}");
    let mut fresh = plan(&r, &sc_bytes, &[&format!("{g}={g2}")], "c15-fresh").plan;
    fresh.groups[0].target = g2.clone();
    let fresh = write_plan(&fresh, "c15-fresh2");
    let fa = approve(&fresh.path, "Switchover", "c15-fresh");
    let fr = switchover(&r, &sc_key, &fresh, &fa);
    std::fs::write(&rec_path, fetch(fr.record_key.as_deref().unwrap())).unwrap();
    let none = output_within(
        {
            let mut c = Command::new(bin());
            c.args(["switchover", "rollback-plan", "--record"])
                .arg(&rec_path)
                .arg("--out")
                .arg(demo_dir().join("p042-c15-none.yaml"));
            c
        },
        60,
    );
    assert!(
        !none.status.success() && text(&none).contains("--delete-offsets"),
        "C15: the absent prior is listed:\n{}",
        text(&none)
    );
    write_evidence(
        "c15",
        &json!({"record": rec, "refused": refused.record, "restored": restored.record, "absent": text(&none)}),
    );
}

/// **C10.**
#[test]
fn a_recreated_target_topic_is_refused() {
    let n = nonce();
    let topic = format!("sw-gen-{n}");
    let g = format!("gen-{n}");
    create_topic(&topic, 1, &[]);
    produce(&topic, 0, 0, 20);
    commit(&g, &[(&topic, 0, 10)]);
    let b = backup(&format!("p042-c10-{n}"), &topic, &[&g]);
    let r = restore(
        &restore_spec(&b, &topic, &format!("c10-{n}-"), true, ""),
        "c10",
    );
    assert_eq!(r.out.status.code(), Some(0), "{}", text(&r.out));
    let target = restored_topic(&r, &topic);
    let (sc_key, sc_bytes) = own_scorecard(&r);
    let p = plan(&r, &sc_bytes, &[], "c10");
    let approval = approve(&p.path, "Switchover", "c10");
    // Deleted and recreated, with the same records.
    harness::kafka_topics(&[
        "--bootstrap-server",
        IN_NETWORK,
        "--delete",
        "--topic",
        &target,
    ]);
    std::thread::sleep(Duration::from_secs(3));
    create_topic(&target, 1, &[]);
    produce(&target, 0, 0, 20);
    let refused = switchover(&r, &sc_key, &p, &approval);
    assert_eq!(refused.out.status.code(), Some(3), "{}", text(&refused.out));
    assert_eq!(
        line(&refused.out, "refusal-reason=").as_deref(),
        Some("TargetGenerationChanged")
    );
    assert_eq!(
        committed(&g, &target, 1),
        vec![None],
        "C10: nothing applied"
    );
    // Control: an unchanged target applies (another restore of the same point).
    let r2 = restore(
        &restore_spec(&b, &topic, &format!("c10b-{n}-"), true, ""),
        "c10b",
    );
    let (k2, b2) = own_scorecard(&r2);
    let p2 = plan(&r2, &b2, &[], "c10b");
    let a2 = approve(&p2.path, "Switchover", "c10b");
    let ok = switchover(&r2, &k2, &p2, &a2);
    assert_eq!(ok.out.status.code(), Some(0), "{}", text(&ok.out));
    write_evidence(
        "c10",
        &json!({"refused": refused.record, "control": ok.record}),
    );
}

/// **C11.**
#[test]
fn only_a_passing_complete_restore_maps_positions() {
    let n = nonce();
    let topic = format!("sw-cov-{n}");
    let g = format!("cov-{n}");
    create_topic(&topic, 1, &[]);
    produce(&topic, 0, 0, 20);
    commit(&g, &[(&topic, 0, 10)]);
    let b = backup(&format!("p042-c11-{n}"), &topic, &[&g]);
    // A sampled restore writes no mapping; the switchover is refused naming coverage.
    let sampled = restore(
        &restore_spec(&b, &topic, &format!("c11s-{n}-"), false, ""),
        "c11s",
    );
    assert!(
        sampled.mapping.is_none(),
        "C11: no mapping over a sampled restore"
    );
    // A plan over the passing complete restore, then pointed at the sampled scorecard.
    let complete = restore(
        &restore_spec(&b, &topic, &format!("c11c-{n}-"), true, ""),
        "c11c",
    );
    let (ck, cb) = own_scorecard(&complete);
    let mut p = plan(&complete, &cb, &[], "c11").plan;
    let (sk, sb) = own_scorecard(&sampled);
    p.restore.scorecard_sha256 = logweir_core::ids::sha256_prefixed(&sb);
    p.restore.plan_hash = sampled.scorecard["approval"]["plan_hash"]
        .as_str()
        .unwrap()
        .to_string();
    let p = write_plan(&p, "c11-sampled");
    let a = approve(&p.path, "Switchover", "c11-sampled");
    let refused = switchover(&sampled, &sk, &p, &a);
    assert_eq!(refused.out.status.code(), Some(3), "{}", text(&refused.out));
    assert!(
        text(&refused.out).contains("VerificationNotComplete: integrity.verification.coverage"),
        "C11 names coverage:\n{}",
        text(&refused.out)
    );
    // A complete restore with a record planted in the target before phase 7:
    // `fail-integrity` and no mapping; the switchover names `unexpected`.
    // (The plant is staged by producing into the restored topic while the
    // runner waits at phase 7 is not needed: a planted record any time
    // before the verification is enough, so it is produced as soon as the
    // topic appears.)
    let planted_spec = restore_spec(&b, &topic, &format!("c11p-{n}-"), true, "");
    let planted_target = format!("c11p-{n}-{topic}");
    let plant = std::thread::spawn(move || {
        let deadline = Instant::now() + Duration::from_secs(300);
        while Instant::now() < deadline {
            if harness::topic_exists(&planted_target) {
                produce(&planted_target, 0, 9000, 1);
                return true;
            }
            std::thread::sleep(Duration::from_millis(200));
        }
        false
    });
    let planted = restore(&planted_spec, "c11p");
    assert!(plant.join().unwrap(), "the plant found the topic");
    assert!(
        planted.mapping.is_none(),
        "C11: no mapping over a failing restore"
    );
    let (pk, pb) = own_scorecard(&planted);
    let mut pp = plan(&complete, &cb, &[], "c11p").plan;
    pp.restore.scorecard_sha256 = logweir_core::ids::sha256_prefixed(&pb);
    pp.restore.plan_hash = planted.scorecard["approval"]["plan_hash"]
        .as_str()
        .unwrap()
        .to_string();
    let pp = write_plan(&pp, "c11-planted");
    let pa = approve(&pp.path, "Switchover", "c11-planted");
    let refused2 = switchover(&planted, &pk, &pp, &pa);
    assert_eq!(
        refused2.out.status.code(),
        Some(3),
        "{}",
        text(&refused2.out)
    );
    assert!(
        text(&refused2.out).contains("integrity.verification.complete.replay.unexpected"),
        "C11 names unexpected:\n{}",
        text(&refused2.out)
    );
    // Control: the passing restore proceeds.
    let cp = plan(&complete, &cb, &[], "c11-ok");
    let ca = approve(&cp.path, "Switchover", "c11-ok");
    let ok = switchover(&complete, &ck, &cp, &ca);
    assert_eq!(ok.out.status.code(), Some(0), "{}", text(&ok.out));
    write_evidence(
        "c11",
        &json!({"sampled": text(&refused.out), "planted": text(&refused2.out), "control": ok.record}),
    );
}

/// **C6.**
#[test]
fn a_filtered_restore_maps_approximate_or_not_restored() {
    let n = nonce();
    let topic = format!("sw-win-{n}");
    let g = format!("win-{n}");
    harness::create_topic_for_fixed_timestamps(&topic, 2, &[]);
    // Records at 1 s apart from a fixed instant: offsets 0..10 on each partition.
    let base = 1_790_000_000_000i64;
    for p in 0..2 {
        let recs: Vec<(i64, String)> = (0..10)
            .map(|i| (base + i * 1000, format!("p{p}-{i}")))
            .collect();
        let refs: Vec<(i64, &str)> = recs.iter().map(|(t, v)| (*t, v.as_str())).collect();
        let _ = p;
        harness::produce_with_timestamps(&topic, &refs).expect("produced");
    }
    commit(&g, &[(&topic, 0, 2), (&topic, 1, 2)]);
    let b = backup(&format!("p042-c6-{n}"), &topic, &[&g]);
    // The window starts at record 6's timestamp: records 2..5 the group had not
    // read are excluded.
    let start = rfc3339(base + 6 * 1000);
    let end = rfc3339(b.receipt.covered.to_ms + 1000);
    let r = restore(
        &restore_spec(
            &b,
            &topic,
            &format!("c6-{n}-"),
            true,
            &format!("restore:\n\x20 point_in_time: \"{start}/{end}\"\n"),
        ),
        "c6",
    );
    let (_, mapping) = r
        .mapping
        .as_ref()
        .unwrap_or_else(|| panic!("a mapping:\n{}", text(&r.out)));
    let e = mapping.groups[&g]
        .entries
        .iter()
        .find(|e| e.partition == 0)
        .unwrap();
    assert_eq!(e.class, "approximate", "C6: {e:?}");
    let skipped = e.skipped.unwrap_or_default();
    // The exact count, from the SOURCE's own records: the first record at or
    // after P = 2 the window selects, and every record before it from P that
    // it excluded. Control: a rule that ignores the selection says exact.
    let source: Vec<(i64, i64)> = {
        let c: BaseConsumer = rdkafka::config::ClientConfig::new()
            .set("bootstrap.servers", harness::bootstrap())
            .set("group.id", format!("probe-s-{n}"))
            .create()
            .unwrap();
        let mut tpl = rdkafka::TopicPartitionList::new();
        tpl.add_partition_offset(&topic, 0, rdkafka::Offset::Offset(0))
            .unwrap();
        c.assign(&tpl).unwrap();
        let mut out = Vec::new();
        let until = Instant::now() + Duration::from_secs(20);
        while out.len() < 10 && Instant::now() < until {
            if let Some(Ok(m)) = c.poll(Duration::from_millis(500)) {
                out.push((m.offset(), m.timestamp().to_millis().unwrap_or_default()));
            }
        }
        out
    };
    let start_ms = base + 6 * 1000;
    let found = source
        .iter()
        .find(|(o, ts)| *o >= 2 && *ts >= start_ms)
        .map_or(i64::MAX, |(o, _)| *o);
    let want = source
        .iter()
        .filter(|(o, ts)| *o >= 2 && *o < found && *ts < start_ms)
        .count() as u64;
    let selected_before = source
        .iter()
        .filter(|(o, ts)| *o < found && *ts >= start_ms)
        .count() as i64;
    assert_eq!(
        skipped, want,
        "C6: the exact skipped count, from the source: {source:?}"
    );
    assert!(want >= 1, "the fixture excludes an unread record");
    assert_eq!(
        e.target_offset,
        Some(selected_before),
        "C6: the first selected record"
    );
    // A partition subset: the other partition is not restored.
    let subset = restore(
        &restore_spec(
            &b,
            &topic,
            &format!("c6s-{n}-"),
            true,
            &format!(
                "restore:\n\x20 point_in_time: \"../{end}\"\n\x20 partitions: {{{topic}: [0]}}\n"
            ),
        ),
        "c6s",
    );
    let (_, sm) = subset
        .mapping
        .as_ref()
        .unwrap_or_else(|| panic!("a mapping:\n{}", text(&subset.out)));
    let e1 = sm.groups[&g]
        .entries
        .iter()
        .find(|e| e.partition == 1)
        .unwrap();
    assert_eq!(
        e1.reason.as_deref(),
        Some("PartitionNotRestored"),
        "C6: {e1:?}"
    );
    write_evidence(
        "c6",
        &json!({"window": mapping, "subset": sm, "skipped": skipped}),
    );
}

/// **C7.**
#[test]
fn a_compaction_hole_maps_to_the_next_record() {
    let n = nonce();
    let topic = format!("sw-cmp-{n}");
    let g = format!("cmp-{n}");
    create_topic(
        &topic,
        1,
        &[
            ("cleanup.policy", "compact"),
            ("segment.ms", "100"),
            ("min.cleanable.dirty.ratio", "0.01"),
            ("delete.retention.ms", "100"),
            ("min.compaction.lag.ms", "0"),
        ],
    );
    // Offset 0 is a key never written again (it survives, so the archive
    // starts at 0); offsets 1..10 are k0..k9 and 11..20 the same keys again:
    // the cleaner removes 1..10, a hole the position 3 sits in.
    let p = producer();
    p.send(
        BaseRecord::to(&topic)
            .partition(0)
            .key("keep")
            .payload("keep"),
    )
    .map_err(|(e, _)| e)
    .unwrap();
    for round in 0..2 {
        for k in 0..10 {
            p.send(
                BaseRecord::to(&topic)
                    .partition(0)
                    .key(&format!("k{k}"))
                    .payload(&format!("r{round}")),
            )
            .map_err(|(e, _)| e)
            .unwrap();
        }
        p.flush(Duration::from_secs(30)).unwrap();
        std::thread::sleep(Duration::from_millis(500));
    }
    // Roll and wait for the cleaner: the record after offset 0 must be 11 or
    // later.
    let deadline = Instant::now() + Duration::from_secs(240);
    loop {
        produce(&topic, 0, 100, 1);
        let c: BaseConsumer = rdkafka::config::ClientConfig::new()
            .set("bootstrap.servers", harness::bootstrap())
            .set("group.id", format!("probe-{n}"))
            .create()
            .unwrap();
        let mut tpl = rdkafka::TopicPartitionList::new();
        tpl.add_partition_offset(&topic, 0, rdkafka::Offset::Offset(0))
            .unwrap();
        c.assign(&tpl).unwrap();
        let mut seen = Vec::new();
        let until = Instant::now() + Duration::from_secs(5);
        while seen.len() < 2 && Instant::now() < until {
            if let Some(Ok(m)) = c.poll(Duration::from_millis(500)) {
                seen.push(m.offset());
            }
        }
        if seen.len() == 2 && seen[1] >= 11 {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "the cleaner never compacted {topic}"
        );
        std::thread::sleep(Duration::from_secs(5));
    }
    commit(&g, &[(&topic, 0, 3)]);
    let b = backup(&format!("p042-c7-{n}"), &topic, &[&g]);
    let r = restore(
        &restore_spec(&b, &topic, &format!("c7-{n}-"), true, ""),
        "c7",
    );
    let (_, mapping) = r
        .mapping
        .as_ref()
        .unwrap_or_else(|| panic!("a mapping:\n{}", text(&r.out)));
    let e = &mapping.groups[&g].entries[0];
    // Target 0 is source 0; target 1 is the first survivor after the hole.
    assert_eq!(
        (e.class.as_str(), e.target_offset),
        ("exact", Some(1)),
        "C7: the next surviving record: {e:?}"
    );
    // Control: no restored record carries lineage 3, so a lookup for
    // lineage = P finds nothing.
    let target = restored_topic(&r, &topic);
    let lineages: Vec<Option<i64>> = {
        let c: BaseConsumer = rdkafka::config::ClientConfig::new()
            .set("bootstrap.servers", harness::bootstrap())
            .set("group.id", format!("probe-t-{n}"))
            .create()
            .unwrap();
        let mut tpl = rdkafka::TopicPartitionList::new();
        tpl.add_partition_offset(&target, 0, rdkafka::Offset::Offset(0))
            .unwrap();
        c.assign(&tpl).unwrap();
        let mut out = Vec::new();
        let until = Instant::now() + Duration::from_secs(20);
        while out.len() < 3 && Instant::now() < until {
            if let Some(Ok(m)) = c.poll(Duration::from_millis(500)) {
                out.push(lineage(&m));
            }
        }
        out
    };
    assert!(
        !lineages.contains(&Some(3)),
        "C7 control: no record has lineage 3: {lineages:?}"
    );
    assert_eq!(
        lineages.get(1).copied().flatten().map(|l| l >= 11),
        Some(true)
    );
    write_evidence(
        "c7",
        &json!({"mapping": mapping, "first_lineages": format!("{lineages:?}")}),
    );
}

/// **C12.** Needs the `streams-protocol` profile.
#[test]
#[ignore = "needs the compose stack (4.3, streams-protocol)"]
fn an_empty_streams_group_is_refused_by_type() {
    let n = nonce();
    let topic = format!("sw-str-{n}");
    let g = format!("str-{n}");
    create_topic(&topic, 1, &[]);
    produce(&topic, 0, 0, 10);
    commit(&g, &[(&topic, 0, 5)]);
    let b = backup(&format!("p042-c12-{n}"), &topic, &[&g]);
    let r = restore(
        &restore_spec(&b, &topic, &format!("c12-{n}-"), true, ""),
        "c12",
    );
    let (sc_key, sc_bytes) = own_scorecard(&r);
    let streams = "logweir-e2e-streams-protocol";
    let _ = Command::new("bash")
        .arg("e2e/compose/groups.sh")
        .args(["stop", "streams"])
        .current_dir(root())
        .output();
    let target = restored_topic(&r, &topic);
    let p = plan(&r, &sc_bytes, &[&format!("{g}={streams}")], "c12");
    let a = approve(&p.path, "Switchover", "c12");
    let run = switchover(&r, &sc_key, &p, &a);
    let o = outcomes(&run);
    assert_eq!(
        (o[0].1.as_str(), o[0].2.as_deref()),
        ("refused", Some("TargetGroupTypeUnsupported")),
        "C12"
    );
    assert_eq!(committed(streams, &target, 1), vec![None], "C12: no commit");
    let _ = Command::new("bash")
        .arg("e2e/compose/groups.sh")
        .args(["start", "streams"])
        .current_dir(root())
        .output();
    write_evidence("c12", &json!({"record": run.record}));
}

/// **C13.** Needs the `acl` profile; the classification only (the restore's
/// target would have to be the `acl` cluster under its restricted principal).
#[test]
#[ignore = "needs the compose stack (4.3, acl)"]
fn a_hidden_target_group_is_not_visible_and_never_created() {
    use logweir::switchover::target::SwitchoverTarget;
    let o = Command::new("bash")
        .arg("e2e/compose/groups.sh")
        .args(["visibility", "apply"])
        .current_dir(root())
        .output()
        .unwrap();
    assert!(o.status.success(), "{}", text(&o));
    let restricted = logweir_kafka::rdkafka_reader::RdKafkaReader::connect(
        &[harness::bootstrap_acl_sasl()],
        logweir_kafka::reader::AuthConfig::ScramSha512 {
            username: harness::SCRAM_USER.to_string(),
            password: harness::SCRAM_PASSWORD.to_string(),
            tls: false,
            tls_ca_file: None,
        },
    )
    .expect("the restricted principal's reader");
    let hidden = restricted.classify("pa-hidden");
    let absent = restricted.classify("pa-never-existed-p042");
    let _ = Command::new("bash")
        .arg("e2e/compose/groups.sh")
        .args(["visibility", "remove"])
        .current_dir(root())
        .output();
    assert_eq!(
        hidden,
        logweir_core::switchover::TargetGroup::NotVisible,
        "C13"
    );
    assert_eq!(
        logweir_core::switchover::admit_group(&hidden),
        Err("TargetNotVisible")
    );
    // Control: listing-only classification would have called it absent (created).
    assert_ne!(hidden, logweir_core::switchover::TargetGroup::Absent);
    write_evidence(
        "c13",
        &json!({"hidden": format!("{hidden:?}"), "absent": format!("{absent:?}")}),
    );
}

/// **C16.**
#[test]
fn a_transactional_archive_maps_nothing() {
    let n = nonce();
    let topic = format!("sw-txn-{n}");
    let g = format!("txn-{n}");
    create_topic(&topic, 1, &[]);
    let p: BaseProducer = rdkafka::config::ClientConfig::new()
        .set("bootstrap.servers", harness::bootstrap())
        .set("transactional.id", format!("p042-{n}"))
        .create()
        .unwrap();
    p.init_transactions(Duration::from_secs(30)).unwrap();
    for _ in 0..2 {
        p.begin_transaction().unwrap();
        for i in 0..3 {
            p.send(
                BaseRecord::to(&topic)
                    .partition(0)
                    .key("k")
                    .payload(&format!("t{i}")),
            )
            .map_err(|(e, _)| e)
            .unwrap();
        }
        p.commit_transaction(Duration::from_secs(30)).unwrap();
    }
    // A read_committed consumer that read the first transaction sits at 4
    // (3 records and the marker): P = 3 is the marker itself.
    commit(&g, &[(&topic, 0, 3)]);
    let b = backup(&format!("p042-c16-{n}"), &topic, &[&g]);
    let r = restore(
        &restore_spec(
            &b,
            &topic,
            &format!("c16-{n}-"),
            true,
            "restore:\n\x20 allow_transaction_markers: true\n",
        ),
        "c16",
    );
    let (_, mapping) = r
        .mapping
        .as_ref()
        .unwrap_or_else(|| panic!("a mapping:\n{}", text(&r.out)));
    for e in &mapping.groups[&g].entries {
        assert_eq!(
            e.reason.as_deref(),
            Some("TransactionalArchive"),
            "C16: {e:?}"
        );
    }
    write_evidence("c16", &json!({"mapping": mapping}));
}

/// **C14, and C2 on 3.9.** Run on a slot brought up with `--kafka 3.9`.
#[test]
#[ignore = "needs the compose stack (3.9)"]
fn an_absent_group_is_created_on_3_9() {
    let n = nonce();
    let topic = format!("sw-39-{n}");
    let (g, live) = (format!("g39-{n}"), format!("live39-{n}"));
    create_topic(&topic, 1, &[]);
    produce(&topic, 0, 0, 20);
    commit(&g, &[(&topic, 0, 10)]);
    commit(&live, &[(&topic, 0, 10)]);
    let b = backup(&format!("p042-c14-{n}"), &topic, &[&g, &live]);
    let r = restore(
        &restore_spec(&b, &topic, &format!("c14-{n}-"), true, ""),
        "c14",
    );
    assert_eq!(r.out.status.code(), Some(0), "{}", text(&r.out));
    let target = restored_topic(&r, &topic);
    let (sc_key, sc_bytes) = own_scorecard(&r);
    let created = format!("new39-{n}");
    let p = plan(&r, &sc_bytes, &[&format!("{g}={created}"), &live], "c14");
    let a = approve(&p.path, "Switchover", "c14");
    start_member(&live, &target, false);
    let run = switchover(&r, &sc_key, &p, &a);
    stop_member(&live);
    let rec = run.record.clone().expect("a record");
    let new = rec.groups.iter().find(|x| x.target == created).unwrap();
    assert!(new.created && new.outcome == "applied", "C14: {new:?}");
    assert_eq!(committed(&created, &target, 1), vec![Some(10)]);
    let l = rec.groups.iter().find(|x| x.target == live).unwrap();
    assert_eq!(
        (l.outcome.as_str(), l.reason.as_deref()),
        ("refused", Some("GroupActive")),
        "C2 on 3.9"
    );
    write_evidence("c14_c2_39", &json!({"record": rec}));
}

// ============================================================ §3.8: the review

/// The `switchoverReadiness` plan over `r`'s evidence at `scorecard_key`.
fn review_plan(r: &Restore, scorecard_key: &str) -> logweir_core::check_contract::CheckPlan {
    use logweir_core::check_contract as cc;
    use logweir_core::destination as d;
    let location = d::DestinationLocation {
        provider: d::StorageProvider::S3,
        bucket: harness::EVIDENCE_BUCKET.to_string(),
        prefix: String::new(),
        region: Some("us-east-1".to_string()),
        endpoint: Some(harness::s3_endpoint()),
        addressing: d::Addressing::PathStyle,
        transport: d::TransportSecurity::InsecureHttp,
    };
    let keyring: logweir_core::execution_contract::EvidenceKeyring =
        serde_json::from_slice(&std::fs::read(evidence_keyring()).expect("the keyring"))
            .expect("a keyring");
    cc::CheckPlan {
        contract: cc::CHECK_PLAN_CONTRACT.to_string(),
        contract_version: cc::CHECK_CONTRACT_VERSION,
        subject_uid: "pf-review".to_string(),
        timeout_seconds: 120,
        policy_digest: None,
        request: cc::CheckRequest::SwitchoverReadiness(Box::new(cc::SwitchoverReadinessRequest {
            restore_name: "cli-restore".to_string(),
            restore_uid: "cli-uid".to_string(),
            restore_plan_sha256: logweir_core::ids::sha256_prefixed(
                &std::fs::read(&r.spec).expect("the restore plan"),
            ),
            scorecard_key: scorecard_key.to_string(),
            target: cc::ConnectionPlan {
                bootstrap_servers: vec![harness::bootstrap()],
                auth_mode: "plaintext".to_string(),
                username: None,
                password_env: None,
                tls: Some(false),
                ca_file: None,
                client_cert_file: None,
                client_key_file: None,
                principal: "User:ANONYMOUS".to_string(),
            },
            evidence: cc::DestinationPlan {
                location_digest: location.location_digest(),
                location,
                name: "compose-evidence".to_string(),
                uid: "dest-uid".to_string(),
                ca_file: None,
                credentials: cc::CredentialMode::Static,
                grant_bindings: Vec::new(),
            },
            keyring,
            groups: Vec::new(),
        })),
    }
}

/// `logweir check run` over `plan`, decoded with the controller's decoder.
fn review(
    plan: &logweir_core::check_contract::CheckPlan,
    label: &str,
) -> (Output, logweir_core::check_contract::CheckRelay) {
    use logweir_core::check_contract as cc;
    let bytes = serde_json::to_vec(plan).expect("serialises");
    let sha = logweir_core::ids::sha256_prefixed(&bytes);
    let path = demo_dir().join(format!("p042-{label}-check-plan.json"));
    std::fs::write(&path, &bytes).expect("written");
    let mut c = Command::new(bin());
    c.args(["check", "run", "--plan"])
        .arg(&path)
        .args(["--check-contract-version", "1"])
        .env("LOGWEIR_CHECK_CONTRACT_VERSION", "1")
        .env("LOGWEIR_CHECK_PLAN_SHA256", &sha)
        .env("LOGWEIR_CHECK_SUBJECT_UID", "pf-review");
    s3_env(&mut c);
    let o = output_within(c, 180);
    let mut d = cc::frames::Decoder::new();
    for l in String::from_utf8_lossy(&o.stdout).lines() {
        d.push_line(l)
            .unwrap_or_else(|e| panic!("a frame does not decode ({e}):\n{}", text(&o)));
    }
    let relay = d
        .finish(&cc::FrameExpectations {
            plan_sha256: sha,
            subject_uid: "pf-review".to_string(),
        })
        .unwrap_or_else(|e| panic!("the relay does not finish ({e}):\n{}", text(&o)));
    (o, relay)
}

fn review_row(
    relay: &logweir_core::check_contract::CheckRelay,
    id: logweir_core::check_contract::CheckId,
) -> logweir_core::check_contract::CheckOutcome {
    relay
        .result()
        .and_then(Result::ok)
        .and_then(|r| r.checks.into_iter().find(|c| c.id == id))
        .unwrap_or_else(|| panic!("no {id} row"))
}

/// **§3.8 (K1's preview): the review reads every position before the
/// switchover, and changes nothing.**
#[test]
fn the_review_reads_every_position_before_the_switchover() {
    use logweir_core::check_contract::{CheckCode, CheckId, CheckState, Stream};
    let n = nonce();
    let topic = format!("sw-review-{n}");
    let app = format!("review-{n}");
    create_topic(&topic, 3, &[]);
    for p in 0..3 {
        produce(&topic, p, 0, 50);
    }
    commit(&app, &[(&topic, 0, 10), (&topic, 1, 20), (&topic, 2, 30)]);
    let b = backup(&format!("p042-review-{n}"), &topic, &[&app]);
    let r = restore(
        &restore_spec(&b, &topic, &format!("rv-{n}-"), true, ""),
        "review",
    );
    assert_eq!(r.out.status.code(), Some(0), "{}", text(&r.out));
    let target = restored_topic(&r, &topic);
    let (_, mapping) = r.mapping.as_ref().expect("a mapping");
    let (sc_key, _) = own_scorecard(&r);

    // 1. Every position, exact, nothing committed; the target is the restore's.
    let (out, relay) = review(&review_plan(&r, &sc_key), "review-ready");
    assert_eq!(out.status.code(), Some(0), "{}", text(&out));
    let ev = review_row(&relay, CheckId::SwitchoverEvidence);
    assert_eq!(
        (ev.state, ev.code),
        (CheckState::Ready, CheckCode::RestoreEvidenceVerified),
        "{}",
        ev.message
    );
    let tg = review_row(&relay, CheckId::SwitchoverTarget);
    assert_eq!(
        (tg.state, tg.code),
        (CheckState::Ready, CheckCode::TargetIsTheRestores),
        "{}",
        tg.message
    );
    let gr = review_row(&relay, CheckId::SwitchoverGroups);
    assert_eq!(
        (gr.state, gr.code),
        (CheckState::Ready, CheckCode::GroupsAdmissible),
        "{}",
        gr.message
    );
    assert_eq!(gr.facts.get("exact").map(String::as_str), Some("3"));
    let details =
        String::from_utf8(relay.stream(Stream::Details).expect("details").to_vec()).expect("utf-8");
    let lines: Vec<Value> = details
        .lines()
        .map(|l| serde_json::from_str(l).expect("a JSON line"))
        .collect();
    assert_eq!(lines.len(), 1, "{details}");
    assert_eq!(lines[0]["group"], app.as_str());
    assert_eq!(lines[0]["topic"], target.as_str());
    let mapped: Vec<Value> = mapping.groups[&app]
        .entries
        .iter()
        .map(|e| {
            json!({"partition": e.partition, "class": "exact", "committed": null,
                        "offset": e.target_offset})
        })
        .collect();
    assert_eq!(lines[0]["positions"], Value::Array(mapped), "{details}");
    assert_eq!(
        committed(&app, &target, 3),
        vec![None, None, None],
        "the review commits nothing"
    );

    // 2. A live member on the target group: `GroupActive`, by name.
    start_member(&app, &target, false);
    let (_, relay) = review(&review_plan(&r, &sc_key), "review-active");
    let gr = review_row(&relay, CheckId::SwitchoverGroups);
    stop_member(&app);
    assert_eq!(
        (gr.state, gr.code),
        (CheckState::NotReady, CheckCode::GroupNotAdmissible),
        "{}",
        gr.message
    );
    assert!(gr.message.starts_with("GroupActive"), "{}", gr.message);

    // 3. The same scorecard without `target.creation` (re-signed).
    let (_, relay) = review(
        &review_plan(&r, &without_creation(&r)),
        "review-no-creation",
    );
    let tg = review_row(&relay, CheckId::SwitchoverTarget);
    assert_eq!(
        (tg.state, tg.code),
        (CheckState::NotReady, CheckCode::TargetNotTheRestores),
        "{}",
        tg.message
    );
    assert!(
        tg.message.starts_with("TargetIdentityUnknown"),
        "{}",
        tg.message
    );
    write_evidence(
        "review",
        &json!({
            "restoreScorecardKey": sc_key,
            "rows": {"evidence": ev.message, "target": "ready", "groupsReady": "every group",
                     "groupsActive": gr.message},
            "details": lines,
            "committedAfter": committed(&app, &target, 3),
            "withoutCreation": tg.message,
        }),
    );
}
