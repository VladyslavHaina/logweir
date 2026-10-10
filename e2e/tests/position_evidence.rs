#![cfg(feature = "e2e")]
//! **PROD-04.1 — consumer position evidence, through the shipped `logweir
//! backup run`, against a real broker.** Every row reads the signed receipt
//! and the positions document it binds back from the bucket and checks them
//! against the broker's OWN tools (`kafka-consumer-groups.sh`,
//! `e2e/compose/groups.sh list`), never against the code under test alone;
//! every accepted receipt is verified by both readers WITH its positions
//! document, which must print the same `consumer_positions` lines, and both
//! readers refuse the same document with one byte changed (arm CP-2).
//!
//! | row | proves | negative control |
//! |---|---|---|
//! | `every_selected_group_gets_one_outcome_in_the_signed_receipt` | on any line: one outcome per selected id (plan and `--consumer-group`); on a line that types groups (3.9, 4.x) classic and consumer groups captured with the broker's type and state, each position equal to the CLI's and related to archived data, every partition listed (a never-committed one `noCommittedPosition`), share and streams groups `GroupTypeNotCaptured`, an absent id `GroupNotFound`; on 3.7.1 (no type served, PROD-04.0 §14.1) every fixture group `GroupTypeNotCaptured`; the catalog point binds the block | a position the CLI has no row for, read as 0, fails the comparison; the absent id is still absent after |
//! | `a_group_hidden_from_the_backup_principal_is_never_absent` (`acl`) | with §3.9's visibility setup, a backup as the restricted principal records the hidden group `failed: NotVisibleToPrincipal`, the visible one captured, an absent id `GroupNotFound` | the same backup as the super user captures the hidden group |
//! | `a_group_that_rebalances_during_the_capture_is_captured_active` | a classic group caught in a rebalance (a second member joined while the first is stopped) is captured in a rebalance state, `active`, its positions the CLI's | an `Empty` reading would fail the state check |
//! | `a_position_beyond_the_end_is_excluded_and_the_end_is_captured` | a non-member commit of 10 on a 4-record partition is `excluded: PositionBeyondEnd` (TI-04.1-2), 4 is `atArchiveEnd`, 2 `withinArchive` | the commit at the end itself is captured |
//! | `expired_records_and_deleted_offsets_are_never_read_as_positions` | a position below the log start (DeleteRecords) is `beforeLogStart`; a group whose offsets on one topic were deleted (the shape offset expiry leaves) is `noCommittedPosition` there, never 0, and keeps its other topic's position | the CLI shows no row for the deleted offsets |
//! | `a_partition_added_during_the_capture_is_listed_not_observed` | a partition added while the engine runs is listed for each captured group as `notObserved: PartitionAddedDuringCapture` | the partitions read at capture are captured |
//! | `the_capture_is_readable_after_the_source_topic_and_group_are_gone` | after the backup the source topic and group are deleted; the receipt still verifies under both readers and still says where the group was | the source itself now answers the group absent |
//! | `a_large_selection_keeps_the_receipt_small_and_its_positions_verified` | review H1's two sizes live — 100 groups (the most) over 10 topics of 11 partitions, and 10 groups over 20 of 12, one record in every partition and every group committed at its end: the signed receipt stays under 128 KiB (half the catalog's read cap; measured 65,530 and 52,049 bytes, most of it the topics' configuration model) and its block under `MAX_BLOCK_BYTES`, every group is captured with every position related, the positions document holds all of them and both readers verify it, and the catalog point carries the summary | the positions document itself is over 256 KiB at both sizes: inline, the receipt would have been `Unreadable` to the catalog |
//!
//! # Running them
//!
//! ```text
//! eval "$(e2e/compose/stack-env.sh --slot 2 --kafka 4.3 --profiles acl,streams-protocol,auth)"
//! just e2e-up
//! cargo build -p logweir
//! LOGWEIR_PYTHON=<python with cryptography> AWS_EC2_METADATA_DISABLED=true \
//!   cargo test -p e2e --features e2e --test position_evidence -- --include-ignored --test-threads=1 --nocapture
//! just e2e-down
//! ```
//!
//! On the default 3.7.1 line (`--slot 4 --profiles auth`, CI's e2e job) only
//! the first row runs; the ignored rows need a broker that types groups.
//! Each row writes what it observed to `position-evidence/<version>/<row>.json`
//! under the stack's scratch directory (`harness::demo_dir()`).
mod harness;

use harness::{bin, demo_dir, engine_bin, engine_digest, engine_mount, engine_version, root};
use logweir_core::backup_receipt::BackupReceipt;
use logweir_core::consumer_positions::{
    ConsumerPositions, GroupSnapshot, PositionEntry, PositionsDocument, RELATED,
};
use logweir_kafka::positions::{CommittedPosition, TopicPartition};
use logweir_kafka::rdkafka_reader::RdKafkaReader;
use logweir_kafka::reader::AuthConfig;
use serde_json::{json, Value};
use std::collections::BTreeMap;
use std::io::{BufRead, Read};
use std::process::{Command, Output, Stdio};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

// ============================================================ processes

/// Run `cmd` to completion or kill it after `secs`, reading both pipes
/// concurrently so a chatty child cannot deadlock on a full pipe.
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

/// `e2e/compose/groups.sh ARGS`, on the stack this process addresses.
fn groups_sh(args: &[&str], secs: u64) -> String {
    harness::stack::ensure_coherent();
    let mut c = Command::new("bash");
    c.arg("e2e/compose/groups.sh")
        .args(args)
        .current_dir(root());
    let o = output_within(c, secs);
    assert!(
        o.status.success(),
        "groups.sh {args:?} failed:\n{}",
        text(&o)
    );
    String::from_utf8_lossy(&o.stdout).into_owned()
}

// ============================================================ the brokers

/// One Kafka cluster of the stack.
struct Broker {
    service: &'static str,
    profile: Option<&'static str>,
    in_network: &'static str,
    plaintext: String,
}

fn default_broker() -> Broker {
    harness::stack::ensure_coherent();
    Broker {
        service: "kafka-broker-1",
        profile: None,
        in_network: "kafka-broker-1:9094",
        plaintext: harness::bootstrap(),
    }
}

fn acl_broker() -> Broker {
    harness::stack::ensure_coherent();
    Broker {
        service: "kafka-acl",
        profile: Some("acl"),
        in_network: "kafka-acl:9094",
        plaintext: harness::bootstrap_acl(),
    }
}

impl Broker {
    /// A command inside the RUNNING broker container, bounded; `detach` runs
    /// it in the background (`exec -d`).
    fn exec(&self, args: &[&str], detach: bool) -> Output {
        harness::stack::ensure_coherent();
        let mut c = Command::new("docker");
        c.args([
            "compose",
            "-p",
            &harness::stack::project(),
            "-f",
            "e2e/compose/docker-compose.yml",
        ]);
        if let Some(p) = self.profile {
            c.args(["--profile", p]);
        }
        c.arg("exec").arg("-T");
        if detach {
            c.arg("-d");
        }
        c.arg(self.service).args(args).current_dir(root());
        c.stdin(Stdio::null());
        output_within(c, 120)
    }

    fn cli_ok(&self, args: &[&str], what: &str) -> String {
        let o = self.exec(args, false);
        assert!(o.status.success(), "{what} failed:\n{}", text(&o));
        String::from_utf8_lossy(&o.stdout).into_owned()
    }

    fn version(&self) -> String {
        self.cli_ok(
            &["/opt/kafka/bin/kafka-topics.sh", "--version"],
            "--version",
        )
        .split_whitespace()
        .next()
        .unwrap_or("unknown")
        .to_string()
    }

    fn reader(&self) -> RdKafkaReader {
        RdKafkaReader::connect(std::slice::from_ref(&self.plaintext), AuthConfig::Plaintext)
            .expect("connect builds local state")
    }

    /// `(topic, partition)` → CURRENT-OFFSET as the broker's CLI prints it
    /// (`None` for its `-`). A partition with NO row is absent: the CLI's "no
    /// committed offset".
    fn cli_offsets(&self, group: &str) -> BTreeMap<(String, i32), Option<i64>> {
        let out = self.exec(
            &[
                "/opt/kafka/bin/kafka-consumer-groups.sh",
                "--bootstrap-server",
                self.in_network,
                "--describe",
                "--group",
                group,
            ],
            false,
        );
        let mut m = BTreeMap::new();
        for line in text(&out).lines() {
            let t: Vec<&str> = line.split_whitespace().collect();
            if t.len() >= 4 && t[0] == group {
                if let Ok(p) = t[2].parse::<i32>() {
                    m.insert((t[1].to_string(), p), t[3].parse::<i64>().ok());
                }
            }
        }
        m
    }

    /// The broker's STATE for `group` (`--describe --state`).
    fn cli_state(&self, group: &str) -> Option<String> {
        let o = self.exec(
            &[
                "/opt/kafka/bin/kafka-consumer-groups.sh",
                "--bootstrap-server",
                self.in_network,
                "--describe",
                "--group",
                group,
                "--state",
            ],
            false,
        );
        text(&o).lines().find_map(|line| {
            let t: Vec<&str> = line.split_whitespace().collect();
            (t.len() >= 3 && t[0] == group).then(|| t[t.len() - 2].to_string())
        })
    }

    fn create_topic(&self, topic: &str, partitions: u32) {
        let p = partitions.to_string();
        self.cli_ok(
            &[
                "/opt/kafka/bin/kafka-topics.sh",
                "--bootstrap-server",
                self.in_network,
                "--create",
                "--topic",
                topic,
                "--partitions",
                &p,
                "--replication-factor",
                "1",
            ],
            &format!("create {topic}"),
        );
        harness::await_created_on(&self.plaintext, topic, partitions as i32);
    }

    fn delete_topic(&self, topic: &str) {
        let _ = self.exec(
            &[
                "/opt/kafka/bin/kafka-topics.sh",
                "--bootstrap-server",
                self.in_network,
                "--delete",
                "--topic",
                topic,
            ],
            false,
        );
    }

    /// `count` keyed records to `topic`'s partition `partition`.
    fn produce(&self, topic: &str, partition: i32, count: usize) {
        use rdkafka::producer::{BaseProducer, BaseRecord, Producer};
        let producer: BaseProducer = rdkafka::config::ClientConfig::new()
            .set("bootstrap.servers", &self.plaintext)
            .set("message.timeout.ms", "15000")
            .create()
            .expect("a producer");
        for i in 0..count {
            let payload = format!("{topic}-{partition}-{i}");
            let key = format!("k{i}");
            loop {
                match producer.send(
                    BaseRecord::to(topic)
                        .partition(partition)
                        .key(&key)
                        .payload(&payload),
                ) {
                    Ok(()) => break,
                    Err((e, _)) if e.to_string().contains("QueueFull") => {
                        producer.poll(Duration::from_millis(50));
                    }
                    Err((e, _)) => panic!("produce to {topic}: {e}"),
                }
            }
        }
        producer
            .flush(Duration::from_secs(30))
            .unwrap_or_else(|e| panic!("flush {topic}: {e}"));
    }

    /// One record in every partition of every topic in `topics`, through ONE
    /// producer flushed once.
    fn produce_everywhere(&self, topics: &[String], partitions: i32) {
        use rdkafka::producer::{BaseProducer, BaseRecord, Producer};
        let producer: BaseProducer = rdkafka::config::ClientConfig::new()
            .set("bootstrap.servers", &self.plaintext)
            .set("message.timeout.ms", "30000")
            .create()
            .expect("a producer");
        for topic in topics {
            for partition in 0..partitions {
                let payload = format!("{topic}-{partition}");
                loop {
                    match producer.send(
                        BaseRecord::to(topic)
                            .partition(partition)
                            .key("k")
                            .payload(&payload),
                    ) {
                        Ok(()) => break,
                        Err((e, _)) if e.to_string().contains("QueueFull") => {
                            producer.poll(Duration::from_millis(50));
                        }
                        Err((e, _)) => panic!("produce to {topic}: {e}"),
                    }
                }
            }
        }
        producer
            .flush(Duration::from_secs(60))
            .unwrap_or_else(|e| panic!("flush: {e}"));
    }

    /// A non-member commit of `offset` on each `(topic, partition)` for
    /// `group` (PROD-04.0a's commit): the broker accepts it from outside the
    /// group while the group has no member, and does not compare the offset
    /// with the log end (PROD-01.4 §7).
    fn commit(&self, group: &str, positions: &[(&str, i32, i64)]) {
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
            match self.reader().commit_positions(group, &positions) {
                Ok(()) => return,
                Err(e) if Instant::now() < deadline => {
                    eprintln!("[prod-04-1] commit {group} retrying: {e}");
                    std::thread::sleep(Duration::from_secs(2));
                }
                Err(e) => panic!("commit {group}: {e}"),
            }
        }
    }

    /// CreatePartitions to `total`, through the admin API, waited on.
    fn add_partitions(&self, topic: &str, total: usize) {
        use rdkafka::admin::{AdminClient, AdminOptions, NewPartitions};
        use rdkafka::client::DefaultClientContext;
        let admin: AdminClient<DefaultClientContext> = rdkafka::config::ClientConfig::new()
            .set("bootstrap.servers", &self.plaintext)
            .create()
            .expect("an admin client");
        let res = block_on(admin.create_partitions(
            &[NewPartitions::new(topic, total)],
            &AdminOptions::new().request_timeout(Some(Duration::from_secs(20))),
        ))
        .expect("the CreatePartitions call answers");
        for r in res {
            r.unwrap_or_else(|(t, e)| panic!("add partitions to {t}: {e}"));
        }
    }

    fn delete_group(&self, group: &str) {
        let _ = self.exec(
            &[
                "/opt/kafka/bin/kafka-consumer-groups.sh",
                "--bootstrap-server",
                self.in_network,
                "--delete",
                "--group",
                group,
            ],
            false,
        );
    }
}

/// Drive one rdkafka admin future to completion on THIS thread, with a hard
/// deadline (FX-4's helper: rdkafka resolves admin futures from its own
/// thread, so no async runtime is needed).
fn block_on<F: std::future::Future>(f: F) -> F::Output {
    use std::sync::Arc;
    use std::task::{Context, Poll, Wake, Waker};
    struct Unpark(std::thread::Thread);
    impl Wake for Unpark {
        fn wake(self: Arc<Self>) {
            self.0.unpark();
        }
    }
    let waker = Waker::from(Arc::new(Unpark(std::thread::current())));
    let mut cx = Context::from_waker(&waker);
    let mut f = std::pin::pin!(f);
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        if let Poll::Ready(v) = f.as_mut().poll(&mut cx) {
            return v;
        }
        assert!(
            Instant::now() < deadline,
            "an admin call did not answer within 60 s"
        );
        std::thread::park_timeout(Duration::from_millis(100));
    }
}

/// Deletes a row's topics and groups on every exit path, a panicking
/// assertion included.
struct Cleanup<'a> {
    broker: &'a Broker,
    topics: Vec<String>,
    groups: Vec<String>,
    /// `pkill` patterns of background members, killed (after a SIGCONT).
    members: Vec<String>,
}

impl Drop for Cleanup<'_> {
    fn drop(&mut self) {
        for m in &self.members {
            let _ = self.broker.exec(&["pkill", "-CONT", "-f", m], false);
            let _ = self.broker.exec(&["pkill", "-TERM", "-f", m], false);
        }
        for g in &self.groups {
            self.broker.delete_group(g);
        }
        for t in &self.topics {
            self.broker.delete_topic(t);
        }
    }
}

/// Whether this broker types groups (ListGroups v5): 3.9 and 4.x do, 3.7.1
/// does not (PROD-04.0 §14.1), which `groups.sh list` shows as TYPE `-`.
fn types_groups(broker: &Broker) -> bool {
    let v = broker.version();
    !v.starts_with("3.7")
}

fn require_typed(broker: &Broker) {
    assert!(
        types_groups(broker),
        "this row needs a broker that types groups (3.9 or 4.x): on {} every group is \
         excluded GroupTypeNotCaptured (PROD-04.0 §14.1). Start the slot with --kafka 3.9 or \
         --kafka 4.3 (the module doc)",
        broker.version()
    );
}

// ============================================================ the pipeline

fn signing_pem() -> std::path::PathBuf {
    root().join("e2e/fixtures/signed/signing.pem")
}

fn allowlist() -> std::path::PathBuf {
    let p = demo_dir().join("prod041-backup-allowed-clusters.json");
    std::fs::write(
        &p,
        "{\"allowed_cluster_ids\": [\"SCRATCH-CLUSTER-NOT-THE-SOURCE\"]}\n",
    )
    .expect("written");
    p
}

fn s3_user() -> String {
    ["minio", "admin"].concat()
}

fn engine_env(c: &mut Command) {
    c.env("AWS_ACCESS_KEY_ID", s3_user())
        .env("AWS_SECRET_ACCESS_KEY", s3_user())
        .env("AWS_REGION", "us-east-1")
        .env("LOGWEIR_ENGINE_BIN", engine_bin())
        .env("LOGWEIR_ENGINE_VERSION", engine_version())
        .env("LOGWEIR_ENGINE_DIGEST", engine_digest())
        .env("LOGWEIR_E2E_ENGINE_MOUNT", engine_mount())
        .env("TMPDIR", engine_mount());
}

/// One `logweir backup run`'s inputs.
struct Plan<'a> {
    backup_id: String,
    bootstrap: String,
    /// The SCRAM principal on `kafka-acl` instead of PLAINTEXT.
    scram: bool,
    topics: &'a [&'a str],
    plan_groups: Vec<String>,
    cli_groups: Vec<String>,
}

/// What one `logweir backup run` printed and signed.
struct Backup {
    backup_id: String,
    receipt_key: String,
    receipt_bytes: Vec<u8>,
    receipt: BackupReceipt,
    catalog_key: Option<String>,
    /// The positions document the receipt binds, read back from the evidence
    /// store at the key the receipt names, and its exact bytes.
    document: Option<(Vec<u8>, PositionsDocument)>,
}

impl Backup {
    fn block(&self) -> &ConsumerPositions {
        self.receipt
            .consumer_positions
            .as_ref()
            .expect("a run that selects groups carries consumer_positions")
    }
    fn group(&self, id: &str) -> &GroupSnapshot {
        self.block()
            .groups
            .get(id)
            .unwrap_or_else(|| panic!("no outcome for {id}: {:#?}", self.block().groups))
    }
    /// The positions document, as put beside the receipt.
    fn doc(&self) -> &PositionsDocument {
        &self
            .document
            .as_ref()
            .expect("a run that selects groups puts its positions document")
            .1
    }
    /// `group`'s document entry for one partition, when it lists one.
    fn entry(&self, id: &str, topic: &str, partition: u32) -> Option<PositionEntry> {
        self.doc()
            .groups
            .get(id)?
            .positions
            .iter()
            .find(|e| e.topic == topic && e.partition == partition)
            .cloned()
    }
    /// `group`'s positions over EVERY partition the document names:
    /// `(topic, partition)` → `(status, position, coverage)`, a partition the
    /// sparse entry does not list read as `noCommittedPosition` — exactly
    /// what its `no_committed_position` count says, and checked to add up.
    fn positions_of(&self, id: &str) -> BTreeMap<(String, i32), Recorded> {
        let doc = self.doc();
        let g = doc
            .groups
            .get(id)
            .unwrap_or_else(|| panic!("{id} has no positions: it was not captured"));
        let mut out = BTreeMap::new();
        let mut unlisted = 0u32;
        for (topic, t) in &doc.topics {
            for f in &t.partitions {
                let key = (topic.clone(), f.partition as i32);
                match g
                    .positions
                    .iter()
                    .find(|e| &e.topic == topic && e.partition == f.partition)
                {
                    Some(e) => out.insert(key, (e.status.clone(), e.position, e.coverage.clone())),
                    None => {
                        unlisted += 1;
                        out.insert(key, ("noCommittedPosition".to_string(), None, None))
                    }
                };
            }
        }
        assert_eq!(
            unlisted, g.no_committed_position,
            "{id}: the unlisted partitions are exactly the ones counted"
        );
        out
    }
}

fn quoted(ids: &[String]) -> String {
    ids.iter()
        .map(|g| serde_json::to_string(g).unwrap())
        .collect::<Vec<_>>()
        .join(", ")
}

fn spec_for(plan: &Plan<'_>) -> std::path::PathBuf {
    let auth = if plan.scram {
        format!(
            "\x20 auth:\n\x20   mode: scramSha512\n\x20   username: {}\n",
            harness::SCRAM_USER
        )
    } else {
        String::new()
    };
    let groups = if plan.plan_groups.is_empty() {
        String::new()
    } else {
        format!("\x20 consumer_groups: [{}]\n", quoted(&plan.plan_groups))
    };
    let spec = demo_dir().join(format!("{}-backup.yaml", plan.backup_id));
    std::fs::write(
        &spec,
        format!(
            "backup_id: {id}\n\
             source:\n\
             \x20 bootstrap_servers: [{bootstrap}]\n\
             \x20 topics: [{topics}]\n\
             {auth}{groups}\
             storage:\n\
             \x20 backend: s3\n\
             \x20 bucket: {bucket}\n\
             \x20 prefix: {id}\n\
             \x20 region: us-east-1\n\
             \x20 endpoint: {endpoint}\n\
             \x20 path_style: true\n\
             \x20 allow_http: true\n\
             backup:\n\
             \x20 compression: zstd\n\
             \x20 segment_max_records: 1000\n\
             \x20 segment_max_bytes: 10485760\n\
             \x20 max_concurrent_partitions: 3\n",
            id = plan.backup_id,
            bootstrap = plan.bootstrap,
            topics = plan.topics.join(", "),
            bucket = harness::ARCHIVE_BUCKET,
            endpoint = harness::s3_endpoint(),
        ),
    )
    .expect("the backup spec");
    spec
}

fn backup_command(plan: &Plan<'_>) -> Command {
    let mut c = Command::new(bin());
    c.args(["backup", "run", "--spec"])
        .arg(spec_for(plan))
        .arg("--allowed-clusters")
        .arg(allowlist())
        .arg("--signing-key")
        .arg(signing_pem());
    for g in &plan.cli_groups {
        c.args(["--consumer-group", g]);
    }
    engine_env(&mut c);
    if plan.scram {
        c.env("LOGWEIR_SOURCE_PASSWORD", harness::SCRAM_PASSWORD);
    }
    c
}

fn archive_store(backup_id: &str) -> logweir_engine_oso::storage::Store {
    std::env::set_var("AWS_ACCESS_KEY_ID", s3_user());
    std::env::set_var("AWS_SECRET_ACCESS_KEY", s3_user());
    std::env::set_var("AWS_REGION", "us-east-1");
    let url: logweir_core::engine::StorageUrl = serde_yaml::from_str(&format!(
        "backend: s3\nbucket: {}\nprefix: {backup_id}\nregion: us-east-1\nendpoint: {}\n\
         path_style: true\nallow_http: true\n",
        harness::ARCHIVE_BUCKET,
        harness::s3_endpoint()
    ))
    .expect("a storage url");
    logweir_engine_oso::storage::Store::read_only_from_url(&url).expect("the archive store")
}

/// The signed receipt a finished run names, read back from the bucket.
fn finished(backup_id: &str, out: Output) -> Backup {
    assert_eq!(
        out.status.code(),
        Some(0),
        "logweir backup run {backup_id} must exit 0:\n{}",
        text(&out)
    );
    let stdout = String::from_utf8_lossy(&out.stdout).into_owned();
    let line = |prefix: &str| {
        stdout
            .lines()
            .find_map(|l| l.strip_prefix(prefix))
            .map(str::to_string)
    };
    let receipt_key = line("receipt-key=").expect("backup run prints receipt-key=");
    let catalog_key = line("catalog-key=");
    let (receipt_bytes, _) = archive_store(backup_id)
        .get_capped(
            &receipt_key,
            logweir_engine_oso::storage::caps::SIGNED_DOCUMENT,
        )
        .unwrap_or_else(|e| panic!("read {receipt_key}: {e}"));
    let receipt: BackupReceipt = serde_json::from_slice(&receipt_bytes).expect("a receipt");
    let document = receipt.consumer_positions.as_ref().map(|cp| {
        let (bytes, _) = archive_store(backup_id)
            // FX-31: the document is bound by the signed receipt, and is read
            // under the runner and CLI's cap for a signed evidence document.
            .get_capped(
                &cp.document.key,
                logweir_engine_oso::storage::caps::SIGNED_DOCUMENT,
            )
            .unwrap_or_else(|e| panic!("read {}: {e}", cp.document.key));
        let doc: PositionsDocument = serde_json::from_slice(&bytes).expect("a positions document");
        (bytes, doc)
    });
    Backup {
        backup_id: backup_id.to_string(),
        receipt_key,
        receipt_bytes,
        receipt,
        catalog_key,
        document,
    }
}

fn backup(plan: &Plan<'_>) -> Backup {
    finished(&plan.backup_id, output_within(backup_command(plan), 900))
}

/// Both readers over the receipt, exactly as an auditor runs them, with no
/// broker: exit codes, and the `consumer_positions` lines each printed, which
/// must be the same.
fn verify_both(b: &Backup) -> Value {
    let dir = demo_dir().join(format!("{}-verify", b.backup_id));
    std::fs::create_dir_all(&dir).expect("dir");
    let doc = dir.join("receipt.json");
    let sig = dir.join("receipt.sig");
    std::fs::write(&doc, &b.receipt_bytes).expect("written");
    let (sidecar, _) = archive_store(&b.backup_id)
        .get_capped(
            &b.receipt_key.replace(".receipt.json", ".receipt.sig"),
            logweir_engine_oso::storage::caps::SIDECAR,
        )
        .expect("the sidecar");
    std::fs::write(&sig, sidecar).expect("written");
    let pubkey = root().join("e2e/fixtures/signed/public.pem");
    // The positions document, exactly as put, beside the receipt.
    let positions = dir.join("receipt.consumer-positions.json");
    std::fs::write(
        &positions,
        &b.document.as_ref().expect("the positions document").0,
    )
    .expect("written");
    let readers = |positions: &std::path::Path| {
        let mut rust = Command::new(bin());
        rust.args([
            "drill",
            "verify",
            "--payload-type",
            "backup-receipt",
            "--scorecard",
        ])
        .arg(&doc)
        .arg("--signature")
        .arg(&sig)
        .arg("--public-key")
        .arg(&pubkey)
        .arg("--consumer-positions")
        .arg(positions);
        let mut py = Command::new(harness::auditor_python());
        py.arg(root().join("docs/verify_scorecard.py"))
            .args(["--payload-type", "backup-receipt", "--consumer-positions"])
            .arg(positions)
            .arg(&doc)
            .arg(&sig)
            .arg(&pubkey);
        (output_within(rust, 60), output_within(py, 60))
    };
    // NEGATIVE CONTROL: one byte of the document changed after signing is
    // refused by BOTH readers at arm CP-2, never read as positions.
    let mut tampered_bytes = b.document.as_ref().unwrap().0.clone();
    let at = tampered_bytes
        .iter()
        .rposition(|c| c.is_ascii_digit())
        .expect("a digit");
    tampered_bytes[at] = if tampered_bytes[at] == b'9' {
        b'8'
    } else {
        tampered_bytes[at] + 1
    };
    let tampered = dir.join("tampered.consumer-positions.json");
    std::fs::write(&tampered, &tampered_bytes).expect("written");
    let (rust_t, py_t) = readers(&tampered);
    assert_eq!(
        rust_t.status.code(),
        Some(4),
        "drill verify:\n{}",
        text(&rust_t)
    );
    assert_eq!(
        py_t.status.code(),
        Some(1),
        "verify_scorecard.py:\n{}",
        text(&py_t)
    );
    for o in [&rust_t, &py_t] {
        assert!(
            text(o).contains("it is not the document this receipt signed"),
            "a tampered positions document is refused at CP-2:\n{}",
            text(o)
        );
    }
    let (rust, py) = readers(&positions);
    let lines = |o: &Output| -> Vec<String> {
        text(o)
            .lines()
            .filter_map(|l| {
                let i = l
                    .find("consumer_positions[")
                    .or_else(|| l.find("consumer_positions:"))?;
                Some(l[i..].to_string())
            })
            .collect()
    };
    let (r, p) = (lines(&rust), lines(&py));
    assert_eq!(
        rust.status.code(),
        Some(0),
        "drill verify:\n{}",
        text(&rust)
    );
    assert_eq!(
        py.status.code(),
        Some(0),
        "verify_scorecard.py:\n{}",
        text(&py)
    );
    assert!(
        !r.is_empty(),
        "drill verify printed no consumer_positions line"
    );
    assert_eq!(
        r, p,
        "the two readers print different consumer position lines"
    );
    assert!(
        r.iter()
            .any(|l| l.contains("verified against this receipt")),
        "{r:#?}"
    );
    json!({"rust_exit": 0, "python_exit": 0, "lines": r,
           "tampered": {"rust_exit": 4, "python_exit": 1}})
}

fn catalog_summary(b: &Backup) -> Value {
    let key = b
        .catalog_key
        .as_ref()
        .expect("backup run wrote its catalog point");
    let (bytes, _) = archive_store(&b.backup_id)
        .get_capped(key, logweir_engine_oso::storage::caps::SIGNED_DOCUMENT)
        .unwrap_or_else(|e| panic!("read {key}: {e}"));
    let record: Value = serde_json::from_slice(&bytes).expect("a catalog record");
    assert_eq!(
        record["consumer_positions"]["sha256"],
        b.block().digest().unwrap(),
        "the catalog point binds the receipt's block by its digest"
    );
    record["consumer_positions"].clone()
}

fn write_evidence(broker: &Broker, row: &str, v: &Value) {
    let dir = demo_dir().join("position-evidence").join(broker.version());
    std::fs::create_dir_all(&dir).expect("the evidence directory");
    let p = dir.join(format!("{row}.json"));
    std::fs::write(&p, serde_json::to_vec_pretty(v).expect("serialises")).expect("written");
    eprintln!("[prod-04-1] evidence: {}", p.display());
}

/// `(status, position, coverage)` of one partition, as the document records
/// it (or `noCommittedPosition` where its sparse entry counts the partition).
type Recorded = (String, Option<i64>, Option<String>);

// ============================================================ row 1

/// **One outcome per selected group, on whatever line the stack runs.** See
/// the module doc.
#[test]
fn every_selected_group_gets_one_outcome_in_the_signed_receipt() {
    let broker = default_broker();
    groups_sh(&["up"], 900);
    let fixture: Vec<(String, String, String)> = groups_sh(&["list"], 300)
        .lines()
        .skip(1)
        .filter_map(|l| {
            let t: Vec<&str> = l.split_whitespace().collect();
            (t.len() == 3).then(|| (t[0].to_string(), t[1].to_string(), t[2].to_string()))
        })
        .collect();
    assert!(!fixture.is_empty(), "groups.sh listed no group");
    let typed = types_groups(&broker);
    let absent = format!("pa-absent-{}", nonce());
    // The plan names every fixture group but the last; the command line the
    // last and the absent id: both selection paths, one outcome each.
    let mut plan_groups: Vec<String> = fixture.iter().map(|(g, ..)| g.clone()).collect();
    let last = plan_groups.pop().expect("a group");
    let b = backup(&Plan {
        backup_id: format!("p041-all-{}", nonce()),
        bootstrap: broker.plaintext.clone(),
        scram: false,
        topics: &["pa-orders"],
        plan_groups,
        cli_groups: vec![last, absent.clone()],
    });
    // AT LEAST the version that defines the block (a renumber at integration
    // makes it later, never earlier).
    harness::assert_format_at_least(
        &b.receipt.format_version,
        logweir_core::backup_receipt::FORMAT_VERSION_WITH_CONSUMER_POSITIONS,
        "the receipt of a backup that selects consumer groups",
    );
    let block = b.block();
    assert_eq!(
        block.groups.len(),
        fixture.len() + 1,
        "exactly one outcome per selected id"
    );
    assert_eq!(
        block.listing, "complete",
        "the super user's listing is complete"
    );
    let partitions = b.doc().topics["pa-orders"].partitions.len();
    assert_eq!(partitions, 3, "pa-orders has three partitions");

    let mut observed = serde_json::Map::new();
    for (group, ty, state) in &fixture {
        let g = b.group(group);
        observed.insert(group.clone(), serde_json::to_value(g).unwrap());
        match (typed, ty.as_str()) {
            // 3.7.1: no type is served, so the 3.7.1 rule excludes every group.
            (false, _) => assert_eq!(
                (
                    g.outcome.as_str(),
                    g.reason.as_deref(),
                    g.group_type.as_deref()
                ),
                ("excluded", Some("GroupTypeNotCaptured"), Some("other")),
                "{group} on a broker below ListGroups v5"
            ),
            (true, "Classic" | "Consumer") => {
                assert_eq!(g.outcome, "captured", "{group} ({ty}): {g:#?}");
                assert_eq!(g.group_type.as_deref(), Some(ty.to_lowercase().as_str()));
                assert_eq!(
                    g.state.as_deref(),
                    Some(state.as_str()),
                    "{group}: the CLI's state"
                );
                assert_eq!(g.active, Some(!matches!(state.as_str(), "Empty" | "Dead")));
                // Every partition listed; each position the CLI's; NEVER 0
                // for a partition the CLI has no committed offset for.
                let cli = broker.cli_offsets(group);
                let recorded = b.positions_of(group);
                assert_eq!(
                    recorded.len(),
                    partitions,
                    "{group}: every partition listed"
                );
                for p in 0..3 {
                    let key = ("pa-orders".to_string(), p);
                    let (status, position, coverage) = &recorded[&key];
                    match cli.get(&key).copied().flatten() {
                        Some(offset) => {
                            assert_eq!(
                                (status.as_str(), *position),
                                ("captured", Some(offset)),
                                "{group} pa-orders:{p}: the CLI says {offset}"
                            );
                            assert!(
                                coverage.as_deref().is_some_and(|c| RELATED.contains(&c)),
                                "{group} pa-orders:{p}: the archive holds pa-orders from 0, so \
                                 {coverage:?} must relate"
                            );
                        }
                        None => assert_eq!(
                            (status.as_str(), *position),
                            ("noCommittedPosition", None),
                            "NEGATIVE CONTROL {group} pa-orders:{p}: the CLI has no committed \
                             offset; absence is never offset 0"
                        ),
                    }
                }
            }
            (true, _) => assert_eq!(
                (
                    g.outcome.as_str(),
                    g.reason.as_deref(),
                    g.group_type.as_deref()
                ),
                ("excluded", Some("GroupTypeNotCaptured"), Some("other")),
                "{group} ({ty}): share and streams groups are not captured"
            ),
        }
    }
    let a = b.group(&absent);
    assert_eq!(
        (a.outcome.as_str(), a.reason.as_deref()),
        ("excluded", Some("GroupNotFound"))
    );
    let lists_absent = broker
        .cli_ok(
            &[
                "/opt/kafka/bin/kafka-consumer-groups.sh",
                "--bootstrap-server",
                broker.in_network,
                "--list",
            ],
            "--list",
        )
        .lines()
        .any(|l| l.trim() == absent);
    assert!(
        !lists_absent,
        "reading positions never created the absent group"
    );

    let verify = verify_both(&b);
    let summary = catalog_summary(&b);
    write_evidence(
        &broker,
        "every_selected_group_gets_one_outcome_in_the_signed_receipt",
        &json!({
            "kafka": broker.version(),
            "typed_line": typed,
            "fixture": fixture,
            "groups": observed,
            "topics": b.doc().topics,
            "verify": verify,
            "catalog_summary": summary,
        }),
    );
}

// ============================================================ row 2

/// A §3.9 visibility state on the `acl` profile, removed on every exit path.
struct Visibility;

impl Drop for Visibility {
    fn drop(&mut self) {
        let mut c = Command::new("bash");
        c.args(["e2e/compose/groups.sh", "visibility", "remove"])
            .current_dir(root());
        let _ = output_within(c, 300);
    }
}

/// **AP-04.1-6 through the shipped runner.** See the module doc.
#[test]
#[ignore = "needs the `acl` profile on a broker that types groups (3.9 or 4.x)"]
fn a_group_hidden_from_the_backup_principal_is_never_absent() {
    let broker = acl_broker();
    require_typed(&broker);
    groups_sh(&["visibility", "apply"], 300);
    let _visibility = Visibility;
    let absent = format!("pa-absent-{}", nonce());
    let selected = vec![
        "pa-visible".to_string(),
        "pa-hidden".to_string(),
        absent.clone(),
    ];

    let restricted = backup(&Plan {
        backup_id: format!("p041-hidden-{}", nonce()),
        bootstrap: harness::bootstrap_acl_sasl(),
        scram: true,
        topics: &["pa-orders"],
        plan_groups: selected.clone(),
        cli_groups: vec![],
    });
    let block = restricted.block();
    assert_eq!(
        block.listing, "notComplete",
        "no Describe on the cluster (T14)"
    );
    let hidden = restricted.group("pa-hidden");
    assert_eq!(
        (
            hidden.outcome.as_str(),
            hidden.reason.as_deref(),
            hidden.counts.is_none()
        ),
        ("failed", Some("NotVisibleToPrincipal"), true),
        "NEVER GroupNotFound: the group may exist, and this principal may not see it"
    );
    let visible = restricted.positions_of("pa-visible");
    assert_eq!(
        visible[&("pa-orders".to_string(), 0)].1,
        Some(5),
        "groups.sh commits 5 for pa-visible"
    );
    let a = restricted.group(&absent);
    assert_eq!(
        (a.outcome.as_str(), a.reason.as_deref()),
        ("excluded", Some("GroupNotFound")),
        "a describable absent id is absent by targeted describe"
    );
    let verify = verify_both(&restricted);

    // CONTROL: the super user captures the hidden group.
    let superuser = backup(&Plan {
        backup_id: format!("p041-hidden-su-{}", nonce()),
        bootstrap: broker.plaintext.clone(),
        scram: false,
        topics: &["pa-orders"],
        plan_groups: selected,
        cli_groups: vec![],
    });
    let seen = superuser.group("pa-hidden");
    assert_eq!(seen.outcome, "captured", "{seen:#?}");
    assert_eq!(
        superuser.positions_of("pa-hidden")[&("pa-orders".to_string(), 0)].1,
        Some(7)
    );
    write_evidence(
        &broker,
        "a_group_hidden_from_the_backup_principal_is_never_absent",
        &json!({
            "restricted": restricted.block().groups,
            "restricted_listing": restricted.block().listing,
            "superuser": superuser.block().groups,
            "verify": verify,
        }),
    );
}

// ============================================================ row 3

/// **A rebalance during the capture.** See the module doc.
#[test]
#[ignore = "needs a broker that types groups (3.9 or 4.x)"]
fn a_group_that_rebalances_during_the_capture_is_captured_active() {
    let broker = default_broker();
    require_typed(&broker);
    let n = nonce();
    let topic = format!("p041-rebalance-{n}");
    let group = format!("p041-rebalance-g-{n}");
    let member = |tag: &str| {
        format!(
            "timeout 600 /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server {net} \
             --topic {topic} --from-beginning --consumer-property client.id={tag} \
             --consumer-property session.timeout.ms=60000 \
             --consumer-property auto.commit.interval.ms=500 --group {group} > /dev/null 2>&1",
            net = broker.in_network
        )
    };
    // The pattern matches the member's own command line and never the
    // `bash -c` carrying it (groups.sh's bracket rule).
    let pattern = |tag: &str| format!("[c]lient.id={tag} ");
    let _cleanup = Cleanup {
        broker: &broker,
        topics: vec![topic.clone()],
        groups: vec![group.clone()],
        members: vec![pattern("p041a"), pattern("p041b")],
    };
    broker.create_topic(&topic, 2);
    broker.produce(&topic, 0, 10);
    broker.produce(&topic, 1, 10);
    broker.exec(&["bash", "-c", &member("p041a")], true);
    // Wait until member A has joined and committed everything.
    let deadline = Instant::now() + Duration::from_secs(120);
    loop {
        let offsets = broker.cli_offsets(&group);
        let state = broker.cli_state(&group);
        if state.as_deref() == Some("Stable")
            && offsets.values().filter_map(|o| *o).sum::<i64>() == 20
        {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "member A never caught up: {state:?} {offsets:?}"
        );
        std::thread::sleep(Duration::from_secs(2));
    }
    // Stop A, start B: the coordinator waits for A to rejoin until A's session
    // times out (60 s), so the group stays in a rebalance meanwhile.
    let stopped = broker.exec(&["pkill", "-STOP", "-f", &pattern("p041a")], false);
    assert!(
        stopped.status.success(),
        "SIGSTOP member A:\n{}",
        text(&stopped)
    );
    broker.exec(&["bash", "-c", &member("p041b")], true);
    let deadline = Instant::now() + Duration::from_secs(30);
    let rebalancing = loop {
        let state = broker.cli_state(&group).unwrap_or_default();
        if state == "PreparingRebalance" || state == "CompletingRebalance" {
            break state;
        }
        assert!(
            Instant::now() < deadline,
            "the group never rebalanced: {state}"
        );
        std::thread::sleep(Duration::from_millis(500));
    };
    let b = backup(&Plan {
        backup_id: format!("p041-rebalance-{n}"),
        bootstrap: broker.plaintext.clone(),
        scram: false,
        topics: &[topic.as_str()],
        plan_groups: vec![group.clone()],
        cli_groups: vec![],
    });
    let g = b.group(&group);
    assert_eq!(g.outcome, "captured", "{g:#?}");
    let states = [g.listed_state.clone(), g.state.clone()];
    assert!(
        states
            .iter()
            .flatten()
            .any(|s| s == "PreparingRebalance" || s == "CompletingRebalance"),
        "a rebalance state was observed: {states:?}"
    );
    assert_eq!(
        g.active,
        Some(true),
        "NEGATIVE CONTROL: a rebalancing group is never quiescent"
    );
    let cli = broker.cli_offsets(&group);
    for ((t, p), (status, position, _)) in b.positions_of(&group) {
        assert_eq!(status, "captured");
        assert_eq!(
            position,
            cli[&(t.clone(), p)],
            "{t}:{p}: the CLI's committed offset"
        );
    }
    let verify = verify_both(&b);
    write_evidence(
        &broker,
        "a_group_that_rebalances_during_the_capture_is_captured_active",
        &json!({"cli_state_before": rebalancing, "group": g, "verify": verify}),
    );
}

// ============================================================ row 4

/// **TI-04.1-2: a position beyond the end.** See the module doc.
#[test]
#[ignore = "needs a broker that types groups (3.9 or 4.x)"]
fn a_position_beyond_the_end_is_excluded_and_the_end_is_captured() {
    let broker = default_broker();
    require_typed(&broker);
    let n = nonce();
    let topic = format!("p041-end-{n}");
    let (beyond, at_end, within) = (
        format!("p041-beyond-{n}"),
        format!("p041-atend-{n}"),
        format!("p041-within-{n}"),
    );
    let _cleanup = Cleanup {
        broker: &broker,
        topics: vec![topic.clone()],
        groups: vec![beyond.clone(), at_end.clone(), within.clone()],
        members: vec![],
    };
    broker.create_topic(&topic, 1);
    broker.produce(&topic, 0, 4);
    broker.commit(&beyond, &[(topic.as_str(), 0, 10)]);
    broker.commit(&at_end, &[(topic.as_str(), 0, 4)]);
    broker.commit(&within, &[(topic.as_str(), 0, 2)]);
    let b = backup(&Plan {
        backup_id: format!("p041-end-{n}"),
        bootstrap: broker.plaintext.clone(),
        scram: false,
        topics: &[topic.as_str()],
        plan_groups: vec![beyond.clone(), at_end.clone(), within.clone()],
        cli_groups: vec![],
    });
    let key = (topic.clone(), 0);
    let facts = &b.doc().topics[&topic].partitions[0];
    assert_eq!(facts.high_watermark, Some(4));
    let p = |g: &str| {
        b.entry(g, &key.0, key.1 as u32)
            .expect("a committed partition is listed")
    };
    let e = p(&beyond);
    assert_eq!(
        (e.status.as_str(), e.reason.as_deref(), e.position),
        ("excluded", Some("PositionBeyondEnd"), Some(10))
    );
    let e = p(&at_end);
    assert_eq!(
        (e.status.as_str(), e.position, e.coverage.as_deref()),
        ("captured", Some(4), Some("atArchiveEnd")),
        "NEGATIVE CONTROL: the end itself is not beyond it"
    );
    let e = p(&within);
    assert_eq!(
        (e.status.as_str(), e.position, e.coverage.as_deref()),
        ("captured", Some(2), Some("withinArchive"))
    );
    let verify = verify_both(&b);
    write_evidence(
        &broker,
        "a_position_beyond_the_end_is_excluded_and_the_end_is_captured",
        &json!({"topic": b.doc().topics, "groups": b.block().groups,
                "positions": b.doc().groups, "verify": verify}),
    );
}

// ============================================================ row 5

/// **Expired records, and offsets the broker no longer holds.** See the
/// module doc.
#[test]
#[ignore = "needs a broker that types groups (3.9 or 4.x)"]
fn expired_records_and_deleted_offsets_are_never_read_as_positions() {
    let broker = default_broker();
    require_typed(&broker);
    let n = nonce();
    let (expired, other) = (format!("p041-expired-{n}"), format!("p041-other-{n}"));
    let (behind, deleted) = (format!("p041-behind-{n}"), format!("p041-deleted-{n}"));
    let _cleanup = Cleanup {
        broker: &broker,
        topics: vec![expired.clone(), other.clone()],
        groups: vec![behind.clone(), deleted.clone()],
        members: vec![],
    };
    broker.create_topic(&expired, 1);
    broker.create_topic(&other, 1);
    broker.produce(&expired, 0, 10);
    broker.produce(&other, 0, 3);
    broker.commit(&behind, &[(expired.as_str(), 0, 3), (other.as_str(), 0, 1)]);
    broker.commit(
        &deleted,
        &[(expired.as_str(), 0, 8), (other.as_str(), 0, 2)],
    );
    // The records before 6 expire (DeleteRecords: what retention does).
    let offsets = format!(
        "{{\"partitions\":[{{\"topic\":\"{expired}\",\"partition\":0,\"offset\":6}}],\"version\":1}}"
    );
    broker.cli_ok(
        &[
            "bash",
            "-c",
            &format!(
                "printf '%s' '{offsets}' > /tmp/p041-delete-records.json && \
                 /opt/kafka/bin/kafka-delete-records.sh --bootstrap-server {} \
                 --offset-json-file /tmp/p041-delete-records.json",
                broker.in_network
            ),
        ],
        "kafka-delete-records",
    );
    // `deleted`'s offsets on `expired` go (offset expiry's shape); its
    // position on `other` stays, so the group stays listed.
    broker.cli_ok(
        &[
            "/opt/kafka/bin/kafka-consumer-groups.sh",
            "--bootstrap-server",
            broker.in_network,
            "--delete-offsets",
            "--group",
            &deleted,
            "--topic",
            &expired,
        ],
        "kafka-consumer-groups --delete-offsets",
    );
    let cli = broker.cli_offsets(&deleted);
    assert!(
        !cli.contains_key(&(expired.clone(), 0)),
        "the CLI has no committed offset for {deleted} on {expired}: {cli:?}"
    );
    let b = backup(&Plan {
        backup_id: format!("p041-expired-{n}"),
        bootstrap: broker.plaintext.clone(),
        scram: false,
        topics: &[expired.as_str(), other.as_str()],
        plan_groups: vec![behind.clone(), deleted.clone()],
        cli_groups: vec![],
    });
    assert_eq!(b.doc().topics[&expired].partitions[0].log_start, Some(6));
    let behind_positions = b.positions_of(&behind);
    assert_eq!(
        behind_positions[&(expired.clone(), 0)],
        (
            "captured".to_string(),
            Some(3),
            Some("beforeLogStart".to_string())
        ),
        "the records it would read next expired from the source"
    );
    let deleted_positions = b.positions_of(&deleted);
    assert_eq!(
        deleted_positions[&(expired.clone(), 0)],
        ("noCommittedPosition".to_string(), None, None),
        "NEGATIVE CONTROL: a deleted offset is never offset 0"
    );
    assert_eq!(deleted_positions[&(other.clone(), 0)].1, Some(2));
    let verify = verify_both(&b);
    write_evidence(
        &broker,
        "expired_records_and_deleted_offsets_are_never_read_as_positions",
        &json!({"topics": b.doc().topics, "groups": b.block().groups,
                "positions": b.doc().groups, "verify": verify}),
    );
}

// ============================================================ row 6

/// **Partitions added during the capture.** The partition is added when the
/// runner announces the engine step (`progress-phase=-1:engine`), which it
/// prints after the group capture and before the engine.
#[test]
#[ignore = "needs a broker that types groups (3.9 or 4.x)"]
fn a_partition_added_during_the_capture_is_listed_not_observed() {
    let broker = default_broker();
    require_typed(&broker);
    let n = nonce();
    let topic = format!("p041-grow-{n}");
    let group = format!("p041-grow-g-{n}");
    let _cleanup = Cleanup {
        broker: &broker,
        topics: vec![topic.clone()],
        groups: vec![group.clone()],
        members: vec![],
    };
    broker.create_topic(&topic, 2);
    broker.produce(&topic, 0, 5);
    broker.produce(&topic, 1, 5);
    broker.commit(&group, &[(topic.as_str(), 0, 2), (topic.as_str(), 1, 3)]);
    let plan = Plan {
        backup_id: format!("p041-grow-{n}"),
        bootstrap: broker.plaintext.clone(),
        scram: false,
        topics: &[topic.as_str()],
        plan_groups: vec![group.clone()],
        cli_groups: vec![],
    };
    let mut cmd = backup_command(&plan);
    cmd.stdout(Stdio::piped()).stderr(Stdio::piped());
    let mut child = cmd.spawn().expect("spawn backup run");
    let stdout = child.stdout.take().expect("stdout");
    let mut stderr = child.stderr.take().expect("stderr");
    let t_err = std::thread::spawn(move || {
        let mut b = Vec::new();
        let _ = stderr.read_to_end(&mut b);
        b
    });
    let mut out = Vec::new();
    let mut added = false;
    for line in std::io::BufReader::new(stdout).lines() {
        let line = line.expect("a stdout line");
        if !added && line.trim() == "progress-phase=-1:engine" {
            // In process, through the admin API: a CLI in the container takes
            // seconds, and the engine can finish first.
            broker.add_partitions(&topic, 3);
            added = true;
        }
        out.extend_from_slice(line.as_bytes());
        out.push(b'\n');
    }
    // Bounded: the pipe closed, so the process is ending.
    let deadline = Instant::now() + Duration::from_secs(120);
    let status = loop {
        if let Some(s) = child.try_wait().expect("wait") {
            break s;
        }
        assert!(Instant::now() < deadline, "backup run did not exit");
        std::thread::sleep(Duration::from_millis(200));
    };
    assert!(added, "the runner never announced the engine step");
    let b = finished(
        &plan.backup_id,
        Output {
            status,
            stdout: out,
            stderr: t_err.join().unwrap_or_default(),
        },
    );
    let facts = &b.doc().topics[&topic].partitions;
    assert_eq!(
        facts.len(),
        3,
        "the partition read after the engine is listed: {:#?}",
        b.doc().topics
    );
    assert!(facts[0].observed && facts[1].observed && !facts[2].observed);
    let positions = b.positions_of(&group);
    assert_eq!(positions[&(topic.clone(), 0)].1, Some(2));
    assert_eq!(positions[&(topic.clone(), 1)].1, Some(3));
    let added_entry = b
        .entry(&group, &topic, 2)
        .expect("the added partition is listed, notObserved");
    assert_eq!(
        (
            added_entry.status.as_str(),
            added_entry.reason.as_deref(),
            added_entry.position
        ),
        ("notObserved", Some("PartitionAddedDuringCapture"), None),
        "NEGATIVE CONTROL: never dropped, never 0"
    );
    let verify = verify_both(&b);
    write_evidence(
        &broker,
        "a_partition_added_during_the_capture_is_listed_not_observed",
        &json!({"topic": b.doc().topics, "groups": b.block().groups,
                "positions": b.doc().groups, "verify": verify}),
    );
}

// ============================================================ row 7

/// **Source loss after the backup.** See the module doc.
#[test]
#[ignore = "needs a broker that types groups (3.9 or 4.x)"]
fn the_capture_is_readable_after_the_source_topic_and_group_are_gone() {
    let broker = default_broker();
    require_typed(&broker);
    let n = nonce();
    let topic = format!("p041-lost-{n}");
    let group = format!("p041-lost-g-{n}");
    let cleanup = Cleanup {
        broker: &broker,
        topics: vec![topic.clone()],
        groups: vec![group.clone()],
        members: vec![],
    };
    broker.create_topic(&topic, 1);
    broker.produce(&topic, 0, 5);
    broker.commit(&group, &[(topic.as_str(), 0, 3)]);
    let b = backup(&Plan {
        backup_id: format!("p041-lost-{n}"),
        bootstrap: broker.plaintext.clone(),
        scram: false,
        topics: &[topic.as_str()],
        plan_groups: vec![group.clone()],
        cli_groups: vec![],
    });
    assert_eq!(b.positions_of(&group)[&(topic.clone(), 0)].1, Some(3));
    // The source loses the group and the topic.
    drop(cleanup);
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        let verdict = broker
            .reader()
            .classify_groups(std::slice::from_ref(&group))
            .expect("a valid id");
        if matches!(
            verdict.verdicts[0].1,
            logweir_kafka::groups::GroupVerdict::Excluded(
                logweir_kafka::groups::Excluded::GroupNotFound { .. }
            )
        ) {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "the source still has {group}: {verdict:?}"
        );
        std::thread::sleep(Duration::from_secs(2));
    }
    // The evidence does not: read back from the bucket, verified with no broker.
    let reread = archive_store(&b.backup_id)
        .get_capped(
            &b.receipt_key,
            logweir_engine_oso::storage::caps::SIGNED_DOCUMENT,
        )
        .expect("the receipt is still in the bucket")
        .0;
    assert_eq!(reread, b.receipt_bytes);
    let verify = verify_both(&b);
    let lines = verify["lines"].as_array().unwrap();
    assert!(
        lines.iter().any(|l| l
            .as_str()
            .unwrap()
            .starts_with(&format!("consumer_positions[{group:?}]: captured"))),
        "{lines:?}"
    );
    let summary = catalog_summary(&b);
    write_evidence(
        &broker,
        "the_capture_is_readable_after_the_source_topic_and_group_are_gone",
        &json!({"verify": verify, "catalog_summary": summary, "group": b.group(&group)}),
    );
}

// ============================================================ row 8

/// **Review H1, live: the receipt stays small however many partitions the
/// selected groups hold.** See the module doc.
#[test]
#[ignore = "needs a broker that types groups (3.9 or 4.x)"]
fn a_large_selection_keeps_the_receipt_small_and_its_positions_verified() {
    let broker = default_broker();
    require_typed(&broker);
    let mut measured = Vec::new();
    for (groups, topics, partitions) in [(100usize, 10usize, 11i32), (10, 20, 12)] {
        let n = nonce();
        let names: Vec<String> = (0..topics)
            .map(|t| format!("p041-h1-{n}-t{t:02}"))
            .collect();
        let ids: Vec<String> = (0..groups)
            .map(|g| format!("p041-h1-{n}-g{g:03}"))
            .collect();
        let _cleanup = Cleanup {
            broker: &broker,
            topics: names.clone(),
            groups: ids.clone(),
            members: vec![],
        };
        for t in &names {
            broker.create_topic(t, partitions as u32);
        }
        // One record in every partition, through one producer: the backup
        // archives offset 0 everywhere, and a commit at 1 is `atArchiveEnd`.
        broker.produce_everywhere(&names, partitions);
        let every: Vec<(&str, i32, i64)> = names
            .iter()
            .flat_map(|t| (0..partitions).map(move |p| (t.as_str(), p, 1)))
            .collect();
        for id in &ids {
            broker.commit(id, &every);
        }
        let topic_refs: Vec<&str> = names.iter().map(String::as_str).collect();
        let b = backup(&Plan {
            backup_id: format!("p041-h1-{n}"),
            bootstrap: broker.plaintext.clone(),
            scram: false,
            topics: &topic_refs,
            plan_groups: ids.clone(),
            cli_groups: vec![],
        });
        let total = topics * partitions as usize;
        let (doc_bytes, doc) = b.document.as_ref().expect("the positions document");
        let size = json!({
            "groups": groups, "topics": topics, "partitions": partitions,
            "receipt_bytes": b.receipt_bytes.len(), "document_bytes": doc_bytes.len(),
        });
        eprintln!("[prod-04-1] h1 {size}");
        // The catalog reads a receipt whole up to 256 KiB: this one is under
        // half of it, and the block in it under its enforced cap.
        assert!(
            b.receipt_bytes.len() < 128 * 1024,
            "the receipt is not well under the catalog's 256 KiB read cap: {size}"
        );
        let block = logweir_core::det_json::to_deterministic_json(b.block()).unwrap();
        assert!(
            block.len() < logweir_core::consumer_positions::MAX_BLOCK_BYTES,
            "the block is over its bound: {} bytes, {size}",
            block.len()
        );
        assert!(
            doc_bytes.len() > 256 * 1024,
            "NEGATIVE CONTROL: the positions are over the cap inline: {size}"
        );
        for id in &ids {
            let g = b.group(id);
            assert_eq!(g.outcome, "captured", "{id}: {g:#?}");
            let c = g.counts.expect("counts");
            assert_eq!(c.total(), total as u64, "{id}: every partition counted");
            assert_eq!(c.never_committed, 0, "{id} committed everywhere");
            assert_eq!(
                u64::from(c.related),
                total as u64,
                "{id}: every position is at the archive's end, so relates"
            );
            assert_eq!(doc.groups[id].positions.len(), total, "{id}");
        }
        let verify = verify_both(&b);
        let summary = catalog_summary(&b);
        assert_eq!(
            summary["groups"].as_array().map(Vec::len),
            Some(groups),
            "the catalog point summarises every group"
        );
        measured.push(
            json!({"size": size, "verify_exit": [verify["rust_exit"], verify["python_exit"]],
                             "tampered": verify["tampered"]}),
        );
    }
    write_evidence(
        &broker,
        "a_large_selection_keeps_the_receipt_small_and_its_positions_verified",
        &json!({"measured": measured}),
    );
}
