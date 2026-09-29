#![cfg(feature = "e2e")]
//! **FX-1 — a drill over an archive that holds the engine's REAL, non-empty
//! consumer-groups snapshot.**
//!
//! Upstream `kafka-backup` writes `<backup_id>/consumer-groups-snapshot.json`
//! when a backup runs with `backup.consumer_group_snapshot: true`
//! (`U:crates/kafka-backup-core/src/backup/engine.rs:846-933`). Logweir never
//! enables it, but an archive written by upstream's CLI or by OSO's operators
//! with it on is a supported drill input (`docs/quickstart.md` Path 3). Before
//! FX-1 every such drill exited 1 with no scorecard:
//!
//! ```text
//! engine: operational: <prefix>/<backup_id>/consumer-groups-snapshot.json:
//!   invalid type: map, expected a sequence at line 6 column 17
//! ```
//!
//! because the vendored shape was invented (`offsets` a list) and
//! `describe()` refused the whole archive over it.
//!
//! # What this row does
//!
//! 1. Produces 36 records into its own three-partition topic.
//! 2. Commits three consumer groups (`rdkafka`, explicit offsets) and reads
//!    back what the BROKER says each committed: that is the oracle. One group
//!    commits only on `orders`, a topic this backup does not archive; another
//!    commits on both.
//! 3. Backs the topic up with the digest-pinned engine and the snapshot ON.
//! 4. Reads the snapshot through `OsoCliEngine::consumer_group_snapshot` and
//!    requires EXACTLY the oracle, restricted to the archived topic: the
//!    engine keeps committed offsets `>= 0` on archived topics only.
//! 5. Drills the archive with the shipped binary and requires a signed
//!    `pass`, verified by both readers.
//!
//! # The negative control
//!
//! The row fails on the parser that stood in the vendored file before FX-1:
//! step 5 exits 1 at `describe()` (measured on compose slot 1 with the binary
//! built at `632ea345`), and step 4 cannot parse the object.
//! `crates/logweir-engine-oso/tests/vendored_parse.rs` keeps that old parser
//! as a standing control over the committed fixture.
//!
//! # The second row (FX-1 fix round, M1)
//!
//! Since FX-1 a snapshot that is NOT the engine's shape no longer refuses the
//! archive, and it must not vanish either: `backup run` and `drill run` each
//! print a `warning:` line on stderr naming the backup set, the object, its
//! digest and the reason, and a WARN event with those fields on the
//! structured log. The row plants the shape that stood in the vendored file
//! before FX-1 beside a real archive and requires both, from the shipped
//! binary, while the backup and the drill still succeed and nothing they sign
//! mentions the snapshot (signed surfacing belongs to PROD-04.1).
//!
//! # Hygiene
//!
//! Every archive, receipt, topic and group a row makes is named with that
//! row's own prefix (`cgsnap-r-…`, `cgsnap-u-…`), which nothing else uses, and
//! a `Drop` guard sweeps them on every exit path, as `pitr_boundary.rs` does
//! for its archive. The two rows hold one lock, so they never run at once even
//! without `--test-threads=1`. Catalog records are left, as in
//! `pitr_boundary.rs`. Each subprocess this file starts is bounded.
mod harness;
use harness::*;

use logweir_core::engine::{DataEngine, StorageUrl};
use logweir_engine_oso::engine::{ConsumerGroupSnapshotRead, OsoCliEngine};
use logweir_engine_oso::vendored::consumer_groups::Position;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{BaseConsumer, CommitMode, Consumer};
use rdkafka::{Offset, TopicPartitionList};
use std::collections::BTreeMap;
use std::path::Path;
use std::process::{Command, Output, Stdio};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

/// Every archive prefix, receipt, topic and group this file creates starts
/// with this, and nothing else in the tree does, so the sweep is exact.
const ID_PREFIX: &str = "cgsnap-";
/// The first row's names: a real snapshot.
const REAL: &str = "cgsnap-r-";
/// The second row's names: an unreadable one.
const UNREADABLE: &str = "cgsnap-u-";

/// The two rows share one stack and one bucket, and each sweeps its own
/// prefix at its start: they must never run at once.
fn serial() -> std::sync::MutexGuard<'static, ()> {
    static LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());
    LOCK.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
}
const PARTITIONS: i32 = 3;
const RECORDS: usize = 36;
/// A topic the stack always has and this row's backup never archives.
const UNARCHIVED: &str = "orders";

fn minio_env() {
    std::env::set_var("AWS_ACCESS_KEY_ID", "minioadmin");
    std::env::set_var("AWS_SECRET_ACCESS_KEY", "minioadmin");
    std::env::set_var("AWS_REGION", "us-east-1");
    std::env::set_var("AWS_EC2_METADATA_DISABLED", "true");
}

// ---------------------------------------------------------------------------
// Bounded subprocesses.
// ---------------------------------------------------------------------------

/// Runs `cmd`, killing it after `secs` and then calling `on_timeout`.
fn run_bounded(cmd: Command, secs: u64, what: &str, on_timeout: &dyn Fn()) -> Output {
    run_bounded_with_input(cmd, None, secs, what, on_timeout)
}

/// `run_bounded`, feeding `input` to the child's stdin when there is one.
fn run_bounded_with_input(
    mut cmd: Command,
    input: Option<Vec<u8>>,
    secs: u64,
    what: &str,
    on_timeout: &dyn Fn(),
) -> Output {
    use std::io::{Read, Write};
    if input.is_some() {
        cmd.stdin(Stdio::piped());
    }
    let mut child = cmd
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap_or_else(|e| panic!("{what}: could not start: {e}"));
    if let Some(bytes) = input {
        let mut stdin = child.stdin.take().expect("piped stdin");
        std::thread::spawn(move || {
            let _ = stdin.write_all(&bytes);
        });
    }
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
        if let Some(s) = child.try_wait().expect("the child is waitable") {
            break s;
        }
        if Instant::now() >= deadline {
            let _ = child.kill();
            let _ = child.wait();
            on_timeout();
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

fn docker(args: &[&str], what: &str) -> Output {
    let mut c = Command::new("docker");
    c.args(args);
    run_bounded(c, 60, what, &|| {})
}

/// `docker compose … <verb> …` against THIS stack, bounded.
fn compose(args: &[&str], secs: u64, what: &str) -> Output {
    stack::ensure_coherent();
    let mut c = Command::new("docker");
    c.args(["compose", "-f", "e2e/compose/docker-compose.yml"])
        .args(args)
        .current_dir(root());
    run_bounded(c, secs, what, &|| {})
}

/// A Kafka tool inside the running broker container.
fn broker_tool(tool: &str, args: &[&str], what: &str) -> Output {
    let bin = format!("/opt/kafka/bin/{tool}");
    let mut all = vec!["exec", "-T", "kafka-broker-1", bin.as_str()];
    all.extend_from_slice(args);
    compose(&all, 120, what)
}

fn mc_bounded(args: &[&str], what: &str) -> Output {
    let mut all = vec!["run", "--rm", "-T", "--entrypoint", "mc", "minio-setup"];
    all.extend_from_slice(args);
    compose(&all, 120, what)
}

/// Writes `bytes` to the object `local/<bucket>/<key>` with `mc pipe`.
fn mc_put(target: &str, bytes: &[u8]) {
    stack::ensure_coherent();
    let mut c = Command::new("docker");
    c.args(["compose", "-f", "e2e/compose/docker-compose.yml"])
        .args(["run", "--rm", "-T", "--entrypoint", "mc", "minio-setup"])
        .args(["pipe", target])
        .current_dir(root());
    let o = run_bounded_with_input(c, Some(bytes.to_vec()), 120, "mc pipe", &|| {});
    assert!(
        o.status.success(),
        "mc pipe {target} exited {:?}\n{}\n{}",
        o.status.code(),
        o.stdout_utf8(),
        o.stderr_utf8()
    );
}

/// Running containers whose arguments name `cfg` (the engine's docker route
/// passes the config path through, and each config path here is unique).
fn kill_engine_containers(cfg: &Path) {
    let ids: Vec<String> = docker(&["ps", "-q", "--no-trunc"], "docker ps")
        .stdout_utf8()
        .split_whitespace()
        .map(String::from)
        .collect();
    if ids.is_empty() {
        return;
    }
    let mut args = vec!["inspect", "-f", "{{.Id}} {{json .Args}}"];
    args.extend(ids.iter().map(String::as_str));
    let needle = cfg.display().to_string();
    for line in docker(&args, "docker inspect").stdout_utf8().lines() {
        if line.contains(&needle) {
            if let Some(id) = line.split_whitespace().next() {
                let _ = docker(&["kill", id], "docker kill");
            }
        }
    }
}

// ---------------------------------------------------------------------------
// The sweep.
// ---------------------------------------------------------------------------

/// Removes every archive prefix, receipt directory, source topic and consumer
/// group whose name starts with `prefix` (one row's own), and returns what
/// survived. The scratch `drill-` topics are the drill's own (`teardown:
/// delete`) and `harness::run_with` empties them first anyway.
fn sweep(prefix: &str) -> Vec<String> {
    assert!(prefix.starts_with(ID_PREFIX), "{prefix}");
    let mut left = Vec::new();

    let keys = |o: Output| -> Vec<String> {
        o.stdout_utf8()
            .lines()
            .filter_map(|l| serde_json::from_str::<serde_json::Value>(l).ok())
            .filter_map(|v| v["key"].as_str().map(str::to_string))
            .filter(|k| k.starts_with(prefix))
            .collect()
    };
    // The archives, at the bucket's root, and the receipts `backup run`
    // writes under `logweir/backups/<backup_id>/`.
    for dir in ["", "logweir/backups/"] {
        let list_arg = format!("local/{ARCHIVE_BUCKET}/{dir}");
        let listing = || mc_bounded(&["--json", "ls", &list_arg], "mc ls");
        for k in keys(listing()) {
            let name = k.split('/').next().unwrap_or(&k).trim_end_matches('/');
            let _ = mc_bounded(
                &[
                    "rm",
                    "--recursive",
                    "--force",
                    &format!("local/{ARCHIVE_BUCKET}/{dir}{name}/"),
                ],
                "mc rm",
            );
        }
        left.extend(
            keys(listing())
                .into_iter()
                .map(|k| format!("object {dir}{k}")),
        );
    }

    let topics = broker_tool(
        "kafka-topics.sh",
        &["--bootstrap-server", "kafka-broker-1:9094", "--list"],
        "list topics",
    )
    .stdout_utf8();
    for t in topics
        .lines()
        .map(str::trim)
        .filter(|t| t.starts_with(prefix))
    {
        let _ = broker_tool(
            "kafka-topics.sh",
            &[
                "--bootstrap-server",
                "kafka-broker-1:9094",
                "--delete",
                "--topic",
                t,
            ],
            "delete topic",
        );
    }

    let groups = || {
        broker_tool(
            "kafka-consumer-groups.sh",
            &["--bootstrap-server", "kafka-broker-1:9094", "--list"],
            "list groups",
        )
        .stdout_utf8()
        .lines()
        .map(str::trim)
        .filter(|g| g.starts_with(prefix))
        .map(str::to_string)
        .collect::<Vec<_>>()
    };
    for g in groups() {
        let _ = broker_tool(
            "kafka-consumer-groups.sh",
            &[
                "--bootstrap-server",
                "kafka-broker-1:9094",
                "--delete",
                "--group",
                &g,
            ],
            "delete group",
        );
    }
    left.extend(groups().into_iter().map(|g| format!("group {g}")));
    left
}

/// Sweeps on every exit path. On the panicking path it only reports what it
/// could not remove: a second panic while unwinding would abort the process
/// and destroy the row's own failure message.
struct Swept(&'static str);

impl Drop for Swept {
    fn drop(&mut self) {
        let left = sweep(self.0);
        if std::thread::panicking() {
            if !left.is_empty() {
                eprintln!("[cgsnap] SWEEP INCOMPLETE on the panicking path: {left:?}");
            }
        } else {
            assert!(left.is_empty(), "the sweep left {left:?}");
        }
    }
}

// ---------------------------------------------------------------------------
// Consumer groups.
// ---------------------------------------------------------------------------

fn consumer(group: &str) -> BaseConsumer {
    ClientConfig::new()
        .set("bootstrap.servers", bootstrap())
        .set("group.id", group)
        .set("enable.auto.commit", "false")
        .set("socket.timeout.ms", "10000")
        .create()
        .unwrap_or_else(|e| panic!("a consumer for {group} at {}: {e}", bootstrap()))
}

/// Commits `positions` for `group` without joining it (explicit offsets on a
/// manual assignment), the way a tool or a simple consumer commits.
fn commit(group: &str, positions: &[(&str, i32, i64)]) {
    let c = consumer(group);
    let mut tpl = TopicPartitionList::new();
    for (topic, partition, offset) in positions {
        tpl.add_partition_offset(topic, *partition, Offset::Offset(*offset))
            .expect("a partition offset");
    }
    c.assign(&tpl).expect("assign");
    c.commit(&tpl, CommitMode::Sync)
        .unwrap_or_else(|e| panic!("commit for {group}: {e}"));
}

/// What the BROKER says `group` has committed on every partition of `topics`
/// (a position with no committed offset is absent).
fn committed(group: &str, topics: &[(&str, i32)]) -> Vec<Position> {
    let c = consumer(group);
    let mut tpl = TopicPartitionList::new();
    for (topic, partitions) in topics {
        for p in 0..*partitions {
            tpl.add_partition(topic, p);
        }
    }
    let got = c
        .committed_offsets(tpl, Duration::from_secs(20))
        .unwrap_or_else(|e| panic!("committed offsets of {group}: {e}"));
    let mut out: Vec<Position> = got
        .elements()
        .iter()
        .filter_map(|e| match e.offset() {
            Offset::Offset(o) => Some(Position {
                topic: e.topic().to_string(),
                partition: e.partition(),
                offset: o,
            }),
            _ => None,
        })
        .collect();
    out.sort();
    out
}

// ---------------------------------------------------------------------------
// The row.
// ---------------------------------------------------------------------------

fn engine_config(backup_id: &str, topic: &str) -> String {
    format!(
        "mode: backup\n\
         backup_id: \"{backup_id}\"\n\
         source:\n\
         \x20 bootstrap_servers:\n\
         \x20   - {bootstrap}\n\
         \x20 topics:\n\
         \x20   include:\n\
         \x20     - \"{topic}\"\n\
         storage:\n\
         \x20 backend: s3\n\
         \x20 bucket: {ARCHIVE_BUCKET}\n\
         \x20 region: us-east-1\n\
         \x20 prefix: {backup_id}\n\
         \x20 endpoint: {s3}\n\
         \x20 path_style: true\n\
         \x20 allow_http: true\n\
         backup:\n\
         \x20 compression: zstd\n\
         \x20 continuous: false\n\
         \x20 segment_max_records: 1000\n\
         \x20 consumer_group_snapshot: true\n",
        bootstrap = bootstrap(),
        s3 = s3_endpoint(),
    )
}

fn drill_spec(backup_id: &str, topic: &str, from_ms: i64, to_ms: i64) -> serde_yaml::Value {
    let rfc = |ms: i64| {
        chrono::DateTime::from_timestamp_millis(ms)
            .expect("a representable instant")
            .to_rfc3339_opts(chrono::SecondsFormat::Millis, true)
    };
    let (bootstrap, s3) = (bootstrap(), s3_endpoint());
    serde_yaml::from_str(&format!(
        "source:\n\
         \x20 storage:\n\
         \x20   backend: s3\n\
         \x20   bucket: {ARCHIVE_BUCKET}\n\
         \x20   prefix: {backup_id}\n\
         \x20   region: us-east-1\n\
         \x20   endpoint: {s3}\n\
         \x20   path_style: true\n\
         \x20   allow_http: true\n\
         \x20 backup: {backup_id}\n\
         \x20 topics: [{topic}]\n\
         target:\n\
         \x20 bootstrap_servers: [{bootstrap}]\n\
         \x20 marker_topic: {MARKER_TOPIC}\n\
         \x20 topic_mapping_prefix: \"{SCRATCH_PREFIX}\"\n\
         \x20 default_replication_factor: 1\n\
         \x20 teardown: delete\n\
         sample:\n\
         \x20 window_start: \"{start}\"\n\
         \x20 window_end: \"{end}\"\n\
         \x20 records_per_partition: 25\n\
         \x20 anchor: head\n\
         objectives:\n\
         \x20 rto_seconds: 900\n\
         \x20 rpo_seconds: 3600\n\
         \x20 pass_rate: 1.0\n\
         evidence:\n\
         \x20 backend: s3\n\
         \x20 bucket: {EVIDENCE_BUCKET}\n\
         \x20 prefix: logweir/\n\
         \x20 region: us-east-1\n\
         \x20 endpoint: {s3}\n\
         \x20 path_style: true\n\
         \x20 allow_http: true\n",
        start = rfc(from_ms - 1000),
        end = rfc(to_ms + 1000),
    ))
    .expect("the drill spec is valid YAML")
}

/// **FX-1.** The engine's real snapshot reads back as exactly what the groups
/// committed, and a drill over the archive that holds it passes, signed and
/// verified by both readers. `rpo_seconds` is 3600 rather than the demo's 300
/// because this row is about the snapshot: the gap between its own records and
/// the drill includes the engine's backup, and a slow host must not turn that
/// into a red that says nothing about FX-1.
#[test]
fn a_drill_over_an_archive_with_a_real_consumer_group_snapshot_passes() {
    let _serial = serial();
    minio_env();
    let left = sweep(REAL);
    assert!(left.is_empty(), "an earlier run left {left:?}");
    let _swept = Swept(REAL);

    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("a clock after 1970")
        .as_millis();
    let backup_id = format!("{REAL}{nonce}");
    let topic = format!("{REAL}src-{nonce}");
    let group = |s: &str| format!("{REAL}{nonce}-{s}");

    // ------------------------------------------------ 1. the source records
    create_topic(&topic, PARTITIONS);
    // The one clock read that reaches an assertion: the records' own
    // `CreateTime`, so the archive's window is the last few seconds and the
    // drill's sample window below is bound to it, not to a wall-clock guess.
    let t0 = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("a clock after 1970")
        .as_millis() as i64
        - 2_000;
    let payloads: Vec<String> = (0..RECORDS).map(|i| format!("cgsnap-{i:03}")).collect();
    let records: Vec<(i64, &str)> = payloads
        .iter()
        .enumerate()
        .map(|(i, p)| (t0 + i as i64, p.as_str()))
        .collect();
    produce_with_timestamps(&topic, &records).unwrap_or_else(|e| panic!("{e}"));
    let per_partition = (RECORDS as i64) / i64::from(PARTITIONS);

    // ------------------------------------ 2. the groups, and the oracle
    let (a, b, c) = (group("a"), group("b"), group("c"));
    commit(
        &a,
        &[
            (topic.as_str(), 0, 3),
            (topic.as_str(), 1, 7),
            (topic.as_str(), 2, per_partition),
        ],
    );
    commit(&b, &[(topic.as_str(), 1, 5), (UNARCHIVED, 0, 0)]);
    commit(&c, &[(UNARCHIVED, 1, 0)]);

    let unarchived_partitions = count_partitions(UNARCHIVED);
    let scope = [
        (topic.as_str(), PARTITIONS),
        (UNARCHIVED, unarchived_partitions),
    ];
    let oracle: BTreeMap<String, Vec<Position>> = [&a, &b, &c]
        .into_iter()
        .map(|g| (g.clone(), committed(g, &scope)))
        .collect();
    eprintln!("[cgsnap] the broker's committed positions: {oracle:#?}");
    // The oracle is the broker's, and it must hold what was committed, or the
    // comparison below compares nothing.
    assert_eq!(oracle[&a].len(), 3, "{oracle:?}");
    assert_eq!(oracle[&b].len(), 2, "{oracle:?}");
    assert_eq!(oracle[&c].len(), 1, "{oracle:?}");
    let expected: BTreeMap<String, Vec<Position>> = oracle
        .iter()
        .map(|(g, ps)| {
            let kept: Vec<Position> = ps
                .iter()
                .filter(|p| p.topic == topic && p.offset >= 0)
                .cloned()
                .collect();
            (g.clone(), kept)
        })
        .filter(|(_, ps)| !ps.is_empty())
        .collect();
    assert_eq!(
        expected.keys().collect::<Vec<_>>(),
        vec![&a, &b],
        "group c commits only on {UNARCHIVED}, which this backup does not archive"
    );

    // ------------------------------- 3. the pinned engine, snapshot ON
    let cfg = engine_mount().join(format!("{backup_id}.yaml"));
    std::fs::write(&cfg, engine_config(&backup_id, &topic)).expect("the engine config");
    let before_backup_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;
    let mut cmd = Command::new(engine_bin());
    cmd.env("LOGWEIR_E2E_ENGINE_MOUNT", engine_mount())
        .env("RUST_LOG", "info")
        .arg("backup")
        .arg("--config")
        .arg(&cfg)
        .current_dir(root());
    let o = run_bounded(cmd, 600, "engine backup", &|| kill_engine_containers(&cfg));
    let after_backup_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;
    let _ = std::fs::remove_file(&cfg);
    assert!(
        o.status.success(),
        "engine backup exited {:?}\n{}\n{}",
        o.status.code(),
        o.stdout_utf8(),
        o.stderr_utf8()
    );

    // ------------------------ 4. the snapshot, read the way Logweir reads it
    let loc = StorageUrl::S3 {
        bucket: ARCHIVE_BUCKET.to_string(),
        prefix: backup_id.clone(),
        region: Some("us-east-1".to_string()),
        endpoint: Some(s3_endpoint()),
        path_style: true,
        allow_http: true,
    };
    let engine = OsoCliEngine::new(
        engine_bin(),
        engine_version(),
        engine_digest(),
        engine_mount(),
        logweir_engine_oso::storage::Store::read_only_from_url(&loc)
            .unwrap_or_else(|e| panic!("a read-only handle over {backup_id}: {e}")),
    );
    let set = engine
        .list_backup_sets(&loc)
        .unwrap_or_else(|e| panic!("list_backup_sets: {e}"))
        .into_iter()
        .find(|s| s.backup_id == backup_id)
        .unwrap_or_else(|| panic!("the archive holds no backup set {backup_id}"));
    let facts = engine
        .describe(&set)
        .unwrap_or_else(|e| panic!("describe() refused an archive with a real snapshot: {e}"));
    let (sha256, snapshot) = match engine
        .consumer_group_snapshot(&set)
        .unwrap_or_else(|e| panic!("reading the snapshot: {e}"))
    {
        ConsumerGroupSnapshotRead::Parsed {
            sha256, snapshot, ..
        } => (sha256, snapshot),
        other => panic!("the engine's own snapshot must parse, got {other:?}"),
    };
    assert_eq!(
        facts.consumer_group_snapshot_sha256.as_deref(),
        Some(sha256.as_str()),
        "describe() publishes the digest of the same object"
    );
    let taken = snapshot
        .snapshot_time
        .expect("the engine writes snapshot_time");
    assert!(
        (before_backup_ms - 60_000..=after_backup_ms + 60_000).contains(&taken),
        "snapshot_time {taken} is not the backup's own clock ({before_backup_ms}..{after_backup_ms})"
    );
    let got: BTreeMap<String, Vec<Position>> = snapshot
        .groups
        .iter()
        .map(|g| (g.group_id.clone(), g.positions().expect("positions")))
        .collect();
    assert_eq!(
        got, expected,
        "the snapshot must hold exactly what the broker says the groups committed on the \
         archived topic"
    );

    // ------------------------------------------------ 5. the drill itself
    let spec = drill_spec(&backup_id, &topic, t0, t0 + RECORDS as i64);
    let r = drill_run(&spec);
    assert_eq!(
        r.out.status.code(),
        Some(0),
        "a drill over an archive with a real consumer-groups snapshot must pass (before FX-1 \
         it exited 1 at describe())\nstdout:\n{}\nstderr:\n{}",
        r.out.stdout_utf8(),
        r.out.stderr_utf8()
    );
    let sc = read_scorecard(&r);
    assert_eq!(sc["outcome"].as_str(), Some("pass"), "{}", sc["measured"]);
    assert_eq!(
        sc["integrity"]["result"].as_str(),
        Some("pass"),
        "{}",
        sc["integrity"]
    );
    assert_eq!(
        sc["integrity"]["mismatches"].as_u64(),
        Some(0),
        "{}",
        sc["integrity"]
    );
    assert_eq!(sc["source"]["backup_id"].as_str(), Some(backup_id.as_str()));
    assert!(logweir_verify(&r).success(), "logweir drill verify");
    assert!(python_verify(&r).success(), "docs/verify_scorecard.py");
}

// ---------------------------------------------------------------------------
// The second row: an unreadable snapshot is TOLD (FX-1 fix round, M1).
// ---------------------------------------------------------------------------

/// The `notice` field and the `[kind]` of the warning line.
const NOTICE_KIND: &str = "consumer-groups-snapshot-unreadable";

/// `logweir backup run`'s spec for this row's topic, under its own
/// `backup_id` and prefix, on this stack's host-side addresses.
fn backup_spec(backup_id: &str, topic: &str) -> std::path::PathBuf {
    let p = demo_dir().join(format!("{backup_id}.backup.yaml"));
    std::fs::write(
        &p,
        format!(
            "backup_id: {backup_id}\n\
             source:\n\
             \x20 bootstrap_servers: [{bootstrap}]\n\
             \x20 topics: [{topic}]\n\
             storage:\n\
             \x20 backend: s3\n\
             \x20 bucket: {ARCHIVE_BUCKET}\n\
             \x20 prefix: {backup_id}\n\
             \x20 region: us-east-1\n\
             \x20 endpoint: {s3}\n\
             \x20 path_style: true\n\
             \x20 allow_http: true\n\
             backup:\n\
             \x20 compression: zstd\n\
             \x20 segment_max_records: 1000\n\
             \x20 segment_max_bytes: 10485760\n\
             \x20 max_concurrent_partitions: 3\n",
            bootstrap = bootstrap(),
            s3 = s3_endpoint(),
        ),
    )
    .expect("the backup spec");
    p
}

/// `logweir backup run` with the digest-pinned engine, as `pitr_boundary.rs`
/// runs it. The allowlist names no live cluster: a backup's SOURCE must not be
/// a permitted scratch target (GC18(c) rail 4).
fn backup_run(spec: &Path, receipt: &Path) -> Output {
    let allow = demo_dir().join("cgsnap-backup-allowed-clusters.json");
    std::fs::write(
        &allow,
        "{\"allowed_cluster_ids\": [\"SCRATCH-CLUSTER-NOT-THE-SOURCE\"]}\n",
    )
    .expect("the backup allowlist");
    let mut c = Command::new(bin());
    c.args(["backup", "run", "--spec"])
        .arg(spec)
        .arg("--allowed-clusters")
        .arg(&allow)
        .arg("--signing-key")
        .arg(root().join("e2e/fixtures/signed/signing.pem"))
        .arg("--triggered-by")
        .arg("fx-1 e2e")
        .arg("--receipt-out")
        .arg(receipt)
        .env("AWS_ACCESS_KEY_ID", "minioadmin")
        .env("AWS_SECRET_ACCESS_KEY", "minioadmin")
        .env("AWS_REGION", "us-east-1")
        .env("LOGWEIR_ENGINE_BIN", engine_bin())
        .env("LOGWEIR_ENGINE_VERSION", engine_version())
        .env("LOGWEIR_ENGINE_DIGEST", engine_digest())
        .env("LOGWEIR_E2E_ENGINE_MOUNT", engine_mount())
        .env("TMPDIR", engine_mount());
    run_bounded(c, 900, "logweir backup run", &|| {})
}

/// `o` told the unreadable snapshot at `key` of `backup_id`: one `warning:`
/// line on stderr and one WARN event with the notice's fields, in the run's
/// span, on the structured log (stdout).
fn assert_told(o: &Output, backup_id: &str, key: &str, what: &str) {
    let stderr = o.stderr_utf8();
    let line = stderr
        .lines()
        .find(|l| l.starts_with("warning: ") && l.contains(key))
        .unwrap_or_else(|| panic!("{what}: no warning line names {key} on stderr:\n{stderr}"));
    for needle in [
        format!("warning: backup set {backup_id}: "),
        "the consumer-groups snapshot is present but unreadable".to_string(),
        "not the consumer-groups snapshot shape kafka-backup writes".to_string(),
        format!("[{NOTICE_KIND}]"),
    ] {
        assert!(
            line.contains(&needle),
            "{what}: `{needle}` missing from: {line}"
        );
    }
    let stdout = o.stdout_utf8();
    let event = stdout
        .lines()
        .filter_map(|l| serde_json::from_str::<serde_json::Value>(l).ok())
        .find(|v| v["level"] == "WARN" && v["fields"]["notice"] == NOTICE_KIND)
        .unwrap_or_else(|| panic!("{what}: no WARN event told the notice on stdout:\n{stdout}"));
    assert_eq!(event["fields"]["key"], key, "{what}: {event}");
    assert_eq!(event["fields"]["backup_id"], backup_id, "{what}: {event}");
    assert!(
        event["fields"]["reason"]
            .as_str()
            .is_some_and(|r| r.contains("expected a map")),
        "{what}: {event}"
    );
    assert!(
        event["span"]["run_id"]
            .as_str()
            .is_some_and(|id| !id.is_empty()),
        "{what}: the event must carry the run's id: {event}"
    );
    eprintln!("[cgsnap] {what} told it: {line}");
    eprintln!("[cgsnap] {what} logged: {event}");
}

/// **FX-1 fix round (M1), live.** An archive whose consumer-groups snapshot
/// is NOT the engine's shape — the one that stood in the vendored file before
/// FX-1 — is backed up and then drilled by the shipped binary. Both succeed,
/// both TELL the snapshot (a stderr line and a structured event), and neither
/// signed document mentions it. At `978450d6`, before the fix round, both ran
/// green and said nothing: the row fails there.
#[test]
fn an_unreadable_snapshot_is_told_by_backup_run_and_drill_run() {
    let _serial = serial();
    minio_env();
    let left = sweep(UNREADABLE);
    assert!(left.is_empty(), "an earlier run left {left:?}");
    let _swept = Swept(UNREADABLE);

    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("a clock after 1970")
        .as_millis();
    let backup_id = format!("{UNREADABLE}{nonce}");
    let topic = format!("{UNREADABLE}src-{nonce}");
    create_topic(&topic, PARTITIONS);
    let t0 = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("a clock after 1970")
        .as_millis() as i64
        - 2_000;
    let payloads: Vec<String> = (0..RECORDS).map(|i| format!("cgsnap-u-{i:03}")).collect();
    let records: Vec<(i64, &str)> = payloads
        .iter()
        .enumerate()
        .map(|(i, p)| (t0 + i as i64, p.as_str()))
        .collect();
    produce_with_timestamps(&topic, &records).unwrap_or_else(|e| panic!("{e}"));

    // The object where the engine keeps it, in a shape no engine writes.
    let key = format!("{backup_id}/{backup_id}/consumer-groups-snapshot.json");
    let invented = format!(
        "{{\"backup_id\":\"{backup_id}\",\"captured_at\":1,\
         \"groups\":[{{\"group_id\":\"g\",\"state\":\"Stable\",\"offsets\":[]}}]}}"
    );
    mc_put(
        &format!("local/{ARCHIVE_BUCKET}/{key}"),
        invented.as_bytes(),
    );

    // ------------------------------------------------ backup run tells it
    let receipt = demo_dir().join(format!("{backup_id}.receipt.json"));
    let o = backup_run(&backup_spec(&backup_id, &topic), &receipt);
    assert_eq!(
        o.status.code(),
        Some(0),
        "logweir backup run over a set holding an unreadable snapshot must succeed\n\
         stdout:\n{}\nstderr:\n{}",
        o.stdout_utf8(),
        o.stderr_utf8()
    );
    assert_told(&o, &backup_id, &key, "backup run");
    let receipt_text = std::fs::read_to_string(&receipt).expect("the receipt was written");
    assert!(
        !receipt_text.contains("consumer-groups"),
        "the receipt signs nothing about the snapshot: {receipt_text}"
    );

    // ------------------------------------------------- drill run tells it
    let spec = drill_spec(&backup_id, &topic, t0, t0 + RECORDS as i64);
    let r = drill_run(&spec);
    assert_eq!(
        r.out.status.code(),
        Some(0),
        "a drill over an archive with an unreadable snapshot must pass\nstdout:\n{}\nstderr:\n{}",
        r.out.stdout_utf8(),
        r.out.stderr_utf8()
    );
    assert_told(&r.out, &backup_id, &key, "drill run");
    let sc = read_scorecard(&r);
    assert_eq!(sc["outcome"].as_str(), Some("pass"), "{}", sc["measured"]);
    assert!(
        !sc.to_string().contains("consumer-groups"),
        "the scorecard signs nothing about the snapshot"
    );
    assert!(logweir_verify(&r).success(), "logweir drill verify");
    assert!(python_verify(&r).success(), "docs/verify_scorecard.py");
}
