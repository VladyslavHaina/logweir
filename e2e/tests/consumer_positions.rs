#![cfg(feature = "e2e")]
//! **PROD-04.0a — committed positions through the safe consumer API,
//! against a real broker.** `RdKafkaReader::committed_positions` and
//! `commit_positions` (PROD-04.0 §4.2, §4.3, §5), each row checked against
//! the broker's OWN answer (`kafka-consumer-groups.sh`, run inside the broker
//! container), never against the API under test alone.
//!
//! | row | proves | negative control |
//! |---|---|---|
//! | `a_classic_empty_group_reads_and_takes_positions` | an Empty classic group's positions equal the CLI's; a committed 0 is 0 and a never-committed partition is `NoCommittedPosition`; a non-member commit lands with epoch −1 and Logweir's marker and the group stays Empty | mapping "no commit" to 0 shows 0 where the CLI has no row |
//! | `a_live_classic_member_makes_the_commit_refused_for_the_whole_group` | a Stable classic group refuses the commit `GroupActive`, readback unchanged; once the member leaves, the same commit applies | the control itself: a row that passed while the member lived would have applied nothing |
//! | `a_consumer_protocol_group_refuses_while_live_and_takes_positions_when_empty` | the same for a KIP-848 group (4.x lines; skipped, saying so, below 4.0) | as above |
//! | `an_absent_group_answers_at_once_with_no_position_and_a_commit_creates_it` | an absent id answers within a fraction of the bound with `NoCommittedPosition` everywhere, is not created by the read, and IS created (simple, Empty) by a commit (K3) | an absent group read as a timeout or as offset 0 fails |
//! | `a_pending_transactional_offset_makes_the_group_unstable_never_the_old_position` | while a TxnOffsetCommit is pending the bounded fetch is `PositionsUnstable` for the group, never the pre-transaction position, and returns after its bound and within [`OVER_BOUND`] of it; after the abort the old position reads | a `read_uncommitted` fetch (no RequireStable) returns the stale position meanwhile: the protection is the API's |
//! | `a_group_hidden_from_the_principal_is_never_absent` (`#[ignore]`, profile `acl`) | a group whose only ACL names another principal is `NotVisibleToPrincipal` (not listed) or `NotAuthorized` (listed), never absent: the coordinator lookup's refusal is read off the handle's queue; its commit is refused `NotAuthorized` and nothing changes; every timed-out call returns within its bound plus [`OVER_BOUND`]; a topic the principal may not Describe is `TopicNotAuthorized` per partition | the same principal on a visible group reads its position; granting Read and Describe makes the same commit apply |
//!
//! # Running them
//!
//! ```text
//! eval "$(e2e/compose/stack-env.sh --slot N --kafka 4.3 --profiles acl)"
//! just e2e-up
//! cargo test -p e2e --features e2e --test consumer_positions -- --include-ignored --test-threads=1 --nocapture
//! just e2e-down
//! ```
//!
//! Every row writes what it observed to `consumer-positions/<row>.json` under
//! the stack's scratch directory (`harness::demo_dir()`).
//!
//! # Hygiene
//!
//! Every topic, group and transactional id a row makes carries a fresh nonce
//! (`cpos-…`), so rows never collide and nothing is shared with other files;
//! the ACLs the hidden-group row adds are removed on every exit path. Every
//! subprocess is bounded ([`output_within`]), and every member a row starts is
//! dropped (closed) before the row returns.
mod harness;

use harness::{demo_dir, root};
use logweir_kafka::positions::{
    CommitError, CommittedPosition, GroupListing, PartitionFailure, PartitionPosition,
    PositionsError, TopicPartition, COMMIT_METADATA_MARKER,
};
use logweir_kafka::rdkafka_reader::RdKafkaReader;
use logweir_kafka::reader::{AuthConfig, ClusterReader};
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{BaseConsumer, CommitMode, Consumer};
use rdkafka::producer::{BaseProducer, Producer};
use rdkafka::{Offset, TopicPartitionList};
use serde_json::{json, Value};
use std::io::Read;
use std::process::{Command, Output, Stdio};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

/// The bound every row's reader uses: short enough to keep the suite quick,
/// and well above the time any answered call takes (the absent row measures
/// that).
const BOUND: Duration = Duration::from_secs(6);

/// What a timed-out call may take beyond [`BOUND`] through `RdKafkaReader`: a
/// fresh handle, the call, and the handle's teardown. Measured on this host
/// against the 6 s bound, on 4.3.1, 3.9.2 and 3.7.1 (PROD-04.0a runs and the
/// review's slot-4 run): timed-out fetches 6101–6108 ms, refused commits
/// 6113–6134 ms, so at most 134 ms over. 3 s is more than twenty times that,
/// room for a loaded CI runner, and far below what a dropped bound costs (the
/// review's mutant waited 20 s; with no deadline the call never returns).
const OVER_BOUND: Duration = Duration::from_secs(3);

/// **A timed-out call waited its bound and no longer** (review M1): a call
/// that returned early did not wait for an answer, and one that returned late
/// did not apply the bound it reports.
fn assert_timed_out_within(what: &str, took: Duration, budget: Duration) {
    assert!(
        took >= BOUND - Duration::from_millis(500),
        "{what}: waited out its {BOUND:?} bound ({took:?})"
    );
    assert!(
        took < budget,
        "{what}: returned within {budget:?} ({took:?}); the bound was not applied"
    );
}

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

// ============================================================ the stack

/// One Kafka cluster of the stack: how a host-side client and the broker's
/// own CLIs reach it.
#[derive(Clone)]
struct Cluster {
    /// The compose service whose container runs the CLIs.
    service: &'static str,
    /// `--profile` the service belongs to, if any.
    profile: Option<&'static str>,
    /// PLAINTEXT inside `kafka-net`, as the CLIs dial it.
    in_network: &'static str,
    /// The host-side PLAINTEXT bootstrap.
    plaintext: String,
}

/// The default broker, `kafka-broker-1`.
fn default_cluster() -> Cluster {
    harness::stack::ensure_coherent();
    Cluster {
        service: "kafka-broker-1",
        profile: None,
        in_network: "kafka-broker-1:9094",
        plaintext: harness::bootstrap(),
    }
}

/// Profile `acl`'s broker: PLAINTEXT is User:ANONYMOUS, a super user.
fn acl_cluster() -> Cluster {
    harness::stack::ensure_coherent();
    Cluster {
        service: "kafka-acl",
        profile: Some("acl"),
        in_network: "kafka-acl:9094",
        plaintext: harness::bootstrap_acl(),
    }
}

impl Cluster {
    /// One of the broker's own CLIs inside its RUNNING container. `exec`,
    /// never `run`; bounded.
    fn cli(&self, args: &[&str]) -> Output {
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
        c.args(["exec", "-T", self.service])
            .args(args)
            .current_dir(root());
        output_within(c, 120)
    }

    fn cli_ok(&self, args: &[&str], what: &str) -> String {
        let o = self.cli(args);
        assert!(o.status.success(), "{what} failed:\n{}", text(&o));
        String::from_utf8_lossy(&o.stdout).into_owned()
    }

    /// The broker's version, from its own `kafka-topics.sh --version`.
    fn version(&self) -> String {
        self.cli_ok(
            &["/opt/kafka/bin/kafka-topics.sh", "--version"],
            "kafka-topics --version",
        )
        .split_whitespace()
        .next()
        .unwrap_or("unknown")
        .to_string()
    }

    fn create_topic(&self, topic: &str, partitions: i32) {
        self.cli_ok(
            &[
                "/opt/kafka/bin/kafka-topics.sh",
                "--bootstrap-server",
                self.in_network,
                "--create",
                "--topic",
                topic,
                "--partitions",
                &partitions.to_string(),
                "--replication-factor",
                "1",
            ],
            &format!("create topic {topic}"),
        );
        let r = self.reader();
        for _ in 0..60 {
            if r.list_topics()
                .map(|ts| ts.iter().any(|t| t.name == topic && t.error.is_none()))
                .unwrap_or(false)
            {
                return;
            }
            std::thread::sleep(Duration::from_millis(250));
        }
        panic!("topic {topic} still absent 15 s after --create");
    }

    /// A super-user reader (the default broker has no authorizer; on `acl`
    /// PLAINTEXT is User:ANONYMOUS, a super user).
    fn reader(&self) -> RdKafkaReader {
        RdKafkaReader::connect(std::slice::from_ref(&self.plaintext), AuthConfig::Plaintext)
            .expect("connect builds local state")
            .with_position_bound(BOUND)
            .expect("6 s is a valid bound")
    }

    /// What the broker says `group` has committed: `(topic, partition)` →
    /// CURRENT-OFFSET (`None` for the CLI's `-`). A partition with NO row is
    /// absent from the map: that is the CLI's "no committed offset".
    fn cli_offsets(&self, group: &str) -> std::collections::BTreeMap<(String, i32), Option<i64>> {
        let out = self.cli_ok(
            &[
                "/opt/kafka/bin/kafka-consumer-groups.sh",
                "--bootstrap-server",
                self.in_network,
                "--describe",
                "--group",
                group,
            ],
            &format!("kafka-consumer-groups --describe {group}"),
        );
        let mut m = std::collections::BTreeMap::new();
        for line in out.lines() {
            let t: Vec<&str> = line.split_whitespace().collect();
            if t.len() >= 4 && t[0] == group {
                if let Ok(p) = t[2].parse::<i32>() {
                    m.insert((t[1].to_string(), p), t[3].parse::<i64>().ok());
                }
            }
        }
        m
    }

    /// The broker's STATE and #MEMBERS for `group` (`--describe --state`),
    /// or `None` when the CLI does not list it.
    fn cli_state(&self, group: &str) -> Option<(String, u32)> {
        let o = self.cli(&[
            "/opt/kafka/bin/kafka-consumer-groups.sh",
            "--bootstrap-server",
            self.in_network,
            "--describe",
            "--group",
            group,
            "--state",
        ]);
        let out = text(&o);
        out.lines().find_map(|line| {
            let t: Vec<&str> = line.split_whitespace().collect();
            if t.len() >= 3 && t[0] == group {
                let members = t[t.len() - 1].parse::<u32>().ok()?;
                Some((t[t.len() - 2].to_string(), members))
            } else {
                None
            }
        })
    }

    fn cli_lists(&self, group: &str) -> bool {
        self.cli_ok(
            &[
                "/opt/kafka/bin/kafka-consumer-groups.sh",
                "--bootstrap-server",
                self.in_network,
                "--list",
            ],
            "kafka-consumer-groups --list",
        )
        .lines()
        .any(|l| l.trim() == group)
    }

    /// Waits until the CLI reports `group` in `state` with `members` members.
    fn wait_state(&self, group: &str, state: &str, members: u32) -> (String, u32) {
        let mut last = None;
        for _ in 0..60 {
            last = self.cli_state(group);
            if let Some((s, m)) = &last {
                if s == state && *m == members {
                    return (s.clone(), *m);
                }
            }
            std::thread::sleep(Duration::from_millis(500));
        }
        panic!("{group}: still {last:?} 30 s later, wanted {state} with {members} member(s)");
    }

    fn acl(&self, op: &str, args: &[&str]) -> String {
        let mut all = vec![
            "/opt/kafka/bin/kafka-acls.sh",
            "--bootstrap-server",
            self.in_network,
            op,
        ];
        if op == "--remove" {
            all.push("--force");
        }
        all.extend_from_slice(args);
        self.cli_ok(&all, &format!("kafka-acls {op} {args:?}"))
    }
}

// ============================================================ fixtures

fn nonce() -> String {
    let n = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("the clock is after 1970")
        .as_nanos();
    format!("{:010}", n % 10_000_000_000)
}

fn tp(t: &str, p: i32) -> TopicPartition {
    TopicPartition::new(t, p)
}

/// A position to commit; only the offset is sent.
fn at(offset: i64) -> CommittedPosition {
    CommittedPosition {
        offset,
        leader_epoch: None,
        metadata: None,
    }
}

/// A REAL member of `group` (classic or KIP-848), joined and assigned,
/// closed (LeaveGroup) when dropped.
struct Member {
    consumer: BaseConsumer,
    group: String,
}

impl Member {
    fn join(cluster: &Cluster, group: &str, topic: &str, protocol: &str) -> Member {
        let consumer: BaseConsumer = ClientConfig::new()
            .set("bootstrap.servers", &cluster.plaintext)
            .set("group.id", group)
            .set("group.protocol", protocol)
            .set("client.id", "cpos-member")
            .set("enable.auto.commit", "false")
            .set("auto.offset.reset", "earliest")
            .create()
            .expect("member consumer");
        consumer.subscribe(&[topic]).expect("subscribe");
        let deadline = Instant::now() + Duration::from_secs(60);
        loop {
            let _ = consumer.poll(Duration::from_millis(250));
            let assigned = consumer.assignment().map(|a| a.count()).unwrap_or(0);
            if assigned > 0 {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "{group}: the {protocol} member was not assigned a partition within 60 s"
            );
        }
        Member {
            consumer,
            group: group.to_string(),
        }
    }

    /// The member's OWN commit, with its generation: how a real application
    /// leaves positions behind.
    fn commit(&self, offsets: &[(&str, i32, i64)]) {
        let mut tpl = TopicPartitionList::new();
        for (t, p, o) in offsets {
            tpl.add_partition_offset(t, *p, Offset::Offset(*o))
                .expect("tpl");
        }
        self.consumer
            .commit(&tpl, CommitMode::Sync)
            .unwrap_or_else(|e| panic!("{}: the member's own commit: {e}", self.group));
    }
}

fn write_evidence(row: &str, v: &Value) {
    let dir = demo_dir().join("consumer-positions");
    std::fs::create_dir_all(&dir).expect("evidence dir");
    let path = dir.join(format!("{row}.json"));
    std::fs::write(&path, serde_json::to_vec_pretty(v).expect("json")).expect("write evidence");
    eprintln!("evidence: {}", path.display());
}

fn pos_json(p: &PartitionPosition) -> Value {
    match p {
        PartitionPosition::Committed(c) => json!({
            "committed": c.offset, "leaderEpoch": c.leader_epoch, "metadata": c.metadata
        }),
        PartitionPosition::NoCommittedPosition => json!("noCommittedPosition"),
        PartitionPosition::Failed(f) => json!({ "failed": format!("{f:?}") }),
    }
}

fn committed(p: Option<&PartitionPosition>) -> Option<i64> {
    match p {
        Some(PartitionPosition::Committed(c)) => Some(c.offset),
        _ => None,
    }
}

// ============================================================ rows

/// An Empty classic group: positions equal the broker's, a committed 0 is
/// 0, a never-committed partition is `NoCommittedPosition`; a non-member
/// commit lands with Logweir's marker and leaves the group Empty.
#[test]
fn a_classic_empty_group_reads_and_takes_positions() {
    let c = default_cluster();
    let n = nonce();
    let topic = format!("cpos-empty-{n}");
    let group = format!("cpos-empty-g-{n}");
    c.create_topic(&topic, 3);
    {
        let m = Member::join(&c, &group, &topic, "classic");
        m.commit(&[(&topic, 0, 5), (&topic, 1, 0)]);
    }
    c.wait_state(&group, "Empty", 0);

    let r = c.reader();
    let parts = [tp(&topic, 0), tp(&topic, 1), tp(&topic, 2)];
    let before = r
        .committed_positions(&group, &parts, GroupListing::Listed)
        .expect("an Empty classic group's positions read");
    let cli_before = c.cli_offsets(&group);

    // The broker's answer is the oracle.
    assert_eq!(cli_before.get(&(topic.clone(), 0)), Some(&Some(5)));
    assert_eq!(cli_before.get(&(topic.clone(), 1)), Some(&Some(0)));
    assert_eq!(
        cli_before.get(&(topic.clone(), 2)),
        None,
        "the CLI prints no row for a partition the group never committed"
    );
    assert_eq!(committed(before.get(&parts[0])), Some(5));
    assert_eq!(
        committed(before.get(&parts[1])),
        Some(0),
        "a committed 0 is the position 0"
    );
    assert_eq!(
        before.get(&parts[2]),
        Some(&PartitionPosition::NoCommittedPosition),
        "NEGATIVE CONTROL: no commit is never offset 0 (the CLI has no row for it)"
    );
    for (_, p) in &before.partitions {
        if let PartitionPosition::Committed(cp) = p {
            assert_eq!(cp.leader_epoch, None, "the safe route exposes no epoch");
        }
    }

    // A non-member commit to the Empty group: p0 moves, p2 gets its first
    // position, p1 is not named and must not move.
    r.commit_positions(
        &group,
        &[(parts[0].clone(), at(8)), (parts[2].clone(), at(2))],
    )
    .expect("an Empty classic group takes a non-member commit");
    let after = r
        .committed_positions(&group, &parts, GroupListing::Listed)
        .expect("readback");
    let cli_after = c.cli_offsets(&group);
    assert_eq!(cli_after.get(&(topic.clone(), 0)), Some(&Some(8)));
    assert_eq!(cli_after.get(&(topic.clone(), 1)), Some(&Some(0)));
    assert_eq!(cli_after.get(&(topic.clone(), 2)), Some(&Some(2)));
    for (i, want) in [(0usize, 8i64), (1, 0), (2, 2)] {
        assert_eq!(committed(after.get(&parts[i])), Some(want), "p{i}");
    }
    let marker = |i: usize| match after.get(&parts[i]) {
        Some(PartitionPosition::Committed(cp)) => cp.metadata.clone(),
        other => panic!("p{i}: {other:?}"),
    };
    assert_eq!(marker(0).as_deref(), Some(COMMIT_METADATA_MARKER));
    assert_eq!(marker(2).as_deref(), Some(COMMIT_METADATA_MARKER));
    assert_ne!(
        marker(1).as_deref(),
        Some(COMMIT_METADATA_MARKER),
        "the partition Logweir did not commit keeps the application's metadata"
    );
    let state = c.wait_state(&group, "Empty", 0);

    write_evidence(
        "a_classic_empty_group_reads_and_takes_positions",
        &json!({
            "broker": c.version(), "topic": topic, "group": group,
            "before": before.partitions.iter().map(|(t, p)| json!({"tp": t.to_string(), "answer": pos_json(p)})).collect::<Vec<_>>(),
            "cliBefore": format!("{cli_before:?}"),
            "after": after.partitions.iter().map(|(t, p)| json!({"tp": t.to_string(), "answer": pos_json(p)})).collect::<Vec<_>>(),
            "cliAfter": format!("{cli_after:?}"),
            "stateAfter": format!("{state:?}"),
        }),
    );
}

/// The live-member refusal and its control, for one group protocol.
fn live_member_refusal(protocol: &str, row: &str) {
    let c = default_cluster();
    let n = nonce();
    let topic = format!("cpos-live-{protocol}-{n}");
    let group = format!("cpos-live-{protocol}-g-{n}");
    c.create_topic(&topic, 1);
    let r = c.reader();
    let p0 = tp(&topic, 0);

    let member = Member::join(&c, &group, &topic, protocol);
    member.commit(&[(&topic, 0, 4)]);
    let live_state = c.wait_state(&group, "Stable", 1);
    let group_type = if protocol == "consumer" {
        // The group really is a KIP-848 group (else the row proves nothing
        // about one): `kafka-groups.sh --list` prints GROUP TYPE PROTOCOL.
        let list = c.cli_ok(
            &[
                "/opt/kafka/bin/kafka-groups.sh",
                "--bootstrap-server",
                c.in_network,
                "--list",
            ],
            "kafka-groups --list",
        );
        let line = list
            .lines()
            .find(|l| l.split_whitespace().next() == Some(group.as_str()))
            .unwrap_or_else(|| panic!("{group} not in kafka-groups --list:\n{list}"))
            .to_string();
        assert!(
            line.split_whitespace().nth(1) == Some("Consumer"),
            "{group} is not a Consumer-type group: {line}"
        );
        line
    } else {
        String::new()
    };

    let started = Instant::now();
    let refused = r
        .commit_positions(&group, &[(p0.clone(), at(9))])
        .expect_err("a group with a live member refuses a non-member commit");
    let refused_ms = started.elapsed().as_millis() as u64;
    assert_eq!(
        refused,
        CommitError::GroupActive {
            group: group.clone()
        }
    );
    assert!(!refused.may_have_applied());
    let while_live = c.cli_offsets(&group);
    assert_eq!(
        while_live.get(&(topic.clone(), 0)),
        Some(&Some(4)),
        "the refused commit changed nothing"
    );
    assert_eq!(
        committed(
            r.committed_positions(&group, std::slice::from_ref(&p0), GroupListing::Listed)
                .expect("read while live")
                .get(&p0)
        ),
        Some(4)
    );

    // CONTROL: the member leaves; the SAME commit now applies.
    drop(member);
    let empty_state = c.wait_state(&group, "Empty", 0);
    r.commit_positions(&group, &[(p0.clone(), at(9))])
        .expect("the same commit applies once the group is Empty");
    let after = c.cli_offsets(&group);
    assert_eq!(after.get(&(topic.clone(), 0)), Some(&Some(9)));

    write_evidence(
        row,
        &json!({
            "broker": c.version(), "protocol": protocol, "topic": topic, "group": group,
            "groupsList": group_type,
            "liveState": format!("{live_state:?}"), "refused": refused.to_string(), "refusedMs": refused_ms,
            "cliWhileLive": format!("{while_live:?}"),
            "emptyState": format!("{empty_state:?}"), "cliAfterControl": format!("{after:?}"),
        }),
    );
}

#[test]
fn a_live_classic_member_makes_the_commit_refused_for_the_whole_group() {
    live_member_refusal(
        "classic",
        "a_live_classic_member_makes_the_commit_refused_for_the_whole_group",
    );
}

/// KIP-848 groups exist from the 4.0 line on by default (`classic,consumer`
/// rebalance protocols); 3.x answers a `consumer` member UNSUPPORTED_VERSION
/// (PROD-04.0 §3.6), so below 4.0 the row says it was not applicable.
#[test]
fn a_consumer_protocol_group_refuses_while_live_and_takes_positions_when_empty() {
    let version = default_cluster().version();
    // An unreadable version is a failure, never "not applicable": a skip that
    // passes on a parse error would hide the row on every line (review L6).
    let major: u32 = version
        .split('.')
        .next()
        .and_then(|m| m.parse().ok())
        .unwrap_or_else(|| panic!("unparseable broker version {version:?}"));
    if major < 4 {
        eprintln!(
            "SKIPPED (not applicable): broker {version} has no KIP-848 consumer groups by default"
        );
        write_evidence(
            "a_consumer_protocol_group_refuses_while_live_and_takes_positions_when_empty",
            &json!({"broker": version, "skipped": "no KIP-848 groups below 4.0"}),
        );
        return;
    }
    live_member_refusal(
        "consumer",
        "a_consumer_protocol_group_refuses_while_live_and_takes_positions_when_empty",
    );
}

/// An absent id answers at once with no position, is not created by the
/// read, and is created by a commit as a simple Empty classic group (K3).
#[test]
fn an_absent_group_answers_at_once_with_no_position_and_a_commit_creates_it() {
    let c = default_cluster();
    let n = nonce();
    let topic = format!("cpos-absent-{n}");
    let group = format!("cpos-absent-g-{n}");
    c.create_topic(&topic, 2);
    let r = c.reader();
    let parts = [tp(&topic, 0), tp(&topic, 1)];
    assert!(!c.cli_lists(&group), "the id is absent before the row");

    let started = Instant::now();
    let got = r
        .committed_positions(&group, &parts, GroupListing::NotListed)
        .expect("an absent, describable id ANSWERS (K7); it is never a timeout");
    let read_ms = started.elapsed().as_millis() as u64;
    for p in &parts {
        assert_eq!(
            got.get(p),
            Some(&PartitionPosition::NoCommittedPosition),
            "NEGATIVE CONTROL: an absent group is never offset 0"
        );
    }
    assert!(
        started.elapsed() < BOUND / 2,
        "an absent id answers at once ({read_ms} ms), not at the bound"
    );
    assert!(!c.cli_lists(&group), "a read never creates the group");

    r.commit_positions(&group, &[(parts[1].clone(), at(3))])
        .expect("a commit to an absent id creates it");
    assert!(c.cli_lists(&group), "the commit created the group");
    let state = c.wait_state(&group, "Empty", 0);
    let cli = c.cli_offsets(&group);
    assert_eq!(cli.get(&(topic.clone(), 1)), Some(&Some(3)));
    assert_eq!(cli.get(&(topic.clone(), 0)), None);

    write_evidence(
        "an_absent_group_answers_at_once_with_no_position_and_a_commit_creates_it",
        &json!({
            "broker": c.version(), "topic": topic, "group": group, "readMs": read_ms,
            "answer": got.partitions.iter().map(|(t, p)| json!({"tp": t.to_string(), "answer": pos_json(p)})).collect::<Vec<_>>(),
            "stateAfterCommit": format!("{state:?}"), "cliAfterCommit": format!("{cli:?}"),
        }),
    );
}

/// While a transactional offset commit is pending, the bounded RequireStable
/// fetch reports the group unstable and never the pre-transaction position.
#[test]
fn a_pending_transactional_offset_makes_the_group_unstable_never_the_old_position() {
    let c = default_cluster();
    let n = nonce();
    let topic = format!("cpos-txn-{n}");
    let group = format!("cpos-txn-g-{n}");
    c.create_topic(&topic, 1);
    let r = c.reader();
    let p0 = tp(&topic, 0);
    r.commit_positions(&group, &[(p0.clone(), at(3))])
        .expect("the stable position 3");

    // A transactional producer sends offset 7 for the group into an OPEN
    // transaction (TxnOffsetCommit) and holds it.
    let meta_consumer: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", &c.plaintext)
        .set("group.id", &group)
        .create()
        .expect("group-metadata consumer");
    let cgm = meta_consumer.group_metadata().expect("group metadata");
    let producer: BaseProducer = ClientConfig::new()
        .set("bootstrap.servers", &c.plaintext)
        .set("transactional.id", format!("cpos-txn-{n}"))
        .create()
        .expect("transactional producer");
    let t = Duration::from_secs(30);
    producer.init_transactions(t).expect("init_transactions");
    producer.begin_transaction().expect("begin_transaction");
    let mut tpl = TopicPartitionList::new();
    tpl.add_partition_offset(&topic, 0, Offset::Offset(7))
        .expect("tpl");
    producer
        .send_offsets_to_transaction(&tpl, &cgm, t)
        .expect("TxnOffsetCommit for the group");

    // NEGATIVE CONTROL: a reader WITHOUT RequireStable returns the stale 3
    // while 7 is pending. Were this 7 (or an error), the pending state the
    // row needs would not exist and the row would prove nothing.
    let stale: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", &c.plaintext)
        .set("group.id", &group)
        .set("isolation.level", "read_uncommitted")
        .create()
        .expect("read_uncommitted consumer");
    let mut ask = TopicPartitionList::new();
    ask.add_partition(&topic, 0);
    let stale_answer = stale
        .committed_offsets(ask, Duration::from_secs(10))
        .expect("a read_uncommitted fetch answers")
        .find_partition(&topic, 0)
        .map(|e| e.offset());
    assert_eq!(
        stale_answer,
        Some(Offset::Offset(3)),
        "without RequireStable the pre-transaction position reads"
    );

    let started = Instant::now();
    let unstable = r
        .committed_positions(&group, std::slice::from_ref(&p0), GroupListing::Listed)
        .expect_err("a pending transactional offset never reads as a position");
    let unstable_took = started.elapsed();
    let unstable_ms = unstable_took.as_millis() as u64;
    assert_eq!(
        unstable,
        PositionsError::PositionsUnstable {
            group: group.clone(),
            bound: BOUND
        }
    );
    assert_timed_out_within("listed pending fetch", unstable_took, BOUND + OVER_BOUND);
    let started = Instant::now();
    let unlisted = r
        .committed_positions(&group, std::slice::from_ref(&p0), GroupListing::NotListed)
        .expect_err("the same timeout");
    let unlisted_took = started.elapsed();
    assert_timed_out_within("unlisted pending fetch", unlisted_took, BOUND + OVER_BOUND);
    assert_eq!(
        unlisted,
        PositionsError::NotVisibleOrUnreachable {
            group: group.clone(),
            bound: BOUND
        },
        "for a group no listing shows, the same timeout is not-visible-or-unreachable (§5)"
    );

    producer.abort_transaction(t).expect("abort");
    let started = Instant::now();
    let mut settled = None;
    while started.elapsed() < Duration::from_secs(30) {
        match r.committed_positions(&group, std::slice::from_ref(&p0), GroupListing::Listed) {
            Ok(g) => {
                settled = Some(g);
                break;
            }
            Err(PositionsError::PositionsUnstable { .. }) => continue,
            Err(e) => panic!("after the abort: {e}"),
        }
    }
    let settled = settled.expect("the group is stable again within 30 s of the abort");
    assert_eq!(committed(settled.get(&p0)), Some(3));

    write_evidence(
        "a_pending_transactional_offset_makes_the_group_unstable_never_the_old_position",
        &json!({
            "broker": c.version(), "topic": topic, "group": group,
            "readUncommittedWhilePending": format!("{stale_answer:?}"),
            "listedWhilePending": unstable.to_string(), "listedMs": unstable_ms,
            "unlistedWhilePending": unlisted.to_string(), "unlistedMs": unlisted_took.as_millis() as u64,
            "afterAbort": pos_json(settled.get(&p0).expect("p0")),
        }),
    );
}

/// Removes the ACLs a row added, on every exit path.
struct AclGuard<'a> {
    cluster: &'a Cluster,
    removals: Vec<Vec<String>>,
}

impl Drop for AclGuard<'_> {
    fn drop(&mut self) {
        for args in &self.removals {
            let borrowed: Vec<&str> = args.iter().map(String::as_str).collect();
            let _ = self.cluster.cli(
                &[
                    &[
                        "/opt/kafka/bin/kafka-acls.sh",
                        "--bootstrap-server",
                        self.cluster.in_network,
                        "--remove",
                        "--force",
                    ][..],
                    &borrowed[..],
                ]
                .concat(),
            );
        }
    }
}

/// A group whose only ACL names another principal is hidden from
/// `User:logweir`: never absent, its commit refused with nothing changed.
#[test]
#[ignore = "needs the `acl` profile: see this file's module doc"]
fn a_group_hidden_from_the_principal_is_never_absent() {
    let c = acl_cluster();
    let props = c.cli_ok(
        &["cat", "/opt/kafka/config/server.properties"],
        "server.properties",
    );
    assert!(
        props.contains(
            "authorizer.class.name=org.apache.kafka.metadata.authorizer.StandardAuthorizer"
        ),
        "kafka-acl must run the StandardAuthorizer"
    );
    let n = nonce();
    let topic = format!("cpos-acl-{n}");
    let hidden_topic = format!("cpos-acl-hidden-t-{n}");
    let visible = format!("cpos-acl-vis-{n}");
    let hidden = format!("cpos-acl-hid-{n}");
    c.create_topic(&topic, 1);
    c.create_topic(&hidden_topic, 1);
    let sup = c.reader();
    let p0 = tp(&topic, 0);
    let h0 = tp(&hidden_topic, 0);
    sup.commit_positions(&visible, &[(p0.clone(), at(5)), (h0.clone(), at(6))])
        .expect("super user: visible group");
    sup.commit_positions(&hidden, &[(p0.clone(), at(7))])
        .expect("super user: hidden group");

    let ops = "User:ops";
    let me = format!("User:{}", harness::SCRAM_USER);
    let hide_group: Vec<String> = [
        "--allow-principal",
        ops,
        "--operation",
        "Describe",
        "--operation",
        "Read",
        "--group",
        &hidden,
    ]
    .iter()
    .map(|s| s.to_string())
    .collect();
    let hide_topic: Vec<String> = [
        "--allow-principal",
        ops,
        "--operation",
        "Describe",
        "--topic",
        &hidden_topic,
    ]
    .iter()
    .map(|s| s.to_string())
    .collect();
    let mut guard = AclGuard {
        cluster: &c,
        removals: vec![hide_group.clone(), hide_topic.clone()],
    };
    c.acl(
        "--add",
        &hide_group.iter().map(String::as_str).collect::<Vec<_>>(),
    );
    c.acl(
        "--add",
        &hide_topic.iter().map(String::as_str).collect::<Vec<_>>(),
    );

    let restricted = RdKafkaReader::connect(
        &[harness::bootstrap_acl_sasl()],
        AuthConfig::ScramSha512 {
            username: harness::SCRAM_USER.into(),
            password: harness::SCRAM_PASSWORD.into(),
            tls: false,
            tls_ca_file: None,
        },
    )
    .expect("connect builds local state")
    .with_position_bound(BOUND)
    .expect("bound");

    // CONTROL: the same principal reads a group it may see; the hidden TOPIC
    // is refused per partition, beside the visible one (T15).
    let vis = restricted
        .committed_positions(&visible, &[p0.clone(), h0.clone()], GroupListing::NotListed)
        .expect("a visible group answers");
    assert_eq!(committed(vis.get(&p0)), Some(5));
    assert_eq!(
        vis.get(&h0),
        Some(&PartitionPosition::Failed(
            PartitionFailure::TopicNotAuthorized
        )),
        "a topic the principal may not Describe is refused by name, never absent"
    );

    // The hidden group: never absent, never a position.
    let started = Instant::now();
    let unlisted = restricted
        .committed_positions(&hidden, std::slice::from_ref(&p0), GroupListing::NotListed)
        .expect_err("a hidden group never reads as absent");
    let unlisted_took = started.elapsed();
    let unlisted_ms = unlisted_took.as_millis() as u64;
    let started = Instant::now();
    let listed = restricted
        .committed_positions(&hidden, std::slice::from_ref(&p0), GroupListing::Listed)
        .expect_err("nor as a position");
    let listed_took = started.elapsed();
    // The coordinator lookup's GROUP_AUTHORIZATION_FAILED reaches the
    // handle's queue (rdkafka_cgrp.c:797-807) while the fetch waits out its
    // bound, so the safe route NAMES the refusal. A reader that ignored the
    // queue would say NotVisibleOrUnreachable / PositionsUnstable here.
    assert_eq!(
        unlisted,
        PositionsError::NotVisibleToPrincipal {
            group: hidden.clone()
        },
        "not listed: may exist and be hidden; never GroupNotFound"
    );
    assert_eq!(
        listed,
        PositionsError::NotAuthorized {
            group: hidden.clone()
        },
        "listed: refused on the group"
    );
    // The refusal is read after the bound (the safe fetch itself only times
    // out), and no later than the bound plus the measured overhead.
    assert_timed_out_within("unlisted hidden fetch", unlisted_took, BOUND + OVER_BOUND);
    assert_timed_out_within("listed hidden fetch", listed_took, BOUND + OVER_BOUND);

    // Its commit is refused, and nothing changes.
    let started = Instant::now();
    let commit = restricted
        .commit_positions(&hidden, &[(p0.clone(), at(9))])
        .expect_err("the hidden group refuses this principal's commit");
    let commit_took = started.elapsed();
    let commit_ms = commit_took.as_millis() as u64;
    // The coordinator wait is `session.timeout.ms` (the bound) plus at most
    // librdkafka's one-second timeout scan; twice the bound covers it.
    assert_timed_out_within("hidden commit", commit_took, 2 * BOUND + OVER_BOUND);
    assert_eq!(
        commit,
        CommitError::NotAuthorized {
            group: hidden.clone()
        },
        "the commit waited for a coordinator the broker refused to name, and says why"
    );
    assert!(!commit.may_have_applied());
    assert_eq!(
        c.cli_offsets(&hidden).get(&(topic.clone(), 0)),
        Some(&Some(7)),
        "nothing changed"
    );

    // CONTROL: grant this principal Read and Describe on the group; the SAME
    // commit now applies, so the refusal above was the ACL's.
    let grant: Vec<String> = [
        "--allow-principal",
        &me,
        "--operation",
        "Describe",
        "--operation",
        "Read",
        "--group",
        &hidden,
    ]
    .iter()
    .map(|s| s.to_string())
    .collect();
    guard.removals.push(grant.clone());
    c.acl(
        "--add",
        &grant.iter().map(String::as_str).collect::<Vec<_>>(),
    );
    let mut granted = Err(CommitError::Client("not tried".into()));
    for _ in 0..20 {
        granted = restricted.commit_positions(&hidden, &[(p0.clone(), at(9))]);
        if granted.is_ok() {
            break;
        }
        std::thread::sleep(Duration::from_millis(500));
    }
    granted.expect("with Read and Describe granted, the same commit applies");
    assert_eq!(
        c.cli_offsets(&hidden).get(&(topic.clone(), 0)),
        Some(&Some(9))
    );
    drop(guard);

    write_evidence(
        "a_group_hidden_from_the_principal_is_never_absent",
        &json!({
            "broker": c.version(), "topic": topic, "hiddenTopic": hidden_topic,
            "visibleGroup": visible, "hiddenGroup": hidden,
            "visible": vis.partitions.iter().map(|(t, p)| json!({"tp": t.to_string(), "answer": pos_json(p)})).collect::<Vec<_>>(),
            "hiddenNotListed": unlisted.to_string(), "hiddenNotListedMs": unlisted_ms,
            "hiddenListed": listed.to_string(), "hiddenListedMs": listed_took.as_millis() as u64,
            "hiddenCommit": commit.to_string(), "hiddenCommitMs": commit_ms,
            "afterGrant": "applied 9",
        }),
    );
}
