#![cfg(feature = "e2e")]
//! **PROD-04.0b — the group and ACL calls inside OD-6's perimeter, against a
//! real broker.** `RdKafkaReader::classify_groups`, `describe_groups`,
//! `group_listings`, `cluster_access` and `capture_acls`
//! (`docs/to-do/decisions/PROD-04.0-admin-path.md` §4, §5, §6), each row
//! checked against the broker's OWN tools run inside its container
//! (`kafka-groups.sh` through `e2e/compose/groups.sh list`,
//! `kafka-consumer-groups.sh`, `kafka-acls.sh`), never against the API under
//! test alone.
//!
//! | row | proves | negative control |
//! |---|---|---|
//! | `every_fixture_group_gets_the_verdict_its_type_calls_for` | one verdict per id; classic and consumer groups captured with the broker's state and member count; share and streams groups `GroupTypeNotCaptured`; an absent id `GroupNotFound` and still absent after | the typed listing ALONE omits the share and streams groups the name listing shows (T3): classifying from it would drop them |
//! | `a_group_hidden_from_the_principal_is_never_absent` (`acl`) | with §3.9's visibility setup the restricted principal's listing is not complete, its hidden group is `NotVisibleToPrincipal`, a describable absent id `GroupNotFound` by targeted describe, and the visible group captured | the super user's complete listing captures the hidden group; granting the principal Describe on the cluster makes the listing complete and the group listed, and its description then `NotAuthorized` |
//! | `acls_without_an_authorizer_are_not_applicable` | on the broker with no authorizer, coverage is `AuthorizerDisabled` with no binding | `kafka-acls.sh --list` says SecurityDisabledException there, and DescribeAcls alone reads "0 bindings" |
//! | `acls_round_trip_and_what_librdkafka_cannot_name_is_counted` (`acl`) | literal, prefixed, wildcard-name, wildcard-principal, ALLOW and DENY bindings on TOPIC, GROUP, TRANSACTIONAL_ID and CLUSTER equal `kafka-acls.sh --list`; CLUSTER is named CLUSTER; USER, DELEGATION_TOKEN and TwoPhaseCommit bindings are counted not-representable, never exported | the CLI lists the unnameable bindings as distinct, and none of them appears among the exported ones |
//! | `a_principal_denied_on_the_cluster_is_capture_denied` (`acl`) | the restricted principal without cluster operations gets `CaptureDenied`, never "captured, 0 bindings" | granting Describe alone gives `Unverified` (its DescribeConfigs is refused, T13); granting DescribeConfigs too gives `Captured` with the super user's bindings |
//! | `thousands_of_admin_calls_keep_the_resident_set_bounded` | the soak of PROD-04.0 §7.2 over every call, against the broker | the measured growth is printed and bounded |
//!
//! # Running them
//!
//! ```text
//! bash scripts/extract-engine.sh
//! eval "$(e2e/compose/stack-env.sh --slot N --kafka 4.3 --profiles acl,streams-protocol)"
//! just e2e-up
//! cargo test -p e2e --features e2e --test group_admin -- --include-ignored --test-threads=1 --nocapture
//! just e2e-down
//! ```
//!
//! `--kafka 3.9` runs the same rows with classic groups only (the fixture and
//! the rows read what the line supports; omit `streams-protocol` there). Every
//! row writes what it observed to `group-admin/<row>.json` under the stack's
//! scratch directory (`harness::demo_dir()`).
//!
//! # Hygiene
//!
//! The groups come from `e2e/compose/groups.sh up` (idempotent). Every ACL a
//! row adds names a fresh nonce or is removed on every exit path (a guard),
//! and §3.9's visibility state is removed by a guard too. Every subprocess is
//! bounded ([`output_within`]).
mod harness;

use harness::{demo_dir, root};
use logweir_kafka::access::ClusterAccess;
use logweir_kafka::acls::{AclCoverage, Unverified};
use logweir_kafka::groups::{
    Absence, CapturableGroup, DescribeFailure, Excluded, GroupFailure, GroupState, GroupType,
    GroupVerdict, IncompleteReason, ListingCompleteness, OtherType,
};
use logweir_kafka::rdkafka_reader::RdKafkaReader;
use logweir_kafka::reader::AuthConfig;
use serde_json::{json, Value};
use std::collections::{BTreeMap, BTreeSet};
use std::io::Read;
use std::process::{Command, Output, Stdio};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

/// The bound of every admin call a row makes.
const BOUND: Duration = Duration::from_secs(10);

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
            Ok(None) => std::thread::sleep(Duration::from_millis(100)),
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

/// The fixture's groups as the BROKER reports them: `(group, TYPE, STATE)`.
fn fixture_groups() -> Vec<(String, String, String)> {
    groups_sh(&["up"], 900);
    groups_sh(&["list"], 300)
        .lines()
        .skip(1)
        .filter_map(|l| {
            let t: Vec<&str> = l.split_whitespace().collect();
            (t.len() == 3).then(|| (t[0].to_string(), t[1].to_string(), t[2].to_string()))
        })
        .collect()
}

// ============================================================ the stack

/// One Kafka cluster of the stack.
struct Cluster {
    service: &'static str,
    profile: Option<&'static str>,
    in_network: &'static str,
    plaintext: String,
}

fn default_cluster() -> Cluster {
    harness::stack::ensure_coherent();
    Cluster {
        service: "kafka-broker-1",
        profile: None,
        in_network: "kafka-broker-1:9094",
        plaintext: harness::bootstrap(),
    }
}

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
    /// One of the broker's own CLIs inside its RUNNING container; bounded.
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

    /// The super-user reader (PLAINTEXT: no authorizer on the default broker,
    /// `User:ANONYMOUS`, a super user, on `acl`).
    fn reader(&self) -> RdKafkaReader {
        RdKafkaReader::connect(std::slice::from_ref(&self.plaintext), AuthConfig::Plaintext)
            .expect("connect builds local state")
            .with_admin_bound(BOUND)
            .expect("10 s is a valid admin bound")
    }

    /// `kafka-consumer-groups.sh --describe --state`: STATE and #MEMBERS.
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
        text(&o).lines().find_map(|line| {
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

    fn acl(&self, op: &str, args: &[&str]) -> Output {
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
        self.cli(&all)
    }

    fn acl_ok(&self, op: &str, args: &[&str]) {
        let o = self.acl(op, args);
        assert!(
            o.status.success(),
            "kafka-acls {op} {args:?}:\n{}",
            text(&o)
        );
    }
}

/// Removes the ACLs a row added, on every exit path.
struct AclGuard<'a> {
    cluster: &'a Cluster,
    removals: Vec<Vec<String>>,
}

impl Drop for AclGuard<'_> {
    fn drop(&mut self) {
        for r in &self.removals {
            let args: Vec<&str> = r.iter().map(String::as_str).collect();
            let _ = self.cluster.acl("--remove", &args);
        }
    }
}

/// §3.9's visibility state, removed on every exit path.
struct Visibility;

impl Visibility {
    fn apply() -> Visibility {
        groups_sh(&["visibility", "apply"], 300);
        Visibility
    }
}

impl Drop for Visibility {
    fn drop(&mut self) {
        let mut c = Command::new("bash");
        c.args(["e2e/compose/groups.sh", "visibility", "remove"])
            .current_dir(root());
        let _ = output_within(c, 300);
    }
}

fn restricted_reader() -> RdKafkaReader {
    RdKafkaReader::connect(
        &[harness::bootstrap_acl_sasl()],
        AuthConfig::ScramSha512 {
            username: harness::SCRAM_USER.into(),
            password: harness::SCRAM_PASSWORD.into(),
            tls: false,
            tls_ca_file: None,
        },
    )
    .expect("connect builds local state")
    .with_admin_bound(BOUND)
    .expect("bound")
}

fn nonce() -> String {
    let n = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("the clock is after 1970")
        .as_nanos();
    format!("{:010}", n % 10_000_000_000)
}

fn write_evidence(row: &str, v: &Value) {
    let dir = demo_dir().join("group-admin");
    std::fs::create_dir_all(&dir).expect("evidence dir");
    let path = dir.join(format!("{row}.json"));
    std::fs::write(&path, serde_json::to_vec_pretty(v).expect("json")).expect("write evidence");
    eprintln!("evidence: {}", path.display());
}

fn verdict_json(v: &GroupVerdict) -> Value {
    match v {
        GroupVerdict::Capture(c) => json!({
            "outcome": "capture",
            "groupType": c.group_type().wire_name(),
            "state": c.state().wire_name(),
            "simple": c.is_simple(),
        }),
        other => json!({ "outcome": format!("{other:?}") }),
    }
}

fn capture_of(v: &GroupVerdict) -> &CapturableGroup {
    match v {
        GroupVerdict::Capture(c) => c,
        other => panic!("expected a capture, got {other:?}"),
    }
}

// ============================================================ groups

/// AP-04.1-1 against the fixture, on whatever types the line supports.
#[test]
fn every_fixture_group_gets_the_verdict_its_type_calls_for() {
    let c = default_cluster();
    let version = c.version();
    let fixture = fixture_groups();
    assert!(!fixture.is_empty(), "groups.sh listed no group");
    let absent = format!("pa-absent-{}", nonce());
    let mut selected: Vec<String> = fixture.iter().map(|(g, _, _)| g.clone()).collect();
    selected.push(absent.clone());
    let r = c.reader();

    // NEGATIVE CONTROL (T3): the typed listing alone omits every share and
    // streams group the name listing shows; classifying from it would drop
    // them.
    let listings = r.group_listings();
    let typed: BTreeSet<&str> = listings.typed.iter().map(|t| t.group_id.as_str()).collect();
    let names: BTreeSet<&str> = listings.names.iter().map(|n| n.group_id.as_str()).collect();
    for (g, ty, _) in &fixture {
        assert!(
            names.contains(g.as_str()),
            "{g}: the name listing shows every type"
        );
        let typed_expected = matches!(ty.as_str(), "Classic" | "Consumer");
        assert_eq!(
            typed.contains(g.as_str()),
            typed_expected,
            "{g} ({ty}): ListConsumerGroups keeps classic and consumer groups only (C7)"
        );
    }
    assert_eq!(
        listings.access.describe(),
        Some(true),
        "{:?}",
        listings.access
    );

    let started = Instant::now();
    let classification = r.classify_groups(&selected).expect("valid ids");
    let took = started.elapsed();
    assert_eq!(classification.completeness, ListingCompleteness::Complete);
    assert!(
        classification.targeted.is_empty(),
        "a complete listing needs no targeted call"
    );
    assert_eq!(
        classification
            .verdicts
            .iter()
            .map(|(g, _)| g.clone())
            .collect::<Vec<_>>(),
        selected,
        "exactly one verdict per selected id, in order"
    );
    let verdicts: BTreeMap<String, GroupVerdict> =
        classification.verdicts.iter().cloned().collect();

    let mut rows = Vec::new();
    let mut captured = Vec::new();
    for (g, ty, state) in &fixture {
        let v = &verdicts[g];
        match ty.as_str() {
            "Classic" | "Consumer" => {
                let cg = capture_of(v);
                let want = if ty == "Classic" {
                    GroupType::Classic
                } else {
                    GroupType::Consumer
                };
                assert_eq!(cg.group_type(), want, "{g}");
                assert_eq!(
                    cg.state().wire_name(),
                    state.as_str(),
                    "{g}: the broker's state"
                );
                captured.push(cg.clone());
            }
            "Share" | "Streams" => assert_eq!(
                v,
                &GroupVerdict::Excluded(Excluded::GroupTypeNotCaptured {
                    why: OtherType::NotInTypedListing
                }),
                "{g} ({ty})"
            ),
            other => {
                panic!("{g}: the broker reported type {other:?}, which this row does not know")
            }
        }
        rows.push(json!({ "group": g, "brokerType": ty, "brokerState": state, "verdict": verdict_json(v) }));
    }
    assert_eq!(
        verdicts[&absent],
        GroupVerdict::Excluded(Excluded::GroupNotFound {
            evidence: Absence::CompleteListing
        })
    );

    // Descriptions of the captured groups: the broker's member count, and an
    // assignment for every live member.
    let mut described = Vec::new();
    for (g, d) in r.describe_groups(&captured) {
        let d = d.unwrap_or_else(|e| panic!("{g}: {e:?}"));
        let (cli_state, cli_members) = c.cli_state(&g).unwrap_or_else(|| panic!("{g}: no CLI row"));
        assert_eq!(d.state.wire_name(), cli_state, "{g}: described state");
        assert_eq!(d.members.len() as u32, cli_members, "{g}: member count");
        for m in &d.members {
            assert!(
                !m.assignment.is_empty() || d.group_type == GroupType::Consumer,
                "{g}: {m:?}"
            );
        }
        described.push(json!({
            "group": g, "state": d.state.wire_name(), "members": d.members.len(),
            "assignor": d.partition_assignor, "assignments": d.members.iter().map(|m| m.assignment.iter().map(|tp| tp.to_string()).collect::<Vec<_>>()).collect::<Vec<_>>(),
            "cli": { "state": cli_state, "members": cli_members }
        }));
    }
    // Reading does not create the absent id.
    assert!(!c.cli_lists(&absent), "{absent}: classification created it");
    write_evidence(
        &format!("every-fixture-group-{version}"),
        &json!({
            "broker": version, "classifyMs": took.as_millis() as u64,
            "typedListing": typed, "nameListing": names,
            "verdicts": rows, "absent": { "id": absent, "verdict": format!("{:?}", verdicts[&absent]) },
            "descriptions": described,
        }),
    );
}

/// AP-04.1-6 and T14, live (§3.9).
#[test]
#[ignore = "needs the `acl` profile"]
fn a_group_hidden_from_the_principal_is_never_absent() {
    let c = acl_cluster();
    let version = c.version();
    let visibility = Visibility::apply();
    let absent = format!("pa-absent-{}", nonce());
    let selected = vec![
        "pa-visible".to_string(),
        "pa-hidden".to_string(),
        absent.clone(),
    ];
    let restricted = restricted_reader();

    // The visibility ACLs are in force once the principal's cluster
    // operations read as an empty REPORTED set.
    let mut access = restricted.cluster_access();
    for _ in 0..40 {
        if access == ClusterAccess::Reported(vec![]) {
            break;
        }
        std::thread::sleep(Duration::from_millis(250));
        access = restricted.cluster_access();
    }
    assert_eq!(access, ClusterAccess::Reported(vec![]));
    let started = Instant::now();
    let cl = restricted.classify_groups(&selected).expect("valid ids");
    let took = started.elapsed();
    let v: BTreeMap<String, GroupVerdict> = cl.verdicts.iter().cloned().collect();
    let ListingCompleteness::NotComplete(why) = &cl.completeness else {
        panic!(
            "a filtered listing is never complete: {:?}",
            cl.completeness
        )
    };
    assert!(
        why.contains(&IncompleteReason::NoDescribeOnCluster),
        "{why:?}"
    );
    assert_eq!(cl.targeted, vec!["pa-hidden".to_string(), absent.clone()]);
    assert_eq!(
        v["pa-hidden"],
        GroupVerdict::Failed(GroupFailure::NotVisibleToPrincipal)
    );
    assert_eq!(
        v[&absent],
        GroupVerdict::Excluded(Excluded::GroupNotFound {
            evidence: Absence::TargetedDescribe
        })
    );
    let visible = capture_of(&v["pa-visible"]);
    assert_eq!(
        (visible.group_type(), visible.state()),
        (GroupType::Classic, GroupState::Empty)
    );
    assert!(took < 3 * (BOUND + Duration::from_secs(5)), "{took:?}");

    // CONTROL 1: the super user's listing is complete and captures it.
    let sup = c.reader().classify_groups(&selected[..2]).expect("valid");
    assert_eq!(sup.completeness, ListingCompleteness::Complete);
    assert_eq!(
        capture_of(&sup.verdicts[1].1).group_type(),
        GroupType::Classic
    );

    // CONTROL 2: Describe on the cluster unfilters the listing; the hidden
    // group is then LISTED (typed classic) and its description refused.
    let me = format!("User:{}", harness::SCRAM_USER);
    let grant = vec![
        "--allow-principal".to_string(),
        me.clone(),
        "--operation".into(),
        "Describe".into(),
        "--cluster".into(),
    ];
    let guard = AclGuard {
        cluster: &c,
        removals: vec![grant.clone()],
    };
    c.acl_ok(
        "--add",
        &grant.iter().map(String::as_str).collect::<Vec<_>>(),
    );
    let mut granted = None;
    for _ in 0..40 {
        let g = restricted.classify_groups(&selected[..2]).expect("valid");
        if g.completeness == ListingCompleteness::Complete {
            granted = Some(g);
            break;
        }
        std::thread::sleep(Duration::from_millis(250));
    }
    let granted = granted.expect("the grant made the listing complete within 10 s");
    let hidden = capture_of(&granted.verdicts[1].1).clone();
    let described = restricted.describe_groups(std::slice::from_ref(&hidden));
    assert_eq!(described[0].1, Err(DescribeFailure::NotAuthorized));
    drop(guard);
    write_evidence(
        &format!("hidden-group-{version}"),
        &json!({
            "broker": version, "classifyMs": took.as_millis() as u64,
            "restricted": { "completeness": format!("{:?}", cl.completeness), "targeted": cl.targeted,
                "verdicts": cl.verdicts.iter().map(|(g, v)| json!({"group": g, "verdict": verdict_json(v)})).collect::<Vec<_>>() },
            "superUser": sup.verdicts.iter().map(|(g, v)| json!({"group": g, "verdict": verdict_json(v)})).collect::<Vec<_>>(),
            "grantedDescribeOnCluster": { "hidden": verdict_json(&granted.verdicts[1].1), "describe": format!("{:?}", described[0].1) },
        }),
    );
    drop(visibility);
}

// ============================================================ ACLs

/// AP-05.3-1, live: the default broker has no authorizer.
#[test]
fn acls_without_an_authorizer_are_not_applicable() {
    let c = default_cluster();
    let version = c.version();
    let capture = c.reader().capture_acls();
    assert_eq!(capture.coverage, AclCoverage::AuthorizerDisabled);
    assert!(capture.bindings.is_empty() && capture.not_representable.is_empty());
    let cli = c.acl("--list", &[]);
    let said = text(&cli);
    assert!(
        said.contains("SecurityDisabledException") || said.contains("No Authorizer is configured"),
        "the broker's own tool must say ACLs are disabled:\n{said}"
    );
    write_evidence(
        &format!("acls-no-authorizer-{version}"),
        &json!({ "broker": version, "coverage": format!("{:?}", capture.coverage), "cli": said.trim() }),
    );
}

/// One binding as `kafka-acls.sh --list` prints it.
type CliAcl = (String, String, String, String, String, String, String);

/// Parses `kafka-acls.sh --list`: `Current ACLs for resource
/// \`ResourcePattern(resourceType=…, name=…, patternType=…)\`:` followed by
/// `(principal=…, host=…, operation=…, permissionType=…)` lines.
fn parse_cli_acls(out: &str) -> Vec<CliAcl> {
    let field = |s: &str, key: &str| -> String {
        let start = s
            .find(&format!("{key}="))
            .map(|i| i + key.len() + 1)
            .unwrap_or(s.len());
        let rest = &s[start..];
        let end = rest.find([',', ')']).unwrap_or(rest.len());
        rest[..end].trim().to_string()
    };
    let mut out_acls = Vec::new();
    let mut resource: Option<(String, String, String)> = None;
    for line in out.lines() {
        if line.contains("ResourcePattern(") {
            resource = Some((
                field(line, "resourceType"),
                field(line, "name"),
                field(line, "patternType"),
            ));
        } else if line.trim_start().starts_with("(principal=") {
            if let Some((rt, name, pt)) = &resource {
                out_acls.push((
                    rt.clone(),
                    name.clone(),
                    pt.clone(),
                    field(line, "principal"),
                    field(line, "host"),
                    field(line, "operation"),
                    field(line, "permissionType"),
                ));
            }
        }
    }
    out_acls
}

/// AP-05.3-3 and AP-05.3-4, live, as the super user on `acl`.
#[test]
#[ignore = "needs the `acl` profile"]
fn acls_round_trip_and_what_librdkafka_cannot_name_is_counted() {
    let c = acl_cluster();
    let version = c.version();
    let four = !version.starts_with('3');
    let n = nonce();
    let who = format!("User:lw-acl-{n}");
    let topic = format!("lw-acl-{n}");
    let prefix = format!("lw-acl-{n}-");
    let deny_topic = format!("lw-acl-deny-{n}");
    let group = format!("lw-acl-g-{n}");
    let txn = format!("lw-acl-tx-{n}");
    let token = format!("lw-tok-{n}");
    let bob = format!("User:lw-bob-{n}");
    let s = |v: &[&str]| v.iter().map(|x| x.to_string()).collect::<Vec<String>>();
    let mut adds: Vec<Vec<String>> = vec![
        s(&[
            "--allow-principal",
            &who,
            "--operation",
            "Read",
            "--topic",
            &topic,
        ]),
        s(&[
            "--allow-principal",
            &who,
            "--operation",
            "Write",
            "--topic",
            &prefix,
            "--resource-pattern-type",
            "prefixed",
        ]),
        s(&[
            "--allow-principal",
            &who,
            "--operation",
            "Describe",
            "--topic",
            "*",
        ]),
        s(&[
            "--deny-principal",
            "User:*",
            "--operation",
            "Write",
            "--topic",
            &deny_topic,
        ]),
        s(&[
            "--allow-principal",
            &who,
            "--operation",
            "Read",
            "--group",
            &group,
        ]),
        s(&[
            "--allow-principal",
            &who,
            "--operation",
            "Write",
            "--transactional-id",
            &txn,
        ]),
        s(&[
            "--allow-principal",
            &who,
            "--operation",
            "Describe",
            "--cluster",
        ]),
        // What librdkafka cannot name (T10): a USER resource with the token
        // operations, a DELEGATION_TOKEN resource.
        s(&[
            "--allow-principal",
            &who,
            "--operation",
            "CreateTokens",
            "--operation",
            "DescribeTokens",
            "--user-principal",
            &bob,
        ]),
        s(&[
            "--allow-principal",
            &who,
            "--operation",
            "Describe",
            "--delegation-token",
            &token,
        ]),
    ];
    if four {
        adds.push(s(&[
            "--allow-principal",
            &who,
            "--operation",
            "TwoPhaseCommit",
            "--transactional-id",
            &txn,
        ]));
    }
    let guard = AclGuard {
        cluster: &c,
        removals: adds.clone(),
    };
    for a in &adds {
        c.acl_ok("--add", &a.iter().map(String::as_str).collect::<Vec<_>>());
    }
    let mine = |name: &str, principal: &str| name.contains(&n) || principal.contains(&n);

    // The broker's own list, as the super user, after the adds are visible.
    let mut cli: Vec<CliAcl> = Vec::new();
    for _ in 0..40 {
        let o = c.acl("--list", &[]);
        cli = parse_cli_acls(&String::from_utf8_lossy(&o.stdout))
            .into_iter()
            .filter(|a| mine(&a.1, &a.3))
            .collect();
        if cli.len() > adds.len() {
            break;
        }
        std::thread::sleep(Duration::from_millis(250));
    }
    let capture = c.reader().capture_acls();
    assert_eq!(capture.coverage, AclCoverage::Captured);
    let exported: BTreeSet<CliAcl> = capture
        .bindings
        .iter()
        .filter(|b| mine(&b.name, &b.principal))
        .map(|b| {
            (
                b.resource_type.kafka_name().to_string(),
                b.name.clone(),
                b.pattern_type.kafka_name().to_string(),
                b.principal.clone(),
                b.host.clone(),
                b.operation.kafka_name().to_string(),
                b.permission.kafka_name().to_string(),
            )
        })
        .collect();
    let unnameable = |a: &CliAcl| {
        matches!(a.0.as_str(), "USER" | "DELEGATION_TOKEN") || a.5 == "TWO_PHASE_COMMIT"
    };
    let cli_nameable: BTreeSet<CliAcl> = cli.iter().filter(|a| !unnameable(a)).cloned().collect();
    let cli_unnameable: Vec<&CliAcl> = cli.iter().filter(|a| unnameable(a)).collect();
    assert_eq!(
        exported, cli_nameable,
        "every nameable binding round-trips exactly"
    );
    assert!(
        exported
            .iter()
            .any(|a| a.0 == "CLUSTER" && a.1 == "kafka-cluster"),
        "the CLUSTER resource is named CLUSTER (T11): {exported:?}"
    );
    assert!(exported.iter().any(|a| a.2 == "PREFIXED" && a.1 == prefix));
    let counted: Vec<_> = capture
        .not_representable
        .iter()
        .filter(|x| x.principal.as_deref().is_some_and(|p| p.contains(&n)))
        .collect();
    assert_eq!(
        counted.len(),
        cli_unnameable.len(),
        "every binding librdkafka cannot name is counted once: CLI {cli_unnameable:?}, counted {counted:?}"
    );
    assert_eq!(
        cli_unnameable.len(),
        if four { 4 } else { 3 },
        "{cli_unnameable:?}"
    );
    write_evidence(
        &format!("acls-round-trip-{version}"),
        &json!({
            "broker": version, "coverage": format!("{:?}", capture.coverage),
            "exported": exported.iter().map(|a| format!("{a:?}")).collect::<Vec<_>>(),
            "cliUnnameable": cli_unnameable.iter().map(|a| format!("{a:?}")).collect::<Vec<_>>(),
            "notRepresentable": counted.iter().map(|x| json!({"principal": x.principal, "name": x.name, "why": x.why, "raw": format!("{:?}", x.raw)})).collect::<Vec<_>>(),
        }),
    );
    drop(guard);
}

/// AP-05.3-2, live: §3.8's denied principal, and the two grants after it.
#[test]
#[ignore = "needs the `acl` profile"]
fn a_principal_denied_on_the_cluster_is_capture_denied() {
    let c = acl_cluster();
    let version = c.version();
    let visibility = Visibility::apply();
    let restricted = restricted_reader();
    let denied = restricted.capture_acls();
    assert_eq!(denied.coverage, AclCoverage::CaptureDenied, "{denied:?}");
    assert!(denied.bindings.is_empty());

    let me = format!("User:{}", harness::SCRAM_USER);
    let describe = vec![
        "--allow-principal".to_string(),
        me.clone(),
        "--operation".into(),
        "Describe".into(),
        "--cluster".into(),
    ];
    let configs = vec![
        "--allow-principal".to_string(),
        me.clone(),
        "--operation".into(),
        "DescribeConfigs".into(),
        "--cluster".into(),
    ];
    let guard = AclGuard {
        cluster: &c,
        removals: vec![describe.clone(), configs.clone()],
    };
    c.acl_ok(
        "--add",
        &describe.iter().map(String::as_str).collect::<Vec<_>>(),
    );
    let mut only_describe = restricted.capture_acls();
    for _ in 0..40 {
        if !matches!(only_describe.coverage, AclCoverage::CaptureDenied) {
            break;
        }
        std::thread::sleep(Duration::from_millis(250));
        only_describe = restricted.capture_acls();
    }
    assert!(
        matches!(
            only_describe.coverage,
            AclCoverage::Unverified(Unverified::AuthorizerUnread(_) | Unverified::AuthorizerMissing)
        ),
        "Describe without DescribeConfigs: the authorizer is unverified (T13), never captured: {:?}",
        only_describe.coverage
    );
    c.acl_ok(
        "--add",
        &configs.iter().map(String::as_str).collect::<Vec<_>>(),
    );
    let mut both = restricted.capture_acls();
    for _ in 0..40 {
        if both.coverage == AclCoverage::Captured {
            break;
        }
        std::thread::sleep(Duration::from_millis(250));
        both = restricted.capture_acls();
    }
    assert_eq!(both.coverage, AclCoverage::Captured);
    let sup = c.reader().capture_acls();
    assert_eq!(
        both.bindings, sup.bindings,
        "the granted principal reads the super user's set"
    );
    assert!(!both.bindings.is_empty());
    write_evidence(
        &format!("acls-denied-{version}"),
        &json!({
            "broker": version,
            "denied": format!("{:?}", denied.coverage),
            "describeOnly": format!("{:?}", only_describe.coverage),
            "describeAndDescribeConfigs": format!("{:?}", both.coverage),
            "bindings": both.bindings.len(),
        }),
    );
    drop(guard);
    drop(visibility);
}

// ============================================================ soak

/// This process's resident set in KiB, from `ps`, bounded.
fn rss_kib() -> u64 {
    let mut c = Command::new("ps");
    c.args(["-o", "rss=", "-p", &std::process::id().to_string()]);
    let o = output_within(c, 30);
    String::from_utf8_lossy(&o.stdout)
        .trim()
        .parse()
        .expect("a number of KiB")
}

/// PROD-04.0 §7.2's soak against the broker: every FFI call, a thousand
/// times over.
#[test]
fn thousands_of_admin_calls_keep_the_resident_set_bounded() {
    let c = default_cluster();
    let version = c.version();
    let fixture = fixture_groups();
    let selected: Vec<String> = fixture.iter().map(|(g, _, _)| g.clone()).collect();
    let r = c.reader();
    let round = || {
        let cl = r.classify_groups(&selected).expect("valid");
        let captured: Vec<CapturableGroup> = cl
            .verdicts
            .iter()
            .filter_map(|(_, v)| match v {
                GroupVerdict::Capture(g) => Some(g.clone()),
                _ => None,
            })
            .collect();
        for (_, d) in r.describe_groups(&captured) {
            d.expect("described");
        }
        assert_eq!(r.capture_acls().coverage, AclCoverage::AuthorizerDisabled);
        // DescribeCluster, the name listing, the typed listing, one
        // description call, DescribeConfigs, DescribeCluster, DescribeAcls.
        7
    };
    for _ in 0..50 {
        round();
    }
    let before = rss_kib();
    let started = Instant::now();
    let mut calls = 0;
    for _ in 0..1000 {
        calls += round();
    }
    let took = started.elapsed();
    let after = rss_kib();
    let grew = after.saturating_sub(before);
    eprintln!("soak: {calls} admin calls in {took:?}; resident set {before} KiB -> {after} KiB (+{grew} KiB)");
    write_evidence(
        &format!("soak-{version}"),
        &json!({ "broker": version, "calls": calls, "ms": took.as_millis() as u64, "rssBeforeKiB": before, "rssAfterKiB": after, "grewKiB": grew }),
    );
    assert!(
        grew < 16 * 1024,
        "{calls} calls grew the resident set by {grew} KiB ({before} -> {after})"
    );
}
