#![cfg(feature = "e2e")]
//! **PROD-01.2 — the compatibility contract's reachable rows, end to end.**
//!
//! Every "supported", "limited" and "unsupported" cell of
//! `docs/support-matrix.md` that this row added cites one of these rows; the
//! record is `docs/to-do/decisions/PROD-01.2-compatibility-contract.md`.
//!
//! | row | needs | proves |
//! |---|---|---|
//! | `the_default_broker_answers_every_capability_check` | the stack as CI runs it | the shipped `logweir check run` reads the broker's ApiVersions through a real connection, plaintext and SASL, and each capability row agrees with the broker's own tool; a plan that lists none gets none |
//! | `the_default_broker_backs_up_restores_and_verifies` | any broker line (`--kafka`) | the generic row: probe, capability checks, backup, restore, verify, on Apache Kafka. The control for the two rows below |
//! | `confluent_platform_backs_up_restores_and_verifies` | profile `confluent` | the same generic row on Confluent Platform 8.3.2, with no difference |
//! | `redpanda_backs_up_and_refuses_a_restore_before_it_starts` | profile `redpanda` | Redpanda v26.2.4: probe and backup work; its groups cannot be typed and the receipt says so; its broker resource reports no timestamp bound; `target.engineProtocol` is `notReady` naming Produce v8, and the restore it predicts cannot run does fail, signing nothing |
//! | `redpanda_authenticates_both_scram_mechanisms` | profile `redpanda` | SCRAM-SHA-256 and SCRAM-SHA-512 against Redpanda's own SCRAM, on both clients; a wrong password is refused |
//! | `seaweedfs_takes_a_backup_a_restore_and_refuses_a_second_claim` | profile `objectstore` | the maintained object store, through Logweir: conditional create, the pinned manifest version on a versioned bucket, backup, restore, verify |
//! | `the_minimum_acls_for_probe_backup_and_restore` | profile `acl` | the minimum-permission profile: exactly the listed ACLs work, and each one removed fails or degrades the way the record says |
//! | `an_unreachable_advertised_address_is_not_a_reachable_cluster` | profile `confluent` | a listener that advertises a dead address: the probe still answers `reachable=true`, the readiness check says the bootstrap answered and the advertised brokers did not, the capability rows are blocked behind it, and a backup fails with no receipt |
//!
//! Every row but the first is `#[ignore]`d, like `config_coverage`'s: each
//! needs a profile CI's default job does not start, or runs a whole drill on
//! a topic of its own. Run them on a slot:
//!
//! ```text
//! eval "$(e2e/compose/stack-env.sh --slot N --kafka 4.3 --profiles redpanda,confluent,acl,objectstore)"
//! just e2e-up
//! cargo build -p logweir
//! AWS_EC2_METADATA_DISABLED=true cargo test -p e2e --features e2e --test compat_contract -- \
//!   --include-ignored --test-threads=1 --nocapture
//! just e2e-down
//! ```
//!
//! Each row writes what it observed to `compat/<row>.json` under the stack's
//! scratch directory (`harness::demo_dir()`).
//!
//! # The oracle is never Logweir's own answer
//!
//! Where a row says what an endpoint serves, the ground truth is the broker's
//! own tool run inside the stack (`kafka-broker-api-versions.sh` from the
//! `apache/kafka` image): [`broker_api_versions`]. The capability row is then
//! REQUIRED to agree with it, whichever way it points, so the same row is
//! right on a 3.7 broker (no group types) and on a 4.3 one.

mod harness;
use harness::*;
use logweir_core::check_contract::frames::Decoder;
use logweir_core::check_contract::{
    capability_checks_for, CheckCode, CheckId, CheckOperation, CheckOutcome, CheckPlan,
    CheckRequest, CheckResult, CheckState, ConnectionPlan, FrameExpectations, Gating,
    OperationReadinessRequest, CHECK_CONTRACT_VERSION, CHECK_PLAN_CONTRACT,
};
use logweir_kafka::inventory::InventoryProbe;
use logweir_kafka::positions::{CommittedPosition, TopicPartition};
use logweir_kafka::rdkafka_reader::RdKafkaReader;
use logweir_kafka::reader::{AuthConfig, ClusterReader, TopicDeleter};
use serde_json::{json, Value};
use std::collections::BTreeMap;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::process::{Command, Output, Stdio};
use std::time::{Duration, Instant};

// ============================================================ endpoints

/// One Kafka-compatible endpoint of the stack, as a host-side client and an
/// in-network one reach it.
#[derive(Clone, Copy, Debug)]
struct Endpoint {
    /// The name in evidence files and messages.
    label: &'static str,
    /// The host-side plaintext bootstrap, as the endpoint advertises it.
    bootstrap: fn() -> String,
    /// The in-network plaintext address, for the broker's own tools.
    in_network: &'static str,
}

const KAFKA: Endpoint = Endpoint {
    label: "kafka",
    bootstrap: harness::bootstrap,
    in_network: "kafka-broker-1:9094",
};
const REDPANDA: Endpoint = Endpoint {
    label: "redpanda",
    bootstrap: harness::bootstrap_redpanda,
    in_network: "redpanda:9094",
};
const CONFLUENT: Endpoint = Endpoint {
    label: "confluent",
    bootstrap: harness::bootstrap_confluent,
    in_network: "kafka-cp:9094",
};

/// How a row authenticates: nothing, or SASL/SCRAM without TLS.
#[derive(Clone, Debug)]
struct Auth {
    mode: &'static str,
    username: Option<&'static str>,
    password: Option<String>,
}

impl Auth {
    fn plaintext() -> Self {
        Self {
            mode: "plaintext",
            username: None,
            password: None,
        }
    }
    fn scram(mode: &'static str, username: &'static str, password: &str) -> Self {
        Self {
            mode,
            username: Some(username),
            password: Some(password.to_string()),
        }
    }
    /// The `auth:` block of a spec, or `None` for plaintext.
    fn yaml(&self) -> Option<serde_yaml::Value> {
        let user = self.username?;
        Some(
            serde_yaml::from_str(&format!(
                "{{mode: {}, username: {user}, tls: false}}",
                self.mode
            ))
            .unwrap(),
        )
    }
    /// Logweir's own client configuration for this row.
    fn config(&self) -> AuthConfig {
        match (self.mode, self.username, &self.password) {
            ("scramSha256", Some(u), Some(p)) => AuthConfig::ScramSha256 {
                username: u.into(),
                password: p.clone(),
                tls: false,
                tls_ca_file: None,
            },
            ("scramSha512", Some(u), Some(p)) => AuthConfig::ScramSha512 {
                username: u.into(),
                password: p.clone(),
                tls: false,
                tls_ca_file: None,
            },
            _ => AuthConfig::Plaintext,
        }
    }
    /// The plan a check is handed: the password by the NAME of the variable
    /// it is projected under, never by value.
    fn connection_plan(&self, bootstrap: &str) -> ConnectionPlan {
        ConnectionPlan {
            bootstrap_servers: vec![bootstrap.to_string()],
            auth_mode: self.mode.to_string(),
            username: self.username.map(str::to_string),
            password_env: self.password.as_ref().map(|_| PASSWORD_ENV.to_string()),
            tls: Some(false),
            ca_file: None,
            client_cert_file: None,
            client_key_file: None,
            principal: format!("User:{}", self.username.unwrap_or("ANONYMOUS")),
        }
    }
}

/// The variable a check's SASL password is projected under in these rows.
const PASSWORD_ENV: &str = "COMPAT_CHECK_PASSWORD";

fn client(bootstrap: &str, auth: &Auth) -> RdKafkaReader {
    RdKafkaReader::connect(&[bootstrap.to_string()], auth.config()).expect("connect is lazy")
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

/// The last `n` characters of `s`: enough of a failed run to say why, never
/// megabytes of engine retries in a panic message.
fn tail(s: &str, n: usize) -> String {
    let count = s.chars().count();
    s.chars().skip(count.saturating_sub(n)).collect()
}

fn nonce() -> String {
    format!("{}", chrono::Utc::now().timestamp_millis())
}

fn rfc3339(ms: i64) -> String {
    chrono::DateTime::from_timestamp_millis(ms)
        .unwrap()
        .to_rfc3339_opts(chrono::SecondsFormat::Secs, true)
}

/// Where a row writes what it observed.
fn evidence(row: &str, value: &Value) {
    let dir = demo_dir().join("compat");
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join(format!("{row}.json"));
    std::fs::write(&path, serde_json::to_vec_pretty(value).unwrap()).unwrap();
    eprintln!("[prod-01-2] {row}: evidence at {}", path.display());
}

// ============================================================ the oracle

/// `docker compose exec -T <service> …`, bounded, on THIS stack.
fn exec_in(service: &str, args: &[&str]) -> Output {
    harness::stack::ensure_coherent();
    let mut c = Command::new("docker");
    c.args([
        "compose",
        "-f",
        "e2e/compose/docker-compose.yml",
        "exec",
        "-T",
        service,
    ])
    .args(args)
    .current_dir(root());
    output_within(c, 120)
}

/// **The ground truth: what the endpoint serves, as the broker's own tool
/// reads it** (`kafka-broker-api-versions.sh` of the `apache/kafka` image,
/// run inside `kafka-broker-1` against the endpoint's in-network listener).
/// API key to `(min, max)`; an API the tool prints as `UNSUPPORTED` is
/// absent.
fn broker_api_versions(ep: Endpoint) -> BTreeMap<i16, (i16, i16)> {
    let out = exec_in(
        "kafka-broker-1",
        &[
            "/opt/kafka/bin/kafka-broker-api-versions.sh",
            "--bootstrap-server",
            ep.in_network,
        ],
    );
    assert!(
        out.status.success(),
        "{}: kafka-broker-api-versions.sh failed:\n{}",
        ep.label,
        tail(&text(&out), 2000)
    );
    let mut ranges = BTreeMap::new();
    for entry in String::from_utf8_lossy(&out.stdout).split(',') {
        // "\tProduce(0): 0 to 13 [usable: 13]" / "\tFoo(45): 0 [usable: 0]"
        let Some((head, rest)) = entry.split_once("): ") else {
            continue;
        };
        let Some(key) = head
            .rsplit_once('(')
            .and_then(|(_, k)| k.parse::<i16>().ok())
        else {
            continue;
        };
        let range = rest.split('[').next().unwrap_or("").trim();
        let parsed = match range.split_once(" to ") {
            Some((a, b)) => a.trim().parse().ok().zip(b.trim().parse().ok()),
            None => range.parse().ok().map(|v| (v, v)),
        };
        if let Some((min, max)) = parsed {
            ranges.insert(key, (min, max));
        }
    }
    assert!(
        ranges.len() >= 20 && ranges.contains_key(&3),
        "{}: the tool's output did not parse into API ranges: {ranges:?}",
        ep.label
    );
    ranges
}

fn spelled(range: Option<&(i16, i16)>) -> String {
    match range {
        Some((a, b)) if a == b => format!("v{a}"),
        Some((a, b)) => format!("v{a}-v{b}"),
        None => "not served".to_string(),
    }
}

// ============================================================ check run

/// The shipped `logweir check run` over one plan: the process, and the result
/// its frames decode to.
fn check_run(plan: &CheckPlan, password: Option<&str>) -> (Output, CheckResult) {
    let bytes = serde_json::to_vec(plan).expect("a plan serialises");
    let sha = logweir_core::ids::sha256_prefixed(&bytes);
    let dir = demo_dir().join("compat");
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join(format!("check-plan-{}.json", &sha[7..23]));
    std::fs::write(&path, &bytes).unwrap();
    let mut c = Command::new(bin());
    c.args(["check", "run", "--plan"])
        .arg(&path)
        .args([
            "--check-contract-version",
            &CHECK_CONTRACT_VERSION.to_string(),
        ])
        .env(
            "LOGWEIR_CHECK_CONTRACT_VERSION",
            CHECK_CONTRACT_VERSION.to_string(),
        )
        .env("LOGWEIR_CHECK_PLAN_SHA256", &sha)
        .env("LOGWEIR_CHECK_SUBJECT_UID", &plan.subject_uid)
        .env_remove(PASSWORD_ENV);
    if let Some(p) = password {
        c.env(PASSWORD_ENV, p);
    }
    let out = output_within(c, 180);
    assert_eq!(
        out.status.code(),
        Some(0),
        "`logweir check run` printed no result:\n{}",
        tail(&text(&out), 3000)
    );
    let mut decoder = Decoder::new();
    for line in String::from_utf8_lossy(&out.stdout).lines() {
        decoder.push_line(line).expect("a frame line decodes");
    }
    let relay = decoder
        .finish(&FrameExpectations {
            plan_sha256: sha,
            subject_uid: plan.subject_uid.clone(),
        })
        .expect("the relay verifies");
    let result = relay
        .result()
        .expect("a result stream")
        .expect("the result document parses");
    (out, result)
}

/// A readiness plan about `operation` over one connection, listing
/// `capabilities`. No destination, no signer: a capability row is about the
/// endpoint.
fn capability_plan(
    operation: CheckOperation,
    connection: ConnectionPlan,
    topics: &[&str],
    capabilities: &[CheckId],
) -> CheckPlan {
    CheckPlan {
        contract: CHECK_PLAN_CONTRACT.to_string(),
        contract_version: CHECK_CONTRACT_VERSION,
        subject_uid: "prod-01-2-compat".into(),
        timeout_seconds: 60,
        policy_digest: None,
        request: CheckRequest::OperationReadiness(Box::new(OperationReadinessRequest {
            operation,
            connection,
            destination: None,
            roles: Vec::new(),
            topics: topics.iter().map(|t| (*t).to_string()).collect(),
            signer_path: None,
            write_probe: false,
            evidence_write: None,
            evidence_read: None,
            skip_checks: Vec::new(),
            capability_checks: capabilities.to_vec(),
        })),
    }
}

fn row(result: &CheckResult, id: CheckId) -> CheckOutcome {
    result
        .checks
        .iter()
        .find(|c| c.id == id)
        .unwrap_or_else(|| {
            panic!(
                "no `{id}` row among {:?}",
                result.checks.iter().map(|c| c.id).collect::<Vec<_>>()
            )
        })
        .clone()
}

/// The capability rows of a BACKUP from `ep` and of a RESTORE into it, each
/// held to the broker's own answer.
///
/// Returns `(the rows as evidence, whether a restore can run)`: the second is
/// the ground truth "the endpoint serves Produce v8", which the caller then
/// requires the real restore to agree with.
fn capability_checks(ep: Endpoint, auth: &Auth, bootstrap: &str, topic: &str) -> (Value, bool) {
    let truth = broker_api_versions(ep);
    let label = ep.label;
    let sasl = auth.username.is_some();
    let password = auth.password.as_deref();

    // --- a backup from this endpoint ----------------------------------------
    let (_, backup) = check_run(
        &capability_plan(
            CheckOperation::Backup,
            auth.connection_plan(bootstrap),
            &[topic],
            capability_checks_for(CheckOperation::Backup),
        ),
        password,
    );
    assert_eq!(
        row(&backup, CheckId::ConnectionAuthenticated).state,
        CheckState::Ready,
        "{label}: {:?}",
        row(&backup, CheckId::ConnectionAuthenticated)
    );

    // connection.engineProtocol: Metadata v9, ListOffsets v5, Fetch v11,
    // DescribeConfigs v1 (+ SaslHandshake v1, SaslAuthenticate v2).
    let mut capture: Vec<(&str, i16, i16)> = vec![
        ("Metadata", 3, 9),
        ("ListOffsets", 2, 5),
        ("Fetch", 1, 11),
        ("DescribeConfigs", 32, 1),
    ];
    if sasl {
        capture.extend([("SaslHandshake", 17, 1), ("SaslAuthenticate", 36, 2)]);
    }
    let serves = |key: i16, version: i16| {
        truth
            .get(&key)
            .is_some_and(|(min, max)| (*min..=*max).contains(&version))
    };
    let capture_ok = capture.iter().all(|(_, key, v)| serves(*key, *v));
    let engine = row(&backup, CheckId::ConnectionEngineProtocol);
    assert_eq!(
        engine.gating,
        Gating::Blocking,
        "{label}: an endpoint the engine cannot read is not a backup source"
    );
    assert_eq!(
        engine.state,
        if capture_ok {
            CheckState::Ready
        } else {
            CheckState::NotReady
        },
        "{label}: connection.engineProtocol must agree with the broker's own answer \
         ({truth:?}): {engine:?}"
    );
    assert_eq!(
        engine.facts.get("engineRequests").map(String::as_str),
        Some(
            capture
                .iter()
                .map(|(api, _, v)| format!("{api} v{v}"))
                .collect::<Vec<_>>()
                .join(", ")
                .as_str()
        ),
        "{label}: the row names what the engine sends"
    );
    if capture_ok {
        // The message quotes the endpoint's own range for each request.
        for (api, key, v) in &capture {
            let want = format!("{api} v{v} (served {})", spelled(truth.get(key)));
            assert!(
                engine.message.contains(&want),
                "{label}: `{want}` is not in: {}",
                engine.message
            );
        }
    }

    // connection.topicConfigsReadable: the plaintext and SASL principals of
    // these fixtures may all DescribeConfigs.
    let configs = row(&backup, CheckId::ConnectionTopicConfigsReadable);
    assert_eq!(
        (configs.state, configs.code, configs.gating),
        (
            CheckState::Ready,
            CheckCode::TopicConfigsReadable,
            Gating::Advisory
        ),
        "{label}: {configs:?}"
    );

    // connection.groupTypes: ListGroups v5, by the broker's own answer.
    let typed = serves(16, 5);
    let groups = row(&backup, CheckId::ConnectionGroupTypes);
    assert_eq!(
        (groups.state, groups.code, groups.gating),
        if typed {
            (
                CheckState::Ready,
                CheckCode::GroupTypesListed,
                Gating::Advisory,
            )
        } else {
            (
                CheckState::NotReady,
                CheckCode::GroupTypesNotListed,
                Gating::Advisory,
            )
        },
        "{label}: connection.groupTypes must agree with the broker's own ListGroups range \
         ({}): {groups:?}",
        spelled(truth.get(&16))
    );
    assert!(
        groups
            .message
            .contains(&format!("ListGroups {}", spelled(truth.get(&16)))),
        "{label}: the row quotes the endpoint's range: {}",
        groups.message
    );
    if !typed {
        assert!(
            groups.remedy.contains("ListGroups v5") && groups.remedy.contains("never as offset 0"),
            "{label}: the fallback is stated: {}",
            groups.remedy
        );
    }

    // --- a restore into this endpoint ---------------------------------------
    let (_, restore) = check_run(
        &capability_plan(
            CheckOperation::Restore,
            auth.connection_plan(bootstrap),
            &[],
            capability_checks_for(CheckOperation::Restore),
        ),
        password,
    );
    let replay_ok = serves(3, 9) && serves(0, 8) && (!sasl || (serves(17, 1) && serves(36, 2)));
    let target = row(&restore, CheckId::TargetEngineProtocol);
    assert_eq!(
        (target.state, target.code, target.gating),
        if replay_ok {
            (
                CheckState::Ready,
                CheckCode::EngineProtocolSupported,
                Gating::Blocking,
            )
        } else {
            (
                CheckState::NotReady,
                CheckCode::EngineProtocolUnsupported,
                Gating::Blocking,
            )
        },
        "{label}: target.engineProtocol must agree with the broker's own Produce range ({}): \
         {target:?}",
        spelled(truth.get(&0))
    );
    if !replay_ok {
        let want = format!(
            "Produce v8 and this endpoint serves Produce {}",
            spelled(truth.get(&0))
        );
        assert!(
            target.message.contains(&want) && target.message.contains("never negotiates"),
            "{label}: `{want}` is not in: {}",
            target.message
        );
        assert!(
            target.remedy.contains("can still be a backup source")
                && target.remedy.contains("docs/support-matrix.md")
                && !target.remedy.ends_with('…'),
            "{label}: the fallback is actionable and whole: {}",
            target.remedy
        );
    }
    let seen = json!({
        "broker_tool": {
            "Produce": spelled(truth.get(&0)), "Fetch": spelled(truth.get(&1)),
            "ListOffsets": spelled(truth.get(&2)), "Metadata": spelled(truth.get(&3)),
            "ListGroups": spelled(truth.get(&16)), "DescribeConfigs": spelled(truth.get(&32)),
            "SaslHandshake": spelled(truth.get(&17)), "SaslAuthenticate": spelled(truth.get(&36)),
        },
        "auth_mode": auth.mode,
        "connection.engineProtocol": engine,
        "connection.topicConfigsReadable": configs,
        "connection.groupTypes": groups,
        "target.engineProtocol": target,
    });
    eprintln!(
        "[prod-01-2] {label} ({}): engineProtocol backup {:?} / restore {:?}; groupTypes {:?}; \
         Produce {}, ListGroups {}",
        auth.mode,
        engine.state,
        target.state,
        groups.state,
        spelled(truth.get(&0)),
        spelled(truth.get(&16))
    );
    (seen, replay_ok)
}

// ============================================================ backup / restore

/// `n` records into `topic`, three partitions, with explicit CreateTime: the
/// window a drill selects is then a fact of this run. Returns `(min, max)`.
fn produce(bootstrap: &str, auth: &Auth, topic: &str, n: usize) -> (i64, i64) {
    use rdkafka::producer::{BaseProducer, BaseRecord, Producer};
    let mut cfg = rdkafka::config::ClientConfig::new();
    cfg.set("bootstrap.servers", bootstrap)
        .set("message.timeout.ms", "15000")
        .set("acks", "all");
    if let (Some(user), Some(password)) = (auth.username, &auth.password) {
        cfg.set("security.protocol", "SASL_PLAINTEXT")
            .set(
                "sasl.mechanism",
                if auth.mode == "scramSha256" {
                    "SCRAM-SHA-256"
                } else {
                    "SCRAM-SHA-512"
                },
            )
            .set("sasl.username", user)
            .set("sasl.password", password);
    }
    let producer: BaseProducer = cfg.create().expect("a producer");
    let base = chrono::Utc::now().timestamp_millis() - 120_000;
    for i in 0..n {
        let ts = base + (i as i64) * 100;
        let key = format!("compat-{i:03}");
        let payload = format!("{{\"compat-row\":{i},\"ts\":{ts}}}");
        producer
            .send(
                BaseRecord::to(topic)
                    .partition((i % 3) as i32)
                    .key(&key)
                    .payload(&payload)
                    .timestamp(ts),
            )
            .map_err(|(e, _)| e)
            .expect("enqueue");
    }
    producer
        .flush(Duration::from_secs(20))
        .expect("the endpoint accepted the records");
    (base, base + (n as i64 - 1) * 100)
}

/// Where a backup's archive and its evidence live.
#[derive(Clone, Debug)]
struct Storage {
    endpoint: String,
    bucket: String,
}

impl Storage {
    fn minio() -> Self {
        Self {
            endpoint: s3_endpoint(),
            bucket: ARCHIVE_BUCKET.to_string(),
        }
    }
}

fn backup_spec(
    bootstrap: &str,
    auth: &Auth,
    topic: &str,
    backup_id: &str,
    storage: &Storage,
) -> PathBuf {
    let mut v: serde_yaml::Value = serde_yaml::from_str(&format!(
        "backup_id: {backup_id}\n\
         source:\n\
         \x20 bootstrap_servers: [\"{bootstrap}\"]\n\
         \x20 topics: [{topic}]\n\
         storage:\n\
         \x20 backend: s3\n\
         \x20 bucket: {}\n\
         \x20 prefix: {backup_id}\n\
         \x20 region: us-east-1\n\
         \x20 endpoint: {}\n\
         \x20 path_style: true\n\
         \x20 allow_http: true\n",
        storage.bucket, storage.endpoint
    ))
    .unwrap();
    if let Some(a) = auth.yaml() {
        v["source"]["auth"] = a;
    }
    let p = demo_dir().join(format!("compat-{backup_id}.yaml"));
    std::fs::write(&p, serde_yaml::to_string(&v).unwrap()).unwrap();
    p
}

/// `logweir backup run` with the stack's engine. The allowlist names a
/// cluster that is not the source (GC18(c) rail 4).
fn backup(spec: &Path, receipt: &Path, auth: &Auth, groups: &[&str]) -> Output {
    let allow = demo_dir().join("compat-backup-allowed.json");
    std::fs::write(
        &allow,
        "{\"allowed_cluster_ids\": [\"SCRATCH-CLUSTER-NOT-THE-SOURCE\"]}\n",
    )
    .unwrap();
    let mut c = Command::new(bin());
    c.args(["backup", "run", "--spec"])
        .arg(spec)
        .arg("--allowed-clusters")
        .arg(&allow)
        .arg("--signing-key")
        .arg(root().join("e2e/fixtures/signed/signing.pem"))
        .arg("--receipt-out")
        .arg(receipt)
        .env_remove("LOGWEIR_SOURCE_PASSWORD")
        .env("AWS_ACCESS_KEY_ID", "minioadmin")
        .env("AWS_SECRET_ACCESS_KEY", "minioadmin")
        .env("AWS_REGION", "us-east-1")
        .env("LOGWEIR_ENGINE_BIN", engine_bin())
        .env("LOGWEIR_ENGINE_VERSION", engine_version())
        .env("LOGWEIR_ENGINE_DIGEST", engine_digest())
        .env("LOGWEIR_E2E_ENGINE_MOUNT", engine_mount())
        .env("TMPDIR", engine_mount());
    for g in groups {
        c.args(["--consumer-group", g]);
    }
    if let Some(p) = &auth.password {
        c.env("LOGWEIR_SOURCE_PASSWORD", p);
    }
    output_within(c, 900)
}

/// Both readers over a receipt the run signed.
fn verify_receipt(receipt: &Path) -> (Option<i32>, Option<i32>) {
    let sig = receipt.with_extension("sig");
    let pubkey = root().join("e2e/fixtures/signed/public.pem");
    let mut rust = Command::new(bin());
    rust.args([
        "drill",
        "verify",
        "--payload-type",
        "backup-receipt",
        "--scorecard",
    ])
    .arg(receipt)
    .arg("--signature")
    .arg(&sig)
    .arg("--public-key")
    .arg(&pubkey);
    let mut py = Command::new(auditor_python());
    py.arg(root().join("docs/verify_scorecard.py"))
        .args(["--payload-type", "backup-receipt"])
        .arg(receipt)
        .arg(&sig)
        .arg(&pubkey);
    (
        output_within(rust, 120).status.code(),
        output_within(py, 120).status.code(),
    )
}

/// Delete every `drill-` topic on an endpoint (the harness's own sweep
/// addresses the default broker), with the scratch-prefix fence the drill has.
fn sweep_drill_topics(bootstrap: &str, auth: &Auth) -> Vec<String> {
    let r = client(bootstrap, auth)
        .with_scratch_prefix(SCRATCH_PREFIX)
        .expect("`drill-` is a scratch namespace");
    let scratch = |r: &RdKafkaReader| -> Vec<String> {
        ClusterReader::list_topics(r)
            .expect("list")
            .into_iter()
            .map(|t| t.name)
            .filter(|n| n.starts_with(SCRATCH_PREFIX))
            .collect()
    };
    let found = scratch(&r);
    if !found.is_empty() {
        TopicDeleter::delete_topics(&r, &found).unwrap();
        for _ in 0..60 {
            if scratch(&r).is_empty() {
                break;
            }
            std::thread::sleep(Duration::from_millis(250));
        }
    }
    found
}

/// The drill spec: the archive `backup_id` wrote, restored into `bootstrap`'s
/// scratch namespace.
fn drill_spec(
    bootstrap: &str,
    auth: &Auth,
    topic: &str,
    backup_id: &str,
    window: (i64, i64),
    storage: &Storage,
) -> serde_yaml::Value {
    let mut v: serde_yaml::Value =
        serde_yaml::from_str(&std::fs::read_to_string(root().join("examples/drill.yaml")).unwrap())
            .unwrap();
    rebind_addresses(&mut v);
    v["source"]["storage"]["prefix"] = backup_id.into();
    v["source"]["storage"]["bucket"] = storage.bucket.clone().into();
    v["source"]["storage"]["endpoint"] = storage.endpoint.clone().into();
    v["source"]["topics"] = serde_yaml::from_str(&format!("[{topic}]")).unwrap();
    v["target"]["bootstrap_servers"] = serde_yaml::from_str(&format!("[\"{bootstrap}\"]")).unwrap();
    if let Some(a) = auth.yaml() {
        v["target"]["auth"] = a;
    }
    v["target"]["teardown"] = "delete".into();
    // The window starts before every record the topic holds (the reason is
    // `auth_modes.rs::drill_spec`'s: a topic that accumulates batches across
    // runs must have its head inside the window).
    v["sample"]["window_start"] = rfc3339(window.0 - 30 * 86_400_000).into();
    v["sample"]["window_end"] = rfc3339(window.1 + 60_000).into();
    v["evidence"]["endpoint"] = storage.endpoint.clone().into();
    v
}

fn allowlist_for(cluster_id: &str) -> PathBuf {
    let p = demo_dir().join("compat-allowed-clusters.json");
    std::fs::write(
        &p,
        serde_json::to_vec_pretty(&json!({
            "allowed_cluster_ids": [cluster_id],
            "source_cluster_id": null,
        }))
        .unwrap(),
    )
    .unwrap();
    p
}

/// `logweir cluster-probe`: interface I14's two lines and its exit code.
fn probe(bootstrap: &str, auth: &Auth) -> (Option<i32>, String, String) {
    let mut c = Command::new(bin());
    c.args(["cluster-probe", "--bootstrap", bootstrap, "--auth-mode"])
        .arg(auth.mode)
        .env_remove("LOGWEIR_SOURCE_PASSWORD");
    if let Some(u) = auth.username {
        c.args(["--username", u]);
    }
    if let Some(p) = &auth.password {
        c.env("LOGWEIR_SOURCE_PASSWORD", p);
    }
    let out = output_within(c, 120);
    let stdout = String::from_utf8_lossy(&out.stdout).into_owned();
    let line = |key: &str| {
        stdout
            .lines()
            .find_map(|l| l.strip_prefix(key))
            .unwrap_or("")
            .to_string()
    };
    (out.status.code(), line("cluster-id="), line("reachable="))
}

/// A simple classic consumer group with one committed position, made by a
/// commit from a non-member (PROD-04.0a's route): a group for the backup to
/// be asked about.
fn commit_a_group(bootstrap: &str, auth: &Auth, topic: &str, group: &str) -> Result<(), String> {
    client(bootstrap, auth)
        .commit_positions(
            group,
            &[(
                TopicPartition::new(topic, 0),
                CommittedPosition {
                    offset: 1,
                    leader_epoch: None,
                    metadata: None,
                },
            )],
        )
        .map_err(|e| e.to_string())
}

/// What one generic row measured.
struct Generic {
    seen: Value,
    replay_ok: bool,
    drill_exit: Option<i32>,
    scorecard: Option<Value>,
    drill_output: String,
}

/// **THE GENERIC ROW.** On one endpoint, in order: the probe; the capability
/// checks (held to the broker's own tool); a backup that also asks for one
/// consumer group, its receipt verified by both readers; and a drill that
/// restores the archive into the same endpoint and verifies it.
///
/// What it REQUIRES of every endpoint: the probe names a cluster id; the
/// backup exits 0 with a receipt both readers accept, a topic id for the
/// topic, `captured` configuration coverage, and for the selected group
/// exactly the outcome the endpoint's ListGroups range implies (`captured` as
/// classic from v5, `excluded: GroupTypeNotCaptured` below it; never offset 0,
/// never absent). The restore's outcome is returned for the caller to judge
/// against `replay_ok`.
fn generic_row(ep: Endpoint, topic: &str) -> Generic {
    let label = ep.label;
    let bootstrap = (ep.bootstrap)();
    let auth = Auth::plaintext();

    // --- probe --------------------------------------------------------------
    let (exit, probed_id, reachable) = probe(&bootstrap, &auth);
    assert_eq!(
        (exit, reachable.as_str()),
        (Some(0), "true"),
        "{label}: cluster-probe"
    );
    let cluster_id = client(&bootstrap, &auth)
        .cluster_id()
        .unwrap_or_else(|e| panic!("{label}: Logweir's client over {bootstrap}: {e}"));
    assert_eq!(
        probed_id, cluster_id,
        "{label}: the probe's id is the cluster's"
    );
    assert!(!cluster_id.is_empty(), "{label}: a cluster id");

    // --- capability checks --------------------------------------------------
    let (capabilities, replay_ok) = capability_checks(ep, &auth, &bootstrap, topic);
    let typed = broker_api_versions(ep)
        .get(&16)
        .is_some_and(|(_, max)| *max >= 5);

    // --- backup -------------------------------------------------------------
    let window = produce(&bootstrap, &auth, topic, 30);
    let group = format!("compat-g-{}", nonce());
    let group_made = commit_a_group(&bootstrap, &auth, topic, &group);
    let backup_id = format!("compat-{label}-{}", nonce());
    let storage = Storage::minio();
    let spec = backup_spec(&bootstrap, &auth, topic, &backup_id, &storage);
    let receipt = demo_dir().join(format!("{backup_id}.receipt.json"));
    let selected: Vec<&str> = if group_made.is_ok() {
        vec![group.as_str()]
    } else {
        Vec::new()
    };
    let out = backup(&spec, &receipt, &auth, &selected);
    assert_eq!(
        out.status.code(),
        Some(0),
        "{label}: backup over {bootstrap} did not exit 0:\n{}",
        tail(&text(&out), 4000)
    );
    let doc: Value = serde_json::from_str(&std::fs::read_to_string(&receipt).expect("receipt"))
        .expect("the receipt is JSON");
    assert_eq!(doc["source"]["cluster_id"], cluster_id, "{label}");
    assert_eq!(
        verify_receipt(&receipt),
        (Some(0), Some(0)),
        "{label}: both readers verify the receipt"
    );
    // The topic's identity: an ID from DescribeTopics, the same before and
    // after the engine. Every endpoint of this suite has topic IDs; one that
    // had none would read `null` with a reason, which this would name.
    let generation = &doc["generations"][topic];
    assert!(
        generation["topic_id"]
            .as_str()
            .is_some_and(|id| !id.is_empty())
            && generation["topic_id"] == generation["topic_id_after"],
        "{label}: the topic's ID is recorded, and did not change during the capture: {generation}"
    );
    // The topic's configuration: read and recorded.
    assert_eq!(
        doc["config_coverage"][topic]["coverage"], "captured",
        "{label}: {}",
        doc["config_coverage"]
    );
    assert_eq!(
        doc["config_coverage"][topic]["timestamp_type"]["value"], "CreateTime",
        "{label}: the effective timestamp type is recorded from the topic's own read"
    );
    // The selected group: captured where the endpoint can type it, and
    // excluded with the reason where it cannot. Never absent.
    let group_outcome = match &group_made {
        Err(why) => json!({"not_made": why}),
        Ok(()) => {
            let g = &doc["consumer_positions"]["groups"][&group];
            if typed {
                assert_eq!(
                    (g["outcome"].as_str(), g["group_type"].as_str()),
                    (Some("captured"), Some("classic")),
                    "{label}: an endpoint that serves ListGroups v5 types the group: {g}"
                );
            } else {
                assert_eq!(
                    (g["outcome"].as_str(), g["reason"].as_str()),
                    (Some("excluded"), Some("GroupTypeNotCaptured")),
                    "{label}: a group this endpoint cannot type is NOT RECORDED as captured, \
                     with the reason: {g}"
                );
                assert!(
                    g.get("counts").is_none() && g.get("state").is_none(),
                    "{label}: an excluded group carries no position evidence: {g}"
                );
            }
            g.clone()
        }
    };

    // --- restore and verify -------------------------------------------------
    sweep_drill_topics(&bootstrap, &auth);
    let dspec = drill_spec(&bootstrap, &auth, topic, &backup_id, window, &storage);
    let allow = allowlist_for(&cluster_id);
    let mut o = RunOpts::new(&dspec);
    o.allowlist = Some(&allow);
    let r = run_with(o);
    let drill_output = format!("{}\n{}", r.out.stdout_utf8(), r.out.stderr_utf8());
    let scorecard = r.scorecard.exists().then(|| read_scorecard(&r));
    if let Some(sc) = &scorecard {
        if sc["outcome"] == "pass" {
            assert!(
                logweir_verify(&r).success(),
                "{label}: logweir drill verify"
            );
            assert!(python_verify(&r).success(), "{label}: verify_scorecard.py");
            assert_eq!(sc["target"]["cluster_id"], cluster_id, "{label}");
        }
    }
    let left = sweep_drill_topics(&bootstrap, &auth);
    let seen = json!({
        "endpoint": label,
        "bootstrap": bootstrap,
        "cluster_id": cluster_id,
        "engine": {"version": engine_version(), "digest": engine_digest()},
        "probe": {"exit": exit, "cluster_id": probed_id, "reachable": reachable},
        "capabilities": capabilities,
        "backup": {
            "exit": out.status.code(),
            "backup_id": backup_id,
            "receipt_format": doc["format_version"],
            "receipt_verified_by_both_readers": true,
            "records": doc["records"],
            "generation": generation,
            "config_coverage": doc["config_coverage"][topic],
            "configuration_entries": doc["topic_configuration"][topic]["entries"]
                .as_object().map(|o| o.keys().cloned().collect::<Vec<_>>()),
            "group": group_outcome,
        },
        "restore": {
            "exit": r.out.status.code(),
            "outcome": scorecard.as_ref().map(|s| s["outcome"].clone()),
            "phases": scorecard.as_ref().map(phase_outcomes),
            "scorecard_format": scorecard.as_ref().map(|s| s["format_version"].clone()),
            "scratch_topics_left_after_the_run": left,
        },
    });
    Generic {
        seen,
        replay_ok,
        drill_exit: r.out.status.code(),
        scorecard,
        drill_output,
    }
}

/// The restore half of a generic row on an endpoint that serves what the
/// engine sends: exit 0, `pass`, phases 0, 3, 6 and 7 `ok`.
fn assert_restored(label: &str, g: &Generic) {
    assert!(
        g.replay_ok,
        "{label}: this row is for an endpoint that serves Produce v8"
    );
    assert_eq!(
        g.drill_exit,
        Some(0),
        "{label}: the drill did not exit 0:\n{}",
        tail(&g.drill_output, 4000)
    );
    let sc = g.scorecard.as_ref().expect("a signed scorecard");
    assert_eq!(sc["outcome"], "pass", "{label}");
    let phases = phase_outcomes(sc);
    for n in [0, 3, 6, 7] {
        assert_eq!(
            phases.get(&n).map(String::as_str),
            Some("ok"),
            "{label}: phase {n} — {phases:?}"
        );
    }
}

// ============================================================ rows

/// **The capability checks, as CI runs the stack.** The shipped
/// `logweir check run` against the default broker's plaintext listener and
/// its SCRAM-SHA-512 one: every capability row is answered from the broker's
/// ApiVersions as read on a real, authenticated connection, and agrees with
/// the broker's own tool whichever line the stack runs (3.7.1 in CI: no group
/// types; 3.9 and 4.x: typed).
///
/// CONTROL, in the same row: the same plan listing NO capability row gets
/// none, which is what keeps an older controller from receiving an id it
/// cannot read.
///
/// Negative controls live beside it: `check_cli.rs`'s unit rows show each
/// finding on an answer that lacks the capability, and
/// `redpanda_backs_up_and_refuses_a_restore_before_it_starts` shows two of
/// them on a real endpoint.
#[test]
fn the_default_broker_answers_every_capability_check() {
    let plaintext = Auth::plaintext();
    let (plain, plain_replay) = capability_checks(KAFKA, &plaintext, &bootstrap(), "orders");
    let scram = Auth::scram("scramSha512", SCRAM_USER, SCRAM_PASSWORD);
    let (sasl, sasl_replay) = capability_checks(KAFKA, &scram, &bootstrap_sasl(), "orders");
    assert!(
        plain_replay && sasl_replay,
        "every Apache Kafka line of this repository serves what a restore sends"
    );
    // On the SASL listener the row asks for the two SASL requests as well.
    assert!(
        sasl["connection.engineProtocol"]["facts"]["engineRequests"]
            .as_str()
            .is_some_and(|r| r.ends_with("SaslHandshake v1, SaslAuthenticate v2")),
        "{}",
        sasl["connection.engineProtocol"]
    );

    // CONTROL: no list, no capability row.
    let (_, unlisted) = check_run(
        &capability_plan(
            CheckOperation::Backup,
            plaintext.connection_plan(&bootstrap()),
            &["orders"],
            &[],
        ),
        None,
    );
    let ids: Vec<CheckId> = unlisted.checks.iter().map(|c| c.id).collect();
    assert!(
        ids.iter().all(|id| !id.is_capability()),
        "a plan that lists no capability row got one: {ids:?}"
    );
    assert!(
        ids.contains(&CheckId::ConnectionTopicsDescribable),
        "{ids:?}"
    );

    evidence(
        "the_default_broker_answers_every_capability_check",
        &json!({"plaintext": plain, "scramSha512": sasl, "unlisted_ids": ids}),
    );
}

/// The generic row on the stack's Apache Kafka broker, over a topic of its
/// own (the default broker's `orders` belongs to the drill suites). Run it on
/// each line with `stack-env.sh --kafka LINE`.
#[test]
#[ignore = "a whole backup and drill on its own topic; run it per broker line (module doc)"]
fn the_default_broker_backs_up_restores_and_verifies() {
    let topic = format!("compat-orders-{}", nonce());
    create_topic(&topic, 3);
    let g = generic_row(KAFKA, &topic);
    let version = String::from_utf8_lossy(
        &exec_in(
            "kafka-broker-1",
            &["/opt/kafka/bin/kafka-topics.sh", "--version"],
        )
        .stdout,
    )
    .trim()
    .to_string();
    let mut seen = g.seen.clone();
    seen["broker_version"] = json!(version);
    evidence("the_default_broker_backs_up_restores_and_verifies", &seen);
    let _ = kafka_topics(&[
        "--bootstrap-server",
        "kafka-broker-1:9094",
        "--delete",
        "--topic",
        &topic,
    ]);
    assert_restored("kafka", &g);
    eprintln!("[prod-01-2] kafka ({version}): probe, backup, restore and verify pass");
}

/// The generic row on Confluent Platform 8.3.2 (`cp-kafka`). It is required
/// to behave as Apache Kafka does: every capability present, the group typed
/// and captured, the restore signed `pass`.
#[test]
#[ignore = "needs the stack's `confluent` profile; see the module doc"]
fn confluent_platform_backs_up_restores_and_verifies() {
    let g = generic_row(CONFLUENT, "orders");
    let version =
        String::from_utf8_lossy(&exec_in("kafka-cp", &["kafka-topics", "--version"]).stdout)
            .trim()
            .to_string();
    assert!(version.contains("-ccs"), "Confluent's build: {version}");
    // Its broker resource reports the record-timestamp bound, as Apache
    // Kafka's does: the control for the Redpanda row's finding.
    let broker = dial(&(CONFLUENT.bootstrap)())
        .broker_configs()
        .expect("broker configs");
    assert!(
        broker.contains_key("log.message.timestamp.before.max.ms")
            && broker.contains_key("log.message.timestamp.type"),
        "confluent: the two keys phase 0 and target.timestampBound read are reported"
    );
    let mut seen = g.seen.clone();
    seen["broker_version"] = json!(version);
    seen["broker_config_keys"] = json!(broker.len());
    evidence("confluent_platform_backs_up_restores_and_verifies", &seen);
    assert_eq!(
        seen["capabilities"]["connection.groupTypes"]["code"], "GroupTypesListed",
        "Confluent Platform 8.3 types groups"
    );
    assert_restored("confluent", &g);
    eprintln!("[prod-01-2] confluent ({version}): probe, backup, restore and verify pass");
}

/// A check-style probe over an endpoint's plaintext listener.
fn dial(bootstrap: &str) -> logweir_kafka::inventory::KafkaInventory {
    logweir::check::kafka::dial(
        &Auth::plaintext().connection_plan(bootstrap),
        Duration::from_secs(30),
    )
    .unwrap_or_else(|e| panic!("dial {bootstrap}: {e}"))
}

/// **Redpanda v26.2.4: what works, what does not, and that Logweir says so
/// before a restore starts.**
///
/// * The probe and the backup work, and the receipt verifies in both readers.
/// * Redpanda serves ListGroups below v5: `connection.groupTypes` is
///   `notReady` (advisory), and the receipt records the selected group as
///   `excluded: GroupTypeNotCaptured`, never as captured.
/// * Its broker resource answers a handful of configuration keys and neither
///   record-timestamp bound nor the broker timestamp type: the premise of
///   `target.timestampBound`'s `TimestampBoundNotReported` and of phase 0 no
///   longer recording `CreateTime` for it.
/// * It serves Produce up to v7 and the engine sends v8: `target.engineProtocol`
///   is `notReady`, blocking, naming both, BEFORE any restore; and the restore
///   it says cannot run is then run, and fails with exit 1 and no scorecard.
///
/// CONTROL: `confluent_platform_backs_up_restores_and_verifies` and
/// `the_default_broker_backs_up_restores_and_verifies`, the same generic row
/// on endpoints where the same checks are `ready` and the restore passes.
#[test]
#[ignore = "needs the stack's `redpanda` profile; see the module doc"]
fn redpanda_backs_up_and_refuses_a_restore_before_it_starts() {
    let g = generic_row(REDPANDA, "orders");
    let mut seen = g.seen.clone();

    assert!(
        seen["cluster_id"]
            .as_str()
            .is_some_and(|id| id.starts_with("redpanda.")),
        "Redpanda names its own cluster id: {}",
        seen["cluster_id"]
    );
    // Group types: the measured gap, on both sides of the backup.
    assert_eq!(
        seen["capabilities"]["connection.groupTypes"]["code"], "GroupTypesNotListed",
        "{}",
        seen["capabilities"]["connection.groupTypes"]
    );
    assert_eq!(
        seen["backup"]["group"]["reason"], "GroupTypeNotCaptured",
        "{}",
        seen["backup"]["group"]
    );

    // The broker resource: what it reports, and what it does not.
    let broker = dial(&(REDPANDA.bootstrap)())
        .broker_configs()
        .expect("Redpanda answers DescribeConfigs on its broker");
    assert!(
        !broker.is_empty(),
        "the answer is not empty: an empty one would be T13's refused read"
    );
    for key in [
        "log.message.timestamp.before.max.ms",
        "log.message.timestamp.difference.max.ms",
        "log.message.timestamp.type",
    ] {
        assert!(
            !broker.contains_key(key),
            "Redpanda's broker resource now reports `{key}`: the record's row about it is stale"
        );
    }
    seen["broker_config_keys_reported"] = json!(broker.keys().collect::<Vec<_>>());

    // The restore: predicted impossible, and impossible.
    assert!(
        !g.replay_ok,
        "Redpanda now serves Produce v8: this row's premise changed, re-measure the matrix"
    );
    assert_eq!(
        seen["capabilities"]["target.engineProtocol"]["code"],
        "EngineProtocolUnsupported"
    );
    assert_eq!(
        g.drill_exit,
        Some(1),
        "the restore the check refuses must not succeed:\n{}",
        tail(&g.drill_output, 3000)
    );
    assert!(
        g.scorecard.is_none(),
        "a restore that could not write signs NOTHING: {:?}",
        g.scorecard
    );
    // The engine's own account names no version: this is why the check does.
    let engine_said = g
        .drill_output
        .lines()
        .rev()
        .find(|l| l.contains("Restore completed with") || l.contains("early eof"))
        .unwrap_or("")
        .to_string();
    assert!(
        !engine_said.is_empty() && !engine_said.contains("v8") && !engine_said.contains("version"),
        "the engine reports a transport error and no version: {engine_said}"
    );
    seen["restore"]["engine_said"] = json!(tail(&engine_said, 400));
    evidence(
        "redpanda_backs_up_and_refuses_a_restore_before_it_starts",
        &seen,
    );
    eprintln!(
        "[prod-01-2] redpanda: backup ok, group {}, target.engineProtocol notReady, restore exit 1 \
         with no scorecard",
        seen["backup"]["group"]["reason"]
    );
}

/// Redpanda's own SCRAM implementation, both mechanisms, both clients: a
/// backup over the SASL listener as `logweir` (SCRAM-SHA-256) and as
/// `logweir512` (SCRAM-SHA-512) exits 0, and its receipt names the mode and
/// the principal and verifies in both readers. The capability check on that
/// listener asks for the SASL requests too.
///
/// NEGATIVE CONTROLS: a wrong password is refused by both Logweir's client
/// and the backup, with no receipt; and a user asked for the mechanism it
/// does not hold is refused.
#[test]
#[ignore = "needs the stack's `redpanda` profile; see the module doc"]
fn redpanda_authenticates_both_scram_mechanisms() {
    let bootstrap = bootstrap_redpanda_sasl();
    let mut seen = serde_json::Map::new();
    for (mode, user) in [("scramSha256", "logweir"), ("scramSha512", "logweir512")] {
        let auth = Auth::scram(mode, user, SCRAM_PASSWORD);
        let cluster_id = client(&bootstrap, &auth)
            .cluster_id()
            .unwrap_or_else(|e| panic!("{mode}: Logweir's client: {e}"));
        let (capabilities, _) = capability_checks(REDPANDA, &auth, &bootstrap, "orders");
        produce(&bootstrap, &auth, "orders", 12);
        let backup_id = format!("compat-redpanda-{}-{}", mode.to_lowercase(), nonce());
        let storage = Storage::minio();
        let spec = backup_spec(&bootstrap, &auth, "orders", &backup_id, &storage);
        let receipt = demo_dir().join(format!("{backup_id}.receipt.json"));
        let out = backup(&spec, &receipt, &auth, &[]);
        let printed = text(&out);
        assert_eq!(
            out.status.code(),
            Some(0),
            "{mode}: backup over Redpanda's SASL listener:\n{}",
            tail(&printed, 4000)
        );
        let receipt_text = std::fs::read_to_string(&receipt).expect("receipt");
        let doc: Value = serde_json::from_str(&receipt_text).unwrap();
        assert_eq!(doc["source"]["auth"]["mode"], mode);
        assert_eq!(doc["source"]["auth"]["username"], user);
        assert_eq!(doc["source"]["cluster_id"], cluster_id);
        assert_eq!(verify_receipt(&receipt), (Some(0), Some(0)), "{mode}");
        for (what, haystack) in [
            ("the backup's output", &printed),
            ("the receipt", &receipt_text),
        ] {
            assert!(
                !haystack.contains(SCRAM_PASSWORD),
                "{mode}: the password reached {what}"
            );
        }

        // NEGATIVE CONTROL: a wrong password.
        let wrong = Auth::scram(mode, user, "definitely-not-the-password");
        assert!(
            client(&bootstrap, &wrong).cluster_id().is_err(),
            "{mode}: a wrong password read the cluster id"
        );
        let neg_id = format!("compat-redpanda-neg-{}-{}", mode.to_lowercase(), nonce());
        let neg_receipt = demo_dir().join(format!("{neg_id}.receipt.json"));
        let refused = backup(
            &backup_spec(&bootstrap, &wrong, "orders", &neg_id, &storage),
            &neg_receipt,
            &wrong,
            &[],
        );
        assert_ne!(
            refused.status.code(),
            Some(0),
            "{mode}: a wrong password backed up"
        );
        assert!(
            !neg_receipt.exists(),
            "{mode}: a refused run wrote a receipt"
        );
        seen.insert(
            mode.to_string(),
            json!({
                "user": user,
                "backup_exit": out.status.code(),
                "receipt_format": doc["format_version"],
                "receipt_verified_by_both_readers": true,
                "wrong_password_exit": refused.status.code(),
                "connection.engineProtocol": capabilities["connection.engineProtocol"],
            }),
        );
    }
    // NEGATIVE CONTROL: Redpanda holds ONE mechanism per user.
    let crossed = Auth::scram("scramSha512", "logweir", SCRAM_PASSWORD);
    assert!(
        client(&bootstrap, &crossed).cluster_id().is_err(),
        "`logweir` holds SCRAM-SHA-256 only and authenticated with SCRAM-SHA-512"
    );
    evidence(
        "redpanda_authenticates_both_scram_mechanisms",
        &Value::Object(seen),
    );
}

/// **The maintained object store, through Logweir** (PROD-01.5's C4): the
/// `objectstore` profile's SeaweedFS as the archive AND evidence store.
///
/// * Conditional create: the backup's execution claim is taken, and a second
///   run under the same `backup_id` stops with
///   `failure-reason=ExecutionAlreadyClaimed` (exit 1) before the engine.
/// * Versioned reads: on the Object Lock bucket (versioning on) the receipt
///   pins the manifest's version id; on the plain bucket it carries none.
/// * The receipt verifies in both readers, and a drill restores the archive
///   into the stack's broker and verifies it, signed `pass`.
///
/// CONTROL: the same backup into MinIO (the default store) carries no version
/// id either, so the pin is the versioned bucket's and not the row's.
#[test]
#[ignore = "needs the stack's `objectstore` profile; see the module doc"]
fn seaweedfs_takes_a_backup_a_restore_and_refuses_a_second_claim() {
    let auth = Auth::plaintext();
    let topic = format!("compat-store-{}", nonce());
    create_topic(&topic, 3);
    let window = produce(&bootstrap(), &auth, &topic, 30);
    let cluster_id = cluster_id();
    let seaweed = |bucket: &str| Storage {
        endpoint: objectstore_endpoint(),
        bucket: bucket.to_string(),
    };
    let take = |storage: &Storage, tag: &str| {
        let backup_id = format!("compat-{tag}-{}", nonce());
        let spec = backup_spec(&bootstrap(), &auth, &topic, &backup_id, storage);
        let receipt = demo_dir().join(format!("{backup_id}.receipt.json"));
        let out = backup(&spec, &receipt, &auth, &[]);
        assert_eq!(
            out.status.code(),
            Some(0),
            "{tag}: backup into {} did not exit 0:\n{}",
            storage.endpoint,
            tail(&text(&out), 4000)
        );
        assert_eq!(verify_receipt(&receipt), (Some(0), Some(0)), "{tag}");
        let doc: Value = serde_json::from_str(&std::fs::read_to_string(&receipt).unwrap()).unwrap();
        (backup_id, spec, doc)
    };

    // --- the plain bucket: claim, refusal of a second claim, restore ---------
    let plain = seaweed("kafka-backups");
    let (backup_id, spec, doc) = take(&plain, "seaweedfs");
    assert!(
        doc["archive"]["manifest_version_id"].is_null(),
        "an unversioned bucket answers no version id: {}",
        doc["archive"]
    );
    let second_receipt = demo_dir().join(format!("{backup_id}.second.receipt.json"));
    let second = backup(&spec, &second_receipt, &auth, &[]);
    let second_text = text(&second);
    assert_eq!(
        second.status.code(),
        Some(1),
        "a second run under one backup_id must not back up again:\n{}",
        tail(&second_text, 3000)
    );
    assert_eq!(
        String::from_utf8_lossy(&second.stdout)
            .lines()
            .rfind(|l| !l.trim().is_empty()),
        Some("failure-reason=ExecutionAlreadyClaimed"),
        "the refusal is the conditional create's, by name:\n{}",
        tail(&second_text, 3000)
    );
    assert!(!second_receipt.exists(), "a refused run wrote a receipt");

    let dspec = drill_spec(&bootstrap(), &auth, &topic, &backup_id, window, &plain);
    let allow = allowlist_for(&cluster_id);
    let mut o = RunOpts::new(&dspec);
    o.allowlist = Some(&allow);
    let r = run_with(o);
    assert_eq!(
        r.out.status.code(),
        Some(0),
        "the drill over a SeaweedFS archive:\n{}",
        tail(
            &format!("{}\n{}", r.out.stdout_utf8(), r.out.stderr_utf8()),
            4000
        )
    );
    let sc = read_scorecard(&r);
    assert_eq!(sc["outcome"], "pass");
    assert!(logweir_verify(&r).success() && python_verify(&r).success());

    // --- the versioned (Object Lock) bucket: the manifest's version is pinned
    let (_, _, locked) = take(&seaweed("kafka-backups-locked"), "seaweedfs-locked");
    let pinned = locked["archive"]["manifest_version_id"]
        .as_str()
        .unwrap_or("")
        .to_string();
    assert!(
        !pinned.is_empty(),
        "a versioned bucket's receipt pins the manifest version it read back: {}",
        locked["archive"]
    );

    // --- CONTROL: MinIO's unversioned bucket carries no pin ------------------
    let (_, _, minio) = take(&Storage::minio(), "minio-control");
    assert!(minio["archive"]["manifest_version_id"].is_null());

    let _ = kafka_topics(&[
        "--bootstrap-server",
        "kafka-broker-1:9094",
        "--delete",
        "--topic",
        &topic,
    ]);
    evidence(
        "seaweedfs_takes_a_backup_a_restore_and_refuses_a_second_claim",
        &json!({
            "store": "SeaweedFS (profile objectstore)",
            "endpoint": objectstore_endpoint(),
            "backup_exit": 0,
            "receipt_format": doc["format_version"],
            "receipt_verified_by_both_readers": true,
            "second_claim": {"exit": second.status.code(), "refusal": "ExecutionAlreadyClaimed"},
            "restore": {"exit": 0, "outcome": sc["outcome"], "verified_by_both_readers": true},
            "unversioned_bucket_manifest_version_id": doc["archive"]["manifest_version_id"],
            "versioned_bucket_manifest_version_id_recorded": true,
            "minio_control_manifest_version_id": minio["archive"]["manifest_version_id"],
        }),
    );
}

// ============================================================ minimum ACLs

/// `kafka-acls.sh` on the ACL-enforcing broker, as the super user.
fn acl_cli(op: &str, args: &[&str]) -> Output {
    let mut all = vec![
        "/opt/kafka/bin/kafka-acls.sh",
        "--bootstrap-server",
        "kafka-acl:9094",
        op,
    ];
    if op == "--remove" {
        all.push("--force");
    }
    all.extend_from_slice(args);
    exec_in("kafka-acl", &all)
}

/// One ALLOW binding: a principal, an operation and a resource.
#[derive(Clone, Debug, PartialEq, Eq)]
struct Grant {
    principal: &'static str,
    operation: &'static str,
    /// `--cluster`, or `--topic <name>` with an optional `prefixed` pattern.
    resource: Vec<String>,
}

impl Grant {
    fn topic(principal: &'static str, operation: &'static str, name: &str) -> Self {
        Self {
            principal,
            operation,
            resource: vec!["--topic".into(), name.into()],
        }
    }
    fn prefix(principal: &'static str, operation: &'static str, prefix: &str) -> Self {
        Self {
            principal,
            operation,
            resource: vec![
                "--topic".into(),
                prefix.into(),
                "--resource-pattern-type".into(),
                "prefixed".into(),
            ],
        }
    }
    fn cluster(principal: &'static str, operation: &'static str) -> Self {
        Self {
            principal,
            operation,
            resource: vec!["--cluster".into()],
        }
    }
    fn args(&self) -> Vec<String> {
        let mut a = vec![
            "--allow-principal".to_string(),
            self.principal.to_string(),
            "--operation".to_string(),
            self.operation.to_string(),
        ];
        a.extend(self.resource.iter().cloned());
        a
    }
    /// `Operation on resource`, as the record's table spells a grant.
    fn spelled(&self) -> String {
        let resource = match self.resource.as_slice() {
            [c] if c == "--cluster" => "Cluster".to_string(),
            [_, name] => format!("Topic {name}"),
            [_, name, ..] => format!("Topic prefix {name}"),
            _ => unreachable!("a grant names a cluster, a topic or a prefix"),
        };
        format!("{} on {resource}", self.operation)
    }
}

/// Every binding a row added, removed again when it ends (or panics), so the
/// `acl` profile is left as the other rows expect it: no ACL at all, and so
/// every resource open again.
struct Grants(Vec<Grant>);

impl Grants {
    fn add(&mut self, g: &Grant) {
        let args = g.args();
        let borrowed: Vec<&str> = args.iter().map(String::as_str).collect();
        let out = acl_cli("--add", &borrowed);
        assert!(
            out.status.success(),
            "kafka-acls --add {args:?}:\n{}",
            tail(&text(&out), 1500)
        );
        if !self.0.contains(g) {
            self.0.push(g.clone());
        }
        std::thread::sleep(ACL_SETTLE);
    }
    fn remove(&mut self, g: &Grant) {
        let args = g.args();
        let borrowed: Vec<&str> = args.iter().map(String::as_str).collect();
        let out = acl_cli("--remove", &borrowed);
        assert!(
            out.status.success(),
            "kafka-acls --remove {args:?}:\n{}",
            tail(&text(&out), 1500)
        );
        self.0.retain(|x| x != g);
        std::thread::sleep(ACL_SETTLE);
    }
}

impl Drop for Grants {
    fn drop(&mut self) {
        for g in std::mem::take(&mut self.0) {
            let args = g.args();
            let borrowed: Vec<&str> = args.iter().map(String::as_str).collect();
            let _ = acl_cli("--remove", &borrowed);
        }
    }
}

/// How long a changed binding is given to reach the broker's authorizer
/// before the next run. One combined KRaft node applies it as it replays its
/// own metadata log.
const ACL_SETTLE: Duration = Duration::from_millis(1500);

/// The restricted principal of the `acl` profile, and the other principal
/// whose bindings close a resource (`allow.everyone.if.no.acl.found` opens a
/// resource only while it has NO binding at all).
const RESTRICTED: &str = "User:logweir";
const OTHER: &str = "User:prod-01-2-other";

/// The `error` field of the run's own "… failed" log line, or its last
/// `failure-reason=` / `refusal-reason=` line: how a refused run READS.
fn how_it_reads(out: &Output) -> String {
    let all = text(out);
    let reason = all
        .lines()
        .rev()
        .find(|l| l.starts_with("failure-reason=") || l.starts_with("refusal-reason="))
        .unwrap_or("");
    let error = all
        .lines()
        .filter_map(|l| serde_json::from_str::<Value>(l).ok())
        .filter(|v| {
            v["fields"]["message"]
                .as_str()
                .is_some_and(|m| m.ends_with(" failed"))
        })
        .filter_map(|v| v["fields"]["error"].as_str().map(str::to_string))
        .next_back()
        .unwrap_or_default();
    let error: String = error
        .lines()
        .next()
        .unwrap_or("")
        .chars()
        .take(420)
        .collect();
    // What the run printed about authorization, if anything: a denied engine
    // request is named only there.
    let said: String = all
        .lines()
        .find(|l| l.to_ascii_lowercase().contains("authoriz"))
        .map(|l| {
            let at = l.to_ascii_lowercase().find("authoriz").unwrap_or(0);
            l.chars().skip(at.saturating_sub(140)).take(320).collect()
        })
        .unwrap_or_default();
    let mut out = format!("{reason} {error}").trim().to_string();
    if !said.is_empty() && !out.contains(&said) {
        out.push_str(&format!(" || authorization: …{said}…"));
    }
    out
}

/// How each phase of a run ended, off its own "phase finished" log lines:
/// phase number to outcome, the outcome's first line capped.
fn phases_logged(out: &Output) -> BTreeMap<String, String> {
    text(out)
        .lines()
        .filter_map(|l| serde_json::from_str::<Value>(l).ok())
        .filter(|v| v["fields"]["message"] == "phase finished")
        .filter_map(|v| {
            let phase = v["fields"]["phase"].as_i64()?;
            let outcome: String = v["fields"]["outcome"]
                .as_str()?
                .lines()
                .next()
                .unwrap_or("")
                .chars()
                .take(260)
                .collect();
            Some((phase.to_string(), outcome))
        })
        .collect()
}

/// **The minimum-permission profile, measured** (PROD-01.2 §3), on the `acl`
/// profile's StandardAuthorizer broker with the restricted SCRAM principal.
///
/// Every resource the rows touch is first CLOSED: it is given a binding for
/// another principal, so nothing is open to `logweir` by
/// `allow.everyone.if.no.acl.found`. Then:
///
/// * **probe**: with NO binding for `logweir` at all, `cluster-probe` answers
///   `reachable=true` with the cluster id. Authentication is all it needs.
/// * **backup**: with exactly `Read` and `DescribeConfigs` on the source
///   topic, a backup exits 0, records the topic's ID and `captured`
///   configuration. Then each is removed in turn.
/// * **restore**: with exactly `Create`, `Write`, `Read` and `Delete` on the
///   target prefix, `DescribeConfigs` on the cluster and `Describe` on the
///   marker topic, a drill restores, verifies (`pass`) and tears down. Then
///   each is removed in turn.
///
/// What each removal does is REQUIRED below, as measured: the row fails if a
/// listed binding turns out not to be needed, or if a denied run stops
/// reading the way the record says it does.
#[test]
#[ignore = "needs the stack's `acl` profile; see the module doc"]
fn the_minimum_acls_for_probe_backup_and_restore() {
    // The authorizer is on: without one every binding below would be refused.
    let listed = acl_cli("--list", &[]);
    assert!(
        listed.status.success() && !text(&listed).contains("SecurityDisabled"),
        "kafka-acl is not running an authorizer: start the stack with the `acl` profile\n{}",
        tail(&text(&listed), 800)
    );
    let n = nonce();
    let topic = format!("mp-{n}-src");
    let prefix = format!("{SCRATCH_PREFIX}mp-{n}-");
    let super_user = Auth::plaintext();
    let restricted = Auth::scram("scramSha512", SCRAM_USER, SCRAM_PASSWORD);
    let (plain, sasl) = (bootstrap_acl(), bootstrap_acl_sasl());

    // The source topic and its records, as the super user.
    let created = exec_in(
        "kafka-acl",
        &[
            "/opt/kafka/bin/kafka-topics.sh",
            "--bootstrap-server",
            "kafka-acl:9094",
            "--create",
            "--topic",
            &topic,
            "--partitions",
            "3",
            "--replication-factor",
            "1",
        ],
    );
    assert!(created.status.success(), "{}", tail(&text(&created), 800));
    await_created_on(&plain, &topic, 3);
    let window = produce(&plain, &super_user, &topic, 30);
    let cluster_id = client(&plain, &super_user)
        .cluster_id()
        .expect("cluster id");

    let mut grants = Grants(Vec::new());
    // CLOSE the four resources.
    for g in [
        Grant::topic(OTHER, "Describe", &topic),
        Grant::prefix(OTHER, "Describe", &prefix),
        Grant::topic(OTHER, "Describe", MARKER_TOPIC),
        Grant::cluster(OTHER, "Describe"),
    ] {
        grants.add(&g);
    }

    // ------------------------------------------------------------ probe
    let (probe_exit, probe_id, reachable) = probe(&sasl, &restricted);
    assert_eq!(
        (probe_exit, reachable.as_str(), probe_id.as_str()),
        (Some(0), "true", cluster_id.as_str()),
        "the probe needs no binding at all: authentication is enough"
    );
    // ...and the closed topic really is closed to it (the control that the
    // setup closes anything).
    let seen_topics: Vec<String> = ClusterReader::list_topics(&client(&sasl, &restricted))
        .expect("an authenticated listing")
        .into_iter()
        .map(|t| t.name)
        .collect();
    assert!(
        !seen_topics.contains(&topic),
        "the closed source topic is visible to the restricted principal with no binding"
    );

    // ------------------------------------------------------------ backup
    let source = [
        Grant::topic(RESTRICTED, "Read", &topic),
        Grant::topic(RESTRICTED, "DescribeConfigs", &topic),
    ];
    for g in &source {
        grants.add(g);
    }
    let storage = Storage::minio();
    let take = |tag: &str| {
        let backup_id = format!("compat-mp-{n}-{tag}");
        let spec = backup_spec(&sasl, &restricted, &topic, &backup_id, &storage);
        let receipt = demo_dir().join(format!("{backup_id}.receipt.json"));
        let out = backup(&spec, &receipt, &restricted, &[]);
        let doc = std::fs::read_to_string(&receipt)
            .ok()
            .and_then(|t| serde_json::from_str::<Value>(&t).ok());
        (backup_id, out, doc)
    };
    let (full_id, full, full_doc) = take("full");
    assert_eq!(
        full.status.code(),
        Some(0),
        "exactly Read and DescribeConfigs on the source topic back it up:\n{}",
        tail(&text(&full), 4000)
    );
    let full_doc = full_doc.expect("a receipt");
    assert_eq!(full_doc["config_coverage"][&topic]["coverage"], "captured");
    assert!(full_doc["generations"][&topic]["topic_id"].is_string());

    let mut backup_rows = Vec::new();
    for g in &source {
        grants.remove(g);
        let (_, out, doc) = take(&format!("no-{}", g.operation.to_lowercase()));
        backup_rows.push(json!({
            "removed": g.spelled(),
            "exit": out.status.code(),
            "receipt": doc.is_some(),
            "coverage": doc.as_ref().map(|d| d["config_coverage"][&topic].clone()),
            "topic_id": doc.as_ref().map(|d| d["generations"][&topic].clone()),
            "reads": how_it_reads(&out),
        }));
        grants.add(g);
    }
    // Without Read: no backup, and no receipt.
    assert_ne!(backup_rows[0]["exit"], 0, "{}", backup_rows[0]);
    assert_eq!(backup_rows[0]["receipt"], false, "{}", backup_rows[0]);
    // Without DescribeConfigs: the backup still succeeds, and the receipt
    // says the configuration was NOT captured, with the reason.
    assert_eq!(backup_rows[1]["exit"], 0, "{}", backup_rows[1]);
    assert_eq!(
        backup_rows[1]["coverage"]["coverage"], "captureDenied",
        "a denied configuration read is recorded as denied, never as no overrides: {}",
        backup_rows[1]
    );
    assert!(
        backup_rows[1]["coverage"].get("timestamp_type").is_none(),
        "and its timestamp type is not recorded: {}",
        backup_rows[1]
    );

    // The capability row names the same gap BEFORE the backup.
    grants.remove(&source[1]);
    let (_, readiness) = check_run(
        &capability_plan(
            CheckOperation::Backup,
            restricted.connection_plan(&sasl),
            &[topic.as_str()],
            &[CheckId::ConnectionTopicConfigsReadable],
        ),
        restricted.password.as_deref(),
    );
    let denied = row(&readiness, CheckId::ConnectionTopicConfigsReadable);
    assert_eq!(
        (denied.state, denied.code, denied.gating),
        (
            CheckState::NotReady,
            CheckCode::TopicConfigsNotReadable,
            Gating::Advisory
        ),
        "{denied:?}"
    );
    assert!(
        denied.message.contains("captureDenied")
            && denied
                .remedy
                .contains("Grant this principal DescribeConfigs"),
        "{denied:?}"
    );
    assert_eq!(denied.detail.as_ref().unwrap()["sample"], json!([topic]));
    grants.add(&source[1]);
    // CONTROL: with the binding back, the same check is ready.
    let (_, readiness) = check_run(
        &capability_plan(
            CheckOperation::Backup,
            restricted.connection_plan(&sasl),
            &[topic.as_str()],
            &[CheckId::ConnectionTopicConfigsReadable],
        ),
        restricted.password.as_deref(),
    );
    assert_eq!(
        row(&readiness, CheckId::ConnectionTopicConfigsReadable).code,
        CheckCode::TopicConfigsReadable
    );

    // ------------------------------------------------------------ restore
    let target = [
        Grant::prefix(RESTRICTED, "Create", &prefix),
        Grant::prefix(RESTRICTED, "Write", &prefix),
        Grant::prefix(RESTRICTED, "Read", &prefix),
        Grant::prefix(RESTRICTED, "Delete", &prefix),
        Grant::cluster(RESTRICTED, "DescribeConfigs"),
        Grant::topic(RESTRICTED, "Describe", MARKER_TOPIC),
    ];
    for g in &target {
        grants.add(g);
    }
    let allow = allowlist_for(&cluster_id);
    let restore = |grants: &Grants| {
        let _ = grants;
        sweep_drill_topics(&plain, &super_user);
        let dspec = drill_spec(&sasl, &restricted, &topic, &full_id, window, &storage);
        let mut o = RunOpts::new(&dspec);
        o.allowlist = Some(&allow);
        o.env = vec![(
            "LOGWEIR_TARGET_PASSWORD".to_string(),
            SCRAM_PASSWORD.to_string(),
        )];
        let r = run_with(o);
        let scorecard = r.scorecard.exists().then(|| read_scorecard(&r));
        let left = sweep_drill_topics(&plain, &super_user);
        (r, scorecard, left)
    };
    let (r, scorecard, _) = restore(&grants);
    assert_eq!(
        r.out.status.code(),
        Some(0),
        "exactly the six listed bindings restore, verify and tear down:\n{}",
        tail(
            &format!("{}\n{}", r.out.stdout_utf8(), r.out.stderr_utf8()),
            4000
        )
    );
    assert_eq!(scorecard.expect("a scorecard")["outcome"], "pass");

    let mut restore_rows = Vec::new();
    for g in &target {
        grants.remove(g);
        let (r, scorecard, left) = restore(&grants);
        restore_rows.push(json!({
            "removed": g.spelled(),
            "exit": r.out.status.code(),
            "outcome": scorecard.as_ref().map(|s| s["outcome"].clone()),
            "scratch_topics_left": left,
            "reads": how_it_reads(&r.out),
            "phases": phases_logged(&r.out),
        }));
        grants.add(g);
    }
    evidence(
        "the_minimum_acls_for_probe_backup_and_restore",
        &json!({
            "broker": "kafka-acl (profile acl), StandardAuthorizer, restricted principal User:logweir over SCRAM-SHA-512",
            "probe": {"bindings": [], "exit": probe_exit, "reachable": reachable},
            "backup": {
                "bindings": source.iter().map(Grant::spelled).collect::<Vec<_>>(),
                "exit": full.status.code(),
                "each_removed": backup_rows,
            },
            "restore": {
                "bindings": target.iter().map(Grant::spelled).collect::<Vec<_>>(),
                "exit": 0, "outcome": "pass",
                "each_removed": restore_rows,
            },
        }),
    );
    // Every listed restore binding is needed: none of the six runs is the
    // full success again.
    for r in &restore_rows {
        assert!(
            !(r["exit"] == 0 && r["outcome"] == "pass" && r["scratch_topics_left"] == json!([])),
            "this binding is not needed for a restore, so it is not part of the minimum: {r}"
        );
    }
    // ...and each denial READS the way the record says (measured 2026-10-10).
    let mapped = format!("{prefix}src");
    let reads = |i: usize| restore_rows[i]["reads"].as_str().unwrap_or("").to_string();
    // Create: refused by name, before anything exists.
    assert_eq!(restore_rows[0]["exit"], 1, "{}", restore_rows[0]);
    assert!(
        reads(0).contains("could not be created") && reads(0).contains("TopicAuthorizationFailed"),
        "{}",
        restore_rows[0]
    );
    assert_eq!(restore_rows[0]["scratch_topics_left"], json!([]));
    // Write: the engine cannot produce; nothing is signed, and the empty
    // target topic Logweir created is left behind.
    assert_eq!(restore_rows[1]["exit"], 1, "{}", restore_rows[1]);
    assert!(restore_rows[1]["outcome"].is_null(), "{}", restore_rows[1]);
    assert_eq!(restore_rows[1]["scratch_topics_left"], json!([mapped]));
    // Read: restored and unverifiable; nothing is signed.
    assert_eq!(restore_rows[2]["exit"], 1, "{}", restore_rows[2]);
    assert!(restore_rows[2]["outcome"].is_null(), "{}", restore_rows[2]);
    assert!(
        reads(2).contains("TopicAuthorizationFailed"),
        "{}",
        restore_rows[2]
    );
    // Delete: the restore is verified and signed `pass`, exit 0, and the
    // target topic OUTLIVES the run although the plan says `teardown: delete`.
    assert_eq!(
        (&restore_rows[3]["exit"], &restore_rows[3]["outcome"]),
        (&json!(0), &json!("pass")),
        "{}",
        restore_rows[3]
    );
    assert_eq!(restore_rows[3]["scratch_topics_left"], json!([mapped]));
    // DescribeConfigs on the cluster: refused at phase 0, by name (FX-4).
    assert_eq!(restore_rows[4]["exit"], 1, "{}", restore_rows[4]);
    assert!(
        reads(4).contains("lacks DescribeConfigs on the cluster"),
        "{}",
        restore_rows[4]
    );
    // Describe on the marker: the guard refuses, and says both things a
    // missing marker can mean.
    assert_eq!(restore_rows[5]["exit"], 3, "{}", restore_rows[5]);
    assert!(
        reads(5).contains("or this principal may not Describe it"),
        "{}",
        restore_rows[5]
    );
    drop(grants);
    let _ = exec_in(
        "kafka-acl",
        &[
            "/opt/kafka/bin/kafka-topics.sh",
            "--bootstrap-server",
            "kafka-acl:9094",
            "--delete",
            "--topic",
            &topic,
        ],
    );
}

// ============================================================ advertised address

/// **An advertised address this runner cannot reach is not a reachable
/// cluster, and a connection test does not show it** (the `confluent`
/// profile's OFFNET listener: it answers a bootstrap, then advertises
/// `127.0.0.1:1`, where nothing listens).
///
/// * `cluster-probe` reads the cluster id off the bootstrap connection and
///   answers `reachable=true`: what a connection test certifies, and all of
///   it.
/// * The readiness check's `connection.authenticated` is `notReady`,
///   `BrokerUnreachable`, blocking, and says it is the ADVERTISED address with
///   a remedy about `advertised.listeners`; every capability row is blocked on
///   it, so none of them says anything about an endpoint nobody reached.
/// * A backup fails, with no receipt.
///
/// CONTROL: the SAME broker over its EXTERNAL listener, in
/// `confluent_platform_backs_up_restores_and_verifies`, where the same check
/// is `ready` and the same backup succeeds.
#[test]
#[ignore = "needs the stack's `confluent` profile; see the module doc"]
fn an_unreachable_advertised_address_is_not_a_reachable_cluster() {
    let bootstrap = bootstrap_confluent_offnet();
    let auth = Auth::plaintext();
    let (exit, id, reachable) = probe(&bootstrap, &auth);
    let real_id = client(&bootstrap_confluent(), &auth)
        .cluster_id()
        .expect("the EXTERNAL listener");
    assert_eq!(
        (exit, reachable.as_str(), id.as_str()),
        (Some(0), "true", real_id.as_str()),
        "the probe certifies that a bootstrap broker answered, and nothing more"
    );

    let started = Instant::now();
    let (_, result) = check_run(
        &capability_plan(
            CheckOperation::Backup,
            auth.connection_plan(&bootstrap),
            &["orders"],
            capability_checks_for(CheckOperation::Backup),
        ),
        None,
    );
    let check_took = started.elapsed();
    let authenticated = row(&result, CheckId::ConnectionAuthenticated);
    assert_eq!(
        (
            authenticated.state,
            authenticated.code,
            authenticated.gating
        ),
        (
            CheckState::NotReady,
            CheckCode::BrokerUnreachable,
            Gating::Blocking
        ),
        "{authenticated:?}"
    );
    assert!(
        authenticated
            .message
            .contains(&format!("named cluster {real_id}"))
            && authenticated
                .message
                .contains("advertised listeners are not reachable"),
        "{}",
        authenticated.message
    );
    assert!(
        authenticated.remedy.contains("advertised.listeners"),
        "{}",
        authenticated.remedy
    );
    for id in capability_checks_for(CheckOperation::Backup) {
        let r = row(&result, *id);
        assert_eq!(
            (r.state, r.code),
            (CheckState::Unknown, CheckCode::BlockedByPrerequisite),
            "a capability row says nothing about an endpoint nobody reached: {r:?}"
        );
    }

    let backup_id = format!("compat-offnet-{}", nonce());
    let storage = Storage::minio();
    let spec = backup_spec(&bootstrap, &auth, "orders", &backup_id, &storage);
    let receipt = demo_dir().join(format!("{backup_id}.receipt.json"));
    let started = Instant::now();
    let out = backup(&spec, &receipt, &auth, &[]);
    assert_ne!(out.status.code(), Some(0), "{}", tail(&text(&out), 3000));
    assert!(
        !receipt.exists(),
        "a backup that reached no broker wrote a receipt"
    );
    evidence(
        "an_unreachable_advertised_address_is_not_a_reachable_cluster",
        &json!({
            "listener": "kafka-cp OFFNET, advertised as 127.0.0.1:1",
            "probe": {"exit": exit, "cluster_id": id, "reachable": reachable},
            "check_seconds": check_took.as_secs_f64(),
            "connection.authenticated": authenticated,
            "capability_rows": "each unknown / BlockedByPrerequisite",
            "backup": {
                "exit": out.status.code(),
                "seconds": started.elapsed().as_secs_f64(),
                "receipt": false,
                "reads": how_it_reads(&out),
                "phases": phases_logged(&out),
            },
        }),
    );
}
