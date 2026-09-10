#![cfg(feature = "e2e")]
//! The refusal table as code, against the live stack. This is the artifact that
//! proves Global Constraints 4, 5 and 11, so it gets one `#[test]` per row.
//!
//! Exit 3 means the plan was refused BEFORE anything ran, so every row here
//! also asserts the two consequences that make that claim true: no scorecard
//! file, and nothing new in the evidence bucket.
mod harness;
use harness::*;

/// Every row that ends in exit 3 goes through here, so the message is asserted
/// too — a bare code assertion can pass for the wrong reason.
fn expect_exit_3(mutate: impl Fn(&mut serde_yaml::Value), needle: &str) {
    let mut spec = spec_default();
    mutate(&mut spec);
    let before = list_evidence_bucket();
    let r = drill_run(&spec);
    let e = r.out.stderr_utf8();
    assert_eq!(r.out.status.code(), Some(3), "{e}");
    assert!(e.contains(needle), "expected `{needle}` in:\n{e}");
    assert!(
        !r.scorecard.exists(),
        "a guard refusal must write NO scorecard: {} exists",
        r.scorecard.display()
    );
    assert_eq!(
        before,
        list_evidence_bucket(),
        "a guard refusal must upload nothing"
    );
}

// --- the three forbidden keys, at BOTH values: the guard is on the KEY -------
#[test]
fn purge_topics_true_is_refused() {
    expect_exit_3(
        |s| s["engine_overrides"]["purge_topics"] = true.into(),
        "purge_topics",
    );
}
#[test]
fn purge_topics_false_is_refused_exactly_the_same() {
    expect_exit_3(
        |s| s["engine_overrides"]["purge_topics"] = false.into(),
        "purge_topics",
    );
}
#[test]
fn dry_run_true_is_refused() {
    expect_exit_3(
        |s| s["engine_overrides"]["dry_run"] = true.into(),
        "dry_run",
    );
}
#[test]
fn dry_run_false_is_refused_exactly_the_same() {
    expect_exit_3(
        |s| s["engine_overrides"]["dry_run"] = false.into(),
        "dry_run",
    );
}
#[test]
fn header_preflight_external_true_is_refused() {
    expect_exit_3(
        |s| s["engine_overrides"]["header_preflight_external"] = true.into(),
        "header_preflight_external",
    );
}
#[test]
fn header_preflight_external_false_is_refused_exactly_the_same() {
    expect_exit_3(
        |s| s["engine_overrides"]["header_preflight_external"] = false.into(),
        "header_preflight_external",
    );
}
#[test]
fn a_forbidden_key_nested_deep_in_the_spec_is_still_found() {
    expect_exit_3(
        |s| s["engine_overrides"]["a"]["b"]["purge_topics"] = true.into(),
        "purge_topics",
    );
}

// --- mapping, allowlist, marker topic ---------------------------------------

/// The topic-mapping row. RENAMED from the brief's
/// `a_selected_topic_with_no_mapping_entry_is_refused`, applying addendum
/// ruling A5's own standard: `phase0_admit::run` BUILDS the mapping from
/// `spec.source.topics` itself, one entry per selected topic, so
/// `check_topic_mapping_coverage`'s `None` arm is unreachable from a spec and
/// the brief's name could never describe what its body does. What an empty
/// prefix actually produces is the OTHER refusal in the same guard — a mapping
/// that would restore each topic over itself — and that is what this asserts.
#[test]
fn a_topic_mapping_that_maps_a_selected_topic_onto_itself_is_refused() {
    expect_exit_3(
        |s| s["target"]["topic_mapping_prefix"] = "".into(),
        "topic_mapping maps `orders` onto itself",
    );
}

/// The allowlist row of the refusal table: a target whose cluster_id is not in
/// allowed_cluster_ids is refused at phase 0. Driven by swapping the allowlist
/// fixture, which is the only thing that actually changes the guard's input
/// (addendum ruling A5).
#[test]
fn a_cluster_id_absent_from_allowed_cluster_ids_is_refused() {
    let spec = spec_default();
    let before = list_evidence_bucket();
    let r = drill_run_with_allowlist(
        &spec,
        &root().join("e2e/fixtures/allowed-clusters-empty.json"),
    );
    let e = r.out.stderr_utf8();
    assert_eq!(r.out.status.code(), Some(3), "{e}");
    assert!(e.contains("allowedClusterIds"), "{e}");
    assert!(!r.scorecard.exists());
    assert_eq!(before, list_evidence_bucket());
}

#[test]
fn a_missing_marker_topic_is_refused() {
    delete_marker_topic();
    let before = list_evidence_bucket();
    let r = drill_run(&spec_default());
    // Put the cluster back before anything can panic out of this test.
    recreate_marker_topic();

    let e = r.out.stderr_utf8();
    assert_eq!(r.out.status.code(), Some(3), "{e}");
    assert!(e.contains("marker topic"), "{e}");
    assert!(!r.scorecard.exists());
    assert_eq!(before, list_evidence_bucket());
}

// --- the sample anchor ------------------------------------------------------

/// `tail` and `random` select archive records phase 7's leading-range read
/// cannot reach. v0.1 REFUSES them at phase 0 rather than silently sampling
/// `head` under a plan that asked for something else — the scorecard would
/// otherwise record an anchor the drill never applied. See
/// `logweir_core::spec::Anchor` for the measurement that forced this.
#[test]
fn a_sample_anchor_of_random_is_refused_before_anything_runs() {
    expect_exit_3(|s| s["sample"]["anchor"] = "random".into(), "sample.anchor");
}

#[test]
fn a_sample_anchor_of_tail_is_refused_exactly_the_same() {
    expect_exit_3(|s| s["sample"]["anchor"] = "tail".into(), "sample.anchor");
}

/// An anchor that is not one of the three does not reach a guard at all: it is
/// unspellable, so the spec does not parse. That is exit 1 (Logweir could not
/// read its own input), not exit 3, and it is loud either way — the point of
/// the closed enum is that it can never degrade to head-like behaviour the way
/// a free-form string did.
#[test]
fn an_unspellable_sample_anchor_fails_to_parse_and_never_degrades_silently() {
    let mut spec = spec_default();
    spec["sample"]["anchor"] = "sideways".into();
    let before = list_evidence_bucket();
    let r = drill_run(&spec);
    let e = r.out.stderr_utf8();
    assert_eq!(r.out.status.code(), Some(1), "{e}");
    assert!(e.contains("drill spec does not parse"), "{e}");
    assert!(e.contains("sideways"), "{e}");
    assert!(!r.scorecard.exists());
    assert_eq!(before, list_evidence_bucket());
}

// --- approval ----------------------------------------------------------------
#[test]
fn an_approval_over_different_spec_bytes_is_refused() {
    let before = list_evidence_bucket();
    let r = drill_run_with_stale_approval();
    let e = r.out.stderr_utf8();
    assert_eq!(r.out.status.code(), Some(3), "{e}");
    assert!(e.contains("plan_hash"), "{e}");
    assert!(!r.scorecard.exists());
    assert_eq!(before, list_evidence_bucket());
}

#[test]
fn an_approval_signed_by_the_wrong_key_is_refused() {
    let before = list_evidence_bucket();
    let r = drill_run_with_wrong_approver_key();
    let e = r.out.stderr_utf8();
    assert_eq!(r.out.status.code(), Some(3), "{e}");
    assert!(e.contains("signature"), "{e}");
    assert!(!r.scorecard.exists());
    assert_eq!(before, list_evidence_bucket());
}

// --- the exit-4 backstop -----------------------------------------------------

/// Exit 4 leaves the bucket EMPTY of this run's artifacts — signing precedes
/// every put, and a put that fails retracts nothing because nothing was
/// written.
///
/// DRIVEN BY AN UNWRITABLE EVIDENCE SINK, not by the brief's unreadable signing
/// key. Measured: an unreadable signing key exits **1**, not 4, and that is the
/// product's own design rather than a defect — `execute_with` loads the signing
/// key immediately after phase 0 (it needs the public half to decide
/// `approval.self_attested`), and a key that cannot be read there means NO
/// DRILL RAN. Exit 4's own contract, in `DrillError::SigningOrLock`'s doc
/// comment, is "the drill RAN, and its result could not be signed"; reporting
/// that for a mistyped `--signing-key` path would be a false claim. See the
/// sibling test below, which pins the exit-1 behaviour so it cannot drift
/// silently either.
///
/// An evidence bucket that does not exist reaches the genuine exit-4 path:
/// `Store::from_url` builds without a round trip, phase 8 validates, zeroes,
/// serialises and SIGNS, and only then does `put_create_only` fail.
#[test]
fn an_unwritable_evidence_sink_exits_4_and_uploads_nothing() {
    let mut spec = spec_default();
    spec["evidence"]["bucket"] = "logweir-evidence-does-not-exist".into();
    let before = list_evidence_bucket();
    let r = drill_run(&spec);
    assert_eq!(r.out.status.code(), Some(4), "{}", r.out.stderr_utf8());
    assert!(
        !r.scorecard.exists(),
        "the artifact is written from the bytes phase 8 stored; a failed put writes none"
    );
    // The whole-bucket before/after comparison, not `evidence_for_run(run_id)`:
    // this run wrote no scorecard and its structured error line carries no
    // `run_id` field, so there is no id to scope by — and the unscoped
    // comparison is the stronger claim anyway. It says nothing at all was
    // added, which covers both this run's artifacts and any it might have
    // written under someone else's key.
    assert_eq!(
        before,
        list_evidence_bucket(),
        "OSO's own rule, adopted verbatim: a signing/lock failure aborts before anything \
         is uploaded"
    );
    let e = r.out.stderr_utf8();
    assert!(
        e.contains("signing or lock proof failed"),
        "exit 4 must say which contract it broke:\n{e}"
    );
}

/// The brief's `an_unreadable_signing_key_exits_4` row, asserting what the
/// product actually does and why that is right: exit 1, no artifact, and a
/// message naming the key. Pinned so the routing cannot change unnoticed in
/// either direction.
#[test]
fn an_unreadable_signing_key_exits_1_because_no_drill_ran() {
    let bad = demo_dir().join("broken-signing-key.pem");
    std::fs::write(
        &bad,
        b"-----BEGIN PRIVATE KEY-----\nnope\n-----END PRIVATE KEY-----\n",
    )
    .unwrap();
    let before = list_evidence_bucket();
    let r = drill_run_with_signing_key(&bad);
    let e = r.out.stderr_utf8();
    assert_eq!(
        r.out.status.code(),
        Some(1),
        "an unreadable signing key means no drill ran, so exit 1 (no artifact), \
         never exit 2 (a drill result) and never exit 4 (a drill that ran but is \
         unattested):\n{e}"
    );
    assert!(
        e.contains("not a P-256 or Ed25519 PKCS#8 key"),
        "the failure must name the key, not something else:\n{e}"
    );
    assert!(!r.scorecard.exists());
    assert_eq!(before, list_evidence_bucket());
}

// --- the schema surface ------------------------------------------------------
#[test]
fn schema_scorecard_prints_the_schema() {
    let o = std::process::Command::new(bin())
        .args(["schema", "scorecard"])
        .output()
        .unwrap();
    assert!(o.status.success());
    assert!(o
        .stdout_utf8()
        .contains("logweir-drill-scorecard-1.0.0.json"));
}

#[test]
fn schema_plan_exits_1_naming_sp3() {
    let o = std::process::Command::new(bin())
        .args(["schema", "plan"])
        .output()
        .unwrap();
    assert_eq!(o.status.code(), Some(1));
    assert!(o.stderr_utf8().contains("SP3"));
}

// --- G-TS: the target topics Logweir creates itself, and spec §17 residual 3 -

/// The broker's KRaft node id (`KAFKA_NODE_ID`, `e2e/compose/docker-compose.yml:26`).
const BROKER_ID: &str = "1001";
/// The PLAINTEXT inter-broker listener (`docker-compose.yml:31`), which exists
/// today — this probe does not depend on Task 7's added listeners.
const INTER_BROKER: &str = "kafka-broker-1:9094";
/// The one topic the probe creates. Inside `drill-`, so the already-scoped
/// `TopicDeleter` can remove it, and named so a stray one is obviously a probe.
const PROBE_TOPIC: &str = "drill-logweir-t8-timestamp-override-probe";

/// `docker compose exec kafka-broker-1 /opt/kafka/bin/kafka-configs.sh …`.
///
/// A **dynamic** broker config: it needs no compose edit, so STANDING RULE 15
/// (Task 7 is the only editor of `docker-compose.yml`) is untouched, and the
/// plan names it as the sanctioned way to probe a broker setting.
fn kafka_configs(args: &[&str]) -> std::process::Output {
    let mut c = std::process::Command::new("docker");
    c.args([
        "compose",
        "-f",
        "e2e/compose/docker-compose.yml",
        "exec",
        "-T",
        "kafka-broker-1",
        "/opt/kafka/bin/kafka-configs.sh",
        "--bootstrap-server",
        INTER_BROKER,
    ]);
    c.args(args);
    c.current_dir(root()).output().expect("docker compose exec")
}

fn alter_broker(args: &[&str]) -> std::process::Output {
    let mut all = vec![
        "--alter",
        "--entity-type",
        "brokers",
        "--entity-name",
        BROKER_ID,
    ];
    all.extend_from_slice(args);
    kafka_configs(&all)
}

fn describe_broker_dynamic_config() -> String {
    let o = kafka_configs(&[
        "--describe",
        "--entity-type",
        "brokers",
        "--entity-name",
        BROKER_ID,
    ]);
    format!("{}{}", o.stdout_utf8(), o.stderr_utf8())
}

/// Reverts the dynamic broker config even if the body of the probe panics.
/// `--delete-config` on a key that is already absent succeeds, so the explicit
/// revert inside the test and this one are both safe.
struct RevertTimestampType;
impl Drop for RevertTimestampType {
    fn drop(&mut self) {
        let _ = alter_broker(&["--delete-config", "log.message.timestamp.type"]);
    }
}

/// **Spec §15 `[UNVERIFIED]` mark 9 / spec §17 residual 3, answered by
/// execution.** Does a broker configured `log.message.timestamp.type =
/// LogAppendTime` accept a per-topic `message.timestamp.type = CreateTime`
/// override?
///
/// The compose broker is on the Apache default (`CreateTime`) —
/// `docker-compose.yml:25-45` sets no `KAFKA_LOG_MESSAGE_TIMESTAMP_TYPE` — so
/// printing the broker's value answers nothing. This alters it DYNAMICALLY,
/// creates one topic through the shipped `TopicCreator` with the shipped
/// `TARGET_TOPIC_CONFIGS`, reads it back through the shipped `topic_configs`,
/// and reverts in the same step, asserting the revert.
///
/// It is also where the ONE binary-level assertion of the
/// `TargetTopicConfigRefused` contract lives: a binary arm needs both a broker
/// and a broker on `LogAppendTime`, which is what this test manufactures. If
/// the broker honours the override the refusal is unreachable here by
/// construction and the transcript says so — the deterministic refusal arm is
/// `crates/logweir/tests/topic_preflight.rs`'s
/// `a_logappendtime_broker_that_refuses_the_override_is_a_guard_refusal`, in
/// process over doubles.
#[test]
fn a_logappendtime_broker_accepts_or_refuses_a_per_topic_override() {
    use logweir_kafka::reader::{
        ClusterReader, NewTopicSpec, TopicCreator, TopicDeleter, TARGET_TOPIC_CONFIGS,
    };

    // Start from a known state, and make sure the probe topic is not left over
    // from an interrupted run.
    let scoped = reader()
        .with_scratch_prefix(SCRATCH_PREFIX)
        .expect("`drill-` is a usable scratch namespace");
    let _ = TopicDeleter::delete_topics(&scoped, &[PROBE_TOPIC.to_string()]);

    let before = describe_broker_dynamic_config();
    println!("--- residual 3, transcript 1: the broker's dynamic config BEFORE ---\n{before}");
    assert!(
        !before.contains("log.message.timestamp.type"),
        "this probe starts from a broker with no dynamic log.message.timestamp.type; found:\n{before}"
    );

    let _revert = RevertTimestampType;
    let altered = alter_broker(&["--add-config", "log.message.timestamp.type=LogAppendTime"]);
    assert!(
        altered.status.success(),
        "kafka-configs --alter failed:\n{}\n{}",
        altered.stdout_utf8(),
        altered.stderr_utf8()
    );

    // Bounded FOREGROUND poll until the broker reports the new value. Dynamic
    // config propagation is asynchronous; nothing here is backgrounded.
    let mut reported = String::new();
    for _ in 0..30 {
        reported = ClusterReader::broker_configs(&reader())
            .expect("the broker answers DescribeConfigs")
            .get("log.message.timestamp.type")
            .cloned()
            .unwrap_or_default();
        if reported == "LogAppendTime" {
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(500));
    }
    assert_eq!(
        reported, "LogAppendTime",
        "the dynamic broker config did not take effect within 15s"
    );
    println!(
        "--- residual 3, transcript 2: broker_configs() reports \
         log.message.timestamp.type={reported} ---"
    );

    // The override attempt, through the SHIPPED seam and the SHIPPED constant.
    let spec = NewTopicSpec {
        name: PROBE_TOPIC.to_string(),
        num_partitions: 1,
        replication_factor: 1,
        configs: TARGET_TOPIC_CONFIGS
            .iter()
            .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
            .collect(),
    };
    let created = TopicCreator::create_topics(&scoped, std::slice::from_ref(&spec))
        .expect("the CreateTopics call itself succeeds");
    println!("--- residual 3, transcript 3: create_topics -> {created:?} ---");
    assert!(
        created.iter().all(|(_, r)| r.is_ok()),
        "creating the probe topic failed: {created:?}"
    );

    let readback = ClusterReader::topic_configs(&scoped, PROBE_TOPIC)
        .expect("the probe topic describes")
        .get("message.timestamp.type")
        .cloned()
        .unwrap_or_default();
    println!(
        "--- residual 3, ANSWER: a broker on log.message.timestamp.type=LogAppendTime, asked for \
         a per-topic message.timestamp.type=CreateTime, reports message.timestamp.type={readback} \
         for {PROBE_TOPIC} ---"
    );
    assert!(
        readback == "CreateTime" || readback == "LogAppendTime",
        "message.timestamp.type read back as {readback:?}, which is neither value Kafka defines"
    );
    let honoured = readback == "CreateTime";

    // The binary arm, reachable only on a broker that REFUSES the override.
    if honoured {
        println!(
            "--- residual 3: this broker HONOURS the per-topic override, so the \
             TargetTopicConfigRefused binary arm is unreachable here by construction. The \
             deterministic arm is crates/logweir/tests/topic_preflight.rs::\
             a_logappendtime_broker_that_refuses_the_override_is_a_guard_refusal. ---"
        );
    } else {
        let r = drill_run(&spec_default());
        let stdout = r.out.stdout_utf8();
        let stderr = r.out.stderr_utf8();
        println!("--- residual 3, binary arm stdout ---\n{stdout}\n--- stderr ---\n{stderr}");
        assert_eq!(
            r.out.status.code(),
            Some(3),
            "a refused override is exit 3: {stderr}"
        );
        // The runner's FINAL non-empty stdout line is the terminal state
        // (interface **I9**): the pod log API has no stream selector, so a
        // controller reads the last line of an interleaved stream.
        let last = stdout
            .lines()
            .rev()
            .find(|l| !l.trim().is_empty())
            .unwrap_or_default();
        assert_eq!(
            last, "refusal-reason=TargetTopicConfigRefused",
            "stdout was:\n{stdout}"
        );
        assert!(
            !r.scorecard.exists(),
            "a guard refusal writes NO scorecard: {} exists",
            r.scorecard.display()
        );
    }

    // Put the cluster back: the probe topic, then the dynamic config, and
    // assert the revert by reading the broker config back.
    TopicDeleter::delete_topics(&scoped, &[PROBE_TOPIC.to_string()])
        .expect("the probe topic deletes");
    let reverted = alter_broker(&["--delete-config", "log.message.timestamp.type"]);
    assert!(
        reverted.status.success(),
        "kafka-configs --delete-config failed:\n{}\n{}",
        reverted.stdout_utf8(),
        reverted.stderr_utf8()
    );
    let mut after = String::new();
    for _ in 0..30 {
        after = describe_broker_dynamic_config();
        if !after.contains("log.message.timestamp.type") {
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(500));
    }
    println!(
        "--- residual 3, transcript 4: the broker's dynamic config AFTER the revert ---\n{after}"
    );
    assert!(
        !after.contains("log.message.timestamp.type"),
        "the dynamic broker config was NOT reverted; every later e2e row would run against a \
         LogAppendTime broker:\n{after}"
    );
    assert_eq!(
        ClusterReader::broker_configs(&reader())
            .expect("the broker answers DescribeConfigs")
            .get("log.message.timestamp.type")
            .cloned()
            .unwrap_or_default(),
        "CreateTime",
        "after the revert the broker is back on the Apache default"
    );
}

/// **G-TS at the binary level, on the compose broker.** The rendered
/// `restore.yaml` says `create_topics: false`, so a passing drill proves
/// LOGWEIR created the target topics — and it created them with
/// `TARGET_TOPIC_CONFIGS`, which is asserted by reading the topic back.
#[test]
fn logweir_creates_the_target_topics_with_the_pinned_config_set() {
    use logweir_kafka::reader::ClusterReader;

    delete_all_drill_topics();
    let r = drill_run(&spec_default());
    assert_eq!(
        r.out.status.code(),
        Some(0),
        "the engine creates nothing now (create_topics: false), so this passing run is the proof \
         Logweir created the target topics itself: {}",
        r.out.stderr_utf8()
    );
    let reader = reader();
    let configs = ClusterReader::topic_configs(&reader, "drill-orders").expect("drill-orders");
    assert_eq!(
        configs.get("message.timestamp.type").map(String::as_str),
        Some("CreateTime"),
        "restored records keep their ORIGINAL timestamps only if the target topic is CreateTime; \
         got {configs:?}"
    );
    assert_eq!(
        configs.get("retention.ms").map(String::as_str),
        Some("-1"),
        "an older restore point writes segments already past a finite retention threshold; \
         got {configs:?}"
    );
    // The partition count is still the MANIFEST's, which is what makes the
    // creation step correct at all — see
    // `phase3_diff::restore_partition_count`.
    assert_eq!(count_partitions("drill-orders"), 3);
}

/// **Spec §6.1 at the binary level, on the compose broker** (review finding
/// 1). A mapped target topic that already exists is refused — exit 3,
/// `refusal-reason=GuardRefused` as the final stdout line, no scorecard,
/// nothing uploaded — and the pre-existing topic is left exactly as it was
/// found: not deleted, not reconfigured, and no OTHER target topic created
/// beside it.
///
/// The pre-created topic carries the CLUSTER DEFAULT retention on purpose. It
/// is the case that made reuse a false claim: `TopicPreflight.configs_set`
/// reported `retention.ms=-1` "as applied" while the topic it reused kept
/// `604800000`, and Task 20 copies that block to
/// `Restore.status.topicPreflight`.
#[test]
fn a_mapped_target_topic_that_already_exists_is_refused() {
    use logweir_kafka::reader::ClusterReader;

    let spec = spec_default();
    let before = list_evidence_bucket();
    let mut opts = RunOpts::new(&spec);
    // `drill-orders` is the first mapped target of the default spec. One
    // partition, against the manifest's three, so the topic is also visibly
    // NOT the topic this restore needs.
    opts.pre_create = vec![("drill-orders".to_string(), 1)];
    let r = run_with(opts);

    let stdout = r.out.stdout_utf8();
    let stderr = r.out.stderr_utf8();
    assert_eq!(
        r.out.status.code(),
        Some(3),
        "a pre-existing mapped target topic is a plan refused before anything ran:\n{stderr}"
    );
    assert!(
        stderr.contains("drill-orders"),
        "the refusal must name the topic:\n{stderr}"
    );
    // Interface **I9**: the runner's FINAL non-empty stdout line. Not
    // `TargetTopicConfigRefused` — that state is for a target CONFIGURATION
    // this build refuses, and spec §3.2 names no state for mere existence.
    let last = stdout
        .lines()
        .rev()
        .find(|l| !l.trim().is_empty())
        .unwrap_or_default();
    assert_eq!(last, "refusal-reason=GuardRefused", "stdout was:\n{stdout}");
    assert!(
        !r.scorecard.exists(),
        "a guard refusal writes NO scorecard: {} exists",
        r.scorecard.display()
    );
    assert_eq!(
        before,
        list_evidence_bucket(),
        "a guard refusal must upload nothing"
    );

    // The target is exactly as it was found.
    assert!(
        topic_exists("drill-orders"),
        "the refusal must not delete the operator's topic"
    );
    assert_eq!(
        count_partitions("drill-orders"),
        1,
        "nor recreate it at another count"
    );
    let reader = reader();
    let configs = ClusterReader::topic_configs(&reader, "drill-orders").expect("drill-orders");
    assert_ne!(
        configs.get("retention.ms").map(String::as_str),
        Some("-1"),
        "the refusal must not have applied the pinned configuration to a topic it did not \
         create; a reused topic keeps its own retention, which is the whole reason this plan is \
         refused rather than continued: {configs:?}"
    );
    assert!(
        !topic_exists("drill-payments"),
        "and it must create nothing: the second mapped target must be absent"
    );

    // Leave the namespace clean for the rows that follow, even though every
    // `run_with` empties it first.
    delete_all_drill_topics();
}
