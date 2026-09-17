//! The shared check framework — D2 §4.3 and §4.4.
//!
//! EVERY TEST HERE IS A PURE-FUNCTION TEST OR A `mock_client` TEST. Nothing
//! dials a socket and nothing waits on a Job. The double PANICS on a request it
//! was not given a route for, which is what makes "the framework did not read
//! that pod's log" an assertion rather than an absence of evidence.
//!
//! READ `a_pod_that_wears_the_label_but_is_not_owned_is_never_read` FIRST. It
//! is D-SEAMS **S6** and defect `SEC-PODLOG`: `batch.kubernetes.io/job-name` is
//! writable by anything that can create a pod, and a check's stdout becomes a
//! custom resource's status and then an API response. The label narrows the
//! list; the controller `ownerReference` UID decides.

use std::collections::BTreeMap;

use chrono::{DateTime, TimeZone, Utc};
use k8s_openapi::api::batch::v1::Job;
use k8s_openapi::api::core::v1::{Event, Pod};
use logweir_core::check_contract::{
    frames, topic_tsv_sha256, CheckCode, CheckId, CheckPlanKind, FrameExpectations, Stream,
    TopicEntry, CHECK_CONTRACT_VERSION,
};
use serde_json::{json, Value};
use weirkeeper::check::{
    self, chunks, job as cjob, limits, plan, pod as cpod, policy, relay, waiting, CheckPhase,
    EventFact, Input, Projections, Waiting,
};
use weirkeeper::job::{RunnerOwner, SecretMount};
use weirkeeper::testing::{mock_client_recording_bodies, Route};

// ---------------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------------

const NS: &str = "logweir-d2-w5";
/// The subject (a `TopicDiscovery`, once W6a lands the kind).
const SUBJECT_UID: &str = "9f2b1c44-0000-4000-8000-0000000000a1";
/// The check Job's UID, which is the only thing a pod can be proved against.
const JOB_UID: &str = "1a2b3c4d-0000-4000-8000-0000000000b2";
/// Another Job's UID — the "wrong owner" in the S6 tests.
const OTHER_JOB_UID: &str = "deadbeef-0000-4000-8000-0000000000c3";
const PLAN_SHA: &str = "sha256:1111111111111111111111111111111111111111111111111111111111111111";
const POD: &str = "lwc-td-9ab-xyz12";
/// The `KafkaCluster` this check dials — `limits`' per-connection ceiling.
const CONNECTION_UID: &str = "c0ffee00-0000-4000-8000-0000000000d4";
/// The `pods/log` path suffix for [`POD`]. A `&'static str`, because
/// [`Route::path_suffix`] is one.
const POD_LOG_PATH: &str = "/pods/lwc-td-9ab-xyz12/log";
/// The check Job's path suffix, spelled out rather than composed, so a change
/// to [`cjob::check_job_name`] has to be argued for here as well as asserted
/// there. `a_check_job_name_is_a_pure_function_of_the_kind_and_the_owner_uid`
/// holds the two together.
const JOB_PATH: &str = "/jobs/lwc-td-288fecc03251c1c31851";
/// The plan `ConfigMap`'s path suffix, spelled out for the same reason.
const PLAN_PATH: &str = "/configmaps/lwc-td-288fecc03251c1c31851-plan";

fn utc(h: u32, m: u32, s: u32) -> DateTime<Utc> {
    Utc.with_ymd_and_hms(2026, 9, 16, h, m, s)
        .single()
        .expect("the fixture instant exists")
}

fn now() -> DateTime<Utc> {
    utc(12, 0, 0)
}

fn job_name() -> String {
    cjob::check_job_name(CheckPlanKind::TopicInventory, SUBJECT_UID)
}

fn owner() -> RunnerOwner {
    RunnerOwner {
        api_version: "logweir.dev/v1alpha1".to_string(),
        kind: "TopicDiscovery".to_string(),
        name: "orders-discovery".to_string(),
        uid: SUBJECT_UID.to_string(),
    }
}

fn expectations() -> FrameExpectations {
    FrameExpectations {
        plan_sha256: PLAN_SHA.to_string(),
        subject_uid: SUBJECT_UID.to_string(),
    }
}

fn spec() -> cjob::CheckJobSpec {
    cjob::CheckJobSpec {
        kind: CheckPlanKind::TopicInventory,
        namespace: NS.to_string(),
        owner: owner(),
        connection_uid: Some(CONNECTION_UID.to_string()),
        plan_config_map: format!("{}-plan", job_name()),
        plan_sha256: PLAN_SHA.to_string(),
        subject_uid: SUBJECT_UID.to_string(),
        timeout_seconds: 120,
        service_account_name: "logweir-runner".to_string(),
        secret_mounts: vec![SecretMount {
            volume: "signing".to_string(),
            secret_name: "logweir-signing-key".to_string(),
            mount_path: "/signing".to_string(),
            items: Vec::new(),
        }],
        config_map_mounts: Vec::new(),
        env_from_secret: Vec::new(),
        env_literal: Vec::new(),
        image: None,
        image_pull_policy: None,
    }
}

/// A Job, finished or not.
fn job_object(finished: Option<&str>, reason: Option<&str>) -> Job {
    let status = match finished {
        Some(kind) => json!({"conditions":[{
            "type": kind, "status": "True",
            "reason": reason,
            "lastProbeTime": "2026-09-16T11:59:00Z",
            "lastTransitionTime": "2026-09-16T11:59:00Z"
        }]}),
        None => json!({}),
    };
    serde_json::from_value(json!({
        "apiVersion": "batch/v1", "kind": "Job",
        "metadata": {
            "name": job_name(), "namespace": NS, "uid": JOB_UID,
            "creationTimestamp": "2026-09-16T11:58:00Z",
            "ownerReferences": [{
                "apiVersion": "logweir.dev/v1alpha1", "kind": "TopicDiscovery",
                "name": "orders-discovery", "uid": SUBJECT_UID,
                "controller": true, "blockOwnerDeletion": true
            }]
        },
        "spec": {"template": {"spec": {"containers": [], "restartPolicy": "Never"}}},
        "status": status
    }))
    .expect("the fixture is a Job")
}

/// A pod whose controller owner is the Job with `owner_uid`, or none at all.
fn pod_object(owner: Option<(&str, &str, bool)>, container: Value) -> Pod {
    let owners = match owner {
        Some((kind, uid, controller)) => json!([{
            "apiVersion": "batch/v1", "kind": kind, "name": job_name(),
            "uid": uid, "controller": controller, "blockOwnerDeletion": true
        }]),
        None => json!([]),
    };
    serde_json::from_value(json!({
        "apiVersion": "v1", "kind": "Pod",
        "metadata": {
            "name": POD, "namespace": NS,
            "creationTimestamp": "2026-09-16T11:58:10Z",
            "ownerReferences": owners,
            "labels": {"batch.kubernetes.io/job-name": job_name()}
        },
        "spec": {"containers": []},
        "status": container
    }))
    .expect("the fixture is a Pod")
}

fn terminated(exit_code: i32) -> Value {
    json!({"phase":"Succeeded","containerStatuses":[{
        "name":"runner","image":"x","imageID":"x","ready":false,"restartCount":0,
        "state":{"terminated":{"exitCode":exit_code,"finishedAt":"2026-09-16T11:59:00Z"}}
    }]})
}

fn waiting_container(reason: &str, message: &str) -> Value {
    json!({"phase":"Pending","containerStatuses":[{
        "name":"runner","image":"x","imageID":"","ready":false,"restartCount":0,
        "state":{"waiting":{"reason":reason,"message":message}}
    }]})
}

fn pod_list(pods: Vec<Pod>) -> String {
    serde_json::to_string(&json!({
        "apiVersion": "v1", "kind": "PodList", "metadata": {}, "items": pods
    }))
    .expect("a serialisable list")
}

/// A well-formed relay for a two-topic inventory.
fn good_log() -> String {
    let entries = vec![TopicEntry::new("orders", 6), TopicEntry::new("payments", 3)];
    let result = br#"{"contract":"logweir.dev/check-result/v1","kind":"topicInventory"}"#;
    let parts = frames::write_parts(Stream::Result, result).expect("small enough to frame");
    let mut streams = BTreeMap::new();
    streams.insert(Stream::Result, (result.to_vec(), parts.len()));
    let end = frames::end_frame(PLAN_SHA, SUBJECT_UID, &streams, Some(&entries));

    let mut out = String::new();
    // A non-frame line FIRST, because the decoder must ignore it (D2 §4.5:
    // the KafkaCluster probe prints its I14 lines on the same stdout).
    out.push_str("cluster-id=M29I2S7FQPyHBEX12Vx7XA\n");
    for e in &entries {
        out.push_str(&frames::write_topic_line(e).expect("a framable entry"));
        out.push('\n');
    }
    for p in parts {
        out.push_str(&p);
        out.push('\n');
    }
    out.push_str(&frames::write_end(&end).expect("a framable end"));
    out.push('\n');
    out
}

// ---------------------------------------------------------------------------
// The Job shape — D2 §4.3's `job.rs` row
// ---------------------------------------------------------------------------

#[test]
fn a_check_job_name_is_a_pure_function_of_the_kind_and_the_owner_uid() {
    let name = cjob::check_job_name(CheckPlanKind::TopicInventory, SUBJECT_UID);
    assert!(name.starts_with("lwc-td-"), "{name}");
    assert_eq!(
        name.len(),
        27,
        "the name is always 27 characters, whatever the owner is called: {name}"
    );
    assert!(
        name.len() <= 63,
        "a Job name has to fit the batch.kubernetes.io/job-name label"
    );
    // The owner's NAME is not in it, which is what removes the `NameTooLong`
    // path the KafkaCluster probe has to carry.
    let long_owner = RunnerOwner {
        name: "a".repeat(200),
        ..spec().owner
    };
    let mut s = spec();
    s.owner = long_owner;
    assert_eq!(
        s.job_name(),
        name,
        "a 200-character owner name changes nothing"
    );

    // Every kind gets its own discriminator, so two checks on the same subject
    // are two Jobs.
    let mut seen: Vec<String> = CheckPlanKind::ALL
        .iter()
        .map(|k| cjob::check_job_name(*k, SUBJECT_UID))
        .collect();
    seen.sort();
    seen.dedup();
    assert_eq!(seen.len(), CheckPlanKind::ALL.len());
    // And two subjects get two Jobs for the same kind.
    assert_ne!(
        cjob::check_job_name(CheckPlanKind::TopicInventory, OTHER_JOB_UID),
        name
    );
}

#[test]
fn the_check_job_is_the_runner_job_shape_plus_labels() {
    let built = cjob::build(&spec());
    let plain = weirkeeper::job::build(&cjob::runner_job_spec(&spec()));

    // THE DEFERRED `job.rs` SEAM. Everything except the two label maps is
    // byte-identical to what `job::build` produces, which is what makes the
    // later move of `labels`/`template_labels` into `RunnerJobSpec` a no-op.
    let mut stripped = built.clone();
    stripped.metadata.labels = None;
    if let Some(s) = stripped.spec.as_mut() {
        s.template.metadata = None;
    }
    assert_eq!(
        serde_json::to_value(&stripped).unwrap(),
        serde_json::to_value(&plain).unwrap(),
        "the check Job differs from the runner Job shape by more than its labels"
    );

    let labels = built.metadata.labels.clone().expect("labels");
    assert_eq!(
        labels.get(cjob::LABEL_MANAGED_BY).map(String::as_str),
        Some("weirkeeper")
    );
    assert_eq!(
        labels.get(cjob::LABEL_COMPONENT).map(String::as_str),
        Some("check")
    );
    assert_eq!(
        labels.get(cjob::LABEL_CHECK_KIND).map(String::as_str),
        Some("topicInventory")
    );
    assert_eq!(
        labels.get(cjob::LABEL_CHECK_OWNER_UID).map(String::as_str),
        Some(SUBJECT_UID)
    );
    let template = built
        .spec
        .as_ref()
        .and_then(|s| s.template.metadata.as_ref())
        .and_then(|m| m.labels.clone())
        .expect("template labels");
    assert_eq!(
        template, labels,
        "the Job and its pod template carry one map"
    );
}

#[test]
fn a_check_job_carries_the_contract_env_the_deadline_margin_and_no_ttl() {
    let built = cjob::build(&spec());
    let job_spec = built.spec.as_ref().expect("a spec");
    assert_eq!(
        job_spec.active_deadline_seconds,
        Some(120 + cjob::DEADLINE_MARGIN_SECONDS)
    );
    assert_eq!(
        job_spec.ttl_seconds_after_finished, None,
        "a TTL at creation time lets pod garbage collection race the relay read"
    );
    assert_eq!(job_spec.backoff_limit, Some(0));

    let container = &job_spec.template.spec.as_ref().unwrap().containers[0];
    assert_eq!(container.name, "runner");
    assert_eq!(
        container.args.as_deref(),
        Some(
            [
                "check",
                "run",
                "--plan",
                "/check/check-plan.json",
                "--check-contract-version",
                "1"
            ]
            .map(String::from)
            .as_slice()
        )
    );
    let env: BTreeMap<String, String> = container
        .env
        .as_ref()
        .unwrap()
        .iter()
        .filter_map(|e| e.value.clone().map(|v| (e.name.clone(), v)))
        .collect();
    assert_eq!(
        env.get(cjob::CONTRACT_VERSION_ENV).map(String::as_str),
        Some(CHECK_CONTRACT_VERSION.to_string().as_str())
    );
    assert_eq!(
        env.get(cjob::PLAN_SHA256_ENV).map(String::as_str),
        Some(PLAN_SHA)
    );
    assert_eq!(
        env.get(cjob::SUBJECT_UID_ENV).map(String::as_str),
        Some(SUBJECT_UID)
    );
    assert_eq!(env.get("RUST_LOG").map(String::as_str), Some("warn"));
    assert_eq!(env.get("TMPDIR").map(String::as_str), Some("/work"));

    // The plan is mounted at /check and NOT through `RunnerJobSpec::plan_config_map`.
    let mounts = container.volume_mounts.as_ref().unwrap();
    assert!(
        mounts
            .iter()
            .any(|m| m.mount_path == "/check" && m.read_only == Some(true)),
        "the check plan is mounted read-only at /check: {mounts:?}"
    );
    assert!(
        !mounts.iter().any(|m| m.mount_path == "/plan"),
        "a check has no /plan mount"
    );
    let pod_spec = job_spec.template.spec.as_ref().unwrap();
    assert_eq!(pod_spec.automount_service_account_token, Some(false));
    assert_eq!(
        pod_spec.service_account_name.as_deref(),
        Some("logweir-runner")
    );
}

// ---------------------------------------------------------------------------
// D-SEAMS S6 — pod identity
// ---------------------------------------------------------------------------

#[test]
fn only_a_controller_owner_reference_with_the_jobs_uid_counts() {
    let owned = pod_object(Some(("Job", JOB_UID, true)), terminated(0));
    assert!(cpod::is_owned_by_job(&owned, JOB_UID));

    // Each of the three conditions, failed one at a time.
    assert!(
        !cpod::is_owned_by_job(
            &pod_object(Some(("Job", OTHER_JOB_UID, true)), terminated(0)),
            JOB_UID
        ),
        "another Job's UID"
    );
    assert!(
        !cpod::is_owned_by_job(
            &pod_object(Some(("Job", JOB_UID, false)), terminated(0)),
            JOB_UID
        ),
        "a non-controller reference is an association somebody else made"
    );
    assert!(
        !cpod::is_owned_by_job(
            &pod_object(Some(("ReplicaSet", JOB_UID, true)), terminated(0)),
            JOB_UID
        ),
        "a ReplicaSet that happens to share the UID string is not this Job"
    );
    assert!(
        !cpod::is_owned_by_job(&pod_object(None, terminated(0)), JOB_UID),
        "an ownerless pod is owned by nothing"
    );
}

/// D2 §12's PLAT-09.1 row
/// `pod::ignores_label_matching_pod_without_job_controller_owner`, over the
/// double, so the ABSENCE of a `pods/log` read is the assertion.
#[tokio::test]
async fn a_pod_that_wears_the_label_but_is_not_owned_is_never_read() {
    for impostor in [
        pod_object(Some(("Job", OTHER_JOB_UID, true)), terminated(0)),
        pod_object(None, terminated(0)),
        pod_object(Some(("Job", JOB_UID, false)), terminated(0)),
    ] {
        // THE TABLE HAS NO `pods/log` ROUTE. The double panics on a request it
        // was not given a route for, so a framework that read the impostor's
        // stdout fails here by panicking rather than by returning bad data.
        let routes = vec![Route {
            method: "GET",
            path_suffix: "/pods",
            status: 200,
            body: pod_list(vec![impostor]),
        }];
        let (client, recorder, _) = mock_client_recording_bodies(routes);
        let observation = check::observe(
            &client,
            NS,
            &job_object(Some("Complete"), None),
            &[],
            &expectations(),
            now(),
        )
        .await
        .expect("the list answered");
        assert_eq!(observation.phase, CheckPhase::Failed);
        assert_eq!(
            observation.reason,
            CheckCode::ResultUnreadable,
            "with no owned pod there is no relay, and nothing is inferred from an exit code \
             that belongs to somebody else's pod"
        );
        let seen = recorder.lock().unwrap();
        assert_eq!(seen.len(), 1, "exactly one call was made: {seen:?}");
        assert!(
            !seen[0].uri.contains(POD_LOG_PATH),
            "the framework read a foreign pod's log: {seen:?}"
        );
    }
}

/// The happy path: an owned pod, a verified relay, and the TTL patched
/// afterwards.
#[tokio::test]
async fn an_owned_pod_with_a_verified_relay_succeeds_and_then_takes_a_ttl() {
    let routes = vec![
        Route {
            method: "GET",
            path_suffix: "/pods",
            status: 200,
            body: pod_list(vec![pod_object(
                Some(("Job", JOB_UID, true)),
                terminated(0),
            )]),
        },
        Route {
            method: "GET",
            path_suffix: POD_LOG_PATH,
            status: 200,
            body: good_log(),
        },
        Route {
            method: "PATCH",
            path_suffix: JOB_PATH,
            status: 200,
            body: r#"{"apiVersion":"batch/v1","kind":"Job","metadata":{"name":"x"}}"#.to_string(),
        },
    ];
    let (client, recorder, bodies) = mock_client_recording_bodies(routes);
    let observation = check::observe(
        &client,
        NS,
        &job_object(Some("Complete"), None),
        &[],
        &expectations(),
        now(),
    )
    .await
    .expect("the double answered");

    assert_eq!(observation.phase, CheckPhase::Succeeded);
    assert_eq!(observation.reason, CheckCode::Succeeded);
    assert_eq!(observation.exit_code, Some(0));
    let relay = observation.relay.expect("a verified relay");
    assert_eq!(relay.topics.len(), 2);
    assert_eq!(
        relay.end.topic_lines.as_ref().map(|t| t.sha256.clone()),
        Some(topic_tsv_sha256(&relay.topics)),
        "the decoder verified the topic digest it was given"
    );
    assert!(!observation.cancel_now);

    // THE TTL IS SENT ONLY BY A CALLER THAT ASKS, AND ONLY AFTER ITS COMMIT.
    // `observe` itself sends no PATCH — asserted by the recorder below.
    {
        let seen = recorder.lock().unwrap();
        assert_eq!(
            seen.iter().filter(|r| r.method == "PATCH").count(),
            0,
            "observe must not write anything: {seen:?}"
        );
    }

    check::set_ttl(&client, NS, &job_name())
        .await
        .expect("the patch route answered");
    let bodies = bodies.lock().unwrap();
    let patch = bodies
        .iter()
        .find(|b| b.method == "PATCH")
        .expect("the TTL patch was sent");
    let body: Value = serde_json::from_str(&patch.body).expect("a merge patch body");
    assert_eq!(
        body["spec"]["ttlSecondsAfterFinished"],
        json!(cjob::TTL_SECONDS)
    );
    assert_eq!(
        body["spec"].as_object().map(serde_json::Map::len),
        Some(1),
        "the TTL patch touches one field and nothing else: {body}"
    );
}

#[tokio::test]
async fn a_running_job_reads_no_log_at_all() {
    let routes = vec![Route {
        method: "GET",
        path_suffix: "/pods",
        status: 200,
        body: pod_list(vec![pod_object(
            Some(("Job", JOB_UID, true)),
            json!({"phase":"Running"}),
        )]),
    }];
    let (client, recorder, _) = mock_client_recording_bodies(routes);
    let observation = check::observe(
        &client,
        NS,
        &job_object(None, None),
        &[],
        &expectations(),
        now(),
    )
    .await
    .expect("the list answered");
    assert_eq!(observation.phase, CheckPhase::Running);
    assert_eq!(observation.reason, CheckCode::PodNotStarted);
    assert!(observation.relay.is_none());
    let seen = recorder.lock().unwrap();
    assert!(
        seen.iter().all(|r| !r.uri.contains(POD_LOG_PATH)),
        "a running check's stdout has no end frame yet; reading it would always be \
         ResultUnreadable: {seen:?}"
    );
}

// ---------------------------------------------------------------------------
// The relay — D2 §4.3's `relay.rs` row
// ---------------------------------------------------------------------------

#[test]
fn the_log_read_names_the_runner_container_and_a_byte_limit() {
    let p = relay::log_params();
    assert_eq!(p.container.as_deref(), Some("runner"));
    assert_eq!(p.limit_bytes, Some(relay::RELAY_LIMIT_BYTES));
    assert_eq!(relay::RELAY_LIMIT_BYTES, 8 * 1024 * 1024);
    assert!(!p.follow, "a check's log is read once, not streamed");
    assert!(
        !p.previous,
        "there is no previous container: restartPolicy is Never"
    );
}

/// D2 §12's framework rows `relay::missing_end_line_is_result_unreadable`,
/// `relay::wrong_plan_sha_is_refused` and `relay::part_digest_mismatch`, plus
/// the truncation case the brief names.
#[test]
fn every_way_a_relay_can_fail_to_verify_is_result_unreadable() {
    let good = good_log();
    assert!(
        relay::decode(&good, &expectations()).is_ok(),
        "the fixture verifies"
    );

    // 1. TRUNCATED: the end frame never arrived, which is what an 8 MiB
    //    `limitBytes` cut or a killed pod looks like.
    let truncated: String = good
        .lines()
        .filter(|l| !l.starts_with("logweir-check-end="))
        .map(|l| format!("{l}\n"))
        .collect();
    let e = relay::decode(&truncated, &expectations()).expect_err("no end frame");
    assert_eq!(e.code, CheckCode::ResultUnreadable);

    // 2. TRUNCATED MID-STREAM: the end frame declares two parts and one arrived.
    let cut: String = good
        .lines()
        .filter(|l| !l.contains("logweir-check-part=result:1/"))
        .map(|l| format!("{l}\n"))
        .collect();
    if cut != good {
        assert_eq!(
            relay::decode(&cut, &expectations())
                .expect_err("a missing part")
                .code,
            CheckCode::ResultUnreadable
        );
    }

    // 3. A TOPIC LINE DROPPED: count and digest both disagree.
    let short: String = good
        .lines()
        .filter(|l| !l.starts_with("logweir-check-topic=payments"))
        .map(|l| format!("{l}\n"))
        .collect();
    assert_eq!(
        relay::decode(&short, &expectations())
            .expect_err("a dropped topic")
            .code,
        CheckCode::ResultUnreadable
    );

    // 4. THE WRONG PLAN. A relay that verifies against another plan's digest is
    //    a relay from another check's Job.
    let other = FrameExpectations {
        plan_sha256: "sha256:2222222222222222222222222222222222222222222222222222222222222222"
            .to_string(),
        subject_uid: SUBJECT_UID.to_string(),
    };
    assert_eq!(
        relay::decode(&good, &other)
            .expect_err("a wrong plan digest")
            .code,
        CheckCode::ResultUnreadable
    );

    // 5. THE WRONG SUBJECT.
    let other = FrameExpectations {
        plan_sha256: PLAN_SHA.to_string(),
        subject_uid: OTHER_JOB_UID.to_string(),
    };
    assert_eq!(
        relay::decode(&good, &other)
            .expect_err("a wrong subject uid")
            .code,
        CheckCode::ResultUnreadable
    );
}

#[test]
fn a_relay_refusal_never_carries_log_content() {
    let secret_bearing = format!(
        "{}\nAWS_SECRET_ACCESS_KEY=wJalrXUtnFEMIK7MDENGbPxRfiCYEXAMPLEKEY\n",
        good_log()
            .lines()
            .filter(|l| !l.starts_with("logweir-check-end="))
            .collect::<Vec<_>>()
            .join("\n")
    );
    let e = relay::decode(&secret_bearing, &expectations()).expect_err("no end frame");
    assert!(
        !e.reason.contains("wJalrXUtnFEMIK7MDENGbPxRfiCYEXAMPLEKEY"),
        "the refusal quoted the log: {}",
        e.reason
    );
    assert!(
        !e.reason.contains("logweir-check-topic="),
        "the refusal quoted a frame: {}",
        e.reason
    );
}

/// The runner's exit-3 contract refusal, read BY KEY NAME from a bounded tail
/// (plan erratum E4) — and read BEFORE the frames, because exit 3 prints none.
#[test]
fn a_contract_refusal_is_read_by_key_name_and_beats_the_absent_frames() {
    let log = "some tracing on stderr\nrefusal-reason=CheckContractMismatch\nmore noise\n";
    assert_eq!(
        relay::refusal_reason(log),
        Some(CheckCode::CheckContractMismatch)
    );
    // The LAST occurrence within the tail wins.
    assert_eq!(
        relay::refusal_reason("refusal-reason=Timeout\nrefusal-reason=DeadlineExceeded\n"),
        Some(CheckCode::DeadlineExceeded)
    );
    // A value outside the closed vocabulary is an ABSENCE, never a fabricated
    // reason: this string becomes a metav1.Condition reason verbatim.
    assert_eq!(relay::refusal_reason("refusal-reason=SomethingNew\n"), None);
    assert_eq!(relay::refusal_reason("nothing here\n"), None);

    let observation = check::classify(&Input {
        job: &job_object(Some("Failed"), None),
        pod: Some(&pod_object(Some(("Job", JOB_UID, true)), terminated(3))),
        events: &[],
        log: Some(log),
        expect: &expectations(),
        now: now(),
    });
    assert_eq!(observation.phase, CheckPhase::Failed);
    assert_eq!(
        observation.reason,
        CheckCode::CheckContractMismatch,
        "a correct exit-3 refusal must not be reported as an unreadable result"
    );
    assert_eq!(observation.exit_code, Some(3));
}

// ---------------------------------------------------------------------------
// The waiting table — D2 §4.3
// ---------------------------------------------------------------------------

fn classify_pod(container: Value, events: &[EventFact], now: DateTime<Utc>) -> Option<Waiting> {
    let pod = pod_object(Some(("Job", JOB_UID, true)), container);
    waiting::classify(&waiting::Observed {
        job: &job_object(None, None),
        pod: Some(&pod),
        events,
        now,
    })
}

#[test]
fn every_waiting_row_of_the_table_has_its_own_code() {
    // 1. A missing Secret, naming it.
    let w = classify_pod(
        waiting_container(
            "CreateContainerConfigError",
            r#"secret "orders-sasl" not found"#,
        ),
        &[],
        now(),
    )
    .expect("classified");
    assert_eq!(w.code, CheckCode::CredentialSecretNotFound);
    assert_eq!(w.secret.as_deref(), Some("orders-sasl"));
    assert!(
        w.is_terminal(),
        "a Secret that is not there will not appear by waiting"
    );

    // 2. A missing KEY inside a Secret that exists, naming both.
    let w = classify_pod(
        waiting_container(
            "CreateContainerConfigError",
            "couldn't find key password in Secret logweir-d2-w5/orders-sasl",
        ),
        &[],
        now(),
    )
    .expect("classified");
    assert_eq!(w.code, CheckCode::CredentialSecretKeyMissing);
    assert_eq!(w.secret.as_deref(), Some("orders-sasl"));
    assert_eq!(w.key.as_deref(), Some("password"));

    // 3. A missing trust bundle.
    let w = classify_pod(
        waiting_container(
            "CreateContainerConfigError",
            r#"configmap "minio-ca" not found"#,
        ),
        &[],
        now(),
    )
    .expect("classified");
    assert_eq!(w.code, CheckCode::TrustBundleNotFound);
    assert_eq!(w.config_map.as_deref(), Some("minio-ca"));

    // 4-6. The three image rows.
    for (reason, code, terminal) in [
        ("ErrImagePull", CheckCode::RunnerImagePullFailed, false),
        ("ImagePullBackOff", CheckCode::RunnerImagePullFailed, false),
        ("ErrImageNeverPull", CheckCode::RunnerImageNotPresent, true),
        ("InvalidImageName", CheckCode::RunnerImageInvalid, true),
    ] {
        let w = classify_pod(waiting_container(reason, "x"), &[], now()).expect("classified");
        assert_eq!(w.code, code, "{reason}");
        assert_eq!(w.is_terminal(), terminal, "{reason}");
    }

    // 7. Unschedulable, only AFTER its grace period.
    let unschedulable = json!({"phase":"Pending","conditions":[{
        "type":"PodScheduled","status":"False","reason":"Unschedulable",
        "lastTransitionTime":"2026-09-16T11:59:00Z"
    }]});
    assert_eq!(
        classify_pod(unschedulable.clone(), &[], utc(12, 0, 0)).map(|w| w.code),
        Some(CheckCode::PodUnschedulable),
        "the condition transitioned 60 s ago, which is exactly the grace boundary"
    );
    assert_eq!(
        classify_pod(unschedulable, &[], utc(11, 59, 59)),
        None,
        "one second inside the grace period is not yet a finding"
    );

    // 8. A FailedMount on the signing volume, and on any other.
    let creating = json!({"phase":"Pending","containerStatuses":[{
        "name":"runner","image":"x","imageID":"","ready":false,"restartCount":0,
        "state":{"waiting":{"reason":"ContainerCreating","message":""}}
    }]});
    let mount = |volume: &str| EventFact {
        reason: "FailedMount".to_string(),
        message: format!(r#"Unable to attach or mount volumes: unmounted volume "{volume}""#),
        involved_kind: "Pod".to_string(),
        involved_name: POD.to_string(),
    };
    let w = classify_pod(creating.clone(), &[mount("signing")], utc(12, 0, 0)).expect("classified");
    assert_eq!(w.code, CheckCode::SigningKeyMissing);
    assert_eq!(w.volume.as_deref(), Some("signing"));
    let w =
        classify_pod(creating.clone(), &[mount("archive-ca")], utc(12, 0, 0)).expect("classified");
    assert_eq!(w.code, CheckCode::VolumeMountFailed);
    assert_eq!(w.volume.as_deref(), Some("archive-ca"));
    assert_eq!(
        classify_pod(creating, &[mount("signing")], utc(11, 58, 30)),
        None,
        "inside the mount grace period a slow mount is not a finding"
    );

    // 9. A disrupted pod beats everything else that is true at the same time.
    let disrupted = json!({
        "phase":"Pending",
        "conditions":[{"type":"DisruptionTarget","status":"True",
                       "lastTransitionTime":"2026-09-16T11:59:00Z"}],
        "containerStatuses":[{"name":"runner","image":"x","imageID":"","ready":false,
            "restartCount":0,"state":{"waiting":{"reason":"ErrImagePull","message":"x"}}}]
    });
    assert_eq!(
        classify_pod(disrupted, &[], now()).map(|w| w.code),
        Some(CheckCode::DisruptedMidCheck)
    );
}

#[test]
fn a_job_with_no_pod_is_explained_by_its_failed_create_event_after_the_grace_period() {
    let sa_missing = EventFact {
        reason: "FailedCreate".to_string(),
        message: r#"Error creating: pods "lwc-td-x" is forbidden: error looking up service account logweir-d2-w5/logweir-runner: serviceaccount "logweir-runner" not found"#.to_string(),
        involved_kind: "Job".to_string(),
        involved_name: job_name(),
    };
    let quota = EventFact {
        reason: "FailedCreate".to_string(),
        message: "Error creating: pods \"lwc-td-x\" is forbidden: exceeded quota: compute"
            .to_string(),
        involved_kind: "Job".to_string(),
        involved_name: job_name(),
    };
    let at = |events: &[EventFact], now: DateTime<Utc>| {
        waiting::classify(&waiting::Observed {
            job: &job_object(None, None),
            pod: None,
            events,
            now,
        })
    };
    assert_eq!(
        at(std::slice::from_ref(&sa_missing), utc(11, 58, 30)).map(|w| w.code),
        Some(CheckCode::RunnerServiceAccountMissing),
        "30 s after the Job was created is exactly the grace boundary"
    );
    assert_eq!(
        at(&[sa_missing], utc(11, 58, 29)),
        None,
        "inside the grace period the Job simply has not made its pod yet"
    );
    assert_eq!(
        at(&[quota], now()).map(|w| w.code),
        Some(CheckCode::PodCreateRejected)
    );
    assert_eq!(at(&[], now()), None, "no event, no finding");
}

#[test]
fn a_jobs_own_deadline_is_a_code_and_not_an_unreadable_relay() {
    let job = job_object(Some("Failed"), Some("DeadlineExceeded"));
    assert!(waiting::job_deadline_exceeded(&job));
    let observation = check::classify(&Input {
        job: &job,
        pod: Some(&pod_object(
            Some(("Job", JOB_UID, true)),
            json!({"phase":"Failed"}),
        )),
        events: &[],
        log: Some("no frames here at all\n"),
        expect: &expectations(),
        now: now(),
    });
    assert_eq!(observation.phase, CheckPhase::Failed);
    assert_eq!(
        observation.reason,
        CheckCode::DeadlineExceeded,
        "a pod killed by its deadline wrote no relay; reporting ResultUnreadable would hide why"
    );
}

#[test]
fn a_terminal_waiting_state_asks_for_an_immediate_cancel() {
    let observation = check::classify(&Input {
        job: &job_object(None, None),
        pod: Some(&pod_object(
            Some(("Job", JOB_UID, true)),
            waiting_container("CreateContainerConfigError", r#"secret "gone" not found"#),
        )),
        events: &[],
        log: None,
        expect: &expectations(),
        now: now(),
    });
    assert_eq!(observation.phase, CheckPhase::Failed);
    assert_eq!(observation.reason, CheckCode::CredentialSecretNotFound);
    assert!(
        observation.cancel_now,
        "a user should not watch a spinner for a Secret that does not exist"
    );

    // Unschedulable is deliberately NOT an immediate cancel: a node can join.
    let observation = check::classify(&Input {
        job: &job_object(None, None),
        pod: Some(&pod_object(
            Some(("Job", JOB_UID, true)),
            json!({"phase":"Pending","conditions":[{
                "type":"PodScheduled","status":"False","reason":"Unschedulable",
                "lastTransitionTime":"2026-09-16T11:58:00Z"}]}),
        )),
        events: &[],
        log: None,
        expect: &expectations(),
        now: now(),
    });
    assert_eq!(observation.reason, CheckCode::PodUnschedulable);
    assert!(!observation.cancel_now);
}

// ---------------------------------------------------------------------------
// Cancellation
// ---------------------------------------------------------------------------

#[tokio::test]
async fn cancel_patches_the_deadline_only_on_an_owned_unfinished_job() {
    // A foreign Job with the same NAME is never touched: the table has no
    // PATCH route, so a framework that sent one panics here.
    let (client, _, _) = mock_client_recording_bodies(Vec::new());
    let foreign: Job = serde_json::from_value(json!({
        "apiVersion":"batch/v1","kind":"Job",
        "metadata":{"name":job_name(),"namespace":NS,"uid":OTHER_JOB_UID,
            "ownerReferences":[{"apiVersion":"apps/v1","kind":"Deployment","name":"x",
                "uid":"someone-else","controller":true}]},
        "spec":{"template":{"spec":{"containers":[],"restartPolicy":"Never"}}},
        "status":{}
    }))
    .unwrap();
    assert!(!check::is_cancellable(&foreign, SUBJECT_UID));
    assert!(!check::cancel(&client, NS, &foreign, SUBJECT_UID)
        .await
        .unwrap());

    // A FINISHED Job is not cancelled either: that would rewrite the reason it
    // finished for.
    assert!(!check::is_cancellable(
        &job_object(Some("Complete"), None),
        SUBJECT_UID
    ));
    assert!(!check::cancel(
        &client,
        NS,
        &job_object(Some("Complete"), None),
        SUBJECT_UID
    )
    .await
    .unwrap());

    // The owned, unfinished one is.
    let routes = vec![Route {
        method: "PATCH",
        path_suffix: JOB_PATH,
        status: 200,
        body: r#"{"apiVersion":"batch/v1","kind":"Job","metadata":{"name":"x"}}"#.to_string(),
    }];
    let (client, _, bodies) = mock_client_recording_bodies(routes);
    assert!(
        check::cancel(&client, NS, &job_object(None, None), SUBJECT_UID)
            .await
            .unwrap()
    );
    let bodies = bodies.lock().unwrap();
    let body: Value = serde_json::from_str(&bodies[0].body).unwrap();
    assert_eq!(body, json!({"spec":{"activeDeadlineSeconds":1}}));
}

// ---------------------------------------------------------------------------
// Attribution — D2 §4.3's "Mapping to check IDs"
// ---------------------------------------------------------------------------

#[test]
fn a_waiting_code_belongs_to_the_check_whose_projection_it_names() {
    let projections = Projections {
        connection_secret: Some("orders-sasl".to_string()),
        destination_secret: Some("minio-keys".to_string()),
        signer_secret: Some("logweir-signing-key".to_string()),
        trust_config_maps: vec!["minio-ca".to_string()],
    };
    let secret_not_found = |name: &str| Waiting {
        code: CheckCode::CredentialSecretNotFound,
        secret: Some(name.to_string()),
        key: None,
        config_map: None,
        volume: None,
        message: String::new(),
    };
    assert_eq!(
        check::attribute(&secret_not_found("orders-sasl"), &projections),
        Some(CheckId::ConnectionCredentialProjected)
    );
    assert_eq!(
        check::attribute(&secret_not_found("minio-keys"), &projections),
        Some(CheckId::DestinationCredentialProjected)
    );
    assert_eq!(
        check::attribute(&secret_not_found("logweir-signing-key"), &projections),
        Some(CheckId::SignerPrivateKeyUsable)
    );
    // THE UNMATCHED CASE IS `None`, AND IT IS A REAL ANSWER. Attributing a
    // Secret this plan did not project to the connection would send an
    // operator to rotate the wrong credential.
    assert_eq!(
        check::attribute(&secret_not_found("somebody-elses"), &projections),
        None
    );
    assert_eq!(
        check::attribute(
            &Waiting {
                code: CheckCode::RunnerImageNotPresent,
                secret: None,
                key: None,
                config_map: None,
                volume: None,
                message: String::new()
            },
            &projections
        ),
        Some(CheckId::RunnerImage)
    );
    assert_eq!(
        check::attribute(
            &Waiting {
                code: CheckCode::TrustBundleNotFound,
                secret: None,
                key: None,
                config_map: Some("minio-ca".to_string()),
                volume: None,
                message: String::new()
            },
            &projections
        ),
        Some(CheckId::DestinationResolved)
    );
}

// ---------------------------------------------------------------------------
// The policy ConfigMap — D2 §4.4
// ---------------------------------------------------------------------------

const GOOD_POLICY: &str = r#"{"version":1,
 "checks":{"maxActivePerNamespace":4,"maxActiveTotal":20,
           "maxActiveDiscoveriesPerConnection":1,"maxEvidenceFetchActivePerNamespace":4},
 "discovery":{"freshSeconds":900,"retentionSeconds":86400,"keepPerConnection":5,
              "defaultMaxTopics":20000,"hardMaxTopics":50000,
              "visibilityAttestations":[
                {"id":"att-orders-prod","namespace":"team-a","kafkaCluster":"source",
                 "clusterId":"M29I2S7FQPyHBEX12Vx7XA","principal":"User:backup",
                 "attestedBy":"platform-admin@example.invalid",
                 "attestedAt":"2026-09-15T00:00:00Z","expiresAt":"2026-12-15T00:00:00Z",
                 "statement":"reviewed ACL export 2026-09-14"}]},
 "preflight":{"defaultTimeoutSeconds":120,"retentionSeconds":3600},
 "engine":{"allowUnverifiedCustomCa":false},
 "evidence":{"controllerIdentityLocations":[
   {"endpoint":"https://minio-b.ns.svc:9000","region":"","bucket":"lw-b"}]},
 "legacyArchiveAddressing":{"endpoint":"","region":"","allowHttp":false,
                            "virtualHostedStyle":false}}"#;

#[tokio::test]
async fn an_absent_policy_is_the_documented_defaults_and_is_ready() {
    // No reference configured at all: nothing is read.
    let (client, recorder, _) = mock_client_recording_bodies(Vec::new());
    let cache = policy::PolicyCache::new();
    let load = policy::load(&client, None, &cache, now()).await.unwrap();
    assert_eq!(
        load,
        policy::PolicyLoad::Defaulted(policy::Policy::defaults())
    );
    assert_eq!(load.code(), CheckCode::PolicyLoaded);
    assert!(recorder.lock().unwrap().is_empty(), "nothing was read");

    // Configured, but the ConfigMap is not there: still the defaults.
    let routes = vec![Route {
        method: "GET",
        path_suffix: "/configmaps/weirkeeper-policy",
        status: 404,
        body: r#"{"kind":"Status","apiVersion":"v1","status":"Failure",
            "message":"configmaps \"weirkeeper-policy\" not found",
            "reason":"NotFound","code":404}"#
            .to_string(),
    }];
    let (client, _, _) = mock_client_recording_bodies(routes);
    let reference = (
        "logweir-system".to_string(),
        "weirkeeper-policy".to_string(),
    );
    let load = policy::load(
        &client,
        Some(&reference),
        &policy::PolicyCache::new(),
        now(),
    )
    .await
    .unwrap();
    assert!(matches!(load, policy::PolicyLoad::Defaulted(_)));
    assert_eq!(load.code(), CheckCode::PolicyLoaded);
    let p = load.policy();
    assert_eq!(p.checks.max_active_per_namespace, 4);
    assert_eq!(p.checks.max_active_total, 20);
    assert_eq!(p.discovery.default_max_topics, 20_000);
    assert_eq!(p.discovery.hard_max_topics, 50_000);
    assert_eq!(p.discovery.keep_per_connection, 5);
    assert_eq!(p.preflight.default_timeout_seconds, 120);
    assert!(
        p.discovery.visibility_attestations.is_empty(),
        "with no policy nothing is ever attestedComplete"
    );
    assert!(p.evidence.controller_identity_locations.is_empty());
    assert!(!p.legacy_archive_addressing.allow_http, "D-SEAMS S5");
}

#[tokio::test]
async fn a_well_formed_policy_is_loaded_and_carries_its_attestation() {
    let routes = vec![Route {
        method: "GET",
        path_suffix: "/configmaps/weirkeeper-policy",
        status: 200,
        body: serde_json::to_string(&json!({
            "apiVersion":"v1","kind":"ConfigMap",
            "metadata":{"name":"weirkeeper-policy","namespace":"logweir-system"},
            "data":{"policy.json": GOOD_POLICY}
        }))
        .unwrap(),
    }];
    let (client, recorder, _) = mock_client_recording_bodies(routes);
    let reference = (
        "logweir-system".to_string(),
        "weirkeeper-policy".to_string(),
    );
    let cache = policy::PolicyCache::new();
    let load = policy::load(&client, Some(&reference), &cache, now())
        .await
        .unwrap();
    assert!(matches!(load, policy::PolicyLoad::Loaded(_)));
    assert_eq!(load.code(), CheckCode::PolicyLoaded);
    let p = load.policy();
    assert_eq!(p.discovery.visibility_attestations.len(), 1);
    assert_eq!(p.discovery.visibility_attestations[0].id, "att-orders-prod");
    assert_eq!(p.evidence.controller_identity_locations[0].bucket, "lw-b");
    assert!(p.digest().starts_with("sha256:"));

    // THE CACHE. A second load inside the TTL sends nothing.
    let again = policy::load(&client, Some(&reference), &cache, now())
        .await
        .unwrap();
    assert_eq!(again, load);
    assert_eq!(recorder.lock().unwrap().len(), 1, "the 30 s cache held");
    // And past the TTL it reads again.
    let _ = policy::load(
        &client,
        Some(&reference),
        &cache,
        now() + chrono::Duration::seconds(31),
    )
    .await
    .unwrap();
    assert_eq!(recorder.lock().unwrap().len(), 2, "the cache expired");
}

/// MALFORMED FAILS CLOSED. Every case below leaves the attestation list and the
/// identity allowlist EMPTY, because a policy nobody can read must never be the
/// reason a topic listing is reported as complete (D-SEAMS S3).
#[test]
fn a_malformed_policy_is_refused_by_name_and_fails_closed() {
    let cases: &[(&str, &str)] = &[
        ("{", "truncated JSON"),
        (
            r#"{"version":2}"#,
            "a version this build does not understand",
        ),
        (
            r#"{"version":1,"checks":{"maxActivePerNamespace":0,"maxActiveTotal":20,
                "maxActiveDiscoveriesPerConnection":1,"maxEvidenceFetchActivePerNamespace":4}}"#,
            "a limit of zero would queue every check forever",
        ),
        (
            r#"{"version":1,"discovery":{"freshSeconds":900,"retentionSeconds":1,
                "keepPerConnection":5,"defaultMaxTopics":60000,"hardMaxTopics":50000}}"#,
            "defaultMaxTopics above hardMaxTopics",
        ),
        (
            r#"{"version":1,"discovery":{"freshSeconds":900,"retentionSeconds":1,
                "keepPerConnection":5,"defaultMaxTopics":10,"hardMaxTopics":999999}}"#,
            "hardMaxTopics above the contract ceiling",
        ),
        (
            r#"{"version":1,"somethingNew":true}"#,
            "an unknown field is a policy this build would silently ignore half of",
        ),
    ];
    for (body, why) in cases {
        let mut data = BTreeMap::new();
        data.insert(policy::POLICY_KEY.to_string(), (*body).to_string());
        let load = policy::from_data(Some(&data));
        let policy::PolicyLoad::Unreadable { policy: p, reason } = &load else {
            panic!("`{why}` was accepted: {load:?}");
        };
        assert_eq!(load.code(), CheckCode::PolicyUnreadable, "{why}");
        assert!(!load.is_readable(), "{why}");
        assert!(!reason.is_empty(), "the refusal must name itself: {why}");
        assert!(
            p.discovery.visibility_attestations.is_empty()
                && p.evidence.controller_identity_locations.is_empty(),
            "`{why}` did not fail closed"
        );
        // The limits still have their defaults, so checks keep running.
        assert_eq!(p.checks.max_active_per_namespace, 4, "{why}");
    }

    // A ConfigMap that exists with the WRONG KEY is malformed, not absent: an
    // administrator meant to configure something.
    let mut data = BTreeMap::new();
    data.insert("policy.yaml".to_string(), GOOD_POLICY.to_string());
    assert_eq!(
        policy::from_data(Some(&data)).code(),
        CheckCode::PolicyUnreadable
    );
    assert_eq!(policy::from_data(None).code(), CheckCode::PolicyUnreadable);

    // And the good one is accepted, so the cases above are not vacuous.
    let mut data = BTreeMap::new();
    data.insert(policy::POLICY_KEY.to_string(), GOOD_POLICY.to_string());
    assert_eq!(
        policy::from_data(Some(&data)).code(),
        CheckCode::PolicyLoaded
    );
}

#[test]
fn the_policy_reference_comes_from_the_environment_or_the_release_namespace() {
    assert_eq!(
        policy::configured_ref(Some("logweir-system/weirkeeper-policy"), None),
        Some((
            "logweir-system".to_string(),
            "weirkeeper-policy".to_string()
        ))
    );
    // Only the namespace: the default name in it.
    assert_eq!(
        policy::configured_ref(None, Some("logweir-system")),
        Some((
            "logweir-system".to_string(),
            policy::DEFAULT_POLICY_NAME.to_string()
        ))
    );
    // Neither: no policy is configured, which is Defaulted and not an error.
    assert_eq!(policy::configured_ref(None, None), None);
    assert_eq!(policy::configured_ref(Some("  "), None), None);
    // A malformed reference names no object, so nothing is read.
    assert_eq!(policy::configured_ref(Some("no-slash"), None), None);
    assert_eq!(policy::configured_ref(Some("/name"), None), None);
    assert_eq!(policy::configured_ref(Some("ns/"), None), None);
}

// ---------------------------------------------------------------------------
// Aggregation
// ---------------------------------------------------------------------------

#[test]
fn an_unattributable_waiting_code_makes_the_whole_set_unknown() {
    use logweir_core::check_contract::{Authority, CheckOutcome, CheckState, Gating, OverallState};
    let ready = CheckOutcome::new(
        CheckId::ConnectionResolved,
        CheckState::Ready,
        Gating::Blocking,
        Authority::Controller,
        CheckCode::Resolved,
    );
    assert_eq!(
        check::overall(std::slice::from_ref(&ready), None),
        OverallState::Ready
    );
    let blocker = Waiting {
        code: CheckCode::CredentialSecretNotFound,
        secret: Some("somebody-elses".to_string()),
        key: None,
        config_map: None,
        volume: None,
        message: String::new(),
    };
    assert_eq!(
        check::overall(&[ready], Some(&blocker)),
        OverallState::Unknown,
        "a pod that could not start makes every Job-sourced answer unknown"
    );
    // The empty set is Unknown and never Ready — the pure crate's rule,
    // restated here because a controller reaches it through this wrapper.
    assert_eq!(check::overall(&[], None), OverallState::Unknown);
}

// ---------------------------------------------------------------------------
// The EventFact converter
// ---------------------------------------------------------------------------

#[test]
fn an_event_reduces_to_the_four_fields_the_table_reads() {
    let event: Event = serde_json::from_value(json!({
        "apiVersion":"v1","kind":"Event",
        "metadata":{"name":"lwc-td-x.1","namespace":NS},
        "reason":"FailedMount",
        "message":"Unable to attach or mount volumes: unmounted volume \"signing\"",
        "involvedObject":{"kind":"Pod","name":POD,"namespace":NS}
    }))
    .unwrap();
    let fact = EventFact::from_event(&event).expect("a reason is present");
    assert_eq!(fact.reason, "FailedMount");
    assert_eq!(fact.involved_kind, "Pod");
    assert_eq!(fact.involved_name, POD);

    let no_reason: Event = serde_json::from_value(json!({
        "apiVersion":"v1","kind":"Event","metadata":{"name":"x","namespace":NS},
        "involvedObject":{}
    }))
    .unwrap();
    assert_eq!(
        EventFact::from_event(&no_reason),
        None,
        "an event with no reason could match no row"
    );
}

// ---------------------------------------------------------------------------
// Source guards — the two rules a fixture cannot express
// ---------------------------------------------------------------------------

fn check_sources() -> Vec<(String, String)> {
    let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src/check");
    let mut out = Vec::new();
    for e in std::fs::read_dir(&dir)
        .expect("the check module ships")
        .flatten()
    {
        let p = e.path();
        if p.extension().is_some_and(|x| x == "rs") {
            out.push((
                p.file_name().unwrap().to_string_lossy().into_owned(),
                std::fs::read_to_string(&p).expect("a readable module"),
            ));
        }
    }
    out.sort();
    assert!(out.len() >= 5, "the scan found only {} modules", out.len());
    out
}

fn code_only(src: &str) -> String {
    src.lines()
        .filter(|l| {
            let t = l.trim_start();
            !(t.starts_with("//") || t.starts_with("///") || t.starts_with("//!"))
        })
        .collect::<Vec<_>>()
        .join("\n")
}

/// **No check path falls back to the legacy pod label.**
///
/// [`weirkeeper::controllers::backup::pod_selectors`] tries
/// `batch.kubernetes.io/job-name` and then the unprefixed `job-name`, because
/// the execution paths must work on a 1.29 cluster that has not backfilled the
/// prefixed one. New code does not need that — the prefixed label has been set
/// since 1.27 — and a second selector is a second chance for a foreign pod to
/// be considered at all (D2 §4.3: "No legacy-label fallback for check Jobs").
/// A fixture cannot express this: the fallback only fires when the first
/// selector returns nothing, which a route table always controls.
#[test]
fn no_check_module_names_the_legacy_pod_label_or_the_two_selector_helper() {
    for (name, src) in check_sources() {
        let code = code_only(&src);
        // The bare token, not only the quoted form: a fallback smuggled into
        // a `format!` string carries no quotes of its own. `JOB_NAME_LABEL` is
        // imported by name, and `job_name` spells its separator differently.
        for forbidden in ["JOB_NAME_LABEL_LEGACY", "pod_selectors(", "job-name"] {
            assert!(
                !code.contains(forbidden),
                "crates/weirkeeper/src/check/{name} names `{forbidden}`; a check's pod is found \
                 by one selector and then proved by its controller ownerReference UID"
            );
        }
    }

    // And the behavioural half, which no grep can give: ONE label, ONE value.
    let selector = cpod::pod_selector("lwc-td-0123456789abcdef0123");
    assert_eq!(
        selector,
        "batch.kubernetes.io/job-name=lwc-td-0123456789abcdef0123"
    );
    assert!(
        !selector.contains(','),
        "a comma is a second label in a Kubernetes selector: {selector}"
    );
    assert_eq!(selector.matches('=').count(), 1, "{selector}");
}

/// **The check framework builds no `Api<Event>` handle.**
///
/// The weirkeeper `ClusterRole` grants no verb on `events`, and
/// `crates/logweir/tests/manifest_lint.rs`'s `every_call_site_has_a_grant`
/// panics on an `Api<T>` whose resource it cannot map — so a handle added here
/// before W11's rule lands breaks that gate rather than 403ing in production.
/// [`weirkeeper::check::EventFact`] is the seam: the classifier takes the facts
/// as values, and the one handle belongs to the controller that will have the
/// grant. When W11 lands `events: [list, watch]` and the lint learns the type,
/// this test is what has to be deleted in the same commit — deliberately, so
/// the grant and the call arrive together.
#[test]
fn the_check_framework_builds_no_event_handle_before_its_grant_exists() {
    for (name, src) in check_sources() {
        let code = code_only(&src);
        assert!(
            !code.contains("Api<Event>"),
            "crates/weirkeeper/src/check/{name} builds an `Api<Event>`, which config/rbac/\
             role.yaml grants nothing on and manifest_lint cannot map to a resource"
        );
    }
    // And the seam it exists instead of: the converter names the k8s type
    // without ever building a handle for it.
    let waiting = check_sources()
        .into_iter()
        .find(|(n, _)| n == "waiting.rs")
        .expect("the classifier module");
    assert!(
        code_only(&waiting.1).contains("pub fn from_event(event: &Event)"),
        "EventFact::from_event is the seam that keeps the classifier complete without a grant"
    );
}

/// **Nothing in the check framework reads a clock.**
///
/// Every `now` is an argument, exactly as on the five existing reconcilers, so
/// a boundary like [`waiting::UNSCHEDULABLE_GRACE`] is assertable rather than
/// observable only by waiting — and so a re-read of the same objects produces a
/// byte-identical patch, which is what stopped the `KafkaCluster` probe's
/// measured 3,388-reconciles-in-ninety-seconds hot loop.
#[test]
fn no_check_module_reads_a_clock() {
    for (name, src) in check_sources() {
        let code = code_only(&src);
        for forbidden in ["Utc::now()", "SystemTime::now", "Instant::now"] {
            assert!(
                !code.contains(forbidden),
                "crates/weirkeeper/src/check/{name} names `{forbidden}`; `now` is an argument \
                 everywhere in this crate"
            );
        }
    }
}

// ---------------------------------------------------------------------------
// The plan ConfigMap — D2 §4.3, §12 "Framework / security additions"
// ---------------------------------------------------------------------------

fn documents() -> plan::PlanDocuments {
    plan::PlanDocuments {
        check_plan: br#"{"contract":"logweir.dev/check-plan/v1"}"#.to_vec(),
        source_ca: Some(b"-----BEGIN CERTIFICATE-----\nAA\n-----END CERTIFICATE-----\n".to_vec()),
        ..plan::PlanDocuments::default()
    }
}

fn plan_object() -> k8s_openapi::api::core::v1::ConfigMap {
    plan::build(&job_name(), NS, &owner(), &documents()).expect("UTF-8 documents")
}

#[test]
fn a_plan_config_map_is_immutable_owned_and_digest_pinned() {
    let cm = plan_object();
    assert_eq!(
        cm.metadata.name.as_deref(),
        Some(format!("{}-plan", job_name()).as_str())
    );
    assert_eq!(
        cm.immutable,
        Some(true),
        "a plan a second pass could rewrite is a document a running pod already mounted"
    );
    let owner_ref = &cm.metadata.owner_references.as_ref().expect("owners")[0];
    assert_eq!(owner_ref.uid, SUBJECT_UID);
    assert_eq!(owner_ref.controller, Some(true));
    assert_eq!(
        owner_ref.block_owner_deletion,
        Some(true),
        "the owner cascade is the delete; nothing in this crate calls Api::delete"
    );
    let data = cm.data.as_ref().expect("data");
    assert_eq!(
        data.keys().cloned().collect::<Vec<_>>(),
        vec!["check-plan.json".to_string(), "source-ca.pem".to_string()],
        "a CA the check does not have is ABSENT, not empty"
    );
    // The annotation and the Job's pinned env are ONE value.
    let digest = documents().check_plan_sha256();
    assert_eq!(
        cm.metadata
            .annotations
            .as_ref()
            .and_then(|a| a.get(plan::DIGEST_ANNOTATION))
            .map(String::as_str),
        Some(digest.as_str())
    );
    assert!(digest.starts_with("sha256:"));

    // A document that is not UTF-8 is named, not mangled.
    let bad = plan::PlanDocuments {
        check_plan: b"{}".to_vec(),
        archive_ca: Some(vec![0xff, 0xfe]),
        ..plan::PlanDocuments::default()
    };
    let e = plan::build(&job_name(), NS, &owner(), &bad).expect_err("invalid UTF-8");
    assert_eq!(e, plan::PlanError::NotUtf8("archive-ca.pem".to_string()));
    assert_eq!(e.code(), CheckCode::CheckPlanConflict);
}

/// D2 §12's framework row `plan::foreign_owner_409_is_conflict`.
///
/// A 409 is the HEALTHY duplicate-reconcile case and is adopted — but only when
/// the object there is the same document, owned by the same subject, and still
/// immutable. Each of the three failures is terminal, because retrying cannot
/// fix "two different plans want one name" and running the other one would
/// check inputs this pass never rendered.
#[tokio::test]
async fn plan_foreign_owner_409_is_conflict() {
    let digest = documents().check_plan_sha256();
    let cm = plan_object();

    // 1. THE HAPPY 409: identical document, same owner, immutable ⇒ adopted.
    let routes = vec![
        Route {
            method: "POST",
            path_suffix: "/configmaps",
            status: 409,
            body: r#"{"kind":"Status","apiVersion":"v1","status":"Failure",
                "message":"configmaps \"x\" already exists","reason":"AlreadyExists","code":409}"#
                .to_string(),
        },
        Route {
            method: "GET",
            path_suffix: PLAN_PATH,
            status: 200,
            body: serde_json::to_string(&cm).unwrap(),
        },
    ];
    let (client, _, _) = mock_client_recording_bodies(routes);
    assert_eq!(
        plan::ensure(&client, NS, &cm, SUBJECT_UID, &digest)
            .await
            .expect("an identical plan is adopted"),
        plan::PlanOutcome::Adopted
    );

    // 2-4. Each of the three ways it is NOT this check's plan.
    let mut foreign = cm.clone();
    foreign.metadata.owner_references.as_mut().unwrap()[0].uid = OTHER_JOB_UID.to_string();
    let mut wrong_digest = cm.clone();
    wrong_digest.metadata.annotations.as_mut().unwrap().insert(
        plan::DIGEST_ANNOTATION.to_string(),
        "sha256:ffff".to_string(),
    );
    let mut mutable = cm.clone();
    mutable.immutable = Some(false);
    let mut uncontrolled = cm.clone();
    uncontrolled.metadata.owner_references.as_mut().unwrap()[0].controller = Some(false);

    for (existing, why) in [
        (foreign, "another subject's UID"),
        (wrong_digest, "a different plan document"),
        (mutable, "an object that could still be rewritten"),
        (uncontrolled, "a non-controller owner reference"),
    ] {
        assert!(
            plan::accepts_existing(&existing, SUBJECT_UID, &digest).is_err(),
            "{why} was adopted"
        );
        let routes = vec![
            Route {
                method: "POST",
                path_suffix: "/configmaps",
                status: 409,
                body: r#"{"kind":"Status","apiVersion":"v1","status":"Failure",
                    "message":"already exists","reason":"AlreadyExists","code":409}"#
                    .to_string(),
            },
            Route {
                method: "GET",
                path_suffix: PLAN_PATH,
                status: 200,
                body: serde_json::to_string(&existing).unwrap(),
            },
        ];
        let (client, _, _) = mock_client_recording_bodies(routes);
        let e = plan::ensure(&client, NS, &cm, SUBJECT_UID, &digest)
            .await
            .expect_err(why);
        match e {
            plan::EnsureError::Plan(p) => assert_eq!(
                p.code(),
                CheckCode::CheckPlanConflict,
                "{why} must be terminal, not a requeue"
            ),
            plan::EnsureError::Api(e) => panic!("{why} was reported as transport: {e}"),
        }
    }
}

#[tokio::test]
async fn a_plan_that_did_not_exist_is_created_and_nothing_is_read() {
    let cm = plan_object();
    let routes = vec![Route {
        method: "POST",
        path_suffix: "/configmaps",
        status: 201,
        body: serde_json::to_string(&cm).unwrap(),
    }];
    let (client, recorder, _) = mock_client_recording_bodies(routes);
    assert_eq!(
        plan::ensure(
            &client,
            NS,
            &cm,
            SUBJECT_UID,
            &documents().check_plan_sha256()
        )
        .await
        .expect("created"),
        plan::PlanOutcome::Created
    );
    let seen = recorder.lock().unwrap();
    assert_eq!(seen.len(), 1, "the happy path reads nothing: {seen:?}");
    assert_eq!(seen[0].method, "POST");
}

// ---------------------------------------------------------------------------
// Result chunks — D2 §4.3, §5.5, §12
// ---------------------------------------------------------------------------

/// D2 §12's PLAT-09.1 unit row `chunks::size_bounds_worst_case_names`.
#[test]
fn chunks_size_bounds_worst_case_names() {
    // Worst-case shape: a 249-character name, six digits of partitions and the
    // full flag set with the longest code this pipeline can carry.
    let worst = |i: usize| {
        let mut e = TopicEntry::new(&format!("{i:09}{}", "n".repeat(240)), 999_999);
        e.internal = true;
        e.expected = true;
        e.error = Some(CheckCode::ClusterAuthorizationFailed);
        e
    };
    let entries: Vec<TopicEntry> = (0..6_000).map(worst).collect();
    let split = chunks::split(&entries);
    assert!(
        split.len() >= 2,
        "6,000 worst-case entries is more than one chunk"
    );
    for c in &split {
        assert!(
            c.lines <= chunks::MAX_CHUNK_LINES,
            "chunk {} carries {} lines",
            c.index,
            c.lines
        );
        assert!(
            c.data.len() <= chunks::MAX_CHUNK_BYTES,
            "chunk {} is {} bytes, over the 768 KiB bound",
            c.index,
            c.data.len()
        );
        assert!(
            c.data.len() < 1024 * 1024,
            "a chunk must stay under the API server's 1 MiB ConfigMap limit"
        );
        assert_eq!(
            c.sha256,
            logweir_core::ids::sha256_prefixed(c.data.as_bytes())
        );
    }
    // NOTHING IS LOST AND NOTHING IS DUPLICATED.
    assert_eq!(
        split.iter().map(|c| c.lines).sum::<usize>(),
        entries.len(),
        "every entry lands in exactly one chunk"
    );
    let rejoined: String = split.iter().map(|c| c.data.as_str()).collect();
    assert_eq!(
        rejoined,
        logweir_core::check_contract::topic_tsv(&entries),
        "the chunks concatenate back to the canonical TSV, in order"
    );
    // Indices are dense, zero-based and lexically ordered by name.
    for (i, c) in split.iter().enumerate() {
        assert_eq!(c.index, i);
    }
    assert_eq!(chunks::chunk_name("lwc-td-x", 0), "lwc-td-x-r000");
    assert_eq!(chunks::chunk_name("lwc-td-x", 19), "lwc-td-x-r019");
    assert!(
        chunks::chunk_name("j", 2) < chunks::chunk_name("j", 10),
        "lexical order"
    );

    // The typical shape D2 §5.5 tabulates: 5,003 entries ⇒ 3 chunks.
    let typical: Vec<TopicEntry> = (0..5_003)
        .map(|i| TopicEntry::new(&format!("orders-{i:05}"), 6))
        .collect();
    let split = chunks::split(&typical);
    assert_eq!(split.len(), 3, "5,003 typical entries at 2,500 lines each");
    assert_eq!(split[2].lines, 3);

    assert!(
        chunks::split(&[]).is_empty(),
        "an empty inventory writes no chunk"
    );

    // THE BYTE BOUND, EXERCISED ON ITS OWN. With Kafka's 249-character name
    // limit the LINE bound always bites first — 2,500 × 308 B is 770,000 B,
    // just under the 786,432 B ceiling — so nothing in the legal-name fixture
    // above can reach the byte bound, and dropping it changed no assertion in
    // this file. It is defence against a raised `MAX_CHUNK_LINES` or a longer
    // flag set, and `TopicEntry` does not enforce Kafka's limit, so this
    // reaches it directly.
    // Which bound bites at legal Kafka names, computed rather than asserted on
    // constants — `clippy::assertions_on_constants` refuses the latter, and it
    // is right to: a constant comparison is optimised out and guards nothing.
    // Feeding the real `split` a full chunk's worth of worst-case entries is
    // the same claim, made against the code.
    let full: Vec<TopicEntry> = (0..chunks::MAX_CHUNK_LINES).map(worst).collect();
    let split = chunks::split(&full);
    assert_eq!(
        split.len(),
        1,
        "{} worst-case legal entries still fit one chunk, so the LINE bound is the one that \
         bites at Kafka's 249-character name limit; the byte bound below is defence against a \
         raised MAX_CHUNK_LINES and is unreachable from the fixture above",
        chunks::MAX_CHUNK_LINES
    );
    assert!(split[0].data.len() <= chunks::MAX_CHUNK_BYTES);
    let fat: Vec<TopicEntry> = (0..3)
        .map(|i| TopicEntry::new(&format!("{i}{}", "n".repeat(400 * 1024)), 1))
        .collect();
    let split = chunks::split(&fat);
    assert_eq!(
        split.len(),
        3,
        "three 400 KiB entries are three chunks: the byte bound splits what the line bound \
         would have left as one"
    );
    for c in &split {
        assert_eq!(c.lines, 1);
        assert!(c.data.len() <= chunks::MAX_CHUNK_BYTES);
    }
    // A single entry larger than the whole bound lands alone rather than
    // looping forever or being dropped.
    let huge = [TopicEntry::new(&"n".repeat(chunks::MAX_CHUNK_BYTES + 1), 1)];
    let split = chunks::split(&huge);
    assert_eq!(split.len(), 1);
    assert_eq!(split[0].lines, 1);
}

/// D2 §12's framework row `chunks::immutable_false_is_conflict`.
#[test]
fn chunks_immutable_false_is_conflict() {
    let entries = [TopicEntry::new("orders", 6)];
    let split = chunks::split(&entries);
    let cm = chunks::build_chunk(&job_name(), NS, &owner(), &split[0], split.len());
    assert_eq!(cm.immutable, Some(true));
    assert_eq!(
        cm.metadata
            .annotations
            .as_ref()
            .and_then(|a| a.get(chunks::CHUNK_ANNOTATION))
            .map(String::as_str),
        Some("1/1"),
        "the chunk annotation is one-based i/n"
    );
    assert_eq!(
        cm.metadata
            .annotations
            .as_ref()
            .and_then(|a| a.get(chunks::FORMAT_ANNOTATION))
            .map(String::as_str),
        Some("logweir.dev/topic-inventory/v1")
    );

    assert!(chunks::accepts_existing(&cm, SUBJECT_UID, &split[0].sha256).is_ok());

    // THE ROW: an object that is not immutable is never adopted, because a
    // result an API response already served could still change under it.
    let mut mutable = cm.clone();
    mutable.immutable = Some(false);
    let e = chunks::accepts_existing(&mutable, SUBJECT_UID, &split[0].sha256)
        .expect_err("a mutable result is a conflict");
    assert_eq!(e.code(), CheckCode::ResultStorageConflict);
    assert!(e.to_string().contains("immutable"), "{e}");

    let mut absent = cm.clone();
    absent.immutable = None;
    assert!(chunks::accepts_existing(&absent, SUBJECT_UID, &split[0].sha256).is_err());

    // And the other two halves of the rule.
    let mut foreign = cm.clone();
    foreign.metadata.owner_references.as_mut().unwrap()[0].uid = OTHER_JOB_UID.to_string();
    assert!(chunks::accepts_existing(&foreign, SUBJECT_UID, &split[0].sha256).is_err());
    assert!(
        chunks::accepts_existing(&cm, SUBJECT_UID, "sha256:ffff").is_err(),
        "a different digest at the same name is a conflict"
    );

    // The details document takes the same shape with its own format.
    let details = chunks::build_details(&job_name(), NS, &owner(), "{\"missing\":1}\n");
    assert_eq!(details.immutable, Some(true));
    assert_eq!(
        details.metadata.name.as_deref(),
        Some(format!("{}-details", job_name()).as_str())
    );
    assert!(details
        .metadata
        .annotations
        .as_ref()
        .is_some_and(|a| !a.contains_key(chunks::CHUNK_ANNOTATION)));
}

#[tokio::test]
async fn every_chunk_is_written_before_the_caller_can_commit() {
    let entries = [TopicEntry::new("orders", 6)];
    let split = chunks::split(&entries);
    let objects = vec![chunks::build_chunk(&job_name(), NS, &owner(), &split[0], 1)];
    let routes = vec![Route {
        method: "POST",
        path_suffix: "/configmaps",
        status: 201,
        body: serde_json::to_string(&objects[0]).unwrap(),
    }];
    let (client, recorder, _) = mock_client_recording_bodies(routes);
    let written = chunks::write_all(&client, NS, SUBJECT_UID, &objects)
        .await
        .expect("written");
    assert_eq!(written, vec![chunks::chunk_name(&job_name(), 0)]);
    let seen = recorder.lock().unwrap();
    assert!(
        seen.iter().all(|r| r.method == "POST"),
        "writing chunks patches no status: the commit is the CALLER's next act: {seen:?}"
    );
}

// ---------------------------------------------------------------------------
// Concurrency limits — D2 §4.3, §4.4, §12
// ---------------------------------------------------------------------------

fn labelled_job(namespace: &str, kind: &str, connection: Option<&str>, finished: bool) -> Job {
    let mut labels = serde_json::Map::new();
    labels.insert("app.kubernetes.io/component".to_string(), json!("check"));
    labels.insert("logweir.dev/check-kind".to_string(), json!(kind));
    if let Some(c) = connection {
        labels.insert("logweir.dev/check-connection-uid".to_string(), json!(c));
    }
    let status = if finished {
        json!({"conditions":[{"type":"Complete","status":"True",
            "lastProbeTime":"2026-09-16T11:59:00Z","lastTransitionTime":"2026-09-16T11:59:00Z"}]})
    } else {
        json!({})
    };
    serde_json::from_value(json!({
        "apiVersion":"batch/v1","kind":"Job",
        "metadata":{"name":format!("lwc-{kind}-{namespace}-{finished}"),
                    "namespace":namespace,"labels":Value::Object(labels)},
        "spec":{"template":{"spec":{"containers":[],"restartPolicy":"Never"}}},
        "status":status
    }))
    .expect("the fixture is a Job")
}

/// **A `Backup`'S OWN DISCOVERY SPENDS THE CONNECTION AND NOTHING ELSE** —
/// PLAT-09.2 against D2 §4.4's `maxActiveDiscoveriesPerConnection`.
///
/// A per-run discovery Job (`logweir.dev/purpose=topic-discovery`) deliberately
/// does NOT wear `app.kubernetes.io/component=check`: nothing admits it —
/// `admit` is never called for work an operator already scheduled — so wearing
/// that label would spend the interactive pool without ever being bounded by
/// it, and a browser click would queue behind a nightly schedule. What it
/// should spend is the per-connection ceiling, which exists to bound
/// simultaneous dials at one broker: eight dynamic schedules firing at 02:00
/// against one `KafkaCluster` is exactly the case.
///
/// KILLS: counting a run discovery into `namespace`/`total` (which would let a
/// schedule queue a console click); ignoring it entirely (which would leave the
/// administrator's connection budget unenforced); attributing it to a
/// connection whose UID it does not carry.
#[test]
fn a_run_discovery_counts_against_the_connection_and_not_the_interactive_pools() {
    let run_discovery = |connection: Option<&str>| -> Job {
        let mut j = labelled_job(NS, "topicInventory", connection, false);
        let labels = j.metadata.labels.get_or_insert_with(Default::default);
        labels.insert(
            "app.kubernetes.io/component".to_string(),
            "run-discovery".to_string(),
        );
        labels.insert(
            "logweir.dev/purpose".to_string(),
            "topic-discovery".to_string(),
        );
        j.metadata.name = Some(format!("lwd-{}", connection.unwrap_or("none")));
        j
    };

    let counts = limits::count(
        &[run_discovery(Some(CONNECTION_UID))],
        NS,
        Some(CONNECTION_UID),
    );
    assert_eq!(counts.per_connection, 1, "it spends the broker's budget");
    assert_eq!(counts.namespace, 0, "and not the namespace pool");
    assert_eq!(counts.total, 0, "and not the installation pool");
    assert_eq!(counts.evidence_namespace, 0);
    assert_eq!(
        limits::admit(
            &counts,
            &policy::ChecksPolicy::default(),
            CheckPlanKind::TopicInventory
        ),
        limits::Admission::Queued(CheckCode::ConcurrencyLimited),
        "an interactive discovery against that connection is now queued"
    );

    // Against a DIFFERENT connection it counts for nothing at all.
    let counts = limits::count(
        &[run_discovery(Some(CONNECTION_UID))],
        NS,
        Some(OTHER_JOB_UID),
    );
    assert_eq!(counts, limits::ActiveCounts::default());

    // A FINISHED run discovery holds nothing, like every other finished Job.
    let mut done = run_discovery(Some(CONNECTION_UID));
    done.status = serde_json::from_value(json!({"conditions":[{"type":"Complete","status":"True",
        "lastProbeTime":"2026-09-16T11:59:00Z","lastTransitionTime":"2026-09-16T11:59:00Z"}]}))
    .expect("a JobStatus");
    assert_eq!(
        limits::count(&[done], NS, Some(CONNECTION_UID)),
        limits::ActiveCounts::default()
    );

    // AND THE INTERACTIVE ADMISSION STILL IGNORES THEM. Four run discoveries in
    // this namespace — more than `maxActivePerNamespace` — admit an interactive
    // check against another connection, because they spend neither pool.
    let crowd: Vec<Job> = (0..4)
        .map(|i| {
            let mut j = run_discovery(Some(CONNECTION_UID));
            j.metadata.name = Some(format!("lwd-{i}"));
            j
        })
        .collect();
    let counts = limits::count(&crowd, NS, Some(OTHER_JOB_UID));
    assert_eq!(counts, limits::ActiveCounts::default());
    assert!(limits::admit(
        &counts,
        &policy::ChecksPolicy::default(),
        CheckPlanKind::TopicInventory
    )
    .is_admitted());
    assert!(limits::admit(
        &counts,
        &policy::ChecksPolicy::default(),
        CheckPlanKind::OperationReadiness
    )
    .is_admitted());

    // An INTERACTIVE check, by contrast, spends both pools and not the
    // connection unless it is a discovery — the rule this one is beside.
    let interactive = labelled_job(NS, "operationReadiness", Some(CONNECTION_UID), false);
    let counts = limits::count(&[interactive], NS, Some(CONNECTION_UID));
    assert_eq!(
        (counts.namespace, counts.total, counts.per_connection),
        (1, 1, 0)
    );
}

/// D2 §12's framework row `limits::queues_over_namespace_cap`.
#[test]
fn limits_queues_over_namespace_cap() {
    let policy = policy::ChecksPolicy::default(); // 4 / 20 / 1 / 4
    let active = |n: usize| -> Vec<Job> {
        (0..n)
            .map(|i| {
                let mut j = labelled_job(NS, "operationReadiness", None, false);
                j.metadata.name = Some(format!("lwc-rd-{i}"));
                j
            })
            .collect()
    };

    // Under the cap: admitted.
    let counts = limits::count(&active(3), NS, None);
    assert_eq!(counts.namespace, 3);
    assert_eq!(counts.total, 3);
    assert_eq!(
        limits::admit(&counts, &policy, CheckPlanKind::OperationReadiness),
        limits::Admission::Admit
    );

    // AT the cap: queued, with the one reason D2 gives every ceiling.
    let counts = limits::count(&active(4), NS, None);
    assert_eq!(
        limits::admit(&counts, &policy, CheckPlanKind::OperationReadiness),
        limits::Admission::Queued(CheckCode::ConcurrencyLimited)
    );
    assert!(!limits::admit(&counts, &policy, CheckPlanKind::OperationReadiness).is_admitted());

    // A FINISHED Job occupies no slot. Counted from the Job's own conditions
    // and not from a CR phase, which is this controller's own writing.
    let mut jobs = active(3);
    jobs.push(labelled_job(NS, "operationReadiness", None, true));
    jobs.push(labelled_job(NS, "operationReadiness", None, true));
    let counts = limits::count(&jobs, NS, None);
    assert_eq!(counts.namespace, 3, "two finished Jobs hold nothing");
    assert!(limits::admit(&counts, &policy, CheckPlanKind::OperationReadiness).is_admitted());

    // ANOTHER NAMESPACE spends the total pool but not this namespace's.
    let mut jobs = active(2);
    jobs.extend((0..5).map(|i| {
        let mut j = labelled_job("other-ns", "operationReadiness", None, false);
        j.metadata.name = Some(format!("lwc-rd-other-{i}"));
        j
    }));
    let counts = limits::count(&jobs, NS, None);
    assert_eq!(counts.namespace, 2);
    assert_eq!(counts.total, 7);
    assert!(limits::admit(&counts, &policy, CheckPlanKind::OperationReadiness).is_admitted());
    // …until the TOTAL cap bites.
    let tight = policy::ChecksPolicy {
        max_active_total: 7,
        ..policy
    };
    assert_eq!(
        limits::admit(&counts, &tight, CheckPlanKind::OperationReadiness),
        limits::Admission::Queued(CheckCode::ConcurrencyLimited)
    );
}

#[test]
fn one_discovery_per_connection_and_a_separate_evidence_pool() {
    let policy = policy::ChecksPolicy::default();

    // D2 §4.4's `maxActiveDiscoveriesPerConnection: 1`.
    let jobs = vec![labelled_job(
        NS,
        "topicInventory",
        Some(CONNECTION_UID),
        false,
    )];
    let counts = limits::count(&jobs, NS, Some(CONNECTION_UID));
    assert_eq!(counts.per_connection, 1);
    assert_eq!(
        limits::admit(&counts, &policy, CheckPlanKind::TopicInventory),
        limits::Admission::Queued(CheckCode::ConcurrencyLimited)
    );
    // A discovery against ANOTHER connection is unaffected.
    let counts = limits::count(&jobs, NS, Some(OTHER_JOB_UID));
    assert_eq!(counts.per_connection, 0);
    assert!(limits::admit(&counts, &policy, CheckPlanKind::TopicInventory).is_admitted());
    // And the per-connection cap is a DISCOVERY rule: a readiness check against
    // the same cluster is not queued by it.
    let counts = limits::count(&jobs, NS, Some(CONNECTION_UID));
    assert!(limits::admit(&counts, &policy, CheckPlanKind::OperationReadiness).is_admitted());

    // THE SEPARATE EVIDENCE POOL. Four interactive checks fill the namespace
    // pool; an evidence fetch is still admitted, because verification must not
    // be starved by interactive checks (D2 §4.3).
    let busy: Vec<Job> = (0..4)
        .map(|i| {
            let mut j = labelled_job(NS, "topicInventory", None, false);
            j.metadata.name = Some(format!("lwc-td-{i}"));
            j
        })
        .collect();
    let counts = limits::count(&busy, NS, None);
    assert_eq!(counts.namespace, 4);
    assert_eq!(counts.evidence_namespace, 0);
    assert_eq!(
        limits::admit(&counts, &policy, CheckPlanKind::TopicInventory),
        limits::Admission::Queued(CheckCode::ConcurrencyLimited)
    );
    assert!(
        limits::admit(&counts, &policy, CheckPlanKind::EvidenceFetch).is_admitted(),
        "an evidence fetch spends its own pool and nothing else"
    );

    // And an evidence fetch does not spend the general pools either.
    let evidence: Vec<Job> = (0..4)
        .map(|i| {
            let mut j = labelled_job(NS, "evidenceFetch", None, false);
            j.metadata.name = Some(format!("lwc-ev-{i}"));
            j
        })
        .collect();
    let counts = limits::count(&evidence, NS, None);
    assert_eq!(counts.namespace, 0);
    assert_eq!(counts.total, 0);
    assert_eq!(counts.evidence_namespace, 4);
    assert!(limits::admit(&counts, &policy, CheckPlanKind::TopicInventory).is_admitted());
    assert_eq!(
        limits::admit(&counts, &policy, CheckPlanKind::EvidenceFetch),
        limits::Admission::Queued(CheckCode::ConcurrencyLimited)
    );
}

#[test]
fn the_active_count_selects_only_check_jobs_and_a_job_this_build_cannot_name_still_holds_a_slot() {
    assert_eq!(
        limits::check_selector(),
        "app.kubernetes.io/component=check"
    );
    assert_eq!(limits::QUEUED_REQUEUE_SECS, 10);

    // A check Job whose kind label this build does not recognise counts against
    // the general pools and against no kind-specific one — the safe direction
    // for a ceiling.
    let unknown = labelled_job(NS, "somethingNew", None, false);
    let counts = limits::count(&[unknown], NS, Some(CONNECTION_UID));
    assert_eq!(counts.namespace, 1);
    assert_eq!(counts.total, 1);
    assert_eq!(counts.per_connection, 0);
    assert_eq!(counts.evidence_namespace, 0);

    assert!(limits::is_active(&labelled_job(
        NS,
        "topicInventory",
        None,
        false
    )));
    assert!(!limits::is_active(&labelled_job(
        NS,
        "topicInventory",
        None,
        true
    )));
}

#[tokio::test]
async fn the_active_count_lists_by_the_component_label_and_nothing_else() {
    let routes = vec![Route {
        method: "GET",
        path_suffix: "/jobs",
        status: 200,
        body: serde_json::to_string(&json!({
            "apiVersion":"batch/v1","kind":"JobList","metadata":{},
            "items":[labelled_job(NS, "topicInventory", Some(CONNECTION_UID), false)]
        }))
        .unwrap(),
    }];
    let (client, recorder, _) = mock_client_recording_bodies(routes);
    let jobs = limits::check_jobs(&client).await.expect("listed");
    assert_eq!(jobs.len(), 1);
    let seen = recorder.lock().unwrap();
    assert_eq!(
        seen.len(),
        1,
        "ONE request, whatever the selector says: a count that costs two \
         installation-wide LISTs per pass is a count nobody can afford"
    );
    // The SET form, since PLAT-09.2: one key, two values, so the same request
    // finds an interactive check and a `Backup`'s own per-run discovery. A
    // selector ANDs its terms and cannot express "either key", which is why the
    // two populations share `app.kubernetes.io/component`.
    let decoded = seen[0].uri.replace("%2F", "/").replace("%2C", ",");
    assert!(
        decoded.contains("app.kubernetes.io/component") && decoded.contains("check"),
        "the count must not list every Job in the cluster: {}",
        seen[0].uri
    );
    assert!(
        decoded.contains("run-discovery"),
        "and it must see a run's own discovery, or the per-connection ceiling \
         does not bound the thing it exists to bound: {}",
        seen[0].uri
    );
    assert_eq!(
        limits::active_selector(),
        "app.kubernetes.io/component in (check,run-discovery)"
    );
    assert_eq!(
        limits::check_selector(),
        "app.kubernetes.io/component=check",
        "the console pool's own membership is unchanged"
    );
    assert!(
        !seen[0].uri.contains("/namespaces/"),
        "the total pool is installation-wide: {}",
        seen[0].uri
    );
}

// ---------------------------------------------------------------------------
// H4 — a finished Job whose pod never wrote a log
// ---------------------------------------------------------------------------

/// **`observe` must reach `classify`'s `DeadlineExceeded` branch.**
///
/// A check pod that sat in `ImagePullBackOff` — which is NOT in
/// `Waiting::is_terminal`, so it is not cancelled early — reaches its
/// `activeDeadlineSeconds` without ever starting its `runner` container. The
/// API server then answers `pods/log` with **400** ("container \"runner\" … is
/// waiting to start"). While `observe` propagated that with `?`, every
/// reconcile returned `Err` and requeued: no status was ever committed, so no
/// TTL was ever patched, and the operator saw a spinner instead of
/// `DeadlineExceeded`.
#[tokio::test]
async fn a_finished_job_whose_pod_never_started_is_classified_not_requeued() {
    let never_started = json!({"phase":"Failed","containerStatuses":[{
        "name":"runner","image":"x","imageID":"","ready":false,"restartCount":0,
        "state":{"waiting":{"reason":"ImagePullBackOff","message":"back-off pulling image"}}
    }]});
    for (code, body) in [
        (
            400,
            r#"{"kind":"Status","apiVersion":"v1","status":"Failure",
                "message":"container \"runner\" in pod \"lwc-td-9ab-xyz12\" is waiting to start: trying and failing to pull image",
                "reason":"BadRequest","code":400}"#,
        ),
        (
            404,
            r#"{"kind":"Status","apiVersion":"v1","status":"Failure",
                "message":"pods \"lwc-td-9ab-xyz12\" not found","reason":"NotFound","code":404}"#,
        ),
    ] {
        let routes = vec![
            Route {
                method: "GET",
                path_suffix: "/pods",
                status: 200,
                body: pod_list(vec![pod_object(
                    Some(("Job", JOB_UID, true)),
                    never_started.clone(),
                )]),
            },
            Route {
                method: "GET",
                path_suffix: POD_LOG_PATH,
                status: code,
                body: body.to_string(),
            },
        ];
        let (client, _, _) = mock_client_recording_bodies(routes);
        let observation = check::observe(
            &client,
            NS,
            &job_object(Some("Failed"), Some("DeadlineExceeded")),
            &[],
            &expectations(),
            now(),
        )
        .await
        .unwrap_or_else(|e| panic!("a {code} on pods/log must not abort the pass: {e}"));
        assert_eq!(observation.phase, CheckPhase::Failed);
        assert_eq!(
            observation.reason,
            CheckCode::RunnerImagePullFailed,
            "a finished Job whose pod never started must report WHAT it was waiting for, not \
             `ResultUnreadable` (the symptom) and not `DeadlineExceeded` (the clock) ({code})"
        );
        assert!(observation.relay.is_none());
    }

    // And the pure deadline shape: a pod with no container status at all, so
    // there is no more specific waiting row to report.
    let routes = vec![
        Route {
            method: "GET",
            path_suffix: "/pods",
            status: 200,
            body: pod_list(vec![pod_object(
                Some(("Job", JOB_UID, true)),
                json!({"phase": "Failed"}),
            )]),
        },
        Route {
            method: "GET",
            path_suffix: POD_LOG_PATH,
            status: 400,
            body: r#"{"kind":"Status","apiVersion":"v1","status":"Failure",
                "message":"waiting to start","reason":"BadRequest","code":400}"#
                .to_string(),
        },
    ];
    let (client, _, _) = mock_client_recording_bodies(routes);
    let observation = check::observe(
        &client,
        NS,
        &job_object(Some("Failed"), Some("DeadlineExceeded")),
        &[],
        &expectations(),
        now(),
    )
    .await
    .expect("a 400 on pods/log must not abort the pass");
    assert_eq!(
        observation.reason,
        CheckCode::DeadlineExceeded,
        "the branch written for a pod that never wrote a relay must be reachable"
    );

    // A transport-class failure is still a requeue: a verdict published from a
    // control-plane failure would be a claim about somebody's cluster.
    assert!(check::is_log_absent(&kube::Error::Api(
        kube::core::ErrorResponse {
            status: "Failure".into(),
            message: "waiting to start".into(),
            reason: "BadRequest".into(),
            code: 400,
        }
    )));
    for code in [401, 403, 429, 500, 503] {
        assert!(
            !check::is_log_absent(&kube::Error::Api(kube::core::ErrorResponse {
                status: "Failure".into(),
                message: "x".into(),
                reason: "y".into(),
                code,
            })),
            "{code} is a control-plane failure, not an absent log"
        );
    }
    let routes = vec![
        Route {
            method: "GET",
            path_suffix: "/pods",
            status: 200,
            body: pod_list(vec![pod_object(
                Some(("Job", JOB_UID, true)),
                never_started,
            )]),
        },
        Route {
            method: "GET",
            path_suffix: POD_LOG_PATH,
            status: 500,
            body: r#"{"kind":"Status","apiVersion":"v1","status":"Failure",
                "message":"internal","reason":"InternalError","code":500}"#
                .to_string(),
        },
    ];
    let (client, _, _) = mock_client_recording_bodies(routes);
    assert!(
        check::observe(
            &client,
            NS,
            &job_object(Some("Failed"), Some("DeadlineExceeded")),
            &[],
            &expectations(),
            now(),
        )
        .await
        .is_err(),
        "a 500 must still requeue"
    );
}

// ---------------------------------------------------------------------------
// L5 — a FailedCreate event must name THIS Job
// ---------------------------------------------------------------------------

#[test]
fn another_workloads_failed_create_never_cancels_this_check() {
    let theirs = EventFact {
        reason: "FailedCreate".to_string(),
        message: "Error creating: pods \"someone-else-x\" is forbidden: exceeded quota: compute"
            .to_string(),
        involved_kind: "Job".to_string(),
        involved_name: "someone-elses-job".to_string(),
    };
    let at = |events: &[EventFact]| {
        waiting::classify(&waiting::Observed {
            job: &job_object(None, None),
            pod: None,
            events,
            now: now(),
        })
    };
    assert_eq!(
        at(std::slice::from_ref(&theirs)),
        None,
        "a `FailedCreate` for another Job is not this check's finding — and \
         `PodCreateRejected` is in the early-cancel set, so taking it would CANCEL a healthy \
         check Job because of somebody else's ResourceQuota"
    );
    // A pod-scoped FailedCreate is not this Job's either.
    let pod_scoped = EventFact {
        involved_kind: "Pod".to_string(),
        involved_name: job_name(),
        ..theirs.clone()
    };
    assert_eq!(at(&[pod_scoped]), None);
    // Ours still is, and it is still found when it is not first.
    let ours = EventFact {
        involved_name: job_name(),
        ..theirs.clone()
    };
    assert_eq!(
        at(&[theirs, ours]).map(|w| w.code),
        Some(CheckCode::PodCreateRejected)
    );
}

// ---------------------------------------------------------------------------
// L6 — the redaction chokepoint holds for a relay refusal
// ---------------------------------------------------------------------------

#[test]
fn a_relay_refusal_reason_goes_through_the_redaction_chokepoint() {
    // `RelayRefusal::reason` reaches `Observation.message` and from there a
    // status condition. No `FrameError` variant reachable from `decode`
    // carries runner bytes today, but `CheckRelay::result()` — which W8 and W9
    // must call — produces one whose `CheckResultError::Contract(String)`
    // quotes the runner's own field verbatim. A chokepoint is only a
    // chokepoint if every path through it calls it.
    let src = std::fs::read_to_string(
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src/check/relay.rs"),
    )
    .expect("the relay module");
    let code = code_only(&src);
    assert!(
        code.contains("reason: redact(&error.to_string())"),
        "a relay refusal's reason must be redacted and capped like every other relayed string"
    );
    // And a long reason is capped, which `redact` does.
    let long = "x".repeat(10_000);
    assert!(
        logweir_core::check_contract::redact(&long).chars().count()
            <= logweir_core::check_contract::MESSAGE_MAX_CHARS,
        "redact caps, and that cap is what this path now inherits"
    );
}

// ---------------------------------------------------------------------------
// Q9 — fail_closed is defence in depth, and it has a guard of its own
// ---------------------------------------------------------------------------

/// `Policy::fail_closed()` is observationally equal to `Policy::defaults()`
/// TODAY, because both default collections are already empty — which is why a
/// reviewer's `fail_closed() -> defaults()` mutant was a no-op rather than a
/// weak guard. This is the guard it lacked: whatever the defaults come to hold,
/// a policy nobody can read clears the two collections that could turn a
/// listing into a completeness claim.
#[test]
fn fail_closed_clears_the_two_collections_whatever_the_defaults_hold() {
    let mut generous = policy::Policy::defaults();
    generous.discovery.visibility_attestations.push(
        serde_json::from_value(json!({
            "id": "att-from-a-future-default",
            "namespace": NS,
            "kafkaCluster": "source",
            "clusterId": "M29I2S7FQPyHBEX12Vx7XA",
            "principal": "User:backup",
            "attestedBy": "platform-admin@example.invalid",
            "attestedAt": "2026-09-15T00:00:00Z",
            "expiresAt": "2026-12-15T00:00:00Z",
            "statement": "a default nobody should inherit"
        }))
        .expect("an Attestation"),
    );
    generous
        .evidence
        .controller_identity_locations
        .push(policy::IdentityLocation {
            endpoint: "https://elsewhere:9000".to_string(),
            region: String::new(),
            bucket: "not-ours".to_string(),
        });
    assert!(!generous.discovery.visibility_attestations.is_empty());

    // `closed()` takes a VALUE, so the rule can be observed on a policy that
    // actually carries both collections. `fail_closed()` is
    // `defaults().closed()`, and the two are equal only because the defaults
    // are empty today — which is why a reviewer's `fail_closed() -> defaults()`
    // mutant was a no-op rather than a weak guard.
    let closed = generous.clone().closed();
    assert!(
        closed.discovery.visibility_attestations.is_empty(),
        "a policy nobody can read must never be the reason a listing is reported as complete"
    );
    assert!(
        closed.evidence.controller_identity_locations.is_empty(),
        "nor the reason an unlisted object-store location is treated as allowlisted"
    );
    assert_ne!(
        closed, generous,
        "if this ever passes vacuously the guard has stopped guarding"
    );
    assert_eq!(
        policy::Policy::fail_closed(),
        policy::Policy::defaults().closed(),
        "the constant and the rule are one thing"
    );
    let closed = policy::Policy::fail_closed();
    assert!(closed.discovery.visibility_attestations.is_empty());
    assert!(closed.evidence.controller_identity_locations.is_empty());
    // The LIMITS keep their defaults, so checks keep running.
    assert_eq!(
        closed.checks,
        policy::ChecksPolicy::default(),
        "failing closed must not stop every check in the installation"
    );
    assert_eq!(closed.discovery.hard_max_topics, 50_000);
}
